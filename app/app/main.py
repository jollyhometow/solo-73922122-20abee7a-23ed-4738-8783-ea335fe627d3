"""
变更日志查询 API（FastAPI）。

只查询独立日志库中已完整提交的事务；采集器按事务原子写入，
因此本接口永远不会读到“半笔事务”。

补传批次：把左开右闭区间 (from_lsn, to_lsn] 内反复更正的 results 变更
合成为稳定的入仓净变化并整体持久化；同区间只产生一份，创建后读取内容不变。
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, Response
from psycopg2.errors import InvalidTextRepresentation, UndefinedObject
from pydantic import BaseModel, Field

from . import backfill
from .backfill_core import BrokenChain
from .config import get_settings
from .db import log_query, source_query
from .lsn import lsn_to_int, lsn_valid

logger = logging.getLogger("api")
settings = get_settings()


@asynccontextmanager
async def lifespan(_: FastAPI):
    # 已有数据卷的环境升级时幂等补齐补传批次表（全新环境由 log-db/initdb 建好）
    try:
        backfill.ensure_schema(settings)
    except Exception:
        logger.exception("ensure backfill schema failed (will rely on initdb scripts)")
    yield


app = FastAPI(
    title="检验结果变更日志 API",
    description=(
        "基于 PostgreSQL 16 逻辑复制（wal2json）的 results 表变更日志。\n\n"
        "- 仅含建槽之后的 INSERT/UPDATE/DELETE；\n"
        "- 更正、删除保留完整旧值，同事务同行多次修改逐条保序；\n"
        "- 按源提交位置（commit_lsn）翻页，顺序与源提交一致；\n"
        "- 回滚事务不会出现；\n"
        "- 补传批次：指定左开右闭的源提交位置区间，把窗口内反复更正的记录\n"
        "  合成为稳定的入仓净变化（首条旧值 + 末条新值），持久化后内容不变。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


# ----------------------------------------------------------------- 响应模型
class Change(BaseModel):
    change_seq: int
    action: str
    table_name: str
    row_lsn: str | None
    old_values: dict[str, Any] | None
    new_values: dict[str, Any] | None


class Commit(BaseModel):
    xid: int
    commit_lsn: str
    begin_lsn: str | None
    commit_time: datetime
    change_count: int
    ingested_at: datetime
    changes: list[Change]


class CommitPage(BaseModel):
    items: list[Commit]
    next_cursor: str | None
    has_more: bool
    limit: int


class FlatChange(BaseModel):
    commit_lsn: str
    xid: int
    commit_time: datetime
    change_seq: int
    action: str
    sample_no: str | None
    test_item: str | None
    old_values: dict[str, Any] | None
    new_values: dict[str, Any] | None


class ChangePage(BaseModel):
    items: list[FlatChange]
    next_cursor: str | None
    has_more: bool
    limit: int


class Status(BaseModel):
    slot_name: str
    collector_alive: bool
    heartbeat_age_seconds: float | None
    persisted_lsn: str | None
    confirmed_lsn: str | None
    last_xid: int | None
    last_change_at: datetime | None
    source_slot_active: bool | None
    source_restart_lsn: str | None
    source_confirmed_flush_lsn: str | None
    source_pg_current_lsn: str | None
    retained_wal_bytes: int | None


@app.get("/", include_in_schema=False)
def root():
    return {"service": "lab-results-cdc", "docs": "/docs"}


@app.get("/health", include_in_schema=False)
def health():
    return {"status": "ok"}


# ------------------------------------------------------------- 提交位置翻页
@app.get("/commits", response_model=CommitPage, summary="按提交位置翻页查询完整事务")
def list_commits(
    cursor: str | None = Query(None, description="上一页返回的 next_cursor（提交位置 LSN），首页不传"),
    limit: int = Query(20, ge=1, le=200),
    direction: Literal["forward", "backward"] = Query("forward"),
):
    if cursor is not None and not lsn_valid(cursor):
        raise HTTPException(status_code=400, detail=f"invalid cursor LSN: {cursor!r}")

    order = "ASC" if direction == "forward" else "DESC"
    cmp_op = ">" if direction == "forward" else "<"
    params: list[Any] = [settings.slot_name]
    where = "slot_name = %s"
    if cursor:
        where += f" AND commit_lsn {cmp_op} %s::pg_lsn"
        params.append(cursor)

    # 多取一条用于判断 has_more
    sql = f"""
        SELECT xid, commit_lsn::text AS commit_lsn, begin_lsn::text AS begin_lsn,
               commit_time, change_count, ingested_at
          FROM source_commit
         WHERE {where}
      ORDER BY commit_lsn {order}
         LIMIT %s
    """
    try:
        rows = log_query(settings, sql, params + [limit + 1])
    except InvalidTextRepresentation:
        raise HTTPException(status_code=400, detail="invalid cursor LSN")

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = page[-1]["commit_lsn"] if has_more and page else None

    if page:
        details = log_query(
            settings,
            """
            SELECT commit_lsn::text AS commit_lsn, change_seq, action, table_name,
                   row_lsn::text AS row_lsn, old_values, new_values
              FROM result_change
             WHERE slot_name = %s AND commit_lsn = ANY(%s::pg_lsn[])
             ORDER BY commit_lsn, change_seq
            """,
            [settings.slot_name, [r["commit_lsn"] for r in page]],
        )
        grouped: dict[str, list] = {}
        for d in details:
            grouped.setdefault(d["commit_lsn"], []).append(d)
        for r in page:
            r["changes"] = grouped.get(r["commit_lsn"], [])

    return CommitPage(items=page, next_cursor=next_cursor, has_more=has_more, limit=limit)


@app.get("/commits/{commit_lsn}", response_model=Commit, summary="按提交位置取单笔事务及全部前后值")
def get_commit(commit_lsn: str):
    if not lsn_valid(commit_lsn):
        raise HTTPException(status_code=400, detail=f"invalid LSN: {commit_lsn!r}")
    try:
        head = log_query(
            settings,
            """
            SELECT xid, commit_lsn::text AS commit_lsn, begin_lsn::text AS begin_lsn,
                   commit_time, change_count, ingested_at
              FROM source_commit
             WHERE slot_name = %s AND commit_lsn = %s::pg_lsn
            """,
            [settings.slot_name, commit_lsn],
        )
    except InvalidTextRepresentation:
        raise HTTPException(status_code=400, detail="invalid LSN")
    if not head:
        raise HTTPException(status_code=404, detail="commit not found")

    changes = log_query(
        settings,
        """
        SELECT change_seq, action, table_name, row_lsn::text AS row_lsn,
               old_values, new_values
          FROM result_change
         WHERE slot_name = %s AND commit_lsn = %s::pg_lsn
      ORDER BY change_seq
        """,
        [settings.slot_name, commit_lsn],
    )
    head[0]["changes"] = changes
    return head[0]


# ------------------------------------------------------------- 行变更检索
@app.get("/changes", response_model=ChangePage, summary="按样本号/动作检索行级变更（含前后值）")
def list_changes(
    sample_no: str | None = Query(None, description="样本号，匹配 new/old 两侧，支持更正与删除追溯"),
    action: Literal["I", "U", "D"] | None = Query(None),
    cursor: str | None = Query(None, description="提交位置 LSN 游标"),
    limit: int = Query(50, ge=1, le=500),
):
    if cursor is not None and not lsn_valid(cursor):
        raise HTTPException(status_code=400, detail=f"invalid cursor LSN: {cursor!r}")

    where = ["rc.slot_name = %s"]
    params: list[Any] = [settings.slot_name]
    if sample_no:
        where.append(
            "COALESCE(rc.new_values ->> 'sample_no', rc.old_values ->> 'sample_no') = %s"
        )
        params.append(sample_no)
    if action:
        where.append("rc.action = %s")
        params.append(action)
    if cursor:
        where.append("rc.commit_lsn > %s::pg_lsn")
        params.append(cursor)

    sql = f"""
        SELECT rc.commit_lsn::text AS commit_lsn, sc.xid, sc.commit_time,
               rc.change_seq, rc.action,
               COALESCE(rc.new_values ->> 'sample_no', rc.old_values ->> 'sample_no') AS sample_no,
               COALESCE(rc.new_values ->> 'test_item', rc.old_values ->> 'test_item') AS test_item,
               rc.old_values, rc.new_values
          FROM result_change rc
          JOIN source_commit sc
            ON sc.slot_name = rc.slot_name AND sc.commit_lsn = rc.commit_lsn
         WHERE {' AND '.join(where)}
      ORDER BY rc.commit_lsn ASC, rc.change_seq ASC
         LIMIT %s
    """
    try:
        rows = log_query(settings, sql, params + [limit + 1])
    except InvalidTextRepresentation:
        raise HTTPException(status_code=400, detail="invalid cursor LSN")

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = page[-1]["commit_lsn"] if has_more and page else None
    return ChangePage(items=page, next_cursor=next_cursor, has_more=has_more, limit=limit)


# ----------------------------------------------------------------- 采集状态
@app.get("/status", response_model=Status, summary="采集状态、续采位置与源槽 WAL 保留情况")
def get_status():
    state_rows = log_query(
        settings,
        """
        SELECT s.persisted_lsn::text AS persisted_lsn,
               s.confirmed_lsn::text AS confirmed_lsn,
               s.last_xid, s.last_change_at,
               EXTRACT(EPOCH FROM (now() - h.heartbeat_at))::float AS heartbeat_age_seconds
          FROM slot_state s
          LEFT JOIN collector_heartbeat h ON h.slot_name = s.slot_name
         WHERE s.slot_name = %s
        """,
        [settings.slot_name],
    )
    state = state_rows[0] if state_rows else {}
    alive = (state.get("heartbeat_age_seconds") is not None
             and state["heartbeat_age_seconds"] < 30)

    slot = {}
    try:
        slot_rows = source_query(
            settings,
            """
            SELECT active,
                   restart_lsn::text AS restart_lsn,
                   confirmed_flush_lsn::text AS confirmed_flush_lsn
              FROM pg_replication_slots
             WHERE slot_name = %s
            """,
            [settings.slot_name],
        )
        if slot_rows:
            slot = slot_rows[0]
        current = source_query(settings, "SELECT pg_current_wal_lsn()::text AS lsn")
        current_lsn = current[0]["lsn"]
        retained = source_query(
            settings,
            "SELECT pg_wal_lsn_diff(%s, %s)::bigint AS bytes",
            [current_lsn, slot.get("restart_lsn") or current_lsn],
        )[0]["bytes"]
    except UndefinedObject:
        current_lsn, retained = None, None
    except Exception:
        # 源库暂不可达时仍返回日志库侧状态
        current_lsn, retained = None, None

    return Status(
        slot_name=settings.slot_name,
        collector_alive=bool(alive),
        heartbeat_age_seconds=state.get("heartbeat_age_seconds"),
        persisted_lsn=state.get("persisted_lsn"),
        confirmed_lsn=state.get("confirmed_lsn"),
        last_xid=state.get("last_xid"),
        last_change_at=state.get("last_change_at"),
        source_slot_active=slot.get("active"),
        source_restart_lsn=slot.get("restart_lsn"),
        source_confirmed_flush_lsn=slot.get("confirmed_flush_lsn"),
        source_pg_current_lsn=current_lsn,
        retained_wal_bytes=retained,
    )


# ================================================================= 补传批次
class BackfillBatchCreate(BaseModel):
    from_lsn: str = Field(
        "0/0",
        description="左开起点（不含该位置的提交）。缺省 0/0 表示从日志开头开始",
        examples=["0/1A2B3C8"],
    )
    to_lsn: str = Field(
        ...,
        description="右闭终点（含），必须是已完整入库的事务提交位置",
        examples=["0/1A2C900"],
    )


class BackfillRow(BaseModel):
    row_seq: int
    row_id: int
    action: str
    old_sample_no: str | None
    old_test_item: str | None
    new_sample_no: str | None
    new_test_item: str | None
    key_renamed: bool
    old_values: dict[str, Any] | None
    new_values: dict[str, Any] | None
    first_commit_lsn: str
    last_commit_lsn: str
    source_changes: int


class BackfillBatch(BaseModel):
    batch_id: int
    slot_name: str
    from_lsn: str
    to_lsn: str
    commit_count: int
    change_count: int
    row_count: int
    created_at: datetime
    rows: list[BackfillRow]


class BackfillBatchSummary(BaseModel):
    batch_id: int
    slot_name: str
    from_lsn: str
    to_lsn: str
    commit_count: int
    change_count: int
    row_count: int
    created_at: datetime


@app.post(
    "/backfill-batches",
    response_model=BackfillBatch,
    status_code=201,
    summary="创建补传批次：左开右闭区间内反复更正的记录合成为稳定净变化",
    responses={
        200: {"description": "同区间批次已存在（并发创建或客户端重试），返回已持久化的那一份"},
        400: {"description": "区间端点不合法（终点不是已完整入库的事务等）"},
        409: {"description": "窗口内变更链断裂，整批不发布；detail 指出断裂的提交位置"},
    },
)
def create_backfill_batch(req: BackfillBatchCreate, response: Response):
    """把 (from_lsn, to_lsn] 内的 results 变更整理成稳定的入仓批次：

    - 按 results.id 分组、源变更顺序排序，取窗口内首条旧值与末条新值合成净变化，
      插入前、删除后视为空；前后相同、插入后删除的行不进入批次；
    - 样本号/检测项目改名时保留旧、新键（old_*/new_*），供下游撤销旧记录并写入新记录；
    - 区间与结果一起持久化，同区间并发创建只产生一份；创建后即使采集继续、
      客户端重试或服务重启，读取内容仍相同。
    """
    if not lsn_valid(req.from_lsn) or not lsn_valid(req.to_lsn):
        raise HTTPException(status_code=400, detail="invalid LSN format")
    if lsn_to_int(req.from_lsn) >= lsn_to_int(req.to_lsn):
        raise HTTPException(
            status_code=400,
            detail=f"左开起点必须小于右闭终点：from_lsn={req.from_lsn}, to_lsn={req.to_lsn}",
        )
    try:
        batch, created = backfill.create_batch(settings, req.from_lsn, req.to_lsn)
    except backfill.IntervalError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except BrokenChain as exc:
        # 整批不发布：指出断裂的提交位置与原因
        raise HTTPException(status_code=409, detail=exc.detail())
    if not created:
        response.status_code = 200
    return batch


@app.get(
    "/backfill-batches",
    response_model=list[BackfillBatchSummary],
    summary="补传批次列表（不含批次行）",
)
def list_backfill_batches():
    return backfill.list_batches(settings)


@app.get(
    "/backfill-batches/{batch_id}",
    response_model=BackfillBatch,
    summary="读取补传批次（内容自创建起不变）",
)
def get_backfill_batch(batch_id: int):
    batch = backfill.get_batch(settings, batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="batch not found")
    return batch
