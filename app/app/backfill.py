"""
补传批次（backfill batch）：区间校验、持久化与读取（全部落在独立日志库）。

语义要点：
  * 区间为源提交位置的左开右闭区间 (from_lsn, to_lsn]；
    to_lsn 必须是已完整入库的事务（存在于 source_commit，采集器按事务原子写入，
    存在即完整）；from_lsn 缺省 0/0 表示从日志开头开始，否则也必须是已入库提交。
  * 窗口内行变更按 (commit_lsn, change_seq) 排序后由 backfill_core.synthesize
    合成净变化；变更链断裂（BrokenChain）时整批不发布、什么都不写。
  * 批次与区间一起持久化，(slot_name, from_lsn, to_lsn) 唯一：
    同区间并发创建/客户端重试只产生一份（ON CONFLICT DO NOTHING 后回读）；
    批次行创建时一次性写入、之后永不修改——采集继续、服务重启后读取内容不变。
"""
from __future__ import annotations

import json
import logging
from typing import Any

from .backfill_core import BrokenChain, synthesize
from .db import log_conn, log_query

logger = logging.getLogger("backfill")

# 与 log-db/initdb/02_backfill.sql 保持一致（修改时两边同步）。
# API 启动时幂等执行，便于在已有数据卷的环境上平滑升级。
SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS backfill_batch (
        id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        slot_name    TEXT   NOT NULL,
        from_lsn     PG_LSN NOT NULL,
        to_lsn       PG_LSN NOT NULL,
        commit_count INTEGER NOT NULL,
        change_count INTEGER NOT NULL,
        row_count    INTEGER NOT NULL,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (slot_name, from_lsn, to_lsn)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS backfill_batch_row (
        id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        batch_id         BIGINT  NOT NULL REFERENCES backfill_batch(id) ON DELETE CASCADE,
        row_seq          INTEGER NOT NULL,
        row_id           BIGINT  NOT NULL,
        action           CHAR(1) NOT NULL CHECK (action IN ('I','U','D')),
        old_sample_no    TEXT,
        old_test_item    TEXT,
        new_sample_no    TEXT,
        new_test_item    TEXT,
        key_renamed      BOOLEAN NOT NULL DEFAULT FALSE,
        old_values       JSONB,
        new_values       JSONB,
        first_commit_lsn PG_LSN NOT NULL,
        last_commit_lsn  PG_LSN NOT NULL,
        source_changes   INTEGER NOT NULL,
        UNIQUE (batch_id, row_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_backfill_batch_row_batch
        ON backfill_batch_row (batch_id, row_seq)
    """,
)


class IntervalError(Exception):
    """区间端点不合法（端点不是已完整入库的事务）。"""


def ensure_schema(settings) -> None:
    """幂等创建补传批次表（新环境由 log-db/initdb 建好，这里兜底升级）。"""
    with log_conn(settings) as conn, conn.cursor() as cur:
        for stmt in SCHEMA_STATEMENTS:
            cur.execute(stmt)
        conn.commit()


def _require_ingested_endpoint(settings, lsn: str, label: str) -> None:
    rows = log_query(
        settings,
        """
        SELECT 1 FROM source_commit
         WHERE slot_name = %s AND commit_lsn = %s::pg_lsn
        """,
        [settings.slot_name, lsn],
    )
    if not rows:
        if label == "起点":
            raise IntervalError(
                f"起点 {lsn} 不是已完整入库的事务；如需从日志开头取数，请使用 0/0"
            )
        raise IntervalError(
            f"终点 {lsn} 不是已完整入库的事务（source_commit 中不存在该提交位置）"
        )


def create_batch(settings, from_lsn: str, to_lsn: str) -> tuple[dict, bool]:
    """创建（或幂等返回）补传批次。返回 (batch, created)。

    - 终点/起点校验失败抛 IntervalError；
    - 窗口内变更链断裂抛 BrokenChain，整批不发布；
    - 同区间已存在时返回已持久化的那一份，created=False。
    """
    _require_ingested_endpoint(settings, to_lsn, "终点")
    if from_lsn != "0/0":
        _require_ingested_endpoint(settings, from_lsn, "起点")

    changes = log_query(
        settings,
        """
        SELECT commit_lsn::text AS commit_lsn, change_seq, action,
               old_values, new_values
          FROM result_change
         WHERE slot_name = %s
           AND commit_lsn >  %s::pg_lsn
           AND commit_lsn <= %s::pg_lsn
         ORDER BY commit_lsn ASC, change_seq ASC
        """,
        [settings.slot_name, from_lsn, to_lsn],
    )

    # 链断裂时在此抛出 BrokenChain：尚未写任何批次数据，整批不发布
    net_rows = synthesize(changes)
    commit_count = len({c["commit_lsn"] for c in changes})

    with log_conn(settings) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO backfill_batch
                (slot_name, from_lsn, to_lsn, commit_count, change_count, row_count)
            VALUES (%s, %s::pg_lsn, %s::pg_lsn, %s, %s, %s)
            ON CONFLICT (slot_name, from_lsn, to_lsn) DO NOTHING
            RETURNING id
            """,
            (
                settings.slot_name, from_lsn, to_lsn,
                commit_count, len(changes), len(net_rows),
            ),
        )
        got = cur.fetchone()
        if got is None:
            # 同区间已存在（并发创建或客户端重试）：返回已持久化的那一份
            conn.rollback()
            existing = get_batch_by_interval(settings, from_lsn, to_lsn)
            if existing is None:  # 理论上不会发生
                raise RuntimeError("backfill batch conflicted but not found afterwards")
            logger.info("batch for (%s, %s] already exists: id=%s, return persisted copy",
                        from_lsn, to_lsn, existing["batch_id"])
            return existing, False

        batch_id = got[0]
        if net_rows:
            cur.executemany(
                """
                INSERT INTO backfill_batch_row
                    (batch_id, row_seq, row_id, action,
                     old_sample_no, old_test_item, new_sample_no, new_test_item,
                     key_renamed, old_values, new_values,
                     first_commit_lsn, last_commit_lsn, source_changes)
                VALUES (%(batch_id)s, %(row_seq)s, %(row_id)s, %(action)s,
                        %(old_sample_no)s, %(old_test_item)s,
                        %(new_sample_no)s, %(new_test_item)s,
                        %(key_renamed)s, %(old_values)s, %(new_values)s,
                        %(first_commit_lsn)s, %(last_commit_lsn)s, %(source_changes)s)
                """,
                [
                    {
                        "batch_id": batch_id,
                        "row_seq": seq,
                        "row_id": r["row_id"],
                        "action": r["action"],
                        "old_sample_no": r["old_sample_no"],
                        "old_test_item": r["old_test_item"],
                        "new_sample_no": r["new_sample_no"],
                        "new_test_item": r["new_test_item"],
                        "key_renamed": r["key_renamed"],
                        "old_values": json.dumps(r["old_values"], ensure_ascii=False)
                        if r["old_values"] is not None else None,
                        "new_values": json.dumps(r["new_values"], ensure_ascii=False)
                        if r["new_values"] is not None else None,
                        "first_commit_lsn": r["first_commit_lsn"],
                        "last_commit_lsn": r["last_commit_lsn"],
                        "source_changes": r["source_changes"],
                    }
                    for seq, r in enumerate(net_rows, start=1)
                ],
            )
        conn.commit()  # 批次头 + 批次行在同一事务落盘，读取只见完整批次

    logger.info("created backfill batch id=%s window=(%s, %s] rows=%s changes=%s",
                batch_id, from_lsn, to_lsn, len(net_rows), len(changes))
    return get_batch(settings, batch_id), True


def get_batch(settings, batch_id: int) -> dict | None:
    heads = log_query(
        settings,
        """
        SELECT id AS batch_id, slot_name,
               from_lsn::text AS from_lsn, to_lsn::text AS to_lsn,
               commit_count, change_count, row_count, created_at
          FROM backfill_batch
         WHERE id = %s
        """,
        [batch_id],
    )
    if not heads:
        return None
    rows = log_query(
        settings,
        """
        SELECT row_seq, row_id, action,
               old_sample_no, old_test_item, new_sample_no, new_test_item,
               key_renamed, old_values, new_values,
               first_commit_lsn::text AS first_commit_lsn,
               last_commit_lsn::text  AS last_commit_lsn,
               source_changes
          FROM backfill_batch_row
         WHERE batch_id = %s
         ORDER BY row_seq
        """,
        [batch_id],
    )
    heads[0]["rows"] = rows
    return heads[0]


def get_batch_by_interval(settings, from_lsn: str, to_lsn: str) -> dict | None:
    rows = log_query(
        settings,
        """
        SELECT id FROM backfill_batch
         WHERE slot_name = %s AND from_lsn = %s::pg_lsn AND to_lsn = %s::pg_lsn
        """,
        [settings.slot_name, from_lsn, to_lsn],
    )
    if not rows:
        return None
    return get_batch(settings, rows[0]["id"])


def list_batches(settings) -> list[dict[str, Any]]:
    return log_query(
        settings,
        """
        SELECT id AS batch_id, slot_name,
               from_lsn::text AS from_lsn, to_lsn::text AS to_lsn,
               commit_count, change_count, row_count, created_at
          FROM backfill_batch
         WHERE slot_name = %s
         ORDER BY id
        """,
        [settings.slot_name],
    )
