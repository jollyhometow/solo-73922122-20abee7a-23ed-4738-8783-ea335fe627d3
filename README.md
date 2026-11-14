# 检验结果变更日志服务（wal2json 逻辑复制 + FastAPI）

实验室数据工程师排查“检验结果漏同步”时，需要回答：**源库一次提交到底改过哪些样本？**
本环境用 PostgreSQL 16 逻辑复制（`wal2json`）持续采集源库 `results` 表的增/改/删，
把**源事务号、提交位置、事务内有序的前后值**原子写入独立日志库，并通过 FastAPI 提供
按提交位置翻页的变更查询与采集状态接口。

补传漏同步数据时，还可以把**一段时间内反复更正的记录整理成稳定的入仓批次**：
指定源提交位置的**左开右闭区间** `(from_lsn, to_lsn]` 创建补传批次，按 `results.id`
和源变更顺序取**窗口内首条的旧行 + 末条的新行**合成净变化，与区间一起持久化；
同区间只产生一份，创建后无论采集继续、客户端重试还是服务重启，读取内容都不变。

## 1. 架构

```
源库 source-db (PG16 + wal2json)                独立日志库 log-db (PG16)
┌─────────────────────────────┐   复制协议流     ┌──────────────────────────┐
│ results 表 (REPLICA ID FULL)│ ───────────────▶ │ source_commit            │
│ 持久逻辑复制槽 lab_results_  │  wal2json B/I/U/ │ result_change (有序前后值)│
│ slot（建槽后变更才入流）     │  D/C 消息        │ slot_state / heartbeat   │
└──────────────┬──────────────┘                  │ backfill_batch(_row)     │
               │ 1.日志库 COMMIT 落盘            │   （补传批次，不可变）    │
               │ 2.再 send_feedback 确认源位置    └──────────▲───────────────┘
               └─────────────────────────────────── ┌───────┴────────┐
                                                    │ collector 容器  │
                                                    └────────────────┘
             ┌──────────────────────────────────────────────────┐
             │ api 容器（FastAPI，只读日志库 + 源槽状态）         │
             │ /docs  /commits  /commits/{lsn}  /changes  /status│
             │ /backfill-batches（创建/读取补传批次）            │
             └──────────────────────────────────────────────────┘
```

## 2. 需求与实现对照

| 要求 | 实现方式 |
| --- | --- |
| 只采集建槽后固定 `results` 表的增改删 | 采集器启动时创建持久逻辑复制槽；wal2json 选项 `add-tables=public.results`、`actions=insert,update,delete` |
| 表含样本号、检测项目、结果 | 源表 `results(sample_no, test_item, result, ...)` |
| 更正、删除保留完整旧值 | `ALTER TABLE results REPLICA IDENTITY FULL` + wal2json `include-old-keys`，旧值整行 JSONB 入库 |
| 保存源事务号、提交位置、有序前后值 | `source_commit(xid, commit_lsn, begin_lsn, commit_time)` + `result_change(change_seq, old_values, new_values)`，同事务同行多次修改各占一行、按 `change_seq` 保序 |
| 回滚不记录 | 逻辑解码只输出已提交事务，ROLLBACK 不产生任何消息 |
| 整笔事务一次可见、顺序与源提交一致 | 每笔源事务在日志库一个本地事务中原子写入；按 `commit_lsn` 全局排序 |
| 先持久提交再确认源位置 | 日志库 `COMMIT` 成功后才 `send_feedback(flush_lsn=end_lsn)`，源槽 `confirmed_flush_lsn/restart_lsn` 随后才推进 |
| 落库后确认前退出/断流/重启，不漏不重 | 重启从持久槽旧位置重新解码；`source_commit(slot_name, commit_lsn)` 唯一约束 + `ON CONFLICT DO NOTHING` 幂等重放 |
| 不能靠扫描当前表补历史 | 唯一数据通道是复制槽 WAL 流；不读取 `results` 当前数据做任何补采 |
| 采集停顿时源库仍可提交 | 复制槽保留所需 WAL（`/status` 的 `retained_wal_bytes` 可见），恢复后自动补采 |
| 查询仅见完整事务 | 写入原子性保证；API 只读已提交数据 |
| 按提交位置翻页 + 采集状态 API | `GET /commits?cursor=<LSN>&limit=`、`GET /commits/{commit_lsn}`、`GET /changes`、`GET /status` |
| 补传批次：左开右闭区间，终点须已完整入库 | `POST /backfill-batches` 校验 `to_lsn` 存在于 `source_commit`（采集器按事务原子写入，存在即完整） |
| 首旧末新合成净变化，插入前/删除后视为空 | 窗口内变更按 `(commit_lsn, change_seq)` 排序、按 `results.id` 分组，取首条 `old_values` 与末条 `new_values` |
| 前后相同、插入后删除的行不进入批次 | 净旧值 == 净新值、或两侧皆空（插入后删除）时跳过该行 |
| 样本号/检测项目改名保留旧、新键 | 批次行同时携带 `old_sample_no/old_test_item` 与 `new_sample_no/new_test_item` 并置 `key_renamed`，下游据此撤销旧记录、写入新记录 |
| 变更链断裂须指出位置、整批不发布 | 相邻变更前后值对不上、I/U 缺新值、U/D 缺完整旧值 → `409` 且 `detail.broken_commit_lsn` 指出断裂的提交位置，不写入任何批次数据 |
| 区间与结果一起持久化，同区间只一份 | `backfill_batch (slot_name, from_lsn, to_lsn)` 唯一约束 + `ON CONFLICT DO NOTHING`，并发创建/客户端重试回读同一份 |
| 创建后读取内容不变 | 批次行创建时一次性写入、之后永不修改；采集继续、服务重启后 `GET /backfill-batches/{id}` 结果相同 |
| 可实际启动的 Compose + /docs + 可控崩溃演示 | `docker compose up -d --build`；`./demo/demo.sh` 一键演示全部场景 |

### 确认时序（正确性核心）

```
wal2json 消息 B,I,U,...,C
        │
        ▼
日志库 BEGIN → insert source_commit/result_change/slot_state → COMMIT（落盘）
        │
        ├─ 若开启崩溃演示：此刻 os._exit（源位置尚未确认）
        ▼
复制连接 send_feedback(flush_lsn = 本事务 end_lsn)
        ▼
源槽 confirmed_flush_lsn 推进，旧 WAL 才允许回收
```

在“日志库已提交、feedback 未生效”之间任何退出：源槽仍停在旧位置 → 重启重放该事务 →
唯一约束幂等吞掉 → 再次确认。因此**不漏记、不重复**，且不依赖扫描当前表。

### 补传批次语义（入仓净变化）

```
POST /backfill-batches  {"from_lsn": "0/1A2B3C8", "to_lsn": "0/1A2C900"}
```

- **区间**：源提交位置的左开右闭区间 `(from_lsn, to_lsn]`；`from_lsn` 缺省 `0/0`
  表示从日志开头开始。`to_lsn`（以及非 `0/0` 的 `from_lsn`）必须是已完整入库的事务，
  否则 `400`。
- **净变化**：窗口内变更按源变更顺序排序、按 `results.id` 分组；每行取**首条的旧行**
  与**末条的新行**；插入前、删除后视为空（`null`）。前后相同、插入后删除的行不进入批次。
- **改名**：`sample_no`/`test_item` 变化时 `key_renamed=true`，批次行同时保留旧键
  （`old_sample_no/old_test_item`）与新键（`new_sample_no/new_test_item`），
  下游据此撤销旧记录并写入新记录。
- **断裂校验**：同一行相邻变更必须衔接（前一条新值 == 后一条旧值），且 I/U 必有完整
  新值、U/D 必有完整旧值；违反时返回 `409`，`detail.broken_commit_lsn` 指出断裂的
  提交位置，**整批不发布**（不写入任何批次数据）。
- **稳定不变**：区间与结果一起持久化（`backfill_batch` + `backfill_batch_row`）；
  同区间并发创建/客户端重试只产生一份（唯一约束保证，重复创建返回 `200` 与同一份内容）；
  创建后即使采集继续、服务重启，`GET /backfill-batches/{id}` 读取内容仍相同。

```bash
# 创建（幂等）：首次 201，同区间重复创建 200 且内容相同
curl -X POST http://localhost:8000/backfill-batches \
  -H 'Content-Type: application/json' \
  -d '{"from_lsn":"0/1A2B3C8","to_lsn":"0/1A2C900"}'
# 读取
curl http://localhost:8000/backfill-batches/1
```

## 3. 快速启动

```bash
docker compose up -d --build
# 文档与接口
open http://localhost:8000/docs      # Swagger UI
```

端口（见 `.env`）：源库 `5432`、日志库 `5433`、API `8000`。

## 4. 一键演示

```bash
./demo/demo.sh
```

脚本自动完成并断言：

1. **正常事务**：同事务对同一行两次更正（两条都保留且有序）、更正、插入、删除（完整旧值）；
2. **回滚不记录**：ROLLBACK 的内容在日志库中为 0 条，源表数据不变；
3. **停顿续采**：停掉 collector → 源库继续提交（日志库不可见、复制槽 WAL 保留量增长）
   → 重启 collector 自动补采；
4. **崩溃恢复**：在“落库后确认前”窗口杀掉采集器 → 日志已可见、源槽未推进 →
   正常重启后幂等续采，事务数与行数均不重复，确认位置最终追平；
5. **翻页与样本检索**：`next_cursor` 翻页、按样本号查看完整前后值；
6. **补传批次**：左开右闭区间创建批次——反复更正合成首旧末新、改名保留旧新键、
   插入后删除与前后相同剔除、篡改日志模拟断裂返回 409 并指出断裂位置、
   并发创建只一份、重试/续采/重启后读取内容不变。

### 手动触发崩溃点（可选）

```bash
docker compose stop collector
docker compose run --rm --no-deps --entrypoint sh collector \
  -c 'touch /var/lib/lab-cdc/crash-once'
CRASH_AFTER_PERSIST=1 docker compose up -d --no-deps collector
# 在源库提交一笔事务后，collector 会在确认前退出一次（标记自动归档，不会反复崩）
docker compose up -d --no-deps collector   # 正常重启续采
```

## 5. API 摘要

| 方法 & 路径 | 说明 |
| --- | --- |
| `GET /docs` | Swagger 交互文档 |
| `GET /commits?cursor=0/..&limit=20&direction=forward` | 按提交位置翻页，每页含整笔事务及有序前后值 |
| `GET /commits/{commit_lsn}` | 取单笔事务（源 xid、提交位置、全部行变更） |
| `GET /changes?sample_no=S2026-0002&action=U` | 按样本号/动作检索行级变更，删除也能按旧样本号追溯 |
| `GET /status` | 采集器存活心跳、已持久/已确认 LSN、源槽 active/restart_lsn、WAL 保留字节数 |
| `POST /backfill-batches` | 创建补传批次：左开右闭区间 `(from_lsn, to_lsn]` 合成净变化并持久化；同区间幂等（201 新建 / 200 已存在 / 409 链断裂） |
| `GET /backfill-batches` | 补传批次列表（区间、事务/变更/净变化行数、创建时间） |
| `GET /backfill-batches/{batch_id}` | 读取补传批次全部净变化行；内容自创建起不变 |

## 6. 运维说明

- 采集器**不设自动重启策略**，以便崩溃演示中进程保持退出；生产环境可加 `restart: unless-stopped`，
  重连与幂等逻辑已内置（流中断 3 秒后重连，从槽位置继续）。
- 长期停采会让源库 WAL 持续增长，`/status` 的 `retained_wal_bytes` 用于监控该积压。
- 复制槽为持久对象，容器重建/镜像升级不会丢位置；清理环境用 `docker compose down -v`
  （会删除两个数据卷与崩溃标记卷，再次启动即为全新基线）。
- 初始基线数据在建槽前写入，按设计不进入变更日志；日志只承载建槽之后的变更流。
- 补传批次表在全新环境由 `log-db/initdb/02_backfill.sql` 建好；已有数据卷升级时，
  API 启动会幂等执行同样的 `CREATE TABLE IF NOT EXISTS` 兜底，无需手工迁移。
- 净变化合成与断裂校验是纯函数（`app/app/backfill_core.py`），可离线单测：
  `cd app && python3 -m unittest discover tests -v`。
