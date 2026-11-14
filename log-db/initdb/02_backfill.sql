-- ============================================================
-- 补传批次：把左开右闭区间 (from_lsn, to_lsn] 内反复更正的
-- results 变更合成为稳定的入仓净变化，并与区间一起持久化。
--   1. (slot_name, from_lsn, to_lsn) 唯一：同区间并发创建、
--      客户端重试只产生一份；
--   2. 批次行在创建时与批次头同事务一次性写入，之后永不修改：
--      采集继续、服务重启后读取内容不变；
--   3. 样本号/检测项目改名时保留旧、新键（old_* 与 new_*），
--      供下游撤销旧记录并写入新记录。
-- 注意：与 app/app/backfill.py 的 SCHEMA_STATEMENTS 保持一致。
-- ============================================================

CREATE TABLE IF NOT EXISTS backfill_batch (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    slot_name    TEXT   NOT NULL,
    from_lsn     PG_LSN NOT NULL,               -- 左开起点（不含该位置的提交）
    to_lsn       PG_LSN NOT NULL,               -- 右闭终点（含，已完整入库的事务）
    commit_count INTEGER NOT NULL,              -- 窗口内源事务数
    change_count INTEGER NOT NULL,              -- 窗口内行变更条数
    row_count    INTEGER NOT NULL,              -- 净变化行数
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (slot_name, from_lsn, to_lsn)
);

CREATE TABLE IF NOT EXISTS backfill_batch_row (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id         BIGINT  NOT NULL REFERENCES backfill_batch(id) ON DELETE CASCADE,
    row_seq          INTEGER NOT NULL,          -- 批次内顺序（按 results.id 升序）
    row_id           BIGINT  NOT NULL,          -- results.id
    action           CHAR(1) NOT NULL CHECK (action IN ('I','U','D')),
    old_sample_no    TEXT,                      -- 旧样本号（插入前为空）
    old_test_item    TEXT,                      -- 旧检测项目（插入前为空）
    new_sample_no    TEXT,                      -- 新样本号（删除后为空）
    new_test_item    TEXT,                      -- 新检测项目（删除后为空）
    key_renamed      BOOLEAN NOT NULL DEFAULT FALSE,  -- 样本号/检测项目是否改名
    old_values       JSONB,                     -- 窗口首条变更的旧值（插入前为空）
    new_values       JSONB,                     -- 窗口末条变更的新值（删除后为空）
    first_commit_lsn PG_LSN NOT NULL,           -- 该行窗口内首条变更的提交位置
    last_commit_lsn  PG_LSN NOT NULL,           -- 该行窗口内末条变更的提交位置
    source_changes   INTEGER NOT NULL,          -- 该行窗口内原始变更条数
    UNIQUE (batch_id, row_id)
);

CREATE INDEX IF NOT EXISTS idx_backfill_batch_row_batch
    ON backfill_batch_row (batch_id, row_seq);
