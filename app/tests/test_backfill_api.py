"""backfill.create_batch 编排逻辑测试：用内存假库替代 PostgreSQL。

覆盖：端点校验、窗口读取、净变化落库、同区间幂等（冲突后回读同一份）、
断裂时整批不发布（库中不留任何批次）。
"""
import sys
import types
import unittest
import json
from contextlib import contextmanager
from unittest import mock

# 本机无 psycopg2：打桩以便导入 app.backfill（本测试只用内存假库，不触真实连接）
if "psycopg2" not in sys.modules:
    _fake = types.ModuleType("psycopg2")
    _fake.connect = lambda *a, **k: None
    _extras = types.ModuleType("psycopg2.extras")
    _extras.RealDictCursor = object
    _fake.extras = _extras
    sys.modules["psycopg2"] = _fake
    sys.modules["psycopg2.extras"] = _extras
# app.db 依赖 app.config（需要 pydantic_settings），本测试用不到，一并打桩
if "app.config" not in sys.modules:
    _config = types.ModuleType("app.config")
    _config.Settings = object
    sys.modules["app.config"] = _config

from app import backfill
from app.backfill_core import BrokenChain
from app.lsn import lsn_to_int


class FakeCursor:
    def __init__(self, db):
        self.db = db
        self._one = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        norm = " ".join(sql.split())
        if norm.startswith("INSERT INTO backfill_batch"):
            slot, from_lsn, to_lsn, commit_count, change_count, row_count = params
            key = (slot, lsn_to_int(from_lsn), lsn_to_int(to_lsn))
            # UNIQUE (slot_name, from_lsn, to_lsn) + ON CONFLICT DO NOTHING
            for bid, b in self.db.batches.items():
                if b["_key"] == key:
                    self._one = None
                    return
            bid = self.db.next_id
            self.db.next_id += 1
            self.db.batches[bid] = {
                "_key": key,
                "batch_id": bid,
                "slot_name": slot,
                "from_lsn": from_lsn,
                "to_lsn": to_lsn,
                "commit_count": commit_count,
                "change_count": change_count,
                "row_count": row_count,
                "created_at": "2026-11-14T00:00:00+00:00",
            }
            self.db.batch_rows[bid] = []
            self._one = (bid,)
        else:
            raise AssertionError(f"unexpected execute: {norm}")

    def executemany(self, sql, rows):
        norm = " ".join(sql.split())
        assert norm.startswith("INSERT INTO backfill_batch_row"), norm
        for r in rows:
            r = dict(r)
            self.db.batch_rows[r["batch_id"]].append(r)

    def fetchone(self):
        return self._one


class FakeConn:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return FakeCursor(self.db)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


class FakeDB:
    """模拟日志库：source_commit / result_change / backfill_batch(_row)。"""

    def __init__(self):
        self.commits = set()     # 已完整入库的 commit_lsn（文本）
        self.changes = []        # result_change 行
        self.batches = {}
        self.batch_rows = {}
        self.next_id = 1

    def query(self, settings, sql, params=None):
        norm = " ".join(sql.split())
        if "FROM source_commit" in norm:
            return [{"?column?": 1}] if params[1] in self.commits else []
        if "FROM result_change" in norm:
            _, from_lsn, to_lsn = params
            lo, hi = lsn_to_int(from_lsn), lsn_to_int(to_lsn)
            out = [c for c in self.changes
                   if lo < lsn_to_int(c["commit_lsn"]) <= hi]
            out.sort(key=lambda c: (lsn_to_int(c["commit_lsn"]), c["change_seq"]))
            return out
        if norm.startswith("SELECT id FROM backfill_batch"):
            _, from_lsn, to_lsn = params
            key = (params[0], lsn_to_int(from_lsn), lsn_to_int(to_lsn))
            return [{"id": b["batch_id"]} for b in self.batches.values()
                    if b["_key"] == key]
        if "FROM backfill_batch_row" in norm:
            rows = []
            for r in self.batch_rows.get(params[0], []):
                r = dict(r)
                # 模拟 psycopg2 对 JSONB 列的自动解码
                for k in ("old_values", "new_values"):
                    if isinstance(r.get(k), str):
                        r[k] = json.loads(r[k])
                rows.append(r)
            rows.sort(key=lambda r: r["row_seq"])
            return rows
        if "FROM backfill_batch" in norm:
            if params and isinstance(params[0], int):  # get_batch by id
                b = self.batches.get(params[0])
                return [dict(b)] if b else []
            return [dict(b) for b in self.batches.values()]
        raise AssertionError(f"unexpected query: {norm}")

    @contextmanager
    def conn(self, settings):
        yield FakeConn(self)


def make_settings():
    return mock.Mock(slot_name="lab_results_slot")


def row(rid, sample, item, result):
    return {"id": rid, "sample_no": sample, "test_item": item, "result": result}


def change(lsn, seq, action, old=None, new=None):
    return {"commit_lsn": lsn, "change_seq": seq, "action": action,
            "old_values": old, "new_values": new}


class TestCreateBatch(unittest.TestCase):
    def setUp(self):
        self.db = FakeDB()
        self.settings = make_settings()
        patches = [
            mock.patch.object(backfill, "log_query", self.db.query),
            mock.patch.object(backfill, "log_conn", self.db.conn),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def test_endpoint_must_be_ingested(self):
        self.db.commits.add("0/100")
        with self.assertRaises(backfill.IntervalError):
            backfill.create_batch(self.settings, "0/0", "0/200")   # 终点未入库
        with self.assertRaises(backfill.IntervalError):
            backfill.create_batch(self.settings, "0/50", "0/100")  # 起点未入库
        # 0/0 起点合法（从日志开头）
        batch, created = backfill.create_batch(self.settings, "0/0", "0/100")
        self.assertTrue(created)
        self.assertEqual(batch["row_count"], 0)

    def test_create_and_idempotent_recreate(self):
        self.db.commits.update({"0/100", "0/200"})
        r0 = row(3, "S1", "血糖", "5.6")
        r1 = row(3, "S1", "血糖", "6.0")
        r2 = row(3, "S1", "血糖", "6.1")
        self.db.changes.extend([
            change("0/100", 1, "U", old=r0, new=r1),
            change("0/200", 1, "U", old=r1, new=r2),
        ])
        batch, created = backfill.create_batch(self.settings, "0/0", "0/200")
        self.assertTrue(created)
        self.assertEqual(batch["row_count"], 1)
        self.assertEqual(batch["change_count"], 2)
        self.assertEqual(batch["commit_count"], 2)
        self.assertEqual(len(batch["rows"]), 1)
        self.assertEqual(batch["rows"][0]["old_values"]["result"], "5.6")
        self.assertEqual(batch["rows"][0]["new_values"]["result"], "6.1")
        self.assertEqual(batch["rows"][0]["row_seq"], 1)

        # 同区间重复创建（客户端重试/并发回读）：同一份，created=False
        again, created = backfill.create_batch(self.settings, "0/0", "0/200")
        self.assertFalse(created)
        self.assertEqual(again["batch_id"], batch["batch_id"])
        self.assertEqual(again["rows"], batch["rows"])
        self.assertEqual(len(self.db.batches), 1)

        # 不同区间是另一份
        other, created = backfill.create_batch(self.settings, "0/100", "0/200")
        self.assertTrue(created)
        self.assertNotEqual(other["batch_id"], batch["batch_id"])
        self.assertEqual(len(self.db.batches), 2)

    def test_broken_chain_publishes_nothing(self):
        self.db.commits.update({"0/100", "0/200"})
        r0 = row(3, "S1", "血糖", "5.6")
        r1 = row(3, "S1", "血糖", "6.0")
        r_bad = row(3, "S1", "血糖", "9.9")
        r2 = row(3, "S1", "血糖", "6.1")
        self.db.changes.extend([
            change("0/100", 1, "U", old=r0, new=r1),
            change("0/200", 1, "U", old=r_bad, new=r2),
        ])
        with self.assertRaises(BrokenChain) as ctx:
            backfill.create_batch(self.settings, "0/0", "0/200")
        self.assertEqual(ctx.exception.broken_commit_lsn, "0/200")
        # 整批不发布：库中没有任何批次
        self.assertEqual(self.db.batches, {})
        self.assertEqual(self.db.batch_rows, {})


if __name__ == "__main__":
    unittest.main()
