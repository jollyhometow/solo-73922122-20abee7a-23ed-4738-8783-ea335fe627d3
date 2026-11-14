"""backfill_core 净变化合成与断裂校验的单元测试（纯 stdlib，可离线运行）。"""
import unittest

from app.backfill_core import BrokenChain, synthesize


def ch(lsn, seq, action, old=None, new=None):
    return {
        "commit_lsn": lsn,
        "change_seq": seq,
        "action": action,
        "old_values": old,
        "new_values": new,
    }


def row(rid, sample, item, result, ts="2026-11-01 08:00:00+00"):
    return {
        "id": rid,
        "sample_no": sample,
        "test_item": item,
        "result": result,
        "updated_at": ts,
    }


class TestSynthesize(unittest.TestCase):
    def test_repeated_updates_collapse_to_first_old_last_new(self):
        r0 = row(3, "S1", "血糖", "5.6")
        r1 = row(3, "S1", "血糖", "6.0")
        r2 = row(3, "S1", "血糖", "6.1")
        out = synthesize([
            ch("0/100", 1, "U", old=r0, new=r1),
            ch("0/100", 2, "U", old=r1, new=r2),
        ])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["action"], "U")
        self.assertEqual(out[0]["old_values"], r0)   # 窗口首条的旧行
        self.assertEqual(out[0]["new_values"], r2)   # 窗口末条的新行
        self.assertEqual(out[0]["source_changes"], 2)
        self.assertFalse(out[0]["key_renamed"])

    def test_insert_then_delete_excluded(self):
        r1 = row(9, "S9", "临时项目", "1.0")
        out = synthesize([
            ch("0/100", 1, "I", new=r1),
            ch("0/101", 1, "D", old=r1),
        ])
        self.assertEqual(out, [])

    def test_update_back_to_original_excluded(self):
        r0 = row(5, "S3", "谷丙转氨酶", "24")
        r1 = row(5, "S3", "谷丙转氨酶", "25")
        out = synthesize([
            ch("0/100", 1, "U", old=r0, new=r1),
            ch("0/100", 2, "U", old=r1, new=r0),   # 改回原值（前后相同）
        ])
        self.assertEqual(out, [])

    def test_plain_insert_and_delete(self):
        ins = row(13, "S13", "降钙素原", "0.05")
        dele = row(2, "S1", "血常规-红细胞", "4.55")
        out = synthesize([
            ch("0/100", 1, "I", new=ins),
            ch("0/100", 2, "D", old=dele),
        ])
        self.assertEqual(len(out), 2)
        by_id = {r["row_id"]: r for r in out}
        self.assertEqual(by_id[13]["action"], "I")
        self.assertIsNone(by_id[13]["old_values"])
        self.assertIsNone(by_id[13]["old_sample_no"])
        self.assertEqual(by_id[13]["new_sample_no"], "S13")
        self.assertEqual(by_id[2]["action"], "D")
        self.assertIsNone(by_id[2]["new_values"])
        self.assertIsNone(by_id[2]["new_sample_no"])
        self.assertEqual(by_id[2]["old_sample_no"], "S1")
        self.assertEqual(by_id[2]["old_test_item"], "血常规-红细胞")

    def test_rename_keeps_old_and_new_keys(self):
        old = row(4, "S2026-0002", "糖化血红蛋白", "5.9%")
        new = row(4, "S2026-0002R", "糖化血红蛋白", "5.9%")
        out = synthesize([ch("0/100", 1, "U", old=old, new=new)])
        self.assertEqual(len(out), 1)
        r = out[0]
        self.assertTrue(r["key_renamed"])
        self.assertEqual(r["old_sample_no"], "S2026-0002")   # 旧键：下游撤销
        self.assertEqual(r["new_sample_no"], "S2026-0002R")  # 新键：下游写入
        self.assertEqual(r["old_test_item"], "糖化血红蛋白")
        self.assertEqual(r["new_test_item"], "糖化血红蛋白")

    def test_test_item_rename_flagged(self):
        old = row(7, "S4", "血钾", "4.3")
        new = row(7, "S4", "血钾（急查）", "4.3")
        out = synthesize([ch("0/100", 1, "U", old=old, new=new)])
        self.assertTrue(out[0]["key_renamed"])
        self.assertEqual(out[0]["old_test_item"], "血钾")
        self.assertEqual(out[0]["new_test_item"], "血钾（急查）")

    def test_rows_sorted_by_row_id_and_input_may_be_unsorted(self):
        r1a, r1b = row(1, "A", "t", "1"), row(1, "A", "t", "2")
        r2a, r2b = row(2, "B", "t", "1"), row(2, "B", "t", "2")
        out = synthesize([
            ch("0/200", 1, "U", old=r2a, new=r2b),
            ch("0/100", 1, "U", old=r1a, new=r1b),
        ])
        self.assertEqual([r["row_id"] for r in out], [1, 2])
        self.assertEqual(out[0]["first_commit_lsn"], "0/100")

    def test_delete_then_insert_same_id_synthesizes_update(self):
        old = row(6, "S3", "肌酐", "78")
        new = row(6, "S3", "肌酐", "80")
        out = synthesize([
            ch("0/100", 1, "D", old=old),
            ch("0/101", 1, "I", new=new),
        ])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["action"], "U")
        self.assertEqual(out[0]["old_values"], old)
        self.assertEqual(out[0]["new_values"], new)

    def test_update_then_delete_synthesizes_delete(self):
        r0 = row(8, "S4", "血钠", "140")
        r1 = row(8, "S4", "血钠", "141")
        out = synthesize([
            ch("0/100", 1, "U", old=r0, new=r1),
            ch("0/101", 1, "D", old=r1),
        ])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["action"], "D")
        self.assertEqual(out[0]["old_values"], r0)  # 窗口首条的旧行
        self.assertIsNone(out[0]["new_values"])

    def test_insert_then_update_synthesizes_insert(self):
        r1 = row(9, "S9", "t", "1")
        r2 = row(9, "S9", "t", "2")
        out = synthesize([
            ch("0/100", 1, "I", new=r1),
            ch("0/101", 1, "U", old=r1, new=r2),
        ])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["action"], "I")
        self.assertIsNone(out[0]["old_values"])
        self.assertEqual(out[0]["new_values"], r2)

    def test_empty_window(self):
        self.assertEqual(synthesize([]), [])


class TestBrokenChain(unittest.TestCase):
    def test_adjacent_mismatch_points_at_second_commit(self):
        r0 = row(3, "S1", "血糖", "5.6")
        r1 = row(3, "S1", "血糖", "6.0")
        r_bad = row(3, "S1", "血糖", "9.9")  # 与上一条新值对不上
        r2 = row(3, "S1", "血糖", "6.1")
        with self.assertRaises(BrokenChain) as ctx:
            synthesize([
                ch("0/100", 1, "U", old=r0, new=r1),
                ch("0/200", 1, "U", old=r_bad, new=r2),
            ])
        exc = ctx.exception
        self.assertEqual(exc.reason, "adjacent_mismatch")
        self.assertEqual(exc.broken_commit_lsn, "0/200")  # 断裂的提交位置
        self.assertEqual(exc.prev_commit_lsn, "0/100")
        self.assertEqual(exc.row_id, 3)
        d = exc.detail()
        self.assertEqual(d["broken_commit_lsn"], "0/200")
        self.assertEqual(d["expected_old_values"], r1)
        self.assertEqual(d["actual_old_values"], r_bad)

    def test_insert_missing_new_values(self):
        with self.assertRaises(BrokenChain) as ctx:
            synthesize([ch("0/100", 1, "I", new=None)])
        self.assertIn(ctx.exception.reason, ("missing_new_values", "missing_row_id"))
        self.assertEqual(ctx.exception.broken_commit_lsn, "0/100")

    def test_update_missing_new_values(self):
        r0 = row(3, "S1", "血糖", "5.6")
        with self.assertRaises(BrokenChain) as ctx:
            synthesize([ch("0/100", 1, "U", old=r0, new=None)])
        self.assertEqual(ctx.exception.reason, "missing_new_values")

    def test_update_missing_old_values(self):
        r1 = row(3, "S1", "血糖", "6.0")
        with self.assertRaises(BrokenChain) as ctx:
            synthesize([ch("0/100", 1, "U", old=None, new=r1)])
        self.assertEqual(ctx.exception.reason, "missing_old_values")

    def test_delete_missing_old_values(self):
        with self.assertRaises(BrokenChain) as ctx:
            synthesize([ch("0/100", 1, "D", old=None)])
        self.assertIn(ctx.exception.reason, ("missing_old_values", "missing_row_id"))

    def test_incomplete_old_values(self):
        bad_old = {"id": 3, "sample_no": "S1"}  # 缺 test_item，不算完整旧值
        r1 = row(3, "S1", "血糖", "6.0")
        with self.assertRaises(BrokenChain) as ctx:
            synthesize([ch("0/100", 1, "U", old=bad_old, new=r1)])
        self.assertEqual(ctx.exception.reason, "incomplete_old_values")
        self.assertEqual(ctx.exception.broken_commit_lsn, "0/100")

    def test_update_after_delete_is_broken(self):
        r0 = row(3, "S1", "血糖", "5.6")
        r1 = row(3, "S1", "血糖", "6.0")
        with self.assertRaises(BrokenChain) as ctx:
            synthesize([
                ch("0/100", 1, "D", old=r0),
                ch("0/200", 1, "U", old=r0, new=r1),  # 删除后不应出现带旧值的更正
            ])
        self.assertEqual(ctx.exception.reason, "adjacent_mismatch")
        self.assertEqual(ctx.exception.broken_commit_lsn, "0/200")


if __name__ == "__main__":
    unittest.main()
