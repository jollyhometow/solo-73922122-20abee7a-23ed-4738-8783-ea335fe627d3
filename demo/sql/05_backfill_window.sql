-- ============================================================
-- 补传批次演示窗口（5 笔事务，共 10 条行变更）：
--   反复更正 -> 净变化只保留首旧末新；
--   样本号/检测项目改名 -> 批次保留旧、新键；
--   插入后删除、改回原值 -> 不进入批次；
--   普通插入/删除 -> 原样进入批次。
-- ============================================================

-- 事务 1：同一行反复更正（5.6 -> 6.0 -> 6.1，净变化只留 5.6 -> 6.1）
BEGIN;
UPDATE results SET result = '6.0'
 WHERE sample_no = 'S2026-0002' AND test_item = '血糖';
UPDATE results SET result = '6.1'
 WHERE sample_no = 'S2026-0002' AND test_item = '血糖';
COMMIT;

-- 事务 2：样本号改名（批次需同时保留旧键 S2026-0002 与新键 S2026-0002R）
BEGIN;
UPDATE results SET sample_no = 'S2026-0002R'
 WHERE sample_no = 'S2026-0002' AND test_item = '糖化血红蛋白';
COMMIT;

-- 事务 3：检测项目改名（旧键 血钾 / 新键 血钾（急查））
BEGIN;
UPDATE results SET test_item = '血钾（急查）'
 WHERE sample_no = 'S2026-0004' AND test_item = '血钾';
COMMIT;

-- 事务 4：插入后删除（不进批次）+ 改回原值（前后相同，不进批次）
BEGIN;
INSERT INTO results (sample_no, test_item, result)
VALUES ('S2026-0008', '临时项目', '1.0');
DELETE FROM results WHERE sample_no = 'S2026-0008' AND test_item = '临时项目';
UPDATE results SET result = '25'
 WHERE sample_no = 'S2026-0003' AND test_item = '谷丙转氨酶';
UPDATE results SET result = '24'
 WHERE sample_no = 'S2026-0003' AND test_item = '谷丙转氨酶';
COMMIT;

-- 事务 5：普通插入 + 普通删除（原样进入批次）
BEGIN;
INSERT INTO results (sample_no, test_item, result)
VALUES ('S2026-0009', '降钙素原', '0.05');
DELETE FROM results WHERE sample_no = 'S2026-0001' AND test_item = '血常规-红细胞';
COMMIT;
