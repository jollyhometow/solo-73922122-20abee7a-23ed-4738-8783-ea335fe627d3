"""
补传批次净变化合成与变更链校验（纯函数，无 I/O，可独立单测）。

输入：窗口 (from_lsn, to_lsn] 内的行变更（dict：commit_lsn / change_seq /
action('I'|'U'|'D') / old_values / new_values），顺序不限，函数内部按
源变更顺序 (commit_lsn, change_seq) 重新排序。

合成规则：
  * 按 results.id 分组，组内保持源变更顺序；
  * 每行取窗口内首条变更的旧值、末条变更的新值合成净变化，
    插入前、删除后视为空（None）；
  * 前后相同、插入后删除的行不进入批次；
  * 样本号/检测项目改名时同时保留旧键与新键，供下游撤销旧记录并写入新记录。

校验规则（违反即抛 BrokenChain，指出断裂的提交位置，整批不发布）：
  * I/U 必须有完整新值，U/D 必须有完整旧值；
  * 同一行相邻变更必须衔接：前一条的新值 == 后一条的旧值
    （删除后的下一条只能是插入，即旧值同样为空）。
"""
from __future__ import annotations

from typing import Any, Iterable

from .lsn import lsn_to_int

# 判定“完整新值/完整旧值”所必需的业务键（results 表主键与业务键）
REQUIRED_KEYS = ("id", "sample_no", "test_item")


class BrokenChain(Exception):
    """窗口内变更链断裂：携带断裂的提交位置与原因，整批不发布。"""

    def __init__(
        self,
        *,
        row_id: int | None,
        broken_commit_lsn: str,
        reason: str,
        message: str,
        prev_commit_lsn: str | None = None,
        expected_old_values: dict | None = None,
        actual_old_values: dict | None = None,
    ):
        super().__init__(message)
        self.row_id = row_id
        self.broken_commit_lsn = broken_commit_lsn
        self.prev_commit_lsn = prev_commit_lsn
        self.reason = reason
        self.message = message
        self.expected_old_values = expected_old_values
        self.actual_old_values = actual_old_values

    def detail(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "error": "broken_change_chain",
            "message": self.message,
            "reason": self.reason,
            "row_id": self.row_id,
            "broken_commit_lsn": self.broken_commit_lsn,
        }
        if self.prev_commit_lsn is not None:
            d["prev_commit_lsn"] = self.prev_commit_lsn
        if self.expected_old_values is not None:
            d["expected_old_values"] = self.expected_old_values
        if self.actual_old_values is not None:
            d["actual_old_values"] = self.actual_old_values
        return d


def _require_complete(values: dict | None, side: str, ch: dict, row_id: int) -> None:
    """I/U 缺新值、U/D 缺完整旧值 -> BrokenChain。"""
    action = ch["action"]
    lsn = ch["commit_lsn"]
    side_cn = "新值" if side == "new" else "完整旧值"
    if values is None:
        raise BrokenChain(
            row_id=row_id,
            broken_commit_lsn=lsn,
            reason=f"missing_{side}_values",
            message=f"results.id={row_id} 在位置 {lsn} 的 {action} 变更缺少{side_cn}",
        )
    missing = [k for k in REQUIRED_KEYS if values.get(k) is None]
    if missing:
        raise BrokenChain(
            row_id=row_id,
            broken_commit_lsn=lsn,
            reason=f"incomplete_{side}_values",
            message=(
                f"results.id={row_id} 在位置 {lsn} 的 {action} 变更{side_cn}"
                f"不完整，缺少键: {', '.join(missing)}"
            ),
        )


def _extract_row_id(ch: dict) -> int:
    for side in ("new_values", "old_values"):
        v = ch.get(side)
        if isinstance(v, dict) and v.get("id") is not None:
            return int(v["id"])
    raise BrokenChain(
        row_id=None,
        broken_commit_lsn=ch["commit_lsn"],
        reason="missing_row_id",
        message=(
            f"位置 {ch['commit_lsn']} 的 {ch['action']} 变更缺少 results.id，"
            "无法按行归组（I/U 缺新值或 U/D 缺完整旧值）"
        ),
    )


def _validate_chain(row_id: int, chain: list[dict]) -> None:
    """校验同一行在窗口内的有序变更：值完整 + 相邻前后值衔接。"""
    prev: dict | None = None
    for ch in chain:
        action = ch["action"]
        old, new = ch.get("old_values"), ch.get("new_values")
        if action in ("I", "U"):
            _require_complete(new, "new", ch, row_id)
        if action in ("U", "D"):
            _require_complete(old, "old", ch, row_id)
        if prev is not None:
            prev_new = prev.get("new_values") if prev["action"] in ("I", "U") else None
            if prev_new != old:
                raise BrokenChain(
                    row_id=row_id,
                    broken_commit_lsn=ch["commit_lsn"],
                    prev_commit_lsn=prev["commit_lsn"],
                    reason="adjacent_mismatch",
                    message=(
                        f"results.id={row_id} 的相邻变更前后值对不上：位置 "
                        f"{prev['commit_lsn']} 变更后的新值与位置 {ch['commit_lsn']} "
                        "变更前的旧值不一致，窗口内变更链断裂"
                    ),
                    expected_old_values=prev_new,
                    actual_old_values=old,
                )
        prev = ch


def synthesize(changes: Iterable[dict]) -> list[dict]:
    """把窗口内行变更合成为净变化行，按 results.id 升序返回。

    每行内容：row_id / action / 旧新样本号与检测项目 / key_renamed /
    old_values / new_values / first_commit_lsn / last_commit_lsn / source_changes。
    """
    ordered = sorted(changes, key=lambda c: (lsn_to_int(c["commit_lsn"]), c["change_seq"]))

    groups: dict[int, list[dict]] = {}
    for ch in ordered:
        groups.setdefault(_extract_row_id(ch), []).append(ch)

    net_rows: list[dict] = []
    for row_id in sorted(groups):
        chain = groups[row_id]
        _validate_chain(row_id, chain)
        first, last = chain[0], chain[-1]
        # 窗口内首条的旧行 + 末条的新行；插入前、删除后视为空
        net_old = first.get("old_values") if first["action"] in ("U", "D") else None
        net_new = last.get("new_values") if last["action"] in ("I", "U") else None

        if net_old is None and net_new is None:
            continue  # 插入后删除：不进入批次
        if net_old == net_new:
            continue  # 前后相同：不进入批次

        if net_old is None:
            action = "I"
        elif net_new is None:
            action = "D"
        else:
            action = "U"

        old_no = net_old.get("sample_no") if net_old else None
        old_item = net_old.get("test_item") if net_old else None
        new_no = net_new.get("sample_no") if net_new else None
        new_item = net_new.get("test_item") if net_new else None

        net_rows.append({
            "row_id": row_id,
            "action": action,
            # 样本号或检测项目改名时保留旧、新键，供下游撤销旧记录并写入新记录
            "old_sample_no": old_no,
            "old_test_item": old_item,
            "new_sample_no": new_no,
            "new_test_item": new_item,
            "key_renamed": action == "U" and (old_no != new_no or old_item != new_item),
            "old_values": net_old,
            "new_values": net_new,
            "first_commit_lsn": first["commit_lsn"],
            "last_commit_lsn": last["commit_lsn"],
            "source_changes": len(chain),
        })
    return net_rows
