"""分隔规则领域服务：维护各格式的切分转义约定与规则版本，产出规则条目数据集。

在通用条目流转之外，承担规则迁移所需的版本化能力：
- 按规则条目的实际引号/转义约定对样本做分列预览，旧逗号规则保持历史兼容行为
- 按格式解析当前生效规则、按编号查规则，供迁移单与批次规则版本记录取数。
"""
from __future__ import annotations

from typing import Any

from app.pipeline.csv_split import rule_split, split_preview, unescape_symbol
from app.services.base import BaseModuleService
from app.store import store


class DelimiterRuleService(BaseModuleService):
    MODULE = "delimiter_rule"
    ENTITY = "规则条目"
    OPERATION_TERM = "切分"
    LIST_FIELDS = ["规则编号", "适用格式", "规则版本号", "分隔符", "引号字符", "转义字符", "换行识别", "命中批次数", "最近调整"]
    REQUIRED_FIELDS = ["规则编号", "适用格式", "分隔符"]
    STATUS_ORDER = ["草稿", "已生效", "已停用", "被替代"]
    ACTION_RULES = {"启效规则": "已生效", "停用规则": "已停用", "标记被替代": "被替代"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def find_rule(self, rule_no: str) -> dict[str, Any] | None:
        """按规则编号取规则条目。"""
        for row in store.rows(self.MODULE):
            if row.get("规则编号") == rule_no:
                return row
        return None

    def active_rule_of(self, fmt: str) -> dict[str, Any] | None:
        """取某格式当前生效的规则条目（同格式唯一已生效）。"""
        for row in store.rows(self.MODULE):
            if row.get("适用格式") == fmt and row.get("status") == "已生效":
                return row
        return None

    def preview(self, rule_no: str, sample_lines: list[str]) -> tuple[bool, str, dict[str, Any] | None]:
        """按规则条目对样本做分列预览，返回逐行分列与列数，不改动任何已确认数据。"""
        rule = self.find_rule(rule_no)
        if rule is None:
            return False, f"规则 {rule_no} 不存在", None
        if not sample_lines:
            return False, "样本行为空，无法预览分列", None
        rows = split_preview(
            sample_lines,
            unescape_symbol(str(rule.get("分隔符") or ",")),
            unescape_symbol(str(rule.get("引号字符") or '"')),
            unescape_symbol(str(rule.get("转义字符") or "\\")),
        )
        column_count = len(rows[0]) if rows else 0
        return True, f"规则 {rule_no} 预览 {len(rows)} 行、{column_count} 列", {
            "规则编号": rule_no,
            "规则版本号": rule.get("规则版本号", ""),
            "适用格式": rule.get("适用格式"),
            "分隔符": rule.get("分隔符"),
            "引号字符": rule.get("引号字符"),
            "转义字符": rule.get("转义字符"),
            "preview_rows": rows,
            "column_count": column_count,
            "consistent": all(len(r) == column_count for r in rows),
        }

    def split_with_rule(self, rule_no: str, line: str) -> list[str]:
        """按指定规则条目切分一行，供字段解析按有效采用版本取数。"""
        rule = self.find_rule(rule_no)
        if rule is None:
            return []
        return rule_split(line, rule)

    def set_status(self, rule_no: str, target_status: str) -> dict[str, Any] | None:
        """迁移流转专用：直接置规则状态并同步 pending 标记。"""
        rule = self.find_rule(rule_no)
        if rule is None:
            return None
        rule["status"] = target_status
        rule["pending"] = target_status not in ("已生效",)
        return rule
