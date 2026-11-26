"""结构嗅探与分隔规则引擎：抽样推断列结构→首行表头判定→切分规则选择。

核心行为：
- 对样本行按分隔规则试切分，推断列数与列类型
- 首行是否表头的结论直接改变字段解析的起始行（1 或 2）
- 同一段文本在不同分隔规则下会切出不同列数，规则切换时置信度下降
- 嗅探结论携带所用规则编号与逐列位置，字段解析只能绑定同一次嗅探，
  切换规则时规则编号不一致即拒绝混用
"""
from __future__ import annotations

from typing import Any

from app.pipeline.csv_split import split_line as _split_line


class DelimiterRule:
    """分隔规则条目。"""

    def __init__(self, rule_id: str, fmt: str, delimiter: str,
                 quotechar: str, escapechar: str,
                 version_label: str = ""):
        self.rule_id = rule_id
        self.fmt = fmt
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.escapechar = escapechar
        self.version_label = version_label
        self.hit_batches: list[str] = []

    def split_line(self, line: str) -> list[str]:
        """按规则切分一行（引号/转义行为与历史实现一致）。"""
        return _split_line(line, self.delimiter, self.quotechar, self.escapechar)

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "fmt": self.fmt,
            "delimiter": self.delimiter,
            "quotechar": self.quotechar,
            "escapechar": self.escapechar,
            "hit_count": len(self.hit_batches),
        }


class SniffResult:
    """结构推断结果。"""

    def __init__(self, batch_id: str, sample_rows: int, column_count: int,
                 column_names: list[str], column_types: list[str],
                 first_row_is_header: bool, confidence: float,
                 parse_start_row: int, rule_id: str = "",
                 column_positions: list[int] | None = None):
        self.batch_id = batch_id
        self.sample_rows = sample_rows
        self.column_count = column_count
        self.column_names = column_names
        self.column_types = column_types
        self.first_row_is_header = first_row_is_header
        self.confidence = confidence
        self.parse_start_row = parse_start_row
        # 嗅探所用规则版本：字段解析绑定时必须与此一致，防止跨规则混用
        self.rule_id = rule_id
        # 每列在切分结果中的位置（0 基），与列名同序产出
        self.column_positions = list(column_positions or [])

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "sample_rows": self.sample_rows,
            "column_count": self.column_count,
            "column_names": self.column_names,
            "column_types": self.column_types,
            "first_row_is_header": self.first_row_is_header,
            "confidence": self.confidence,
            "parse_start_row": self.parse_start_row,
            "rule_id": self.rule_id,
            "column_positions": self.column_positions,
        }


class StructureSniffer:
    """结构嗅探器：管理分隔规则并执行列结构推断。"""

    LOW_CONFIDENCE = 0.60

    def __init__(self) -> None:
        self._rules: dict[str, DelimiterRule] = {}
        self._sniffs: dict[str, SniffResult] = {}

    def register_rule(self, rule: DelimiterRule) -> None:
        self._rules[rule.rule_id] = rule

    def get_active_rule(self, fmt: str) -> DelimiterRule | None:
        """获取指定格式的第一个注册规则（按注册顺序）。"""
        for rule in self._rules.values():
            if rule.fmt == fmt:
                return rule
        return None

    def replace_rule(self, fmt: str, new_rule: DelimiterRule) -> DelimiterRule | None:
        """替换指定格式的生效规则，返回被替代的旧规则。

        同格式只保留一条生效规则：命中旧规则的批次清单迁移到新规则上，
        旧规则本身被移出注册表（在分隔规则服务里体现为「被替代」状态）。
        """
        old: DelimiterRule | None = None
        for rule_id in list(self._rules):
            rule = self._rules[rule_id]
            if rule.fmt == fmt:
                if old is None:
                    old = rule
                del self._rules[rule_id]
        if old is not None:
            new_rule.hit_batches = list(old.hit_batches)
        self._rules[new_rule.rule_id] = new_rule
        return old

    def _guess_type(self, values: list[str]) -> str:
        """按样本值推断列类型。"""
        non_empty = [v.strip() for v in values if v.strip()]
        if not non_empty:
            return "str"
        all_int = True
        all_num = True
        all_date = True
        for v in non_empty:
            try:
                int(v)
            except ValueError:
                all_int = False
            try:
                float(v)
            except ValueError:
                all_num = False
            parts = v.replace("T", " ").split(" ")
            if not (len(parts[0].split("-")) == 3 or len(parts) == 2):
                all_date = False
        if all_int:
            return "int"
        if all_num:
            return "float"
        if all_date:
            return "datetime"
        return "str"

    def sniff(self, batch_id: str, fmt: str,
              sample_lines: list[str]) -> tuple[bool, SniffResult]:
        """对样本行执行结构嗅探。返回 (是否可用, 推断结果)。

        推断结果携带所用规则编号，供字段解析侧做同次嗅探一致性校验。
        """
        rule = self.get_active_rule(fmt)
        if rule is None:
            result = SniffResult(batch_id, len(sample_lines), 0, [], [], False, 0.0, 1)
            self._sniffs[batch_id] = result
            return False, result
        return self._sniff_with_rule(batch_id, rule, sample_lines)

    def sniff_with_rule(self, batch_id: str, rule_id: str,
                        sample_lines: list[str]) -> tuple[bool, SniffResult]:
        """按指定规则编号执行嗅探：迁移场景下显式锁定规则版本，避免取到别的生效规则。"""
        rule = self._rules.get(rule_id)
        if rule is None:
            result = SniffResult(batch_id, 0, 0, [], [], False, 0.0, 1)
            self._sniffs[batch_id] = result
            return False, result
        return self._sniff_with_rule(batch_id, rule, sample_lines)

    def _sniff_with_rule(self, batch_id: str, rule: DelimiterRule,
                         sample_lines: list[str]) -> tuple[bool, SniffResult]:
        rule.hit_batches.append(batch_id)
        split_rows = [rule.split_line(line) for line in sample_lines if line.strip()]
        if len(split_rows) < 2:
            # 样本不足两行无法验证列结构一致性，判定低置信不得启动解析
            result = SniffResult(
                batch_id, len(split_rows),
                len(split_rows[0]) if split_rows else 0,
                [], [], False, 0.30, 1, rule_id=rule.rule_id)
            self._sniffs[batch_id] = result
            return False, result

        counts = [len(r) for r in split_rows]
        column_count = counts[0]
        consistent = sum(1 for c in counts if c == column_count)
        consistency = consistent / len(counts)

        first_row_is_header = False
        if consistency >= 0.8 and len(split_rows) >= 2:
            first = split_rows[0]
            rest = split_rows[1:]
            first_is_alpha = all(
                v.strip() and not v.strip().replace(".", "").replace("-", "").isdigit()
                for v in first if v.strip()
            )
            # 数据行里至少有一列是数值/日期型，说明首行是列名而非数据
            data_has_typed_column = any(
                self._guess_type([r[i] for r in rest if i < len(r)]) in ("int", "float", "datetime")
                for i in range(min(column_count, 4))
            ) if column_count else False
            first_row_is_header = bool(
                first_is_alpha and (data_has_typed_column or column_count <= 2)
            )

        data_rows = split_rows[1:] if first_row_is_header else split_rows
        column_names = (
            [c.strip() for c in split_rows[0]] if first_row_is_header
            else [f"col-{i}" for i in range(column_count)]
        )
        column_types = [
            self._guess_type([r[i] for r in data_rows if i < len(r)])
            for i in range(column_count)
        ]

        confidence = round(
            consistency * (0.95 if first_row_is_header or column_count <= 8 else 0.7), 2
        )
        parse_start_row = 2 if first_row_is_header else 1
        column_positions = list(range(column_count))

        result = SniffResult(
            batch_id, len(split_rows), column_count, column_names,
            column_types, first_row_is_header, confidence, parse_start_row,
            rule_id=rule.rule_id, column_positions=column_positions,
        )
        self._sniffs[batch_id] = result
        return confidence >= self.LOW_CONFIDENCE, result

    def guard_same_sniff(self, batch_id: str, rule_id: str) -> tuple[bool, str]:
        """同次嗅探守卫：字段解析所用规则必须与嗅探锁定规则一致，否则拒绝混用。"""
        result = self._sniffs.get(batch_id)
        if result is None:
            return False, f"批次 {batch_id} 尚无结构嗅探结论，解析依据缺失"
        if not result.rule_id:
            # 历史嗅探未记录规则编号时按兼容口径放行，不阻断老批次重放
            return True, "历史嗅探无规则编号，按兼容口径放行"
        if result.rule_id != rule_id:
            return False, (
                f"批次 {batch_id} 嗅探锁定规则 {result.rule_id}，"
                f"与待采用规则 {rule_id} 不一致，切换不得混用"
            )
        return True, f"规则 {rule_id} 与同次嗅探一致"

    def get_sniff(self, batch_id: str) -> SniffResult | None:
        return self._sniffs.get(batch_id)

    def parse_start_row(self, batch_id: str) -> int:
        """字段解析消费的起始行结论。"""
        result = self._sniffs.get(batch_id)
        return result.parse_start_row if result else 1

    def snapshot(self) -> dict[str, Any]:
        return {
            "rules": {k: v.to_dict() for k, v in self._rules.items()},
            "sniffs": {k: v.to_dict() for k, v in self._sniffs.items()},
        }
