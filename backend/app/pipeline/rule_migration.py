"""分隔规则迁移引擎：规则版本化→同次嗅探绑定→可预览/启用/回滚的迁移单编排。

核心行为：
- 规则条目带版本号与状态，启用新规则不删除旧规则，回滚时旧规则重新生效
- 迁移单永久保留新旧两份分列预览，回滚后仍可对照
- 每个批次的每次采用记一条「批次规则版本记录」（采用轮次），字段解析、首行表头、
  列位置必须来自同一次结构嗅探，切换规则不得混用
- 已确认（字段已交付）的列不可替换或清空；恢复逗号解析时按迁移单辨别旧动作结果，
  只在旧列已确认时续接，绝不覆盖已认可列
- 字段解析按批次规则版本记录给出的有效采用版本取规则，并保留对已确认旧轮的回查
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.pipeline.csv_split import split_line


@dataclass
class RuleVersion:
    """带版本与状态的分隔规则。"""

    rule_id: str
    fmt: str
    version_label: str
    delimiter: str
    quotechar: str
    escapechar: str
    status: str = "草稿"
    hit_batches: list[str] = field(default_factory=list)

    def split_line(self, line: str) -> list[str]:
        return split_line(line, self.delimiter, self.quotechar, self.escapechar)

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "fmt": self.fmt,
            "version_label": self.version_label,
            "delimiter": self.delimiter,
            "quotechar": self.quotechar,
            "escapechar": self.escapechar,
            "status": self.status,
            "hit_batches": list(self.hit_batches),
        }


@dataclass
class AdoptionRound:
    """批次规则版本记录：一个批次在某条规则上的一次采用轮次。"""

    record_id: str
    batch_id: str
    rule_id: str
    version_label: str
    round_no: int
    migration_id: str = ""
    state: str = "已领取样本"
    sniff_id: str = ""
    column_count: int = 0
    columns: list[str] = field(default_factory=list)
    column_positions: list[int] = field(default_factory=list)
    first_row_is_header: bool = False
    parse_start_row: int = 1
    confirmed: bool = False
    operator: str = ""
    adopted_at: str = ""
    delivered_at: str = ""
    history: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "record_id": self.record_id,
            "batch_id": self.batch_id,
            "rule_id": self.rule_id,
            "version_label": self.version_label,
            "round_no": self.round_no,
            "migration_id": self.migration_id,
            "state": self.state,
            "sniff_id": self.sniff_id,
            "column_count": self.column_count,
            "columns": list(self.columns),
            "column_positions": list(self.column_positions),
            "first_row_is_header": self.first_row_is_header,
            "parse_start_row": self.parse_start_row,
            "confirmed": self.confirmed,
            "operator": self.operator,
            "adopted_at": self.adopted_at,
            "delivered_at": self.delivered_at,
            "history": list(self.history),
        }


@dataclass
class MigrationOrder:
    """规则迁移单：绑定同一格式的新旧两条规则，承载预览与流转状态。"""

    migration_id: str
    fmt: str
    old_rule_id: str
    new_rule_id: str
    operator: str
    status: str = "待启用"
    old_preview: list[list[str]] = field(default_factory=list)
    new_preview: list[list[str]] = field(default_factory=list)
    sample_lines: list[str] = field(default_factory=list)
    actions: list[dict[str, str]] = field(default_factory=list)
    created_at: str = ""
    activated_at: str = ""
    rolled_back_at: str = ""

    def log(self, action: str, outcome: str, at: str) -> None:
        self.actions.append({"action": action, "outcome": outcome, "time": at})

    def to_dict(self) -> dict[str, Any]:
        return {
            "migration_id": self.migration_id,
            "fmt": self.fmt,
            "old_rule_id": self.old_rule_id,
            "new_rule_id": self.new_rule_id,
            "operator": self.operator,
            "status": self.status,
            "old_preview": [list(r) for r in self.old_preview],
            "new_preview": [list(r) for r in self.new_preview],
            "actions": list(self.actions),
            "created_at": self.created_at,
            "activated_at": self.activated_at,
            "rolled_back_at": self.rolled_back_at,
        }


class RuleMigrationManager:
    """规则迁移管理器：规则版本、迁移单与批次采用轮次的统一编排。"""

    # 采用轮次状态
    CLAIMED = "已领取样本"
    IN_FLIGHT = "字段在途"
    DELIVERED = "已交付"
    ROLLED_BACK = "已回滚"
    STATE_CLAIMED = CLAIMED
    STATE_IN_FLIGHT = IN_FLIGHT
    STATE_DELIVERED = DELIVERED
    STATE_ROLLED_BACK = ROLLED_BACK

    def __init__(self) -> None:
        self._rules: dict[str, RuleVersion] = {}
        self._migrations: dict[str, MigrationOrder] = {}
        # batch_id -> 按轮次顺序排列的采用记录
        self._adoptions: dict[str, list[AdoptionRound]] = {}
        self._seq = 0

    # ------------------------------------------------------------------
    # 规则注册与查询
    # ------------------------------------------------------------------
    def register_rule(self, rule: RuleVersion) -> None:
        self._rules[rule.rule_id] = rule

    def get_rule(self, rule_id: str) -> RuleVersion | None:
        return self._rules.get(rule_id)

    def active_rule(self, fmt: str) -> RuleVersion | None:
        """当前生效规则：同格式取唯一「已生效」条目。"""
        for rule in self._rules.values():
            if rule.fmt == fmt and rule.status == "已生效":
                return rule
        return None

    def _next_record_id(self, batch_id: str) -> str:
        self._seq += 1
        return f"BRV-{batch_id}-{self._seq:03d}"

    # ------------------------------------------------------------------
    # 预览：同一份样本分别按两条规则切分，结果随迁移单永久保留
    # ------------------------------------------------------------------
    def preview_rule(self, rule_id: str, sample_lines: list[str]) -> dict[str, Any]:
        rule = self._rules.get(rule_id)
        if rule is None:
            return {"ok": False, "message": f"规则 {rule_id} 不存在", "rows": []}
        rows = [rule.split_line(line) for line in sample_lines if line.strip()]
        column_count = len(rows[0]) if rows else 0
        return {
            "ok": True,
            "rule_id": rule_id,
            "version_label": rule.version_label,
            "delimiter": rule.delimiter,
            "rows": rows,
            "column_count": column_count,
            "consistent": all(len(r) == column_count for r in rows),
        }

    def create_migration(self, migration_id: str, fmt: str, old_rule_id: str,
                         new_rule_id: str, sample_lines: list[str],
                         operator: str, created_at: str) -> tuple[bool, str, MigrationOrder | None]:
        """建迁移单：校验同格式两条规则，并固化新旧两份分列预览。"""
        if migration_id in self._migrations:
            return False, f"迁移单 {migration_id} 已存在", None
        old = self._rules.get(old_rule_id)
        new = self._rules.get(new_rule_id)
        if old is None or new is None:
            return False, "迁移所依据的规则条目不存在，无法建单", None
        if old.fmt != fmt or new.fmt != fmt:
            return False, f"两条规则的适用格式必须都是 {fmt}", None
        if old_rule_id == new_rule_id:
            return False, "新旧规则不能是同一条目", None
        if not sample_lines:
            return False, "样本行为空，无法生成迁移预览", None

        order = MigrationOrder(
            migration_id=migration_id, fmt=fmt,
            old_rule_id=old_rule_id, new_rule_id=new_rule_id,
            operator=operator, sample_lines=list(sample_lines),
            created_at=created_at,
        )
        order.old_preview = self.preview_rule(old_rule_id, sample_lines)["rows"]
        order.new_preview = self.preview_rule(new_rule_id, sample_lines)["rows"]
        order.log("建单", "固化新旧分列预览", created_at)
        self._migrations[migration_id] = order
        return True, f"迁移单 {migration_id} 已建，预览分列已固化", order

    def get_migration(self, migration_id: str) -> MigrationOrder | None:
        return self._migrations.get(migration_id)

    # ------------------------------------------------------------------
    # 启用 / 回滚 / 重试
    # ------------------------------------------------------------------
    def activate(self, migration_id: str, at: str) -> tuple[bool, str, MigrationOrder | None]:
        """启用新规则：旧规则标记被替代但保留，新规则生效。"""
        order = self._migrations.get(migration_id)
        if order is None:
            return False, f"迁移单 {migration_id} 不存在", None
        if order.status not in ("待启用", "已回滚"):
            return False, f"迁移单处于 {order.status}，不能启用", None
        old = self._rules[order.old_rule_id]
        new = self._rules[order.new_rule_id]
        old.status = "被替代"
        new.status = "已生效"
        order.status = "已启用"
        order.activated_at = at
        order.log("启用", f"{old.rule_id}→{new.rule_id} 新规则生效", at)
        return True, f"新规则 {new.rule_id} 已生效，旧规则 {old.rule_id} 标记被替代并保留", order

    def interrupt(self, migration_id: str, reason: str, at: str) -> tuple[bool, str, MigrationOrder | None]:
        """中断迁移：在途采用轮次保留，等待回滚或重试。"""
        order = self._migrations.get(migration_id)
        if order is None:
            return False, f"迁移单 {migration_id} 不存在", None
        if order.status != "已启用":
            return False, f"迁移单处于 {order.status}，不能中断", None
        order.status = "已中断"
        order.log("中断", reason, at)
        return True, f"迁移已中断：{reason}", order

    def rollback(self, migration_id: str, reason: str, at: str) -> tuple[bool, str, MigrationOrder | None]:
        """回滚：旧规则重新生效、新规则停用；在途轮次标记回滚，已交付轮次与预览保留。"""
        order = self._migrations.get(migration_id)
        if order is None:
            return False, f"迁移单 {migration_id} 不存在", None
        if order.status not in ("已启用", "已中断"):
            return False, f"迁移单处于 {order.status}，不能回滚", None
        old = self._rules[order.old_rule_id]
        new = self._rules[order.new_rule_id]
        old.status = "已生效"
        new.status = "已停用"
        order.status = "已回滚"
        order.rolled_back_at = at
        order.log("回滚", reason, at)
        for rounds in self._adoptions.values():
            for rnd in rounds:
                if rnd.migration_id != migration_id:
                    continue
                if rnd.state in (self.STATE_CLAIMED, self.STATE_IN_FLIGHT):
                    rnd.state = self.STATE_ROLLED_BACK
                    rnd.history.append(f"{at} 随迁移单回滚：{reason}")
        return True, (
            f"已回滚：旧规则 {old.rule_id} 重新生效，新规则 {new.rule_id} 停用；"
            "在途轮次回滚，已确认列与两份预览保留"
        ), order

    def retry(self, migration_id: str, at: str) -> tuple[bool, str, MigrationOrder | None]:
        """中断重试前置：只允许从已回滚状态重新启用，且不动任何已确认列。"""
        order = self._migrations.get(migration_id)
        if order is None:
            return False, f"迁移单 {migration_id} 不存在", None
        if order.status != "已回滚":
            return False, f"迁移单处于 {order.status}，请先回滚再重试", None
        return self.activate(migration_id, at)

    # ------------------------------------------------------------------
    # 批次采用轮次：领取样本 → 绑定同次嗅探 → 字段交付
    # ------------------------------------------------------------------
    def claim_sample(self, migration_id: str, batch_id: str, operator: str,
                     at: str) -> tuple[bool, str, AdoptionRound | None]:
        """从嗅探页领取一次样本：在新规则上开一条采用轮次（已领取样本）。"""
        order = self._migrations.get(migration_id)
        if order is None:
            return False, f"迁移单 {migration_id} 不存在", None
        if order.status != "已启用":
            return False, f"迁移单处于 {order.status}，只有已启用迁移可领取样本", None
        rounds = self._adoptions.setdefault(batch_id, [])
        # 同一迁移下已有在途/已领取轮次时直接返回，不重复开轮
        existing = next(
            (r for r in rounds
             if r.migration_id == migration_id and r.state in (self.STATE_CLAIMED, self.STATE_IN_FLIGHT)),
            None,
        )
        if existing is not None:
            return True, f"批次 {batch_id} 已有在途采用轮次 {existing.record_id}，复用该轮", existing
        new_rule = self._rules[order.new_rule_id]
        if batch_id not in new_rule.hit_batches:
            new_rule.hit_batches.append(batch_id)
        rnd = AdoptionRound(
            record_id=self._next_record_id(batch_id),
            batch_id=batch_id, rule_id=new_rule.rule_id,
            version_label=new_rule.version_label,
            round_no=len(rounds) + 1, migration_id=migration_id,
            state=self.STATE_CLAIMED, operator=operator, adopted_at=at,
        )
        rnd.history.append(f"{at} {operator} 按新规则 {new_rule.rule_id} 领取样本")
        rounds.append(rnd)
        order.log("领取样本", f"批次 {batch_id} 轮次 {rnd.record_id}", at)
        return True, f"批次 {batch_id} 已在新规则 {new_rule.rule_id} 上领取样本", rnd

    def bind_sniff(self, record_id: str, sniff_id: str, column_count: int,
                   columns: list[str], column_positions: list[int],
                   first_row_is_header: bool, parse_start_row: int,
                   rule_id: str, at: str) -> tuple[bool, str, AdoptionRound | None]:
        """把同一次嗅探的字段解析、首行结论与列位置绑定到采用轮次。

        切换不能混用：嗅探所用规则必须与轮次采用规则一致，否则拒绝绑定。
        """
        rnd = self._find_round(record_id)
        if rnd is None:
            return False, f"批次规则版本记录 {record_id} 不存在", None
        if rnd.state not in (self.STATE_CLAIMED, self.STATE_IN_FLIGHT):
            return False, f"轮次 {record_id} 处于 {rnd.state}，不能再绑定嗅探", None
        if rule_id != rnd.rule_id:
            return False, (
                f"嗅探规则 {rule_id} 与轮次采用规则 {rnd.rule_id} 不一致，"
                "字段解析与列位置不得跨规则混用"
            ), None
        rnd.sniff_id = sniff_id
        rnd.column_count = column_count
        rnd.columns = list(columns)
        rnd.column_positions = list(column_positions) if column_positions else list(range(column_count))
        rnd.first_row_is_header = first_row_is_header
        rnd.parse_start_row = parse_start_row
        rnd.state = self.STATE_IN_FLIGHT
        rnd.history.append(
            f"{at} 绑定嗅探 {sniff_id}：{column_count} 列，起始行 {parse_start_row}")
        return True, f"轮次 {record_id} 已绑定同次嗅探 {sniff_id}，字段进入在途", rnd

    def deliver_fields(self, record_id: str, at: str) -> tuple[bool, str, AdoptionRound | None]:
        """字段交付：在途轮次转为已交付，列被确认后不可再替换或清空。"""
        rnd = self._find_round(record_id)
        if rnd is None:
            return False, f"批次规则版本记录 {record_id} 不存在", None
        if rnd.state != self.STATE_IN_FLIGHT:
            return False, f"轮次 {record_id} 处于 {rnd.state}，不能交付字段", None
        rnd.state = self.STATE_DELIVERED
        rnd.confirmed = True
        rnd.delivered_at = at
        rnd.history.append(f"{at} 字段交付，{rnd.column_count} 列确认锁定")
        return True, f"轮次 {record_id} 字段已交付，列已确认锁定", rnd

    # ------------------------------------------------------------------
    # 有效采用版本与字段解析
    # ------------------------------------------------------------------
    def rounds_of(self, batch_id: str) -> list[AdoptionRound]:
        return list(self._adoptions.get(batch_id, []))

    def effective_adoption(self, batch_id: str) -> AdoptionRound | None:
        """字段解析使用的有效采用版本：优先在途轮次，其次最近一条已交付轮次。

        在途轮次代表尚未交付的当前请求；没有在途时才回退到最近确认的交付轮次。
        """
        rounds = self._adoptions.get(batch_id, [])
        for rnd in reversed(rounds):
            if rnd.state == self.STATE_IN_FLIGHT:
                return rnd
        for rnd in reversed(rounds):
            if rnd.state == self.STATE_DELIVERED:
                return rnd
        return None

    def confirmed_rounds(self, batch_id: str) -> list[AdoptionRound]:
        """已确认旧轮回查：所有字段已交付的轮次都保留可查。"""
        return [r for r in self._adoptions.get(batch_id, []) if r.confirmed]

    def parse_fields(self, batch_id: str, lines: list[str]
                     ) -> tuple[bool, str, dict[str, Any] | None]:
        """按有效采用版本的规则与起始行输出实际解析字段，并附旧轮回查句柄。"""
        rnd = self.effective_adoption(batch_id)
        if rnd is None:
            return False, f"批次 {batch_id} 没有有效采用版本，字段解析不得启动", None
        rule = self._rules.get(rnd.rule_id)
        if rule is None:
            return False, f"轮次采用规则 {rnd.rule_id} 已不存在", None
        if not rnd.sniff_id:
            return False, f"轮次 {rnd.record_id} 尚未绑定嗅探，解析依据缺失", None

        parsed: list[dict[str, Any]] = []
        for offset, line in enumerate(lines):
            row_num = rnd.parse_start_row + offset
            cells = rule.split_line(line)
            parsed.append({
                "row_num": row_num,
                "rule_id": rule.rule_id,
                "version_label": rule.version_label,
                "record_id": rnd.record_id,
                "cells": cells,
            })
        return True, f"按有效采用版本 {rule.rule_id} 自第 {rnd.parse_start_row} 行解析", {
            "batch_id": batch_id,
            "record_id": rnd.record_id,
            "rule_id": rule.rule_id,
            "version_label": rule.version_label,
            "sniff_id": rnd.sniff_id,
            "parse_start_row": rnd.parse_start_row,
            "rows": parsed,
            "prior_confirmed": [r.record_id for r in self.confirmed_rounds(batch_id)],
        }

    # ------------------------------------------------------------------
    # 恢复逗号解析：按迁移单辨别旧动作结果，不覆盖已认可列
    # ------------------------------------------------------------------
    def restore_comma(self, batch_id: str, operator: str, at: str
                      ) -> tuple[bool, str, AdoptionRound | None]:
        """为同一批恢复旧（逗号）解析：旧列已确认才续接，已认可列绝不替换/清空。"""
        rounds = self._adoptions.get(batch_id, [])
        latest_new = next(
            (r for r in reversed(rounds)
             if r.migration_id and self._rules[r.rule_id] is not None
             and r.rule_id == self._migrations[r.migration_id].new_rule_id),
            None,
        )
        if latest_new is None:
            return False, f"批次 {batch_id} 没有新规则采用轮次，无需恢复逗号", None
        order = self._migrations[latest_new.migration_id]
        old_rule = self._rules[order.old_rule_id]

        old_confirmed = next(
            (r for r in reversed(rounds)
             if r.rule_id == old_rule.rule_id and r.confirmed),
            None,
        )
        # 已有旧规则在途/已领取轮次则直接续用，不重复开轮、不动已确认列
        old_open = next(
            (r for r in reversed(rounds)
             if r.rule_id == old_rule.rule_id and r.state in (self.STATE_CLAIMED, self.STATE_IN_FLIGHT)),
            None,
        )
        if old_open is not None:
            return True, f"批次 {batch_id} 旧规则轮次 {old_open.record_id} 仍在途，继续该轮", old_open
        rnd = AdoptionRound(
            record_id=self._next_record_id(batch_id),
            batch_id=batch_id, rule_id=old_rule.rule_id,
            version_label=old_rule.version_label,
            round_no=len(rounds) + 1, migration_id=order.migration_id,
            state=self.STATE_CLAIMED, operator=operator, adopted_at=at,
        )
        if old_confirmed is not None:
            # 旧动作结果已认可：续接原列，仅推进轮次，不替换也不清空
            rnd.columns = list(old_confirmed.columns)
            rnd.column_count = old_confirmed.column_count
            rnd.column_positions = list(old_confirmed.column_positions)
            rnd.first_row_is_header = old_confirmed.first_row_is_header
            rnd.parse_start_row = old_confirmed.parse_start_row
            rnd.history.append(
                f"{at} 按迁移单辨别到旧轮 {old_confirmed.record_id} 已确认 {rnd.column_count} 列，"
                "续接原列不替换不清空")
        else:
            rnd.history.append(
                f"{at} 旧规则无已确认列，开新轮次待重新嗅探，已认可的新规则列保持不变")
        rounds.append(rnd)
        order.log("恢复逗号", f"批次 {batch_id} 续接旧规则 {old_rule.rule_id}", at)
        suffix = "，续接原列不替换不清空" if old_confirmed is not None else "，待重新嗅探"
        return True, f"批次 {batch_id} 已导向旧规则 {old_rule.rule_id}{suffix}", rnd

    # ------------------------------------------------------------------
    # 证据、发布看板与资格引导
    # ------------------------------------------------------------------
    def evidence(self, *, batch_id: str = "", rule_id: str = "") -> list[dict[str, Any]]:
        """打开采用证据：按批次或规则汇总采用轮次及其迁移单、预览与嗅探锚点。"""
        result: list[dict[str, Any]] = []
        for batch, rounds in self._adoptions.items():
            if batch_id and batch != batch_id:
                continue
            for rnd in rounds:
                if rule_id and rnd.rule_id != rule_id:
                    continue
                order = self._migrations.get(rnd.migration_id)
                result.append({
                    **rnd.to_dict(),
                    "migration_status": order.status if order else "",
                    "old_preview_columns": (
                        len(order.old_preview[0]) if order and order.old_preview else 0),
                    "new_preview_columns": (
                        len(order.new_preview[0]) if order and order.new_preview else 0),
                })
        return result

    def publish_board(self) -> dict[str, Any]:
        """发布页：区分已领取样本（在途轮次）与待处理的新批（已生效规则尚无采用轮次）。"""
        claimed: list[dict[str, Any]] = []
        claimed_batches: set[str] = set()
        for batch_id, rounds in self._adoptions.items():
            for rnd in rounds:
                if rnd.state in (self.STATE_CLAIMED, self.STATE_IN_FLIGHT):
                    claimed.append(rnd.to_dict())
                    claimed_batches.add(batch_id)
        pending_new: list[dict[str, Any]] = []
        for rule in self._rules.values():
            if rule.status != "已生效":
                continue
            for batch_id in rule.hit_batches:
                if batch_id in claimed_batches:
                    continue
                if not any(r.state in (self.STATE_IN_FLIGHT, self.STATE_DELIVERED)
                           for r in self._adoptions.get(batch_id, [])):
                    pending_new.append({"batch_id": batch_id, "rule_id": rule.rule_id})
        return {"claimed_samples": claimed, "pending_new_batches": pending_new}

    def operator_guidance(self, batch_id: str, operator: str) -> dict[str, Any]:
        """按服务端资格把操作员导向旧批继续，或重新选择合格规则。"""
        rounds = self._adoptions.get(batch_id, [])
        # 资格判定：存在当前仍生效规则上的在途轮次 → 可继续旧批，否则重选合格规则
        eligible_old = next(
            (r for r in rounds
             if r.state in (self.STATE_CLAIMED, self.STATE_IN_FLIGHT)
             and (rule := self._rules.get(r.rule_id)) is not None
             and rule.status == "已生效"),
            None,
        )
        if eligible_old is not None:
            return {
                "eligible": True,
                "direction": "继续旧批",
                "batch_id": batch_id,
                "rule_id": eligible_old.rule_id,
                "record_id": eligible_old.record_id,
                "message": f"操作员 {operator} 具备旧批资格，沿轮次 {eligible_old.record_id} 继续",
            }
        qualified = [r.rule_id for r in self._rules.values() if r.status == "已生效"]
        return {
            "eligible": False,
            "direction": "重新选择合格规则",
            "batch_id": batch_id,
            "qualified_rules": qualified,
            "message": "旧规则在途轮次已随回滚关闭，请在合格规则中重新选择后再领取",
        }

    def snapshot(self) -> dict[str, Any]:
        return {
            "rules": {k: v.to_dict() for k, v in self._rules.items()},
            "migrations": {k: v.to_dict() for k, v in self._migrations.items()},
            "adoptions": {
                batch: [r.to_dict() for r in rounds]
                for batch, rounds in self._adoptions.items()
            },
        }

    def _find_round(self, record_id: str) -> AdoptionRound | None:
        for rounds in self._adoptions.values():
            for rnd in rounds:
                if rnd.record_id == record_id:
                    return rnd
        return None
