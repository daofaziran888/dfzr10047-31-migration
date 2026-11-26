"""批次规则版本记录领域服务：登记每个批次实际采用的分隔规则版本与采用轮次，产出批次规则版本数据集。

一条记录对应一个批次在某条规则上的一次采用，字段解析、首行表头结论与列位置
必须来自同一次结构嗅探；已交付（确认）的轮次永久保留回查，回滚与恢复逗号
都不允许替换或清空已认可的列。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class BatchRuleVersionService(BaseModuleService):
    MODULE = "batch_rule_version"
    ENTITY = "批次规则版本记录"
    OPERATION_TERM = "采用"
    LIST_FIELDS = ["版本记录编号", "关联批次", "规则编号", "规则版本号", "迁移单编号", "采用轮次", "采用状态", "结构嗅探编号", "确认列数", "起始行", "采用时间"]
    REQUIRED_FIELDS = ["版本记录编号", "关联批次", "规则编号"]
    STATUS_ORDER = ["已领取样本", "字段在途", "已交付", "已回滚"]
    ACTION_RULES = {"标记在途": "字段在途", "确认交付": "已交付", "登记回滚": "已回滚"}
    NEGATIVE_ACTIONS = ["登记回滚"]

    CLAIMED = "已领取样本"
    IN_FLIGHT = "字段在途"
    DELIVERED = "已交付"
    ROLLED_BACK = "已回滚"
    OPEN_STATES = (CLAIMED, IN_FLIGHT)

    def find_record(self, record_no: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("版本记录编号") == record_no:
                return row
        return None

    def rounds_of_batch(self, intake_batch: str) -> list[dict[str, Any]]:
        """同批全部采用轮次，按轮次顺序返回，供两版列数与确认范围对照。"""
        rows = [r for r in store.rows(self.MODULE) if r.get("关联批次") == intake_batch]
        return sorted(rows, key=lambda r: int(str(r.get("采用轮次") or 0)))

    def next_round_no(self, intake_batch: str) -> int:
        return len(self.rounds_of_batch(intake_batch)) + 1

    def open_round(self, intake_batch: str, migration_no: str = "") -> dict[str, Any] | None:
        """批次当前在途/已领取的轮次（可限定迁移单）。"""
        for row in reversed(self.rounds_of_batch(intake_batch)):
            if row.get("采用状态") not in self.OPEN_STATES:
                continue
            if migration_no and row.get("迁移单编号") != migration_no:
                continue
            return row
        return None

    def effective_adoption(self, intake_batch: str) -> dict[str, Any] | None:
        """字段解析使用的有效采用版本：优先在途轮次，其次最近一条已交付轮次。"""
        rounds = self.rounds_of_batch(intake_batch)
        for row in reversed(rounds):
            if row.get("采用状态") == self.IN_FLIGHT:
                return row
        for row in reversed(rounds):
            if row.get("采用状态") == self.DELIVERED:
                return row
        return None

    def confirmed_rounds(self, intake_batch: str) -> list[dict[str, Any]]:
        """已确认旧轮回查：所有已交付轮次保留可查。"""
        return [r for r in self.rounds_of_batch(intake_batch)
                if r.get("采用状态") == self.DELIVERED]

    def open_round_on_rule(self, intake_batch: str, rule_no: str) -> dict[str, Any] | None:
        for row in reversed(self.rounds_of_batch(intake_batch)):
            if row.get("规则编号") == rule_no and row.get("采用状态") in self.OPEN_STATES:
                return row
        return None

    def confirmed_round_on_rule(self, intake_batch: str, rule_no: str) -> dict[str, Any] | None:
        for row in reversed(self.rounds_of_batch(intake_batch)):
            if row.get("规则编号") == rule_no and row.get("采用状态") == self.DELIVERED:
                return row
        return None

    def open_record(self, intake_batch: str, rule_no: str, version_label: str,
                    migration_no: str, operator: str, at: str) -> tuple[bool, str, dict[str, Any] | None]:
        """开一条采用轮次（领取样本）；同规则同迁移在途轮次复用，不重复开轮。"""
        existing = next(
            (r for r in reversed(self.rounds_of_batch(intake_batch))
             if r.get("采用状态") in self.OPEN_STATES
             and r.get("规则编号") == rule_no
             and (not migration_no or r.get("迁移单编号") == migration_no)),
            None,
        )
        if existing is not None:
            return True, f"批次 {intake_batch} 在途轮次 {existing['版本记录编号']} 已存在，复用该轮", existing
        rows = store.rows(self.MODULE)
        round_no = self.next_round_no(intake_batch)
        record = {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "status": self.CLAIMED,
            "pending": True,
            "abnormal": False,
            "版本记录编号": f"BRV-{intake_batch}-{round_no:03d}",
            "关联批次": intake_batch,
            "规则编号": rule_no,
            "规则版本号": version_label,
            "迁移单编号": migration_no,
            "采用轮次": str(round_no),
            "采用状态": self.CLAIMED,
            "结构嗅探编号": "",
            "确认列数": "0",
            "列名清单": "",
            "列位置": "",
            "首行是否表头": "",
            "起始行": "1",
            "操作者标识": operator,
            "采用时间": at,
            "交付时间": "",
            "操作留痕": f"{at} {operator} 按规则 {rule_no} 领取样本",
        }
        rows.append(record)
        return True, f"批次 {intake_batch} 已在规则 {rule_no} 上开启第 {round_no} 轮采用", record

    def bind_sniff(self, record_no: str, sniff_no: str, column_count: int,
                   columns: list[str], column_positions: list[int],
                   first_row_is_header: bool, parse_start_row: int,
                   rule_no: str, at: str) -> tuple[bool, str, dict[str, Any] | None]:
        """把同次嗅探的字段解析、首行结论、列位置绑定到轮次；跨规则混用直接拒绝。"""
        record = self.find_record(record_no)
        if record is None:
            return False, f"版本记录 {record_no} 不存在", None
        if record.get("采用状态") not in self.OPEN_STATES:
            return False, f"轮次 {record_no} 处于 {record.get('采用状态')}，不能再绑定嗅探", None
        if record.get("规则编号") != rule_no:
            return False, (
                f"嗅探规则 {rule_no} 与轮次采用规则 {record.get('规则编号')} 不一致，"
                "字段解析与列位置不得跨规则混用"
            ), None
        record["结构嗅探编号"] = sniff_no
        record["确认列数"] = str(column_count)
        record["列名清单"] = ",".join(columns)
        record["列位置"] = ",".join(str(p) for p in column_positions)
        record["首行是否表头"] = "是" if first_row_is_header else "否"
        record["起始行"] = str(parse_start_row)
        record["采用状态"] = self.IN_FLIGHT
        record["status"] = self.IN_FLIGHT
        record["操作留痕"] += f"；{at} 绑定同次嗅探 {sniff_no}（{column_count} 列，起始行 {parse_start_row}）"
        return True, f"轮次 {record_no} 已绑定同次嗅探 {sniff_no}，字段进入在途", record

    def deliver_fields(self, record_no: str, at: str) -> tuple[bool, str, dict[str, Any] | None]:
        """字段交付：轮次转已交付，列被确认锁定，之后不可替换或清空。"""
        record = self.find_record(record_no)
        if record is None:
            return False, f"版本记录 {record_no} 不存在", None
        if record.get("采用状态") != self.IN_FLIGHT:
            return False, f"轮次 {record_no} 处于 {record.get('采用状态')}，不能交付字段", None
        record["采用状态"] = self.DELIVERED
        record["status"] = self.DELIVERED
        record["pending"] = False
        record["交付时间"] = at
        record["操作留痕"] += f"；{at} 字段交付，{record.get('确认列数')} 列确认锁定"
        return True, f"轮次 {record_no} 字段已交付，列已确认锁定", record

    def mark_rolled_back(self, migration_no: str, reason: str, at: str) -> int:
        """迁移回滚时把该迁移单下在途轮次标记回滚，已交付轮次保留。"""
        changed = 0
        for row in store.rows(self.MODULE):
            if row.get("迁移单编号") != migration_no:
                continue
            if row.get("采用状态") in self.OPEN_STATES:
                row["采用状态"] = self.ROLLED_BACK
                row["status"] = self.ROLLED_BACK
                row["操作留痕"] += f"；{at} 随迁移单回滚：{reason}"
                changed += 1
        return changed

    def evidence(self, *, intake_batch: str = "", rule_no: str = "") -> list[dict[str, Any]]:
        """打开采用证据：按批次或规则过滤采用轮次。"""
        rows = store.rows(self.MODULE)
        if intake_batch:
            rows = [r for r in rows if r.get("关联批次") == intake_batch]
        if rule_no:
            rows = [r for r in rows if r.get("规则编号") == rule_no]
        return [dict(r) for r in rows]
