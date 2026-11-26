"""规则迁移领域服务：把存量分隔规则迁移成可预览、可启用、可回滚的版本化流程，产出规则迁移单数据集。

迁移单永久保留新旧两份分列预览与实际采用记录；启用/回滚只切换规则条目状态，
不删除旧规则。样本领取、同次嗅探绑定、字段交付、恢复逗号与字段解析均在此
协调分隔规则、结构嗅探与批次规则版本记录三个数据集，已确认列不可替换或清空。
"""
from __future__ import annotations

import json
from typing import Any

from app.services.base import BaseModuleService
from app.services.delimiter_rule import DelimiterRuleService
from app.services.batch_rule_version import BatchRuleVersionService
from app.services.schema_sniff import SchemaSniffService
from app.store import store


class RuleMigrationService(BaseModuleService):
    MODULE = "rule_migration"
    ENTITY = "规则迁移单"
    OPERATION_TERM = "迁移"
    LIST_FIELDS = ["迁移单编号", "适用格式", "旧规则编号", "新规则编号", "迁移状态", "操作者标识", "旧预览列数", "新预览列数", "创建时间", "最近动作时间"]
    REQUIRED_FIELDS = ["迁移单编号", "适用格式", "旧规则编号", "新规则编号"]
    STATUS_ORDER = ["待启用", "已启用", "已中断", "已回滚"]
    ACTION_RULES = {"启用迁移": "已启用", "中断迁移": "已中断", "回滚迁移": "已回滚"}
    NEGATIVE_ACTIONS = ["中断迁移", "回滚迁移"]

    def __init__(self) -> None:
        self.rule_svc = DelimiterRuleService()
        self.version_svc = BatchRuleVersionService()
        self.sniff_svc = SchemaSniffService()

    # ------------------------------------------------------------------
    def _find(self, migration_no: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("迁移单编号") == migration_no:
                return row
        return None

    @staticmethod
    def _log(row: dict[str, Any], text: str) -> None:
        row["操作留痕"] = (str(row.get("操作留痕") or "") + ("；" if row.get("操作留痕") else "") + text)

    # ------------------------------------------------------------------
    # 建单与预览
    # ------------------------------------------------------------------
    def create_migration(self, migration_no: str, fmt: str, old_rule_no: str,
                         new_rule_no: str, sample_lines: list[str],
                         operator: str, at: str) -> tuple[bool, str, dict[str, Any] | None]:
        """建迁移单并固化新旧两份分列预览（预览随单保留，回滚后仍可对照）。"""
        if self._find(migration_no) is not None:
            return False, f"迁移单 {migration_no} 已存在", None
        old_rule = self.rule_svc.find_rule(old_rule_no)
        new_rule = self.rule_svc.find_rule(new_rule_no)
        if old_rule is None or new_rule is None:
            return False, "迁移所依据的规则条目不存在，无法建单", None
        if old_rule.get("适用格式") != fmt or new_rule.get("适用格式") != fmt:
            return False, f"两条规则的适用格式必须都是 {fmt}", None
        if old_rule_no == new_rule_no:
            return False, "新旧规则不能是同一条目", None
        if not sample_lines:
            return False, "样本行为空，无法生成迁移预览", None

        ok_old, _m, old_preview = self.rule_svc.preview(old_rule_no, sample_lines)
        ok_new, _m, new_preview = self.rule_svc.preview(new_rule_no, sample_lines)
        if not (ok_old and ok_new):
            return False, "分列预览生成失败，迁移单未建立", None

        rows = store.rows(self.MODULE)
        entry = {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "status": "待启用",
            "pending": True,
            "abnormal": False,
            "迁移单编号": migration_no,
            "适用格式": fmt,
            "旧规则编号": old_rule_no,
            "新规则编号": new_rule_no,
            "迁移状态": "待启用",
            "操作者标识": operator,
            "旧预览列数": str(old_preview["column_count"]),
            "新预览列数": str(new_preview["column_count"]),
            "创建时间": at,
            "最近动作时间": at,
            "旧规则预览": json.dumps(old_preview["preview_rows"], ensure_ascii=False),
            "新规则预览": json.dumps(new_preview["preview_rows"], ensure_ascii=False),
            "操作留痕": f"{at} 建单并固化新旧分列预览",
        }
        rows.append(entry)
        return True, f"迁移单 {migration_no} 已建，新旧分列预览已固化", dict(entry)

    def preview_detail(self, migration_no: str) -> dict[str, Any] | None:
        """输出迁移单两份预览的分列明细，供两个页面打开采用证据时对照。"""
        row = self._find(migration_no)
        if row is None:
            return None
        return {
            "迁移单编号": migration_no,
            "迁移状态": row.get("迁移状态"),
            "旧规则编号": row.get("旧规则编号"),
            "新规则编号": row.get("新规则编号"),
            "旧预览列数": row.get("旧预览列数"),
            "新预览列数": row.get("新预览列数"),
            "旧规则预览": json.loads(str(row.get("旧规则预览") or "[]")),
            "新规则预览": json.loads(str(row.get("新规则预览") or "[]")),
            "操作留痕": row.get("操作留痕"),
        }

    # ------------------------------------------------------------------
    # 启用 / 中断 / 回滚 / 重试
    # ------------------------------------------------------------------
    def activate(self, migration_no: str, at: str) -> tuple[bool, str, dict[str, Any] | None]:
        row = self._find(migration_no)
        if row is None:
            return False, f"迁移单 {migration_no} 不存在", None
        if row.get("迁移状态") not in ("待启用", "已回滚"):
            return False, f"迁移单处于 {row.get('迁移状态')}，不能启用", None
        self.rule_svc.set_status(row["旧规则编号"], "被替代")
        self.rule_svc.set_status(row["新规则编号"], "已生效")
        row["迁移状态"] = "已启用"
        row["status"] = "已启用"
        row["pending"] = False
        row["最近动作时间"] = at
        self._log(row, f"{at} 启用：{row['旧规则编号']}→{row['新规则编号']} 新规则生效")
        return True, f"新规则 {row['新规则编号']} 已生效，旧规则标记被替代并保留", dict(row)

    def interrupt(self, migration_no: str, reason: str, at: str) -> tuple[bool, str, dict[str, Any] | None]:
        row = self._find(migration_no)
        if row is None:
            return False, f"迁移单 {migration_no} 不存在", None
        if row.get("迁移状态") != "已启用":
            return False, f"迁移单处于 {row.get('迁移状态')}，不能中断", None
        row["迁移状态"] = "已中断"
        row["status"] = "已中断"
        row["pending"] = True
        row["abnormal"] = True
        row["最近动作时间"] = at
        self._log(row, f"{at} 中断：{reason}")
        return True, f"迁移已中断：{reason}", dict(row)

    def rollback(self, migration_no: str, reason: str, at: str) -> tuple[bool, str, dict[str, Any] | None]:
        """回滚：旧规则重新生效、新规则停用；在途轮次回滚，预览与已确认列保留。"""
        row = self._find(migration_no)
        if row is None:
            return False, f"迁移单 {migration_no} 不存在", None
        if row.get("迁移状态") not in ("已启用", "已中断"):
            return False, f"迁移单处于 {row.get('迁移状态')}，不能回滚", None
        self.rule_svc.set_status(row["旧规则编号"], "已生效")
        self.rule_svc.set_status(row["新规则编号"], "已停用")
        changed = self.version_svc.mark_rolled_back(migration_no, reason, at)
        row["迁移状态"] = "已回滚"
        row["status"] = "已回滚"
        row["pending"] = True
        row["最近动作时间"] = at
        self._log(row, f"{at} 回滚（{reason}）：在途轮次 {changed} 条回滚，预览与已确认列保留")
        return True, (
            f"已回滚：旧规则 {row['旧规则编号']} 重新生效，新规则 {row['新规则编号']} 停用；"
            f"{changed} 条在途轮次回滚，两份预览与已确认列保留"
        ), dict(row)

    def retry(self, migration_no: str, at: str) -> tuple[bool, str, dict[str, Any] | None]:
        """中断后重试：必须先回滚，已确认列不被再次替换或清空。"""
        row = self._find(migration_no)
        if row is None:
            return False, f"迁移单 {migration_no} 不存在", None
        if row.get("迁移状态") != "已回滚":
            return False, f"迁移单处于 {row.get('迁移状态')}，请先回滚再重试", None
        return self.activate(migration_no, at)

    # ------------------------------------------------------------------
    # 领取样本 → 绑定同次嗅探 → 字段交付
    # ------------------------------------------------------------------
    def claim_sample(self, migration_no: str, intake_batch: str, operator: str,
                     at: str) -> tuple[bool, str, dict[str, Any] | None]:
        row = self._find(migration_no)
        if row is None:
            return False, f"迁移单 {migration_no} 不存在", None
        if row.get("迁移状态") != "已启用":
            return False, f"迁移单处于 {row.get('迁移状态')}，只有已启用迁移可领取样本", None
        new_rule = self.rule_svc.find_rule(row["新规则编号"])
        ok, msg, record = self.version_svc.open_record(
            intake_batch, new_rule["规则编号"], str(new_rule.get("规则版本号") or ""),
            migration_no, operator, at)
        if ok and "已在规则" in msg:
            self._log(row, f"{at} 批次 {intake_batch} 领取样本（轮次 {record['版本记录编号']}）")
        return ok, msg, record

    def bind_sniff(self, record_no: str, sniff_no: str, at: str
                   ) -> tuple[bool, str, dict[str, Any] | None]:
        """绑定同次嗅探：嗅探命中规则须与轮次采用规则一致，列位置与首行结论取自该嗅探。"""
        record = self.version_svc.find_record(record_no)
        if record is None:
            return False, f"版本记录 {record_no} 不存在", None
        sniff = next((r for r in store.rows("schema_sniff")
                      if r.get("嗅探编号") == sniff_no), None)
        if sniff is None:
            return False, f"结构嗅探 {sniff_no} 不存在", None
        rule_no = str(sniff.get("命中分隔规则") or "")
        if rule_no and rule_no != record.get("规则编号"):
            return False, (
                f"嗅探 {sniff_no} 命中规则 {rule_no} 与轮次采用规则 {record.get('规则编号')} "
                "不一致，切换不得混用"
            ), None
        column_count = int(str(sniff.get("推断列数") or 0))
        columns = [c.strip() for c in str(sniff.get("列名清单") or "").split(",") if c.strip()]
        start_row = self.sniff_svc.parse_start_row(sniff_no)
        is_header = sniff.get("首行是否表头") == "是"
        return self.version_svc.bind_sniff(
            record_no, sniff_no, column_count, columns, list(range(column_count)),
            is_header, start_row, str(record.get("规则编号")), at)

    def deliver_fields(self, record_no: str, at: str) -> tuple[bool, str, dict[str, Any] | None]:
        return self.version_svc.deliver_fields(record_no, at)

    # ------------------------------------------------------------------
    # 有效采用版本与字段解析
    # ------------------------------------------------------------------
    def effective_adoption(self, intake_batch: str) -> dict[str, Any] | None:
        return self.version_svc.effective_adoption(intake_batch)

    def parse_fields(self, intake_batch: str, data_lines: list[str]
                     ) -> tuple[bool, str, dict[str, Any] | None]:
        """按批次规则版本记录的有效采用版本，从对应起始行输出实际解析字段。"""
        record = self.version_svc.effective_adoption(intake_batch)
        if record is None:
            return False, f"批次 {intake_batch} 没有有效采用版本，字段解析不得启动", None
        if not record.get("结构嗅探编号"):
            return False, f"轮次 {record['版本记录编号']} 尚未绑定嗅探，解析依据缺失", None
        rule_no = str(record["规则编号"])
        start_row = int(str(record.get("起始行") or 1))
        parsed_rows: list[dict[str, Any]] = []
        for offset, line in enumerate(data_lines):
            cells = self.rule_svc.split_with_rule(rule_no, line)
            parsed_rows.append({
                "row_num": start_row + offset,
                "cells": cells,
                "规则编号": rule_no,
                "版本记录编号": record["版本记录编号"],
                "结构嗅探编号": record["结构嗅探编号"],
            })
        return True, f"按有效采用版本 {rule_no} 自第 {start_row} 行解析", {
            "关联批次": intake_batch,
            "版本记录编号": record["版本记录编号"],
            "规则编号": rule_no,
            "规则版本号": record.get("规则版本号"),
            "结构嗅探编号": record["结构嗅探编号"],
            "起始行": start_row,
            "确认列数": record.get("确认列数"),
            "rows": parsed_rows,
            "prior_confirmed": [r["版本记录编号"]
                                for r in self.version_svc.confirmed_rounds(intake_batch)],
        }

    # ------------------------------------------------------------------
    # 恢复逗号：按迁移单辨别旧动作结果，不替换/清空已认可列
    # ------------------------------------------------------------------
    def restore_comma(self, intake_batch: str, operator: str, at: str
                      ) -> tuple[bool, str, dict[str, Any] | None]:
        rounds = self.version_svc.rounds_of_batch(intake_batch)
        latest_new = None
        for r in reversed(rounds):
            migration_no = r.get("迁移单编号")
            if not migration_no:
                continue
            order = self._find(migration_no)
            if order is not None and r.get("规则编号") == order["新规则编号"]:
                latest_new = r
                break
        if latest_new is None:
            return False, f"批次 {intake_batch} 没有新规则采用轮次，无需恢复逗号", None
        order = self._find(str(latest_new["迁移单编号"]))
        old_rule_no = str(order["旧规则编号"])

        # 旧规则已有在途轮次则续用；不允许复用新规则在途轮次
        open_old = self.version_svc.open_round_on_rule(intake_batch, old_rule_no)
        if open_old is not None:
            return True, f"批次 {intake_batch} 旧规则轮次 {open_old['版本记录编号']} 仍在途，继续该轮", open_old

        ok, msg, record = self.version_svc.open_record(
            intake_batch, old_rule_no,
            str(self.rule_svc.find_rule(old_rule_no).get("规则版本号") or ""),
            order["迁移单编号"], operator, at)
        if not ok:
            return False, msg, record

        confirmed_old = self.version_svc.confirmed_round_on_rule(intake_batch, old_rule_no)
        if confirmed_old is not None:
            # 旧动作结果已认可：续接原列，只推进轮次，绝不再次替换或清空
            record["确认列数"] = confirmed_old["确认列数"]
            record["列名清单"] = confirmed_old["列名清单"]
            record["列位置"] = confirmed_old["列位置"]
            record["首行是否表头"] = confirmed_old["首行是否表头"]
            record["起始行"] = confirmed_old["起始行"]
            record["操作留痕"] += (
                f"；{at} 按迁移单辨别旧轮 {confirmed_old['版本记录编号']} 已确认 "
                f"{record['确认列数']} 列，续接原列不替换不清空")
        else:
            record["操作留痕"] += (
                f"；{at} 旧规则无已确认列，开新轮待重新嗅探，已认可的新规则列保持不变")
        self._log(order, f"{at} 批次 {intake_batch} 恢复逗号，导向旧规则 {old_rule_no}")
        return True, f"批次 {intake_batch} 已导向旧规则 {old_rule_no} 继续", record

    # ------------------------------------------------------------------
    # 证据、发布看板、资格引导
    # ------------------------------------------------------------------
    def evidence(self, *, intake_batch: str = "", rule_no: str = "") -> list[dict[str, Any]]:
        """两处页面共用的采用证据：批次规则版本记录 + 所属迁移单预览锚点。"""
        items = self.version_svc.evidence(intake_batch=intake_batch, rule_no=rule_no)
        for item in items:
            order = self._find(str(item.get("迁移单编号") or ""))
            if order is not None:
                item["迁移状态"] = order.get("迁移状态")
                item["旧预览列数"] = order.get("旧预览列数")
                item["新预览列数"] = order.get("新预览列数")
        return items

    def publish_board(self) -> dict[str, Any]:
        """发布页：已领取样本（在途轮次）与待处理新批分区展示。"""
        claimed = [
            dict(r) for r in store.rows("batch_rule_version")
            if r.get("采用状态") in (BatchRuleVersionService.CLAIMED,
                                     BatchRuleVersionService.IN_FLIGHT)
        ]
        claimed_batches = {r["关联批次"] for r in claimed}
        delivered_batches = {
            r["关联批次"] for r in store.rows("batch_rule_version")
            if r.get("采用状态") == BatchRuleVersionService.DELIVERED
        }
        known_batches = {
            r.get("关联投递批次") for r in store.rows("parse_batch")
            if r.get("关联投递批次")
        }
        pending_new = sorted(
            b for b in known_batches
            if b not in claimed_batches and b not in delivered_batches
        )
        return {
            "claimed_samples": claimed,
            "pending_new_batches": [{"关联批次": b} for b in pending_new],
        }

    def operator_guidance(self, intake_batch: str, operator: str) -> dict[str, Any]:
        """服务端资格：在途轮次的规则当前仍生效→继续旧批；否则重选合格规则。"""
        for record in self.version_svc.rounds_of_batch(intake_batch):
            if record.get("采用状态") not in BatchRuleVersionService.OPEN_STATES:
                continue
            rule = self.rule_svc.find_rule(str(record.get("规则编号")))
            if rule is not None and rule.get("status") == "已生效":
                return {
                    "eligible": True,
                    "direction": "继续旧批",
                    "关联批次": intake_batch,
                    "规则编号": rule["规则编号"],
                    "版本记录编号": record["版本记录编号"],
                    "message": f"操作员 {operator} 具备旧批资格，沿轮次 {record['版本记录编号']} 继续",
                }
        qualified = [
            r["规则编号"] for r in store.rows("delimiter_rule")
            if r.get("status") == "已生效"
        ]
        return {
            "eligible": False,
            "direction": "重新选择合格规则",
            "关联批次": intake_batch,
            "qualified_rules": qualified,
            "message": "旧规则在途轮次已随回滚关闭，请在合格规则中重新选择后再领取",
        }
