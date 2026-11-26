"""规则迁移链路测试：可预览/启用/回滚的版本化分隔规则、同次嗅探绑定与字段解析。

覆盖两条主线：
- 管线层 RuleMigrationManager：迁移单状态机、采用轮次、回滚保留、恢复逗号不覆盖确认列
- 服务层 rule_migration / batch_rule_version：真实读写内存仓库，服务端查验有效采用版本，
  路由函数以直接调用方式验证入参出参（环境无 httpx，不起 TestClient）
"""
from __future__ import annotations

import pytest

from app.pipeline.rule_migration import (
    RuleMigrationManager, RuleVersion, AdoptionRound,
)
from app.pipeline.structure_sniffer import (
    StructureSniffer, DelimiterRule,
)
from app.routers import migration as migration_router
from app.schemas import (
    MigrationCreatePayload, MigrationActionPayload, ClaimPayload,
    BindPayload, RecordActionPayload, RestorePayload, ParsePayload,
    SamplePayload, GuidancePayload,
)
from app.services import SERVICES
from app.store import store


# 同一段文本：逗号 4 列，分号 1 列，用于验证两份筛选依据可对照
COMMA_LINES = [
    "rec_id,rec_timestamp,data_value,payload_bytes",
    "R-1,2026-09-01 08:00:00,100.5,1024",
    "R-2,2026-09-02 09:00:00,200.5,2048",
]
SEMICOLON_LINES = [
    "rec_id;rec_timestamp;data_value;payload_bytes",
    "R-1;2026-09-01 08:00:00;100.5;1024",
]
# 带引号与转义的样本：旧逗号规则必须按历史引号/转义行为重现原列与原值
QUOTED_LINES = [
    'rec_id,note,data_value',
    'R-1,"hello, world",100.5',
    'R-2,"say ""hi""",200.5',
    r'R-3,a\,b,300.5',
]


def _manager() -> RuleMigrationManager:
    mgr = RuleMigrationManager()
    mgr.register_rule(RuleVersion("DR-OLD", "csv", "v1.4.0", ",", '"', "\\"))
    mgr.register_rule(RuleVersion("DR-NEW", "csv", "v2.0.0", ";", '"', "\\"))
    return mgr


# ======================================================================
# 管线层：RuleMigrationManager 状态机
# ======================================================================

class TestRuleMigrationLifecycle:
    def test_preview_keeps_two_column_sets(self):
        mgr = _manager()
        ok, msg, order = mgr.create_migration(
            "RM-1", "csv", "DR-OLD", "DR-NEW", COMMA_LINES, "op-1", "t1")
        assert ok is True
        assert order.old_preview[0] == ["rec_id", "rec_timestamp", "data_value", "payload_bytes"]
        # 逗号文本按分号规则切出单列
        assert order.new_preview[0] == ["rec_id,rec_timestamp,data_value,payload_bytes"]
        assert "预览" in msg

    def test_create_rejects_different_fmt_and_empty_sample(self):
        mgr = _manager()
        mgr.register_rule(RuleVersion("DR-T", "tsv", "v1", "\t", '"', "\\"))
        ok, msg, order = mgr.create_migration(
            "RM-X", "csv", "DR-OLD", "DR-T", COMMA_LINES, "op", "t")
        assert ok is False and order is None and "适用格式" in msg
        ok2, _m, order2 = mgr.create_migration(
            "RM-X2", "csv", "DR-OLD", "DR-NEW", [], "op", "t")
        assert ok2 is False and order2 is None

    def test_activate_swaps_rule_status_but_keeps_old_rule(self):
        mgr = _manager()
        mgr.create_migration("RM-1", "csv", "DR-OLD", "DR-NEW", COMMA_LINES, "op", "t1")
        ok, _m, order = mgr.activate("RM-1", "t2")
        assert ok is True and order.status == "已启用"
        assert mgr.get_rule("DR-OLD").status == "被替代"
        assert mgr.get_rule("DR-NEW").status == "已生效"
        assert mgr.active_rule("csv").rule_id == "DR-NEW"
        # 旧规则条目仍在注册表中可回查
        assert mgr.get_rule("DR-OLD") is not None

    def test_claim_requires_activated_migration(self):
        mgr = _manager()
        mgr.create_migration("RM-1", "csv", "DR-OLD", "DR-NEW", COMMA_LINES, "op", "t1")
        ok, msg, _r = mgr.claim_sample("RM-1", "FI-1", "duty-1", "t2")
        assert ok is False and "已启用" in msg
        mgr.activate("RM-1", "t3")
        ok2, msg2, rnd = mgr.claim_sample("RM-1", "FI-1", "duty-1", "t4")
        assert ok2 is True and rnd.state == mgr.CLAIMED
        assert rnd.rule_id == "DR-NEW"

    def test_duplicate_claim_reuses_inflight_round(self):
        mgr = _manager()
        mgr.create_migration("RM-1", "csv", "DR-OLD", "DR-NEW", COMMA_LINES, "op", "t1")
        mgr.activate("RM-1", "t2")
        _ok, _m, first = mgr.claim_sample("RM-1", "FI-1", "duty", "t3")
        ok, msg, second = mgr.claim_sample("RM-1", "FI-1", "duty", "t4")
        assert ok is True and second.record_id == first.record_id
        assert "复用" in msg
        assert len(mgr.rounds_of("FI-1")) == 1

    def test_bind_sniff_rejects_cross_rule_mix(self):
        mgr = _manager()
        mgr.create_migration("RM-1", "csv", "DR-OLD", "DR-NEW", COMMA_LINES, "op", "t1")
        mgr.activate("RM-1", "t2")
        _ok, _m, rnd = mgr.claim_sample("RM-1", "FI-1", "duty", "t3")
        # 同次嗅探：规则与轮次一致 → 允许
        ok, _m, bound = mgr.bind_sniff(
            rnd.record_id, "SS-1", column_count=1, columns=["whole"],
            column_positions=[0], first_row_is_header=False,
            parse_start_row=1, rule_id="DR-NEW", at="t4")
        assert ok is True and bound.state == "字段在途" and bound.sniff_id == "SS-1"
        # 另开一批，用旧规则嗅探却要绑新规则轮次 → 拒绝混用
        _ok, _m, rnd2 = mgr.claim_sample("RM-1", "FI-2", "duty", "t5")
        ok2, msg2, _r = mgr.bind_sniff(
            rnd2.record_id, "SS-2", column_count=4,
            columns=["a", "b", "c", "d"], column_positions=[0, 1, 2, 3],
            first_row_is_header=True, parse_start_row=2,
            rule_id="DR-OLD", at="t6")
        assert ok2 is False and "不得跨规则混用" in msg2
    def test_deliver_locks_confirmed_columns(self):
        mgr = _manager()
        mgr.create_migration("RM-1", "csv", "DR-OLD", "DR-NEW", SEMICOLON_LINES, "op", "t1")
        mgr.activate("RM-1", "t2")
        _ok, _m, rnd = mgr.claim_sample("RM-1", "FI-9", "duty", "t3")
        mgr.bind_sniff(rnd.record_id, "SS-9", column_count=4,
                       columns=["a", "b", "c", "d"], column_positions=[0, 1, 2, 3],
                       first_row_is_header=True, parse_start_row=2,
                       rule_id="DR-NEW", at="t4")
        ok, _m, delivered = mgr.deliver_fields(rnd.record_id, "t5")
        assert ok is True and delivered.confirmed is True
        # 已交付轮次不能再绑定嗅探或重复交付
        ok2, _m, _r = mgr.bind_sniff(
            rnd.record_id, "SS-X", 1, ["x"], [0], False, 1, "DR-NEW", "t6")
        assert ok2 is False
        ok3, _m, _r = mgr.deliver_fields(rnd.record_id, "t7")
        assert ok3 is False

    def test_rollback_restores_old_rule_and_keeps_preview(self):
        mgr = _manager()
        mgr.create_migration("RM-1", "csv", "DR-OLD", "DR-NEW", COMMA_LINES, "op", "t1")
        mgr.activate("RM-1", "t2")
        _ok, _m, rnd = mgr.claim_sample("RM-1", "FI-1", "duty", "t3")
        mgr.bind_sniff(rnd.record_id, "SS-1", column_count=1, columns=["x"],
                       column_positions=[0], first_row_is_header=False,
                       parse_start_row=1, rule_id="DR-NEW", at="t4")
        ok, msg, order = mgr.rollback("RM-1", "分号上线中断", "t5")
        assert ok is True and "回滚" in msg
        assert mgr.get_rule("DR-OLD").status == "已生效"
        assert mgr.get_rule("DR-NEW").status == "已停用"
        # 在途轮次回滚，两份预览仍保留
        assert mgr.rounds_of("FI-1")[0].state == "已回滚"
        assert order.old_preview and order.new_preview
        assert order.status == "已回滚"

    def test_retry_without_rollback_rejected(self):
        mgr = _manager()
        mgr.create_migration("RM-1", "csv", "DR-OLD", "DR-NEW", COMMA_LINES, "op", "t1")
        mgr.activate("RM-1", "t2")
        mgr.interrupt("RM-1", "中断待重试", "t3")
        ok, msg, _o = mgr.retry("RM-1", "t4")
        assert ok is False and "先回滚" in msg
        mgr.rollback("RM-1", "先回滚", "t5")
        ok2, _m, order = mgr.retry("RM-1", "t6")
        assert ok2 is True and order.status == "已启用"

    def test_restore_comma_keeps_confirmed_old_columns(self):
        mgr = _manager()
        mgr.create_migration("RM-1", "csv", "DR-OLD", "DR-NEW", COMMA_LINES, "op", "t1")
        mgr.activate("RM-1", "t2")
        # 旧规则轮次已确认 4 列（历史批次）
        old_round = AdoptionRound(
            "BRV-FI-7-001", "FI-7", "DR-OLD", "v1.4.0", 1,
            state="已交付", column_count=4,
            columns=["rec_id", "rec_timestamp", "data_value", "payload_bytes"],
            first_row_is_header=True, parse_start_row=2, confirmed=True)
        mgr._adoptions["FI-7"] = [old_round]
        # 新规则在途轮次
        ok, _m, new_round = mgr.claim_sample("RM-1", "FI-7", "duty", "t3")
        assert ok is True and new_round.rule_id == "DR-NEW"
        # 恢复逗号：续接旧轮已确认的 4 列，不替换不清空
        ok2, msg2, restored = mgr.restore_comma("FI-7", "duty", "t4")
        assert ok2 is True and restored.rule_id == "DR-OLD"
        assert restored.column_count == 4
        assert restored.columns == old_round.columns
        assert old_round.confirmed is True and old_round.columns[0] == "rec_id"
        assert "续接原列" in msg2

    def test_restore_comma_without_confirmed_opens_fresh_round(self):
        mgr = _manager()
        mgr.create_migration("RM-1", "csv", "DR-OLD", "DR-NEW", COMMA_LINES, "op", "t1")
        mgr.activate("RM-1", "t2")
        _ok, _m, new_round = mgr.claim_sample("RM-1", "FI-8", "duty", "t3")
        mgr.bind_sniff(new_round.record_id, "SS-8", column_count=1, columns=["x"],
                       column_positions=[0], first_row_is_header=False,
                       parse_start_row=1, rule_id="DR-NEW", at="t4")
        ok, _m, restored = mgr.restore_comma("FI-8", "duty", "t5")
        assert ok is True and restored.rule_id == "DR-OLD"
        assert restored.column_count == 0  # 旧列未确认，待重新嗅探
        # 新规则在途列未被清空
        assert new_round.column_count == 1

    def test_effective_adoption_and_parse_fields_keep_history(self):
        mgr = _manager()
        mgr.create_migration("RM-1", "csv", "DR-OLD", "DR-NEW", SEMICOLON_LINES, "op", "t1")
        mgr.activate("RM-1", "t2")
        # 新规则在途轮次（字段尚未交付的当前请求）
        _ok, _m, rnd = mgr.claim_sample("RM-1", "FI-1", "duty", "t3")
        mgr.bind_sniff(rnd.record_id, "SS-NEW", column_count=4,
                       columns=["rec_id", "rec_timestamp", "data_value", "payload_bytes"],
                       column_positions=[0, 1, 2, 3],
                       first_row_is_header=True, parse_start_row=2,
                       rule_id="DR-NEW", at="t4")
        # 历史已确认旧轮保留回查
        mgr._adoptions["FI-1"].insert(0, AdoptionRound(
            "BRV-FI-1-001", "FI-1", "DR-OLD", "v1.4.0", 1,
            state="已交付", column_count=4, confirmed=True,
            first_row_is_header=True, parse_start_row=2, sniff_id="SS-OLD"))
        effective = mgr.effective_adoption("FI-1")
        assert effective.record_id == rnd.record_id and effective.rule_id == "DR-NEW"
        ok, _m, detail = mgr.parse_fields("FI-1", SEMICOLON_LINES[1:])
        assert ok is True
        assert detail["rows"][0]["cells"] == ["R-1", "2026-09-01 08:00:00", "100.5", "1024"]
        assert detail["parse_start_row"] == 2
        # 已确认旧轮仍可回查
        assert detail["prior_confirmed"] == ["BRV-FI-1-001"]

    def test_parse_fields_blocked_without_effective_adoption(self):
        mgr = _manager()
        ok, msg, detail = mgr.parse_fields("FI-NOPE", COMMA_LINES)
        assert ok is False and detail is None and "有效采用版本" in msg

    def test_publish_board_splits_claimed_and_pending(self):
        mgr = _manager()
        mgr.create_migration("RM-1", "csv", "DR-OLD", "DR-NEW", COMMA_LINES, "op", "t1")
        mgr.activate("RM-1", "t2")
        mgr.claim_sample("RM-1", "FI-CLAIMED", "duty", "t3")
        board = mgr.publish_board()
        assert {r["batch_id"] for r in board["claimed_samples"]} == {"FI-CLAIMED"}
        # 已生效新规则命中但无在途轮次的批次属于待处理新批
        mgr.get_rule("DR-NEW").hit_batches.append("FI-PENDING")
        board2 = mgr.publish_board()
        assert any(p["batch_id"] == "FI-PENDING" for p in board2["pending_new_batches"])

    def test_operator_guidance_by_server_eligibility(self):
        mgr = _manager()
        mgr.create_migration("RM-1", "csv", "DR-OLD", "DR-NEW", COMMA_LINES, "op", "t1")
        mgr.activate("RM-1", "t2")
        _ok, _m, rnd = mgr.claim_sample("RM-1", "FI-1", "duty", "t3")
        # 回滚后旧规则生效、新规则在途轮次关闭 → 引导重选合格规则
        mgr.rollback("RM-1", "中断", "t4")
        guide = mgr.operator_guidance("FI-1", "duty")
        assert guide["eligible"] is False and guide["direction"] == "重新选择合格规则"
        assert "DR-OLD" in guide["qualified_rules"]
        # 旧规则上的在途轮次 → 引导继续旧批
        mgr._adoptions["FI-2"] = [AdoptionRound(
            "BRV-FI-2-001", "FI-2", "DR-OLD", "v1.4.0", 1, state="已领取样本")]
        guide2 = mgr.operator_guidance("FI-2", "duty")
        assert guide2["eligible"] is True and guide2["direction"] == "继续旧批"

    def test_evidence_openable_by_batch_and_rule(self):
        mgr = _manager()
        mgr.create_migration("RM-1", "csv", "DR-OLD", "DR-NEW", COMMA_LINES, "op", "t1")
        mgr.activate("RM-1", "t2")
        mgr.claim_sample("RM-1", "FI-1", "duty", "t3")
        by_batch = mgr.evidence(batch_id="FI-1")
        assert len(by_batch) == 1 and by_batch[0]["migration_status"] == "已启用"
        by_rule = mgr.evidence(rule_id="DR-NEW")
        assert all(r["rule_id"] == "DR-NEW" for r in by_rule) and len(by_rule) == 1


# ======================================================================
# 管线层：同次嗅探守卫与历史引号/转义兼容
# ======================================================================

class TestSnifferVersionGuard:
    def test_sniff_carries_rule_id_and_positions(self):
        sniffer = StructureSniffer()
        sniffer.register_rule(DelimiterRule("DR-1", "csv", ",", '"', "\\"))
        _ok, result = sniffer.sniff("FI-1", "csv", COMMA_LINES)
        assert result.rule_id == "DR-1"
        assert result.column_positions == [0, 1, 2, 3]
        assert result.parse_start_row == 2

    def test_sniff_with_explicit_rule_locks_version(self):
        sniffer = StructureSniffer()
        sniffer.register_rule(DelimiterRule("DR-COMMA", "csv", ",", '"', "\\"))
        sniffer.register_rule(DelimiterRule("DR-SEMI", "csv", ";", '"', "\\"))
        _ok, result = sniffer.sniff_with_rule("FI-1", "DR-SEMI", COMMA_LINES)
        assert result.rule_id == "DR-SEMI"
        assert result.column_count == 1  # 逗号文本在分号规则下是单列

    def test_same_sniff_guard_blocks_rule_switch(self):
        sniffer = StructureSniffer()
        sniffer.register_rule(DelimiterRule("DR-1", "csv", ",", '"', "\\"))
        sniffer.sniff("FI-1", "csv", COMMA_LINES)
        ok, _m = sniffer.guard_same_sniff("FI-1", "DR-1")
        assert ok is True
        ok2, msg = sniffer.guard_same_sniff("FI-1", "DR-OTHER")
        assert ok2 is False and "不得混用" in msg

    def test_legacy_quoted_escaped_samples_reproduce_columns_and_values(self):
        """旧逗号规则读带引号样本：引号包裹的逗号不切列、双写引号与反斜杠转义按历史行为还原。"""
        sniffer = StructureSniffer()
        sniffer.register_rule(DelimiterRule("DR-1", "csv", ",", '"', "\\"))
        _ok, result = sniffer.sniff("FI-Q", "csv", QUOTED_LINES)
        assert result.column_count == 3
        rule = sniffer.get_active_rule("csv")
        rows = [rule.split_line(line) for line in QUOTED_LINES]
        assert rows[1] == ["R-1", "hello, world", "100.5"]
        assert rows[2] == ["R-2", 'say "hi"', "200.5"]
        assert rows[3] == ["R-3", "a,b", "300.5"]


# ======================================================================
# 服务层：真实内存仓库的迁移编排
# ======================================================================

@pytest.fixture
def fresh_store():
    store.reload_seed()
    yield store
    store.reload_seed()


class TestRuleMigrationService:
    def _create(self, mgr_svc, migration_no="RM-T1"):
        return mgr_svc.create_migration(
            migration_no, "csv", "DR-001", "DR-003", COMMA_LINES, "op-1", "2026-09-10T08:00:00")

    def test_create_preview_persisted_and_openable(self, fresh_store):
        svc = SERVICES["rule_migration"]
        ok, msg, order = self._create(svc)
        assert ok is True and "预览" in msg
        detail = svc.preview_detail("RM-T1")
        assert detail["旧预览列数"] == "4"
        assert detail["新预览列数"] == "1"
        assert detail["旧规则预览"][1][2] == "100.5"

    def test_activate_rollback_rule_status_flow(self, fresh_store):
        svc = SERVICES["rule_migration"]
        rule_svc = SERVICES["delimiter_rule"]
        self._create(svc)
        ok, _m, _o = svc.activate("RM-T1", "2026-09-10T09:00:00")
        assert ok is True
        assert rule_svc.find_rule("DR-001")["status"] == "被替代"
        assert rule_svc.find_rule("DR-003")["status"] == "已生效"
        ok2, _m, _o2 = svc.rollback("RM-T1", "上线中断", "2026-09-10T10:00:00")
        assert ok2 is True
        assert rule_svc.find_rule("DR-001")["status"] == "已生效"
        assert rule_svc.find_rule("DR-003")["status"] == "已停用"
        # 回滚后预览仍在
        assert svc.preview_detail("RM-T1")["旧规则预览"]

    def test_claim_bind_deliver_full_round(self, fresh_store):
        svc = SERVICES["rule_migration"]
        version_svc = SERVICES["batch_rule_version"]
        self._create(svc)
        svc.activate("RM-T1", "t2")
        ok, _m, record = svc.claim_sample("RM-T1", "FI-MIG-1", "duty-1", "t3")
        assert ok is True and record["采用状态"] == "已领取样本"
        # 为该批建一条与新规则 DR-003 同次的嗅探
        sniff_svc = SERVICES["schema_sniff"]
        sniff_svc.sniff_structure(
            "SS-MIG-1", "FI-MIG-1",
            [["whole"], ["R-1,2026,100.5,1024"]], "DR-003")
        ok2, _m, bound = svc.bind_sniff(record["版本记录编号"], "SS-MIG-1", "t4")
        assert ok2 is True and bound["采用状态"] == "字段在途"
        assert bound["结构嗅探编号"] == "SS-MIG-1"
        ok3, _m, delivered = svc.deliver_fields(record["版本记录编号"], "t5")
        assert ok3 is True and delivered["采用状态"] == "已交付"
        assert version_svc.confirmed_rounds("FI-MIG-1")[0]["版本记录编号"] == record["版本记录编号"]

    def test_bind_sniff_blocks_cross_rule(self, fresh_store):
        svc = SERVICES["rule_migration"]
        self._create(svc)
        svc.activate("RM-T1", "t2")
        _ok, _m, record = svc.claim_sample("RM-T1", "FI-MIG-2", "duty", "t3")
        # 嗅探命中 DR-001（旧规则），轮次采用 DR-003（新规则）→ 拒绝
        SERVICES["schema_sniff"].sniff_structure(
            "SS-MIG-2", "FI-MIG-2",
            [["a", "b"], ["1", "2"]], "DR-001")
        ok, msg, _r = svc.bind_sniff(record["版本记录编号"], "SS-MIG-2", "t4")
        assert ok is False and "不得混用" in msg

    def test_rollback_marks_inflight_rounds_but_keeps_delivered(self, fresh_store):
        svc = SERVICES["rule_migration"]
        version_svc = SERVICES["batch_rule_version"]
        self._create(svc)
        svc.activate("RM-T1", "t2")
        # 在途批
        _ok, _m, inflight = svc.claim_sample("RM-T1", "FI-INFLIGHT", "duty", "t3")
        # 已交付批
        _ok, _m, delivered_rec = svc.claim_sample("RM-T1", "FI-DONE", "duty", "t3")
        SERVICES["schema_sniff"].sniff_structure(
            "SS-DONE", "FI-DONE", [["x"], ["1"]], "DR-003")
        svc.bind_sniff(delivered_rec["版本记录编号"], "SS-DONE", "t4")
        svc.deliver_fields(delivered_rec["版本记录编号"], "t5")
        svc.rollback("RM-T1", "中断", "t6")
        assert version_svc.find_record(inflight["版本记录编号"])["采用状态"] == "已回滚"
        assert version_svc.find_record(delivered_rec["版本记录编号"])["采用状态"] == "已交付"

    def test_parse_fields_uses_effective_adoption_and_start_row(self, fresh_store):
        svc = SERVICES["rule_migration"]
        self._create(svc)
        svc.activate("RM-T1", "t2")
        _ok, _m, record = svc.claim_sample("RM-T1", "FI-PARSE", "duty", "t3")
        # DR-003 是分号规则，给分号样本做同次嗅探（首行表头 → 起始行 2）
        SERVICES["schema_sniff"].sniff_structure(
            "SS-PARSE", "FI-PARSE",
            [["a", "b", "c"], ["1", "2", "3"], ["4", "5", "6"]], "DR-003")
        svc.bind_sniff(record["版本记录编号"], "SS-PARSE", "t4")
        ok, _m, detail = svc.parse_fields("FI-PARSE", ["1;2;3", "4;5;6"])
        assert ok is True
        assert detail["起始行"] == 2
        assert detail["rows"][0]["row_num"] == 2
        assert detail["rows"][0]["cells"] == ["1", "2", "3"]
        assert detail["规则编号"] == "DR-003"

    def test_parse_fields_blocked_without_adoption(self, fresh_store):
        svc = SERVICES["rule_migration"]
        ok, msg, detail = svc.parse_fields("FI-NO-SUCH", ["a,b"])
        assert ok is False and detail is None and "有效采用版本" in msg

    def test_restore_comma_does_not_replace_confirmed_columns(self, fresh_store):
        svc = SERVICES["rule_migration"]
        version_svc = SERVICES["batch_rule_version"]
        # seed: FI-20260901001 已有 DR-001 已交付 4 列
        confirmed = version_svc.confirmed_round_on_rule("FI-20260901001", "DR-001")
        assert confirmed is not None and confirmed["确认列数"] == "4"
        # 建迁移并启用，再开新规则轮次
        self._create(svc, "RM-RC")
        svc.activate("RM-RC", "t2")
        svc.claim_sample("RM-RC", "FI-20260901001", "duty", "t3")
        ok, msg, restored = svc.restore_comma("FI-20260901001", "duty", "t4")
        assert ok is True and restored["规则编号"] == "DR-001"
        assert restored["确认列数"] == "4"
        assert restored["列名清单"] == confirmed["列名清单"]
        assert "不替换不清空" in restored["操作留痕"]
        # 原已确认轮次原列不变
        assert version_svc.confirmed_round_on_rule(
            "FI-20260901001", "DR-001")["列名清单"] == confirmed["列名清单"]

    def test_publish_board_partitions_claimed_and_pending(self, fresh_store):
        svc = SERVICES["rule_migration"]
        board = svc.publish_board()
        claimed_batches = {r["关联批次"] for r in board["claimed_samples"]}
        # seed 中 FI-20260902002 是已领取样本（在途）
        assert "FI-20260902002" in claimed_batches
        pending_batches = {p["关联批次"] for p in board["pending_new_batches"]}
        assert "FI-20260902002" not in pending_batches

    def test_operator_guidance_after_rollback(self, fresh_store):
        svc = SERVICES["rule_migration"]
        # seed: FI-20260902003 的新规则轮次已回滚
        guide = svc.operator_guidance("FI-20260902003", "duty-2")
        assert guide["eligible"] is False
        assert guide["direction"] == "重新选择合格规则"
        assert "DR-006" in guide["qualified_rules"] or "DR-001" in guide["qualified_rules"]

    def test_evidence_filterable_from_both_pages(self, fresh_store):
        svc = SERVICES["rule_migration"]
        by_batch = svc.evidence(intake_batch="FI-20260902002")
        assert len(by_batch) == 1 and by_batch[0]["迁移状态"] == "已启用"
        by_rule = svc.evidence(rule_no="DR-006")
        assert all(r["规则编号"] == "DR-006" for r in by_rule) and len(by_rule) >= 2

    def test_retry_requires_prior_rollback(self, fresh_store):
        svc = SERVICES["rule_migration"]
        self._create(svc, "RM-RETRY")
        svc.activate("RM-RETRY", "t2")
        svc.interrupt("RM-RETRY", "中断", "t3")
        ok, msg, _o = svc.retry("RM-RETRY", "t4")
        assert ok is False and "先回滚" in msg

    def test_quoted_sample_preview_reproduces_history(self, fresh_store):
        """旧逗号规则预览带引号样本：分列与取值按历史引号/转义规则重现。"""
        rule_svc = SERVICES["delimiter_rule"]
        ok, _m, detail = rule_svc.preview("DR-001", QUOTED_LINES)
        assert ok is True and detail["column_count"] == 3
        assert detail["preview_rows"][1] == ["R-1", "hello, world", "100.5"]
        assert detail["preview_rows"][2] == ["R-2", 'say "hi"', "200.5"]
        assert detail["preview_rows"][3] == ["R-3", "a,b", "300.5"]


# ======================================================================
# 路由层：直接调用路由函数（无 httpx 环境）
# ======================================================================

class TestMigrationRouter:
    def test_preview_and_order_endpoints(self, fresh_store):
        prev = migration_router.preview_rule("DR-001", SamplePayload(sample_lines=QUOTED_LINES))
        assert prev.ok is True and prev.entry["column_count"] == 3

        created = migration_router.create_order(MigrationCreatePayload(
            migration_no="RM-API-1", fmt="csv", old_rule_no="DR-001",
            new_rule_no="DR-003", sample_lines=COMMA_LINES,
            operator="op", at="2026-09-10T08:00:00"))
        assert created.ok is True
        detail = migration_router.order_preview("RM-API-1")
        assert detail["ok"] is True and detail["detail"]["新预览列数"] == "1"

        activated = migration_router.activate_order(
            "RM-API-1", MigrationActionPayload(at="t2"))
        assert activated.ok is True
        rolled = migration_router.rollback_order(
            "RM-API-1", MigrationActionPayload(reason="中断回滚", at="t3"))
        assert rolled.ok is True and "保留" in rolled.message

    def test_claim_bind_restore_guidance_endpoints(self, fresh_store):
        migration_router.create_order(MigrationCreatePayload(
            migration_no="RM-API-2", fmt="csv", old_rule_no="DR-001",
            new_rule_no="DR-003", sample_lines=COMMA_LINES, operator="op", at="t1"))
        migration_router.activate_order("RM-API-2", MigrationActionPayload(at="t2"))
        claim = migration_router.claim_sample(ClaimPayload(
            migration_no="RM-API-2", intake_batch="FI-API-1", operator="duty", at="t3"))
        assert claim.ok is True
        record_no = claim.entry["版本记录编号"]
        SERVICES["schema_sniff"].sniff_structure(
            "SS-API-1", "FI-API-1", [["x"], ["1,2"]], "DR-003")
        bound = migration_router.bind_sniff(BindPayload(
            record_no=record_no, sniff_no="SS-API-1", at="t4"))
        assert bound.ok is True
        parsed = migration_router.parse_fields(ParsePayload(
            intake_batch="FI-API-1", data_lines=["1,2"]))
        assert parsed.ok is True and parsed.entry["rows"][0]["cells"] == ["1,2"]

        restored = migration_router.restore_comma(RestorePayload(
            intake_batch="FI-API-1", operator="duty", at="t5"))
        assert restored.ok is True and restored.entry["规则编号"] == "DR-001"
        guidance = migration_router.operator_guidance(GuidancePayload(
            intake_batch="FI-API-1", operator="duty"))
        assert "direction" in guidance
        board = migration_router.publish_board()
        assert "claimed_samples" in board and "pending_new_batches" in board
        evidence = migration_router.evidence(intake_batch="FI-API-1")
        assert len(evidence["items"]) >= 2

    def test_deliver_endpoint_locks_round(self, fresh_store):
        migration_router.create_order(MigrationCreatePayload(
            migration_no="RM-API-3", fmt="csv", old_rule_no="DR-001",
            new_rule_no="DR-003", sample_lines=COMMA_LINES, operator="op", at="t1"))
        migration_router.activate_order("RM-API-3", MigrationActionPayload(at="t2"))
        claim = migration_router.claim_sample(ClaimPayload(
            migration_no="RM-API-3", intake_batch="FI-API-3", operator="duty", at="t3"))
        SERVICES["schema_sniff"].sniff_structure(
            "SS-API-3", "FI-API-3", [["x"], ["1"]], "DR-003")
        migration_router.bind_sniff(BindPayload(
            record_no=claim.entry["版本记录编号"], sniff_no="SS-API-3", at="t4"))
        delivered = migration_router.deliver_fields(
            claim.entry["版本记录编号"], RecordActionPayload(at="t5"))
        assert delivered.ok is True and delivered.entry["采用状态"] == "已交付"


# ======================================================================
# 端到端：值班场景——新分号规则部署、旧回放器未交付、回滚重交、两版可对照
# ======================================================================

class TestEndToEndDutyScenario:
    def test_full_duty_scenario_two_filter_bases_and_next_action(self, fresh_store):
        svc = SERVICES["rule_migration"]
        rule_svc = SERVICES["delimiter_rule"]
        version_svc = SERVICES["batch_rule_version"]
        batch = "FI-DUTY-1"

        # 1) 旧逗号规则读取带引号样本，准备分号版迁移单并从嗅探页领取一次
        ok, _m, preview = rule_svc.preview("DR-001", QUOTED_LINES)
        assert ok is True and preview["column_count"] == 3
        ok, _m, order = svc.create_migration(
            "RM-DUTY", "csv", "DR-001", "DR-003", QUOTED_LINES, "admin", "t1")
        assert ok is True
        svc.activate("RM-DUTY", "t2")
        ok, _m, claimed = svc.claim_sample("RM-DUTY", batch, "on-duty-01", "t3")
        assert ok is True and claimed["采用状态"] == "已领取样本"

        # 2) 字段未交付前中断并回滚，再重新提交发布
        svc.interrupt("RM-DUTY", "旧回放器字段未交付", "t4")
        ok, _m, _o = svc.retry("RM-DUTY", "t4.5")  # 未回滚先重试 → 拒绝
        assert ok is False
        ok, _m, _o = svc.rollback("RM-DUTY", "分号上线中断", "t5")
        assert ok is True
        ok, _m, _o = svc.retry("RM-DUTY", "t6")  # 回滚后重交发布
        assert ok is True

        # 3) 重看两个版本的实际列数与确认范围：已确认旧列不变，在途请求下一步可操作
        detail = svc.preview_detail("RM-DUTY")
        assert detail["旧预览列数"] == "3"
        assert detail["新预览列数"] == "1"
        rounds = version_svc.rounds_of_batch(batch)
        assert rounds[0]["采用状态"] == "已回滚"  # 在途轮次随回滚关闭
        # 同批两份筛选依据可对照（证据可打开）
        evidence = svc.evidence(intake_batch=batch)
        assert len(evidence) == 1 and evidence[0]["规则编号"] == "DR-003"

        # 4) 服务端资格：在途轮次已回滚 → 引导重新选择合格规则
        guide = svc.operator_guidance(batch, "on-duty-01")
        assert guide["eligible"] is False
        assert guide["direction"] == "重新选择合格规则"

        # 5) 旧逗号规则的原列与值仍可按历史规则重现
        ok, _m, legacy = rule_svc.preview("DR-001", QUOTED_LINES)
        assert legacy["preview_rows"][1] == ["R-1", "hello, world", "100.5"]

    def test_confirmed_old_columns_survive_reparse_round(self, fresh_store):
        """恢复逗号解析时已认可列不被再次替换或清空，且旧轮可回查。"""
        svc = SERVICES["rule_migration"]
        version_svc = SERVICES["batch_rule_version"]
        batch = "FI-20260901001"
        original = version_svc.confirmed_round_on_rule(batch, "DR-001")
        original_columns = original["列名清单"]

        svc.create_migration("RM-KEEP", "csv", "DR-001", "DR-003", COMMA_LINES, "op", "t1")
        svc.activate("RM-KEEP", "t2")
        svc.claim_sample("RM-KEEP", batch, "duty", "t3")
        ok, _m, restored = svc.restore_comma(batch, "duty", "t4")
        assert ok is True
        # 已确认旧轮原列原样保留
        assert version_svc.confirmed_round_on_rule(batch, "DR-001")["列名清单"] == original_columns
        # 新轮续接同样的列
        assert restored["列名清单"] == original_columns
        # 历史已确认轮次仍在回查清单里
        assert any(r["版本记录编号"] == original["版本记录编号"]
                   for r in version_svc.confirmed_rounds(batch))
