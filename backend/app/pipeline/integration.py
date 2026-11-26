"""端到端解析管线编排器：连接全部管线组件的跨模块数据链路。

10 条贯通链路：
1. 文件投递→格式探测→解析路由→字段解析→解析批次闭合→输出契约→结果投递
2. 编码归一(不可映射字符)→字段解析→坏行隔离→复核回补→批次行数回补
3. 结构嗅探(首行是否表头)→分隔规则命中→字段解析起始行改写
4. 嵌套展平(数组展开行倍增)→取值转型(失败进坏行)→单位归一→输出契约
5. 抽取模板过渡(新旧并存)→按批次时间命中版本→模板差异→重解析位点范围
6. 重复指纹(内容/结构指纹)→命中重复→结果投递阻断
7. 抽样复核(取值偏差超限)→阻断输出契约→触发重解析任务
8. 解析水位(全部批次闭合才推进)→重解析覆盖策略(旧投递保留 vs 被替代)
9. 解析指标结算(隔离率来自坏行隔离真实计数、回退率来自解析路由真实回退)
10. 解析路由首选失败→备选回退→回退成功进入字段解析 / 耗尽判定路由失败
"""
from __future__ import annotations

from typing import Any, Callable

from app.pipeline.format_probe_chain import FormatProbeChain
from app.pipeline.parser_route_engine import ParserRouteEngine
from app.pipeline.field_parse_engine import FieldParseEngine
from app.pipeline.quarantine_manager import QuarantineManager
from app.pipeline.output_contract_validator import OutputContractValidator
from app.pipeline.result_deliver_manager import ResultDeliverManager
from app.pipeline.watermark_and_reparse import WatermarkAndReparse
from app.pipeline.fingerprint_dedup import FingerprintDedup
from app.pipeline.structure_sniffer import StructureSniffer
from app.pipeline.transform_stage import TransformStage
from app.pipeline.template_version import TemplateVersionManager
from app.pipeline.review_metric import ReviewMetricEngine
from app.store import store


def _next_id(module: str) -> int:
    rows = store.rows(module)
    return max((int(r.get("id", 0)) for r in rows), default=0) + 1


def _append_row(module: str, values: dict[str, Any], *, status: str,
                pending: bool, abnormal: bool = False) -> dict[str, Any]:
    """向内存仓库追加一行，字段名与 spec 保持一致。"""
    row = {"id": _next_id(module), "status": status,
           "pending": pending, "abnormal": abnormal}
    row.update(values)
    store.rows(module).append(row)
    return row


class ParsePipelineOrchestrator:
    """解析管线编排器：执行 10 条业务链路并记录执行轨迹。"""

    def __init__(self) -> None:
        self.probe = FormatProbeChain()
        self.route = ParserRouteEngine()
        self.parse = FieldParseEngine()
        self.quarantine = QuarantineManager()
        self.contract = OutputContractValidator()
        self.deliver = ResultDeliverManager()
        self.watermark = WatermarkAndReparse()
        self.dedup = FingerprintDedup()
        self.sniffer = StructureSniffer()
        self.transform = TransformStage()
        self.templates = TemplateVersionManager()
        self.review = ReviewMetricEngine()
        self.audit_log: list[dict[str, Any]] = []
        self._run_id = 0

    def _log(self, link: str, step: str, outcome: str, detail: Any = None) -> None:
        self._run_id += 1
        self.audit_log.append({
            "run_id": self._run_id, "link": link, "step": step,
            "outcome": outcome, "detail": detail,
        })

    # ------------------------------------------------------------------
    # 链路1：投递→探测→路由→字段解析→批次闭合→契约→投递
    # ------------------------------------------------------------------
    def link1_intake_to_deliver(
        self, batch_id: str, channel: str, filename: str, magic_hex: str,
        declared_format: str, size_bytes: int, arrive_time: str,
        parser_chain: list[str], template_id: str,
        field_defs: list[dict[str, Any]], raw_lines: list[dict[str, str]],
        contract_columns: list[str], mandatory: list[str], min_rows: int,
        target_path: str, output_format: str, deliver_time: str,
    ) -> dict[str, Any]:
        link = "intake-to-deliver"
        _append_row("file_intake", {
            "批次编号": batch_id, "来源渠道": channel, "原始文件名": filename,
            "字节大小": str(size_bytes), "投递时间": arrive_time,
            "文件后缀": filename[filename.rfind("."):], "声明格式": declared_format,
            "接收节点": "node-01",
        }, status="已接收", pending=True)

        ok, msg, probe = self.probe.probe_format(batch_id, magic_hex, declared_format)
        self._log(link, "format_probe", "ok" if ok else f"FAIL:{msg}")
        resolved = self.probe.get_resolved_format(batch_id)
        _append_row("format_probe", {
            "探测编号": f"FP-{batch_id}", "关联批次": batch_id,
            "魔数特征": magic_hex, "推断格式": resolved,
            "置信度": f"{probe.confidence:.2f}" if probe else "0.00",
            "结构层级": "1", "是否压缩": "否", "探测耗时": "40",
        }, status="已确认" if ok else "存疑", pending=not ok, abnormal=not ok)

        self.route.register_parsers(resolved, parser_chain)
        routed, decision = self.route.route(
            f"PR-{batch_id}", batch_id, resolved,
            f"魔数特征+声明格式{declared_format}", arrive_time)
        self._log(link, "parser_route", "ok" if routed else "FAIL:no-parser")
        if not routed:
            _append_row("parse_batch", {
                "批次记录编号": f"PB-{batch_id}", "关联投递批次": batch_id,
                "解析器标识": "", "总行数": "0", "成功行数": "0",
                "隔离行数": "0", "批次状态": "无法解析", "完成时间": "",
            }, status="已作废", pending=False, abnormal=True)
            return {"link": link, "ok": False, "stage": "parser_route",
                    "message": f"格式 {resolved} 无可用解析器"}

        self.parse.register_template(template_id, field_defs)
        start_row = self.sniffer.parse_start_row(batch_id)
        success_rows = 0
        quarantined = 0
        for offset, raw in enumerate(raw_lines):
            row_num = start_row + offset
            result = self.parse.parse_row(batch_id, template_id, row_num, raw)
            if result.all_success:
                success_rows += 1
            else:
                quarantined += 1
                failed = [f.to_dict() for f in result.fields if not f.success]
                self.quarantine.quarantine_row(
                    batch_id, row_num, str(raw), failed)
        total_rows = len(raw_lines)
        self._log(link, "field_parse",
                  f"{success_rows}/{total_rows} rows ok, {quarantined} quarantined")

        batch_status = "已闭合" if quarantined == 0 else "存在隔离"
        _append_row("parse_batch", {
            "批次记录编号": f"PB-{batch_id}", "关联投递批次": batch_id,
            "解析器标识": decision.selected_parser if decision else "",
            "总行数": str(total_rows), "成功行数": str(success_rows),
            "隔离行数": str(quarantined), "批次状态": batch_status,
            "完成时间": deliver_time,
        }, status=batch_status, pending=quarantined > 0, abnormal=quarantined > 0)

        contract_id = f"OC-{batch_id}"
        self.contract.register_rule(contract_id, f"PB-{batch_id}",
                                    contract_columns, contract_columns,
                                    mandatory, min_rows)
        blocked_by_quarantine = self.quarantine.get_unreviewed_count(batch_id) > 0
        passed, check = self.contract.validate(
            contract_id, contract_columns, success_rows, deliver_time,
            has_unreviewed_quarantine=blocked_by_quarantine)
        _append_row("output_contract", {
            "契约编号": contract_id, "关联批次": f"PB-{batch_id}",
            "输出列集合": ",".join(contract_columns),
            "列顺序约定": ",".join(contract_columns),
            "必填列": ",".join(mandatory), "行数下限": str(min_rows),
            "契约校验结论": "通过" if passed else ";".join(check.failures),
            "校验时间": deliver_time,
        }, status="已通过" if passed else "未通过",
            pending=not passed, abnormal=not passed)
        self._log(link, "output_contract", "passed" if passed else "blocked")

        if not passed:
            return {"link": link, "ok": False, "stage": "output_contract",
                    "message": ";".join(check.failures),
                    "quarantined": quarantined, "success_rows": success_rows}

        dup_blocked, dup_msg = self.dedup.is_deliver_blocked(batch_id)
        if dup_blocked:
            self._log(link, "result_deliver", f"BLOCKED:{dup_msg}")
            return {"link": link, "ok": False, "stage": "result_deliver",
                    "message": dup_msg}

        record = self.deliver.create_deliver(
            f"RD-{batch_id}", contract_id, target_path, success_rows, output_format)
        delivered, dmsg, _rec = self.deliver.deliver(record.record_id, deliver_time)
        _append_row("result_deliver", {
            "投递记录编号": record.record_id, "关联契约": contract_id,
            "目标位置": target_path, "投递行数": str(success_rows),
            "投递格式": output_format, "重试次数": str(record.retry_count),
            "投递结论": record.conclusion, "投递时间": deliver_time,
        }, status="已送达" if delivered else "部分送达",
            pending=not delivered, abnormal=not delivered)
        self._log(link, "result_deliver", "ok" if delivered else f"FAIL:{dmsg}")
        return {"link": link, "ok": delivered, "stage": "result_deliver",
                "message": dmsg, "success_rows": success_rows,
                "quarantined": quarantined}

    # ------------------------------------------------------------------
    # 链路2：编码归一(不可映射字符)→字段解析→坏行隔离→复核回补
    # ------------------------------------------------------------------
    def link2_encoding_quarantine(
        self, batch_id: str, source_encoding: str, target_encoding: str,
        unmappable_count: int, replace_strategy: str, raw_lines: list[dict[str, str]],
        template_id: str, field_defs: list[dict[str, Any]], norm_time: str,
    ) -> dict[str, Any]:
        link = "encoding-quarantine"
        lossy = unmappable_count > 0
        _append_row("encoding_norm", {
            "转码编号": f"EN-{batch_id}", "关联批次": batch_id,
            "原始编码": source_encoding, "目标编码": target_encoding,
            "不可映射字符数": str(unmappable_count), "替换策略": replace_strategy,
            "转码后字节": str(sum(len(str(v)) for r in raw_lines for v in r.values())),
            "转码耗时": "180",
        }, status="存在损字" if lossy else "已完成",
            pending=lossy, abnormal=lossy)
        self._log(link, "encoding_norm",
                  f"{unmappable_count} unmappable chars, strategy={replace_strategy}")

        self.parse.register_template(template_id, field_defs)
        quarantined_rows: list[int] = []
        for offset, raw in enumerate(raw_lines):
            row_num = offset + 1
            result = self.parse.parse_row(batch_id, template_id, row_num, raw)
            if not result.all_success:
                failed = [f.to_dict() for f in result.fields if not f.success]
                self.quarantine.quarantine_row(batch_id, row_num, str(raw), failed)
                quarantined_rows.append(row_num)
        self._log(link, "field_parse_quarantine",
                  f"rows quarantined: {quarantined_rows}")

        return {"link": link, "ok": True,
                "lossy": lossy,
                "unmappable_count": unmappable_count,
                "quarantined_rows": quarantined_rows,
                "quarantine_rate": self.quarantine.quarantine_rate(
                    batch_id, len(raw_lines))}

    def reinstate_quarantined_row(
        self, batch_id: str, row_num: int, corrected: dict[str, Any],
        reviewer: str, review_time: str,
    ) -> tuple[bool, str]:
        """复核回补：更正取值后回补进批次，成功行数 +1、隔离行数 -1。"""
        ok = self.quarantine.review_row(batch_id, row_num, "回补", corrected)
        if not ok:
            return False, f"批次 {batch_id} 行 {row_num} 不在隔离清单中"

        for row in store.rows("parse_batch"):
            if row.get("关联投递批次") == batch_id:
                row["成功行数"] = str(int(row["成功行数"]) + 1)
                row["隔离行数"] = str(max(0, int(row["隔离行数"]) - 1))
                if int(row["隔离行数"]) == 0:
                    row["批次状态"] = "已闭合"
                    row["status"] = "已闭合"
                    row["pending"] = False
                    row["abnormal"] = False
                break

        _append_row("row_quarantine", {
            "隔离编号": f"RQ-{batch_id}-{row_num}", "关联批次": batch_id,
            "原始行号": str(row_num), "失败字段": ",".join(corrected.keys()),
            "失败原因": "解析失败已复核", "原始行文本": str(corrected),
            "复核结论": f"{reviewer} 确认回补", "隔离时间": review_time,
        }, status="已回补", pending=False)
        self._log("encoding-quarantine", "reinstate",
                  f"row {row_num} back to batch {batch_id}")
        return True, f"行 {row_num} 已回补进批次 {batch_id}"

    # ------------------------------------------------------------------
    # 链路3：结构嗅探(首行是否表头)→分隔规则→解析起始行改写
    # ------------------------------------------------------------------
    def link3_sniff_parse_start(
        self, batch_id: str, fmt: str, sample_lines: list[str],
        template_id: str, field_defs: list[dict[str, Any]],
        data_rows: list[dict[str, str]], sniff_time: str,
    ) -> dict[str, Any]:
        link = "sniff-parse-start"
        usable, sniff = self.sniffer.sniff(batch_id, fmt, sample_lines)
        self._log(link, "schema_sniff",
                  f"header={sniff.first_row_is_header} start_row={sniff.parse_start_row} "
                  f"confidence={sniff.confidence}")
        _append_row("schema_sniff", {
            "嗅探编号": f"SS-{batch_id}", "关联批次": batch_id,
            "样本行数": str(sniff.sample_rows), "推断列数": str(sniff.column_count),
            "列名清单": ",".join(sniff.column_names),
            "列类型推断": ";".join(sniff.column_types),
            "首行是否表头": "是" if sniff.first_row_is_header else "否",
            "嗅探置信度": f"{sniff.confidence:.2f}",
            "命中分隔规则": sniff.rule_id,
            "列位置": ",".join(str(p) for p in sniff.column_positions),
        }, status="已推断" if usable else "低置信",
            pending=not usable, abnormal=not usable)

        if not usable:
            return {"link": link, "ok": False, "stage": "schema_sniff",
                    "message": f"嗅探置信度 {sniff.confidence:.2f} 过低，解析不得启动",
                    "parse_start_row": sniff.parse_start_row}

        self.parse.register_template(template_id, field_defs)
        parsed_rows: list[int] = []
        for offset, raw in enumerate(data_rows):
            row_num = sniff.parse_start_row + offset
            self.parse.parse_row(batch_id, template_id, row_num, raw)
            parsed_rows.append(row_num)
        self._log(link, "field_parse_from_start",
                  f"parsed rows start at {sniff.parse_start_row}: {parsed_rows[:3]}…")
        return {"link": link, "ok": True,
                "first_row_is_header": sniff.first_row_is_header,
                "parse_start_row": sniff.parse_start_row,
                "parsed_row_numbers": parsed_rows}

    # ------------------------------------------------------------------
    # 链路4：嵌套展平→取值转型→单位归一→输出契约
    # ------------------------------------------------------------------
    def link4_flatten_cast_normalize(
        self, batch_id: str, channel: str, path_id: str,
        documents: list[dict[str, Any]], cast_rules: list[Any],
        contract_id: str, contract_columns: list[str], mandatory: list[str],
        min_rows: int, check_time: str,
    ) -> dict[str, Any]:
        link = "flatten-cast-normalize"
        ok, flat_rows, flatten_msg = self.transform.flatten_document(path_id, documents)
        self._log(link, "nested_flatten",
                  "ok" if ok else f"FAIL:{flatten_msg}")
        if not ok:
            return {"link": link, "ok": False, "stage": "nested_flatten",
                    "message": flatten_msg}

        for rule in cast_rules:
            self.transform.register_cast_rule(rule)
        cast_ok, cast_failed = self.transform.cast_rows(batch_id, flat_rows)
        for failure in cast_failed:
            self.quarantine.quarantine_row(
                batch_id, failure["row_index"] + 1,
                str(failure["raw_row"]),
                [{"field_name": "cast", "error_reason": r}
                 for r in failure["failed_fields"]])
        self._log(link, "value_cast",
                  f"{len(cast_ok)} ok, {len(cast_failed)} failed → quarantine")

        normalized, refused = self.transform.normalize_units(batch_id, channel, cast_ok)
        self._log(link, "unit_normalize",
                  f"{len(normalized)} rows normalized, {len(refused)} refused")

        self.contract.register_rule(contract_id, batch_id, contract_columns,
                                    contract_columns, mandatory, min_rows)
        actual_columns = sorted({
            k for row in normalized for k in row if not k.startswith("__")
        })
        passed, check = self.contract.validate(
            contract_id, actual_columns, len(normalized), check_time)
        self._log(link, "output_contract", "passed" if passed else "blocked")
        return {"link": link, "ok": passed,
                "flatten_msg": flatten_msg,
                "flat_rows": len(flat_rows),
                "cast_ok": len(cast_ok),
                "cast_failed": len(cast_failed),
                "normalized_rows": len(normalized),
                "unit_refused": refused,
                "contract_failures": check.failures,
                "message": ";".join(check.failures) if not passed else "契约通过"}

    # ------------------------------------------------------------------
    # 链路5：模板过渡(新旧并存)→命中版本→模板差异→重解析位点范围
    # ------------------------------------------------------------------
    def link5_tmpl_transition_reparse(
        self, old_tmpl: Any, new_tmpl: Any, transition_time: str,
        batch_times: dict[str, str], batch_positions: dict[str, str],
        reparse_task_id: str, coverage_strategy: str,
    ) -> dict[str, Any]:
        link = "tmpl-transition-reparse"
        self.templates.register_version(old_tmpl)
        self.templates.register_version(new_tmpl)
        old_tmpl.status = "已生效"
        old_tmpl.effective_from = old_tmpl.effective_from or "2026-01-01T00:00:00"

        ok, msg, coexisting = self.templates.start_transition(
            new_tmpl.template_id, transition_time)
        self._log(link, "tmpl_transition", msg)
        if not ok:
            return {"link": link, "ok": False, "stage": "transition", "message": msg}

        hit_versions: dict[str, str] = {}
        for batch_id, batch_time in batch_times.items():
            hit = self.templates.resolve_hit_version(
                new_tmpl.name, new_tmpl.fmt, batch_time)
            if hit:
                self.templates.record_hit(hit, batch_id)
                hit_versions[batch_id] = hit
        self._log(link, "version_hit", f"{hit_versions}")

        affected = [b for b, v in hit_versions.items() if v == old_tmpl.template_id]
        diff = self.templates.diff_versions(
            old_tmpl.template_id, new_tmpl.template_id, len(affected))
        _append_row("tmpl_diff", {
            "差异编号": f"TD-{old_tmpl.version}-{new_tmpl.version}",
            "旧模板版本": old_tmpl.version, "新模板版本": new_tmpl.version,
            "差异字段": ",".join(diff.field_list()) if diff else "",
            "差异类别": (
                f"新增{len(diff.added)},删除{len(diff.removed)},变更{len(diff.changed)}"
                if diff else ""
            ),
            "影响批次数": str(len(affected)),
            "修正方案": "按位点范围重解析" if affected else "无需重解析",
            "登记时间": transition_time,
        }, status="已收敛" if (diff and diff.converged) else "偏离超限",
            pending=not (diff and diff.converged),
            abnormal=not (diff and diff.converged))
        self._log(link, "tmpl_diff",
                  f"{diff.diff_field_count if diff else 0} fields differ, "
                  f"converged={diff.converged if diff else None}")

        if not affected:
            return {"link": link, "ok": True, "stage": "tmpl_diff",
                    "message": "过渡期内所有批次都命中新版本，无需重解析",
                    "hit_versions": hit_versions, "affected": []}

        affected_positions = {b: batch_positions[b] for b in affected
                              if b in batch_positions}
        planned, plan_msg, rng = self.templates.plan_reparse_range(
            reparse_task_id, diff, affected_positions)
        self._log(link, "reparse_range", plan_msg)
        if planned and rng:
            created, cmsg, _task = self.watermark.create_reparse(
                reparse_task_id, affected[0], f"模板 {old_tmpl.version}→{new_tmpl.version} 差异重解析",
                coverage_strategy, rng.start_position, rng.end_position,
                len(rng.affected_batches))
            _append_row("reparse_task", {
                "重解析编号": reparse_task_id, "关联批次": affected[0],
                "重解析原因": f"模板{old_tmpl.version}→{new_tmpl.version}差异{diff.diff_field_count}字段",
                "覆盖策略": coverage_strategy,
                "起始位点": rng.start_position, "截止位点": rng.end_position,
                "受影响投递数": str(len(rng.affected_batches)),
                "重解析结论": "",
            }, status="待执行", pending=True)
            self._log(link, "reparse_create", cmsg)
        return {"link": link, "ok": planned,
                "stage": "reparse_range",
                "message": plan_msg,
                "hit_versions": hit_versions,
                "affected": affected,
                "diff": diff.to_dict() if diff else None,
                "range": rng.to_dict() if rng else None}

    # ------------------------------------------------------------------
    # 链路6：重复指纹→命中重复→结果投递阻断
    # ------------------------------------------------------------------
    def link6_dup_fingerprint_deliver(
        self, record_id: str, batch_id: str, raw_lines: list[str],
        column_names: list[str], check_time: str,
        contract_id: str, target_path: str, row_count: int,
        output_format: str, deliver_time: str,
    ) -> dict[str, Any]:
        link = "dup-fingerprint-deliver"
        content_hash = FingerprintDedup.compute_content_hash(raw_lines)
        schema_hash = FingerprintDedup.compute_schema_hash(column_names)
        record = self.dedup.check_duplicate(record_id, batch_id,
                                            content_hash, schema_hash, check_time)
        _append_row("dup_fingerprint", {
            "指纹编号": record_id, "关联批次": batch_id,
            "内容指纹": content_hash, "结构指纹": schema_hash,
            "命中重复批次": record.duplicate_of,
            "重复判定": record.verdict, "去重处置": record.disposition,
            "判定时间": check_time,
        }, status={
            "唯一": "判定唯一", "内容重复": "确认重复",
            "结构相同内容不同": "疑似重复",
        }.get(record.verdict, "已计算"),
            pending=record.disposition == "待人工确认",
            abnormal=record.verdict == "内容重复")
        self._log(link, "dup_check",
                  f"verdict={record.verdict} disposition={record.disposition}")

        blocked, block_msg = self.dedup.is_deliver_blocked(batch_id)
        if blocked:
            self._log(link, "result_deliver", f"BLOCKED:{block_msg}")
            return {"link": link, "ok": False, "stage": "result_deliver",
                    "message": block_msg, "verdict": record.verdict,
                    "duplicate_of": record.duplicate_of, "delivered": False}

        deliver_record = self.deliver.create_deliver(
            f"RD-{batch_id}", contract_id, target_path, row_count, output_format)
        delivered, dmsg, _r = self.deliver.deliver(
            deliver_record.record_id, deliver_time)
        self._log(link, "result_deliver", dmsg)
        return {"link": link, "ok": delivered, "stage": "result_deliver",
                "message": dmsg, "verdict": record.verdict,
                "delivered": delivered}

    # ------------------------------------------------------------------
    # 链路7：抽样复核偏差超限→阻断输出契约→触发重解析
    # ------------------------------------------------------------------
    def link7_review_block_reparse(
        self, sample_id: str, batch_id: str, ratio: float, reviewer: str,
        parsed_rows: list[dict[str, Any]], raw_rows: list[dict[str, Any]],
        compare_fields: list[str], contract_id: str, contract_columns: list[str],
        mandatory: list[str], min_rows: int, review_time: str,
        reparse_task_id: str = "", coverage_strategy: str = "旧投递保留",
    ) -> dict[str, Any]:
        link = "review-block-reparse"
        sample = self.review.run_review(sample_id, batch_id, parsed_rows,
                                        raw_rows, ratio, reviewer, compare_fields)
        _append_row("sample_review", {
            "样本编号": sample_id, "关联批次": batch_id,
            "抽样比例": f"{ratio:.2f}", "抽样行数": str(sample.sampled_rows),
            "字段一致数": str(sample.consistent_count),
            "取值偏差数": str(sample.deviation_count),
            "复核结论": sample.conclusion, "复核人标识": reviewer,
        }, status=sample.status, pending=sample.status == "复核中",
            abnormal=sample.status == "已驳回")
        self._log(link, "sample_review",
                  f"deviation_rate={sample.deviation_rate:.2%} status={sample.status}")

        self.contract.register_rule(contract_id, batch_id, contract_columns,
                                    contract_columns, mandatory, min_rows)
        blocked, block_msg = self.review.is_batch_blocked_by_review(batch_id)
        passed, check = self.contract.validate(
            contract_id, contract_columns, len(parsed_rows), review_time,
            has_unreviewed_quarantine=blocked)
        self._log(link, "output_contract",
                  "blocked-by-review" if blocked else
                  ("passed" if passed else "blocked"))

        reparse_created = None
        if blocked and reparse_task_id:
            ok, msg, task = self.watermark.create_reparse(
                reparse_task_id, batch_id,
                f"抽样复核驳回:{sample.conclusion}", coverage_strategy,
                batch_id, batch_id, 1)
            reparse_created = task.to_dict() if task else None
            self._log(link, "reparse_trigger", msg)

        return {"link": link, "ok": passed,
                "deviation_rate": sample.deviation_rate,
                "review_status": sample.status,
                "contract_passed": passed,
                "contract_failures": check.failures,
                "blocked_msg": block_msg,
                "reparse_task": reparse_created}

    # ------------------------------------------------------------------
    # 链路8：水位推进(全部闭合)→重解析覆盖策略
    # ------------------------------------------------------------------
    def link8_watermark_reparse_coverage(
        self, channel: str, batch_ids: list[str], closed_batch_ids: list[str],
        watermark_id: str, advance_time: str,
        reparse_task_id: str = "", coverage_strategy: str = "",
        old_deliver_id: str = "", reason: str = "",
        start_position: str = "", end_position: str = "",
    ) -> dict[str, Any]:
        link = "watermark-reparse-coverage"
        wm = self.watermark.register_channel(channel, len(batch_ids))
        for batch_id in closed_batch_ids:
            if batch_id in batch_ids:
                self.watermark.mark_batch_closed(channel, batch_id)
        advanced, adv_msg = self.watermark.advance_watermark(
            channel, max(closed_batch_ids) if closed_batch_ids else "", advance_time)
        _append_row("parse_watermark", {
            "水位编号": watermark_id, "来源渠道": channel,
            "已解析至位点": wm.position, "待解析数量": str(wm.total_unparsed),
            "最近批次": closed_batch_ids[-1] if closed_batch_ids else "",
            "水位推进时间": advance_time,
            "滞后批次数": str(wm.lag_count), "滞后原因": wm.lag_reason,
        }, status="已推进" if advanced else "推进中断",
            pending=not advanced, abnormal=not advanced)
        self._log(link, "watermark_advance", adv_msg)

        reparse_result = None
        if reparse_task_id and coverage_strategy:
            ok, msg, task = self.watermark.create_reparse(
                reparse_task_id, batch_ids[0] if batch_ids else "",
                reason or "覆盖策略重解析", coverage_strategy,
                start_position or (batch_ids[0] if batch_ids else ""),
                end_position or (batch_ids[-1] if batch_ids else ""),
                len(batch_ids))
            if ok and old_deliver_id:
                self.watermark.register_coverage(
                    batch_ids[0] if batch_ids else "", coverage_strategy,
                    old_deliver_id)
            reparse_result = {"created": ok, "message": msg,
                              "strategy": coverage_strategy,
                              "old_deliver_kept": coverage_strategy == "旧投递保留"}
            self._log(link, "reparse_coverage", msg)

        return {"link": link, "ok": advanced,
                "watermark_advanced": advanced,
                "message": adv_msg,
                "unparsed_remaining": wm.total_unparsed,
                "reparse": reparse_result}

    # ------------------------------------------------------------------
    # 链路9：解析指标结算(隔离率/回退率来自真实计数)
    # ------------------------------------------------------------------
    def link9_metric_settle(
        self, reading_id: str, window: str, channel: str, batch_id: str,
        total_rows: int, parse_durations_ms: list[float], collected_at: str,
    ) -> dict[str, Any]:
        link = "metric-settle"
        quarantined_rows = len(self.quarantine.get_quarantined(batch_id))
        quarantine_rate = quarantined_rows / total_rows if total_rows > 0 else 0.0
        fallback_decisions, total_decisions = self.route.fallback_stats()
        reading = self.review.collect_metric(
            reading_id, window, channel, total_rows, parse_durations_ms,
            quarantined_rows, total_rows, fallback_decisions, total_decisions,
            collected_at)
        _append_row("parse_metric", {
            "指标读数编号": reading_id, "统计窗口": window, "来源渠道": channel,
            "解析吞吐行数": str(total_rows),
            "平均解析耗时": f"{reading.avg_parse_ms:.2f}",
            "隔离率": f"{reading.quarantine_rate:.4f}",
            "回退率": f"{reading.fallback_rate:.4f}",
            "采集时间": collected_at,
        }, status="已结算", pending=False)
        self._log(link, "metric_collect",
                  f"quarantine_rate={quarantine_rate:.4f} "
                  f"fallback_rate={reading.fallback_rate:.4f}")
        return {"link": link, "ok": True,
                "reading": reading.to_dict(),
                "quarantined_rows": quarantined_rows,
                "quarantine_rate": quarantine_rate,
                "fallback_decisions": fallback_decisions,
                "total_decisions": total_decisions}

    # ------------------------------------------------------------------
    # 链路10：首选解析器失败→备选回退→字段解析/路由失败
    # ------------------------------------------------------------------
    def link10_route_fallback_parse(
        self, decision_id: str, batch_id: str, resolved_format: str,
        parser_chain: list[str], try_parse: Callable[[str], bool],
        decide_time: str, template_id: str,
        field_defs: list[dict[str, Any]], raw_lines: list[dict[str, str]],
    ) -> dict[str, Any]:
        link = "route-fallback-parse"
        self.route.register_parsers(resolved_format, parser_chain)
        routed, _decision = self.route.route(
            decision_id, batch_id, resolved_format, "格式探测结论", decide_time)
        if not routed:
            return {"link": link, "ok": False, "stage": "route",
                    "message": f"格式 {resolved_format} 无解析器注册"}

        ok, msg, decision = self.route.execute_with_fallback(
            decision_id, try_parse, decide_time)
        _append_row("parser_route", {
            "决策编号": decision_id, "关联批次": batch_id,
            "推断格式": resolved_format,
            "命中解析器": decision.selected_parser if decision else "",
            "备选解析器": ",".join(decision.fallback_parsers) if decision else "",
            "路由依据": "首选失败按备选顺序回退" if (decision and decision.fallback_count) else "首选命中",
            "回退次数": str(decision.fallback_count if decision else 0),
            "决策时间": decide_time,
        }, status=decision.status if decision else "路由失败",
            pending=not ok, abnormal=not ok)
        self._log(link, "route_fallback", msg)

        if not ok:
            return {"link": link, "ok": False, "stage": "route_fallback",
                    "message": msg,
                    "fallback_count": decision.fallback_count if decision else 0}

        self.parse.register_template(template_id, field_defs)
        success = 0
        for offset, raw in enumerate(raw_lines):
            result = self.parse.parse_row(batch_id, template_id, offset + 1, raw)
            if result.all_success:
                success += 1
        self._log(link, "field_parse_after_fallback",
                  f"parser={decision.selected_parser} {success}/{len(raw_lines)} rows ok")
        return {"link": link, "ok": True,
                "selected_parser": decision.selected_parser,
                "fallback_count": decision.fallback_count,
                "success_rows": success,
                "message": msg}

    # ------------------------------------------------------------------
    def status_report(self) -> dict[str, Any]:
        """聚合全部管线组件的运行状态报告。"""
        return {
            "audit_log": self.audit_log,
            "probe": self.probe.snapshot(),
            "route": self.route.snapshot(),
            "parse": self.parse.snapshot(),
            "quarantine": self.quarantine.snapshot(),
            "contract": self.contract.snapshot(),
            "deliver": self.deliver.snapshot(),
            "watermark": self.watermark.snapshot(),
            "dedup": self.dedup.snapshot(),
            "sniffer": self.sniffer.snapshot(),
            "transform": self.transform.snapshot(),
            "templates": self.templates.snapshot(),
            "review": self.review.snapshot(),
        }
