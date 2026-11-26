"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.file_intake import FileIntakeService
from app.services.format_probe import FormatProbeService
from app.services.encoding_norm import EncodingNormService
from app.services.parser_route import ParserRouteService
from app.services.extract_tmpl import ExtractTmplService
from app.services.field_parse import FieldParseService
from app.services.row_quarantine import RowQuarantineService
from app.services.parse_batch import ParseBatchService
from app.services.schema_sniff import SchemaSniffService
from app.services.delimiter_rule import DelimiterRuleService
from app.services.batch_rule_version import BatchRuleVersionService
from app.services.rule_migration import RuleMigrationService
from app.services.nested_flatten import NestedFlattenService
from app.services.value_cast import ValueCastService
from app.services.unit_normalize import UnitNormalizeService
from app.services.output_contract import OutputContractService
from app.services.result_deliver import ResultDeliverService
from app.services.parse_watermark import ParseWatermarkService
from app.services.reparse_task import ReparseTaskService
from app.services.dup_fingerprint import DupFingerprintService
from app.services.sample_review import SampleReviewService
from app.services.parse_metric import ParseMetricService
from app.services.tmpl_diff import TmplDiffService
from app.services.intake_audit import IntakeAuditService

SERVICES: dict[str, BaseModuleService] = {
    "file_intake": FileIntakeService(),
    "format_probe": FormatProbeService(),
    "encoding_norm": EncodingNormService(),
    "parser_route": ParserRouteService(),
    "extract_tmpl": ExtractTmplService(),
    "field_parse": FieldParseService(),
    "row_quarantine": RowQuarantineService(),
    "parse_batch": ParseBatchService(),
    "schema_sniff": SchemaSniffService(),
    "delimiter_rule": DelimiterRuleService(),
    "batch_rule_version": BatchRuleVersionService(),
    "rule_migration": RuleMigrationService(),
    "nested_flatten": NestedFlattenService(),
    "value_cast": ValueCastService(),
    "unit_normalize": UnitNormalizeService(),
    "output_contract": OutputContractService(),
    "result_deliver": ResultDeliverService(),
    "parse_watermark": ParseWatermarkService(),
    "reparse_task": ReparseTaskService(),
    "dup_fingerprint": DupFingerprintService(),
    "sample_review": SampleReviewService(),
    "parse_metric": ParseMetricService(),
    "tmpl_diff": TmplDiffService(),
    "intake_audit": IntakeAuditService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)
