"""规则迁移专用接口：预览、启用/中断/回滚/重试、领取、绑定嗅探、交付、恢复逗号与证据看板。

通用资源路由只覆盖列表/登记/动作，迁移的跨数据集编排走这里的专用路径，
业务判断全部落在服务层，路由层只做参数解包与结果封装。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.schemas import ActionResult
from app.services import service_for
from app.services.rule_migration import RuleMigrationService
from app.services.delimiter_rule import DelimiterRuleService

router = APIRouter(prefix="/api/rule-migration", tags=["规则迁移"])


class SamplePayload(BaseModel):
    sample_lines: list[str] = Field(default_factory=list)


class MigrationCreatePayload(BaseModel):
    migration_no: str
    fmt: str
    old_rule_no: str
    new_rule_no: str
    sample_lines: list[str] = Field(default_factory=list)
    operator: str = "on-duty"
    at: str = ""


class MigrationActionPayload(BaseModel):
    reason: str = ""
    operator: str = "on-duty"
    at: str = ""


class ClaimPayload(BaseModel):
    migration_no: str
    intake_batch: str
    operator: str = "on-duty"
    at: str = ""


class BindPayload(BaseModel):
    record_no: str
    sniff_no: str
    at: str = ""


class RecordActionPayload(BaseModel):
    at: str = ""


class RestorePayload(BaseModel):
    intake_batch: str
    operator: str = "on-duty"
    at: str = ""


class ParsePayload(BaseModel):
    intake_batch: str
    data_lines: list[str] = Field(default_factory=list)


class GuidancePayload(BaseModel):
    intake_batch: str
    operator: str = "on-duty"


def _svc() -> RuleMigrationService:
    svc = service_for("rule_migration")
    assert isinstance(svc, RuleMigrationService)
    return svc


def _result(ok: bool, message: str, entry: Any = None) -> ActionResult:
    return ActionResult(ok=ok, message=message, entry=entry)


@router.post("/previews/{rule_no}", response_model=ActionResult)
def preview_rule(rule_no: str, payload: SamplePayload) -> ActionResult:
    """规则分列预览：按规则条目的引号/转义约定切分样本，不改动任何已确认数据。"""
    rule_svc: DelimiterRuleService = service_for("delimiter_rule")  # type: ignore[assignment]
    ok, message, detail = rule_svc.preview(rule_no, payload.sample_lines)
    return _result(ok, message, detail)


@router.post("/orders", response_model=ActionResult)
def create_order(payload: MigrationCreatePayload) -> ActionResult:
    ok, message, order = _svc().create_migration(
        payload.migration_no, payload.fmt, payload.old_rule_no,
        payload.new_rule_no, payload.sample_lines, payload.operator, payload.at)
    return _result(ok, message, order)


@router.get("/orders/{migration_no}/preview", response_model=dict)
def order_preview(migration_no: str) -> dict[str, Any]:
    """迁移单两份分列预览（回滚后仍保留，供两页对照）。"""
    detail = _svc().preview_detail(migration_no)
    if detail is None:
        return {"ok": False, "message": f"迁移单 {migration_no} 不存在"}
    return {"ok": True, "detail": detail}


@router.post("/orders/{migration_no}/activate", response_model=ActionResult)
def activate_order(migration_no: str, payload: MigrationActionPayload) -> ActionResult:
    ok, message, order = _svc().activate(migration_no, payload.at)
    return _result(ok, message, order)


@router.post("/orders/{migration_no}/interrupt", response_model=ActionResult)
def interrupt_order(migration_no: str, payload: MigrationActionPayload) -> ActionResult:
    ok, message, order = _svc().interrupt(migration_no, payload.reason, payload.at)
    return _result(ok, message, order)


@router.post("/orders/{migration_no}/rollback", response_model=ActionResult)
def rollback_order(migration_no: str, payload: MigrationActionPayload) -> ActionResult:
    ok, message, order = _svc().rollback(migration_no, payload.reason, payload.at)
    return _result(ok, message, order)


@router.post("/orders/{migration_no}/retry", response_model=ActionResult)
def retry_order(migration_no: str, payload: MigrationActionPayload) -> ActionResult:
    """中断后重试：服务端强制先回滚，未回滚直接拒绝。"""
    ok, message, order = _svc().retry(migration_no, payload.at)
    return _result(ok, message, order)


@router.post("/claims", response_model=ActionResult)
def claim_sample(payload: ClaimPayload) -> ActionResult:
    ok, message, record = _svc().claim_sample(
        payload.migration_no, payload.intake_batch, payload.operator, payload.at)
    return _result(ok, message, record)


@router.post("/adoptions/bind-sniff", response_model=ActionResult)
def bind_sniff(payload: BindPayload) -> ActionResult:
    ok, message, record = _svc().bind_sniff(payload.record_no, payload.sniff_no, payload.at)
    return _result(ok, message, record)


@router.post("/adoptions/{record_no}/deliver", response_model=ActionResult)
def deliver_fields(record_no: str, payload: RecordActionPayload) -> ActionResult:
    ok, message, record = _svc().deliver_fields(record_no, payload.at)
    return _result(ok, message, record)


@router.post("/restore-comma", response_model=ActionResult)
def restore_comma(payload: RestorePayload) -> ActionResult:
    ok, message, record = _svc().restore_comma(
        payload.intake_batch, payload.operator, payload.at)
    return _result(ok, message, record)


@router.post("/parse-fields", response_model=ActionResult)
def parse_fields(payload: ParsePayload) -> ActionResult:
    ok, message, detail = _svc().parse_fields(payload.intake_batch, payload.data_lines)
    return _result(ok, message, detail)


@router.get("/evidence", response_model=dict)
def evidence(intake_batch: str = "", rule_no: str = "") -> dict[str, Any]:
    """两处页面共用的采用证据：可按批次或规则打开。"""
    return {"items": _svc().evidence(intake_batch=intake_batch, rule_no=rule_no)}


@router.get("/publish-board", response_model=dict)
def publish_board() -> dict[str, Any]:
    """发布页：已领取样本与待处理新批分区。"""
    return _svc().publish_board()


@router.post("/operator-guidance", response_model=dict)
def operator_guidance(payload: GuidancePayload) -> dict[str, Any]:
    """按服务端资格把操作员导向旧批继续或重新选择合格规则。"""
    return _svc().operator_guidance(payload.intake_batch, payload.operator)
