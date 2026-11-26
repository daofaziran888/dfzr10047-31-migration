"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None


# ---------------------------------------------------------------------
# 规则迁移专用接口请求体
# ---------------------------------------------------------------------
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
