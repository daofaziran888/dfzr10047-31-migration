"""业务路由汇总：规则迁移专用路径优先注册，通用资源路由兜底模块资源。"""
from __future__ import annotations

from app.routers import migration, resource

# 迁移路由含更具体的 /api/rule-migration 前缀，先注册避免被动态路径抢占。
ROUTERS = [migration, resource]
