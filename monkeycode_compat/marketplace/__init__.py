"""市场子包：GitHub 代管的 MCP / 插件 / Skill 市场。

``router`` 是公开的 ``/status``（常驻挂载，供消费侧直读 raw）；
``admin_router`` 是 ``/admin/*`` 管理接口（常驻挂载，请求时按是否配了
github_token 放行，未配置时返回 404）。
"""
from __future__ import annotations

from . import config
from .routes import admin_router, router

__all__ = ["admin_router", "config", "router"]
