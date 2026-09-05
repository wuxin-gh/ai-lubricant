"""MCP market API — 汇总入口。

原 2000+ 行单文件已按职责拆分为 5 个子模块，各自带 ``APIRouter(prefix="/mcp")``，
此处 include_router 汇总挂载。``from mcp.api import router``（main.py 入口）保持不变。

模块划分：
- api_catalog.py        服务注册表（CRUD + manifest/assets/sop + 配置资源 + tools）
- api_principals.py     principal / grants / service-users / runtime-settings / market-settings
- api_runtime.py        MCP Runtime 代理路由（版本/安全审查/热加载/连接配置）
- api_my_services.py    用户个人外部 MCP
- api_builtin_tools.py  内置工具管理端聚合视图

共享 helper 与常量在 api_common.py。
"""
from __future__ import annotations

from fastapi import APIRouter

# 子模块各自带 prefix="/mcp"：父 router 不再加前缀，仅做 include_router 汇总，
# 避免 /mcp + /mcp 的前缀叠加。main.py 的 `from mcp.api import router` 不变。
from .api_catalog import router as catalog_router
from .api_principals import router as principals_router
from .api_runtime import router as runtime_router
from .api_my_services import router as my_services_router
from .api_builtin_tools import router as builtin_tools_router

# 兼容拆分前的 helper 导入路径。外部调用和历史测试曾直接从 mcp.api 导入这些符号；
# 实现仍归属各子模块，此处只 re-export，避免模块拆分造成无关行为回归。
from .api_common import _manifest_for_service, _runtime_public_base, _runtime_settings
from .api_catalog import _agent_config_schema
from .api_my_services import _merge_service_headers, _my_service_to_dict

router = APIRouter(tags=["mcp"])
router.include_router(catalog_router)
router.include_router(principals_router)
router.include_router(runtime_router)
router.include_router(my_services_router)
router.include_router(builtin_tools_router)
