"""资源路由注册顺序回归（422 事故的锁定点）。

FastAPI 按注册顺序匹配路由：若 /resources/{resource_id}（动态整数段）注册在
/resources/cdp-clients 等静态子路径之前，GET /resources/cdp-clients 会把
"cdp-clients" 当作 resource_id 解析整数失败，返回 422（int_parsing）。
本测试双保险：
1) 静态检查 router.routes 的注册顺序（静态路径必须先于 {resource_id} 通配）；
2) 用 TestClient 实际打 /resources/cdp-clients，断言不是 422 验证错误
   （未登录场景下应是 401，说明已命中正确路由而非参数解析失败）。
"""
import os
import sys

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from fastapi import FastAPI
from starlette.testclient import TestClient

from user_platform.routes_builtin_tools import router  # noqa: E402

# 静态子路径：不能被 /resources/{resource_id} 通配吞掉。
STATIC_RESOURCE_PATHS = (
    "/api/v1/users/builtin-tools/resources/cdp-clients",
    "/api/v1/users/builtin-tools/resources/mail-services",
    "/api/v1/users/builtin-tools/resources/devices",
    "/api/v1/users/builtin-tools/resources/device-pairing-codes",
)
WILDCARD_PATH = "/api/v1/users/builtin-tools/resources/{resource_id}"


def _registered_paths() -> list[str]:
    return [getattr(route, "path", "") for route in router.routes]


def test_static_resource_paths_registered_before_wildcard():
    paths = _registered_paths()
    wildcard_index = paths.index(WILDCARD_PATH)
    for static_path in STATIC_RESOURCE_PATHS:
        assert static_path in paths, f"missing route: {static_path}"
        assert paths.index(static_path) < wildcard_index, (
            f"{static_path} registered after {WILDCARD_PATH}; "
            "FastAPI would parse the static segment as resource_id and 422"
        )


def test_get_cdp_clients_is_not_path_validation_error():
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    resp = client.get("/api/v1/users/builtin-tools/resources/cdp-clients")
    # 未带登录态：命中了正确路由（鉴权依赖拒绝）→ 401/403，而路径解析失败是 422。
    assert resp.status_code != 422, (
        f"path validation failed: {resp.text} — static route shadowed by {{resource_id}}"
    )


def test_get_mail_services_is_not_path_validation_error():
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    resp = client.get("/api/v1/users/builtin-tools/resources/mail-services")
    assert resp.status_code != 422, (
        f"path validation failed: {resp.text} — static route shadowed by {{resource_id}}"
    )
