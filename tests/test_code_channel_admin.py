"""代码渠道 admin 响应 + 热更新换类回归。

后端写路径（创建/更新/加载）早已接受 ``code`` 字段；这里锁的是：
1. 读路径：``_provider_base_response`` 必须回传 ``code``，否则前端编辑态无法回显源码、
   改完保存会把空串覆盖回去；默认配置给 code 渠道留了空 ``code`` 初值。
2. 热更新：改 ``code`` 后 ``_load_provider_runtime`` 必须整体换池（新 exec 出的类替换旧
   client_class），否则旧 pool 仍按旧类实例化账号，新代码永不生效。
"""
import asyncio

import admin
from rate_limiter import ModelClientPool


def _code_cfg(code: str = "") -> dict:
    return {
        "enabled": True,
        "type": "builtin",
        "builtin_type": "code",
        "remark": "代码渠道",
        "protocol": "openai",
        "chat_path": "/v1/chat/completions",
        "code": code,
        "accounts": [],
        "rate_limit": {},
    }


def test_provider_base_response_includes_code():
    """代码渠道的 base 响应必须带 code，前端编辑态才能回显。"""
    cfg = _code_cfg("class P:\n    @staticmethod\n    async def init_auth(p): return True\n")
    resp = admin._provider_base_response("my-code", cfg)
    assert resp["builtin_type"] == "code"
    assert resp["code"] == "class P:\n    @staticmethod\n    async def init_auth(p): return True\n"


def test_provider_base_response_code_defaults_to_empty():
    """缺 code 字段时响应回空串而非 None，前端 safeString 才不报错。"""
    cfg = _code_cfg()
    cfg.pop("code")
    resp = admin._provider_base_response("my-code", cfg)
    assert resp.get("code") == ""


def test_builtin_default_config_has_code_field():
    """非目录创建代码渠道时，默认配置要给空 code 初值，否则前端草稿里 code 是 undefined。"""
    default_cfg = admin._builtin_provider_default_config("code")
    assert default_cfg is not None
    # builtin_type 由 create_custom_provider 在创建时写入，默认配置本身不带；
    # 这里只锁 code 字段存在且为空串，供前端 safeString 兜底。
    assert "code" in default_cfg
    assert default_cfg["code"] == ""


def test_requires_base_url_validation_by_channel_type():
    """渠道地址必填校验：自定义渠道 / 未豁免代码渠道空地址 400；豁免 spec / 有地址放行。"""
    import pytest
    from fastapi import HTTPException
    from providers.code_loader import invalidate_cache

    invalidate_cache()
    # 自定义渠道空地址 → 400
    custom_no_url = {"type": "custom", "custom_channel": True, "base_url": ""}
    with pytest.raises(HTTPException) as exc:
        admin._validate_provider_requires_base_url("c-custom", custom_no_url)
    assert "base_url" in str(exc.value.detail)

    # 代码渠道 spec 写 REQUIRES_BASE_URL=False → 空地址放行
    waived = "class E:\n    REQUIRES_BASE_URL = False\n    @staticmethod\n    async def init_auth(p, is_check=False): return True\n"
    admin._validate_provider_requires_base_url(
        "c-waived", {"type": "builtin", "builtin_type": "code", "code": waived, "base_url": ""})

    # 未豁免代码渠道空地址 → 400
    needs = "class N:\n    @staticmethod\n    async def init_auth(p, is_check=False): return True\n"
    with pytest.raises(HTTPException):
        admin._validate_provider_requires_base_url(
            "c-needs", {"type": "builtin", "builtin_type": "code", "code": needs, "base_url": ""})

    # 有地址 → 放行
    admin._validate_provider_requires_base_url(
        "c-has", {"type": "custom", "custom_channel": True, "base_url": "https://api.example.com"})
    invalidate_cache()


_SRC_A = (
    "class ChannelA:\n"
    "    @staticmethod\n"
    "    async def init_auth(p, is_check=False): return True\n"
    "    @staticmethod\n"
    "    async def stream_chat(p, m, msgs, **k):\n"
    "        yield {'content': 'A', 'thinking': '', 'tool_calls': []}\n"
)
_SRC_B = (
    "class ChannelB:\n"
    "    @staticmethod\n"
    "    async def init_auth(p, is_check=False): return True\n"
    "    @staticmethod\n"
    "    async def stream_chat(p, m, msgs, **k):\n"
    "        yield {'content': 'B', 'thinking': '', 'tool_calls': []}\n"
)


def test_load_provider_runtime_swaps_class_when_code_changes(monkeypatch):
    """改 code → _load_provider_runtime 用新 exec 出的适配器类整体换池，旧 client_class 不残留。

    这是「保存立即生效」的核心：不换类的话 pool 还按旧类实例化账号，新代码不生效。
    """
    from providers.code_loader import invalidate_cache
    invalidate_cache()

    name = "code-hotswap-test"

    async def _fake_policy(_name):
        return {}

    monkeypatch.setattr(admin, "get_effective_provider_policy", _fake_policy)
    # 隔离副作用：模型刷新/头模板加载/pool 初始化/proxy 读取都打桩，只验证换类。
    monkeypatch.setattr(admin, "_init_pool_and_refresh_models", lambda pool: asyncio.sleep(0))
    monkeypatch.setattr(admin, "_runtime_accounts", lambda accounts, proxies=None: accounts or [])

    async def _noop_headers():
        return None

    monkeypatch.setattr(ModelClientPool, "_ensure_header_templates_loaded", classmethod(lambda cls: _noop_headers()))

    async def run():
        try:
            await admin._load_provider_runtime(name, _code_cfg(_SRC_A))
            cls_a = ModelClientPool.get_provider_pool(name).client_class
            assert cls_a.spec.__name__ == "ChannelA"
            assert cls_a.PROVIDER_NAME == name  # loader 覆盖为 channel id

            await admin._load_provider_runtime(name, _code_cfg(_SRC_B))
            cls_b = ModelClientPool.get_provider_pool(name).client_class
            assert cls_b.spec.__name__ == "ChannelB"
            assert cls_b is not cls_a  # 换了类，不是复用旧 pool 的旧类
        finally:
            ModelClientPool._provider_pools.pop(name, None)
            invalidate_cache()

    asyncio.run(run())

