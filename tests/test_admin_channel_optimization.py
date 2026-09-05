import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import admin
import config
import pytest
from config_store import RedisConfigCache


ADMIN_HTML = Path(__file__).resolve().parents[1] / "static" / "admin.html"


def admin_html() -> str:
    return ADMIN_HTML.read_text(encoding="utf-8")


def function_body(source: str, name: str) -> str:
    marker = f"function {name}("
    assert marker in source, f"Function {name} declaration not found"
    start = source.index(marker)
    brace = source.index("{", start)
    depth = 0
    quote = None
    escaped = False
    template_expr_depth = 0
    for index in range(brace, len(source)):
        char = source[index]
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif quote == "`" and char == "$" and source[index + 1:index + 2] == "{":
                template_expr_depth += 1
            elif quote == "`" and char == "}" and template_expr_depth:
                template_expr_depth -= 1
            elif char == quote and not template_expr_depth:
                quote = None
            continue
        if char in "'\"`":
            quote = char
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[brace + 1:index]
    raise AssertionError(f"Function {name} body not found")


def test_mask_secret_preserves_edges_without_exposing_full_token():
    assert admin._mask_secret("sk-1234567890abcdef") == "sk-1...cdef"
    assert admin._mask_secret("short") == "*****"


def test_find_duplicate_account_tokens_groups_only_non_empty_duplicate_tokens():
    accounts = [
        {"username": "a", "token": "sk-same-token", "switch": True},
        {"username": "b", "token": "sk-same-token", "switch": False},
        {"username": "c", "token": "sk-unique-token", "switch": True},
        {"username": "d", "token": "", "switch": True},
        {"username": "e", "switch": True},
    ]

    duplicates = admin._find_duplicate_account_tokens(accounts)

    assert duplicates == [
        {
            "token_masked": "sk-s...oken",
            "count": 2,
            "accounts": [
                {"username": "a", "enabled": True},
                {"username": "b", "enabled": False},
            ],
        }
    ]
    assert all("token" not in group for group in duplicates)
    assert all(
        "token" not in account
        for group in duplicates
        for account in group["accounts"]
    )


def test_duplicate_token_endpoint_is_registered_after_accounts_endpoint():
    routes = [getattr(route, "path", "") for route in admin.router.routes]

    duplicate_route = "/admin/providers/{name}/accounts/duplicate-tokens"
    assert duplicate_route in routes
    duplicate_index = routes.index(duplicate_route)
    conflicting_dynamic_routes = [
        index
        for index, path in enumerate(routes)
        if path.startswith("/admin/providers/{name}/accounts/{username}")
    ]

    assert all(duplicate_index < index for index in conflicting_dynamic_routes)


def test_account_list_header_has_duplicate_token_button():
    html = admin_html()

    assert "检测重复秘钥" in html
    assert "showDuplicateTokens(${escAttr(JSON.stringify(name))})" in html


def test_duplicate_token_modal_calls_backend_endpoint_and_handles_empty_state():
    html = admin_html()
    body = function_body(html, "showDuplicateTokens")

    assert "/accounts/duplicate-tokens" in body
    assert "encodeURIComponent(name)" in body
    assert "未发现重复秘钥" in body
    assert "esc(group.token_masked || '')" in body
    assert "esc(account.username || '-')" in body


def test_provider_retry_count_endpoint_is_not_registered():
    routes = [getattr(route, "path", "") for route in admin.router.routes]

    assert "/admin/providers/{name}/retry-count" not in routes


def test_provider_tags_normalize_and_serialize_defensively():
    assert admin._normalize_provider_tags([" 国内 ", "推荐", "国内", "", "GPU", "gpu"]) == [
        "国内",
        "推荐",
        "GPU",
        "gpu",
    ]
    assert admin._normalize_provider_tags("invalid") == []
    assert admin._normalize_provider_tags(["valid", 1, None]) == ["valid"]

    assert admin._provider_base_response("example", {})["tags"] == []
    assert admin._provider_summary_response("example", {"tags": "invalid"})["tags"] == []
    assert admin._provider_lite_response("example", {"tags": [" A ", "A"]}, [])["tags"] == ["A"]


def test_provider_tags_strict_writes_reject_invalid_values():
    import pytest
    from fastapi import HTTPException

    for value in ("tag", {"tag": True}, ["valid", 1]):
        with pytest.raises(HTTPException) as exc_info:
            admin._normalize_provider_tags(value, strict=True)
        assert exc_info.value.status_code == 400


def test_provider_summary_response_does_expose_channel_retry_count():
    response = admin._provider_summary_response("example", {"retry_count": 9})

    assert response["retry_count"] == 9

    response_default = admin._provider_summary_response("example", {})

    assert response_default["retry_count"] is None


def test_provider_base_response_does_expose_channel_retry_count():
    response = admin._provider_base_response("example", {"retry_count": 9})

    assert response["retry_count"] == 9

    response_default = admin._provider_base_response("example", {})

    assert response_default["retry_count"] is None


def test_builtin_default_config_does_not_include_retry_count():
    cfg = admin._builtin_provider_default_config("cloudflare")

    assert cfg is not None
    assert "retry_count" not in cfg


def test_cloudflare_auto_update_toggle_is_editable_in_builtin_card():
    body = function_body(admin_html(), "buildBuiltinChannelConfig")

    assert "autoUpdateModelsToggle" in body
    assert "hint-tip" in body
    assert "开启后定时从 Cloudflare 拉取上游模型列表" in body
    assert "toggleHtml(p.auto_update_models === true, 'autoUpdateModelsToggle')" in body
    assert "cfg.auto_update_models" in function_body(admin_html(), "doSaveChannelConfig")


def test_redis_config_cache_forwards_narrow_provider_writes(monkeypatch):
    calls = []

    class Store:
        _providers_cache = {
            "jiekou": {"base_url": "https://example.test", "accounts": [{"username": "u1"}]}
        }

        async def write_provider_base_async(self, name, data, *, drop_accounts=False):
            calls.append(("base", name, data, drop_accounts))

        async def upsert_provider_account_async(self, name, account):
            calls.append(("upsert", name, account))

        async def delete_provider_account_async(self, name, username):
            calls.append(("delete", name, username))
            return True

    cache = RedisConfigCache(Store())
    cached = []

    async def fake_set_provider_cache(name, data):
        cached.append((name, data))

    monkeypatch.setattr(cache, "set_provider_cache", fake_set_provider_cache)

    async def run():
        await cache.write_provider_base_async("jiekou", {"base_url": "https://example.test"})
        await cache.upsert_provider_account_async("jiekou", {"username": "u1"})
        removed = await cache.delete_provider_account_async("jiekou", "u1")
        return removed

    assert asyncio.run(run()) is True
    assert calls == [
        ("base", "jiekou", {"base_url": "https://example.test"}, False),
        ("upsert", "jiekou", {"username": "u1"}),
        ("delete", "jiekou", "u1"),
    ]
    assert len(cached) == 3


def test_read_builtin_provider_config_merges_cloudflare_defaults(monkeypatch):
    class Store:
        async def read_provider_async(self, name):
            assert name == "cloudflare"
            return {"enabled": False, "accounts": []}

    monkeypatch.setattr(admin, "CONFIG_STORE", Store())

    cfg = asyncio.run(admin._read_provider_config("cloudflare"))

    assert cfg["enabled"] is False
    assert cfg["auto_update_models"] is True
    assert cfg["chat_path"] == "/chat/completions"


def test_provider_base_response_exposes_billing_mode():
    response = admin._provider_base_response("example", {"billing_mode": "request"})

    assert response["billing_mode"] == "request"
    assert admin._provider_base_response("example", {})["billing_mode"] == "token"


def test_custom_provider_config_persists_billing_mode():
    base = {"chat_protocols": [{"protocol": "openai", "path": "/v1/chat/completions"}]}

    assert admin._custom_provider_config_from_payload({**base, "billing_mode": "request"})["billing_mode"] == "request"
    assert admin._custom_provider_config_from_payload(base)["billing_mode"] == "token"
    assert admin._custom_provider_config_from_payload({**base, "billing_mode": "unknown"})["billing_mode"] == "token"


def test_custom_provider_config_persists_normalized_tags():
    cfg = admin._custom_provider_config_from_payload({
        "chat_protocols": [{"protocol": "openai", "path": "/v1/chat/completions"}],
        "tags": [" 国内 ", "推荐", "国内"],
    })

    assert cfg["tags"] == ["国内", "推荐"]


def test_update_custom_provider_does_accept_tags_and_hot_updates_runtime():
    """标签是运行时选路的筛选维度，不能被排除在热更新之外。

    rate_limiter 选路按 channel.tags 现取现判；若改标签不刷新本机 pool.channel 快照，
    本机要等 60s 对账才生效（而其它实例经 EVENT_CHANNEL 立即生效），造成实例间漂移。
    """
    source = Path(admin.__file__).read_text(encoding="utf-8")
    start = source.index("async def update_custom_provider_config")
    end = source.index("# ==================== 账号操作", start)
    body = source[start:end]

    assert '"remark", "tags", "price_remark"' in body
    assert 'changed_keys - {"tags"}' not in body
    assert "if changed_keys:" in body


def test_tags_only_update_hot_updates_channel_snapshot(monkeypatch):
    """只改标签也要热更新本机 Channel 快照，让选路立刻按新标签筛选。"""
    writes = []
    hot_updates = []
    runtime_loads = []

    async def fake_require_admin(token):
        return None

    async def fake_read_provider_config(name):
        return {"remark": "Example", "tags": ["old"]}

    async def fake_write_provider_base(name, cfg):
        writes.append((name, dict(cfg)))

    async def fake_load_provider_runtime(name, cfg):
        runtime_loads.append((name, cfg))

    async def fake_log_operation(*args, **kwargs):
        return None

    monkeypatch.setattr(admin, "_require_admin", fake_require_admin)
    monkeypatch.setattr(admin, "_read_provider_config", fake_read_provider_config)
    monkeypatch.setattr(admin, "_write_provider_base", fake_write_provider_base)
    monkeypatch.setattr(admin, "_load_provider_runtime", fake_load_provider_runtime)
    monkeypatch.setattr(admin, "_log_operation", fake_log_operation)
    monkeypatch.setattr(admin, "_hot_update_custom_provider", lambda *args: hot_updates.append(args))
    monkeypatch.setattr(admin.ModelClientPool, "get_provider_pool", lambda name: object())

    result = asyncio.run(admin.update_custom_provider_config("example", {"tags": [" new ", "new"]}))

    assert result == {"ok": True}
    assert writes == [("example", {"remark": "Example", "tags": ["new"]})]
    # tags 变更命中热更新：本机 pool.channel 快照被刷新（changed_keys 含 "tags"）。
    assert hot_updates == [("example", {"remark": "Example", "tags": ["new"]}, {"tags"})]
    assert runtime_loads == []


def test_create_custom_provider_does_write_retry_count():
    source = Path(admin.__file__).read_text(encoding="utf-8")
    start = source.index("def _custom_provider_config_from_payload")
    end = source.index("async def _fetch_unsaved_custom_provider_models", start)

    assert '"retry_count"' in source[start:end]


def test_custom_provider_auto_update_models_defaults_to_false():
    """自定义渠道默认不开启定时同步：避免上游无 /v1/models 的渠道被周期任务改写模型。"""
    source = Path(admin.__file__).read_text(encoding="utf-8")
    start = source.index("def _custom_provider_config_from_payload")
    end = source.index("async def _fetch_unsaved_custom_provider_models", start)

    assert '"auto_update_models": data.get("auto_update_models", False)' in source[start:end]


def test_update_custom_provider_does_accept_retry_count():
    source = Path(admin.__file__).read_text(encoding="utf-8")
    start = source.index("async def update_custom_provider_config")
    end = source.index("# ==================== 账号操作", start)

    assert '"retry_count"' in source[start:end]


def test_channel_config_cards_do_not_render_retry_count():
    html = admin_html()

    assert "重试次数" not in function_body(html, "buildBuiltinChannelConfig")
    assert "重试次数" not in function_body(html, "showCustomConfig")
    assert "ccRetryCount" not in html


def test_custom_channel_creation_no_longer_posts_retry_count():
    html = admin_html()

    assert "retry_count" not in function_body(html, "createCustomChannelFromPage")
    assert "渠道未单独配置" not in html


def test_anthropic_messages_awaits_async_chat_validation():
    source = Path(admin.__file__).resolve().parent / "main.py"
    body = source.read_text(encoding="utf-8")
    # 校验已收拢到统一入口 dispatch_entry 的 anthropic 分支（HTTP 端点改为薄封装）。
    start = body.index("async def dispatch_entry")
    end = body.index("@app.post(\"/v1/chat/completions\")", start)
    entry_body = body[start:end]

    assert "model, _ = await _validate_chat_request(body, api_key)" in entry_body
    assert "model, _ = _validate_chat_request(body, api_key)" not in entry_body


def test_custom_channel_preview_models_uses_unsaved_provider_endpoint():
    html = admin_html()
    body = function_body(html, "fetchCcModels")

    assert "/admin/custom-providers/upstream-models" in body
    assert "/admin/fetch-models" not in body
    assert "readCustomChannelForm(false)" in body


def test_custom_channel_creation_uses_channel_model_table_card():
    html = admin_html()
    create_body = function_body(html, "renderCustomCreatePage")
    card_body = function_body(html, "renderCcModelConfigCard")

    assert "custom-create-grid" in create_body
    assert "renderCcModelConfigCard()" in create_body
    assert "渠道模型列表" in card_body
    assert "model-config-card" in card_body
    assert "model-table-scroll" in card_body
    assert "+ 添加一行" in card_body
    assert "从上游同步" in card_body
    assert "清空" in card_body
    assert "ccModelTable" in card_body
    assert "上游模型 ID" in card_body
    assert "对外模型 ID" in card_body
    assert "ccModelList" not in create_body
    assert "ccRedirectList" not in create_body


def test_model_table_height_uses_shared_scroll_class_not_320px_inline_height():
    html = admin_html()

    assert ".model-table-scroll{height:230px;max-height:230px" in html
    assert "height:320px;max-height:320px" not in html
    assert "model-config-table" in function_body(html, "renderModelConfig")
    assert "model-config-table" in function_body(html, "renderCcModelConfigCard")


def test_custom_channel_accounts_use_modal_list_not_inline_forms():
    html = admin_html()
    create_body = function_body(html, "renderCustomCreatePage")
    add_body = function_body(html, "addCcAccount")
    render_body = function_body(html, "renderCcAccounts")
    read_body = function_body(html, "readCustomChannelForm")

    assert "showCcAccountModal(null)" in add_body
    assert "account-card" in render_body
    assert "ccAccUser_" not in render_body
    assert "ccAccProxyCustom" not in html
    assert "_ccAccounts.map" in read_body
    assert "ccAccUser_" not in read_body
    assert "自动测试" in create_body
    assert '<option value="false" selected>关闭</option>' in create_body


def test_proxy_ui_forces_pool_binding_and_supports_credentials():
    html = admin_html()

    assert "pxUsername" in function_body(html, "proxyEditorHtml")
    assert "pxPassword" in function_body(html, "proxyEditorHtml")
    assert "proxy_id" in function_body(html, "showAddAccount")
    assert "proxy_id" in function_body(html, "editAccount")
    assert "acProxyCustom" not in html
    assert "ef_proxy_custom" not in html
    assert "手动输入代理" not in html


def test_custom_channel_model_table_collects_redirect_names_for_create():
    html = admin_html()
    create_body = function_body(html, "createCustomChannelFromPage")
    collect_body = function_body(html, "collectCcModelRowsFromDom")

    assert "collectCcModelRowsFromDom()" in create_body
    assert "modelRows" in create_body
    assert "provider" in create_body and "/models" in create_body
    assert "model-input-id" in collect_body
    assert "model-input-alias" in collect_body
    assert "model_id: aliasVal || upstream" in collect_body
    assert "_ccModelRedirects" not in create_body


def test_custom_channel_upstream_sync_modal_supports_alias_editing():
    html = admin_html()
    body = function_body(html, "fetchCcModels")

    assert "fetchModelListBox" in body
    assert "fetchAlias_" in body
    assert "对外模型 ID" in body
    assert "setCcModelRows(selected)" in body
    assert "renderCcModelList()" in body


def test_custom_channel_preview_models_endpoint_does_not_save_config():
    source = Path(admin.__file__).read_text(encoding="utf-8")
    body = source[source.index("async def preview_custom_provider_upstream_models"):source.index("@router.get(\"/custom-providers\")")]

    assert "_fetch_unsaved_custom_provider_models" in body
    assert "_write_provider_config" not in body
    assert "_load_provider_runtime" not in body


def test_proxy_helpers_support_credentials_and_account_proxy_id(monkeypatch):
    monkeypatch.setattr(admin, "_read_json", lambda path: {"proxies": [{"id": "p1", "name": "P1", "url": "http://127.0.0.1:7890", "username": "u s", "password": "p@ss"}]})

    proxies = admin._read_proxies()

    assert proxies[0]["id"] == "p1"
    assert admin._proxy_effective_url(proxies[0]) == "http://u%20s:p%40ss@127.0.0.1:7890"
    account = admin._normalize_account_proxy_ref({"username": "u", "password": "k", "proxy_id": "p1"}, proxies)
    assert account["proxy_id"] == "p1"
    assert "proxy" not in account
    assert admin._runtime_accounts([account], proxies)[0]["proxy"] == "http://u%20s:p%40ss@127.0.0.1:7890"
    assert admin._runtime_accounts([{"username": "none"}], proxies)[0]["proxy"] is None
    assert admin._runtime_accounts([{"username": "legacy", "proxy": "http://legacy.example:1234"}], proxies)[0]["proxy"] == "http://legacy.example:1234"
    assert admin._runtime_accounts([{"username": "deleted", "proxy_id": "missing"}], proxies)[0]["proxy"] is None


def _make_runtime_account():
    """构造最小可用的运行态 provider/AccountClient 替身，用于代理热更新测试。"""

    class _Provider:
        def __init__(self, username, proxy):
            self.username = username
            self.proxy = proxy

    class _Client:
        def __init__(self, username, proxy):
            self.username = username
            self.provider = _Provider(username, proxy)

    return _Client


def _install_runtime_pool(provider_name, clients):
    """把一组替身账号挂到 ModelClientPool 的给定渠道下；返回恢复函数。

    用 SimpleNamespace 直接充当 pool，避免构造真实 ProviderPool/Channel 带来的副作用。
    """
    pool = SimpleNamespace(clients=list(clients))
    saved = dict(admin.ModelClientPool._provider_pools)
    admin.ModelClientPool._provider_pools[provider_name] = pool

    def restore():
        admin.ModelClientPool._provider_pools.clear()
        admin.ModelClientPool._provider_pools.update(saved)
    return restore


def _stub_config_store(monkeypatch, accounts_by_provider: dict):
    """让 _hot_update_proxy_pool 看到一组持久化账号配置。"""
    providers = {
        name: {"accounts": accounts}
        for name, accounts in accounts_by_provider.items()
    }
    monkeypatch.setattr(admin, "CONFIG_STORE", SimpleNamespace(list_providers=lambda: providers))


def test_reload_account_updates_proxy_config_id_in_place(monkeypatch):
    """账号换绑/解绑代理时同步 ProxyManager 的真实路由字段，无需重启。"""
    proxies = [
        {"id": "p1", "name": "P1", "url": "http://proxy-1.example:7890"},
        {"id": "p2", "name": "P2", "url": "http://proxy-2.example:7890"},
    ]
    monkeypatch.setattr(admin, "_read_proxies", lambda: proxies)

    provider = SimpleNamespace(
        username="acc1", password="", proxy="http://proxy-1.example:7890",
        url_prefix=None, proxy_config_id="p1",
    )
    client = SimpleNamespace(provider=provider, username="acc1")
    pool = SimpleNamespace(clients=[client], channel=SimpleNamespace(enabled=False))
    monkeypatch.setattr(admin.ModelClientPool, "get_provider_pool", lambda name: pool)

    admin._reload_account_into_pool("demo", {"username": "acc1", "proxy_id": "p2"}, 0, {})
    assert provider.proxy == "http://proxy-2.example:7890"
    assert provider.proxy_config_id == "p2"

    admin._reload_account_into_pool("demo", {"username": "acc1"}, 0, {})
    assert provider.proxy is None
    assert provider.proxy_config_id == ""


def test_reload_account_hot_updates_metadata_in_place(monkeypatch):
    """账号 metadata 保存后热更新到运行态 client，无需重启；整快照替换语义。"""
    monkeypatch.setattr(admin, "_read_proxies", lambda: [])
    provider = SimpleNamespace(username="acc1", password="", proxy=None, url_prefix=None, proxy_config_id="")
    client = SimpleNamespace(provider=provider, username="acc1", metadata={"org_id": "old"})
    pool = SimpleNamespace(clients=[client], channel=SimpleNamespace(enabled=False))
    monkeypatch.setattr(admin.ModelClientPool, "get_provider_pool", lambda name: pool)

    # 带新 metadata 的快照 → 整体替换运行态 metadata。
    admin._reload_account_into_pool("demo", {"username": "acc1", "metadata": {"org_id": "new", "team": "infra"}}, 0, {})
    assert client.metadata == {"org_id": "new", "team": "infra"}

    # 快照不含 metadata → 不清空（非 metadata 的就地更新不触碰已有 metadata）。
    admin._reload_account_into_pool("demo", {"username": "acc1", "balance": 5}, 0, {})
    assert client.metadata == {"org_id": "new", "team": "infra"}

    # 空快照 metadata={} → 显式清空（前端清空所有字段后保存的语义）。
    admin._reload_account_into_pool("demo", {"username": "acc1", "metadata": {}}, 0, {})
    assert client.metadata == {}


def test_reload_account_new_account_passes_metadata_to_client(monkeypatch):
    """新账号入池时 metadata 传入 AccountClient 构造（启动/首次加载路径）。"""
    monkeypatch.setattr(admin, "_read_proxies", lambda: [])

    built: list[dict] = {}

    class FakeProvider:
        def __init__(self, **kwargs):
            self.username = kwargs.get("username")
            self.password = kwargs.get("password", "")
            self.proxy = kwargs.get("proxy")
            self.url_prefix = kwargs.get("url_prefix")
            self.PROVIDER_NAME = "custom"

        def attach_channel(self, channel):
            self._channel = channel

    class FakeAccountClient:
        def __init__(self, provider, rpm, **kwargs):
            self.provider = provider
            self.username = provider.username
            self.metadata = dict(kwargs.get("metadata") or {})
            built["metadata"] = self.metadata
            built["disabled"] = kwargs.get("disabled")

    pool = SimpleNamespace(
        clients=[],
        channel=SimpleNamespace(enabled=False),
        client_class=FakeProvider,
    )
    monkeypatch.setattr(admin, "_refresh_models_after_account_init", lambda *a, **k: None)
    monkeypatch.setattr(admin.ModelClientPool, "get_provider_pool", lambda name: pool)
    monkeypatch.setattr(admin, "AccountClient", FakeAccountClient)

    admin._reload_account_into_pool(
        "demo",
        {"username": "acc-new", "metadata": {"org_id": "org-9", "tier": "pro"}},
        0,
        {"provider_name": "demo"},
    )

    assert built["metadata"] == {"org_id": "org-9", "tier": "pro"}



def test_hot_update_proxy_pool_refreshes_resolved_url_in_place(monkeypatch):
    """修改代理 URL/认证后，所有绑定该 proxy_id 的运行中 provider 立即变为新有效地址。"""
    new_proxies = [{"id": "p1", "name": "P1", "url": "http://10.0.0.1:8888", "username": "u s", "password": "p@ss"}]
    monkeypatch.setattr(admin, "_read_proxies", lambda: new_proxies)
    _stub_config_store(monkeypatch, {"demo": [
        {"username": "acc1", "proxy_id": "p1"},
        {"username": "acc2", "proxy_id": "p1"},
        {"username": "acc3"},  # 未绑定代理
    ]})

    Client = _make_runtime_account()
    clients = [Client("acc1", "http://u:p@127.0.0.1:7890"), Client("acc2", "http://u:p@127.0.0.1:7890"), Client("acc3", "")]
    restore = _install_runtime_pool("demo", clients)
    try:
        result = admin._hot_update_proxy_pool(new_proxies)
    finally:
        restore()

    expected = "http://u%20s:p%40ss@10.0.0.1:8888"
    assert clients[0].provider.proxy == expected
    assert clients[1].provider.proxy == expected
    assert clients[2].provider.proxy == ""  # 无代理的等价表示不触发无意义赋值
    assert result["changed"] == 2
    assert "demo:acc1" in result["accounts"] and "demo:acc2" in result["accounts"]


def test_hot_update_proxy_pool_deleting_referenced_proxy_clears_runtime_value(monkeypatch):
    """删除被引用的代理后，运行中 provider 代理清空（与全新加载语义一致）。"""
    new_proxies: list = []  # 代理已被删除
    monkeypatch.setattr(admin, "_read_proxies", lambda: new_proxies)
    _stub_config_store(monkeypatch, {"demo": [{"username": "acc1", "proxy_id": "p1"}]})

    Client = _make_runtime_account()
    client = Client("acc1", "http://u:p@127.0.0.1:7890")
    restore = _install_runtime_pool("demo", [client])
    try:
        result = admin._hot_update_proxy_pool(new_proxies)
    finally:
        restore()

    assert client.provider.proxy is None  # 代理被删 → 清空运行态
    assert result["changed"] == 1


def test_hot_update_proxy_pool_preserves_account_client_identity_and_state(monkeypatch):
    """代理热更新不重建 AccountClient/provider，限流/认证等运行态对象保持同一实例。"""
    new_proxies = [{"id": "p1", "name": "P1", "url": "http://10.0.0.1:8888", "username": "u", "password": "p"}]
    monkeypatch.setattr(admin, "_read_proxies", lambda: new_proxies)
    _stub_config_store(monkeypatch, {"demo": [{"username": "acc1", "proxy_id": "p1"}]})

    Client = _make_runtime_account()
    client = Client("acc1", "http://u:p@127.0.0.1:7890")
    original_provider = client.provider
    restore = _install_runtime_pool("demo", [client])
    try:
        admin._hot_update_proxy_pool(new_proxies)
    finally:
        restore()

    assert client.provider is original_provider  # 同一 provider 对象，原地更新
    assert client.provider.proxy == "http://u:p@10.0.0.1:8888"


def test_hot_update_proxy_pool_leaves_literal_non_pool_proxy_untouched(monkeypatch):
    """旧式字面量代理（不在代理池中）不受代理池变更影响。"""
    new_proxies = [{"id": "p1", "name": "P1", "url": "http://10.0.0.1:8888", "username": "u", "password": "p"}]
    monkeypatch.setattr(admin, "_read_proxies", lambda: new_proxies)
    _stub_config_store(monkeypatch, {"demo": [
        {"username": "acc1", "proxy": "http://legacy.example:1234"},  # 字面量旧代理
    ]})

    Client = _make_runtime_account()
    client = Client("acc1", "http://legacy.example:1234")
    restore = _install_runtime_pool("demo", [client])
    try:
        result = admin._hot_update_proxy_pool(new_proxies)
    finally:
        restore()

    assert client.provider.proxy == "http://legacy.example:1234"  # 不受代理池变更影响
    assert result["changed"] == 0


def test_hot_update_proxy_pool_treats_empty_proxy_as_no_change(monkeypatch):
    """启动时空字符串与 None 都代表无代理，不应被首次对账计为变更。"""
    _stub_config_store(monkeypatch, {"demo": [
        {"username": "empty"},
        {"username": "none"},
    ]})

    Client = _make_runtime_account()
    clients = [Client("empty", ""), Client("none", None)]
    restore = _install_runtime_pool("demo", clients)
    try:
        result = admin._hot_update_proxy_pool([])
    finally:
        restore()

    assert result == {"changed": 0, "accounts": []}
    assert clients[0].provider.proxy == ""
    assert clients[1].provider.proxy is None


def test_update_proxies_hot_reloads_runtime_and_publishes_event(monkeypatch):
    """update_proxies 持久化后同步刷新运行态代理，并广播不含密钥的 EVENT_PROXY 事件。"""
    async def fake_require_admin(*args, **kwargs):
        return None

    async def fake_write_json_async(path, data):
        return None

    monkeypatch.setattr(admin, "_require_admin", fake_require_admin)
    monkeypatch.setattr(admin, "_read_json", lambda path: {})
    monkeypatch.setattr(admin, "_write_json_async", fake_write_json_async)
    _stub_config_store(monkeypatch, {"demo": [{"username": "acc1", "proxy_id": "p1"}]})

    Client = _make_runtime_account()
    client = Client("acc1", "http://u:p@127.0.0.1:7890")
    restore = _install_runtime_pool("demo", [client])

    captured: list = []

    async def fake_runtime_publish(event_type, name=None, version=None, extra=None):
        captured.append((event_type, name, extra))

    monkeypatch.setattr(admin.runtime_sync, "publish", fake_runtime_publish)

    new_proxies = [{"id": "p1", "name": "P1", "url": "http://10.0.0.1:8888", "username": "u", "password": "p"}]
    try:
        result = asyncio.run(admin.update_proxies(new_proxies))
    finally:
        restore()

    assert client.provider.proxy == "http://u:p@10.0.0.1:8888"
    assert result["hot_reloaded"] == 1
    assert captured and captured[0][0] == "proxy"  # 事件类型为 proxy，不含 extra 密钥
    assert captured[0][2] is None


def _make_runtime_account_with_prefix():
    """带 url_prefix 字段的运行态替身，用于前缀代理热更新测试。"""

    class _Provider:
        def __init__(self, username, proxy=None, url_prefix=None):
            self.username = username
            self.proxy = proxy
            self.url_prefix = url_prefix

    class _Client:
        def __init__(self, username, proxy=None, url_prefix=None):
            self.username = username
            self.provider = _Provider(username, proxy, url_prefix)

    return _Client


def test_hot_update_proxy_pool_applies_url_prefix_mode(monkeypatch):
    """绑定 url_prefix 模式代理后，运行中 provider 得到前缀基址且 proxy 置空。"""
    new_proxies = [{"id": "pre1", "name": "P1", "mode": "url_prefix", "url": "https://relay.example/p", "username": "", "password": ""}]
    monkeypatch.setattr(admin, "_read_proxies", lambda: new_proxies)
    _stub_config_store(monkeypatch, {"demo": [{"username": "acc1", "proxy_id": "pre1"}]})

    Client = _make_runtime_account_with_prefix()
    client = Client("acc1", proxy=None, url_prefix=None)
    restore = _install_runtime_pool("demo", [client])
    try:
        result = admin._hot_update_proxy_pool(new_proxies)
    finally:
        restore()

    assert client.provider.proxy is None
    assert client.provider.url_prefix == "https://relay.example/p"
    assert result["changed"] == 1


def test_hot_update_proxy_pool_switches_network_to_url_prefix(monkeypatch):
    """从 network 切换到 url_prefix：清空旧 proxy，写入新前缀。"""
    new_proxies = [{"id": "pre1", "name": "P1", "mode": "url_prefix", "url": "https://relay.example/p", "username": "", "password": ""}]
    monkeypatch.setattr(admin, "_read_proxies", lambda: new_proxies)
    _stub_config_store(monkeypatch, {"demo": [{"username": "acc1", "proxy_id": "pre1"}]})

    Client = _make_runtime_account_with_prefix()
    client = Client("acc1", proxy="http://old.example:7890", url_prefix=None)
    restore = _install_runtime_pool("demo", [client])
    try:
        result = admin._hot_update_proxy_pool(new_proxies)
    finally:
        restore()

    assert client.provider.proxy is None
    assert client.provider.url_prefix == "https://relay.example/p"
    assert result["changed"] == 1


def test_hot_update_proxy_pool_deleting_url_prefix_clears_both_fields(monkeypatch):
    """删除被绑定的 url_prefix 代理后，两个运行时字段都恢复无代理。"""
    new_proxies: list = []
    monkeypatch.setattr(admin, "_read_proxies", lambda: new_proxies)
    _stub_config_store(monkeypatch, {"demo": [{"username": "acc1", "proxy_id": "pre1"}]})

    Client = _make_runtime_account_with_prefix()
    client = Client("acc1", proxy=None, url_prefix="https://relay.example/p")
    restore = _install_runtime_pool("demo", [client])
    try:
        result = admin._hot_update_proxy_pool(new_proxies)
    finally:
        restore()

    assert client.provider.proxy is None
    assert client.provider.url_prefix is None
    assert result["changed"] == 1


def test_normalize_proxy_item_validates_url_prefix_mode():
    """url_prefix 模式校验：要求绝对 http(s)、去尾 /、清空认证、拒绝 query。"""
    ok = admin._normalize_proxy_item({"name": "P", "mode": "url_prefix", "url": "https://relay.example/p/", "username": "u", "password": "x"})
    assert ok["mode"] == "url_prefix"
    assert ok["url"] == "https://relay.example/p"
    assert ok["username"] == "" and ok["password"] == ""

    for bad in (
        {"name": "P", "mode": "url_prefix", "url": "relay.example/p"},
        {"name": "P", "mode": "url_prefix", "url": "https://relay.example/p?x=1"},
        {"name": "P", "mode": "bogus", "url": "http://x"},
    ):
        try:
            admin._normalize_proxy_item(bad)
            assert False, f"expected HTTPException for {bad}"
        except admin.HTTPException:
            pass


def test_normalize_proxy_item_defaults_to_network_mode():
    """缺省 mode 的旧条目规范化为 network，保留认证。"""
    item = admin._normalize_proxy_item({"name": "P", "url": "http://127.0.0.1:7890", "username": "u", "password": "x"})
    assert item["mode"] == "network"
    assert item["username"] == "u" and item["password"] == "x"


def test_normalize_proxy_item_direct_mode_needs_no_url():
    """direct 模式（强制直连）无需 url，规范化后清空 url/认证。"""
    item = admin._normalize_proxy_item({"name": "D", "mode": "direct", "url": "http://ignored", "username": "u", "password": "x"})
    assert item["mode"] == "direct"
    assert item["url"] == ""
    assert item["username"] == "" and item["password"] == ""
    # 仅名称即可创建 direct 条目
    only_name = admin._normalize_proxy_item({"name": "D2", "mode": "direct"})
    assert only_name["mode"] == "direct" and only_name["url"] == ""



def test_model_test_page_infers_mode_without_type_selector():
    html = admin_html()
    render_body = function_body(html, "renderTestModelPage")
    sync_body = function_body(html, "tmSyncMode")
    mode_body = function_body(html, "tmCurrentMode")
    run_body = function_body(html, "runTestModel")

    assert "测试类型" not in render_body
    assert 'id="tmMode"' not in render_body
    assert "<select id=\"tmMode\"" not in render_body
    assert "output_modalities" in function_body(html, "tmOutputModalities")
    assert "caps.image_generation === true" in mode_body
    assert "caps.video_generation === true" in mode_body
    assert "tmSetDisplay('tmMediaSettings', mode !== 'chat')" in sync_body
    assert "const mode = tmCurrentMode();" in run_body


def test_model_test_media_parameter_settings_are_collapsible_labels():
    html = admin_html()
    render_body = function_body(html, "renderTestModelPage")

    assert "<details id=\"tmMediaSettings\"" in render_body
    assert "<summary>参数设置</summary>" in render_body
    assert "tm-media-options" in render_body
    assert "data-tip=\"图片常用 1024x1024" in render_body
    assert "data-tip=\"一次返回几个结果" in render_body
    assert "data-tip=\"url 返回图片链接" in render_body
    assert "data-tip=\"生成视频的目标时长" in render_body
    assert "url（返回图片链接）" not in render_body
    assert 'form-hint' not in render_body[render_body.index('id="tmMediaOptions"'):render_body.index('class="tm-composer-actions"')]


def test_provider_account_test_restores_missing_runtime_before_lookup(monkeypatch):
    calls = []
    pool = SimpleNamespace(clients=[])

    async def fake_require_admin(token):
        return None

    async def fake_ensure_provider_runtime(name):
        calls.append(name)
        return True

    monkeypatch.setattr(admin, "_require_admin", fake_require_admin)
    monkeypatch.setattr(admin, "_ensure_provider_runtime", fake_ensure_provider_runtime)
    monkeypatch.setattr(admin.ModelClientPool, "get_provider_pool", lambda name: pool if calls else None)
    monkeypatch.setattr(admin, "_read_provider_config", lambda name: asyncio.sleep(0, result={"protocol": "openai"}))

    result = asyncio.run(admin.test_provider_accounts(
        "cloudflare",
        {"username": "account", "model": "model", "test_type": "chat"},
        token="t",
    ))

    assert calls == ["cloudflare"]
    assert result == {"ok": True, "results": []}


def test_provider_account_test_keeps_not_found_when_runtime_cannot_be_restored(monkeypatch):
    async def fake_require_admin(token):
        return None

    async def fake_ensure_provider_runtime(name):
        assert name == "missing"
        return False

    monkeypatch.setattr(admin, "_require_admin", fake_require_admin)
    monkeypatch.setattr(admin, "_ensure_provider_runtime", fake_ensure_provider_runtime)

    with pytest.raises(admin.HTTPException) as exc_info:
        asyncio.run(admin.test_provider_accounts(
            "missing",
            {"username": "account", "model": "model", "test_type": "chat"},
            token="t",
        ))

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "渠道 'missing' 不存在"


def test_provider_account_test_keeps_internal_model_for_logs_and_uses_upstream_for_call():
    source = Path(admin.__file__).read_text(encoding="utf-8")
    body = source[source.index("async def test_provider_accounts"):source.index("@router.get(\"/accounts/{username}/daily-usage\")")]

    assert "upstream_model_id = ModelClientPool.resolve_upstream_id(name, model)" in body
    assert "model = ModelClientPool.resolve_upstream_id(name, model)" not in body
    assert '"model": model' in body
    assert '"actual_model": model' in body
    assert 'c.provider.chat(upstream_model_id, test_messages' in body
    assert '"upstream_returned_model": model' not in body



def test_provider_accounts_reads_cooldown_reason_from_redis_info():
    source = Path(admin.__file__).read_text(encoding="utf-8")
    body = source[source.index("async def _provider_accounts_response"):source.index("def _provider_summary_response", source.index("async def _provider_accounts_response"))]

    # 冻结改纯内存：不再调 Redis batch_get_*，直接读 _freeze_state。
    assert "batch_get_account_cooldowns" not in body
    assert "batch_get_provider_model_cooldowns" not in body
    assert "_memory_account_cooldown" in body
    assert "client.freeze_reason()" in body
    assert 'cooldown_info.get("reason")' in body




def test_provider_lite_response_omits_runtime_account_statistics():
    response = admin._provider_lite_response(
        "p1",
        {"enabled": False, "remark": "渠道", "custom_channel": True},
        ["m1"],
    )

    assert response == {
        "id": "p1",
        "name": "p1",
        "remark": "渠道",
        "enabled": False,
        "custom_channel": True,
        "builtin_type": "",
        "protocol": None,
        "models": ["m1"],
        "tags": [],
    }


def test_provider_lite_accounts_preserves_account_and_model_cooldown_flags(monkeypatch):
    # 冻结改纯内存：通过 pool.clients 的 _freeze_state 判定，不再 monkeypatch Redis。
    def fake_pool(name):
        if name == "p1":
            clients = {
                "a": SimpleNamespace(username="a", _freeze_state={
                    "account": {"until": None, "reason": "account"},
                    "models": {},
                }),
                "b": SimpleNamespace(username="b", _freeze_state={
                    "account": {"until": 0, "reason": ""},
                    "models": {"m1": {"until": None, "reason": "model"}},
                }),
            }
        return SimpleNamespace(channel=None, clients=list(clients.values()))

    monkeypatch.setattr(admin.ModelClientPool, "get_provider_pool", fake_pool)

    response = asyncio.run(admin._provider_lite_accounts_response("p1", {
        "accounts": [
            {"username": "a", "switch": True},
            {"username": "b", "switch": False},
            {"username": "a", "switch": False},
            {"username": ""},
        ],
    }))

    # a: 账号级永久冻结；b: 账号未冻但有模型级冻结 → cooldown=True。
    # switch 关 / 未加载才判 disabled；b 开关关 = disabled。
    assert response == [
        {"username": "a", "switch": True, "cooldown": True, "state": "cooling"},
        {"username": "b", "switch": False, "cooldown": True, "state": "disabled"},
    ]


def test_provider_copy_accounts_omits_runtime_fields():
    response = admin._provider_copy_accounts_response({
        "accounts": [
            {"username": "a", "password": "p", "api_key": "k", "key": "legacy", "proxy": "proxy"},
            {"username": "a", "password": "ignored"},
            {"username": "b"},
        ],
    })

    assert response == [
        {"username": "a", "password": "p", "api_key": "k", "key": "legacy"},
        {"username": "b", "password": None, "api_key": None, "key": None},
    ]


def test_provider_summary_uses_scanned_cooldown_keys_not_runtime_until(monkeypatch):
    # 冻结改纯内存：通过 pool.clients 的 _freeze_state 判定，cooldowns_all 参数不再使用。
    def fake_pool(name):
        if name == "p1":
            provider = SimpleNamespace(SUPPORTS_TOKEN_AUTO_REFRESH=False)
            return SimpleNamespace(channel=None, clients=[
                SimpleNamespace(username="a", provider=provider, _in_flight=0, is_frozen=True, _freeze_state={
                    "account": {"until": None, "reason": "frozen"},
                    "models": {},
                }),
                SimpleNamespace(username="b", provider=provider, _in_flight=0, is_frozen=False, _freeze_state={
                    "account": {"until": 0, "reason": ""},
                    "models": {"m1": {"until": None, "reason": "model"}},
                }),
            ])
        return None

    monkeypatch.setattr(admin.ModelClientPool, "get_provider_pool", fake_pool)

    summary = admin._provider_summary_response(
        "p1",
        {"enabled": True, "accounts": [{"username": "a"}, {"username": "b"}]},
    )

    assert summary["cooldown_account_count"] == 2


def test_provider_summary_no_longer_depends_on_runtime_cooldown_until():
    source = Path(admin.__file__).read_text(encoding="utf-8")
    body = source[source.index("def _provider_summary_response"):source.index("@router.get(\"/providers\")")]

    assert "_cooldown_until" not in body
    # 冻结改纯内存：不再读 cooldowns_all，直接遍历 _freeze_state。
    assert "cooldowns_all" not in body
    assert "_freeze_state" in body


def test_provider_account_status_response_distinguishes_account_and_model_cooldowns():
    response = admin._provider_account_status_response(
        "p1",
        {"a": {"remaining": 10, "until": 123, "reason": "account"}},
        {
            "a": [{"model": "m1", "remaining": 20, "until": 456, "reason": "model"}],
            "b": [{"model": "m2", "remaining": 30, "until": 789, "reason": "model-only"}],
        },
    )

    accounts = {item["username"]: item for item in response["accounts"]}
    assert accounts["a"]["cooldown_scope"] == "mixed"
    assert accounts["a"]["cooldown_reason"] == "account"
    assert accounts["a"]["model_cooldowns"][0]["model"] == "m1"
    assert accounts["b"]["cooldown_scope"] == "model"
    assert accounts["b"]["cooldown_reason"] == ""


def test_scan_all_cooldowns_reads_index_without_global_scan():
    """渠道总览改为读冻结索引（smembers）+ 精确 key TTL 校验，不再全库 SCAN。"""
    source = (Path(admin.__file__).resolve().parent / "limits" / "backend.py").read_text(encoding="utf-8")
    body = source[source.index("async def scan_all_cooldowns"):source.index("def calendar_window_id", source.index("async def scan_all_cooldowns"))]

    # 不再依赖 scan_iter 全库扫描
    assert "scan_iter" not in body
    # 改为读渠道冻结索引集合
    assert "_INDEX_PROVIDERS" in body
    assert "smembers" in body
    # 索引 key 命名隔离：cooldown_index:* 避免与渠道名 'index' 时的普通冷却 key 冲突
    assert admin.RedisLimitBackend._INDEX_PROVIDERS.startswith("limit:cooldown_index:")
    # 冻结判定以精确 cooldown key 的 TTL 为准
    assert "remaining <= 0" in body



def test_custom_channel_modal_edits_upstream_stream_setting():
    html = admin_html()
    body = function_body(html, "showCustomConfig")

    assert "ccStream" in body
    assert "上游请求流式" in body
    assert "upstream_stream:" in body
    assert "cfg.upstream_stream ?? cfg.supports_stream" in body



def test_custom_channel_creation_posts_upstream_stream_not_supports_stream():
    html = admin_html()
    body = function_body(html, "readCustomChannelForm")

    assert "upstream_stream:" in body
    assert "supports_stream" not in body



def test_provider_summary_includes_numeric_updated_at_ts(monkeypatch):
    monkeypatch.setattr(admin, "_provider_models", lambda name: [])

    summary = admin._provider_summary_response(
        "p1",
        {
            "enabled": True,
            "accounts": [],
            "updated_at": 1710000000,
        },
    )

    assert summary["updated_at_ts"] == 1710000000


def test_provider_updated_at_ts_accepts_utc_datetime():
    updated_at = datetime(2024, 3, 9, 16, 0, 0, tzinfo=timezone.utc)

    assert admin._provider_updated_at_ts({"updated_at": updated_at}) == updated_at.timestamp()


def test_provider_updated_at_ts_accepts_iso_string_timestamp():
    updated_at = datetime(2024, 3, 9, 16, 0, 0, tzinfo=timezone.utc)

    assert admin._provider_updated_at_ts({"updated_at": updated_at.isoformat()}) == updated_at.timestamp()


def test_db_provider_config_source_serializes_updated_at_before_returning_config():
    db_source = (Path(admin.__file__).resolve().parent / "db.py").read_text(encoding="utf-8")
    get_start = db_source.index("async def get_provider_config")
    list_start = db_source.index("async def list_provider_configs")
    list_end = db_source.index("async def insert_request_log", list_start)

    provider_config_source = db_source[get_start:list_end]

    assert '"updated_at": row["updated_at"].isoformat()' in provider_config_source
    assert '"updated_at": row["updated_at"],' not in provider_config_source


def test_provider_config_with_iso_updated_at_is_json_serializable():
    config = {"updated_at": datetime(2024, 3, 9, 16, 0, 0, tzinfo=timezone.utc).isoformat()}

    json.dumps(config)


def test_db_provider_config_list_query_selects_updated_at():
    db_source = (Path(admin.__file__).resolve().parent / "db.py").read_text(encoding="utf-8")
    start = db_source.index("async def list_provider_configs")
    end = db_source.index("async def insert_request_log", start)
    list_provider_configs_source = db_source[start:end]

    assert "FROM provider_configs" in list_provider_configs_source
    assert "SELECT name, enabled, rate_limit, config, updated_at" in list_provider_configs_source


def test_channels_page_has_sort_and_model_filter_controls():
    html = admin_html()
    body = function_body(html, "renderChannelsPage")

    assert "channelSortBy" in body
    assert "channelSortDir" in body
    assert "channelModelFilter" in body
    assert "全部模型" in body


def test_channel_cards_filter_by_exact_model_and_sort_by_updated_at_default():
    html = admin_html()
    body = function_body(html, "renderChannelCards")

    assert "modelFilter" in body
    assert "channelHasModel" in body
    assert "updated_at_ts" in body
    assert "没有支持该模型的渠道" in body


def test_channel_name_sort_uses_provider_name_not_remark():
    html = admin_html()
    body = function_body(html, "renderChannelCards")

    assert "(p.remark || p.name || '')" not in body
    assert "if (sortBy === 'name') return (p.name || '').toLowerCase();" in body


def test_channel_model_helpers_normalize_object_ids_and_ignore_non_strings():
    html = admin_html()
    model_id_body = function_body(html, "channelModelId")
    values_body = function_body(html, "channelModelValues")
    has_model_body = function_body(html, "channelHasModel")

    assert "typeof model === 'string'" in model_id_body
    assert "model.model_id" in model_id_body
    assert "model.name" in model_id_body
    assert "model.id" in model_id_body
    assert "typeof value === 'string'" in model_id_body
    assert "channelModelId" in values_body
    assert "channelModelId" in has_model_body


def test_channel_sort_direction_only_applies_to_primary_comparison():
    html = admin_html()
    body = function_body(html, "renderChannelCards")

    assert "const primaryResult" in body
    assert "return sortDir === 'asc' ? primaryResult : -primaryResult;" in body
    assert "return String(a.name || '').localeCompare(String(b.name || ''));" in body
    assert "sortDir === 'asc' ? result : -result" not in body


def test_route_detail_uses_supported_channel_switches_not_manual_entry_add_delete():
    html = admin_html()
    body = function_body(html, "openRouteDetail")

    assert "routeChannelRows" in body
    assert "支持该模型的渠道" in body
    assert "+ 增加顺序项" not in body
    assert "re-account" not in body


def test_route_channel_rows_preserve_existing_enabled_order_and_default_all_enabled():
    html = admin_html()

    assert "function routeChannelRows(" in html
    assert "function collectRouteChannelEntries(" in html
    rows_body = function_body(html, "routeChannelRows")
    collect_body = function_body(html, "collectRouteChannelEntries")

    assert "supportedProviders" in rows_body
    assert "route.entries" in rows_body
    assert "route.__defaultAllChannels" in rows_body
    assert "route-channel-enabled" in rows_body
    assert "route-channel-row" in collect_body


def test_route_channel_rows_preserve_existing_account_in_dataset_and_overview():
    html = admin_html()
    rows_body = function_body(html, "routeChannelRows")
    collect_body = function_body(html, "collectRouteChannelEntries")
    detail_body = function_body(html, "openRouteDetail")

    assert "accountByProvider" in rows_body
    assert "data-account" in rows_body
    assert "existingAccount" in rows_body
    assert "指定账号" in rows_body
    assert "dataset.account" in collect_body
    assert "指定账号" in detail_body or "routeChannelRows(route)" in detail_body


def test_save_model_routing_normalizes_and_validates_routes_before_post():
    html = admin_html()
    save_body = function_body(html, "saveModelRouting")
    normalize_body = function_body(html, "normalizeRoutesForSave")
    validate_body = function_body(html, "validateRoutesForSave")

    assert "normalizeRoutesForSave" in save_body
    assert "validateRoutesForSave" in save_body
    assert "if (!validateRoutesForSave(routes)) return" in save_body
    assert "routes" in save_body
    assert "window._modelRoutes" in save_body
    assert "filter(e => e && e.provider)" in normalize_body
    assert "请先打开模型专用线路详情，选择模型并确认支持渠道后再保存" in validate_body
    assert "api('/admin/model-routing'" in save_body


def test_collect_route_entries_for_save_does_not_serialize_default_all_as_empty_entries():
    html = admin_html()

    if "function collectRouteEntriesForSave(" in html:
        body = function_body(html, "collectRouteEntriesForSave")
        assert "return []" not in body
        assert "collectRouteChannelEntries()" in body


def test_open_route_detail_saves_displayed_enabled_channel_entries():
    html = admin_html()
    body = function_body(html, "openRouteDetail")

    assert "entries: collectRouteChannelEntries()" in body or "entries: collectRouteEntriesForSave(route)" in body


def test_open_route_detail_refreshes_supported_channels_on_model_change_not_input():
    html = admin_html()
    body = function_body(html, "openRouteDetail")

    assert "modelInput.oninput" not in body
    assert "modelInput.onchange = () => refreshRouteChannelBody(route)" in body


def test_route_channel_switch_binding_uses_toggle_markup():
    html = admin_html()
    body = function_body(html, "bindRouteChannelSwitches")

    assert ".toggle input" in body
    assert ".switch input" not in body


def test_route_channel_switch_binding_tracks_explicit_user_edits():
    html = admin_html()
    body = function_body(html, "bindRouteChannelSwitches")

    assert "dataset.userEdited" in body


def test_refresh_route_channel_body_preserves_default_all_until_user_edits():
    html = admin_html()
    body = function_body(html, "refreshRouteChannelBody")

    assert "route.__defaultAllChannels" in body
    assert "dataset.userEdited" in body
    assert "entries" in body
    assert "[]" in body


def test_open_route_detail_refreshes_supported_channels_when_model_changes():
    html = admin_html()
    body = function_body(html, "openRouteDetail")

    assert "function refreshRouteChannelBody(" in html
    assert "refreshRouteChannelBody(route)" in body or "rdModel" in body


def test_refresh_route_channel_body_preserves_current_enabled_entries():
    html = admin_html()
    body = function_body(html, "refreshRouteChannelBody")

    assert "collectRouteChannelEntries()" in body
    assert "routeChannelRows(draft)" in body
    assert "bindRouteChannelSwitches()" in body


def test_provider_base_response_exposes_extra_retry_status_codes():
    response = admin._provider_base_response(
        "example",
        {"extra_retry_status_codes": [408, "409", 408, 700]},
    )
    assert response["extra_retry_status_codes"] == [408, 409]


def test_normalize_extra_retry_status_codes_rejects_invalid_values():
    assert admin._normalize_extra_retry_status_codes([409, "408", 409]) == [408, 409]
    for bad in ([99], [True], ["abc"], [700]):
        try:
            admin._normalize_extra_retry_status_codes(bad)
            assert False, f"expected HTTPException for {bad}"
        except admin.HTTPException:
            pass


def test_get_provider_retry_count_uses_channel_override(monkeypatch):
    cache = {"chan": {"retry_count": 4}}

    class FakeStore:
        def read_main(self):
            return {"retry": {"max_retries": 3}}

        store = type("obj", (), {"_providers_cache": cache})()

    monkeypatch.setattr(config, "CONFIG_STORE", FakeStore())

    assert config.Config.get_provider_retry_count("chan") == 4


def test_get_provider_retry_count_defaults_to_zero_when_unset(monkeypatch):
    cache = {"chan": {"timeout": 600}}

    class FakeStore:
        def read_main(self):
            return {"retry": {"max_retries": 3}}

        store = type("obj", (), {"_providers_cache": cache})()

    monkeypatch.setattr(config, "CONFIG_STORE", FakeStore())

    assert config.Config.get_provider_retry_count("chan") == 0
    assert config.Config.get_provider_retry_count("missing") == 0
    assert config.Config.get_global_retry_count() == 3


def test_get_provider_retry_count_bounds_and_disable(monkeypatch):
    cache = {
        "huge": {"retry_count": 99},
        "neg": {"retry_count": -5},
        "off": {"retry_count": 0},
    }

    class FakeStore:
        def read_main(self):
            return {"retry": {"max_retries": 3}}

        store = type("obj", (), {"_providers_cache": cache})()

    monkeypatch.setattr(config, "CONFIG_STORE", FakeStore())

    assert config.Config.get_provider_retry_count("huge") == 10
    assert config.Config.get_provider_retry_count("neg") == 0
    assert config.Config.get_provider_retry_count("off") == 0
