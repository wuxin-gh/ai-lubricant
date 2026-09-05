"""获取模型列表：空列表视为成功；仅新增模型 ID 去首个 "/" 前缀，已有模型不覆盖。"""
import asyncio

import pytest
from fastapi import HTTPException

import rate_limiter
from rate_limiter import ModelClientPool


# ----------------------------- 需求 1：空列表视为成功 -----------------------------


def test_fetch_unsaved_custom_provider_models_empty_success_returns_empty(monkeypatch):
    """上游 200 返回空列表（无 _last_fetch_models_error）→ 返回空列表，不报 502。"""
    import admin

    class _Provider:
        _last_fetch_models_error = ""

        async def fetch_upstream_model_list(self):
            return []

    monkeypatch.setattr(admin, "_runtime_accounts", lambda accounts: [{"username": "u", "password": "k"}])
    monkeypatch.setattr(admin, "_read_proxies", lambda: [])
    monkeypatch.setattr(admin, "_provider_extra", lambda name, cfg: {})
    monkeypatch.setattr(admin, "CustomProvider", lambda **kwargs: _Provider())

    payload = {
        "base_url": "https://example.com",
        "accounts": [{"password": "k"}],
        "chat_protocols": [{"protocol": "openai", "path": "/v1/chat/completions"}],
    }

    result = asyncio.run(admin._fetch_unsaved_custom_provider_models(payload))

    assert result == []


def test_fetch_unsaved_custom_provider_models_error_raises_502(monkeypatch):
    """上游请求失败（_last_fetch_models_error 有值）→ 仍抛 502，保持异常路径不变。"""
    import admin

    class _Provider:
        _last_fetch_models_error = "GET https://x/v1/models → HTTP 500"

        async def fetch_upstream_model_list(self):
            return []

    monkeypatch.setattr(admin, "_runtime_accounts", lambda accounts: [{"username": "u", "password": "k"}])
    monkeypatch.setattr(admin, "_read_proxies", lambda: [])
    monkeypatch.setattr(admin, "_provider_extra", lambda name, cfg: {})
    monkeypatch.setattr(admin, "CustomProvider", lambda **kwargs: _Provider())

    with pytest.raises(HTTPException) as exc:
        asyncio.run(admin._fetch_unsaved_custom_provider_models({
            "base_url": "https://example.com",
            "accounts": [{"password": "k"}],
            "chat_protocols": [{"protocol": "openai", "path": "/v1/chat/completions"}],
        }))
    assert exc.value.status_code == 502
    assert "上游模型列表获取失败" in exc.value.detail


# ----------------------------- 需求 2：仅新增模型去 "/" 前缀 -----------------------------


def test_strip_owner_prefix_only_for_new_models():
    """owner/model → 仅保留首个 / 之后；无 / 或尾部为空则原样返回。

    切前缀的实现已从 ModelClientPool._strip_owner_prefix 移到 channel 模块，
    好让 model_id 改写规则复用同一份逻辑；此处断言的行为契约不变。
    """
    from channel import strip_model_owner_prefix as strip
    assert strip("1111/glm-5.2") == "glm-5.2"
    assert strip("openai/gpt-4o") == "gpt-4o"
    assert strip("glm-5.2") == "glm-5.2"
    assert strip("org/team/model") == "team/model"
    assert strip("org/") == "org/"
    assert strip("") == ""


def _make_pool_with_provider(provider):
    from channel import Channel

    class _Pool:
        clients = [type("Client", (), {"provider": provider})()]
        channel = Channel("prov", {"provider_name": "prov"})

        async def get_initialized_provider(self):
            return self.clients[0].provider

    return _Pool()


def test_fetch_provider_upstream_models_new_prefers_short_id_tracked_keeps_existing(monkeypatch):
    """手动获取：新模型 model_id/current_model_id 去前缀；已跟踪模型沿用表内 model_id，不覆盖。"""

    class _Provider:
        auto_update_models = True
        _last_fetch_models_error = ""

        async def fetch_upstream_model_list(self):
            return [
                {"id": "1111/glm-5.2"},   # 新模型 → 去前缀
                {"id": "acme/kept"},      # 已跟踪 → 不覆盖
            ]

    pool = _make_pool_with_provider(_Provider())

    async def list_provider_models(cls, provider=None):
        return [{"provider": "prov", "upstream_model_id": "acme/kept", "model_id": "custom-kept"}]

    monkeypatch.setattr(ModelClientPool, "get_provider_pool", classmethod(lambda cls, name: pool))
    monkeypatch.setattr(rate_limiter.PostgresClient, "list_provider_models", classmethod(list_provider_models))

    result = asyncio.run(ModelClientPool.fetch_provider_upstream_models("prov"))
    by_upstream = {m["upstream_model_id"]: m for m in result["upstream_models"]}

    new = by_upstream["1111/glm-5.2"]
    assert new["tracked"] is False
    assert new["model_id"] == "glm-5.2"
    assert new["current_model_id"] == "glm-5.2"

    kept = by_upstream["acme/kept"]
    assert kept["tracked"] is True
    # 已跟踪：current_model_id 用表内值，不被裁剪覆盖。
    assert kept["current_model_id"] == "custom-kept"


def test_fetch_provider_upstream_models_returns_both_raw_and_rewritten(monkeypatch):
    """预览同时返回 raw_model_id（原始）与 model_id（改写后），前端开关在两者间切换，无需重新拉取。"""

    class _Provider:
        auto_update_models = True
        _last_fetch_models_error = ""

        async def fetch_upstream_model_list(self):
            return [{"id": "1111/glm-5.2:free"}]  # 含 owner/ 与后缀

    pool = _make_pool_with_provider(_Provider())
    pool.channel.apply({"model_id_rewrite_rules": [{"pattern": r":free$", "replacement": ""}]})

    async def list_provider_models(cls, provider=None):
        return []

    monkeypatch.setattr(ModelClientPool, "get_provider_pool", classmethod(lambda cls, name: pool))
    monkeypatch.setattr(rate_limiter.PostgresClient, "list_provider_models", classmethod(list_provider_models))

    result = asyncio.run(ModelClientPool.fetch_provider_upstream_models("prov"))
    row = {m["upstream_model_id"]: m for m in result["upstream_models"]}["1111/glm-5.2:free"]
    # 原始名原样保留，未切 /、未套规则
    assert row["raw_model_id"] == "1111/glm-5.2:free"
    # model_id 是改写后短名：去 owner/ 前缀 + 去掉 :free
    assert row["model_id"] == "glm-5.2"
    # 未跟踪：current_model_id 默认取改写后短名
    assert row["current_model_id"] == "glm-5.2"
    assert row["tracked"] is False
    # 规则确实改了名字 → is_regex=True，前端开关据此把这行滤出列表
    assert row["is_regex"] is True
    assert row["regex_model_id"] == "glm-5.2"


def test_fetch_provider_upstream_models_marks_is_regex_only_when_rules_hit(monkeypatch):
    """is_regex 只认「规则改了名字」：切 owner/ 前缀是无配置时的默认形态，不算命中。

    前端「模拟过滤及改写规则」开关按这个标记滤行，所以判定基线必须是切完前缀之后
    的值——否则带 owner/ 的模型会被误判成命中，一开开关整列表都空了。
    """

    class _Provider:
        auto_update_models = True
        _last_fetch_models_error = ""

        async def fetch_upstream_model_list(self):
            return [
                {"id": "1111/glm-5.2:free"},  # 规则命中
                {"id": "1111/glm-5.2"},       # 只切前缀，规则没动它
                {"id": "plain"},              # 既无前缀也不命中
            ]

    pool = _make_pool_with_provider(_Provider())
    pool.channel.apply({"model_id_rewrite_rules": [{"pattern": r"[-:]free$", "replacement": ""}]})

    async def list_provider_models(cls, provider=None):
        return []

    monkeypatch.setattr(ModelClientPool, "get_provider_pool", classmethod(lambda cls, name: pool))
    monkeypatch.setattr(rate_limiter.PostgresClient, "list_provider_models", classmethod(list_provider_models))

    result = asyncio.run(ModelClientPool.fetch_provider_upstream_models("prov"))
    by_upstream = {m["upstream_model_id"]: m for m in result["upstream_models"]}

    assert by_upstream["1111/glm-5.2:free"]["is_regex"] is True
    assert by_upstream["1111/glm-5.2"]["is_regex"] is False
    assert by_upstream["plain"]["is_regex"] is False


def test_fetch_provider_upstream_models_is_regex_false_without_rules(monkeypatch):
    """没配规则的渠道全员 is_regex=False——开关开了也不该滤掉任何行。"""

    class _Provider:
        auto_update_models = True
        _last_fetch_models_error = ""

        async def fetch_upstream_model_list(self):
            return [{"id": "1111/glm-5.2:free"}]

    pool = _make_pool_with_provider(_Provider())

    async def list_provider_models(cls, provider=None):
        return []

    monkeypatch.setattr(ModelClientPool, "get_provider_pool", classmethod(lambda cls, name: pool))
    monkeypatch.setattr(rate_limiter.PostgresClient, "list_provider_models", classmethod(list_provider_models))

    result = asyncio.run(ModelClientPool.fetch_provider_upstream_models("prov"))
    row = result["upstream_models"][0]
    assert row["is_regex"] is False
    assert row["regex_model_id"] == "glm-5.2:free"


def test_fetch_provider_upstream_models_preserves_root_and_explicit_raw_metadata(monkeypatch):
    """Custom 根级字段可导入；显式 raw 存在时仍优先返回上游原始对象。"""

    explicit_raw = {"id": "raw/model", "context_length": 131072}

    class _Provider:
        _last_fetch_models_error = ""

        async def fetch_upstream_model_list(self):
            return [
                {"id": "root/model", "max_context_tokens": 32768, "is_thinking": True},
                {"id": "wrapped/model", "max_context_tokens": 4096, "raw": explicit_raw},
            ]

    pool = _make_pool_with_provider(_Provider())

    async def list_provider_models(cls, provider=None):
        return []

    monkeypatch.setattr(ModelClientPool, "get_provider_pool", classmethod(lambda cls, name: pool))
    monkeypatch.setattr(rate_limiter.PostgresClient, "list_provider_models", classmethod(list_provider_models))

    result = asyncio.run(ModelClientPool.fetch_provider_upstream_models("prov"))
    by_upstream = {m["upstream_model_id"]: m for m in result["upstream_models"]}

    assert by_upstream["root/model"]["raw"]["max_context_tokens"] == 32768
    assert by_upstream["root/model"]["raw"]["is_thinking"] is True
    assert by_upstream["wrapped/model"]["raw"] is explicit_raw


def test_refresh_models_strips_prefix_only_on_add_not_update(monkeypatch):
    """自动同步：新增模型用裁剪后的 model_id；已有模型映射不被前缀裁剪改动。"""

    class _Provider:
        auto_update_models = True
        _last_fetch_models_error = ""

        async def fetch_upstream_model_list(self):
            return [{"id": "1111/glm-5.2"}]

    pool = _make_pool_with_provider(_Provider())

    provider_rows = []
    upserts = []
    deletes = []

    async def list_provider_models(cls, provider=None):
        return list(provider_rows)

    async def upsert_provider_model(cls, provider, upstream_model_id, model_id):
        upserts.append((provider, upstream_model_id, model_id))
        provider_rows.append({"provider": provider, "upstream_model_id": upstream_model_id, "model_id": model_id})

    async def delete_provider_model(cls, provider, upstream_model_id):
        deletes.append((provider, upstream_model_id))
        return True

    async def rebuild_model_response_cache(cls):
        return None

    monkeypatch.setattr(ModelClientPool, "get_provider_names", classmethod(lambda cls: ["prov"]))
    monkeypatch.setattr(ModelClientPool, "get_provider_pool", classmethod(lambda cls, name: pool))
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"prov": pool})
    monkeypatch.setattr(rate_limiter.PostgresClient, "list_provider_models", classmethod(list_provider_models))
    monkeypatch.setattr(rate_limiter.PostgresClient, "upsert_provider_model", classmethod(upsert_provider_model))
    monkeypatch.setattr(rate_limiter.PostgresClient, "delete_provider_model", classmethod(delete_provider_model))
    monkeypatch.setattr(ModelClientPool, "rebuild_model_response_cache", classmethod(rebuild_model_response_cache))

    asyncio.run(ModelClientPool.refresh_models())

    # 新增时裁剪为 glm-5.2
    assert upserts == [("prov", "1111/glm-5.2", "glm-5.2")]
    assert deletes == []

    # 再次同步：该行已存在且映射未漂移 → 不再 update、不重复写、不删。
    upserts.clear()
    asyncio.run(ModelClientPool.refresh_models())
    assert upserts == []
    assert deletes == []


def _refresh_pool_with_rules(rules):
    """带改写规则的渠道池，供 refresh_models 过滤测试复用。"""
    class _Provider:
        auto_update_models = True
        _last_fetch_models_error = ""

        async def fetch_upstream_model_list(self):
            return [{"id": "glm-5.2"}, {"id": "glm-5.2:free"}]

    pool = _make_pool_with_provider(_Provider())
    pool.channel.apply({"model_id_rewrite_rules": rules})
    return pool


def _patch_refresh(monkeypatch, pool, provider_rows):
    upserts, deletes = [], []

    async def list_provider_models(cls, provider=None):
        return list(provider_rows)

    async def upsert_provider_model(cls, provider, upstream_model_id, model_id):
        upserts.append((provider, upstream_model_id, model_id))
        provider_rows.append({"provider": provider, "upstream_model_id": upstream_model_id, "model_id": model_id})

    async def delete_provider_model(cls, provider, upstream_model_id):
        deletes.append((provider, upstream_model_id))
        return True

    async def rebuild_model_response_cache(cls):
        return None

    monkeypatch.setattr(ModelClientPool, "get_provider_names", classmethod(lambda cls: ["prov"]))
    monkeypatch.setattr(ModelClientPool, "get_provider_pool", classmethod(lambda cls, name: pool))
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"prov": pool})
    monkeypatch.setattr(rate_limiter.PostgresClient, "list_provider_models", classmethod(list_provider_models))
    monkeypatch.setattr(rate_limiter.PostgresClient, "upsert_provider_model", classmethod(upsert_provider_model))
    monkeypatch.setattr(rate_limiter.PostgresClient, "delete_provider_model", classmethod(delete_provider_model))
    monkeypatch.setattr(ModelClientPool, "rebuild_model_response_cache", classmethod(rebuild_model_response_cache))
    return upserts, deletes


def test_refresh_keeps_only_regex_hit_models(monkeypatch):
    """规则就是过滤条件：满足（is_regex=true）的留下并改名，不满足的丢掉。

    上游 glm-5.2 / glm-5.2:free，规则 [-:]free$ 只搜到 glm-5.2:free →
    它留下、改名成 glm-5.2 进库；glm-5.2 没被搜到，不进库。
    """
    pool = _refresh_pool_with_rules([{"pattern": r"[-:]free$", "replacement": ""}])
    upserts, deletes = _patch_refresh(monkeypatch, pool, [])

    asyncio.run(ModelClientPool.refresh_models())

    by_uid = {uid: mid for _, uid, mid in upserts}
    # 满足规则的 free 模型保留，改名为 glm-5.2
    assert "glm-5.2:free" in by_uid and by_uid["glm-5.2:free"] == "glm-5.2"
    # 不满足规则的 glm-5.2 不进库
    assert "glm-5.2" not in by_uid
    assert deletes == []


def test_refresh_removes_existing_non_matching_models(monkeypatch):
    """已入库但不满足规则的模型，再次同步被删掉。"""
    pool = _refresh_pool_with_rules([{"pattern": r"[-:]free$", "replacement": ""}])
    # 库里已有 glm-5.2（不满足规则），上游也有 glm-5.2:free（满足）
    rows = [{"provider": "prov", "upstream_model_id": "glm-5.2", "model_id": "glm-5.2"}]
    upserts, deletes = _patch_refresh(monkeypatch, pool, rows)

    asyncio.run(ModelClientPool.refresh_models())

    assert ("prov", "glm-5.2") in deletes
    assert "glm-5.2" not in {uid for _, uid, _ in upserts}
    assert "glm-5.2:free" in {uid for _, uid, _ in upserts}


def test_refresh_no_rules_keeps_all_unchanged(monkeypatch):
    """没配规则的渠道：所有上游模型照常导入，不过滤。"""
    pool = _refresh_pool_with_rules([])  # 空规则 → has_rules=False
    upserts, deletes = _patch_refresh(monkeypatch, pool, [])

    asyncio.run(ModelClientPool.refresh_models())

    upserted_ids = [uid for _, uid, _ in upserts]
    assert "glm-5.2" in upserted_ids
    assert "glm-5.2:free" in upserted_ids
    assert deletes == []


# ------------------- 需求 3：拉上游模型列表不选禁用账号 -------------------


def _stub_client(username: str, *, disabled: bool, is_init: bool = True):
    """最小 AccountClient 替身：只提供 get_initialized_provider 用到的三个面。"""
    provider = type("P", (), {"PROVIDER_NAME": "prov", "is_init": lambda self: is_init})()
    return type("C", (), {"username": username, "disabled": disabled, "provider": provider})()


def _real_pool(clients):
    from rate_limiter import ProviderPool

    pool = ProviderPool("prov", type("CC", (), {}))
    pool.clients = list(clients)
    return pool


def test_get_initialized_provider_skips_disabled_accounts():
    """禁用账号即使凭据已初始化也不被选中去拉上游模型列表。"""
    disabled = _stub_client("off", disabled=True)
    live = _stub_client("on", disabled=False)
    pool = _real_pool([disabled, live])

    # 轮询起点落在禁用账号上，也必须跳过它选到 live。
    for _ in range(3):
        assert asyncio.run(pool.get_initialized_provider()) is live.provider


def test_get_initialized_provider_returns_none_when_all_disabled():
    """账号全部禁用 → 返回 None，不退化成随便挑一个。"""
    pool = _real_pool([_stub_client("a", disabled=True), _stub_client("b", disabled=True)])
    assert asyncio.run(pool.get_initialized_provider()) is None


def test_get_initialized_provider_still_ignores_uninitialized():
    """未初始化的账号仍然跳过（原有语义不变）。"""
    pool = _real_pool([_stub_client("a", disabled=False, is_init=False)])
    assert asyncio.run(pool.get_initialized_provider()) is None


def test_fetch_upstream_models_all_disabled_reports_disabled_not_init_failure(monkeypatch):
    """账号全禁用时报「已全部禁用」，而不是误导性的「均未完成初始化」。"""
    pool = _real_pool([_stub_client("a", disabled=True)])
    monkeypatch.setattr(ModelClientPool, "get_provider_pool", classmethod(lambda cls, name: pool))

    with pytest.raises(HTTPException) as exc:
        asyncio.run(ModelClientPool.fetch_provider_upstream_models("prov"))
    assert exc.value.status_code == 400
    assert "已全部禁用" in exc.value.detail
