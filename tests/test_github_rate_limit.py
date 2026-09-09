"""GitHub API 限流识别 + 直白报错（含恢复时间）回归。

起因：上传 APK 时服务端转传 GitHub Release 第一步 ``get_release_by_tag`` 撞 403
限流，原始报错是一大段 JSON、看不出原因也不知道要等多久。这里把限流识别抽成
共享 helper，覆盖到发行链、探针、榜单同步与巡检各出网点；同步层额外做「熔断」——
配额耗尽后立刻停掉本轮探针，不再对剩下的条目逐个打 403。
"""
from __future__ import annotations

import asyncio
import time

import pytest

from user_platform.git_clients import (
    RATE_LIMIT_PREFIX,
    GitClientError,
    github_rate_limit_message,
    is_rate_limit_error,
)


# ── 共享 fake 响应 ──────────────────────────────────────────────────────────
class _Resp:
    """轻量假响应：status / headers / 异步 text() / json()，对齐 aiohttp 与
    proxy_manager.OutboundResponse 的出网点用到的接口。"""

    def __init__(self, status: int, headers: dict | None = None, *, text: str = "", payload=None):
        self.status = status
        self.headers = headers or {}
        self._text = text
        self._payload = payload

    async def text(self):
        return self._text

    async def json(self):
        return self._payload

    async def read(self):
        return self._text.encode("utf-8", "replace")


class _RespCM:
    def __init__(self, resp: _Resp):
        self._resp = resp

    async def __aenter__(self):
        return self._resp

    async def __aexit__(self, *exc):
        return False


class _Session:
    def __init__(self, resp: _Resp):
        self._resp = resp

    def get(self, *args, **kwargs):
        return _RespCM(self._resp)

    def post(self, *args, **kwargs):
        return _RespCM(self._resp)


class _SessionCM:
    def __init__(self, resp: _Resp):
        self._resp = resp

    async def __aenter__(self):
        return _Session(self._resp)

    async def __aexit__(self, *exc):
        return False


def _primary_resp() -> _Resp:
    return _Resp(
        403,
        {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(int(time.time()) + 1800),
         "X-RateLimit-Limit": "5000"},
        text='{"message": "API rate limit exceeded for user ID 76577283."}',
    )


# ── 共享测试小件 ──────────────────────────────────────────────────────────────
async def _async_noop(*args, **kwargs):
    return None


async def _no_hints():
    return {}


async def _budget_ok(proxy_id=""):
    return 4900  # 充足：不触发预算护栏


async def _budget_low(proxy_id=""):
    return 120  # 低于默认阈值 500：触发预算护栏


# ── helper 本体 ─────────────────────────────────────────────────────────────
def test_primary_rate_limit_message_carries_cause_and_recovery():
    msg = github_rate_limit_message(_primary_resp())
    assert msg is not None
    assert msg.startswith(RATE_LIMIT_PREFIX)
    assert "配额 5000 次/小时已用尽" in msg
    assert "预计" in msg and "恢复" in msg
    assert "分钟" in msg  # 1800s 倒计时落在分钟档（29-30 之间，不锁定具体数）
    assert is_rate_limit_error(msg)


def test_secondary_rate_limit_uses_retry_after():
    resp = _Resp(429, {"Retry-After": "60"},
                 text='{"message": "You have exceeded a secondary rate limit."}')
    msg = github_rate_limit_message(resp)
    assert msg is not None
    assert "二级限流" in msg
    assert "恢复" in msg


def test_non_rate_limit_403_returns_none():
    # 权限/凭证类 403（remaining 非零、body 无 rate limit 字样）不归限流管。
    resp = _Resp(403, {"X-RateLimit-Remaining": "4999"},
                 text='{"message": "Resource not accessible by integration"}')
    assert github_rate_limit_message(resp) is None


def test_401_and_200_are_not_rate_limit():
    assert github_rate_limit_message(_Resp(401, text="bad credentials")) is None
    assert github_rate_limit_message(_Resp(200, payload={})) is None


def test_is_rate_limit_error_detects_prefix_only():
    assert is_rate_limit_error("GitHub API 限流：…")
    assert not is_rate_limit_error("HTTP 403")
    assert not is_rate_limit_error(None)


def test_message_without_reset_headers_still_gives_cause():
    # 代理层把 X-RateLimit-Reset / Retry-After 都剥掉的极端情况：仍给出原因。
    resp = _Resp(403, {"X-RateLimit-Remaining": "0"}, text="API rate limit exceeded")
    msg = github_rate_limit_message(resp, resp._text)
    assert msg is not None
    assert "配额" in msg
    assert "请稍后重试" in msg
    assert "恢复" not in msg  # 没有恢复时刻可读


# ── 发行链：get_release_by_tag 抛直白原因 ────────────────────────────────────
def test_get_release_by_tag_raises_friendly_rate_limit(monkeypatch):
    import user_platform.git_clients as gc
    from user_platform.marketplace import github as gh

    monkeypatch.setattr(gc, "_http_session", lambda: _SessionCM(_primary_resp()))
    with pytest.raises(GitClientError) as ei:
        asyncio.run(gh.get_release_by_tag("o", "r", "tok", "device-control-v0.1.7"))
    msg = str(ei.value)
    assert msg.startswith(RATE_LIMIT_PREFIX)
    assert "恢复" in msg
    assert "device-control-v0.1.7" not in msg  # 限流时不暴露内部 tag


def test_get_release_by_tag_non_rate_limit_keeps_status_in_message(monkeypatch):
    import user_platform.git_clients as gc
    from user_platform.marketplace import github as gh

    resp = _Resp(403, {"X-RateLimit-Remaining": "4999"},
                 text='{"message": "Resource not accessible by integration"}')
    monkeypatch.setattr(gc, "_http_session", lambda: _SessionCM(resp))
    with pytest.raises(GitClientError) as ei:
        asyncio.run(gh.get_release_by_tag("o", "r", "tok", "v1"))
    assert "HTTP 403" in str(ei.value)


# ── 探针层 ─────────────────────────────────────────────────────────────────
def test_probe_get_json_raises_friendly_rate_limit(monkeypatch):
    from user_platform.marketplace import leaderboard_probe as lp
    import providers.proxy_manager as pm

    class _Manager:
        async def request(self, **kwargs):
            return _primary_resp()

    monkeypatch.setattr(pm, "get_proxy_manager", lambda: _Manager())
    with pytest.raises(RuntimeError) as ei:
        asyncio.run(lp.get_json("https://api.github.com/repos/o/r"))
    assert RATE_LIMIT_PREFIX in str(ei.value)


def test_probe_get_json_non_rate_limit_keeps_http_status(monkeypatch):
    from user_platform.marketplace import leaderboard_probe as lp
    import providers.proxy_manager as pm

    class _Manager:
        async def request(self, **kwargs):
            return _Resp(404, text="not found")

    monkeypatch.setattr(pm, "get_proxy_manager", lambda: _Manager())
    with pytest.raises(RuntimeError) as ei:
        asyncio.run(lp.get_json("https://api.github.com/repos/o/r/git/trees/x"))
    assert "HTTP 404" in str(ei.value)


def test_fetch_text_reraises_rate_limit(monkeypatch):
    from user_platform.marketplace import leaderboard_probe as lp

    async def fake_raw(full_name, path, ref, *, proxy_id="", max_bytes=lp._MAX_FILE_BYTES):
        return None  # raw 未命中 → 落 Contents API

    async def fake_get_json(url, *, proxy_id=""):
        raise RuntimeError(f"{RATE_LIMIT_PREFIX}：配额 5000 次/小时已用尽")

    monkeypatch.setattr(lp, "fetch_raw_text", fake_raw)
    monkeypatch.setattr(lp, "_gh_get_json", fake_get_json)
    with pytest.raises(RuntimeError) as ei:
        asyncio.run(lp._gh_fetch_text("o/r", "README.md", "main"))
    assert RATE_LIMIT_PREFIX in str(ei.value)


def test_fetch_text_swallows_non_rate_limit_errors(monkeypatch):
    from user_platform.marketplace import leaderboard_probe as lp

    async def fake_raw(full_name, path, ref, *, proxy_id="", max_bytes=lp._MAX_FILE_BYTES):
        return None  # raw 未命中 → 落 Contents API

    async def fake_get_json(url, *, proxy_id=""):
        raise RuntimeError("HTTP 404")

    monkeypatch.setattr(lp, "fetch_raw_text", fake_raw)
    monkeypatch.setattr(lp, "_gh_get_json", fake_get_json)
    assert asyncio.run(lp._gh_fetch_text("o/r", "README.md", "main")) is None


def test_fetch_text_prefers_raw_and_skips_api(monkeypatch):
    """raw 命中时 _gh_fetch_text 直接返回，绝不碰 Contents API（省配额）。"""
    from user_platform.marketplace import leaderboard_probe as lp

    async def fake_raw(full_name, path, ref, *, proxy_id="", max_bytes=lp._MAX_FILE_BYTES):
        return "# hello from raw"

    async def boom_get_json(url, *, proxy_id=""):
        raise AssertionError("raw 命中后不应再调 Contents API")

    monkeypatch.setattr(lp, "fetch_raw_text", fake_raw)
    monkeypatch.setattr(lp, "_gh_get_json", boom_get_json)
    assert asyncio.run(lp._gh_fetch_text("o/r", "README.md", "main")) == "# hello from raw"


def test_raw_fetch_text_returns_text_on_200(monkeypatch):
    """raw 200 → 文本；不占 api.github.com（URL 打的是 raw.githubusercontent.com）。"""
    from user_platform.marketplace import leaderboard_probe as lp
    import providers.proxy_manager as pm

    seen = {}

    class _Manager:
        async def request(self, *, url, **kwargs):
            seen["url"] = url
            return _Resp(200, text="SKILL body")

    monkeypatch.setattr(pm, "get_proxy_manager", lambda: _Manager())
    monkeypatch.setattr(lp, "_raw_disabled_until", 0.0, raising=False)
    out = asyncio.run(lp.fetch_raw_text("o/r", "SKILL.md", "main"))
    assert out == "SKILL body"
    assert seen["url"].startswith("https://raw.githubusercontent.com/o/r/main/")


def test_raw_fetch_text_none_on_404(monkeypatch):
    """raw 404 → None（调用方回 Contents API，不当真缺失）。"""
    from user_platform.marketplace import leaderboard_probe as lp
    import providers.proxy_manager as pm

    class _Manager:
        async def request(self, **kwargs):
            return _Resp(404, text="not found")

    monkeypatch.setattr(pm, "get_proxy_manager", lambda: _Manager())
    monkeypatch.setattr(lp, "_raw_disabled_until", 0.0, raising=False)
    assert asyncio.run(lp.fetch_raw_text("o/r", "missing.md", "main")) is None


def test_raw_fetch_text_skipped_for_head_ref(monkeypatch):
    """ref=HEAD/空 raw 解析不了 → 直接 None，连请求都不发。"""
    from user_platform.marketplace import leaderboard_probe as lp
    import providers.proxy_manager as pm

    class _Manager:
        async def request(self, **kwargs):
            raise AssertionError("HEAD/空 ref 不应发 raw 请求")

    monkeypatch.setattr(pm, "get_proxy_manager", lambda: _Manager())
    monkeypatch.setattr(lp, "_raw_disabled_until", 0.0, raising=False)
    assert asyncio.run(lp.fetch_raw_text("o/r", "README.md", "HEAD")) is None
    assert asyncio.run(lp.fetch_raw_text("o/r", "README.md", "")) is None


# ── 榜单同步：限流熔断 ──────────────────────────────────────────────────────


def test_sync_once_stops_probing_on_rate_limit(monkeypatch):
    """探针返回限流错误后，本轮立即停：只 upsert 已探的第一条，后续 board 不再拉。"""
    import user_platform.marketplace.leaderboard_sync as ls
    import user_platform.marketplace.leaderboard_probe as lp
    import user_platform.marketplace.source_config as sc
    import marketplace_leaderboard_store as store
    import marketplace_sync_run_store as srs

    async def fake_settings():
        return {
            "leaderboard_repo": "o/r",
            "leaderboard_boards": "skills,mcp",
            "leaderboard_sync_interval_hours": 24,
        }

    async def fake_is_enabled():
        return True

    fetched = []

    async def fake_fetch_board(session, repo, path, *, proxy_id=""):
        fetched.append(path)
        return {"repos": [
            {"full_name": "a/one", "updated_at": "2026-01-01T00:00:00Z"},
            {"full_name": "b/two", "updated_at": "2026-01-01T00:00:00Z"},
            {"full_name": "c/three", "updated_at": "2026-01-01T00:00:00Z"},
        ]}

    rate_limit_msg = f"{RATE_LIMIT_PREFIX}：配额 5000 次/小时已用尽，预计 10:40 恢复（还有 30 分钟）"

    async def fake_attach_probe(item, *, proxy_id=""):
        # 第一条探针就把限流原因写进 probe.error，模拟「配额已耗尽」。
        item["external_data"]["probe"] = {"error": rate_limit_msg}
        return item

    upserted = []

    async def fake_upsert(item):
        upserted.append(item["repo_full_name"])

    monkeypatch.setattr(ls, "_settings", fake_settings)
    monkeypatch.setattr(ls, "is_enabled", fake_is_enabled)
    monkeypatch.setattr(ls, "_fetch_board", fake_fetch_board)
    monkeypatch.setattr(lp, "attach_probe", fake_attach_probe)
    monkeypatch.setattr(store, "upsert_item", fake_upsert)
    monkeypatch.setattr(store, "load_probe_reuse_hints", _no_hints)
    monkeypatch.setattr(ls, "_probe_budget_remaining", _budget_ok)
    monkeypatch.setattr(srs, "record_run", _async_noop)
    monkeypatch.setattr(sc, "record_source_sync", _async_noop)

    result = asyncio.run(ls.sync_once())

    assert result["ok"] is False
    # 只有第一个 board 被拉：第二个 board 因限流熔断未触达。
    assert len(fetched) == 1
    # 已探的第一条照常 upsert（错误 probe 被 pop，旧 probe 由 SQL 保留规则带回）；
    # 第二、三条本轮跳过。
    assert upserted == ["a/one"]
    assert any(is_rate_limit_error(e) for e in result["errors"])
    assert "提前结束" in result["detail"]


def test_sync_once_runs_full_without_rate_limit(monkeypatch):
    """无限制时同步照常走完所有 board —— 锁住熔断逻辑没有误伤正常路径。"""
    import user_platform.marketplace.leaderboard_sync as ls
    import user_platform.marketplace.leaderboard_probe as lp
    import user_platform.marketplace.source_config as sc
    import marketplace_leaderboard_store as store
    import marketplace_sync_run_store as srs

    async def fake_settings():
        return {"leaderboard_repo": "o/r", "leaderboard_boards": "skills,mcp",
                "leaderboard_sync_interval_hours": 24}

    async def fake_is_enabled():
        return True

    fetched = []

    async def fake_fetch_board(session, repo, path, *, proxy_id=""):
        fetched.append(path)
        return {"repos": [
            {"full_name": "a/one", "updated_at": "2026-01-01T00:00:00Z"},
            {"full_name": "b/two", "updated_at": "2026-01-01T00:00:00Z"},
        ]}

    async def fake_attach_probe(item, *, proxy_id=""):
        item["external_data"]["probe"] = {"error": ""}
        return item

    upserted = []

    async def fake_upsert(item):
        upserted.append(item["repo_full_name"])

    monkeypatch.setattr(ls, "_settings", fake_settings)
    monkeypatch.setattr(ls, "is_enabled", fake_is_enabled)
    monkeypatch.setattr(ls, "_fetch_board", fake_fetch_board)
    monkeypatch.setattr(lp, "attach_probe", fake_attach_probe)
    monkeypatch.setattr(store, "upsert_item", fake_upsert)
    monkeypatch.setattr(store, "load_probe_reuse_hints", _no_hints)
    monkeypatch.setattr(ls, "_probe_budget_remaining", _budget_ok)
    monkeypatch.setattr(srs, "record_run", _async_noop)
    monkeypatch.setattr(sc, "record_source_sync", _async_noop)

    result = asyncio.run(ls.sync_once())

    assert result["ok"] is True
    assert len(fetched) == 2  # 两个 board 都拉了
    assert len(upserted) == 4  # skills 2 + mcp 2
    assert "提前结束" not in result["detail"]
    # 探针统计上 detail：全部走了网络路径。
    assert "探针复用 0、新探 4" in result["detail"]


# ── 探针复用（零网络）与预算护栏 ─────────────────────────────────────────────
def _hint(*, error="", fetched_at="2026-09-07T00:00:00+00:00", updated="2026-01-01T00:00:00+00:00"):
    return {"probe_error": error, "fetched_at": fetched_at, "upstream_updated_at": updated,
            "stack": {"languages": {"Python": 10}}, "stack_tags": ["python"]}


def test_probe_reusable_requires_healthy_fresh_and_unchanged():
    from user_platform.marketplace.leaderboard_sync import _probe_reusable

    external = {"upstream_updated_at": "2026-01-01T00:00:00+00:00"}
    # 健康 + 新鲜 + 上游没变 → 复用
    assert _probe_reusable(_hint(), external) is True
    # 上轮探针失败（含限流）→ 重探
    assert _probe_reusable(_hint(error="GitHub API 限流：…"), external) is False
    # 上游有更新 → 重探
    assert _probe_reusable(_hint(), {"upstream_updated_at": "2026-02-01T00:00:00+00:00"}) is False
    # 探针超龄（默认 7 天）→ 强制重探一次
    assert _probe_reusable(_hint(fetched_at="2026-01-01T00:00:00+00:00"), external) is False
    # 上游两侧都没给 updated_at（None==None）→ 复用（由年龄上限兜底）
    assert _probe_reusable(_hint(updated=None), {"upstream_updated_at": None}) is True
    # 没有上轮记录 → 探
    assert _probe_reusable(None, external) is False
    # fetched_at 解析不了 → 探
    assert _probe_reusable(_hint(fetched_at="not-a-date"), external) is False


def test_probe_reuse_disabled_via_env(monkeypatch):
    """LEADERBOARD_PROBE_MAX_AGE_DAYS=0 = 永不复用（每轮全量重探的逃生门）。"""
    from user_platform.marketplace.leaderboard_sync import _probe_reusable

    monkeypatch.setenv("LEADERBOARD_PROBE_MAX_AGE_DAYS", "0")
    external = {"upstream_updated_at": "2026-01-01T00:00:00+00:00"}
    assert _probe_reusable(_hint(), external) is False


def test_sync_once_reuses_unchanged_probes_without_network(monkeypatch):
    """上游没变的条目零网络复用：不调 attach_probe、external_data 不带 probe 键、stack 带回。"""
    import user_platform.marketplace.leaderboard_sync as ls
    import user_platform.marketplace.leaderboard_probe as lp
    import user_platform.marketplace.source_config as sc
    import marketplace_leaderboard_store as store
    import marketplace_sync_run_store as srs

    async def fake_settings():
        return {"leaderboard_repo": "o/r", "leaderboard_boards": "skills",
                "leaderboard_sync_interval_hours": 24}

    async def fake_is_enabled():
        return True

    async def fake_fetch_board(session, repo, path, *, proxy_id=""):
        return {"repos": [
            {"full_name": "a/unchanged", "updated_at": "2026-01-01T00:00:00Z"},
            {"full_name": "b/changed", "updated_at": "2026-03-01T00:00:00Z"},
            {"full_name": "c/newcomer", "updated_at": "2026-03-01T00:00:00Z"},
        ]}

    probed_calls = []

    async def fake_attach_probe(item, *, proxy_id=""):
        probed_calls.append(item["repo_full_name"])
        item["external_data"]["probe"] = {"error": "", "fresh": True}
        return item

    upserted = {}

    async def fake_upsert(item):
        upserted[item["repo_full_name"]] = item

    hints = {
        ("agent-leaderboard", "skills", "a/unchanged"): _hint(updated="2026-01-01T00:00:00+00:00"),
        ("agent-leaderboard", "skills", "b/changed"): _hint(updated="2026-01-01T00:00:00+00:00"),
        # c/newcomer 无历史 → 必探
    }

    async def fake_hints():
        return hints

    monkeypatch.setattr(ls, "_settings", fake_settings)
    monkeypatch.setattr(ls, "is_enabled", fake_is_enabled)
    monkeypatch.setattr(ls, "_fetch_board", fake_fetch_board)
    monkeypatch.setattr(lp, "attach_probe", fake_attach_probe)
    monkeypatch.setattr(store, "upsert_item", fake_upsert)
    monkeypatch.setattr(store, "load_probe_reuse_hints", fake_hints)
    monkeypatch.setattr(ls, "_probe_budget_remaining", _budget_ok)
    monkeypatch.setattr(srs, "record_run", _async_noop)
    monkeypatch.setattr(sc, "record_source_sync", _async_noop)

    result = asyncio.run(ls.sync_once())

    # 只有变化的 b/changed 和新条目 c/newcomer 走了网络探针。
    assert probed_calls == ["b/changed", "c/newcomer"]
    # 复用条目：external_data 不带 probe 键（upsert 保留规则在 SQL 侧移植旧 probe），
    # stack/stack_tags 从 hint 带回写列。
    reused_item = upserted["a/unchanged"]
    assert "probe" not in reused_item["external_data"]
    assert reused_item["stack"] == {"languages": {"Python": 10}}
    assert reused_item["stack_tags"] == ["python"]
    # 新探条目照常带新 probe。
    assert upserted["b/changed"]["external_data"]["probe"] == {"error": "", "fresh": True}
    assert "探针复用 1、新探 2" in result["detail"]
    assert result["ok"] is True


def test_sync_once_skips_new_probes_when_budget_low(monkeypatch):
    """余量不足：本轮只同步元数据——有旧 stack 带回，新条目不探也不带 probe。"""
    import user_platform.marketplace.leaderboard_sync as ls
    import user_platform.marketplace.leaderboard_probe as lp
    import user_platform.marketplace.source_config as sc
    import marketplace_leaderboard_store as store
    import marketplace_sync_run_store as srs

    async def fake_settings():
        return {"leaderboard_repo": "o/r", "leaderboard_boards": "skills",
                "leaderboard_sync_interval_hours": 24}

    async def fake_is_enabled():
        return True

    async def fake_fetch_board(session, repo, path, *, proxy_id=""):
        return {"repos": [
            {"full_name": "a/known", "updated_at": "2026-01-01T00:00:00Z"},
            {"full_name": "b/fresh", "updated_at": "2026-03-01T00:00:00Z"},
        ]}

    async def fake_attach_probe(item, *, proxy_id=""):
        raise AssertionError("预算不足时不应触网探针")

    async def fake_hints():
        return {("agent-leaderboard", "skills", "a/known"): _hint(updated="2026-01-01T00:00:00+00:00")}

    upserted = {}

    async def fake_upsert(item):
        upserted[item["repo_full_name"]] = item

    monkeypatch.setattr(ls, "_settings", fake_settings)
    monkeypatch.setattr(ls, "is_enabled", fake_is_enabled)
    monkeypatch.setattr(ls, "_fetch_board", fake_fetch_board)
    monkeypatch.setattr(lp, "attach_probe", fake_attach_probe)
    monkeypatch.setattr(store, "upsert_item", fake_upsert)
    monkeypatch.setattr(store, "load_probe_reuse_hints", fake_hints)
    monkeypatch.setattr(ls, "_probe_budget_remaining", _budget_low)
    monkeypatch.setattr(srs, "record_run", _async_noop)
    monkeypatch.setattr(sc, "record_source_sync", _async_noop)

    result = asyncio.run(ls.sync_once())

    # 元数据照常落库，两条都不探（a/known 复用、b/fresh 跳过）。
    assert set(upserted) == {"a/known", "b/fresh"}
    assert "probe" not in upserted["a/known"]["external_data"]
    assert "probe" not in upserted["b/fresh"]["external_data"]
    # 旧 stack 带回；新条目无历史 → 空。
    assert upserted["a/known"]["stack"] == {"languages": {"Python": 10}}
    assert upserted["b/fresh"]["stack"] == {}
    assert "配额不足跳过 1" in result["detail"]


def test_rate_limited_probe_preserves_old_probe_on_upsert(monkeypatch):
    """撞限流的那条：错误 probe pop 掉再落库（upsert 保留规则留住旧 probe），随后熔断。"""
    import user_platform.marketplace.leaderboard_sync as ls
    import user_platform.marketplace.leaderboard_probe as lp
    import user_platform.marketplace.source_config as sc
    import marketplace_leaderboard_store as store
    import marketplace_sync_run_store as srs

    async def fake_settings():
        return {"leaderboard_repo": "o/r", "leaderboard_boards": "skills,mcp",
                "leaderboard_sync_interval_hours": 24}

    async def fake_is_enabled():
        return True

    async def fake_fetch_board(session, repo, path, *, proxy_id=""):
        return {"repos": [
            {"full_name": "a/victim", "updated_at": "2026-03-01T00:00:00Z"},
            {"full_name": "b/after", "updated_at": "2026-03-01T00:00:00Z"},
        ]}

    async def fake_attach_probe(item, *, proxy_id=""):
        item["external_data"]["probe"] = {"error": "GitHub API 限流：配额 5000 次/小时已用尽，预计 10:40 恢复"}
        return item

    upserted = {}

    async def fake_upsert(item):
        upserted[item["repo_full_name"]] = item

    async def fake_hints():
        return {("agent-leaderboard", "skills", "a/victim"): _hint(updated="2026-01-01T00:00:00+00:00")}

    monkeypatch.setattr(ls, "_settings", fake_settings)
    monkeypatch.setattr(ls, "is_enabled", fake_is_enabled)
    monkeypatch.setattr(ls, "_fetch_board", fake_fetch_board)
    monkeypatch.setattr(lp, "attach_probe", fake_attach_probe)
    monkeypatch.setattr(store, "upsert_item", fake_upsert)
    monkeypatch.setattr(store, "load_probe_reuse_hints", fake_hints)
    monkeypatch.setattr(ls, "_probe_budget_remaining", _budget_ok)
    monkeypatch.setattr(srs, "record_run", _async_noop)
    monkeypatch.setattr(sc, "record_source_sync", _async_noop)

    result = asyncio.run(ls.sync_once())

    # 撞限流的 a/victim：错误标记被 pop，probe 键不存在（SQL 侧移植旧 probe），
    # stack 从 hint 兜住。
    assert "probe" not in upserted["a/victim"]["external_data"]
    assert upserted["a/victim"]["stack"] == {"languages": {"Python": 10}}
    # b/after 与第二个 board 未触达（熔断）。
    assert "b/after" not in upserted
    assert result["ok"] is False
    assert any(is_rate_limit_error(e) for e in result["errors"])
    assert "提前结束" in result["detail"]

