"""Scene preset normalization, rendering and persistence.

Covers the three conversation entry points that carry an implicit scene, plus the
two properties the design depends on: the page's volatile URL never reaches the
prompt, and the persisted chat_settings keys stay the legacy ones so existing
rows and storage-layer filters keep working.
"""
import os
import sys

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from agent import scene_context as sc


# ── browser (CDP) ────────────────────────────────────────────────────

def test_browser_scene_uses_tab_id_from_session_key():
    spec = sc.normalize(cdp={
        "client_id": "cli-1",
        "session_key": "cli-1:42",
        "client_alias": "我的浏览器",
    })
    assert spec is not None
    assert spec.type == sc.BROWSER
    # tab_id is the stable page identity, parsed out of "{client_id}:{tabId}".
    assert spec.identity["client_id"] == "cli-1"
    assert spec.identity["tab_id"] == "42"
    assert spec.identity["client_alias"] == "我的浏览器"
    # The scene exists to drive cdp-bridge, so its SOP gets inlined on turn one.
    assert spec.services == ("cdp-bridge",)


def test_browser_scene_prompt_carries_client_id_not_url():
    """client_id is a required tool argument; a page URL is volatile state."""
    spec = sc.normalize(cdp={"client_id": "cli-1", "session_key": "cli-1:42"})
    prompt = sc.render_prompt(spec)
    assert "cli-1" in prompt
    assert "tab_id: 42" in prompt
    # No URL, and the model is told to fetch the page itself.
    assert "http" not in prompt
    assert "scan" in prompt


def test_browser_scene_omits_alias_when_same_as_client_id():
    spec = sc.normalize(cdp={"client_id": "cli-1", "session_key": "cli-1:7", "client_alias": "cli-1"})
    assert "client_alias" not in spec.identity


def test_browser_scene_tolerates_mismatched_session_key():
    """A session_key not prefixed by client_id yields no tab_id, not a crash."""
    spec = sc.normalize(cdp={"client_id": "cli-1", "session_key": "other:9"})
    assert spec is not None
    assert "tab_id" not in spec.identity


def test_browser_scene_requires_client_id():
    assert sc.normalize(cdp={"client_id": "", "session_key": "x:1"}) is None


# ── node terminal ────────────────────────────────────────────────────

def test_node_scene_binds_node_id_and_defers_cwd():
    spec = sc.normalize(node_id="node-7")
    assert spec.type == sc.NODE_TERMINAL
    assert spec.identity == {"node_id": "node-7"}
    # Terminal cwd / recent output are per-turn facts injected elsewhere.
    assert spec.services == ()
    prompt = sc.render_prompt(spec)
    assert "node-7" in prompt
    assert "node_shell_exec" in prompt
    assert "confirmation_required" in prompt


# ── marketplace admin ────────────────────────────────────────────────

def test_marketplace_scene_inlines_its_service():
    spec = sc.normalize(context="marketplace_admin")
    assert spec.type == sc.MARKETPLACE_ADMIN
    assert spec.services == ("marketplace-status",)
    prompt = sc.render_prompt(spec)
    assert "市场管理" in prompt
    # 规则里点名外部榜单草稿工具（场景保留能力的核心增量）与保留性质说明
    assert "marketplace-status" in prompt
    assert "marketplace_leaderboard_list" in prompt
    assert "MCP 选择器" in prompt


def test_unknown_context_is_not_a_scene():
    assert sc.normalize(context="something_else") is None
    assert sc.normalize() is None


# ── precedence & rendering ───────────────────────────────────────────

def test_cdp_wins_over_other_inputs():
    """A CDP conversation is never also a node/marketplace scene."""
    spec = sc.normalize(
        cdp={"client_id": "cli-1", "session_key": "cli-1:1"},
        node_id="node-7",
        context="marketplace_admin",
    )
    assert spec.type == sc.BROWSER


def test_render_prompt_of_none_is_empty():
    assert sc.render_prompt(None) == ""


def test_append_prompt_preserves_base_and_scene():
    spec = sc.normalize(node_id="node-7")
    merged = sc.append_prompt("你是一个运维助手。", spec)
    assert merged.startswith("你是一个运维助手。")
    assert "node-7" in merged


def test_append_prompt_handles_empty_sides():
    spec = sc.normalize(node_id="node-7")
    assert sc.append_prompt("", spec) == sc.render_prompt(spec)
    assert sc.append_prompt(None, spec) == sc.render_prompt(spec)
    assert sc.append_prompt("仅基础提示", None) == "仅基础提示"


# ── scene rebind (continuing a CDP conversation from another tab) ─────

def _cdp(tab_id: str) -> sc.SceneSpec:
    return sc.normalize(cdp={"client_id": "cli-1", "session_key": f"cli-1:{tab_id}"})


def test_replace_prompt_swaps_tab_id_and_keeps_base():
    """A dead tabId must not survive into the rebound prompt."""
    old, new = _cdp("42"), _cdp("77")
    base = sc.append_prompt("你是一个浏览器助手。", old)

    merged = sc.replace_prompt(base, old, new)

    assert merged.startswith("你是一个浏览器助手。")
    assert "tab_id: 77" in merged
    assert "tab_id: 42" not in merged
    # Exactly one scene segment remains — the old one was removed, not appended to.
    assert merged.count("[场景: browser]") == 1
    assert merged == sc.append_prompt("你是一个浏览器助手。", new)


def test_replace_prompt_without_base_prompt():
    old, new = _cdp("42"), _cdp("77")
    merged = sc.replace_prompt(sc.render_prompt(old), old, new)
    assert merged == sc.render_prompt(new)


def test_replace_prompt_with_no_old_scene_appends():
    """A conversation created before scenes existed just gains one."""
    new = _cdp("77")
    assert sc.replace_prompt("只有基础提示", None, new) == sc.append_prompt("只有基础提示", new)


def test_replace_prompt_leaves_prompt_alone_when_old_segment_absent():
    """If the stored prompt never had the old segment, nothing is torn out."""
    old, new = _cdp("42"), _cdp("77")
    merged = sc.replace_prompt("完全无关的提示词", old, new)
    assert merged.startswith("完全无关的提示词")
    assert "tab_id: 77" in merged


def test_replace_prompt_is_idempotent_for_same_tab():
    """Continuing in the same tab rewrites the segment to an identical one."""
    spec = _cdp("42")
    base = sc.append_prompt("你是一个浏览器助手。", spec)
    assert sc.replace_prompt(base, spec, spec) == base


# ── persistence (legacy key names) ───────────────────────────────────

def test_persist_uses_legacy_cdp_keys():
    """Storage filters read cdp_client_id / cdp_session_key; do not rename them."""
    spec = sc.normalize(cdp={"client_id": "cli-1", "session_key": "cli-1:42", "client_alias": "别名"})
    assert sc.persist(spec) == {
        "cdp_client_id": "cli-1",
        "cdp_session_key": "cli-1:42",
        "cdp_client_alias": "别名",
    }


def test_persist_defaults_alias_to_client_id():
    spec = sc.normalize(cdp={"client_id": "cli-1", "session_key": "cli-1:42"})
    assert sc.persist(spec)["cdp_client_alias"] == "cli-1"


def test_persist_node_and_marketplace_and_none():
    assert sc.persist(sc.normalize(node_id="node-7")) == {"node_id": "node-7"}
    assert sc.persist(sc.normalize(context="marketplace_admin")) == {"context": "marketplace_admin"}
    assert sc.persist(None) == {}


# ── scheduled (no human present) ─────────────────────────────────────

def test_scheduled_scene_identity_and_rules():
    spec = sc.normalize(scheduled={"job_id": 12, "name": "每日巡检", "cron": "0 9 * * *"})
    assert spec is not None
    assert spec.type == sc.SCHEDULED
    assert spec.identity == {"job_id": "12", "task": "每日巡检", "schedule": "0 9 * * *"}
    # 定时场景不驱动某个 MCP 服务，只声明「现场没人」这件事。
    assert spec.services == ()
    prompt = sc.render_prompt(spec)
    assert "每日巡检" in prompt
    # 关键约束：问不到人 + 结论要自己落地。
    assert "ask_user" in prompt


def test_scheduled_scene_requires_job_id():
    assert sc.normalize(scheduled={"name": "无 id"}) is None


# ── round trip through chat_settings ─────────────────────────────────

@pytest.mark.parametrize("kwargs", [
    {"cdp": {"client_id": "cli-1", "session_key": "cli-1:42", "client_alias": "别名"}},
    {"node_id": "node-7"},
    {"context": "marketplace_admin"},
    {"scheduled": {"job_id": 12, "name": "每日巡检", "cron": "0 9 * * *"}},
])
def test_scene_survives_persist_reload(kwargs):
    """A continuation turn rebuilds the same scene from stored chat_settings."""
    original = sc.normalize(**kwargs)
    reloaded = sc.from_chat_settings(sc.persist(original))
    assert reloaded == original


def test_from_chat_settings_without_scene_keys():
    assert sc.from_chat_settings(None) is None
    assert sc.from_chat_settings({}) is None
    assert sc.from_chat_settings({"reasoning_effort": "high"}) is None
