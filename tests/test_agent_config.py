import sys, os

# Ensure the worktree root is on sys.path so that
# ``from agent.config import AgentConfig`` resolves
_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from agent.config import AgentConfig


def _assert_default_config(cfg: AgentConfig) -> None:
    assert cfg.enabled is False
    assert cfg.api_key == ""
    assert cfg.model == ""
    assert cfg.subagent_api_key == ""
    assert cfg.subagent_model == ""
    assert cfg.max_turns == 80
    assert cfg.max_concurrent_tasks == 5
    assert cfg.memory_enabled is True
    assert cfg.skill_auto_learn is True
    assert cfg.browser_enabled is False
    assert cfg.subagent_enabled is True
    assert cfg.scheduler_enabled is False
    assert cfg.cdp_bridge_mode == "stdio"
    assert cfg.cdp_bridge_token == ""
    assert cfg.workspace_root == "agent/workspace"
    assert cfg.allowed_roots == ["agent/workspace", "agent/temp"]
    assert cfg.denied_patterns == [
        r"^/etc/",
        r"^/var/",
        r"config\.json$",
        r"db\.py$",
        r"\.env$",
        r"\.git/",
        r"node_modules/",
    ]


def test_agent_config_defaults() -> None:
    cfg = AgentConfig()

    assert cfg.enabled is False
    assert cfg.max_turns == 80
    assert cfg.model == ""
    assert cfg.subagent_model == ""
    assert cfg.cdp_bridge_mode == "stdio"
    assert cfg.memory_enabled is True
    assert cfg.browser_enabled is False
    _assert_default_config(cfg)


def test_from_dict_sets_known_fields_and_leaves_others_default() -> None:
    cfg = AgentConfig.from_dict({"api_key": "k1", "model": "m1", "enabled": True})

    assert cfg.enabled is True
    assert cfg.api_key == "k1"
    assert cfg.model == "m1"
    assert cfg.max_turns == 80
    assert cfg.model == "m1"
    assert cfg.subagent_model == ""
    assert cfg.cdp_bridge_mode == "stdio"
    assert cfg.memory_enabled is True
    assert cfg.browser_enabled is False


def test_from_dict_ignores_unknown_keys_without_raising() -> None:
    cfg = AgentConfig.from_dict({"bogus_field": 123, "model": "m"})

    assert cfg.model == "m"
    assert not hasattr(cfg, "bogus_field")

    # All other fields remain at default
    assert cfg.enabled is False
    assert cfg.api_key == ""
    assert cfg.subagent_model == ""
    assert cfg.max_turns == 80
    assert cfg.memory_enabled is True
    assert cfg.browser_enabled is False
    assert cfg.cdp_bridge_mode == "stdio"
    assert cfg.workspace_root == "agent/workspace"


def test_from_dict_empty_none_and_string_return_default_config() -> None:
    assert AgentConfig.from_dict({}) == AgentConfig()
    assert AgentConfig.from_dict(None) == AgentConfig()
    assert AgentConfig.from_dict("notadict") == AgentConfig()


def test_round_trip_from_to_dict_reproduces_non_default_config() -> None:
    cfg = AgentConfig(
        enabled=True,
        api_key="k",
        model="m",
        subagent_api_key="sk",
        subagent_model="sm",
        max_turns=123,
        max_concurrent_tasks=7,
        memory_enabled=False,
        skill_auto_learn=False,
        browser_enabled=True,
        subagent_enabled=False,
        scheduler_enabled=True,
        cdp_bridge_mode="http",
        cdp_bridge_token="token",
        workspace_root="custom/workspace",
        allowed_roots=["custom/root"],
        denied_patterns=[r"secret\.txt$"],
    )

    reloaded = AgentConfig.from_dict(cfg.to_dict())

    assert reloaded == cfg


def test_allowed_roots_and_denied_patterns_are_non_empty_independent_lists() -> None:
    first = AgentConfig()
    second = AgentConfig()

    assert first.allowed_roots
    assert first.denied_patterns
    assert second.allowed_roots
    assert second.denied_patterns
    assert first.allowed_roots is not second.allowed_roots
    assert first.denied_patterns is not second.denied_patterns

    first.allowed_roots.append("mutated")
    first.denied_patterns.append(r"mutated$")

    assert "mutated" not in second.allowed_roots
    assert r"mutated$" not in second.denied_patterns
