"""Remote node-client safety (data service dials the control process).

The in-process node client is gone. The data service always dials the separate
control process over HTTP, so the safety properties that matter now are:

* With a control base url + token configured, ``enabled`` is True and calls go
  over HTTP (no local service is ever built).
* With base url / token unset, ``enabled`` is False and every control call
  raises ``NodeServerUnavailable`` before any network attempt.
"""
from __future__ import annotations

import asyncio

import pytest


def _reload_with(monkeypatch, **overrides):
    """Patch the loaded data settings with agent_compose_* overrides."""
    from dataclasses import replace

    import monkeycode_compat.config as cfg
    import monkeycode_compat.node_client.client as client_mod

    monkeypatch.setattr(
        cfg,
        "settings",
        replace(
            cfg.settings,
            agent_compose_base_url=overrides.get("base_url", "http://control.local"),
            node_control_token=overrides.get("token", "tok-123"),
        ),
    )
    return cfg, client_mod


def test_client_enabled_when_base_url_and_token_set(monkeypatch):
    _, client_mod = _reload_with(monkeypatch, base_url="http://control.local", token="tok-123")
    assert client_mod.NodeClient().enabled is True


def test_client_disabled_when_base_url_or_token_missing(monkeypatch):
    _, client_mod = _reload_with(monkeypatch, base_url="", token="tok-123")
    assert client_mod.NodeClient().enabled is False

    _, client_mod = _reload_with(monkeypatch, base_url="http://control.local", token="")
    assert client_mod.NodeClient().enabled is False


def test_disabled_client_raises_before_network(monkeypatch):
    _, client_mod = _reload_with(monkeypatch, base_url="", token="")

    client = client_mod.NodeClient()
    assert client.enabled is False

    async def _run():
        with pytest.raises(client_mod.NodeServerUnavailable):
            await client.list_nodes()
        with pytest.raises(client_mod.NodeServerUnavailable):
            await client.dispatch_session("node-x", {"provider": "claude"})

    asyncio.run(_run())
