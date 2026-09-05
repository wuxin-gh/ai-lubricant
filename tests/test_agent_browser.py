import os
import sys

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from agent.browser import BrowserAdapter, BrowserUnavailableError, CDPBridgeBrowser
from agent.config import AgentConfig


def test_connect_when_browser_disabled_raises_without_import() -> None:
    adapter = BrowserAdapter(AgentConfig(browser_enabled=False))

    with pytest.raises(BrowserUnavailableError, match="(?i)browser.*disabled"):
        adapter.connect()


def test_connect_rejects_removed_direct_bridge() -> None:
    adapter = BrowserAdapter(AgentConfig(browser_enabled=True, cdp_bridge_token="token"))

    with pytest.raises(BrowserUnavailableError, match="Direct CDP bridge startup is removed"):
        adapter.connect()


@pytest.mark.parametrize(
    ("method_name", "kwargs"),
    [
        ("navigate", {"url": "https://example.com"}),
        ("scan", {}),
        ("scan", {"text_only": True}),
        ("execute_js", {"script": "return 1;"}),
        ("screenshot", {}),
        ("close", {}),
    ],
)
def test_browser_operations_raise_clear_unavailable_error(
    method_name: str,
    kwargs: dict,
) -> None:
    adapter = BrowserAdapter(AgentConfig(browser_enabled=False))

    with pytest.raises(BrowserUnavailableError, match="unavailable|disabled|runtime"):
        getattr(adapter, method_name)(**kwargs)


def test_cdp_bridge_browser_aliases_browser_adapter() -> None:
    assert CDPBridgeBrowser is BrowserAdapter
