from __future__ import annotations

from user_platform.review_executors import (
    capability_matrix,
    get_review_framework,
    missing_capabilities,
    required_tools,
)


def test_open_code_review_framework_declares_base_tools():
    framework = get_review_framework("open_code_review_delegate")
    assert framework is not None
    assert framework.base_tools == ("git", "runtime", "node", "npm", "ocr")
    assert "claude" in framework.supported_editors
    assert "codex" in framework.supported_editors
    assert "opencode" in framework.supported_editors


def test_required_tools_appends_editor_cli():
    tools = required_tools("open_code_review_delegate", "codex")
    assert "editor:codex" in tools
    assert set(("git", "runtime", "node", "npm", "ocr")).issubset(set(tools))


def test_capability_matrix_maps_node_labels_per_provider():
    labels = {
        "git_version": "2.47.0",
        "node_version": "22.1.0",
        "npm_version": "10.1.0",
        "runtime_version": "1.0.0",
        "ocr_version": "1.0.0",
        "editor_version_claude": "2.0.0",
    }
    matrix = capability_matrix(labels, "open_code_review_delegate", "claude")
    assert matrix["git"]["installed"] is True
    assert matrix["ocr"]["installed"] is True
    assert matrix["editor:claude"]["installed"] is True
    assert missing_capabilities(labels, "open_code_review_delegate", "claude") == []


def test_missing_editor_cli_is_reported():
    labels = {
        "git_version": "2.47.0",
        "node_version": "22.1.0",
        "npm_version": "10.1.0",
        "runtime_version": "1.0.0",
        "ocr_version": "1.0.0",
    }
    # codex CLI not installed → editor:codex missing.
    missing = missing_capabilities(labels, "open_code_review_delegate", "codex")
    assert "editor:codex" in missing


def test_unknown_framework_returns_none():
    assert get_review_framework("unknown") is None
    assert capability_matrix({}, "unknown", "claude") == {}
    assert required_tools("unknown", "claude") == ()
