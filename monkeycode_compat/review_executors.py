"""Registry of code-review frameworks + per-editor capability requirements.

A *framework* is the review methodology (currently OpenCodeReview delegate). It
is deliberately independent from the *editor* (claude / codex / opencode) that
performs the reasoning: the same framework runs on any supported editor, and the
required node capabilities are the framework's base tools plus the chosen
editor's CLI. Adding a framework or supporting a new editor is an additive entry
here, not a rewrite of scheduling or webhook config.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ReviewFramework:
    name: str
    label: str
    description: str
    # Node tools every editor needs for this framework, keyed by the capability
    # helper name (see TOOL_CAPABILITY_KEYS). The editor's own CLI is appended
    # from EDITOR_CAPABILITY_KEYS at gate time.
    base_tools: tuple[str, ...]
    # Runtime providers this framework can drive. Codex is intentionally absent:
    # canonical Codex Tasks require a pre-registered installation_id + bootstrap
    # content, which a background webhook cannot currently source without
    # weakening first-request authentication. Gemini CLI is supported on equal
    # footing with claude/opencode: the review skill is isolated, so any CLI
    # that can run the delegate skill works. Cursor CLI is ACP-backed but still
    # runs the same isolated review skill on the node.
    supported_editors: tuple[str, ...] = field(default=("claude", "opencode", "gemini", "cursor"))


# Node capability label keys, mapped from the logical tool name.
TOOL_CAPABILITY_KEYS: dict[str, str] = {
    "git": "git_version",
    "node": "node_version",
    "npm": "npm_version",
    "runtime": "runtime_version",
    "ocr": "ocr_version",
}

# Editor provider → the capability label proving that editor CLI is installed.
EDITOR_CAPABILITY_KEYS: dict[str, str] = {
    "claude": "editor_version_claude",
    "codex": "editor_version_codex",
    "opencode": "editor_version_opencode",
    "gemini": "editor_version_gemini",
    "cursor": "editor_version_cursor",
}

REVIEW_FRAMEWORKS: dict[str, ReviewFramework] = {
    "open_code_review_delegate": ReviewFramework(
        name="open_code_review_delegate",
        label="OpenCodeReview Delegate",
        description=(
            "The editor performs the review while OpenCodeReview provides "
            "deterministic diff and rule scaffolding via its delegate skill."
        ),
        base_tools=("git", "runtime", "node", "npm", "ocr"),
        supported_editors=("claude", "opencode", "gemini", "cursor"),
    ),
}

DEFAULT_FRAMEWORK = "open_code_review_delegate"


def get_review_framework(name: str) -> ReviewFramework | None:
    return REVIEW_FRAMEWORKS.get((name or "").strip().lower() or DEFAULT_FRAMEWORK)


def required_tools(framework_name: str, editor_provider: str) -> tuple[str, ...]:
    """Base framework tools + the chosen editor's CLI capability tool.

    The editor CLI is represented as a synthetic tool name ``editor:<provider>``
    so the capability matrix can label it independently of the version key.
    """
    framework = get_review_framework(framework_name)
    if framework is None:
        return ()
    provider = (editor_provider or "").strip().lower()
    tools = list(framework.base_tools)
    if provider in EDITOR_CAPABILITY_KEYS:
        tools.append(f"editor:{provider}")
    return tuple(tools)


def _capability_key(tool: str) -> str | None:
    if tool.startswith("editor:"):
        return EDITOR_CAPABILITY_KEYS.get(tool.split(":", 1)[1])
    return TOOL_CAPABILITY_KEYS.get(tool)


def capability_matrix(
    labels: dict | None, framework_name: str, editor_provider: str
) -> dict[str, dict]:
    labels = labels or {}
    matrix: dict[str, dict] = {}
    for tool in required_tools(framework_name, editor_provider):
        key = _capability_key(tool)
        version = str(labels.get(key) or "").strip() if key else ""
        matrix[tool] = {
            "installed": bool(version),
            "version": version or None,
            "capability_key": key,
        }
    return matrix


def missing_capabilities(
    labels: dict | None, framework_name: str, editor_provider: str
) -> list[str]:
    return [
        tool
        for tool, state in capability_matrix(labels, framework_name, editor_provider).items()
        if not state["installed"]
    ]
