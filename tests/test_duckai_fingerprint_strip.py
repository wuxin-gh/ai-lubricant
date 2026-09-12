"""duck.ai spec（specs/duckai.py）的出站编辑器指纹剥离契约。

duck.ai 不收 role=system（发 system 直接 400），框架把 system 内容拼进最后一条 user
消息。若客户端是 Claude Code / Codex 等，这会把它们的身份模板句 + 计费头键值段原样
带进 user 消息发到上游——既是编辑器指纹泄漏，也是上游逐字内容黑名单的触发器。

本测试锁住 specs/duckai.py 的剥离行为：
1. Claude Code 身份句 → 整句删除，不再出现在出站 user 内容里；
2. Codex CLI 身份句 + git 状态注入句 → 删除；
3. x-anthropic-billing-header / cc_* 键值段 → 剥离，正常行保留；
4. 不含特征串的普通 system → 逐字保留（零改动）；
5. 无 system 的普通对话 → 不受影响。

specs/ 在 .gitignore 内、由使用者自持，文件不存在时整个模块跳过。
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_SPEC_PATH = Path(__file__).resolve().parent.parent / "specs" / "duckai.py"

if not _SPEC_PATH.exists():
    pytest.skip("specs/duckai.py 不存在（使用者自持）", allow_module_level=True)


@pytest.fixture(scope="module")
def mod():
    """加载 spec 模块本身（拿模块级 _strip_editor_fingerprint / _normalize_messages_for_duck）。"""
    spec = importlib.util.spec_from_file_location("duckai_spec_module", _SPEC_PATH)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


class _P:
    """最小 provider 替身：_normalize_messages_for_duck 只用到 flatten_tool_history。"""

    @staticmethod
    def flatten_tool_history(messages):
        return messages


def _final_user_content(mod, messages, tools_prompt=""):
    out = mod._normalize_messages_for_duck(_P(), messages, tools_prompt)
    # 输出绝不含 system role，且 system 内容拼进最后一条 user。
    assert all((m.get("role") or "") != "system" for m in out)
    return out[-1]["content"] if out else ""


def test_claude_code_identity_stripped(mod):
    msgs = [
        {
            "role": "system",
            "content": (
                "You are Claude Code, Anthropic's official CLI tool for Claude. "
                "You are an interactive agent that helps users with software engineering tasks. "
                "Main branch (you will usually use this for PRs)\n"
                "git status context here"
            ),
        },
        {"role": "user", "content": "hi"},
    ]
    content = _final_user_content(mod, msgs)
    assert "Claude Code" not in content
    assert "official CLI" not in content
    assert "Main branch (" not in content
    # 非指纹行原样保留。
    assert "git status context here" in content
    assert content.rstrip().endswith("hi")


def test_codex_identity_stripped(mod):
    msgs = [
        {
            "role": "system",
            "content": (
                "You are a coding agent running inside the Codex CLI, a coding agent "
                "running in your terminal.\n"
                "Default branch (you will usually use this for PRs)"
            ),
        },
        {"role": "user", "content": "do it"},
    ]
    content = _final_user_content(mod, msgs)
    assert "Codex" not in content
    assert "Default branch (" not in content


def test_billing_header_and_cc_kv_stripped(mod):
    msgs = [
        {
            "role": "system",
            "content": (
                "x-anthropic-billing-header: tier=pro; cc_version=2.1.0; "
                "cc_entrypoint=cli;\n"
                "Keep this normal line."
            ),
        },
        {"role": "user", "content": "q"},
    ]
    content = _final_user_content(mod, msgs)
    assert "cc_version" not in content
    assert "billing-header" not in content
    assert "Keep this normal line." in content


def test_normal_system_untouched(mod):
    msgs = [
        {"role": "system", "content": "You are a helpful pirate. Answer in pirate speak."},
        {"role": "user", "content": "ahoy"},
    ]
    content = _final_user_content(mod, msgs)
    assert content == "You are a helpful pirate. Answer in pirate speak.\n\nahoy"


def test_no_system_unaffected(mod):
    msgs = [
        {"role": "user", "content": "plain"},
        {"role": "assistant", "content": "ok"},
    ]
    out = mod._normalize_messages_for_duck(_P(), msgs, "")
    assert out == msgs


def test_strip_is_idempotent(mod):
    """重入安全：剥离后的文本再过一次不变（不发散、不滚雪球）。"""
    raw = "You are Claude Code, Anthropic's official CLI tool for Claude. cc_x=1; \nkeep"
    once = mod._strip_editor_fingerprint(raw)
    twice = mod._strip_editor_fingerprint(once)
    assert once == twice
