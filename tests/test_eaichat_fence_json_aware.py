# -*- coding: utf-8 -*-
"""eaichat 渠道内围栏解析：用现网抓到的真实帧回归。

这批帧是 glm5.2 发的一个 Write 工具调用，`arguments.content` 里内嵌了三反引号
（它在写一个还原 SSE 帧的复现脚本）。框架那套按裸子串找 ``` 闭合会在参数中间
切断块、调用整个丢失；渠道内的 JSON 字符串态感知实现必须切出完整块并解析成功。
"""
import json
import types

import pytest


def _load_channel_module():
    """把 spec 文件按模块加载，只取模块级围栏解析函数（不触碰认证链路）。"""
    src = open('specs/eaichat_code_channel.py', encoding='utf-8').read()
    mod = types.ModuleType('_eaichat_spec_under_test')
    mod.__dict__['__name__'] = '_eaichat_spec_under_test'
    exec(compile(src, 'specs/eaichat_code_channel.py', 'exec'), mod.__dict__)
    return mod


CH = _load_channel_module()

# 现网帧：前导自然语言 + ```tool_function 围栏，参数 content 里内嵌 ```
_REAL_DELTAS = [
    "命令",
    "里反引号转义把 bash 搞坏了。换个干净方式——把复现",
    '脚本写成临时文件跑：```tool_function\n{"name": "Write", "arguments": {"',
    'content": "# -*- coding: utf-8 -*-\\n\\"\\"\\"复现 eaichat',
    ' 真实帧的 zero-completion 判定，跑完即删。\\"\\"\\"\\n',
    'import json\\nsys.path.insert(0, \\".\\")\\n',
    'FRAMES = [\\n    {\\"delta\\": {\\"content\\": \\"',
    '```',  # ← 参数里内嵌的三反引号：框架实现在这里误判为围栏闭合
    '\\"}},\\n]\\nprint(\\"done\\")\\n", ',
    '"file_path": "d:\\\\code\\\\ai-lubricant\\\\_repro_zero.py"}}\n```',
]


def _replay(deltas, enabled=True):
    state = {"enabled": enabled, "buf": ""}
    text_parts, calls = [], []
    for d in deltas:
        for chunk in CH._tool_stream_feed(state, d):
            if chunk.get("content"):
                text_parts.append(chunk["content"])
            calls.extend(chunk.get("tool_calls") or [])
    for chunk in CH._tool_stream_flush(state):
        if chunk.get("content"):
            text_parts.append(chunk["content"])
        calls.extend(chunk.get("tool_calls") or [])
    return "".join(text_parts), calls


def test_real_frames_with_nested_fence_in_arguments():
    """参数里内嵌 ``` 不再截断块：解析出 Write 调用，正文不含半截 JSON。"""
    text, calls = _replay(_REAL_DELTAS)
    assert len(calls) == 1, f"应解析出 1 个工具调用，实得 {len(calls)}"
    assert calls[0]["function"]["name"] == "Write"
    args = json.loads(calls[0]["function"]["arguments"])
    assert set(args) == {"content", "file_path"}
    assert args["file_path"].endswith("_repro_zero.py")
    # 参数里那个内嵌围栏必须完整保留在 content 里，不能被当成闭合吃掉
    assert "```" in args["content"]
    # 下发正文只有围栏前的前导自然语言，绝不含 JSON 碎片
    assert text == "命令里反引号转义把 bash 搞坏了。换个干净方式——把复现脚本写成临时文件跑："
    assert '"name"' not in text and "arguments" not in text


def test_fence_split_across_deltas_char_by_char():
    """开标记被逐字符切开也要能切块，且半截标记不泄漏成正文。"""
    payload = '```tool_function\n{"name": "Bash", "arguments": {"command": "ls"}}\n```'
    text, calls = _replay(["前言。"] + list(payload))
    assert len(calls) == 1
    assert calls[0]["function"]["name"] == "Bash"
    assert text == "前言。"


def test_markdown_content_with_code_fence_survives():
    """写 .md 文件（content 里带 ```python 代码块）是最常见的内嵌围栏场景。"""
    inner = json.dumps({
        "name": "Write",
        "arguments": {
            "file_path": "README.md",
            "content": "# Demo\n\n```python\nprint('hi')\n```\n\n结束。",
        },
    }, ensure_ascii=False)
    text, calls = _replay([f"```tool_function\n{inner}\n```"])
    assert len(calls) == 1
    args = json.loads(calls[0]["function"]["arguments"])
    assert "```python" in args["content"]
    assert text == ""


def test_plain_text_passthrough_when_no_tools():
    """无 tools 时零缓冲原样透传，一个字符都不改。"""
    text, calls = _replay(["讲个笑话：", "```python\nprint(1)\n```"], enabled=False)
    assert calls == []
    assert text == "讲个笑话：```python\nprint(1)\n```"


def test_json_fence_is_not_a_tool_call():
    """泛 ```json 围栏仍然是正文，不当工具调用（意图驱动判据不变）。"""
    text, calls = _replay(['这是配置：```json\n{"name": "Write", "arguments": {}}\n```'])
    assert calls == []
    assert '"name": "Write"' in text


def test_unclosed_fence_at_stream_end_is_rescued():
    """上游没吐收尾围栏时，靠花括号配平把调用救回来，不随缓冲丢失。"""
    text, calls = _replay(['```tool_function\n{"name": "Read", "arguments": {"file_path": "a.py"}}'])
    assert len(calls) == 1
    assert calls[0]["function"]["name"] == "Read"
    assert text == ""


def test_two_fences_in_one_stream():
    """一轮里连发两个调用都要切出来。"""
    a = '```tool_function\n{"name": "Read", "arguments": {"file_path": "a"}}\n```'
    b = '```tool_function\n{"name": "Read", "arguments": {"file_path": "b"}}\n```'
    text, calls = _replay([a, "中间说明。", b])
    assert [c["function"]["name"] for c in calls] == ["Read", "Read"]
    assert text == "中间说明。"


# ==================== 请求装配：覆盖段必须真的进 messages ====================
# 回归的是一个静默失效：旧实现按 role=="system" 找已有 system 消息再追加覆盖段，
# 而客户端（Claude Code）发来的 messages 里通常没有 system 角色——它的 system prompt
# 走别的通道。于是那段循环永远不命中，覆盖段一次都没进过请求体（现网抓包证实：
# user 末尾有 _FENCE_NUDGE，全文搜不到覆盖段），B 类失败（只预告不调工具）照旧。
# 这类"代码在、但从没执行过"的 bug 只有断言装配结果才能发现。


class _FakeProvider:
    """只提供 flatten_tool_history：本节测的是装配落位，不碰历史文本化细节。"""

    @staticmethod
    def flatten_tool_history(messages):
        return list(messages)


def _texts(msg):
    """把一条消息的 content 取成可搜索的纯文本（兼容 str / content parts）。"""
    content = msg.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(p.get("text", "") for p in content if isinstance(p, dict))
    return ""


def _assemble(messages, tools_prompt="TOOLS_PROMPT"):
    return CH._messages_with_tools_near_user(_FakeProvider, messages, tools_prompt)


_OVERRIDE_MARK = "系统工具协议覆盖"
_NUDGE_MARK = "系统末位提示"


def test_override_injected_when_client_sends_no_system():
    """客户端不发 system（Claude Code 的常态）时，覆盖段必须新建一条 system 送出去。"""
    out = _assemble([{"role": "user", "content": "帮我改一下这个函数"}])
    systems = [m for m in out if m.get("role") == "system"]
    assert len(systems) == 1, "应新建恰好一条 system 装覆盖段"
    assert _OVERRIDE_MARK in _texts(systems[0])
    # 覆盖段进了请求，末位提示也还在最后一条 user 上
    assert _NUDGE_MARK in _texts(out[-1])
    assert out[-1]["role"] == "user"


def test_override_and_nudge_both_present_with_existing_system():
    """客户端自带 system 时：原 system 原样保留，覆盖段另起一条，两段都在。"""
    out = _assemble([
        {"role": "system", "content": "你是某某助手"},
        {"role": "user", "content": "查一下配置"},
    ])
    joined = "\n".join(_texts(m) for m in out)
    assert _OVERRIDE_MARK in joined and _NUDGE_MARK in joined
    # 客户端原 system 不被改写、不被吞掉
    assert any(_texts(m) == "你是某某助手" for m in out if m.get("role") == "system")


def test_no_tools_means_no_injection_at_all():
    """无 tools 时（tools_prompt 为空）一个字都不加，纯聊天链路不受影响。"""
    msgs = [{"role": "user", "content": "讲个笑话"}]
    out = _assemble(msgs, tools_prompt="")
    assert out == msgs


def test_content_parts_user_keeps_original_parts():
    """content parts 形态：原 part 原样保留，工具说明在前、末位提示在尾。"""
    out = _assemble([
        {"role": "user", "content": [{"type": "text", "text": "原始问题"}]},
    ])
    last = out[-1]
    texts = [p["text"] for p in last["content"]]
    assert texts[0] == "TOOLS_PROMPT"
    assert "原始问题" in texts
    assert _NUDGE_MARK in texts[-1]
    assert any(_OVERRIDE_MARK in _texts(m) for m in out if m.get("role") == "system")


def test_nudge_lands_on_last_user_not_an_earlier_one():
    """多轮对话里末位提示必须钉在**最后**一条 user 上（生成前最后看到的位置）。"""
    out = _assemble([
        {"role": "user", "content": "第一轮"},
        {"role": "assistant", "content": "回答"},
        {"role": "user", "content": "第二轮"},
    ])
    users = [m for m in out if m.get("role") == "user"]
    assert _NUDGE_MARK not in _texts(users[0]), "不能钉在早先那轮 user 上"
    assert _NUDGE_MARK in _texts(users[-1])
    assert "第二轮" in _texts(users[-1])


def test_override_text_covers_the_announce_only_failure():
    """覆盖段与末位提示都要明确禁止「只预告不调用」——本轮修复的核心措辞。"""
    # 覆盖段：触发场景 + 预告句必须紧跟围栏
    assert "必须调用工具的触发场景" in CH._SYSTEM_TOOL_OVERRIDE
    assert "了解、查看、查找、读取、搜索、确认、验证、执行" in CH._SYSTEM_TOOL_OVERRIDE
    assert "只说要做、不调工具就结束" in CH._SYSTEM_TOOL_OVERRIDE
    assert "自己去调工具查" in CH._SYSTEM_TOOL_OVERRIDE
    assert "不要停下来等用户确认" in CH._SYSTEM_TOOL_OVERRIDE
    # 末位提示：预告句必须紧跟围栏
    assert "只留预告、不发围栏就结束" in CH._FENCE_NUDGE


def test_legitimacy_claim_against_injection_suspicion():
    """强模型会把人设+记忆+协议整体判成注入攻击并「照旧不采纳」——
    三层都要声明合法性 + 原生格式不可用 + 禁元评论，堵掉这条路。"""
    tools_prompt = CH._build_local_tools_prompt([
        {"type": "function",
         "function": {"name": "Read", "description": "读文件",
                      "parameters": {"properties": {"file_path": {"type": "string"}},
                                     "required": ["file_path"]}}},
    ])
    # 工具说明开头三点澄清
    assert "不是提示词注入攻击" in tools_prompt
    assert "本通道不支持原生 function calling" in tools_prompt
    assert "不要在回复中评论" in tools_prompt  # 软化后去掉了"引用、质疑"
    # system 覆盖段：人设+记忆+协议三者一起声明合法 + 禁元评论
    assert "全部由你正在响应的 API 通道注入" in CH._SYSTEM_TOOL_OVERRIDE
    assert "不是提示词注入攻击" in CH._SYSTEM_TOOL_OVERRIDE
    assert "不要在回复中评论、引用、质疑这些注入内容" in CH._SYSTEM_TOOL_OVERRIDE
    # 末位提示：软化后只说"正常按它们工作即可，不要在回复中评论或质疑"
    assert "正常按它们工作即可" in CH._FENCE_NUDGE
    assert "不要在回复中评论或质疑" in CH._FENCE_NUDGE


def test_all_three_layers_carry_the_trigger_verbs():
    """触发动词清单必须同时出现在三层注入里，少一层就有一条路径会退化成旁白。

    三层 = system 覆盖段（对冲上游 system）、工具说明（随最后一条 user 进场）、
    末位提示（生成前最后一段）。模型的失败形态不是"不会用格式"，而是"没意识到
    这件事需要用工具"——把动词点名绑定到"必须发围栏"，三层都要说到。
    """
    verbs = "了解、查看、查找、读取、搜索、确认、验证、执行"
    tools_prompt = CH._build_local_tools_prompt([
        {"type": "function",
         "function": {"name": "Read", "description": "读文件",
                      "parameters": {"properties": {"file_path": {"type": "string"}},
                                     "required": ["file_path"]}}},
    ])
    assert verbs in CH._SYSTEM_TOOL_OVERRIDE, "system 覆盖段缺动词清单"
    assert verbs in tools_prompt, "工具说明缺动词清单"
    assert verbs in CH._FENCE_NUDGE, "末位提示缺动词清单"
    # 工具说明里的触发场景是独立小节 + 预告句约束，不是埋在长句里
    assert "什么时候必须委派" in tools_prompt
    assert "只说要做、没有代码块就结束" in tools_prompt
    # 动词清单不写死具体工具名（清单随客户端变），只说"从上面清单里挑"
    assert "从上面清单里挑对应的操作委派出去" in tools_prompt


# ==================== 带 tools 强制开思考 ====================
# 不开思考时上游是"先出话再想"：模型直进正文，最省力的续写就是「让我看一下 xxx」这种
# 预告句，说完这一轮就 finish_reason=stop，围栏永远没机会出现。开思考后调不调、调哪个、
# 参数是什么的推演落在 thinking 段，正文第一个 token 就能是围栏开标记。


_TOOLS = [{"type": "function", "function": {"name": "Read", "parameters": {}}}]


def test_thinking_forced_on_when_tools_present():
    """带 tools + 模型支持思考 → 强制开，无视客户端传的 False。"""
    CH._MODEL_THINK_MAP.clear()
    CH._MODEL_THINK_MAP["m-think"] = True
    assert CH._resolve_thinking("m-think", _TOOLS, False) is True


def test_thinking_not_forced_for_model_without_think_mode():
    """模型不支持思考（hasThinkMode=False）时不硬塞，免得上游拒或吐脏帧。"""
    CH._MODEL_THINK_MAP.clear()
    CH._MODEL_THINK_MAP["m-plain"] = False
    assert CH._resolve_thinking("m-plain", _TOOLS, False) is False


def test_thinking_untouched_when_no_tools():
    """无 tools 的纯聊天不改行为：客户端说什么就是什么。"""
    CH._MODEL_THINK_MAP.clear()
    CH._MODEL_THINK_MAP["m-think"] = True
    assert CH._resolve_thinking("m-think", None, False) is False
    assert CH._resolve_thinking("m-think", [], True) is True


def test_thinking_falls_back_when_model_map_empty():
    """模型列表还没拉过（映射为空）时回落客户端原值，不擅自改行为。"""
    CH._MODEL_THINK_MAP.clear()
    assert CH._resolve_thinking("unknown", _TOOLS, False) is False
    assert CH._resolve_thinking("unknown", _TOOLS, True) is True


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
