# -*- coding: utf-8 -*-
"""eaichat 实抓消息体统一回归：specs/fixtures/eaichat/ 下每条 JSON 是一段现网真实流。

每次改动围栏解析 / 帧翻译后，所有夹具一起重放——保证「这次修新形态」不打破
「上次修好的旧形态」。夹具协议：

- ``protocol``: "openai"（choices[].delta 流）或 "anthropic"（event: message_* 流）
- ``tools``: 本次请求声明的工具名（喂给「方法是否存在」校验）
- ``deltas`` (openai): 每项一帧，``content``/``reasoning``/``audit``/``finish`` 任取
- ``events`` (anthropic): 每项一个事件，``type`` + 同名字段
- ``expect``: 断言。``expected_pass=false`` 表示该形态**已知未修**（一旦实现层补上
  对应能力，这个标记必须删掉并换成真实断言，否则夹具就会一直以失败形态被容忍）。

新增实抓形态的工作流：抓包 → 存一条夹具（描述里写清这条流的问题所在）→ 在 expect
里写下**当时实现的真实行为** → 修复实现 → 把 expect 改成修复后的正确断言。夹具
文件即回归清单，永远比记忆可靠。
"""
import json
import types
from pathlib import Path

import pytest


def _load_channel_module():
    src = open('specs/eaichat_code_channel.py', encoding='utf-8').read()
    mod = types.ModuleType('_eaichat_spec_under_test')
    mod.__dict__['__name__'] = '_eaichat_spec_under_test'
    exec(compile(src, 'specs/eaichat_code_channel.py', 'exec'), mod.__dict__)
    return mod


CH = _load_channel_module()
FIXTURE_DIR = Path('specs/fixtures/eaichat')


def _iter_fixtures():
    if not FIXTURE_DIR.is_dir():
        return
    for path in sorted(FIXTURE_DIR.glob('*.json')):
        yield path, json.loads(path.read_text(encoding='utf-8'))


def _openai_frame(data):
    """夹具 delta 项 -> 与实抓帧同构的 event_data dict。"""
    delta = {"role": "assistant", "type": "text"}
    if data.get("reasoning") is not None:
        delta["reasoning_content"] = data["reasoning"]
    if data.get("content") is not None:
        delta["content"] = data["content"]
    choice = {"delta": delta, "index": 0}
    if data.get("finish"):
        choice["finish_reason"] = data["finish"]
    event = {"choices": [choice], "conversation_id": "fixture",
             "object": "chat.completion.chunk", "status": "generating"}
    if data.get("audit") is not None:
        event["audit_result"] = data["audit"]
    return event


def _replay(fixture):
    """按 stream_chat 的真实顺序重放一条夹具：帧翻译 -> 流末 flush -> finish。"""
    tool_state = {"enabled": bool(fixture.get("tools")),
                  "buf": "",
                  "names": CH._known_tool_names(
                      [{"type": "function", "function": {"name": n}}
                       for n in fixture.get("tools") or []])}
    thinking, text_parts, calls, usages = [], [], [], []
    finish_reason = ""
    anth_state: dict = {}
    oai_state: dict = {}
    if fixture["protocol"] == "anthropic":
        for ev in fixture["events"]:
            for chunk in CH._anthropic_event_chunks(
                    ev["type"], ev, tool_state, anth_state):
                if chunk.get("thinking"):
                    thinking.append(chunk["thinking"])
                if chunk.get("content"):
                    text_parts.append(chunk["content"])
                calls.extend(chunk.get("tool_calls") or [])
                if chunk.get("usage"):
                    usages.append(chunk["usage"])
        # 截断兜底（与 stream_chat 同序：flush 之后）
        if anth_state.get("usage") and not anth_state.get("usage_sent"):
            usages.append(anth_state["usage"])
    else:
        for item in fixture["deltas"]:
            for chunk in CH._openai_event_chunks(_openai_frame(item), tool_state, oai_state):
                if chunk.get("thinking"):
                    thinking.append(chunk["thinking"])
                if chunk.get("content"):
                    text_parts.append(chunk["content"])
                calls.extend(chunk.get("tool_calls") or [])
    for chunk in CH._tool_stream_flush(tool_state):
        if chunk.get("content"):
            text_parts.append(chunk["content"])
        calls.extend(chunk.get("tool_calls") or [])
    finish_reason = oai_state.get("finish_reason", "")
    return ("".join(thinking), "".join(text_parts), calls, usages, finish_reason)


def _check_expect(fixture, thinking, text, calls, usages, finish_reason):
    """按夹具 expect 逐项断言；expected_pass=false 的夹具在 xfail 里跑。"""
    exp = fixture["expect"]
    problems = []
    if "thinking" in exp and thinking != exp["thinking"]:
        problems.append(f"thinking={thinking[:80]!r} != 期望 {exp['thinking'][:80]!r}")
    if "thinking_contains" in exp and exp["thinking_contains"] not in thinking:
        problems.append(f"thinking 缺 {exp['thinking_contains']!r}")
    if "text" in exp and text != exp["text"]:
        problems.append(f"text={text[:120]!r} != 期望 {exp['text'][:120]!r}")
    if "text_contains" in exp and exp["text_contains"] not in text:
        problems.append(f"text 缺 {exp['text_contains']!r}")
    if "tool_calls" in exp:
        got_names = [c["function"]["name"] for c in calls]
        want_names = [t["name"] for t in exp["tool_calls"]]
        if got_names != want_names:
            problems.append(f"tool_calls={got_names} != 期望 {want_names}")
        else:
            for got, want in zip(calls, exp["tool_calls"]):
                args = json.loads(got["function"]["arguments"])
                if "arguments_equals" in want and args != want["arguments_equals"]:
                    problems.append(f"{got['function']['name']} 参数 != 期望")
                if "arguments_keys" in want and set(args) != set(want["arguments_keys"]):
                    problems.append(f"{got['function']['name']} 参数键 != 期望")
                if "arguments_contains" in want and want["arguments_contains"] not in got["function"]["arguments"]:
                    problems.append(f"{got['function']['name']} 参数缺 {want['arguments_contains']!r}")
    if "tool_call_count_exact" in exp and len(calls) != exp["tool_call_count_exact"]:
        problems.append(f"调用数 {len(calls)} != 期望 {exp['tool_call_count_exact']}")
    if "usage" in exp and usages != exp["usage"]:
        problems.append(f"usage={usages} != 期望 {exp['usage']}")
    if "finish_reason" in exp and finish_reason != exp["finish_reason"]:
        problems.append(f"finish_reason={finish_reason!r} != 期望 {exp['finish_reason']!r}")
    return problems


def _assert_fixture(fixture):
    results = _replay(fixture)
    problems = _check_expect(fixture, *results)
    assert not problems, (
        f"夹具 {fixture['name']}（{fixture['model']}）回归失败：\n  - "
        + "\n  - ".join(problems))


# 已知未修的形态：expected_pass=false。修好后删掉这个标记、把 expect 改成
# 修复后的正确断言，夹具自动从 xfail 转成回归防线。
_Pending = None  # 占位避免 IDE 把下面的推导式当常量折叠


def _pending_fixtures():
    return [f for _, f in _iter_fixtures() if f["expect"].get("expected_pass") is False]


def _passing_fixtures():
    return [f for _, f in _iter_fixtures() if f["expect"].get("expected_pass") is not False]


@pytest.mark.parametrize("fixture", _passing_fixtures(), ids=lambda f: f["name"])
def test_fixture_replay(fixture):
    """实抓消息体逐条重放：所有已修形态一起回归，改动不许打破任何一条。"""
    _assert_fixture(fixture)


@pytest.mark.xfail(reason="已知未修形态：修复后删除 expected_pass=false")
@pytest.mark.parametrize("fixture", _pending_fixtures(), ids=lambda f: f["name"])
def test_fixture_pending(fixture):
    """已知未修形态的「当前行为」锁：expect 记录的是**当前实现的真实行为**
    （调用漏成正文等），通过 = 行为没漂移。一旦有人改实现让这些形态的行为变了
    （不管变好变坏），这条立刻失败——逼着改动者有意识地更新夹具：修好了就把
    expected_pass=false 删掉、expect 换成正确断言转成正式回归；弄坏了就回滚。"""
    _assert_fixture(fixture)


def test_fixture_dir_exists_and_populated():
    """夹具目录是回归的地基：空了/没了必须立刻发现，不能静默跳过全部用例。"""
    fixtures = list(_iter_fixtures())
    assert len(fixtures) >= 8, (
        f"实抓夹具应有至少 8 条（现网 6 种已修形态 + 2 条已知未修），实得 {len(fixtures)}。"
        "夹具文件在 specs/fixtures/eaichat/，丢了就等于回归防线没了。")
    names = [f["name"] for _, f in fixtures]
    assert len(names) == len(set(names)), "夹具 name 必须唯一"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
