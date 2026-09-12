"""AtomCode daemon spec（specs/atomcode_daemon.py）的翻译契约。

spec 移植自 AtomCode2API（Go）——本测试锁住移植层的行为契约，防止后续
改框架钩子模型时悄悄破坏：
1. spec 能被 code_loader 加载（普通类 + 可识别钩子）；
2. 消息拍平对齐 Go FormatMessages（system 抽离 / User-Assistant 标签 / tool 按 User）；
3. ConversationKey 对齐 Go 版语义（含 system、排除最后一条、MD5 前 16 位）；
4. SSE 事件翻译对齐 Go TranslateToOpenAIChunk（text/reasoning/tool_start/tokens/跳过）；
5. provider 解析兼容 {providers:[...]} 与裸数组两种形态、大小写不敏感匹配；
6. 聊天链路拼 body：message/system/session_id/provider，done 帧回写 session。

specs/ 在 .gitignore 内、由使用者自持，文件不存在时整个模块跳过。
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from providers.code_loader import load_code_provider_class, invalidate_cache

_SPEC_PATH = Path(__file__).resolve().parent.parent / "specs" / "atomcode_daemon.py"

if not _SPEC_PATH.exists():
    pytest.skip("specs/atomcode_daemon.py 不存在（使用者自持）", allow_module_level=True)

_SOURCE = _SPEC_PATH.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def channel_cls():
    cls = load_code_provider_class("atomcode-daemon-test", _SOURCE)
    yield cls
    invalidate_cache("atomcode-daemon-test")


def _run(coro):
    return asyncio.run(coro)


def _patch_redis(monkeypatch, fake_redis):
    """spec 的 exec 命名空间里 JdbcClient 来自 rd 模块注入，直接改类属性即可。"""
    import rd
    monkeypatch.setattr(rd.JdbcClient, "redis", fake_redis, raising=False)



def _make_provider(channel_cls, daemon_url="", channel_base_url=""):
    """造一个池外实例（不挂渠道），需要的字段全部 kwargs 注入。

    channel_base_url 非空时挂一个 fallback _channel：池外实例的 base_url
    property 读 _channel_get，没有 _channel 时恒空串。
    """
    p = channel_cls(username="local", password="", proxy="", daemon_url=daemon_url, _api_key="")
    if channel_base_url:
        p._channel = SimpleNamespace(base_url=channel_base_url)
    return p


# ==================== 加载与声明 ====================

def test_spec_loads_with_expected_hooks(channel_cls):
    hooks = channel_cls._spec_hooks
    assert {
        "init_auth", "check_auth", "is_init", "fetch_models",
        "stream_chat", "non_stream_chat", "check_message",
        "account_schema", "begin_device_flow", "poll_device_flow",
        "clear_conversations",
    }.issubset(hooks)


def test_class_flags(channel_cls):
    assert channel_cls.SUPPORTS_MULTI_MESSAGES is True
    assert channel_cls.SUPPORTS_TOKEN_AUTO_REFRESH is False
    assert channel_cls.APPLY_CLIENT_PRESET is False


def test_account_schema_declares_device_code(channel_cls):
    schema = channel_cls.account_schema()
    assert schema["provider_name"] == "atomcode-daemon-test"
    assert schema["auth_start"]["mode"] == "device_code"
    keys = {f["key"] for f in schema["fields"]}
    assert "daemon_url" in keys


# ==================== 地址解析 ====================

def test_daemon_base_prefers_account_over_channel(channel_cls):
    import specs.atomcode_daemon as m
    # 账号级 daemon_url 优先
    p_channeled = _make_provider(channel_cls, daemon_url="http://10.0.0.2:13456",
                                  channel_base_url="http://127.0.0.1:13456")
    assert m._daemon_base(p_channeled) == "http://10.0.0.2:13456"
    # 账号没填时回落渠道地址（去尾斜杠）
    p2 = _make_provider(channel_cls, channel_base_url="http://10.0.0.1:13456/")
    assert m._daemon_base(p2) == "http://10.0.0.1:13456"


# ==================== 消息拍平（Go FormatMessages） ====================

def test_split_system_extracts_system_messages():
    import specs.atomcode_daemon as m
    msgs = [
        {"role": "system", "content": "You are helpful."},
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "hello"},
    ]
    system, rest = m._split_system(msgs)
    assert system == "You are helpful."
    assert [x["role"] for x in rest] == ["user", "assistant"]


def test_format_messages_labels_and_multimodal():
    import specs.atomcode_daemon as m
    msgs = [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "hello"},
        {"role": "tool", "content": "42"},  # tool 结果按 User 算（对齐 Go）
        {"role": "user", "content": [{"type": "text", "text": "what?"}]},
    ]
    assert m._format_messages(msgs) == (
        "User: hi\n\nAssistant: hello\n\nUser: 42\n\nUser: what?"
    )


def test_format_messages_skips_empty_and_system():
    import specs.atomcode_daemon as m
    msgs = [
        {"role": "system", "content": "sys"},
        {"role": "user", "content": ""},
    ]
    assert m._format_messages(msgs) == ""


# ==================== ConversationKey（Go 语义） ====================

def test_conversation_key_excludes_last_message():
    import specs.atomcode_daemon as m
    prefix = [
        {"role": "user", "content": "same turn"},
    ]
    key_a = m._conversation_key([*prefix, {"role": "user", "content": "answer A"}], "")
    key_b = m._conversation_key([*prefix, {"role": "user", "content": "answer B"}], "")
    # 最后一条变化不影响 key（同对话续接 session）
    assert key_a == key_b
    # 前缀变化则 key 变（新对话）
    key_c = m._conversation_key(
        [{"role": "user", "content": "different turn"},
         {"role": "user", "content": "answer A"}], "")
    assert key_a != key_c
    # 16 位 hex
    assert len(key_a) == 16


# ==================== SSE 事件翻译（Go TranslateToOpenAIChunk） ====================

def test_translate_text_and_reasoning():
    import specs.atomcode_daemon as m
    assert m._translate_event({"type": "text", "content": "hi"}) == {
        "content": "hi", "thinking": "", "tool_calls": [],
    }
    assert m._translate_event({"type": "reasoning", "content": "hmm"}) == {
        "content": "", "thinking": "hmm", "tool_calls": [],
    }


def test_translate_tool_start_parses_arguments():
    import specs.atomcode_daemon as m
    frame = m._translate_event({
        "type": "tool_start", "id": "c1", "name": "search",
        "arguments": "{\"q\": \"x\"}",
    })
    tc = frame["tool_calls"][0]
    assert tc["id"] == "c1"
    assert tc["function"] == {"name": "search", "arguments": {"q": "x"}}
    # 脏 JSON 参数保字符串（出口层归一）
    dirty = m._translate_event({
        "type": "tool_start", "id": "c2", "name": "n", "arguments": "not-json",
    })
    assert dirty["tool_calls"][0]["function"]["arguments"] == "not-json"
    # id 缺失时兜底 call_N
    no_id = m._translate_event({"type": "tool_start", "name": "n", "arguments": "{}"})
    assert no_id["tool_calls"][0]["id"].startswith("call_")


def test_translate_tokens_yields_usage():
    import specs.atomcode_daemon as m
    frame = m._translate_event({"type": "tokens", "prompt": 10, "completion": 5, "total": 15})
    assert frame["usage"] == {
        "prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15,
    }
    # total 缺失时由 prompt+completion 补
    frame2 = m._translate_event({"type": "tokens", "prompt": 3, "completion": 4})
    assert frame2["usage"]["total_tokens"] == 7


def test_translate_error_event_raises_502():
    import specs.atomcode_daemon as m
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc:
        m._translate_event({"type": "error", "message": "daemon boom"})
    assert exc.value.status_code == 502


def test_translate_skips_tool_output():
    import specs.atomcode_daemon as m
    frame = m._translate_event({"type": "tool_output", "output": "o"})
    assert frame["content"] == "" and not frame["tool_calls"]


# ==================== provider 解析（Go FindProviderForModel） ====================

class _FakeResponse:
    def __init__(self, status, payload):
        self.status = status
        self._payload = payload

    async def text(self):
        return self._payload

    async def json(self):
        return json.loads(self._payload)


class _FakeSession:
    """GET 捕获 + 可编程响应序列。"""

    def __init__(self, responses):
        self._responses = list(responses)
        self.requests = []

    def __call__(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def get(self, url, headers=None, proxy=None):
        self.requests.append(("GET", url))
        return _Ctx(self._responses.pop(0))


class _Ctx:
    def __init__(self, resp):
        self._resp = resp

    async def __aenter__(self):
        return self._resp

    async def __aexit__(self, *a):
        return False


def test_resolve_provider_matches_model_case_insensitive(channel_cls, monkeypatch):
    import specs.atomcode_daemon as m
    providers_json = json.dumps([
        {"name": "deepseek", "model": "DeepSeek-V4-Flash"},
        {"name": "qwen", "model": "Qwen/Qwen3-VL-8B-Instruct"},
    ])
    session = _FakeSession([_FakeResponse(200, providers_json)])
    p = _make_provider(channel_cls)
    monkeypatch.setattr(p, "_make_session", session)

    assert _run(m._resolve_provider(p, "deepseek-v4-flash")) == "deepseek"
    assert _run(m._resolve_provider(p, "qwen/qwen3-vl-8b-instruct")) == "qwen"
    # 未匹配的模型返回空串（daemon 自行兜底路由）
    assert _run(m._resolve_provider(p, "unknown-model")) == ""


def test_resolve_provider_accepts_wrapper_object(channel_cls, monkeypatch):
    import specs.atomcode_daemon as m
    wrapped = json.dumps({"providers": [
        {"name": "deepseek", "model": "deepseek-v4-flash"},
    ]})
    session = _FakeSession([_FakeResponse(200, wrapped)])
    p = _make_provider(channel_cls)
    monkeypatch.setattr(p, "_make_session", session)
    assert _run(m._resolve_provider(p, "deepseek-v4-flash")) == "deepseek"


# ==================== 聊天链路 ====================

def test_stream_chat_builds_daemon_body_and_translates(channel_cls, monkeypatch):
    """端到端：system 抽离 / 拍平 / body 形态 / SSE 翻译 / done 帧 session 回写。"""
    import specs.atomcode_daemon as m

    sse_events = [
        'data: {"type":"text","content":"Hi"}\n\n',
        'data: {"type":"reasoning","content":"thinking..."}\n\n',
        'data: {"type":"tool_start","id":"t1","name":"search","arguments":"{\\"q\\":\\"x\\"}"}\n\n',
        'data: {"type":"tokens","prompt":10,"completion":5,"total":15}\n\n',
        'data: {"type":"done","session_id":"sess-1"}\n\n',
        'data: [DONE]\n\n',
    ]
    captured = {}

    async def fake_send_sse(method, url, headers, json=None, **kw):
        captured["url"] = url
        captured["headers"] = headers
        captured["body"] = json
        for ev in sse_events:
            yield ev

    providers_json = json.dumps([{"name": "deepseek", "model": "deepseek-v4-flash"}])
    session = _FakeSession([_FakeResponse(200, providers_json)])
    p = _make_provider(channel_cls, channel_base_url="http://127.0.0.1:13456")
    monkeypatch.setattr(p, "_make_session", session)
    monkeypatch.setattr(p, "send_sse_request", fake_send_sse)

    written = {}

    class _FakeRedis:
        async def get(self, key):
            return None  # 无历史 session → 新会话

        async def set(self, key, value, ex=None):
            written[key] = (value, ex)

    _patch_redis(monkeypatch, _FakeRedis())

    msgs = [
        {"role": "system", "content": "You are helpful."},
        {"role": "user", "content": "hi"},
    ]
    frames = []
    _run(_collect(p, "deepseek-v4-flash", msgs, frames))

    # body：拍平单串 + system 独立字段 + provider 路由 + stream
    body = captured["body"]
    assert body["message"] == "User: hi"
    assert body["system"] == "You are helpful."
    assert body["provider"] == "deepseek"
    assert body["stream"] is True
    assert "session_id" not in body  # 无历史 session 不带
    # URL 与请求头
    assert captured["url"] == "http://127.0.0.1:13456/chat"
    assert captured["headers"]["Accept"] == "text/event-stream"

    # 帧：首帧空 dict + text/reasoning/tool_start/tokens
    assert frames[0] == {}
    contents = [f for f in frames if f.get("content")]
    thinkings = [f for f in frames if f.get("thinking")]
    tools = [f for f in frames if f.get("tool_calls")]
    usages = [f for f in frames if f.get("usage")]
    assert contents[0]["content"] == "Hi"
    assert thinkings[0]["thinking"] == "thinking..."
    assert tools[0]["tool_calls"][0]["function"]["name"] == "search"
    assert usages[0]["usage"]["total_tokens"] == 15

    # done 帧 session 回写 Redis（TTL 30 分钟）
    assert any(v == ("sess-1", 1800) for v in written.values())


def test_stream_chat_reuses_session_id(channel_cls, monkeypatch):
    """Redis 有历史 session 时 body 带 session_id（多轮续接）。"""
    import specs.atomcode_daemon as m

    sse_events = [
        'data: {"type":"done","session_id":"sess-2"}\n\n',
        'data: [DONE]\n\n',
    ]
    captured = {}

    async def fake_send_sse(method, url, headers, json=None, **kw):
        captured["body"] = json
        for ev in sse_events:
            yield ev

    providers_json = json.dumps([{"name": "d", "model": "deepseek-v4-flash"}])
    session = _FakeSession([_FakeResponse(200, providers_json)])
    p = _make_provider(channel_cls, channel_base_url="http://127.0.0.1:13456")
    monkeypatch.setattr(p, "_make_session", session)
    monkeypatch.setattr(p, "send_sse_request", fake_send_sse)

    class _FakeRedis:
        async def get(self, key):
            return "sess-1"  # 已有会话

        async def set(self, key, value, ex=None):
            pass

    _patch_redis(monkeypatch, _FakeRedis())

    frames = []
    _run(_collect(p, "deepseek-v4-flash", [{"role": "user", "content": "next"}], frames))
    assert captured["body"]["session_id"] == "sess-1"
    # done 帧未变时无内容帧（只有首帧空 dict）
    assert frames == [{}]


async def _collect(p, model_id, messages, out):
    async for frame in p._do_stream_chat(model_id, messages, stream=True):
        out.append(frame)


def test_non_stream_chat_aggregates(channel_cls, monkeypatch):
    import specs.atomcode_daemon as m

    sse_events = [
        'data: {"type":"text","content":"He"}\n\n',
        'data: {"type":"text","content":"llo"}\n\n',
        'data: {"type":"tokens","prompt":7,"completion":2,"total":9}\n\n',
        'data: {"type":"done","session_id":"s"}\n\n',
        'data: [DONE]\n\n',
    ]

    async def fake_send_sse(method, url, headers, json=None, **kw):
        for ev in sse_events:
            yield ev

    providers_json = json.dumps([{"name": "d", "model": "deepseek-v4-flash"}])
    session = _FakeSession([_FakeResponse(200, providers_json)])
    p = _make_provider(channel_cls, channel_base_url="http://127.0.0.1:13456")
    monkeypatch.setattr(p, "_make_session", session)
    monkeypatch.setattr(p, "send_sse_request", fake_send_sse)

    class _FakeRedis:
        async def get(self, key):
            return None

        async def set(self, key, value, ex=None):
            pass

    _patch_redis(monkeypatch, _FakeRedis())

    resp = _run(p._do_non_stream_chat("deepseek-v4-flash",
                                      [{"role": "user", "content": "hi"}], stream=False))
    message = resp["choices"][0]["message"]
    assert message["content"] == "Hello"
    assert resp["usage"]["prompt_tokens"] == 7
    assert resp["usage"]["completion_tokens"] == 2
    assert resp["usage"]["total_tokens"] == 9


# ==================== check_message ====================

def test_check_message_rejects_empty(channel_cls):
    p = _make_provider(channel_cls)
    assert _run(p.check_message("m", [])) is False
    assert _run(p.check_message("m", [{"role": "user", "content": ""}])) is False
    assert _run(p.check_message("m", [{"role": "user", "content": "hi"}])) is True
    # 只有 system 也拒绝（对齐 Go「no messages to process」）
    assert _run(p.check_message("m", [{"role": "system", "content": "sys"}])) is False
