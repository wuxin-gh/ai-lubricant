"""ZCode spec（specs/zcode.py）的翻译契约。

spec 移植自 zcode2api（https://github.com/dengyie/zcode2api，Python 网关）——本测试锁住
移植层的行为契约，防止后续改框架钩子模型时悄悄破坏：
1. spec 能被 code_loader 加载（普通类 + 可识别钩子）；
2. 凭据模式判定对齐 zcode2api Account.create（JWT 两段点 → jwt / 其它 → apiKey）；
3. body 变换对齐 gateway._normalize_body + body_transform（system 官方块前置 / cache_control /
   metadata.user_id / max_tokens 钳制 / 模型名大小写归一 / string content 桥接）；
4. 身份头形态对齐 identity.build_identity_headers（字段集 + start-plan 追踪头三件套，
   绝不发 x-query-id / x-session-id）；
5. anthropic SSE 事件 → 统一帧翻译（text/thinking/tool_use/input_json_delta/usage/finish）；
6. 非流式响应体转换（content blocks → OpenAI message + tool_calls）；
7. 验证码挑战判定（403 header 形态 / 400+3007 body 形态 / 文案形态）；
8. 额度摘要（同模型多窗口合并）。

specs/ 在 .gitignore 内、由使用者自持，文件不存在时整个模块跳过。
"""
from __future__ import annotations

import asyncio
import base64
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from providers.code_loader import load_code_provider_class, invalidate_cache

_SPEC_PATH = Path(__file__).resolve().parent.parent / "specs" / "zcode.py"

if not _SPEC_PATH.exists():
    pytest.skip("specs/zcode.py 不存在（使用者自持）", allow_module_level=True)

_SOURCE = _SPEC_PATH.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def channel_cls():
    cls = load_code_provider_class("zcode-test", _SOURCE)
    yield cls
    invalidate_cache("zcode-test")


def _make_provider(channel_cls, *, password: str = "", credential_mode: str = "", **extra):
    """造一个池外实例（不挂渠道），需要的字段全部 kwargs 注入。"""
    return channel_cls(username="test", password=password, credential_mode=credential_mode, _api_key=password, **extra)


def _module():
    """加载 spec 模块本身（拿模块级 helper；与 code_loader 的 exec 命名空间同一套定义）。"""
    import importlib.util
    spec = importlib.util.spec_from_file_location("zcode_spec_module", _SPEC_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ==================== 加载与声明 ====================

def test_spec_loads_with_expected_hooks(channel_cls):
    hooks = channel_cls._spec_hooks
    assert {
        "account_schema", "is_init", "init_auth", "check_auth", "health_check",
        "begin_device_flow", "poll_device_flow", "refresh_auth",
        "fetch_models", "stream_chat", "non_stream_chat",
    }.issubset(hooks)


def test_class_flags(channel_cls):
    assert channel_cls.SUPPORTS_MULTI_MESSAGES is True
    # JWT 无刷新链路（过期需重登），不支持自愈。
    assert channel_cls.SUPPORTS_TOKEN_AUTO_REFRESH is False
    assert channel_cls.SCHEDULED_REFRESH is True
    # ZCode 网关按自己的 CLI 头校验，必须关掉伪装头。
    assert channel_cls.APPLY_CLIENT_PRESET is False


def test_account_schema_declares_device_code(channel_cls):
    schema = channel_cls.account_schema()
    assert schema["provider_name"] == "zcode-test"
    assert schema["auth_start"]["mode"] == "device_code"
    keys = {f["key"] for f in schema["fields"]}
    assert {"credential_mode", "password", "plan_name", "quota_text", "last_claim"} == keys


def test_account_fields_covers_device_profile(channel_cls):
    fields = set(channel_cls.ACCOUNT_FIELDS)
    assert {"device_mid", "fp_platform", "fp_arch", "fp_os_version", "fp_language",
            "fp_timezone", "fp_screen", "last_claim"} <= fields


def test_account_schema_declares_claim_display(channel_cls):
    schema = channel_cls.account_schema()
    keys = {f["key"] for f in schema["fields"]}
    assert "last_claim" in keys
    assert "last_claim" in schema["metadata_badges"]


# ==================== 凭据模式判定 ====================

def _make_jwt(payload: dict) -> str:
    seg = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
    return f"h1.{seg}.sig"


def test_mode_detects_jwt_by_shape(channel_cls):
    p = _make_provider(channel_cls, password=_make_jwt({"sub": "u1"}))
    assert p._mode(p) == "jwt" if hasattr(p, "_mode") else True
    mod = _module()
    assert mod._mode(p) == "jwt"
    assert mod._is_jwt_mode(p) is True


def test_mode_defaults_to_api_key(channel_cls):
    mod = _module()
    p = _make_provider(channel_cls, password="ak.sk123")
    assert mod._mode(p) == "apiKey"
    assert mod._is_jwt_mode(p) is False


def test_mode_explicit_override_wins(channel_cls):
    mod = _module()
    # 显式 apiKey 覆盖 JWT 形态判定（zcode2api Account.create 同语义：provider 才是主判据）
    p = _make_provider(channel_cls, password=_make_jwt({"sub": "u1"}), credential_mode="apiKey")
    assert mod._mode(p) == "apiKey"


def test_jwt_user_id_prefers_user_id_then_sub():
    mod = _module()
    assert mod._jwt_user_id(_make_jwt({"user_id": "u-uuid", "sub": "s-uuid"})) == "u-uuid"
    assert mod._jwt_user_id(_make_jwt({"sub": "s-uuid"})) == "s-uuid"
    for bad in ("", "not-a-jwt", "h1.!!!not-base64!!!.sig", None):
        assert mod._jwt_user_id(bad) is None


def test_resolve_model_name_maps_aliases_and_strips_prefix():
    mod = _module()
    assert mod._resolve_model_name("glm-5.3") == "GLM-5.3"
    assert mod._resolve_model_name("GLM-5.3") == "GLM-5.3"
    assert mod._resolve_model_name("glm-turbo") == "GLM-5-Turbo"
    assert mod._resolve_model_name("bigmodel/GLM-5.3") == "GLM-5.3"
    assert mod._resolve_model_name("glm-9.9") == "glm-9.9"  # 未知模型原样


# ==================== body 变换 ====================

def _jwt_body_channel(channel_cls):
    mod = _module()
    p = _make_provider(channel_cls, password=_make_jwt({"user_id": "u-uuid"}))
    return mod, p


def test_transform_prepends_official_system_blocks_idempotently(channel_cls):
    mod, p = _jwt_body_channel(channel_cls)
    body = {"model": "GLM-5.3", "messages": [{"role": "user", "content": "hi"}], "system": "my prompt"}
    mod._transform_body(p, body, "GLM-5.3")
    system = body["system"]
    assert isinstance(system, list)
    assert system[0]["text"] == "You are ZCode, an interactive coding agent"
    # currentModel 动态块 + 官方三块 + 用户原 system
    assert system[3]["text"] == "- You are powered by the model named GLM-5.3."
    assert system[-1]["text"] == "my prompt"
    assert all(b.get("cache_control") == {"type": "ephemeral"} for b in system[:4])
    # 幂等：再跑一遍不叠加
    mod._transform_body(p, body, "GLM-5.3")
    assert sum(1 for b in body["system"] if b["text"].startswith("You are ZCode")) == 1


def test_transform_api_key_mode_skips_system_blocks(channel_cls):
    mod = _module()
    p = _make_provider(channel_cls, password="ak.sk")
    body = {"model": "GLM-5.3", "messages": [{"role": "user", "content": "hi"}], "system": "my prompt"}
    mod._transform_body(p, body, "GLM-5.3")
    assert body["system"] == "my prompt"  # apiKey 通道不动 system


def test_transform_injects_user_id_metadata(channel_cls):
    mod, p = _jwt_body_channel(channel_cls)
    body = {"model": "GLM-5.3", "messages": [], "metadata": {"trace": "x"}}
    mod._transform_body(p, body, "GLM-5.3")
    assert body["metadata"]["user_id"] == "u-uuid"
    assert body["metadata"]["trace"] == "x"  # 保留已有字段


def test_transform_cache_control_on_last_message(channel_cls):
    mod, p = _jwt_body_channel(channel_cls)
    body = {
        "model": "GLM-5.3",
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": "q"}]},
            {"role": "assistant", "content": [{"type": "text", "text": "a"}]},
        ],
    }
    mod._transform_body(p, body, "GLM-5.3")
    msgs = body["messages"]
    assert msgs[-1]["content"][-1]["cache_control"] == {"type": "ephemeral"}
    assert msgs[0]["content"][0].get("cache_control") is None


def test_transform_clamps_max_tokens(channel_cls):
    mod = _module()
    p = _make_provider(channel_cls, password="ak")
    body = {"model": "GLM-5.3", "messages": [], "max_tokens": 99999999}
    mod._transform_body(p, body, "GLM-5.3")
    assert body["max_tokens"] == mod.MAX_TOKENS_LIMIT
    body2 = {"model": "GLM-5.3", "messages": [], "max_tokens": 0}
    mod._transform_body(p, body2, "GLM-5.3")
    assert body2["max_tokens"] == 1
    body3 = {"model": "GLM-5.3", "messages": []}
    mod._transform_body(p, body3, "GLM-5.3")
    assert body3["max_tokens"] == mod.MAX_TOKENS_DEFAULT


def test_transform_bridges_string_content(channel_cls):
    mod = _module()
    p = _make_provider(channel_cls, password="ak")
    body = {"model": "GLM-5.3", "messages": [{"role": "user", "content": "plain"}]}
    mod._transform_body(p, body, "GLM-5.3")
    assert body["messages"][0]["content"] == [{"type": "text", "text": "plain"}]


def test_transform_maps_model_alias(channel_cls):
    mod = _module()
    p = _make_provider(channel_cls, password="ak")
    body = {"model": "glm-5.3-flash", "messages": []}
    mod._transform_body(p, body, "glm-5.3-flash")
    assert body["model"] == "GLM-5.3-Flash"


# ==================== 出站 body 构造 ====================

def test_build_outbound_body_prefers_raw_anthropic_body(channel_cls):
    mod = _module()
    p = _make_provider(channel_cls, password="ak")
    raw = {"model": "glm-5.3", "max_tokens": 100,
           "messages": [{"role": "user", "content": [{"type": "text", "text": "hi", "cache_control": {"type": "ephemeral"}}]}]}
    body = mod._build_outbound_body(p, "glm-5.3", [], False, {"_raw_anthropic_body": raw})
    # 直通保留 block 结构（cache_control 不丢），但 model 归一 + stream 覆写
    assert body["messages"][0]["content"][0]["cache_control"] == {"type": "ephemeral"}
    assert body["model"] == "GLM-5.3"
    assert body["stream"] is False
    assert body["max_tokens"] == 100


def test_build_outbound_body_converts_openai_messages(channel_cls):
    mod = _module()
    p = _make_provider(channel_cls, password="ak")
    messages = [{"role": "system", "content": "sys"},
                {"role": "user", "content": "hello"}]
    body = mod._build_outbound_body(p, "GLM-5.3", messages, True, {})
    assert body["stream"] is True
    assert body["model"] == "GLM-5.3"
    # system 抽出为顶层 system；user 转 block
    assert body["system"] == "sys"
    roles = [m.get("role") for m in body["messages"]]
    assert "system" not in roles
    assert any(isinstance(m.get("content"), list) for m in body["messages"])


# ==================== 身份头 / 追踪头 ====================

def test_identity_headers_full_shape(channel_cls):
    mod = _module()
    p = _make_provider(channel_cls, password="ak",
                       device_mid="mid-1", fp_platform="darwin", fp_arch="arm64",
                       fp_os_version="25.5.0", fp_language="zh-CN", fp_timezone="Asia/Shanghai")
    headers = mod._identity_headers(p)
    assert headers["User-Agent"] == "ZCode/3.10.2"
    assert headers["X-ZCode-App-Version"] == "3.10.2"
    assert headers["X-ZCode-Agent"] == "glm"
    assert headers["X-Platform"] == "darwin-arm64"
    assert headers["X-Os-Category"] == "macos"
    assert headers["X-Device-Mid"] == "mid-1"
    assert headers["X-Title"] == "Z Code@cli"
    assert headers["HTTP-Referer"] == "https://zcode.z.ai/"


def test_trace_headers_start_plan_never_send_query_id():
    mod = _module()
    headers = mod._trace_headers()
    # start-plan 只发这三个；x-query-id / x-session-id 误发触发 3012
    assert set(headers) == {"x-request-id", "x-zcode-session-type", "x-zcode-trace-id"}
    assert headers["x-zcode-session-type"] == "main"
    # 每请求全新 UUID
    assert headers["x-request-id"] != mod._trace_headers()["x-request-id"]


def test_messages_headers_jwt_vs_api_key(channel_cls):
    mod = _module()
    jwt = _make_jwt({"sub": "u1"})
    p_jwt = _make_provider(channel_cls, password=jwt, device_mid="m", fp_platform="linux", fp_arch="x64")
    h = mod._messages_headers(p_jwt, "param-1", "cn")
    assert h["Authorization"] == f"Bearer {jwt}"
    assert h["X-Aliyun-Captcha-Verify-Param"] == "param-1"
    assert h["X-Aliyun-Captcha-Verify-Region"] == "cn"
    assert "x-api-key" not in h

    p_ak = _make_provider(channel_cls, password="ak.sk")
    h2 = mod._messages_headers(p_ak, None, None)
    assert h2["x-api-key"] == "ak.sk"
    assert "Authorization" not in h2
    assert "X-Device-Mid" not in h2  # apiKey 通道无身份头
    assert mod.CAPTCHA_HEADER not in h2


class _StubChannel:
    """最小渠道 stub：模拟 server.channel.Channel.get(key, default) 读取原始配置键。"""

    def __init__(self, **raw):
        self._raw = raw

    def get(self, key, default=None):
        return self._raw.get(key, default)


def test_messages_url_split_by_mode(channel_cls):
    mod = _module()
    p_jwt = _make_provider(channel_cls, password=_make_jwt({"sub": "u"}))
    p_jwt._channel = _StubChannel(base_url="https://zcode.z.ai")
    assert mod._messages_url(p_jwt) == "https://zcode.z.ai/api/v1/zcode-plan/anthropic/v1/messages"
    p_ak = _make_provider(channel_cls, password="ak")
    p_ak._channel = _StubChannel(base_url="https://zcode.z.ai")
    assert mod._messages_url(p_ak) == "https://api.z.ai/api/anthropic/v1/messages"
    # api_key_base 渠道配置覆盖
    p_ak2 = _make_provider(channel_cls, password="ak")
    p_ak2._channel = _StubChannel(base_url="https://zcode.z.ai", api_key_base="https://open.bigmodel.cn/")
    assert mod._messages_url(p_ak2) == "https://open.bigmodel.cn/api/anthropic/v1/messages"


# ==================== SSE 事件 → 统一帧 ====================

def _feed(mod, state, events: list[str]):
    frames = []
    for event in events:
        event_type, payload = mod.__dict__["_parse_event"](event) if "_parse_event" in mod.__dict__ else (None, None)
    return frames


def _sse(event_type: str, payload: dict) -> str:
    return f"event: {event_type}\ndata: {json.dumps(payload)}\n\n"


def test_stream_parser_translates_full_event_sequence():
    mod = _module()
    state = mod._new_stream_parser()
    frames = []
    events = [
        _sse("message_start", {"message": {"id": "msg_1", "usage": {"input_tokens": 12}}}),
        _sse("content_block_start", {"index": 0, "content_block": {"type": "thinking", "thinking": ""}}),
        _sse("content_block_delta", {"index": 0, "delta": {"type": "thinking_delta", "thinking": "hmm"}}),
        _sse("content_block_start", {"index": 1, "content_block": {"type": "text", "text": ""}}),
        _sse("content_block_delta", {"index": 1, "delta": {"type": "text_delta", "text": "hi"}}),
        _sse("content_block_start", {"index": 2, "content_block": {"type": "tool_use", "id": "tu_1", "name": "search", "input": {}}}),
        _sse("content_block_delta", {"index": 2, "delta": {"type": "input_json_delta", "partial_json": "{\"q\":1}"}}),
        _sse("message_delta", {"delta": {"stop_reason": "tool_use"}, "usage": {"output_tokens": 34}}),
    ]
    for event in events:
        # parse_sse_type 是框架 BaseProvider 的 staticmethod；spec 侧直接手工拆
        import re as _re
        m = _re.search(r"^event: (\S+)\ndata: (.+)$", event.strip(), _re.S)
        event_type, payload = m.group(1), json.loads(m.group(2))
        frames.extend(mod._parser_feed(state, event_type, payload))

    assert {"thinking": "hmm"} in frames
    assert {"content": "hi"} in frames
    # tool_use 首帧带 id+name，后续帧带 arguments 增量
    tc_start = [f for f in frames if "tool_calls" in f][0]["tool_calls"][0]
    assert tc_start["id"] == "tu_1" and tc_start["function"]["name"] == "search" and tc_start["function"]["arguments"] == ""
    tc_delta = [f for f in frames if "tool_calls" in f][1]["tool_calls"][0]
    assert tc_delta["function"]["arguments"] == '{"q":1}'
    assert {"finish_reason": "tool_calls"} in frames
    # usage：message_start 的 input + message_delta 的 output
    final = mod._parser_final_usage(state)
    assert final == {"prompt_tokens": 12, "completion_tokens": 34, "total_tokens": 46}
    assert state["message_id"] == "msg_1"


def test_stream_parser_empty_usage_returns_none():
    mod = _module()
    state = mod._new_stream_parser()
    assert mod._parser_final_usage(state) is None


# ==================== 非流式响应体转换 ====================

def test_parse_anthropic_response_body_maps_blocks_and_tools(channel_cls):
    mod = _module()
    p = _make_provider(channel_cls, password="ak")
    data = {
        "id": "msg_9", "model": "GLM-5.3", "stop_reason": "tool_use",
        "content": [
            {"type": "thinking", "thinking": "let me think"},
            {"type": "text", "text": "hello "},
            {"type": "text", "text": "world"},
            {"type": "tool_use", "id": "tu_2", "name": "search", "input": {"q": "x"}},
        ],
        "usage": {"input_tokens": 5, "output_tokens": 7},
    }
    resp = mod._parse_anthropic_response_body(json.dumps(data), "GLM-5.3", p)
    msg = resp["choices"][0]["message"]
    assert msg["content"] == "hello world"
    assert msg["reasoning_content"] == "let me think"
    tc = msg["tool_calls"][0]
    assert tc["id"] == "tu_2" and tc["function"]["name"] == "search"
    assert json.loads(tc["function"]["arguments"]) == {"q": "x"}
    assert resp["usage"]["prompt_tokens"] == 5
    assert resp["usage"]["completion_tokens"] == 7
    assert resp["usage"]["total_tokens"] == 12
    # tool_calls 存在时 finish_reason 归一为 tool_calls（对齐 anthropic stop_reason 映射）
    assert resp["choices"][0]["finish_reason"] == "tool_calls"


def test_parse_anthropic_response_body_raises_upstream_error(channel_cls):
    mod = _module()
    p = _make_provider(channel_cls, password="ak")
    err = json.dumps({"type": "error", "error": {"type": "overloaded_error", "message": "overloaded"}})
    with pytest.raises(Exception) as excinfo:
        mod._parse_anthropic_response_body(err, "GLM-5.3", p)
    assert "overloaded" in str(excinfo.value)


# ==================== 验证码挑战判定 ====================

def test_captcha_challenge_detection():
    mod = _module()
    from fastapi import HTTPException as _HE
    assert mod._is_captcha_challenge(_HE(status_code=403, detail="captcha required")) is True
    assert mod._is_captcha_challenge(_HE(status_code=400, detail='{"code":3007}')) is True
    assert mod._is_captcha_challenge(_HE(status_code=403, detail="verify token expired")) is True
    # 非挑战：其它状态码 / 普通鉴权失败（403 已排除挑战形态的语义）
    assert mod._is_captcha_challenge(_HE(status_code=429, detail="captcha")) is False
    assert mod._is_captcha_challenge(_HE(status_code=401, detail="unauthorized")) is False
    assert mod._is_captcha_challenge(_HE(status_code=502, detail="bad gateway")) is False


def _reset_provision(mod):
    """重置共享探测状态（_CAPTCHA_STATE 是 spec 模块级单例，测试间互相污染）。"""
    mod._CAPTCHA_STATE.provision_ok = None
    mod._CAPTCHA_STATE.provision_at = 0
    mod._CAPTCHA_STATE.provision_error = ""
    mod._CAPTCHA_STATE.pool.clear()


def test_provision_solver_reports_concrete_reason(channel_cls, monkeypatch, tmp_path):
    """探测失败必须给出具体卡点，不是笼统的「已缓存」。

    cwd 换到无求解器的临时目录 + 清空环境变量：仓库若克隆了 .tmp-zcode2api，
    自动发现会在真实 cwd 命中求解器，必须隔离才能测「未找到」分支。
    """
    mod = _module()
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ZCODE_CAPTCHA_SOLVER", raising=False)
    _reset_provision(mod)
    p = _make_provider(channel_cls, password=_make_jwt({"sub": "u"}))

    ok, msg = asyncio.run(mod._provision_solver(p))
    assert ok is False
    assert "captcha_solver_path" in msg
    assert "已缓存" not in msg


def test_provision_solver_fail_cache_preserves_reason_for_30s(channel_cls, monkeypatch, tmp_path):
    """失败缓存期内重入：仍返回上一次的具体原因，且提示自动重探倒计时。"""
    mod = _module()
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ZCODE_CAPTCHA_SOLVER", raising=False)
    _reset_provision(mod)
    p = _make_provider(channel_cls, password=_make_jwt({"sub": "u"}))

    asyncio.run(mod._provision_solver(p))  # 触发一次探测（未找到路径）
    assert mod._CAPTCHA_STATE.provision_ok is False

    ok2, msg2 = asyncio.run(mod._provision_solver(p))  # 缓存期内重入
    assert ok2 is False
    assert "captcha_solver_path" in msg2      # 具体原因没被吞
    assert "自动重探" in msg2                 # 告知会重探


def test_provision_solver_recovers_after_fail_cache(channel_cls, monkeypatch, tmp_path):
    """配置就位后探测通过（临时目录模拟 solver.js + node_modules/happy-dom）。"""
    import os as _os
    mod = _module()
    p = _make_provider(channel_cls, password=_make_jwt({"sub": "u"}))
    _reset_provision(mod)

    # 用真实临时目录模拟已装好的求解器：solver.js + node_modules/happy-dom
    tmp = tempfile.mkdtemp()
    _os.makedirs(_os.path.join(tmp, "node_modules", "happy-dom"), exist_ok=True)
    solver_path = _os.path.join(tmp, "solver.js")
    with open(solver_path, "w") as fh:
        fh.write("// stub\n")

    monkeypatch.setenv("ZCODE_CAPTCHA_SOLVER", solver_path)
    ok, msg = asyncio.run(mod._provision_solver(p))
    assert ok is True, msg


def test_solver_path_auto_discovery(channel_cls, monkeypatch, tmp_path):
    """自动发现三级：显式配置 > cwd 相对 .tmp-zcode2api > 都没有返回空。"""
    import os as _os
    mod = _module()
    p = _make_provider(channel_cls, password=_make_jwt({"sub": "u"}))

    # 1. 显式配置优先
    p._channel = _StubChannel(captcha_solver_path=r"D:\explicit\solver.js")
    assert mod._solver_path(p) == r"D:\explicit\solver.js"
    p._channel = None

    # 2. cwd 下 .tmp-zcode2api/captcha_node/solver.js 自动命中
    _os.makedirs(_os.path.join(tmp_path, ".tmp-zcode2api", "captcha_node"), exist_ok=True)
    auto = _os.path.join(tmp_path, ".tmp-zcode2api", "captcha_node", "solver.js")
    with open(auto, "w") as fh:
        fh.write("// stub\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ZCODE_CAPTCHA_SOLVER", raising=False)
    assert mod._solver_path(p) == auto

    # 3. 完全独立的空目录 + 无环境变量 → 空串（后续探测给带指引的报错）
    # （tmp_path 的上级会被第 2 步命中，必须用独立 mkdtemp 隔离）
    monkeypatch.chdir(tempfile.mkdtemp())
    assert mod._solver_path(p) == ""


# ==================== 额度摘要 ====================

def test_summarize_quota_merges_same_model_windows():
    mod = _module()
    balances = [
        {"show_name": "GLM-5.3", "remaining_units": 100},
        {"show_name": "GLM-5.3", "remaining_units": 50},   # 同模型多窗口合并
        {"show_name": "GLM-5-Turbo", "remaining_units": 0},
    ]
    quota, summary = mod._summarize_quota(balances)
    assert quota == {"GLM-5.3": 150, "GLM-5-Turbo": 0}
    assert "GLM-5.3" in summary and "150" in summary
    assert mod._summarize_quota([]) == ({}, "")


# ==================== is_init / init_auth ====================

def test_is_init_requires_credential(channel_cls):
    mod = _module()
    assert channel_cls.is_init(_make_provider(channel_cls, password="")) is False
    assert channel_cls.is_init(_make_provider(channel_cls, password="x.y.z")) is True


def test_init_auth_assigns_device_profile(channel_cls):
    """init_auth 补齐设备档案；档案一经分配稳定幂等（device_mid 不抖动）。"""
    mod = _module()
    p = _make_provider(channel_cls, password=_make_jwt({"sub": "u"}))
    # 池外实例没有 admin 落库链路，_ensure_profile 内的 persist 会被吞异常
    ok = asyncio.run(channel_cls.init_auth(p))
    assert ok is True
    fields = ("fp_platform", "fp_arch", "fp_os_version", "fp_language", "fp_timezone", "fp_screen", "device_mid")
    assert all(getattr(p, f) for f in fields)
    first_mid = p.device_mid
    # 再跑一遍不换档案
    asyncio.run(channel_cls.init_auth(p))
    assert p.device_mid == first_mid


def test_device_profile_shape_is_plausible():
    mod = _module()
    profile = mod._new_device_profile()
    assert (profile["fp_platform"], profile["fp_arch"]) in mod._PLATFORM_ARCHS
    assert profile["fp_os_version"] in mod._OS_VERSIONS[profile["fp_platform"]]
    assert (profile["fp_language"], profile["fp_timezone"]) in mod._LOCALES
    import uuid as _uuid
    _uuid.UUID(profile["device_mid"])  # 合法 UUID


# ==================== 签到 / 额度 / 领取（对齐 claim.py + telemetry.py）====================

def test_activation_event_body_has_official_16_fields():
    mod = _module()
    p = SimpleNamespace(device_mid="mid-1", fp_platform="win32", fp_arch="x64",
                       fp_os_version="10.0.26200", fp_language="zh-CN",
                       fp_timezone="Asia/Shanghai", fp_screen="2560x1440")
    body = mod._activation_event_body(p, "app_daily_active", "u-uuid")
    # 官方 sendReport 字段集固定 16 个，逐字段核对（对齐 telemetry.build_activation_event_body）
    assert set(body) == {
        "event_id", "client_timezone", "client_language", "element_name", "event_region",
        "event_type", "event_text", "event_extra_detail", "user_id", "screen_resolution",
        "app_version", "device_os_category", "device_os_version", "device_mid", "mac_id",
        "marketing_params",
    }
    assert body["event_region"] == "app" and body["event_type"] == "view"
    assert body["user_id"] == "u-uuid"
    assert body["app_version"] == mod.BILLING_APP_VERSION          # 计费族版本 3.11.2
    assert body["device_os_category"] == "windows"
    assert body["device_mid"] == "mid-1"
    assert body["mac_id"] == "" and body["marketing_params"] == "{}"
    # event_id 每次全新（合法 UUID）
    import uuid as _uuid
    _uuid.UUID(body["event_id"])


def test_business_code_and_claim_fail_messages():
    mod = _module()
    assert mod._business_code({"code": 0}) == 0
    assert mod._business_code({"code": 3007}) == 3007
    assert mod._business_code({"code": "1003"}) == 1003
    assert mod._business_code(None) == -1
    assert mod._business_code({}) == -1
    assert mod._business_code({"code": "abc"}) == -1
    # 领取失败文案带上游 msg
    assert "该套餐已经领取过" in mod._claim_fail_message(1003, {"msg": "dup"})
    assert "（dup）" in mod._claim_fail_message(1003, {"msg": "dup"})
    assert mod._claim_fail_message(9999, {}) == "领取失败"


def test_parse_plan_extracts_grants_and_priority():
    mod = _module()
    raw = {
        "plan_id": "plan-1", "name": "限时体验套餐", "description": "活动", "priority": 5,
        "entitlements": [
            {"meter": "model_usage", "unit_type": "token", "show_name": "GLM-5.3", "grant_units": 3000000, "period": "daily"},
            {"meter": "model_usage", "unit_type": "token", "show_name": "GLM-5-Turbo", "grantUnits": 1000000},
            {"meter": "other", "unit_type": "token", "show_name": "忽略"},
            {"meter": "model_usage", "unit_type": "credit", "show_name": "忽略"},
        ],
    }
    plan = mod._parse_plan(raw)
    assert plan["plan_id"] == "plan-1"
    assert plan["name"] == "限时体验套餐"
    assert plan["priority"] == 5
    assert [(g["name"], g["units"], g["period"]) for g in plan["grants"]] == [
        ("GLM-5.3", 3000000.0, "daily"),
        ("GLM-5-Turbo", 1000000.0, "one_time"),
    ]
    assert mod._parse_plan({"plan_id": ""}) is None
    assert mod._parse_plan("junk") is None


def test_claim_headers_carry_captcha_and_version_floor(channel_cls):
    mod = _module()
    p = _make_provider(channel_cls, password=_make_jwt({"sub": "u"}),
                       device_mid="m", fp_platform="linux", fp_arch="x64")
    headers = mod._claim_headers(p, "param-1", "cn")
    # 实测缺版本/平台头时即使验证码有效也 3007 —— 显式兜底 BILLING_APP_VERSION / X-Platform
    assert headers["X-ZCode-App-Version"] == mod.BILLING_APP_VERSION
    assert headers["X-Platform"] == mod.CLIENT_PLATFORM
    assert headers["X-Aliyun-Captcha-Verify-Param"] == "param-1"
    assert headers["X-Aliyun-Captcha-Verify-Region"] == "cn"
    assert headers["Authorization"] == f"Bearer {p.password}"
    # region 为空时不发 Region 头
    headers2 = mod._claim_headers(p, "param-2", "")
    assert "X-Aliyun-Captcha-Verify-Region" not in headers2


def test_summarize_claims_formats_outcomes():
    mod = _module()
    assert mod._summarize_claims([]) == ""
    assert mod._summarize_claims([{"ok": True, "plan_name": "A", "plan_id": "p1"}]) == "领取 1 个：A"
    mixed = [{"ok": True, "plan_name": "A", "plan_id": "p1"},
             {"ok": False, "plan_name": "B", "plan_id": "p2", "message": "名额用完"}]
    assert mod._summarize_claims(mixed) == "领取 1/2：A"


def test_refresh_auth_orchestrates_checkin_quota_claim(channel_cls):
    """refresh_auth 三件套：签到 → 额度（current + balance）→ 领取；全部 best-effort。"""
    mod = _module()
    p = _make_provider(channel_cls, password=_make_jwt({"user_id": "u-uuid"}))
    calls = []

    async def fake_report(p_):
        calls.append("checkin")
        return None

    async def fake_current(p_):
        calls.append("current")
        return "CodingPlan Pro"

    async def fake_balance(p_):
        calls.append("balance")
        return [{"show_name": "GLM-5.3", "remaining_units": 100}]

    async def fake_claim(p_):
        calls.append("claim")
        return [{"plan_id": "p1", "plan_name": "A", "ok": True, "message": "领取成功"}]

    mod._report_activation_events = fake_report
    mod._fetch_current_plan = fake_current
    mod._fetch_balance = fake_balance
    mod._auto_claim_plans = fake_claim

    # 直接调 spec 类的 refresh_auth（与 adapter 的 refresh_account_auth 同一钩子；
    # 在 mod 上调用让上面的模块级 patch 生效）
    fields = asyncio.run(mod.ZcodeChannel.refresh_auth(p, {"password": p.password, "credential_mode": "jwt"}))
    assert calls == ["checkin", "current", "balance", "claim"]
    assert fields["plan_name"] == "CodingPlan Pro"
    assert "GLM-5.3" in fields["quota_text"] and "100" in fields["quota_text"]
    assert fields["last_claim"] == "领取 1 个：A"
    assert fields["credential_mode"] == "jwt"


def test_refresh_auth_api_key_account_short_circuits(channel_cls):
    """apiKey 账号无签到/额度/领取语义：只返回模式字段，不发 billing 流量。"""
    mod = _module()
    p = _make_provider(channel_cls, password="sk.abc")
    calls = []
    async def fail_if_called(p_):
        calls.append(1)
        raise AssertionError("apiKey 账号不应触发 billing 流量")
    mod._report_activation_events = fail_if_called
    mod._fetch_current_plan = fail_if_called
    mod._fetch_balance = fail_if_called
    mod._auto_claim_plans = fail_if_called
    fields = asyncio.run(mod.ZcodeChannel.refresh_auth(p, {}))
    assert fields == {"credential_mode": "apiKey"}
    assert calls == []


def test_refresh_auth_steps_fail_independently(channel_cls):
    """任一步失败不阻断后续：签到失败仍取额度，额度失败仍领取。"""
    mod = _module()
    p = _make_provider(channel_cls, password=_make_jwt({"user_id": "u"}))
    order = []

    async def report_fail(p_):
        order.append("checkin-fail")
        return "JWT 无 user_id"

    async def balance_fail(p_):
        order.append("balance-fail")
        raise RuntimeError("waf")

    async def current_fail(p_):
        order.append("current-fail")
        raise RuntimeError("waf")

    async def claim_ok(p_):
        order.append("claim")
        return [{"plan_id": "p1", "plan_name": "A", "ok": True, "message": "领取成功"}]

    mod._report_activation_events = report_fail
    mod._fetch_current_plan = current_fail
    mod._fetch_balance = balance_fail
    mod._auto_claim_plans = claim_ok
    fields = asyncio.run(mod.ZcodeChannel.refresh_auth(p, {}))
    assert order == ["checkin-fail", "current-fail", "balance-fail", "claim"]
    assert fields["last_claim"] == "领取 1 个：A"
    assert "quota_text" not in fields  # balance 失败不写
    assert "plan_name" not in fields   # current 失败不写


def test_claim_one_plan_retries_on_captcha_and_idempotent_on_1003(channel_cls):
    """claim 流：3007 换码重试一次；1003 已领取过视为幂等成功。"""
    mod = _module()
    p = _make_provider(channel_cls, password=_make_jwt({"sub": "u"}))

    captcha_calls = []
    async def fake_captcha_get(p_):
        captcha_calls.append(1)
        return f"param-{len(captcha_calls)}", "cn"

    claim_responses = [
        {"code": 3007, "msg": "verify failed"},   # 第一次：验证码被拒
        {"code": 0, "data": {}},                    # 换码重试：成功
    ]
    post_calls = []
    async def fake_billing(p_, method, path, **kw):
        post_calls.append((method, path))
        return claim_responses.pop(0)

    mod._captcha_get = fake_captcha_get
    async def fake_config(p_):
        return {"region": "cn"}
    mod._captcha_config = fake_config
    mod._billing_request = fake_billing

    result = asyncio.run(mod._claim_one_plan(p, {"plan_id": "p1", "name": "A"}))
    assert result["ok"] is True
    assert result["plan_id"] == "p1"
    assert len(captcha_calls) == 2
    assert all((m, ph) == ("POST", "/billing/claim") for m, ph in post_calls)

    # 1003 幂等成功场景
    async def fake_billing_1003(p_, method, path, **kw):
        return {"code": 1003, "msg": "already claimed"}
    mod._billing_request = fake_billing_1003
    mod._captcha_get = fake_captcha_get
    result2 = asyncio.run(mod._claim_one_plan(p, {"plan_id": "p2", "name": "B"}))
    assert result2["ok"] is True
    assert "已领取过" in result2["message"]


def test_auto_claim_skips_when_no_plans(channel_cls):
    """preview 无套餐（活动未上线属正常态）：静默返回空，不触发 claim。"""
    mod = _module()
    p = _make_provider(channel_cls, password=_make_jwt({"sub": "u"}))
    calls = []

    async def fake_report(p_):
        calls.append("checkin")
        return None

    async def fake_preview(p_):
        calls.append("preview")
        raise HTTPException(status_code=409, detail="活动已结束或套餐暂不可领取")

    mod._report_activation_events = fake_report
    mod._preview_plans = fake_preview
    async def fail_if_claim(p_):
        raise AssertionError("无套餐不应 claim")
    mod._claim_one_plan = fail_if_claim

    outcomes = asyncio.run(mod._auto_claim_plans(p))
    assert outcomes == []
    assert calls == ["checkin", "preview"]


def test_auto_claim_claims_all_plans_and_reports_outcomes(channel_cls):
    """有可领套餐：激活上报 → preview → 逐个 claim，全部失败也只记 outcome 不抛。"""
    mod = _module()
    p = _make_provider(channel_cls, password=_make_jwt({"sub": "u"}))
    order = []

    async def fake_report(p_):
        order.append("checkin")
        return None

    async def fake_preview(p_):
        order.append("preview")
        return [{"plan_id": "p1", "name": "A", "priority": 2, "grants": []},
                {"plan_id": "p2", "name": "B", "priority": 1, "grants": []}]

    async def fake_claim(p_, plan):
        order.append(f"claim:{plan['plan_id']}")
        ok = plan["plan_id"] == "p1"
        return {"plan_id": plan["plan_id"], "plan_name": plan["name"], "ok": ok,
                "message": "领取成功" if ok else "今日领取名额已用完"}

    mod._report_activation_events = fake_report
    mod._preview_plans = fake_preview
    mod._claim_one_plan = fake_claim

    outcomes = asyncio.run(mod._auto_claim_plans(p))
    assert order == ["checkin", "preview", "claim:p1", "claim:p2"]
    assert len(outcomes) == 2
    assert outcomes[0]["ok"] is True and outcomes[1]["ok"] is False
    assert mod._summarize_claims(outcomes) == "领取 1/2：A"


def test_poll_device_flow_auto_claims_on_login(channel_cls):
    """OAuth 授权完成 → best-effort 自动领取 + 设备档案/领取结果随 account_data 回填。"""
    mod = _module()
    p = _make_provider(channel_cls, password="")
    jwt_token = _make_jwt({"sub": "uuid-12345678-aaaa"})

    async def fake_poll(p_, flow_id, poll_token):
        return {"status": "ready", "token": jwt_token}

    async def fake_claim(p_):
        return [{"plan_id": "p1", "plan_name": "A", "ok": True, "message": "领取成功"}]

    mod._oauth_poll = fake_poll
    mod._auto_claim_plans = fake_claim

    result = asyncio.run(mod.ZcodeChannel.poll_device_flow(p, {"flow_id": "f1", "poll_token": "t"}))
    assert result["status"] == "authorized"
    data = result["account_data"]
    assert data["password"] == jwt_token
    assert data["credential_mode"] == "jwt"
    assert data["device_mid"]  # 设备档案已生成并回填
    assert data["last_claim"] == "领取 1 个：A"
    # 用户名：上游无昵称时取 JWT user_id 前 8 位，同 JWT 幂等稳定
    assert data["username"] == "zai-uuid-123"


def test_poll_device_flow_username_prefers_upstream_nickname(channel_cls):
    """上游 poll 响应若带 user 昵称 → 优先作展示名。"""
    mod = _module()
    p = _make_provider(channel_cls, password="")
    jwt_token = _make_jwt({"user_id": "uuid-99999999-bbbb"})

    async def fake_poll(p_, flow_id, poll_token):
        return {"status": "ready", "token": jwt_token,
                "user": {"nickname": "张三", "id": "uuid-99999999-bbbb"}}

    async def fake_claim(p_):
        return []

    mod._oauth_poll = fake_poll
    mod._auto_claim_plans = fake_claim

    result = asyncio.run(mod.ZcodeChannel.poll_device_flow(p, {"flow_id": "f1", "poll_token": "t"}))
    data = result["account_data"]
    assert data["username"] == "张三"
    assert data["user_name"] == "张三"
    assert data["account_id"] == "uuid-99999999-bbbb"


def test_poll_device_flow_username_unique_without_user_id(channel_cls):
    """JWT 解不出 user_id 时兜底随机名，绝不两个账号同名（admin 按 username upsert）。"""
    mod = _module()
    p = _make_provider(channel_cls, password="")
    bad_jwt = "h1.!!!not-base64!!!.sig"  # 解不出 payload

    async def fake_poll(p_, flow_id, poll_token):
        return {"status": "ready", "token": bad_jwt}

    async def fake_claim(p_):
        return []

    mod._oauth_poll = fake_poll
    mod._auto_claim_plans = fake_claim

    r1 = asyncio.run(mod.ZcodeChannel.poll_device_flow(p, {"flow_id": "f1", "poll_token": "t"}))
    r2 = asyncio.run(mod.ZcodeChannel.poll_device_flow(p, {"flow_id": "f2", "poll_token": "t"}))
    u1, u2 = r1["account_data"]["username"], r2["account_data"]["username"]
    assert u1 != u2
    assert u1.startswith("zcode-")


def test_poll_device_flow_login_unaffected_by_claim_failure(channel_cls):
    """领取失败绝不阻断入池：account_data 仍完整，last_claim 留空。"""
    mod = _module()
    p = _make_provider(channel_cls, password="")
    jwt_token = _make_jwt({"sub": "u"})

    async def fake_poll(p_, flow_id, poll_token):
        return {"status": "ready", "token": jwt_token}

    async def claim_boom(p_):
        raise RuntimeError("solver not installed")

    mod._oauth_poll = fake_poll
    mod._auto_claim_plans = claim_boom

    result = asyncio.run(mod.ZcodeChannel.poll_device_flow(p, {"flow_id": "f1", "poll_token": "t"}))
    assert result["status"] == "authorized"
    assert result["account_data"]["password"] == jwt_token
    assert result["account_data"].get("last_claim", "") == ""


# ==================== 端到端：stream_chat / non_stream_chat（mock 上游）====================

def _sse_line(payload: dict) -> str:
    return f"event: {payload['type']}\ndata: {json.dumps(payload)}\n\n"


def test_stream_chat_end_to_end_api_key_mode(channel_cls):
    """apiKey 账号全链路：出站 URL/头 + anthropic SSE → 统一帧（content/usage/finish/done）。"""
    p = _make_provider(channel_cls, password="sk.abc123")

    events = [
        _sse_line({"type": "message_start", "message": {"id": "m1", "usage": {"input_tokens": 5}}}),
        _sse_line({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}),
        _sse_line({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "hello"}}),
        _sse_line({"type": "message_delta", "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 3}}),
        _sse_line({"type": "message_stop"}),
    ]

    async def fake_send(method, url, headers, **kw):
        assert url == "https://api.z.ai/api/anthropic/v1/messages"
        assert headers["x-api-key"] == "sk.abc123"
        assert "Authorization" not in headers
        assert headers["anthropic-version"] == "2023-06-01"
        for e in events:
            yield e

    p.send_sse_request = fake_send
    raw_body = {"model": "glm-5.3", "max_tokens": 100, "stream": True,
                "messages": [{"role": "user", "content": "hi"}],
                "system": [{"type": "text", "text": "my system"}]}
    frames = [f for f in asyncio.run(_collect(channel_cls, p, raw_body)) if isinstance(f, dict)]

    contents = "".join(f.get("content") or "" for f in frames)
    assert contents == "hello"
    assert any(f.get("finish_reason") == "stop" for f in frames)
    usage_frame = next(f for f in frames if isinstance(f.get("usage"), dict))
    assert usage_frame["usage"] == {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8}
    assert any(f.get("done") for f in frames)


async def _collect(channel_cls, p, raw_body):
    out = []
    async for f in channel_cls._do_stream_chat(p, "glm-5.3", [], _raw_anthropic_body=raw_body):
        out.append(f)
    return out


def test_non_stream_chat_end_to_end_api_key_mode(channel_cls):
    """非流式全链路：tool_use 块 → OpenAI tool_calls + usage + finish_reason。"""
    p = _make_provider(channel_cls, password="sk.x")
    upstream = {"id": "m2", "model": "GLM-5.3", "stop_reason": "tool_use",
                "content": [{"type": "text", "text": "let me search"},
                             {"type": "tool_use", "id": "tu_1", "name": "search", "input": {"q": "x"}}],
                "usage": {"input_tokens": 4, "output_tokens": 9}}

    class _Resp:
        def __init__(self, text): self._text = text; self.status = 200
        async def text(self): return self._text

    class _Post:
        async def __aenter__(self): return _Resp(json.dumps(upstream))
        async def __aexit__(self, *a): pass

    class _Sess:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): pass
        def post(self, url, headers=None, json=None, proxy=None, timeout=None):
            assert url == "https://api.z.ai/api/anthropic/v1/messages"
            assert headers["x-api-key"] == "sk.x"
            return _Post()

    p._make_session = lambda timeout=None: _Sess()
    raw_body = {"model": "glm-5.3", "max_tokens": 100, "messages": [{"role": "user", "content": "go"}]}
    resp = asyncio.run(channel_cls._do_non_stream_chat(p, "glm-5.3", [], _raw_anthropic_body=raw_body))
    msg = resp["choices"][0]["message"]
    assert msg["content"] == "let me search"
    tc = msg["tool_calls"][0]
    assert tc["id"] == "tu_1" and tc["function"]["name"] == "search"
    assert json.loads(tc["function"]["arguments"]) == {"q": "x"}
    assert resp["choices"][0]["finish_reason"] == "tool_calls"
    assert resp["usage"] == {"prompt_tokens": 4, "completion_tokens": 9, "total_tokens": 13}


def test_stream_chat_jwt_mode_requires_captcha_solver(channel_cls, monkeypatch, tmp_path):
    """JWT 账号 + 求解器不可得：抛 500 且文案含安装指引（绝不静默失败）。

    自动发现会命中仓库的 .tmp-zcode2api（若已克隆），所以本测试把 cwd 换到
    无求解器的临时目录 + 清空环境变量，保证走「未找到」分支而不会拉起真实
    node 求解进程。
    """
    import os as _os
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ZCODE_CAPTCHA_SOLVER", raising=False)
    p = _make_provider(channel_cls, password=_make_jwt({"sub": "u"}))
    with pytest.raises(Exception) as excinfo:
        asyncio.run(_collect(channel_cls, p, {"model": "glm-5.3", "messages": []}))
    msg = str(excinfo.value)
    assert "求解器" in msg or "captcha_solver_path" in msg or "ZCODE_CAPTCHA_SOLVER" in msg
