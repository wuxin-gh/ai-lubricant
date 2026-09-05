"""代码渠道 spec 契约 + 适配器。

作者在管理端「源码」Tab 贴一个**普通类**（不继承任何框架基类），方法用 ``@staticmethod``
声明、第一个参数 ``p`` 就是渠道实例。这个类只写想改的钩子；没写的一切回落到
``CustomProvider`` 的配置驱动实现（base_url / 协议 / 超时都读渠道配置）。

``build_adapter_class(name, spec)`` 把这个 spec 类包成一个 ``CustomProvider`` 子类适配器：
适配器持有指针 ``cls.spec = <作者的类>``，每个框架方法先查 spec 有没有对应钩子，有就
``hook(self, ...)``，没有就 ``super()`` 回落。于是「贴一个 spec 类 = 造一个同级渠道」。

钩子命名用友好别名（框架里带下划线的一律去掉）。完整清单见 ``HOOK_ALIASES`` 与
``docs/providers/code-channel.md``。调用约定：钩子首参 ``p`` = 渠道实例；``@staticmethod``
与普通 ``def self`` 两种写法都能跑（收集时统一按 ``fn(provider, ...)`` 调用）。
"""
from __future__ import annotations

import inspect
from typing import Callable

from providers.base import BaseProvider
from providers.custom import CustomProvider


class CodeSpecError(Exception):
    """spec 类不合法（无可识别钩子 / 多个候选 / 误继承基类）。"""


# ---- 钩子别名 -> 说明（值仅用于报错文案）----
HOOK_ALIASES: dict[str, str] = {
    # 认证生命周期
    "init_auth": "准备/刷新认证 (p, is_check=False) -> bool",
    "check_auth": "校验登录态 (p) -> bool",
    "is_init": "凭据是否就绪 (p) -> bool",
    "health_check": "健康检查 (p) -> bool",
    "refresh_auth": "刷新令牌并写回 (p, account, cfg) -> dict",
    # 聊天
    "stream_chat": "流式聊天 async def (p, model_id, messages, **kw) yield 帧",
    "non_stream_chat": "非流式聊天 (p, model_id, messages, **kw) -> OpenAI 响应体",
    # 媒体生成（图片/视频/语音）：默认走 CustomProvider 的 OpenAI 标准格式
    # （POST /v1/images/generations 等），但不少上游媒体生成是非标的（如 duck.ai
    # 图片走 chat SSE + GenerateImage 工具）。spec 声明这些钩子即可接管，否则回落默认。
    "generate_image": "图片生成 async def (p, model_id, prompt, **kw) -> OpenAI images 响应体 {created,data:[{b64_json|url}]}",
    "generate_video": "视频生成 async def (p, model_id, prompt, **kw) -> OpenAI 视频响应体",
    "generate_speech": "语音合成 async def (p, model_id, text, **kw) -> OpenAI TTS 响应体",
    # 模型列表
    "fetch_models": "拉取上游模型 (p) -> list[dict]",
    # 请求塑形
    "headers": "后处理请求头 (p, base_headers, kwargs) -> dict",
    "build_url": "覆盖聊天 URL (p, kwargs) -> str",
    "base_url": "覆盖渠道地址 (p) -> str|None；影响聊天/模型/图片/视频/语音全部路径",
    "payload": "后处理请求体 (p, endpoint, model_id, messages, stream, kwargs) -> dict；kwargs['_base_payload'] 是标准 body",
    "parse_chunk": "自定义 SSE 帧解析 (p, event_str) -> dict|None",
    # 响应 / 配额回调
    "on_response_headers": "响应头回调 (p, headers, model_id, ctx)",
    "update_quota": "从响应头更新配额 (p, headers, model_id)",
    "check_message": "请求前放行判定 (p, model_id, messages) -> bool",
    "record_message": "请求记账 (p, model_id, messages)",
    # 账号授权
    "account_schema": "声明前端字段/授权入口 () -> dict",
    "build_auth_start": "回调式授权启动 (name, account, redirect_uri, state, cfg, auth_context=None) -> dict",
    "handle_auth_callback": "回调式授权回调 (name, params, state_data, cfg) -> dict",
    "handle_loopback_callback": "本机回调处理 (p, params, poll_params, callback_url='') -> dict|None",
    "begin_device_flow": "设备码授权启动 (p, auth_context=None) -> dict",
    "poll_device_flow": "设备码授权轮询 (p, poll_params, auth_context=None) -> dict",
    # 杂项生命周期
    "on_channel_attached": "渠道配置注入后钩子 (p)",
    "clear_conversations": "清理会话 (p, max_age_hours) -> int",
    "close": "收尾 (p)",
}

# 复制到适配器类的类级开关（作者在 spec 上写 SUPPORTS_XXX = True 即生效）。
CLASS_FLAGS = (
    "SUPPORTS_TOKEN_AUTO_REFRESH",
    "SUPPORTS_MULTI_MESSAGES",
    "NEEDS_STREAM_TOOL_FALLBACK",
    "SCHEDULED_REFRESH",
    # tools-as-prompt：上游不认 OpenAI function calling，或上游有自己的服务端工具集。
    # spec 写 TOOLS_AS_PROMPT = True 即可让框架统一：payload 不带 tools（用
    # p.tools_for_payload(kwargs.get("tools")) 取值）、工具说明进 system、历史轮
    # tool_calls/tool 结果自动文本化。不声明则按原生 tools 直传上游。
    # 不发 tools 是硬要求：上游收到该字段就查自家工具表，本地工具在它那边不存在，
    # 会直接回「工具名称不存在: xxx」，提示词措辞拦不住。
    "TOOLS_AS_PROMPT",
    # 文本形态：xml / json / hermes，决定提示词与历史轮渲染（两侧必须同形态）。
    "TOOLS_PROMPT_FORMAT",
    # 渠道地址是否必填。默认 True：管理端建/改渠道时 base_url 空就 400，因为「渠道地址」
    # 是前端让用户填的字段，spec 不该把上游域名写死（写死等于骗用户：填了不生效）。
    # 完全不打外网的 spec（如 Echo 样例、纯本地 mock）写 REQUIRES_BASE_URL = False 豁免。
    "REQUIRES_BASE_URL",
    # 账号级字段名清单：这些 key 从账号配置取值、挂成实例属性，spec 钩子直接 p.xxx 读。
    # 与 account_schema() 声明的字段自动求并集，所以只需在这里补 schema 里没有的运行时
    # 派生字段（如 access_token_expires_at_ms）。详见 _account_field_names。
    "ACCOUNT_FIELDS",
    # 是否套 client_preset 伪装头。默认 True（沿用 CustomProvider 行为）。
    # 上游按自己的 CLI 校验请求头、不能带 opencode/claude-code 那套伪装头的渠道写 False：
    # 框架只发认证头，spec 再用 headers 钩子补自己的。注意 client_preset="none" 不等于
    # 不套——_effective_client_preset 会按协议兜到默认 preset，只有这个开关能真正关掉。
    "APPLY_CLIENT_PRESET",
)


def collect_hooks(spec: type) -> dict[str, Callable]:
    """扫 spec 类（含 MRO）收集所有可识别钩子 -> 裸函数。子类覆盖父类同名钩子。"""
    hooks: dict[str, Callable] = {}
    for klass in reversed(spec.__mro__):
        if klass is object:
            continue
        for alias in HOOK_ALIASES:
            fn = _resolve_hook(klass, alias)
            if fn is not None:
                hooks[alias] = fn
    return hooks


def _unknown_public_methods(spec: type) -> list[str]:
    """spec 里既不是合法钩子、又不是私有/下划线的公开可调用名——大概率是拼错的钩子。"""
    out: list[str] = []
    for name, raw in vars(spec).items():
        if name.startswith("_") or name in HOOK_ALIASES or name in CLASS_FLAGS:
            continue
        target = raw.__func__ if isinstance(raw, (staticmethod, classmethod)) else raw
        if callable(target):
            out.append(name)
    return out


def validate_spec(name: str, spec: type) -> dict[str, Callable]:
    """校验 spec 类，返回钩子表。误继承基类 / 无可识别钩子都抛 CodeSpecError。"""
    if issubclass(spec, BaseProvider):
        raise CodeSpecError(
            f"代码渠道 {name!r} 的类 {spec.__name__} 不应继承 BaseProvider / CustomProvider。"
            f"请写一个普通类，方法用 @staticmethod、第一个参数为渠道实例 p，"
            f"框架能力通过 p.xxx 调用。合法钩子：{', '.join(sorted(HOOK_ALIASES))}"
        )
    hooks = collect_hooks(spec)
    if not hooks:
        unknown = _unknown_public_methods(spec)
        hint = f"（发现未识别方法：{', '.join(unknown)}，是否拼错？）" if unknown else ""
        raise CodeSpecError(
            f"代码渠道 {name!r} 的类 {spec.__name__} 未定义任何可识别钩子{hint}。"
            f"合法钩子：{', '.join(sorted(HOOK_ALIASES))}"
        )
    return hooks


def _resolve_hook(spec: type, alias: str) -> Callable | None:
    """在 spec 类里找别名钩子，返回可直接 ``fn(provider, ...)`` 调用的裸函数。

    ``@staticmethod`` / ``@classmethod`` 取 ``__func__``，普通 ``def`` 取函数本身；
    找不到返回 None。
    """
    raw = spec.__dict__.get(alias)
    if raw is None:
        return None
    if isinstance(raw, (staticmethod, classmethod)):
        return raw.__func__
    if callable(raw):
        return raw
    return None


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


def _accepts_extra_positional(fn: Callable, base_count: int) -> bool:
    """钩子能否多收一个位置参数（在 ``base_count`` 个必传参数之后）。

    用于 ``auth_context`` 这类**追加的可选尾参**：老 spec 写 ``begin_device_flow(p)``，
    新 spec 写 ``begin_device_flow(p, auth_context=None)``，框架按签名决定传几个参数。

    必须是**事先探测签名**而不是「先多传、撞 TypeError 再重试少传」——钩子体内部自己抛的
    ``TypeError`` 会被那种写法误当成签名不匹配而重放一次，产生重复的上游副作用
    （设备码流里就是重复申请一次授权）。签名取不到时按老签名调用（保守）。
    """
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):  # 内建/C 实现等取不到签名
        return False
    slots = 0
    for param in sig.parameters.values():
        if param.kind is inspect.Parameter.VAR_POSITIONAL:
            return True  # *args 能吃下任意多个
        if param.kind in (inspect.Parameter.POSITIONAL_ONLY,
                          inspect.Parameter.POSITIONAL_OR_KEYWORD):
            slots += 1
    return slots > base_count


async def _call_with_optional_context(fn: Callable, args: tuple, auth_context):
    """调钩子；签名容得下就把 ``auth_context`` 作为尾参传入，否则按老签名调。"""
    if auth_context is not None and _accepts_extra_positional(fn, len(args)):
        return await _maybe_await(fn(*args, auth_context))
    return await _maybe_await(fn(*args))


def _spec_plain_callable(spec: type, alias: str) -> Callable | None:
    """解析「无渠道实例」的类级钩子（account_schema / build_auth_start / handle_auth_callback）。

    返回一个可直接调用的 callable：``@staticmethod`` / 普通 ``def`` 原样返回；
    ``@classmethod`` 包一层把 spec 作为 cls 传入。找不到返回 None。
    """
    raw = None
    for klass in spec.__mro__:
        if alias in klass.__dict__:
            raw = klass.__dict__[alias]
            break
    if raw is None:
        return None
    if isinstance(raw, classmethod):
        fn = raw.__func__

        def wrapper(*a, **k):
            return fn(spec, *a, **k)

        # ``inspect.signature`` normally sees ``*a, **k`` on the wrapper, which would make
        # every legacy class hook look context-aware. Preserve the underlying signature
        # minus ``cls`` so optional trailing auth_context stays genuinely backward compatible.
        try:
            params = list(inspect.signature(fn).parameters.values())[1:]
            wrapper.__signature__ = inspect.Signature(params)  # type: ignore[attr-defined]
        except (TypeError, ValueError):
            pass
        return wrapper
    if isinstance(raw, staticmethod):
        return raw.__func__
    if callable(raw):
        return raw
    return None


def _account_field_names(spec: type) -> tuple[str, ...]:
    """spec 要挂成实例属性的账号字段名：``account_schema()`` 声明的字段 ∪ ``ACCOUNT_FIELDS``。

    ``account_schema`` 是「前端表单显示哪些输入框」的声明，凡是声明了的字段前端都会写回
    账号行，spec 钩子理应能读到，所以自动纳入、不必重复列一遍。``ACCOUNT_FIELDS`` 补的是
    表单里没有、但运行时要读写的派生字段（如 ``access_token_expires_at_ms``）。

    ``account_schema`` 求值失败（作者写挂了）不阻断加载：字段集退化成只有 ``ACCOUNT_FIELDS``，
    钩子照常注入，作者在管理端点开授权时才会看到自己的报错。
    """
    names: list[str] = []
    fn = _spec_plain_callable(spec, "account_schema")
    if fn is not None:
        try:
            schema = fn()
        except Exception:  # noqa: BLE001 - schema 是作者代码，坏了不该拖垮整个渠道加载
            schema = None
        if isinstance(schema, dict):
            for field in schema.get("fields") or []:
                if isinstance(field, dict):
                    key = field.get("key")
                    # username/password 由 BaseProvider.__init__ 持有，别在这里二次覆盖。
                    if isinstance(key, str) and key and key not in ("username", "password"):
                        names.append(key)
    declared = getattr(spec, "ACCOUNT_FIELDS", ()) or ()
    if isinstance(declared, str):  # 作者写成 "access_token" 而非元组时的容错
        declared = (declared,)
    for key in declared:
        if isinstance(key, str) and key and key not in ("username", "password"):
            names.append(key)
    return tuple(dict.fromkeys(names))  # 去重且保序


def _make_adapter_init(account_fields: tuple[str, ...]):
    """造适配器 ``__init__``：把账号字段挂成实例属性，再跑 CustomProvider 的。

    ``CustomProvider.__init__`` 只从 kwargs 取 ``_api_key``，其余账号字段（access_token /
    refresh_token / enterprise_id / …）会丢掉，spec 钩子就读不到。这里按字段集补挂，让
    ``p.access_token`` 这类写法成立。

    同时也是 admin 热更新的前提：``_apply_account_update_in_place`` 用
    ``hasattr(provider, key)`` 决定要不要把新凭据写进运行态实例，属性不存在就整段跳过，
    刷新出来的 token 永远进不了内存对象。所以缺值也要显式挂空串占位，不能只挂有值的。
    """
    def __init__(self, username: str, password: str = "", proxy: str = None, **kwargs):
        # 先挂字段再跑父类：CustomProvider.__init__ 会为池外临时实例 attach fallback Channel，
        # 进而调用 spec.on_channel_attached；设备码 scanner / 手动刷新实例没有第二次 attach，
        # 若晚挂字段，钩子看到的 access_token/region 等会是 AttributeError。
        for key in account_fields:
            value = kwargs.get(key)
            setattr(self, key, "" if value is None else value)
        CustomProvider.__init__(self, username, password, proxy, **kwargs)

    return __init__


# ==================== 适配器方法（模块级，装进每个适配器类的 namespace）====================
# 统一用显式 ``CustomProvider.xxx(self, ...)`` 回落，而非零参 super()——这些函数在模块级定义、
# 动态塞进 type() 造的类里，没有 __class__ cell，零参 super() 会失效。适配器直接继承
# CustomProvider（中间无其它基类），显式调用等价于 super()。


async def _adapter_init_auth(self, is_check: bool = False) -> bool:
    hook = self._spec_hooks.get("init_auth")
    if hook is not None:
        return bool(await _maybe_await(hook(self, is_check)))
    return await CustomProvider.init_auth(self, is_check)


async def _adapter_check_auth(self) -> bool:
    hook = self._spec_hooks.get("check_auth")
    if hook is not None:
        return bool(await _maybe_await(hook(self)))
    return await CustomProvider.check_auth(self)


def _adapter_is_init(self) -> bool:
    hook = self._spec_hooks.get("is_init")
    if hook is not None:
        return bool(hook(self))
    return CustomProvider.is_init(self)


async def _adapter_health_check(self) -> bool:
    hook = self._spec_hooks.get("health_check")
    if hook is not None:
        return bool(await _maybe_await(hook(self)))
    return await CustomProvider.health_check(self)


async def _adapter_fetch_upstream_model_list(self) -> list:
    hook = self._spec_hooks.get("fetch_models")
    if hook is not None:
        return await _maybe_await(hook(self))
    return await CustomProvider.fetch_upstream_model_list(self)


async def _adapter_do_stream_chat(self, model_id, messages, **kwargs):
    hook = self._spec_hooks.get("stream_chat")
    if hook is not None:
        async for chunk in hook(self, model_id, messages, **kwargs):
            yield chunk
        return
    # 只写了 non_stream_chat：把非流式结果拍成单帧 yield（绝不回落配置驱动去发第二次）。
    ns_hook = self._spec_hooks.get("non_stream_chat")
    if ns_hook is not None:
        result = await _maybe_await(ns_hook(self, model_id, messages, **kwargs))
        message = (result.get("choices") or [{}])[0].get("message", {}) if isinstance(result, dict) else {}
        yield {
            "content": message.get("content", ""),
            "thinking": message.get("reasoning_content", ""),
            "tool_calls": message.get("tool_calls", []),
            "usage": result.get("usage") if isinstance(result, dict) else None,
        }
        return
    async for chunk in CustomProvider._do_stream_chat(self, model_id, messages, **kwargs):
        yield chunk


async def _adapter_do_non_stream_chat(self, model_id, messages, **kwargs):
    hook = self._spec_hooks.get("non_stream_chat")
    if hook is not None:
        return await _maybe_await(hook(self, model_id, messages, **kwargs))
    # 只写了 stream_chat：聚合流式帧成 OpenAI 响应体（绝不回落配置驱动去发第二次）。
    if "stream_chat" in self._spec_hooks:
        return await self._collect_stream_chat_result(model_id, messages, **kwargs)
    return await CustomProvider._do_non_stream_chat(self, model_id, messages, **kwargs)


# ---- 媒体生成：spec 声明即接管，否则回落 CustomProvider 的 OpenAI 标准格式 ----

async def _adapter_generate_image(self, model_id, prompt, **kwargs):
    hook = self._spec_hooks.get("generate_image")
    if hook is not None:
        return await _maybe_await(hook(self, model_id, prompt, **kwargs))
    return await CustomProvider.generate_image(self, model_id, prompt, **kwargs)


async def _adapter_generate_video(self, model_id, prompt, **kwargs):
    hook = self._spec_hooks.get("generate_video")
    if hook is not None:
        return await _maybe_await(hook(self, model_id, prompt, **kwargs))
    return await CustomProvider.generate_video(self, model_id, prompt, **kwargs)


async def _adapter_generate_speech(self, model_id, text, **kwargs):
    hook = self._spec_hooks.get("generate_speech")
    if hook is not None:
        return await _maybe_await(hook(self, model_id, text, **kwargs))
    return await CustomProvider.generate_speech(self, model_id, text, **kwargs)


# ---- 请求塑形：headers/payload 是「后处理」，build_url/parse_chunk 是「覆盖」 ----

def _adapter_headers(self, endpoint=None, apply_preset: bool = True, kwargs: dict | None = None) -> dict:
    # CustomProvider._headers 的签名是 (endpoint, apply_preset, kwargs)；先按框架算好，
    # 再交作者后处理。作者钩子签名 headers(p, base_headers, kwargs)。
    # APPLY_CLIENT_PRESET=False 的 spec 强制关掉伪装头：只留认证头，其余交 headers 钩子补。
    # 必须在这里关——client_preset="none" 会被 _effective_client_preset 兜到协议默认 preset，
    # 配置层关不掉。
    if getattr(self, "APPLY_CLIENT_PRESET", True) is False:
        apply_preset = False
    base = CustomProvider._headers(self, endpoint, apply_preset, kwargs)
    hook = self._spec_hooks.get("headers")
    if hook is not None:
        out = hook(self, base, kwargs or {})
        if out:
            return out
    return base


def _adapter_base_url(self):
    """覆盖型 property：作者 base_url(p) 返回非空字符串即生效，否则回落渠道配置。

    比 build_url 更靠底层——_url / _chat_url / _image_url / _video_url / _speech_url 和
    模型列表都读 self.base_url，所以一个钩子覆盖全部出站路径（Copilot 的
    endpoints.api 动态 base 就是这个形态）。
    """
    hook = self._spec_hooks.get("base_url")
    if hook is not None:
        out = hook(self)
        if out:
            return out
    return CustomProvider.base_url.fget(self)


def _adapter_chat_url(self, kwargs: dict | None = None) -> str:
    hook = self._spec_hooks.get("build_url")
    if hook is not None:
        out = hook(self, kwargs or {})
        if out:
            return out
    return CustomProvider._chat_url(self, kwargs)


def _adapter_build_protocol_payload(self, endpoint, model_id, messages, stream, **kwargs) -> dict:
    base = CustomProvider._build_protocol_payload(self, endpoint, model_id, messages, stream, **kwargs)
    hook = self._spec_hooks.get("payload")
    if hook is not None:
        # 作者通常只想在标准 body 上改一个字段。把框架结果以内部键透传给钩子，
        # 不改变对外签名；钩子返回 falsy 仍保留标准 body。
        hook_kwargs = {**kwargs, "_base_payload": base}
        out = hook(self, endpoint, model_id, messages, stream, hook_kwargs)
        if out:
            return out
    return base


def _adapter_parse_sse_data(self, event_str):
    """覆盖型：作者 parse_chunk(p, event_str) 返回 dict|None。

    dict -> (content, thinking, tool_calls) 元组（对齐 BaseProvider.parse_sse_data 契约）；
    None / 未声明 -> 回落标准协议解析。
    """
    hook = self._spec_hooks.get("parse_chunk")
    if hook is not None:
        out = hook(self, event_str)
        if out:
            return (
                out.get("content", "") or "",
                out.get("thinking", "") or "",
                out.get("tool_calls", []) or [],
            )
        if out is not None:  # 显式返回 falsy dict / 空 -> 视为「本帧无内容」
            return "", "", []
    return CustomProvider.parse_sse_data(event_str)


# ---- 响应 / 配额回调 ----

async def _adapter_on_response_headers(self, headers, model_id=None, context=None) -> None:
    hook = self._spec_hooks.get("on_response_headers")
    if hook is not None:
        await _maybe_await(hook(self, headers, model_id, context))
        return
    await CustomProvider.on_response_headers(self, headers, model_id, context)


async def _adapter_update_quota_from_headers(self, headers, model_id=None) -> None:
    hook = self._spec_hooks.get("update_quota")
    if hook is not None:
        await _maybe_await(hook(self, headers, model_id))
        return
    await CustomProvider.update_quota_from_headers(self, headers, model_id)


async def _adapter_check_message(self, model_id, messages=None) -> bool:
    hook = self._spec_hooks.get("check_message")
    if hook is not None:
        return bool(await _maybe_await(hook(self, model_id, messages)))
    return await CustomProvider.check_message(self, model_id, messages)


async def _adapter_record_message(self, model_id, messages=None) -> None:
    hook = self._spec_hooks.get("record_message")
    if hook is not None:
        await _maybe_await(hook(self, model_id, messages))
        return
    await CustomProvider.record_message(self, model_id, messages)


# ---- 账号授权：设备码（实例方法）+ 本机回调 + 刷新 ----

async def _adapter_begin_device_flow(self, auth_context: dict | None = None) -> dict:
    hook = self._spec_hooks["begin_device_flow"]  # 仅在声明时注入
    return await _call_with_optional_context(hook, (self,), auth_context)


async def _adapter_poll_device_flow(self, poll_params: dict, auth_context: dict | None = None) -> dict:
    hook = self._spec_hooks.get("poll_device_flow")
    if hook is not None:
        return await _call_with_optional_context(hook, (self, poll_params), auth_context)
    return {"status": "pending"}


async def _adapter_handle_loopback_callback(self, params: dict, poll_params: dict, callback_url: str = ""):
    """本机回调（上游只认 127.0.0.1:{port} 那类桌面客户端模型）。未认领返回 None。

    ``params`` 是解析好的 query 参数；``callback_url`` 是整条回调地址——手工补投
    （跨机场景用户把地址栏 URL 粘回来）时按签名可选接收，spec 需要片段（#fragment）
    或非常规参数时自己解析。真实浏览器回调没有整条 URL（只有 query），传空串。
    """
    hook = self._spec_hooks.get("handle_loopback_callback")
    if hook is None:
        return None
    if callback_url and _accepts_extra_positional(hook, 3):
        return await _maybe_await(hook(self, params, poll_params, callback_url))
    return await _maybe_await(hook(self, params, poll_params))


async def _adapter_refresh_account_auth(self, account: dict, cfg: dict | None = None) -> dict:
    hook = self._spec_hooks["refresh_auth"]  # 仅在声明时注入
    return await _maybe_await(hook(self, account, cfg))


# ---- 杂项生命周期 ----

def _adapter_on_channel_attached(self) -> None:
    CustomProvider._on_channel_attached(self)
    hook = self._spec_hooks.get("on_channel_attached")
    if hook is not None:
        hook(self)


async def _adapter_clear_conversations(self, max_age_hours: int = 2):
    hook = self._spec_hooks.get("clear_conversations")
    if hook is not None:
        return await _maybe_await(hook(self, max_age_hours))
    return await CustomProvider.clear_conversations(self, max_age_hours)


async def _adapter_close(self):
    hook = self._spec_hooks.get("close")
    if hook is not None:
        await _maybe_await(hook(self))
        return
    await CustomProvider.close(self)


# 别名钩子 -> 适配器方法名 + 挂载点（实例方法名）。
# always=True 常驻（回落 CustomProvider）；always=False 仅在 spec 声明对应钩子时才注入
# （admin 靠 getattr/501 探测能力，无脑常驻会让探测误判）。
_INSTANCE_BINDINGS = (
    # (适配器函数, 挂到适配器类上的方法名, always)
    (_adapter_init_auth, "init_auth", True),
    (_adapter_check_auth, "check_auth", True),
    (_adapter_is_init, "is_init", True),
    (_adapter_health_check, "health_check", True),
    (_adapter_fetch_upstream_model_list, "fetch_upstream_model_list", True),
    (_adapter_do_stream_chat, "_do_stream_chat", True),
    (_adapter_do_non_stream_chat, "_do_non_stream_chat", True),
    (_adapter_headers, "_headers", True),
    (_adapter_chat_url, "_chat_url", True),
    (_adapter_build_protocol_payload, "_build_protocol_payload", True),
    (_adapter_parse_sse_data, "parse_sse_data", True),
    (_adapter_on_response_headers, "on_response_headers", True),
    (_adapter_update_quota_from_headers, "update_quota_from_headers", True),
    (_adapter_check_message, "check_message", True),
    (_adapter_record_message, "record_message", True),
    (_adapter_on_channel_attached, "_on_channel_attached", True),
    (_adapter_clear_conversations, "clear_conversations", True),
    (_adapter_close, "close", True),
    # 能力探测型：仅在声明时注入
    (_adapter_begin_device_flow, "begin_device_flow", False),
    (_adapter_poll_device_flow, "poll_device_flow", False),
    (_adapter_handle_loopback_callback, "handle_loopback_callback", False),
    (_adapter_refresh_account_auth, "refresh_account_auth", False),
    (_adapter_generate_image, "generate_image", False),
    (_adapter_generate_video, "generate_video", False),
    (_adapter_generate_speech, "generate_speech", False),
)

# always=False 的方法名 -> 触发注入的 spec 钩子别名。
_PRESENCE_TRIGGER = {
    "begin_device_flow": "begin_device_flow",
    "poll_device_flow": "poll_device_flow",
    "handle_loopback_callback": "handle_loopback_callback",
    "refresh_account_auth": "refresh_auth",
    "generate_image": "generate_image",
    "generate_video": "generate_video",
    "generate_speech": "generate_speech",
}


def build_adapter_class(name: str, spec: type) -> type:
    """把作者的 spec 类包成一个 ``CustomProvider`` 子类适配器。

    - 校验 spec（误继承基类 / 无钩子都抛 CodeSpecError）。
    - 适配器持有 ``cls.spec = <作者的类>`` 与 ``cls._spec_hooks = {别名: 裸函数}`` 指针，
      每个框架方法先查钩子、有就调、没有就回落 CustomProvider。
    - 类级开关（SUPPORTS_*/SCHEDULED_REFRESH）从 spec 复制。
    - classmethod 型账号钩子（account_schema/build_auth_start/handle_auth_callback）在
      适配器上定义为 classmethod，读 cls.spec —— 因为 admin 用 provider_class.<method>() 调用。
    - always=False 的能力探测方法仅在 spec 声明对应钩子时注入（否则不出现，admin 探测走 501 回落）。
    """
    hooks = validate_spec(name, spec)

    namespace: dict = {
        "PROVIDER_NAME": name,
        "spec": spec,
        "_spec_hooks": hooks,
        "__doc__": getattr(spec, "__doc__", None),
    }

    for fn, method_name, always in _INSTANCE_BINDINGS:
        if always:
            namespace[method_name] = fn
        else:
            trigger = _PRESENCE_TRIGGER.get(method_name, method_name)
            if trigger in hooks:
                namespace[method_name] = fn

    # base_url 是 property，不能走 _INSTANCE_BINDINGS（那里挂的是普通函数）。
    # 仅在声明时注入，未声明就继承 CustomProvider 原 property。
    if "base_url" in hooks:
        namespace["base_url"] = property(_adapter_base_url)

    # 类级开关：spec 上写了就复制到适配器类。
    for flag in CLASS_FLAGS:
        if flag in vars(spec):
            namespace[flag] = getattr(spec, flag)

    # 账号字段挂实例属性：有字段要挂才加 __init__，否则保持 CustomProvider 的（行为不变）。
    account_fields = _account_field_names(spec)
    if account_fields:
        namespace["ACCOUNT_FIELDS"] = account_fields
        namespace["__init__"] = _make_adapter_init(account_fields)

    # classmethod 型账号钩子：仅在 spec 声明时注入（admin 探测 account_schema/回调授权）。
    if "account_schema" in hooks:
        namespace["account_schema"] = classmethod(_make_account_schema(spec))
    if "build_auth_start" in hooks:
        namespace["build_account_auth_start"] = classmethod(_make_build_auth_start(spec))
    if "handle_auth_callback" in hooks:
        namespace["handle_account_auth_callback"] = classmethod(_make_handle_auth_callback(spec))

    adapter = type(f"CodeChannel_{name}", (CustomProvider,), namespace)
    return adapter


def _make_account_schema(spec: type):
    fn = _spec_plain_callable(spec, "account_schema")

    def account_schema(cls) -> dict:
        schema = fn() if fn is not None else {}
        if not isinstance(schema, dict):
            schema = {}
        schema.setdefault("provider_name", cls.PROVIDER_NAME)
        return schema

    return account_schema


def _make_build_auth_start(spec: type):
    fn = _spec_plain_callable(spec, "build_auth_start")

    async def build_account_auth_start(cls, provider_name, account, redirect_uri, state, cfg=None, auth_context=None) -> dict:
        return await _call_with_optional_context(
            fn, (provider_name, account, redirect_uri, state, cfg), auth_context)

    return build_account_auth_start


def _make_handle_auth_callback(spec: type):
    fn = _spec_plain_callable(spec, "handle_auth_callback")

    async def handle_account_auth_callback(cls, provider_name, params, state_data, cfg=None) -> dict:
        return await _maybe_await(fn(provider_name, params, state_data, cfg))

    return handle_account_auth_callback


