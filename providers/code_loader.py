"""代码渠道加载器：把管理端贴的 Python 源码 exec 成一个渠道适配器类。

信任模型
--------
贴进代码渠道的源码以**本服务进程的权限**执行，等同于部署一个 ``providers/xxx.py``。
仅授权管理员可编辑渠道配置；任何能创建/编辑代码渠道的管理员 = 能执行任意代码。
不做重沙箱、不做进程隔离（已与需求确认为 raw-Python 信任级）。

写法（v2：spec 类）
--------
作者贴一个**普通类**（不继承任何框架基类），方法用 ``@staticmethod`` 声明、第一个参数
``p`` 就是渠道实例。这个类只写想改的钩子；没写的一切回落到 ``CustomProvider`` 的配置驱动
实现（base_url / 协议 / 超时都读渠道配置）。加载器把它交给
``code_spec.build_adapter_class`` 包成一个 ``CustomProvider`` 子类适配器。完整钩子表见
``docs/providers/code-channel.md`` 与 ``providers/code_spec.HOOK_ALIASES``。

**不再支持贴 BaseProvider / CustomProvider 子类**：扫到误继承基类的类会明确报错并给出迁移
指引，避免两套写法并存、文档口径分裂。

为什么不收窄 __builtins__
--------
贴的代码可能带 ``from Crypto.Cipher import AES`` 这类真实 import。``import`` 语句走 Python
import 系统、调用 ``__builtins__.__import__``，一旦收窄 __import__，所有 import 全废。所以
builtins 不收窄，作者可以 ``import`` 任何已装包（含 aiohttp——但正常写 spec 不需要，发请求
统一走 ``p.send_sse_request`` / ``p._make_session``，出站代理、连接复用、请求留痕都由框架接管，
自己起 aiohttp 会绕开这一切）。

注入的 helper 见 ``_build_namespace``：只放写 spec 真正常用的东西——框架核心
（BaseProvider / CustomProvider / make_insecure_connector / JdbcClient / HTTPException）、
授权与回调类（Channel / ModelClientPool / AccountClient / ProviderLimitState /
get_proxy_manager）、``generate_completion_id``（建响应体的 id）、文本转工具调用
（``parse_tool_calls_from_content`` / ``generate_tools_prompt`` / ``render_tool_call_json``
/ ``render_tool_result_json`` / ``render_tool_call_xml`` / ``render_tool_result_xml``——
prompt 注入型渠道必备，上游不认 function calling 时把工具说明塞进 system、解析模型吐的
文本工具调用、历史轮反向序列化）、``logger`` 与常用 stdlib。
**usage 不再交给作者拼**：框架会按内容自动估算兜底，spec 只在自己拿到上游真实 usage 时
``yield {"usage": {...}}`` 覆盖即可，因此不注入 estimate_usage / normalize_usage / merge_usage，
也不注入 aiohttp。完整说明见 ``docs/providers/code-channel.md``。
"""
from __future__ import annotations

import hashlib

from providers.base import BaseProvider, make_insecure_connector
from providers.custom import CustomProvider
from providers.code_spec import (
    CodeSpecError,
    HOOK_ALIASES,
    build_adapter_class,
)


# ---- 注入到 exec namespace 的 helper（作者少写 import，但不妨碍自己 import）----
def _build_namespace() -> dict:
    import asyncio
    import base64
    import hashlib as _hashlib
    import json
    import re
    import secrets
    import string
    import time as _time
    import traceback
    import uuid
    from datetime import datetime, timezone
    from typing import AsyncGenerator
    from urllib.parse import urlparse, parse_qs

    from fastapi import HTTPException
    from loguru import logger

    from rd import JdbcClient
    from message_utils import generate_completion_id
    # 渠道 / 账号 / 限流 / 出站代理：贴的 provider 要读渠道配置、走统一出口、
    # 或自管登录态时直接用这些类（语义与框架一致，避免各写各的）。
    from channel import Channel
    from rate_limiter import ModelClientPool, AccountClient
    from limits import ProviderLimitState
    from providers.proxy_manager import get_proxy_manager
    # 文本转工具调用（prompt 注入型渠道必备）：上游不认 OpenAI function calling 时，
    # 用 generate_tools_prompt 把工具说明塞进 system，模型按约定吐 XML/JSON 文本，
    # 框架在流式路径自动解析（请求带 tools 时缓冲切块），非流式用 parse_tool_calls
    # 兜底；历史轮的 tool_calls / tool 结果用 render_* 系列反向序列化回 content。
    # （p.build_tools_prompt / p.parse_tool_calls 也已挂在实例上，模块函数供直接调用。）
    from tool_utils import (
        parse_tool_calls_from_content,
        generate_tools_prompt,
        render_tool_call_json,
        render_tool_result_json,
        render_tool_call_xml,
        render_tool_result_xml,
    )

    return {
        # 框架核心
        "BaseProvider": BaseProvider,
        "CustomProvider": CustomProvider,
        "make_insecure_connector": make_insecure_connector,
        "JdbcClient": JdbcClient,
        "HTTPException": HTTPException,
        # 渠道 / 账号 / 限流 / 出口（授权类与回调类）
        "Channel": Channel,                 # 渠道领域对象；self._channel 即此类型
        "ModelClientPool": ModelClientPool, # 渠道池单例（读兄弟渠道、查路由等）
        "AccountClient": AccountClient,     # 账号客户端（冻结/冷却/auth 状态）
        "ProviderLimitState": ProviderLimitState,  # 每账号限额状态机
        "get_proxy_manager": get_proxy_manager,    # 统一出站代理出口（鉴权/分流）
        # 建响应体用的 completion id 生成器（chatcmpl-xxx）；建 OpenAI 响应体时当 id 传入。
        "generate_completion_id": generate_completion_id,
        # 文本转工具调用：见上方 import 注释
        "parse_tool_calls_from_content": parse_tool_calls_from_content,
        "generate_tools_prompt": generate_tools_prompt,
        "render_tool_call_json": render_tool_call_json,
        "render_tool_result_json": render_tool_result_json,
        "render_tool_call_xml": render_tool_call_xml,
        "render_tool_result_xml": render_tool_result_xml,
        # 常用依赖
        "logger": logger,
        # 常用 stdlib
        "asyncio": asyncio,
        "json": json,
        "re": re,
        "time": _time,
        "uuid": uuid,
        "hashlib": _hashlib,
        "base64": base64,
        "secrets": secrets,
        "string": string,
        "traceback": traceback,
        "datetime": datetime,
        "timezone": timezone,
        "AsyncGenerator": AsyncGenerator,
        "urlparse": urlparse,
        "parse_qs": parse_qs,
    }


class CodeChannelError(Exception):
    """代码渠道加载失败（语法错 / 没有 spec 类 / 多个 spec 类 / 误继承基类 / 无钩子）。"""


# (name -> (sha1, cls)) 缓存：源码没变就不重 exec。
_CLASS_CACHE: dict[str, tuple[str, type]] = {}


def _has_public_callable(spec: type) -> bool:
    """类自身（不含 object 继承）是否带任何公开可调用属性——用于判断「作者想写 spec 类」。

    有公开方法但全是拼错的钩子时仍算候选，交 ``validate_spec`` 报「未定义可识别钩子」
    并把未识别方法名列进报文（防静默不生效）。
    """
    for _name, raw in vars(spec).items():
        if _name.startswith("_"):
            continue
        target = raw.__func__ if isinstance(raw, (staticmethod, classmethod)) else raw
        if callable(target):
            return True
    return False


def _find_spec_class(namespace: dict) -> type | None:
    """在 exec 后的 namespace 里找作者的 spec 类。

    spec 类 = 在本源码里定义（``__module__`` == exec 的模块名）、未继承 BaseProvider、
    且带至少一个公开可调用方法（可识别钩子或疑似拼错的钩子）的普通类。排除注入的 helper
    类（BaseProvider/CustomProvider/Channel/… ——它们 ``__module__`` 不是本源码），也排除
    误继承 BaseProvider 的类（交上层报迁移指引）。

    多个候选抛 CodeChannelError；零个返回 None（候选合法性 / 钩子校验交 validate_spec）。
    """
    module_name = namespace.get("__name__")
    candidates = []
    inherited_base = []
    for value in namespace.values():
        if not isinstance(value, type):
            continue
        # 只认在本贴源码里定义的类（注入的 helper 类 __module__ 不同）。
        if getattr(value, "__module__", None) != module_name:
            continue
        if issubclass(value, BaseProvider):
            inherited_base.append(value)
            continue
        if _has_public_callable(value):
            candidates.append(value)
    if not candidates:
        # 全是误继承基类的类 -> 抛出可读迁移指引（区别于「什么类都没写」）。
        if inherited_base:
            names = ", ".join(c.__name__ for c in inherited_base)
            raise CodeChannelError(
                f"代码渠道不再继承框架基类（发现 {names} 继承了 BaseProvider/CustomProvider）。"
                f"请写一个普通类，方法用 @staticmethod、第一个参数为渠道实例 p，"
                f"框架能力通过 p.xxx 调用。合法钩子：{', '.join(sorted(HOOK_ALIASES))}"
            )
        return None
    if len(candidates) > 1:
        names = ", ".join(c.__name__ for c in candidates)
        raise CodeChannelError(
            f"代码渠道只能定义一个 spec 类，发现 {len(candidates)} 个：{names}"
        )
    return candidates[0]


def load_code_provider_class(name: str, source: str) -> type:
    """exec 贴的源码，把作者的 spec 类包成 CustomProvider 子类适配器返回。

    - 作者写一个**普通类**（不继承基类），方法用 ``@staticmethod``、首参 ``p`` = 渠道实例，
      只写想改的钩子；没写的回落 CustomProvider 配置驱动实现。
    - 加载器扫出唯一的 spec 类，交 ``code_spec.build_adapter_class`` 包成适配器。适配器
      ``PROVIDER_NAME`` 被强制为渠道 id（对齐 ``redis_prefix`` / 日志），作者无需（也不应）
      自己写。
    - 误继承 BaseProvider/CustomProvider、无 spec 类、无可识别钩子都抛 CodeChannelError
      并给迁移/修正指引。
    - 按 ``(name, sha1(source))`` 缓存；源码没变直接复用，变了重 exec、register 覆盖。
    """
    if not source or not source.strip():
        raise CodeChannelError(f"代码渠道 {name!r} 的 code 字段为空")

    source_hash = hashlib.sha1(source.encode("utf-8")).hexdigest()
    cached = _CLASS_CACHE.get(name)
    if cached and cached[0] == source_hash:
        return cached[1]

    namespace = _build_namespace()
    namespace["__name__"] = f"code_provider_{name}"
    # exec 默认会注入完整 __builtins__（含 __import__），让贴的代码照常 import 任何已装包。

    try:
        exec(compile(source, f"<code-channel:{name}>", "exec"), namespace)
    except SyntaxError as exc:
        raise CodeChannelError(
            f"代码渠道 {name!r} 语法错误 (行 {exc.lineno}): {exc.msg}"
        ) from exc
    except Exception as exc:  # noqa: BLE001
        raise CodeChannelError(
            f"代码渠道 {name!r} 执行失败: {type(exc).__name__}: {exc}"
        ) from exc

    spec = _find_spec_class(namespace)
    if spec is None:
        raise CodeChannelError(
            f"代码渠道 {name!r} 未定义任何 spec 类（带可识别钩子的普通类）。"
            f"合法钩子：{', '.join(sorted(HOOK_ALIASES))}"
        )

    try:
        cls = build_adapter_class(name, spec)
    except CodeSpecError as exc:
        raise CodeChannelError(str(exc)) from exc

    _CLASS_CACHE[name] = (source_hash, cls)
    return cls


def invalidate_cache(name: str | None = None) -> None:
    """清除缓存（调试/测试用）。name=None 清全部。"""
    if name is None:
        _CLASS_CACHE.clear()
    else:
        _CLASS_CACHE.pop(name, None)
