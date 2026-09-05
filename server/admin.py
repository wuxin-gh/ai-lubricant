"""配置管理 API - 热更新 + 认证 + 账号状态"""
import asyncio
import hashlib
import inspect
import json
import os
import random
import re
import secrets
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from functools import wraps
from typing import Any, Literal, Optional
from urllib.parse import parse_qs, quote, urlsplit, urlparse

from fastapi import APIRouter, Body, HTTPException, Header, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from loguru import logger

import config
import runtime_sync
from channel import (
    compile_model_id_rewrite_rules,
    default_config_with_chat_protocols,
    is_model_id_template_entry,
    normalize_model_id_rewrite_rules,
    normalize_upstream_stream,
    set_model_rule_template_cache,
)
from limit_policy_store import (
    clear_policy_cache,
    get_default_freeze_policy,
    get_effective_provider_policy,
    get_effective_provider_policy_sync,
    policy_fields_from_rate_limit,
    validate_cooldown_policy,
    validate_freeze_policy,
)
from providers import CustomProvider, EdgeOneAIProvider
from providers.base import BaseProvider
from providers.cloudflare import CloudflareProvider
# 账号测试模板与出站 Header 模板共用同一套变量实现（变量集只在 custom.py 维护一份）。
from providers.custom import build_request_template_variables, render_account_template_paths
from proxy_utils import (
    canonical_proxy,
    canonical_url_prefix,
    find_proxy as _find_proxy,
    proxy_effective_url as _proxy_effective_url,
    proxy_mode,
    proxy_prefix_base,
    resolve_account_proxy,
    resolve_account_proxy_config_id,
    resolve_account_url_prefix,
    runtime_accounts,
)
from rate_limiter import ModelClientPool, AccountClient, AccountState, ProviderPool, NoAvailableAccountError
from limits.backend import RedisLimitBackend
from db import PostgresClient
from rd import JdbcClient
import usage_utils
from usage_utils import merge_usage, normalize_usage

from project_paths import deleted_backups_dir, repo_root

# 主配置真相源是 DB 的 app_config['main']（经 CONFIG_STORE 读写），CONFIG_FILE
# 只作为分支哨兵与 deleted_backups 的定位基准。走 project_paths 解析到仓库根，
# 避免本文件迁入 server/ 后 .parent 指错、把备份写到 server/ 下。
CONFIG_FILE = repo_root() / "config.json"
CONFIG_STORE = config.CONFIG_STORE

router = APIRouter(prefix="/admin")

# 无前缀路由：上游只回调 127.0.0.1:{port}/oauth/callback 的桌面客户端型 OAuth（CodeArts
# 那类）需要一个根级落点，/admin 前缀装不下。由 main.py 在 SPA catch-all 之前挂载。
loopback_router = APIRouter()

# 非激活方案在内存快照里派生组条目时用的名字分隔符（与 model_catalog.SCHEME_GROUP_SEP 一致）。
# 组名/别名/方案名不得含此串，否则会与派生条目撞名——校验层直接拒绝。
_SCHEME_GROUP_SEP = "#scheme:"

# admin 登录 session 使用 Redis 缓存，key: admin:session:{token}
ADMIN_SESSION_PREFIX = "admin:session"
ADMIN_SESSION_TTL = 86400 * 7  # 7 天
# 账号授权任务默认存活时长（callback 型未显式带 expires_at 时用）。device_code 型的
# expires_at 由 provider 的 expires_in 决定。事实源是 Postgres account_auth_states 表。
ACCOUNT_AUTH_STATE_TTL = 900
_admin_sessions_mem: dict[str, dict] = {}

# 当前请求的 C 端 session cookie（由 main.py 的 middleware 填充）。
# _require_admin 从这里读 cookie 做「C 端 session + role==admin」主鉴权，
# 无需改动 141 处调用点的签名（它们仍只传 Authorization header 作应急兜底）。
from contextvars import ContextVar

_current_user_cookie: ContextVar[str | None] = ContextVar("_current_user_cookie", default=None)


def set_current_user_cookie(cookie: str | None) -> None:
    """供 main.py middleware 调用：把当前请求的 C 端 session cookie 存入 contextvar。"""
    _current_user_cookie.set(cookie or None)


def _get_redis():
    return JdbcClient.redis


async def _set_admin_session(token: str, expires_at: float):
    """保存 admin session 到 Redis"""
    _admin_sessions_mem[token] = {"expires_at": expires_at}
    redis = _get_redis()
    if redis is not None:
        key = f"{ADMIN_SESSION_PREFIX}:{token}"
        # 使用 RedisJdbc.set，确保走 get_key() 前缀逻辑；setex 未被 RedisJdbc 包装。
        await redis.set(key, str(int(expires_at)), ex=ADMIN_SESSION_TTL)


async def _get_admin_session(token: str) -> dict | None:
    """从 Redis 读取 admin session"""
    redis = _get_redis()
    if redis is not None:
        key = f"{ADMIN_SESSION_PREFIX}:{token}"
        val = await redis.get(key)
        if val:
            expires_at = int(val.decode() if isinstance(val, bytes) else val)
            if time.time() < expires_at:
                return {"expires_at": expires_at}
    session = _admin_sessions_mem.get(token)
    if session and time.time() < session["expires_at"]:
        return session
    _admin_sessions_mem.pop(token, None)
    return None


async def _del_admin_session(token: str):
    """从 Redis 删除 admin session"""
    _admin_sessions_mem.pop(token, None)
    redis = _get_redis()
    if redis is not None:
        key = f"{ADMIN_SESSION_PREFIX}:{token}"
        await redis.delete(key)


async def _set_account_auth_state(state: str, data: dict):
    # 若调用方已带 expires_at（device_code 授权任务），沿用它，使 TTL 不被每次轮询刷新；
    # 否则按默认 TTL 计算一个新的过期点（callback 型授权）。
    data = data or {}
    expires_at = float(data.get("expires_at") or 0) or (time.time() + ACCOUNT_AUTH_STATE_TTL)
    payload = {**data, "expires_at": expires_at}
    await PostgresClient.upsert_account_auth_state(state, payload)


async def _get_account_auth_state(state: str) -> dict | None:
    return await PostgresClient.get_account_auth_state(state)


async def _del_account_auth_state(state: str):
    await PostgresClient.delete_account_auth_state(state)


_AUTH_SECRET_KEYS = {
    "refresh_token", "access_token", "github_token", "copilot_token",
    "password", "id_token", "user_info",
}


def _mask_auth_dict(data: dict) -> dict:
    """脱敏授权字典：secret 字段只保留首尾各 4 位，其余打码。供日志使用。"""
    if not isinstance(data, dict):
        return {}
    out: dict = {}
    for k, v in data.items():
        if k in _AUTH_SECRET_KEYS and isinstance(v, str) and v:
            out[k] = (v[:4] + "****" + v[-4:]) if len(v) > 8 else "****"
        elif isinstance(v, dict):
            out[k] = _mask_auth_dict(v)
        else:
            out[k] = v
    return out


# ==================== Device Code 授权中央扫描器 ====================
# 单一常驻 task，轮询所有 device_code 类型的授权任务（Postgres 为唯一事实源）。
# 重启后从 account_auth_states 表恢复 pending 任务继续轮询；expires_at 过期 = 自动取消。

_auth_scanner_task: "asyncio.Task | None" = None
# 扫描器复用的 provider 实例（按渠道名缓存，poll_device_flow 只读 proxy/user_agent）
_auth_scanner_instances: dict[str, Any] = {}


async def _list_pending_device_code_tasks() -> list[tuple[str, dict]]:
    """枚举 pending 的 device_code 授权任务 → ``[(state, record)]``。

    走 ``account_auth_states`` 的 (task_type, status, expires_at) 复合索引，成本只跟
    进行中的授权数（通常 0~几个）相关。历史实现用 Redis ``SCAN MATCH`` 全库遍历，开销
    随 keyspace 线性恶化，并发一高就撞 0.5s 硬截止刷 device_auth_scan 降级告警。
    """
    try:
        return await PostgresClient.list_pending_device_code_states()
    except Exception as e:
        logger.error(f"[auth-scanner] list pending tasks failed: {e}")
        return []


def _build_scanner_instance(provider_name: str, cfg: dict, account: dict | None = None, cache_key: str | None = None):
    """构造（或复用）用于轮询的 provider 实例。poll_device_flow/token exchange 必须跟账号代理走。"""
    key = cache_key or provider_name
    inst = _auth_scanner_instances.get(key)
    if inst is not None:
        return inst
    provider_class = _get_provider_class(provider_name, cfg)
    runtime_acc = dict(account or {})
    if not runtime_acc.get("username"):
        runtime_acc["username"] = f"{provider_name}-auth"
    runtime_acc["proxy"] = _resolve_account_proxy(runtime_acc)
    runtime_acc["url_prefix"] = _resolve_account_url_prefix(runtime_acc)
    extra = _provider_extra(provider_name, cfg)
    extra.update({k: v for k, v in runtime_acc.items() if k not in ("username", "password", "proxy", "url_prefix", "proxy_id", "switch")})
    inst = provider_class(
        username=runtime_acc.get("username"),
        password=runtime_acc.get("password", ""),
        proxy=runtime_acc.get("proxy"),
        url_prefix=runtime_acc.get("url_prefix"),
        **extra,
    )
    _auth_scanner_instances[key] = inst
    return inst


def _accepts_extra_arg(fn, base_count: int) -> bool:
    """Whether a bound callable takes one more positional arg beyond ``base_count``.

    ``poll_device_flow`` / ``begin_device_flow`` gained a trailing optional
    ``auth_context``. Built-in providers and older code-channel adapters still use the
    original signature, so probe before passing it — never call-and-retry on TypeError,
    which would replay a hook that raised TypeError from its own body.
    """
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    slots = 0
    for param in sig.parameters.values():
        if param.kind is inspect.Parameter.VAR_POSITIONAL:
            return True
        if param.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
            slots += 1
    return slots > base_count


async def _poll_one_auth_task(state: str, record: dict):
    """对单个 device_code 任务执行一次轮询，结果写回 state_data。"""
    provider_name = record.get("provider", "")
    poll_params = record.get("poll_params") or {}
    now = time.time()

    # 过期检查（双保险，TTL 也会删 key）
    if now >= float(record.get("expires_at") or 0):
        record["status"] = "expired"
        await _set_account_auth_state(state, record)
        _auth_scanner_instances.pop(f"{provider_name}:{state}", None)
        logger.info(f"[auth-scanner] task expired provider={provider_name} state={state}")
        return

    try:
        cfg = await _read_provider_config(provider_name)
        inst = _build_scanner_instance(provider_name, cfg, record.get("account") or {}, cache_key=f"{provider_name}:{state}")
        auth_context = record.get("auth_context")
        poll = inst.poll_device_flow
        coro = (poll(poll_params, auth_context)
                if auth_context is not None and _accepts_extra_arg(poll, 1)
                else poll(poll_params))
        result = await asyncio.wait_for(coro, timeout=30)
    except asyncio.TimeoutError:
        logger.warning(f"[auth-scanner] poll timeout provider={provider_name} state={state}")
        record["next_poll_at"] = now + max(int(record.get("interval") or 5), 1)
        await _set_account_auth_state(state, record)
        return
    except Exception as e:
        logger.error(f"[auth-scanner] poll error provider={provider_name} state={state}: {e}")
        record["next_poll_at"] = now + max(int(record.get("interval") or 5), 1)
        await _set_account_auth_state(state, record)
        return

    status = (result or {}).get("status", "pending")
    if status == "pending":
        record["next_poll_at"] = now + max(int(record.get("interval") or 5), 1)
        await _set_account_auth_state(state, record)
        return

    if status == "authorized":
        # 落库在扫描器内完成（_finalize_authorized_auth），不再依赖前端继续轮询
        # /auth/status 才写账号；authorized 之后也不再有 GET 带写副作用的路径。
        record["status"] = "authorized"
        record["account_data"] = result.get("account_data") or {}
        await _set_account_auth_state(state, record)
        logger.info(
            f"[auth-scanner] task authorized provider={provider_name} state={state} "
            f"account_data={_mask_auth_dict(record['account_data'])}"
        )
        try:
            await _finalize_authorized_auth(provider_name, state, record)
        except Exception as e:  # noqa: BLE001 - 落库失败不吞扫描异常链，交状态查询兜底重试
            logger.error(f"[auth-scanner] finalize authorized failed provider={provider_name} state={state}: {e}")
    else:  # expired / error
        record["status"] = status
        record["error"] = result.get("error", "")
        await _set_account_auth_state(state, record)
        logger.info(f"[auth-scanner] task {status} provider={provider_name} state={state} error={record.get('error')!r}")
    _auth_scanner_instances.pop(f"{provider_name}:{state}", None)


async def _auth_scanner_loop():
    """常驻扫描循环：每秒检查到期的 device_code 任务并轮询，收尾清扫过期行。"""
    logger.info("[auth-scanner] loop started")
    _sweep_tick = 0
    while True:
        try:
            await asyncio.sleep(1)
            now = time.time()
            tasks = await _list_pending_device_code_tasks()
            due = [(s, r) for s, r in tasks if now >= float(r.get("next_poll_at") or 0)]
            if due:
                await asyncio.gather(*[_poll_one_auth_task(s, r) for s, r in due], return_exceptions=True)
            # 每轮扫完清扫过期任务（含 callback 型）。任务量小，但每秒 DELETE 无谓占连接，
            # 故限频到每 30 轮（约 30s）一次——授权 TTL 15 分钟，延迟清理完全可接受。
            _sweep_tick += 1
            if _sweep_tick >= 30:
                _sweep_tick = 0
                try:
                    removed = await PostgresClient.delete_expired_account_auth_states()
                    if removed:
                        logger.info(f"[auth-scanner] swept {removed} expired auth states")
                except Exception as e:
                    logger.error(f"[auth-scanner] sweep expired failed: {e}")
        except asyncio.CancelledError:
            logger.info("[auth-scanner] loop cancelled")
            raise
        except Exception as e:
            logger.error(f"[auth-scanner] loop error: {e}")
            await asyncio.sleep(5)


async def start_auth_scanner():
    """启动 device_code 授权中央扫描器（lifespan 调用）。"""
    global _auth_scanner_task
    if _auth_scanner_task and not _auth_scanner_task.done():
        return
    _auth_scanner_task = asyncio.create_task(_auth_scanner_loop())


async def stop_auth_scanner():
    """停止扫描器（lifespan 调用）。"""
    global _auth_scanner_task
    if _auth_scanner_task and not _auth_scanner_task.done():
        _auth_scanner_task.cancel()
        try:
            await _auth_scanner_task
        except asyncio.CancelledError:
            pass
    _auth_scanner_task = None
    _auth_scanner_instances.clear()


# ==================== 每日定时刷新授权 ====================
# 每天本地 10:00 对所有「声明 SCHEDULED_REFRESH=True 且自己覆盖了 refresh_account_auth」的
# 渠道账号调 refresh_account_auth，主动刷新 access_token 并写回配置。
# 历史内置 OAuth 渠道（copilot/codebuddy/atomcode/eaichat）已迁移为代码渠道 spec，
# 它们在 spec 类上声明 SCHEDULED_REFRESH=True + refresh_auth 钩子即沿用本机制；
# 检查登录态（不刷新）由 check_auth 在健康检测/定期 check_account 中完成，与本定时刷新职责分离。
_daily_auth_refresh_task: asyncio.Task | None = None


def _provider_supports_scheduled_refresh(provider_class) -> bool:
    """渠道是否纳入每日定时刷新。

    必须同时满足：显式声明 ``SCHEDULED_REFRESH=True``，且自己覆盖了 ``refresh_account_auth``
    （≠ BaseProvider 的 501 桩）。后者排除纯 CustomProvider / 未声明 refresh_auth 钩子的代码
    渠道——它们没有可刷新的令牌，调下去只会吃 501。
    """
    if not getattr(provider_class, "SCHEDULED_REFRESH", False):
        return False
    return provider_class.refresh_account_auth is not BaseProvider.refresh_account_auth


def _seconds_until_next_10am() -> float:
    """距下一个本地 10:00 的秒数。"""
    now = datetime.now()
    target = now.replace(hour=10, minute=0, second=0, microsecond=0)
    if now >= target:
        # 今天 10:00 已过，算到明天 10:00
        import calendar
        from datetime import timedelta
        target = target + timedelta(days=1)
    return (target - now).total_seconds()


def _merge_saved_account_for_auth(account: dict, cfg: dict) -> dict:
    """授权启动时把已保存账号字段（尤其 proxy/proxy_id）合并进请求账号。"""
    item = dict(account or {})
    username = (item.get("username") or "").strip()
    if not username:
        return item
    for saved in cfg.get("accounts", []) or []:
        if not isinstance(saved, dict):
            continue
        if (saved.get("username") or "").strip() != username:
            continue
        merged = dict(saved)
        merged.update({k: v for k, v in item.items() if v is not None and v != ""})
        # 代理跟账号走：请求未带 proxy/proxy_id 时，保留已保存账号的代理引用
        if not item.get("proxy") and not item.get("proxy_id"):
            if saved.get("proxy_id"):
                merged["proxy_id"] = saved.get("proxy_id")
            elif saved.get("proxy"):
                merged["proxy"] = saved.get("proxy")
        return merged
    return item


async def _refresh_one_account_auth(name: str, cfg: dict, account: dict, index: int) -> bool:
    """刷新单个账号 token 并写回配置 + reload 池。成功返回 True。

    泛化自原 _refresh_one_atomcode_account：凡 _provider_supports_scheduled_refresh 的渠道
    都走这条路，调 provider.refresh_account_auth 拿要回写的字段。
    """
    username = account.get("username") or ""
    try:
        provider_class = _get_provider_class(name, cfg)
        if not _provider_supports_scheduled_refresh(provider_class):
            return False
        pool = ModelClientPool.get_provider_pool(name)
        provider = None
        if pool:
            for client in pool.clients:
                if client.username == username:
                    provider = client.provider
                    break
        if provider is None:
            runtime_acc = dict(account)
            runtime_acc["proxy"] = _resolve_account_proxy(account)
            runtime_acc["url_prefix"] = _resolve_account_url_prefix(account)
            extra = _provider_extra(name, cfg)
            extra.update({k: v for k, v in runtime_acc.items() if k not in ("username", "password", "proxy", "url_prefix", "proxy_id", "switch")})
            provider = provider_class(
                username=runtime_acc.get("username", username),
                password=runtime_acc.get("password", ""),
                proxy=runtime_acc.get("proxy"),
                url_prefix=runtime_acc.get("url_prefix"),
                **extra,
            )
        updates = await provider.refresh_account_auth(dict(account), cfg)
        if not isinstance(updates, dict):
            logger.warning(f"[daily-auth-refresh] {name}/{username} refresh returned non-dict")
            return False
        merged = dict(account)
        merged.update({k: v for k, v in updates.items() if v is not None})
        merged = _normalize_account_proxy_ref(merged, _read_proxies())
        cfg.setdefault("accounts", [])[index] = merged
        # 单账号窄写：只 UPSERT 该账号行 + 刷新内存 + 广播，不整份重写渠道。
        await _persist_account(name, merged)
        _reload_account_into_pool(name, merged, _provider_rpm(cfg), _provider_extra(name, cfg))
        logger.info(f"[daily-auth-refresh] {name}/{username} token 刷新成功")
        return True
    except Exception as e:
        logger.error(f"[daily-auth-refresh] {name}/{username} token 刷新失败: {e}")
        return False


async def _daily_auth_refresh_loop():
    logger.info("[daily-auth-refresh] 定时刷新任务启动")
    while True:
        try:
            await asyncio.sleep(_seconds_until_next_10am())
        except asyncio.CancelledError:
            break
        try:
            providers = await config.Config.get_providers()
            for name in list(providers.keys()):
                try:
                    cfg = await _read_provider_config(name)
                except Exception:
                    continue
                provider_class = _get_provider_class(name, cfg)
                if not _provider_supports_scheduled_refresh(provider_class):
                    continue
                accounts = cfg.get("accounts", []) or []
                for idx, acc in enumerate(accounts):
                    if not isinstance(acc, dict) or acc.get("switch") is False:
                        continue
                    # _refresh_one_account_auth 内部按账号窄写落库 + 广播，无需整份回写。
                    await _refresh_one_account_auth(name, cfg, acc, idx)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[daily-auth-refresh] loop error: {e}")


async def start_daily_auth_refresh():
    global _daily_auth_refresh_task
    if _daily_auth_refresh_task and not _daily_auth_refresh_task.done():
        return
    _daily_auth_refresh_task = asyncio.create_task(_daily_auth_refresh_loop())


async def stop_daily_auth_refresh():
    global _daily_auth_refresh_task
    if _daily_auth_refresh_task and not _daily_auth_refresh_task.done():
        _daily_auth_refresh_task.cancel()
        try:
            await _daily_auth_refresh_task
        except asyncio.CancelledError:
            pass
    _daily_auth_refresh_task = None


def _admin_stream_payloads(chunk) -> list[dict]:
    if isinstance(chunk, dict):
        return [chunk]
    if not isinstance(chunk, str):
        return []
    payloads = []
    for line in chunk.splitlines():
        line = line.strip()
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if not data or data == "[DONE]":
            continue
        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            payloads.append(payload)
    return payloads


def _admin_extract_response_model(payload) -> str:
    for data in _admin_stream_payloads(payload):
        model = data.get("model")
        if isinstance(model, str) and model.strip():
            return model.strip()
        response = data.get("response")
        if isinstance(response, dict) and isinstance(response.get("model"), str) and response["model"].strip():
            return response["model"].strip()
        message = data.get("message")
        if isinstance(message, dict) and isinstance(message.get("model"), str) and message["model"].strip():
            return message["model"].strip()
    return ""


def _admin_extract_usage_payload(data: dict) -> dict | None:
    if isinstance(data.get("usage"), dict):
        return data["usage"]
    response = data.get("response")
    if isinstance(response, dict) and isinstance(response.get("usage"), dict):
        return response["usage"]
    message = data.get("message")
    if isinstance(message, dict) and isinstance(message.get("usage"), dict):
        return message["usage"]
    for choice in data.get("choices") or []:
        if not isinstance(choice, dict):
            continue
        if isinstance(choice.get("usage"), dict):
            return choice["usage"]
        msg = choice.get("message")
        if isinstance(msg, dict) and isinstance(msg.get("usage"), dict):
            return msg["usage"]
    return None


UPSTREAM_ZERO_COMPLETION_MESSAGE = "upstream response usage reports zero completion tokens"


def _admin_validate_upstream_usage_payload(payload, had_content: bool = False) -> None:
    # 先做协议无关的内容判定：只要本 payload（含跨 chunk 累计的 had_content）见过真实输出，
    # 就不能因为上游 usage.completion_tokens=0 判失败——这只是不可靠的上游统计。
    payload_has_content = had_content
    zero_usage_payloads: list[dict] = []
    for data in _admin_stream_payloads(payload):
        if _has_first_token_content(data):
            payload_has_content = True
        usage = _admin_extract_usage_payload(data)
        if usage is None:
            continue
        if normalize_usage(usage)["completion_tokens"] <= 0:
            zero_usage_payloads.append(data)

    if not zero_usage_payloads:
        return

    # 有真实内容：删除零 usage，交给后续统计按内容估算兜底，语义对齐 main._validate_upstream_usage_payload。
    if payload_has_content:
        for data in zero_usage_payloads:
            if "usage" in data:
                del data["usage"]
        return

    raise HTTPException(status_code=502, detail=UPSTREAM_ZERO_COMPLETION_MESSAGE)


def _has_first_token_content(chunk):
    if chunk is None:
        return False
    if isinstance(chunk, str):
        lines = chunk.splitlines()
        looks_like_sse = any(line.lstrip().startswith(("data:", "event:", "id:", "retry:", ":")) for line in lines)
        if not looks_like_sse:
            return bool(chunk.strip())
        for line in lines:
            line = line.strip()
            if not line or line.startswith(":"):
                continue
            if not line.startswith("data:"):
                continue
            line = line[5:].strip()
            if not line or line == "[DONE]":
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                return True
            if _has_first_token_content(payload):
                return True
        return False
    if isinstance(chunk, dict):
        for choice in chunk.get("choices", []) or []:
            delta = choice.get("delta") or {}
            if isinstance(delta, str):
                if delta.strip():
                    return True
            elif isinstance(delta, dict):
                if delta.get("content") or delta.get("text") or delta.get("reasoning_content") or delta.get("reasoning") or delta.get("thinking") or delta.get("thinking_delta") or delta.get("tool_calls") or delta.get("function_call"):
                    return True
            message = choice.get("message") or {}
            if isinstance(message, str):
                if message.strip():
                    return True
            elif isinstance(message, dict) and (message.get("content") or message.get("tool_calls") or message.get("function_call")):
                return True
            text = choice.get("text")
            if text:
                return True
        if chunk.get("type") == "content_block_start":
            content_block = chunk.get("content_block") or {}
            if isinstance(content_block, dict) and content_block.get("type") in ("text", "thinking", "tool_use"):
                return True
        if chunk.get("type") == "content_block_delta":
            delta_obj = chunk.get("delta") or {}
            if isinstance(delta_obj, str):
                if delta_obj.strip():
                    return True
            elif isinstance(delta_obj, dict) and (delta_obj.get("text") or delta_obj.get("thinking") or delta_obj.get("partial_json") or delta_obj.get("input_json_delta")):
                return True
        if chunk.get("type") == "response.output_item.added":
            item = chunk.get("item") or {}
            if isinstance(item, dict) and item.get("type") in ("message", "function_call"):
                return True
        if chunk.get("type") == "text_delta" and chunk.get("text"):
            return True
        if chunk.get("type") == "thinking_delta" and chunk.get("thinking"):
            return True
        delta = chunk.get("delta") or {}
        if isinstance(delta, str):
            if delta.strip():
                return True
        elif isinstance(delta, dict) and (delta.get("content") or delta.get("text") or delta.get("reasoning_content") or delta.get("reasoning") or delta.get("thinking") or delta.get("thinking_delta") or delta.get("tool_calls") or delta.get("function_call")):
            return True
        if chunk.get("text") or chunk.get("content") or chunk.get("tool_calls") or chunk.get("function_call"):
            return True
        return False
    return bool(chunk)


def _read_json(path: Path) -> dict:
    if path == CONFIG_FILE:
        return CONFIG_STORE.read_main()
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


async def _read_json_async(path: Path) -> dict:
    if path == CONFIG_FILE and hasattr(CONFIG_STORE, "read_main_async"):
        return await CONFIG_STORE.read_main_async()
    return _read_json(path)


def _write_json(path: Path, data: dict):
    if path == CONFIG_FILE:
        CONFIG_STORE.write_main(data)
    else:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    config.Config.reload()


async def _log_operation(token: str, action: str, target_type: str = None, target_name: str = None, old_data: dict = None, new_data: dict = None):
    try:
        # Single-password admin channel: there is no per-user identity, only a
        # session token. Storing a token prefix as the operator leaks a
        # credential fragment into a queryable table and reads as gibberish in
        # the audit UI, so persist a stable "admin" marker instead. The audit
        # page shows this as「管理员」regardless of which token was used.
        operator = "admin" if token else "system"
        await PostgresClient.insert_operation_log(operator, action, target_type, target_name, old_data, new_data)
    except Exception:
        pass


# 删除渠道/账号前把被删数据落成独立备份文件，防误删无法恢复。
# 与 config.json 同级的 deleted_backups/ 目录（已随 config.json 一并 gitignore）。
_DELETE_BACKUP_DIR = deleted_backups_dir()


def _write_delete_backup(kind: str, name: str, payload: dict) -> None:
    """把一次删除的完整快照写入 deleted_backups/{kind}-{name}-{ts}.json。

    kind: "provider" | "account"。写文件是同步 IO，但内容是内存快照、体积很小，
    不打上游、不查库；任何异常都吞掉，绝不阻断删除主流程。
    """
    try:
        _DELETE_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        safe = re.sub(r"[^A-Za-z0-9._-]", "_", f"{kind}-{name}")
        path = _DELETE_BACKUP_DIR / f"{safe}-{ts}.json"
        record = {
            "kind": kind,
            "name": name,
            "deleted_at": datetime.now().isoformat(timespec="seconds"),
            "data": payload,
        }
        path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(f"[admin] delete backup written kind={kind} name={name} path={path}")
    except Exception as exc:
        logger.warning(f"[admin] delete backup failed kind={kind} name={name}: {exc}")


async def _write_json_async(path: Path, data: dict):
    if path == CONFIG_FILE and hasattr(CONFIG_STORE, "write_main_async"):
        await CONFIG_STORE.write_main_async(data)
    else:
        _write_json(path, data)
        return
    await config.Config.reload_async()
    await runtime_sync.publish(runtime_sync.EVENT_MAIN_CONFIG, "__default__")


def _builtin_provider_default_config(name: str) -> dict | None:
    defaults = {
        "cloudflare": {
            "enabled": True,
            "type": "builtin",
            "remark": "Cloudflare Workers AI",
            "protocol": "openai",
            "base_url": "",
            "chat_path": "/chat/completions",
            "models_path": "",
            "supports_stream": True,
            "auto_update_models": True,
            "timeout": 120,
            "account_priority": 0,
            "account_weight": 1,
            "health_check": {"enabled": True, "interval_minutes": 30, "test_model": "@cf/meta/llama-3.1-8b-instruct"},
            "client_preset": "none",
            "rate_limit": {"rpm_per_account": 0, "tpm_per_account": 0, "tpm_per_model": 0, "concurrent_per_account": 0},
            "accounts": [],
        },
        "edgeone-ai": {
            "enabled": True,
            "type": "builtin",
            "remark": "EdgeOne AI",
            "protocol": "openai",
            "base_url": "",
            "chat_path": "/v1/chat/completions",
            "models_path": "",
            "supports_stream": True,
            "auto_update_models": False,
            "timeout": 120,
            "account_priority": 0,
            "account_weight": 1,
            "health_check": {"enabled": False, "interval_minutes": 30, "test_model": "deepseek-v4-flash"},
            "client_preset": "none",
            "rate_limit": {"rpm_per_account": 0, "tpm_per_account": 0, "tpm_per_model": 0, "concurrent_per_account": 0},
            "accounts": [],
        },
        "code": {
            "enabled": True,
            "type": "builtin",
            "remark": "自定义渠道",
            "protocol": "openai",
            "base_url": "",
            "chat_path": "/v1/chat/completions",
            "models_path": "",
            # 贴的 Provider 源码；空串=还没贴，_get_provider_class 会回落 CustomProvider。
            # 前端创建时从渠道目录 preset.code 带 Echo 样例进来。
            "code": "",
            "supports_stream": True,
            "auto_update_models": False,
            "timeout": 120,
            "client_preset": "none",
            "rate_limit": {"rpm_per_account": 0, "tpm_per_account": 0, "tpm_per_model": 0, "concurrent_per_account": 0},
            "accounts": [],
        },
    }
    cfg = defaults.get(name)
    # 单一事实来源：把内置默认里的顶层 protocol/chat_path/upstream_stream/client_preset
    # 折叠为 chat_protocols 行，返回给渠道创建流程直接写入 DB。
    return default_config_with_chat_protocols(dict(cfg)) if cfg else None


def _mask_secret(value: str) -> str:
    text = str(value or "")
    if len(text) <= 8:
        return "*" * len(text)
    return f"{text[:4]}...{text[-4:]}"


def _proxy_id(item: dict) -> str:
    raw = (item.get("id") or "").strip()
    if raw:
        return raw
    mode = str(item.get("mode") or "network").strip().lower()
    # node 模式 url 为空，改以 node_id 入 seed，避免同名不同节点的条目撞 id。
    disc = item.get("node_id", "") if mode == "node" else item.get("url", "")
    seed = f"{item.get('name', '')}|{mode}|{disc}"
    return "proxy_" + hashlib.sha1(seed.encode("utf-8")).hexdigest()[:12]


def _normalize_proxy_item(item: dict) -> dict:
    if not isinstance(item, dict):
        raise HTTPException(status_code=400, detail="代理配置必须是对象")
    name = (item.get("name") or "").strip()
    url = (item.get("url") or "").strip()
    mode = str(item.get("mode") or "network").strip().lower()
    if mode not in ("network", "url_prefix", "direct", "node"):
        raise HTTPException(status_code=400, detail=f"代理 mode 非法: {mode}")
    node_id = ""
    # direct=强制直连，无需 URL；network/url_prefix 必须有 name 和 url。
    if mode == "direct":
        if not name:
            raise HTTPException(status_code=400, detail="代理配置必须包含 name")
        url, username, password = "", "", ""
    elif mode == "node":
        # node=通过执行节点转发出站：无 URL，靠 node_id 选节点。
        if not name:
            raise HTTPException(status_code=400, detail="代理配置必须包含 name")
        node_id = (item.get("node_id") or "").strip()
        if not node_id:
            raise HTTPException(status_code=400, detail="node 模式必须选择转发节点")
        url, username, password = "", "", ""
    elif not name or not url:
        raise HTTPException(status_code=400, detail="代理配置必须包含 name 和 url")
    elif mode == "url_prefix":
        # 前缀基址必须是绝对 http(s) URL，去尾 /；拒绝 query/fragment 以免拼接语义含糊。
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.netloc:
            raise HTTPException(status_code=400, detail="url_prefix 模式要求绝对 http/https 前缀地址")
        if parts.query or parts.fragment:
            raise HTTPException(status_code=400, detail="url_prefix 前缀地址不允许带 query/fragment")
        url = url.rstrip("/")
        username, password = "", ""
    else:
        username = (item.get("username") or "").strip()
        password = item.get("password") or ""
    result = {
        "id": _proxy_id({**item, "name": name, "url": url, "mode": mode}),
        "name": name,
        "mode": mode,
        "url": url,
        "username": username,
        "password": password,
    }
    # node_id 仅 node 模式携带，避免污染其余模式的条目结构。
    if mode == "node":
        result["node_id"] = node_id
    return result


def _normalize_proxies(items) -> list[dict]:
    seen: set[str] = set()
    result: list[dict] = []
    for item in items or []:
        proxy = _normalize_proxy_item(item)
        if proxy["id"] in seen:
            raise HTTPException(status_code=400, detail=f"代理 ID 重复: {proxy['id']}")
        seen.add(proxy["id"])
        result.append(proxy)
    return result


def _read_proxies() -> list[dict]:
    cfg = _read_json(CONFIG_FILE)
    return _normalize_proxies(cfg.get("proxies", []))


def _resolve_account_proxy(acc: dict, proxies: list[dict] | None = None) -> str | None:
    proxies = proxies if proxies is not None else _read_proxies()
    return resolve_account_proxy(acc, proxies)


def _resolve_account_url_prefix(acc: dict, proxies: list[dict] | None = None) -> str | None:
    proxies = proxies if proxies is not None else _read_proxies()
    return resolve_account_url_prefix(acc, proxies)


def _resolve_account_proxy_config_id(acc: dict, proxies: list[dict] | None = None) -> str:
    proxies = proxies if proxies is not None else _read_proxies()
    return resolve_account_proxy_config_id(acc, proxies)


def _runtime_accounts(accounts: list[dict], proxies: list[dict] | None = None) -> list[dict]:
    proxies = proxies if proxies is not None else _read_proxies()
    return runtime_accounts(accounts, proxies)


def _normalize_account_proxy_ref(acc: dict, proxies: list[dict]) -> dict:
    item = dict(acc or {})
    ref = (item.get("proxy_id") or item.get("proxy") or "").strip()
    if ref:
        proxy = _find_proxy(proxies, ref)
        if proxy:
            item["proxy_id"] = proxy["id"]
        else:
            # 代理引用已失效（代理被删/改名，账号仍持旧 ref）。运行时路径 resolve_account_proxy
            # 对失效引用是容忍回退的，持久化路径也应对齐：不阻断保存，清空引用，账号回退到无代理。
            # 否则前端编辑该账号（表单预填的就是失效 ref）会永远 400，UI 改不掉。
            logger.warning(f"[admin] 账号 {item.get('username')} 代理引用失效已清空: {ref}")
            item.pop("proxy_id", None)
    else:
        item.pop("proxy_id", None)
    item.pop("proxy", None)
    return item


def _sanitize_proxy_for_log(proxy: dict) -> dict:
    item = dict(proxy or {})
    if item.get("password"):
        item["password"] = _mask_secret(item["password"])
    return item


def _hot_update_proxy_pool(proxies: list[dict] | None = None) -> dict:
    """代理池变更后原地刷新运行中账号的代理状态，不重建账号池。

    - 以最新规范化代理列表为准，遍历各 ProviderPool 的账号；
    - 从渠道持久化配置按 (provider, username) 找回账号的 proxy_id 引用，
      复用 _resolve_account_proxy / _resolve_account_url_prefix 重新生成有效值；
    - 仅原地赋值 client.provider.proxy / client.provider.url_prefix，保留限流窗口/认证/冷却/路由等运行态；
    - 代理被删除但账号仍保留 proxy_id 时清空运行时代理（与全新加载语义一致）；
    - 未受代理池管理的旧式字面量代理保持不变（resolve 返回原值，前后一致不改）。

    返回 {"changed": n, "accounts": [...]} 供日志/接口/测试断言。
    """
    proxies = proxies if proxies is not None else _read_proxies()
    try:
        providers_cfg = CONFIG_STORE.list_providers() or {}
    except Exception as exc:
        logger.warning(f"[admin] 代理池热更新读取渠道配置失败: {exc}")
        return {"changed": 0, "accounts": []}

    changed: list[str] = []
    for provider_name in ModelClientPool.get_provider_names():
        pool = ModelClientPool.get_provider_pool(provider_name)
        if pool is None:
            continue
        cfg = providers_cfg.get(provider_name) or {}
        accounts_by_user = {
            a.get("username"): a
            for a in cfg.get("accounts", [])
            if isinstance(a, dict) and a.get("username")
        }
        for client in pool.clients:
            account = accounts_by_user.get(client.username)
            if account is None:
                # 无持久化账号配置（例如未落库的兜底渠道）无法确定 proxy_id，跳过。
                continue
            new_proxy = _resolve_account_proxy(account, proxies)
            new_prefix = _resolve_account_url_prefix(account, proxies)
            new_proxy_config_id = _resolve_account_proxy_config_id(account, proxies)
            provider = client.provider
            old_proxy = canonical_proxy(getattr(provider, "proxy", None))
            old_prefix = canonical_url_prefix(getattr(provider, "url_prefix", None))
            old_proxy_config_id = getattr(provider, "proxy_config_id", "") or ""
            if (old_proxy != new_proxy or old_prefix != new_prefix
                    or old_proxy_config_id != new_proxy_config_id):
                provider.proxy = new_proxy
                provider.url_prefix = new_prefix
                provider.proxy_config_id = new_proxy_config_id
                changed.append(f"{provider_name}:{client.username}")

    if changed:
        logger.info(f"[admin] 代理池变更已同步 {len(changed)} 个运行中账号: {', '.join(changed)}")
    return {"changed": len(changed), "accounts": changed}


async def _publish_proxy_event() -> None:
    """广播代理池变更（其它实例据此重读主配置代理列表 + 原地刷新运行态代理）。

    不在事件中携带代理 URL / 用户名 / 密码，接收方一律回读权威配置。
    """
    try:
        await runtime_sync.publish(runtime_sync.EVENT_PROXY)
    except Exception:
        pass


def _find_duplicate_account_tokens(accounts: list[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = {}
    for account in accounts or []:
        token = (account.get("token") or "").strip()
        if not token:
            continue
        grouped.setdefault(token, []).append(account)

    duplicates = []
    for token, token_accounts in grouped.items():
        if len(token_accounts) < 2:
            continue
        duplicates.append({
            "token_masked": _mask_secret(token),
            "count": len(token_accounts),
            "accounts": [
                {
                    "username": account.get("username") or "",
                    "enabled": account.get("switch") is not False,
                }
                for account in token_accounts
            ],
        })
    return duplicates


def _provider_updated_at_ts(cfg: dict) -> float:
    raw = cfg.get("updated_at_ts", cfg.get("updated_at", 0))
    if isinstance(raw, datetime):
        return raw.timestamp()
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
        except ValueError:
            pass
    try:
        return float(raw or 0)
    except (TypeError, ValueError):
        return 0.0


async def _read_provider_config(name: str) -> dict:
    builtin_cfg = _builtin_provider_default_config(name)
    try:
        stored_cfg = await CONFIG_STORE.read_provider_async(name)
        if stored_cfg is not None:
            if builtin_cfg is not None:
                merged = dict(builtin_cfg)
                merged.update(stored_cfg)
                return merged
            return stored_cfg
    except (FileNotFoundError, AttributeError):
        pass
    raise HTTPException(status_code=404, detail=f"provider '{name}' 不存在")


async def _read_provider_base_config(name: str) -> dict:
    """只读渠道基础配置（不含 accounts），供基础配置接口使用。"""
    builtin_cfg = _builtin_provider_default_config(name)
    try:
        stored_cfg = await CONFIG_STORE.read_provider_base_async(name)
        if stored_cfg is not None:
            if builtin_cfg is not None:
                merged = dict(builtin_cfg)
                merged.update(stored_cfg)
                return merged
            return stored_cfg
    except (FileNotFoundError, AttributeError):
        pass
    raise HTTPException(status_code=404, detail=f"provider '{name}' 不存在")


async def _publish_channel_event(name: str) -> None:
    """广播渠道基础配置变更（其它实例据此从 DB 重读渠道行 + 更新 pool.channel）。"""
    try:
        await runtime_sync.publish(runtime_sync.EVENT_CHANNEL, name)
    except Exception:
        pass


async def _publish_account_event(name: str, username: str) -> None:
    """广播单账号变更（其它实例据此重读该账号 + 热加载入池）。"""
    try:
        await runtime_sync.publish(
            runtime_sync.EVENT_ACCOUNT,
            f"{name}:{username}",
            extra={"provider": name, "username": username},
        )
    except Exception:
        pass


async def _write_provider_config(name: str, data: dict):
    """整份写渠道配置（含 accounts 全量覆盖）+ 刷新本实例内存快照 + 广播 channel 事件。

    仅用于创建/整体重建场景（create_custom_provider / relay 建渠道 / 后台批量刷新）；
    账号增改删走 _persist_account/_remove_account_row 窄写，基础配置走 _write_provider_base。
    """
    await CONFIG_STORE.write_provider_async(name, data)
    await _publish_channel_event(name)


async def _write_provider_base(name: str, cfg: dict, *, drop_accounts: bool = False):
    """窄写渠道基础配置（只 UPSERT provider_configs 行，不碰 accounts 表）+ 广播 channel 事件。"""
    await CONFIG_STORE.write_provider_base_async(name, cfg, drop_accounts=drop_accounts)
    await _publish_channel_event(name)


async def _persist_account(name: str, account: dict):
    """窄写单账号行（UPSERT provider_accounts）+ 刷新内存快照 + 广播 account 事件。"""
    await CONFIG_STORE.upsert_provider_account_async(name, account)
    await _publish_account_event(name, account.get("username", ""))


async def _remove_account_row(name: str, username: str) -> bool:
    """窄写单账号删除（DELETE provider_accounts）+ 刷新内存快照 + 广播 account 事件。"""
    removed = await CONFIG_STORE.delete_provider_account_async(name, username)
    await _publish_account_event(name, username)
    return removed


def _provider_extra(name: str, cfg: dict) -> dict:
    provider_extra = {k: v for k, v in cfg.items() if k not in ("accounts", "rate_limit")}
    provider_extra.update({"provider_name": name, "rate_limit": cfg.get("rate_limit", {})})
    try:
        provider_extra["limit_policy"] = get_effective_provider_policy_sync(name)
    except Exception:
        pass
    return provider_extra


def _provider_rpm(cfg: dict) -> int:
    rate_limit = cfg.get("rate_limit", {})
    return rate_limit.get("rpm_per_account", rate_limit.get("requests_per_minute_per_account", 0))


async def _provider_config_exists_in_registry(name: str) -> bool:
    return name in (await config.Config.get_providers())


async def _provider_config_exists(name: str) -> bool:
    try:
        await _read_provider_config(name)
        return True
    except HTTPException:
        return False


def _route_entry_matches_provider(entry: dict, provider_name: str) -> bool:
    return (entry.get("provider") or "").strip() == provider_name


def _route_entry_matches_account(entry: dict, provider_name: str, username: str) -> bool:
    if not _route_entry_matches_provider(entry, provider_name):
        return False
    account = (entry.get("account") or entry.get("username") or "").strip()
    return account == username


def _remove_provider_from_routing_config(cfg: dict, provider_name: str) -> bool:
    changed = False
    routes = cfg.get("model_routes", {}).get("routes", [])
    new_routes = []
    for route in routes if isinstance(routes, list) else []:
        if not isinstance(route, dict):
            continue
        entries = route.get("entries")
        if isinstance(entries, list):
            new_entries = [e for e in entries if isinstance(e, dict) and not _route_entry_matches_provider(e, provider_name)]
            if len(new_entries) != len(entries):
                changed = True
            if not new_entries:
                changed = True
                continue
            route = {**route, "entries": new_entries}
        elif (route.get("provider") or "").strip() == provider_name:
            changed = True
            continue
        new_routes.append(route)
    if changed:
        cfg["model_routes"] = {"routes": new_routes}

    groups = cfg.get("model_groups", {}).get("groups", {})
    if isinstance(groups, dict):
        for group in groups.values():
            if not isinstance(group, dict):
                continue
            for fld in ("provider_whitelist", "provider_blacklist"):
                values = group.get(fld)
                if not isinstance(values, list):
                    continue
                filtered = [p for p in values if p != provider_name]
                if filtered != values:
                    group[fld] = filtered
                    changed = True
    return changed


def _remove_account_from_routing_config(cfg: dict, provider_name: str, username: str) -> bool:
    changed = False
    routes = cfg.get("model_routes", {}).get("routes", [])
    new_routes = []
    for route in routes if isinstance(routes, list) else []:
        if not isinstance(route, dict):
            continue
        entries = route.get("entries")
        if isinstance(entries, list):
            new_entries = [e for e in entries if isinstance(e, dict) and not _route_entry_matches_account(e, provider_name, username)]
            if len(new_entries) != len(entries):
                changed = True
            if not new_entries:
                changed = True
                continue
            route = {**route, "entries": new_entries}
        else:
            account = (route.get("account") or route.get("username") or "").strip()
            if (route.get("provider") or "").strip() == provider_name and account == username:
                changed = True
                continue
        new_routes.append(route)
    if changed:
        cfg["model_routes"] = {"routes": new_routes}
    return changed


async def _remove_provider_runtime(name: str):
    ModelClientPool._provider_pools.pop(name, None)
    # 删除渠道只需从 DB 重建剩余渠道路由 + /v1/models 响应缓存（纯 DB + 内存），
    # 不能带 sync_upstream=True——那会遍历所有剩余渠道逐个打上游拉模型列表，
    # 任一上游慢/挂起就会把整个 DELETE 请求串行拖住（表现为"删除渠道卡住"）。
    # 上游模型同步交由后台 refresh_models_loop 负责。
    await ModelClientPool.refresh_models(sync_upstream=False)


async def _cleanup_account_routes(provider_name: str, username: str) -> bool:
    cfg = await _read_json_async(CONFIG_FILE)
    old_data = {"model_routes": cfg.get("model_routes", {})}
    changed = _remove_account_from_routing_config(cfg, provider_name, username)
    if changed:
        await _write_json_async(CONFIG_FILE, cfg)
        await _log_operation("", "cleanup_account_routes", "account", username, old_data, {"model_routes": cfg.get("model_routes", {})})
    return changed


def _hash_password(password: str, salt: str = "") -> tuple[str, str]:
    """返回 (hash, salt)"""
    if not salt:
        salt = secrets.token_hex(16)
    h = hashlib.sha256(f"{salt}:{password}".encode()).hexdigest()
    return h, salt


def _get_admin_config() -> dict:
    return _read_json(CONFIG_FILE).get("admin", {})


def _save_admin_config(admin_cfg: dict):
    cfg = _read_json(CONFIG_FILE)
    cfg["admin"] = admin_cfg
    _write_json(CONFIG_FILE, cfg)


async def _resolve_admin_from_user_session() -> Optional[str]:
    """主鉴权：C 端 session cookie + role==admin。

    从 contextvar 取当前请求的 C 端 session cookie（由 main.py middleware 填充），
    经 monkeycode_compat 解析用户；role=="admin" 且 active 则通过，返回 user_id。
    compat 未启用 / cookie 缺失 / 非 admin / 任何异常都返回 None（交由应急兜底）。
    """
    cookie = _current_user_cookie.get()
    if not cookie:
        return None
    try:
        from monkeycode_compat.config import settings as _compat_settings
        if not _compat_settings.enabled:
            return None
        from monkeycode_compat.auth_service import auth_service
        authed = await auth_service.resolve_session(cookie)
    except Exception:
        return None
    if authed is None:
        return None
    if getattr(authed, "role", None) != "admin":
        return None
    return str(authed.id)


async def _require_admin(authorization: Optional[str] = Header(None)):
    """admin 接口鉴权（异步，支持 Redis）。

    主路径：C 端 session cookie + role==admin（无需前端显式传 token，cookie 同源自动带）。
    应急兜底：旧的 Bearer admin token（/admin/login 单密码换取），用于 compat 关闭
    或 session 服务异常时仍能进管理端，避免把自己锁在外面。
    返回一个字符串身份标识（user_id 或 admin token），兼容审计用法。
    """
    # 主路径：C 端 admin 用户会话
    user_id = await _resolve_admin_from_user_session()
    if user_id:
        return user_id

    # 应急兜底：Bearer admin token
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="需要登录")
    token = authorization[7:]
    session = await _get_admin_session(token)
    if not session or time.time() > session["expires_at"]:
        await _del_admin_session(token)
        raise HTTPException(status_code=401, detail="会话已过期，请重新登录")
    return token


async def _init_pool_and_refresh_models(pool):
    await pool.restore_cooldowns()
    await pool.init_all()
    await ModelClientPool.refresh_models()


async def _refresh_models_after_account_init(provider, client):
    now = time.time()
    try:
        await provider.init_auth(True)
        client.auth_ok = True
        client.auth_checked_at = now
        client.auth_error = ""
        await ModelClientPool.refresh_models()
    except Exception as e:
        client.auth_ok = False
        client.auth_checked_at = now
        client.auth_error = f"初始化失败: {e}"


def _is_custom_provider_config(cfg: dict) -> bool:
    return cfg.get("type") == "custom" or bool(cfg.get("custom_channel"))


def _validate_provider_name(name: str):
    if not re.fullmatch(r"[a-zA-Z0-9_-]{2,50}", name or ""):
        raise HTTPException(status_code=400, detail="渠道名只能包含字母、数字、下划线、中划线，长度 2-50")


def _auto_generate_name(remark: str, existing_names: set[str]) -> str:
    """从 remark/显示名自动生成唯一的内部 name。"""
    text = (remark or "channel").strip().lower()
    # 将中文和特殊字符替换为 -
    base = re.sub(r"[^a-z0-9_-]+", "-", text).strip("-")
    if len(base) < 2:
        base = "ch-" + base if base else "ch"
    base = base[:40]
    name = base
    counter = 2
    while name in existing_names:
        name = f"{base}-{counter}"
        counter += 1
    return name


def _normalize_base_url_for_compare(url: str) -> str:
    value = (url or "").strip()
    return (value[:-1] if value.endswith("/") else value).lower()


def _trim_one_trailing_slash(url: str) -> str:
    value = (url or "").strip()
    return value[:-1] if value.endswith("/") else value


def _quick_provider_slug(base_url: str) -> tuple[str, str]:
    try:
        parts = urlsplit(base_url)
    except Exception:
        parts = None
    host = (parts.hostname if parts else "") or ""
    slug = re.sub(r"[^a-z0-9_-]+", "-", host.removeprefix("api.").split(".")[0].lower()).strip("-") or "custom"
    if len(slug) < 2:
        slug = f"cc-{slug}"
    return slug[:50], host or base_url


def _extract_quick_provider_connections(raw: str) -> list[dict]:
    text = (raw or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="请输入 base_url 和 API Key")

    candidates: list[dict] = []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            candidates = [parsed]
        elif isinstance(parsed, list):
            candidates = [x for x in parsed if isinstance(x, dict)]
    except Exception:
        candidates = []

    if candidates:
        conns = []
        for item in candidates:
            key = (item.get("key") or item.get("api_key") or item.get("password") or "").strip()
            url = (item.get("url") or item.get("base_url") or "").strip()
            if key and url:
                conns.append({"key": key, "url": _trim_one_trailing_slash(url)})
        if conns:
            return conns

    conns = []
    current_url = ""
    keys: list[str] = []
    for line in [x.strip() for x in text.splitlines() if x.strip()]:
        if line.startswith("{") and line.endswith("}"):
            try:
                item = json.loads(line)
            except Exception:
                item = None
            if isinstance(item, dict):
                key = (item.get("key") or item.get("api_key") or item.get("password") or "").strip()
                url = (item.get("url") or item.get("base_url") or "").strip()
                if key and url:
                    conns.append({"key": key, "url": _trim_one_trailing_slash(url)})
                    continue
        if re.match(r"^https?://", line, re.I):
            current_url = _trim_one_trailing_slash(line)
        else:
            keys.append(line)
    if conns:
        return conns
    if not current_url:
        raise HTTPException(status_code=400, detail="请至少输入一行 base_url，例如 https://api.example.com")
    if not keys:
        raise HTTPException(status_code=400, detail="请至少输入一个 API Key")
    return [{"key": key, "url": current_url} for key in keys]


async def _detect_quick_custom_provider(raw: str) -> dict:
    conns = _extract_quick_provider_connections(raw)
    base_url = _trim_one_trailing_slash(conns[0]["url"])
    if not re.match(r"^https?://", base_url, re.I):
        raise HTTPException(status_code=400, detail="base_url 格式不正确")
    keys = []
    seen_keys = set()
    for item in conns:
        if _normalize_base_url_for_compare(item["url"]) != _normalize_base_url_for_compare(base_url):
            raise HTTPException(status_code=400, detail="一次快捷创建只能使用同一个 base_url")
        if item["key"] not in seen_keys:
            seen_keys.add(item["key"])
            keys.append(item["key"])
    provider_configs = await config.Config.get_providers()
    normalized = _normalize_base_url_for_compare(base_url)
    duplicates = []
    for name in _provider_names(provider_configs):
        cfg = provider_configs.get(name, {}) or {}
        if _normalize_base_url_for_compare(cfg.get("base_url")) == normalized:
            duplicates.append({"name": name, "remark": cfg.get("remark", ""), "base_url": cfg.get("base_url", "")})
    name, remark = _quick_provider_slug(base_url)
    return {
        "ok": True,
        "base_url": base_url,
        "name": name,
        "remark": remark,
        "accounts": [{"username": f"key-{i + 1}", "password": key, "switch": True} for i, key in enumerate(keys)],
        "duplicate_providers": duplicates,
    }


async def _load_provider_runtime(name: str, cfg: dict):
    pool = ModelClientPool.get_provider_pool(name)
    provider_class = _get_provider_class(name, cfg)
    # 代码渠道改 code 会 exec 出新类；旧 pool 仍持有旧 client_class 时，下面 add_accounts
    # 还按旧类实例化账号，新代码永不生效。类变了（或还没注册）就整体换池：
    # register_provider 直接替换 _provider_pools[name]，channel/clients 重建，语义对齐
    # needs_full_reload。普通渠道类对象不变（module-level 单例），is 命中即跳过，保留运行态。
    if pool is None or pool.client_class is not provider_class:
        ModelClientPool.register_provider(name, provider_class)
        pool = ModelClientPool.get_provider_pool(name)
    pool.clients.clear()
    rpm = cfg.get("rate_limit", {}).get("rpm_per_account", cfg.get("rate_limit", {}).get("requests_per_minute_per_account", 0))
    provider_extra = {k: v for k, v in cfg.items() if k not in ("accounts", "rate_limit")}
    provider_extra["rate_limit"] = cfg.get("rate_limit", {})
    provider_extra["limit_policy"] = await get_effective_provider_policy(name)
    provider_extra["provider_name"] = name
    await ModelClientPool._ensure_header_templates_loaded()
    pool.add_accounts(_runtime_accounts(cfg.get("accounts", [])), rpm, provider_extra)
    asyncio.create_task(_init_pool_and_refresh_models(pool))


async def _ensure_provider_runtime(name: str, cfg: dict | None = None) -> bool:
    """确保渠道运行池已加载。配置存在但池缺失时按配置重建池（兼容历史配置/热更新遗漏）。返回是否已就绪。"""
    if ModelClientPool.get_provider_pool(name):
        return True
    try:
        cfg = cfg or await _read_provider_config(name)
    except HTTPException:
        return False
    await _load_provider_runtime(name, cfg)
    return ModelClientPool.get_provider_pool(name) is not None



async def _ensure_provider_runtime_loaded(name: str, cfg: dict | None = None):
    if ModelClientPool.get_provider_pool(name):
        return
    await _load_provider_runtime(name, cfg or await _read_provider_config(name))


def _get_provider_class(name: str, cfg: dict | None = None):
    """根据渠道名称或配置获取对应的 Provider 类"""
    mapping = {
        "cloudflare": CloudflareProvider,
        "edgeone-ai": EdgeOneAIProvider,
    }
    # 直接匹配名称
    cls = mapping.get(name)
    if cls:
        return cls
    # 从模板创建的渠道，通过 builtin_type 匹配
    if cfg:
        builtin_type = cfg.get("builtin_type") or cfg.get("type") or ""
        cls = mapping.get(builtin_type)
        if cls:
            return cls
        # 代码渠道：贴的 Python 源码 exec 出 spec 类，包成 CustomProvider 子类适配器。
        # 覆盖所有 CRUD 热更新路径（_load_provider_runtime 都过这里）。
        if builtin_type == "code" and cfg.get("code"):
            from providers.code_loader import load_code_provider_class, CodeChannelError
            try:
                return load_code_provider_class(name, cfg["code"])
            except CodeChannelError as exc:
                # 让管理端 CRUD 回 400 而非 500，错误信息直达前端
                raise HTTPException(status_code=400, detail=str(exc))
    return CustomProvider


# ==================== 热更新辅助 ====================

# 账号级凭据字段：热更新时就地写回到现有 provider 实例（不重建），
# 保证 token/api_key 轮换下一次请求即生效。password/proxy 单独处理。
_ACCOUNT_CREDENTIAL_KEYS = {
    "token", "access_token", "refresh_token", "id_token",
    "github_token", "api_key", "authorization", "cookie",
    "cookies", "session", "access_token_expires_at_ms",
    "secure_1psid", "secure_1psidts",
}


def _account_limit_from(acc: dict, key: str, per_account_key: str, rate_limit_extra: dict) -> int:
    return int(acc.get(key, acc.get(per_account_key, rate_limit_extra.get(per_account_key, 0))) or 0)


def _apply_account_update_in_place(pool, client, runtime_acc: dict, rpm: int,
                                   rate_limit_extra: dict, is_disabled: bool):
    """就地更新现有 AccountClient/provider 的账号级字段，保留运行态。

    保留：_in_flight / 冷却 / RPM·TPM 滑窗 / auth 缓存 / 智能路由统计。
    仅当凭据（password/proxy/token 等）确有变化时才触发重新认证。
    """
    provider = client.provider
    # CustomProvider 系渠道运行时凭据只认 _api_key（headers 只读它）；下面裁决时要用「更新前」
    # 的值，才能判断前端回传的 api_key 是真的换了、还是陈旧回显。
    orig_api_key = getattr(provider, "api_key", "") if isinstance(provider, CustomProvider) else None

    # ---- 凭据就地写回 + 变更检测 ----
    creds_changed = False
    new_proxy = runtime_acc.get("proxy")
    if new_proxy != getattr(provider, "proxy", None):
        provider.proxy = new_proxy
        creds_changed = True
    # proxy_config_id 是 ProxyManager 出站路由的唯一真相字段（self.proxy 仅历史兼容）。
    # 账号换绑/解绑代理时必须就地改它，否则出站仍按旧配置路由——「改了代理不生效」。
    # 用 != 直接比较（含空串），才能覆盖「从有到无」的清空场景。
    if "proxy_config_id" in runtime_acc:
        new_proxy_config_id = runtime_acc.get("proxy_config_id") or ""
        if new_proxy_config_id != (getattr(provider, "proxy_config_id", "") or ""):
            provider.proxy_config_id = new_proxy_config_id
            creds_changed = True
    if "url_prefix" in runtime_acc:
        new_prefix = runtime_acc.get("url_prefix")
        if new_prefix != getattr(provider, "url_prefix", None):
            provider.url_prefix = new_prefix
            creds_changed = True
    if "password" in runtime_acc:
        new_pw = runtime_acc.get("password") or ""
        if new_pw and new_pw != getattr(provider, "password", ""):
            provider.password = new_pw
            creds_changed = True
    # 通用凭据字段：只接受非空新值。update_account 的 secret 保留逻辑（见 secret_keys）
    # 保证这些字段在 DB 里永远不会被写成空——前端不回传时旧值会被补回。所以「空」只可能
    # 是调用方传了残缺快照，跟着清掉会误删仍然有效的 OAuth token。
    for key in _ACCOUNT_CREDENTIAL_KEYS:
        if key not in runtime_acc or not hasattr(provider, key):
            continue
        new_val = runtime_acc.get(key)
        if new_val not in (None, "") and new_val != getattr(provider, key, None):
            setattr(provider, key, new_val)
            creds_changed = True

    # 代码渠道的 spec 用 ACCOUNT_FIELDS / account_schema 声明自己的账号字段，字段名由作者定
    # （enterprise_id / user_login / region / …），不在上面那张通用凭据表里。适配器已把它们挂成
    # 实例属性，这里一并同步，否则「授权刷新出新值但内存对象还是旧的」。
    # 这些字段不受 secret 保留逻辑覆盖，清空是能真落到 DB 的，所以要按 != 直接比较（含空串）
    # 让「从有到无」也生效——否则 DB 已清空、内存仍持旧值，60s 对账走同一守卫也修不回来。
    for key in (set(getattr(type(provider), "ACCOUNT_FIELDS", ()) or ()) - _ACCOUNT_CREDENTIAL_KEYS):
        if key not in runtime_acc or not hasattr(provider, key):
            continue
        new_val = runtime_acc.get(key)
        if new_val != getattr(provider, key, None):
            setattr(provider, key, new_val)
            creds_changed = True

    # CustomProvider 系渠道（自定义/jiekou 等）运行时凭据是 _api_key，但前端「密钥」输入框实际
    # 绑定的是 password 字段，提交时又会连带回传旧 api_key/key（或被 update_account 的 secret
    # 保留逻辑补回）。api_key 在初始化里优先级最高，陈旧 api_key 会遮蔽用户新填的 password，
    # 导致「改了密钥/密码却不生效」。这里在所有字段写回后统一裁决：显式且确有变化的 api_key/key
    # 优先，否则用（变化的）password 作为运行时凭据。判断基准是更新前的 orig_api_key。
    if isinstance(provider, CustomProvider):
        explicit = runtime_acc.get("api_key") or runtime_acc.get("key")
        new_pw = runtime_acc.get("password")
        if explicit and explicit != orig_api_key:
            resolved = explicit
        elif new_pw and new_pw != orig_api_key:
            resolved = new_pw
        else:
            resolved = None
        if resolved and resolved != getattr(provider, "api_key", ""):
            provider.api_key = resolved
            creds_changed = True

    # ---- 账号级限额：改阈值，不动运行态窗口 ----
    # RPM/TPM/并发仍支持账号级覆盖；RPD 已收敛到渠道级策略 account_rpd。
    client.rpm_limit = rpm
    client.tpm_limit = _account_limit_from(runtime_acc, "tpm_limit", "tpm_per_account", rate_limit_extra)
    client.concurrent_limit = _account_limit_from(runtime_acc, "concurrent_limit", "concurrent_per_account", rate_limit_extra)
    if runtime_acc.get("balance") is not None:
        client.balance = float(runtime_acc.get("balance") or 0)
    client.balance_threshold = float(runtime_acc.get("balance_threshold") or 0)
    # 冻结态属运行态（内存桶 + Redis 兜底），apply 不碰：in-place 更新保留既有冻结态，
    # 新账号的永久冻结由 restore_cooldowns 从 Redis 无 TTL key 回灌，不来自 DB is_frozen。

    # 账号元数据：配置类字段，热更新时整快照替换（与限额/凭据同机制）。
    incoming_metadata = runtime_acc.get("metadata")
    if isinstance(incoming_metadata, dict):
        client.metadata = dict(incoming_metadata)

    # ---- 发送门禁（switch）----
    client.disabled = is_disabled
    client.disable_reason = "switch_off" if is_disabled else ""

    # 凭据变了才重新认证；否则保留 auth 缓存/冷却/统计/in_flight。
    channel_enabled = getattr(pool.channel, "enabled", True) if pool.channel is not None else True
    if creds_changed and channel_enabled and not client.disabled:
        asyncio.create_task(_refresh_models_after_account_init(provider, client))


def _reload_account_into_pool(provider_name: str, acc: dict, rpm: int, extra: dict,
                              proxies: list[dict] | None = None):
    """将单个账号热加载到运行中的 ProviderPool。

    账号已存在则**就地更新字段**（保留 in_flight/冷却/统计/auth 缓存），
    仅新账号才创建新 provider + AccountClient 入池。

    注意：switch=False 的账号仍会加载入池（用 disabled=True 软跳过发送路由），
    以便手动测试、拉模型列表、认证等 Path A/B 操作依然可用。

    proxies：调用方已解析好的代理池（省掉本函数重读+规范化）。批量遍历账号的调用方
    （如 60s 对账）应传入，避免 O(账号数 × 代理数) 的重复解析。
    """
    pool = ModelClientPool.get_provider_pool(provider_name)
    if not pool:
        return

    username = acc.get("username", "")
    runtime_acc = dict(acc)
    proxies = proxies if proxies is not None else _read_proxies()
    runtime_acc["proxy"] = _resolve_account_proxy(acc, proxies)
    runtime_acc["url_prefix"] = _resolve_account_url_prefix(acc, proxies)
    runtime_acc["proxy_config_id"] = _resolve_account_proxy_config_id(acc, proxies)
    is_disabled = acc.get("switch") is False
    rate_limit_extra = extra.get("rate_limit", {}) if isinstance(extra.get("rate_limit"), dict) else {}

    existing = next((c for c in pool.clients if c.username == username), None)
    if existing is not None:
        # 已有账号：就地字段级更新，保留运行态。
        _apply_account_update_in_place(pool, existing, runtime_acc, rpm, rate_limit_extra, is_disabled)
        return

    # 新账号：构建 provider + AccountClient 入池。
    extra_copy = {k: v for k, v in extra.items() if k != "proxies"}
    extra_copy.setdefault("provider_name", provider_name)
    extra_copy.update({k: v for k, v in runtime_acc.items() if k not in ("username", "password", "proxy", "url_prefix", "proxy_id", "switch")})

    provider = pool.client_class(
        username=runtime_acc["username"],
        password=runtime_acc.get("password", ""),
        proxy=runtime_acc.get("proxy"),
        url_prefix=runtime_acc.get("url_prefix"),
        **extra_copy
    )
    if pool.channel is not None:
        provider.attach_channel(pool.channel)
    client = AccountClient(
        provider,
        rpm,
        tpm_limit=_account_limit_from(runtime_acc, "tpm_limit", "tpm_per_account", rate_limit_extra),
        concurrent_limit=_account_limit_from(runtime_acc, "concurrent_limit", "concurrent_per_account", rate_limit_extra),
        balance=float(acc.get("balance") or 0) if acc.get("balance") is not None else None,
        balance_threshold=float(acc.get("balance_threshold") or 0),
        is_frozen=bool(acc.get("is_frozen") or False),
        disabled=is_disabled,
        disable_reason="switch_off" if is_disabled else "",
        metadata=acc.get("metadata") if isinstance(acc.get("metadata"), dict) else None,
    )
    pool.clients.append(client)

    channel_enabled = getattr(pool.channel, "enabled", True) if pool.channel is not None else True
    if channel_enabled and not client.disabled:
        asyncio.create_task(_refresh_models_after_account_init(provider, client))


def _remove_account_from_pool(provider_name: str, username: str):
    """从运行中的 pool 移除账号（账号真正删除时使用）。

    同时清掉 Redis 里该账号的冻结兜底键（账号级 + 全部模型级 + 索引）：内存对象随
    pool.clients 一起消失，但 Redis 键会残留，后续同名账号重建 / restore_cooldowns
    回灌会把幽灵冻结带回来。
    """
    pool = ModelClientPool.get_provider_pool(provider_name)
    if pool:
        pool.clients = [c for c in pool.clients if c.username != username]
    asyncio.create_task(RedisLimitBackend.clear_account_cooldown(provider_name, username))
    asyncio.create_task(ModelClientPool.refresh_models())


def _set_account_disabled(provider_name: str, username: str, disabled: bool) -> bool:
    """就地翻转账号的发送门禁状态；不重建账号实例。"""
    pool = ModelClientPool.get_provider_pool(provider_name)
    if not pool:
        return False
    for client in pool.clients:
        if client.username == username:
            client.disabled = bool(disabled)
            client.disable_reason = "switch_off" if disabled else ""
            # 自定义渠道（贴码/jiekou/自定义等，均继承 CustomProvider）走 api_key 直连，
            # 无需 OAuth 体检；从禁用改启用时直接置为已认证，避免 auth_ok 停在 None
            # 让前端久久显示「待检查」徽章。
            if not disabled and isinstance(getattr(client, "provider", None), CustomProvider):
                client.auth_ok = True
                client.auth_error = ""
            return True
    return False


def _update_pool_rpm(provider_name: str, rpm: int):
    """热更新 pool 中所有账号的 RPM"""
    pool = ModelClientPool.get_provider_pool(provider_name)
    if pool:
        for c in pool.clients:
            c.rpm_limit = rpm


# 任一改动需要刷新一次模型列表的字段
_MODEL_REFRESH_KEYS = {
    "models_path", "chat_protocols", "auto_update_models", "model_id_rewrite_rules",
}


def _hot_update_custom_provider(provider_name: str, cfg: dict, changed_keys: set[str]):
    """渠道配置热更新：替换 Channel 快照，尽量保留账号运行时状态。"""
    pool = ModelClientPool.get_provider_pool(provider_name)
    if not pool:
        return

    provider_extra = _provider_extra(provider_name, cfg)
    if pool.channel is not None:
        pool.channel.apply(provider_extra)

    rate_limit = cfg.get("rate_limit") or {}
    rpm_changed = "rate_limit" in changed_keys
    new_rpm = rate_limit.get("rpm_per_account", rate_limit.get("requests_per_minute_per_account", 0))
    new_tpm = rate_limit.get("tpm_per_account", 0)
    new_concurrent = rate_limit.get("concurrent_per_account", 0)

    for client in pool.clients:
        provider = client.provider
        if getattr(provider, "_channel", None) is not pool.channel and pool.channel is not None:
            provider.attach_channel(pool.channel)
        # AccountClient 限额 — 只改阈值，不动 _requests / _token_usages / _in_flight / _cooldown_until
        if rpm_changed:
            client.rpm_limit = int(new_rpm or 0)
            client.tpm_limit = int(new_tpm or 0)
            client.concurrent_limit = int(new_concurrent or 0)

    if "billing_mode" in changed_keys:
        ModelClientPool._provider_billing_mode[provider_name] = cfg.get("billing_mode", "token")
    if changed_keys & _MODEL_REFRESH_KEYS:
        asyncio.create_task(ModelClientPool.refresh_models())


# ==================== 认证 ====================

@router.post("/login")
async def admin_login(data: dict):
    """管理员登录"""
    password = data.get("password", "")
    admin_cfg = _get_admin_config()

    if not admin_cfg.get("password_hash"):
        # 首次使用：设置初始密码
        h, s = _hash_password(password)
        admin_cfg["password_hash"] = h
        admin_cfg["password_salt"] = s
        _save_admin_config(admin_cfg)
    else:
        h, _ = _hash_password(password, admin_cfg.get("password_salt", ""))
        if h != admin_cfg["password_hash"]:
            raise HTTPException(status_code=401, detail="密码错误")

    token = secrets.token_hex(32)
    duration = admin_cfg.get("session_duration_hours", 24) * 3600
    await _set_admin_session(token, time.time() + duration)
    return {"token": token, "expires_in": int(duration)}


@router.post("/change-password")
async def change_admin_password(data: dict, authorization: Optional[str] = Header(None)):
    """修改管理员密码"""
    await _require_admin(authorization)
    old_pw = data.get("old_password", "")
    new_pw = data.get("new_password", "")
    if not new_pw or len(new_pw) < 4:
        raise HTTPException(status_code=400, detail="新密码至少4位")

    admin_cfg = _get_admin_config()
    h, _ = _hash_password(old_pw, admin_cfg.get("password_salt", ""))
    if h != admin_cfg.get("password_hash", ""):
        raise HTTPException(status_code=401, detail="旧密码错误")

    new_h, new_s = _hash_password(new_pw)
    admin_cfg["password_hash"] = new_h
    admin_cfg["password_salt"] = new_s
    _save_admin_config(admin_cfg)
    await _log_operation(authorization, "change_admin_password", "admin", "admin", None, {"changed": True})
    return {"ok": True}


@router.get("/config/admin")
async def get_admin_config(authorization: Optional[str] = Header(None)):
    """获取管理员配置（不含密码哈希）"""
    await _require_admin(authorization)
    cfg = _get_admin_config()
    return {
        "session_duration_hours": cfg.get("session_duration_hours", 24),
    }


@router.put("/config/admin/session")
async def update_admin_session(duration_hours: int = 24, authorization: Optional[str] = Header(None)):
    """修改 session 有效时长"""
    await _require_admin(authorization)
    if duration_hours < 1 or duration_hours > 720:
        raise HTTPException(status_code=400, detail="时长范围: 1-720 小时")
    admin_cfg = _get_admin_config()
    old_duration = admin_cfg.get("session_duration_hours", 24)
    admin_cfg["session_duration_hours"] = duration_hours
    _save_admin_config(admin_cfg)
    await _log_operation(authorization, "update_admin_session", "admin", "admin", {"session_duration_hours": old_duration}, {"session_duration_hours": duration_hours})
    return {"ok": True}


@router.post("/logout")
async def admin_logout(authorization: Optional[str] = Header(None)):
    """退出登录"""
    if authorization and authorization.startswith("Bearer "):
        await _del_admin_session(authorization[7:])
    return {"ok": True}


@router.get("/check-auth")
async def check_auth(token: str = Header(None, alias="Authorization")):
    """检查登录状态"""
    try:
        await _require_admin(token)
        return {"ok": True}
    except HTTPException:
        return JSONResponse({"ok": False}, status_code=401)


# ==================== 页面 ====================
# 旧的静态 admin.html 已废弃并移除；用户门户(/console)与管理后台(/manager)
# 统一由 user-frontend submodule 构建的 React SPA 提供。此处仅保留 SPA 兜底，
# 确保历史书签 / 子路径刷新仍能进入后台。
_ADMIN_INDEX_FILE = Path(__file__).parent / "user-frontend" / "dist" / "index.html"


@router.get("")
async def admin_page():
    return FileResponse(_ADMIN_INDEX_FILE)


@router.get("/page/{name}", include_in_schema=False)
async def admin_static_page(name: str):
    return FileResponse(_ADMIN_INDEX_FILE)


@router.get("/provider/{name}", include_in_schema=False)
async def admin_provider_page(name: str):
    return FileResponse(_ADMIN_INDEX_FILE)


# ==================== 主配置 ====================

_MAIN_CONFIG_PATCH_FIELDS = {
    "admin": {"session_duration_hours": None},
    "system": {"debug": None},
    "model_refresh": {"interval_minutes": None},
    "message_delete": {"enabled": None, "interval_minutes": None},
    "log_retention": {"max_entries": None},
    "data_retention": {"log_days": None, "cleanup_hour": None},
    "retry": {
        "max_retries": None,
        "context_overflow_not_retryable_enabled": None,
        "non_retryable_parameter_errors": {
            "status_codes": None,
            "types": None,
            "codes": None,
            "params": None,
            "markers": None,
        },
    },
    "stream": {"incomplete_error_enabled": None},
    "rate_limit": {"status_codes": None, "cooldown_seconds": None, "exception_cooldown_seconds": None, "allow_token_reservation_overflow": None},
    "account_test": {"types": None},
}


def _strip_bootstrap_config(data: dict) -> dict:
    result = dict(data)
    result.pop("postgres", None)
    result.pop("redis", None)
    return result


def _merge_main_config_patch(current: dict, patch: dict, allowed: dict = _MAIN_CONFIG_PATCH_FIELDS) -> dict:
    if not isinstance(patch, dict):
        raise HTTPException(status_code=400, detail="主配置更新必须是对象")
    merged = dict(current)
    for key, value in patch.items():
        allowed_value = allowed.get(key)
        if allowed_value is None and key not in allowed:
            raise HTTPException(status_code=400, detail=f"不允许通过主配置接口修改 {key}")
        if isinstance(allowed_value, dict):
            if not isinstance(value, dict):
                raise HTTPException(status_code=400, detail=f"{key} 必须是对象")
            existing = merged.get(key)
            if existing is None:
                existing = {}
            if not isinstance(existing, dict):
                raise HTTPException(status_code=400, detail=f"当前配置 {key} 必须是对象")
            merged[key] = _merge_main_config_patch(existing, value, allowed_value)
        else:
            if value is None:
                raise HTTPException(status_code=400, detail=f"{key} 不能为 null")
            merged[key] = value
    return merged


@router.get("/config/main")
async def get_main_config(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    data = _strip_bootstrap_config(_read_json(CONFIG_FILE))
    # 合并不可重试规则默认值：配置未设置时返回后端默认值
    # （兼容旧 key non_retryable_upstream_errors）
    retry = dict(data.get("retry") or {})
    changed = False
    if "non_retryable_parameter_errors" not in retry and "non_retryable_upstream_errors" not in retry:
        retry["non_retryable_parameter_errors"] = dict(config.DEFAULT_NON_RETRYABLE_PARAMETER_ERRORS)
        changed = True
    if "context_overflow_not_retryable_enabled" not in retry:
        retry["context_overflow_not_retryable_enabled"] = config.DEFAULT_CONTEXT_OVERFLOW_NOT_RETRYABLE_ENABLED
        changed = True
    if changed:
        data["retry"] = retry
    # 账号测试类型默认回填：配置未设置时返回后端默认值，让弹框首次打开即看到默认可编辑项。
    account_test = dict(data.get("account_test") or {})
    if not isinstance(account_test.get("types"), list) or not account_test.get("types"):
        account_test["types"] = [dict(t) for t in config.DEFAULT_TEST_TYPES]
        data["account_test"] = account_test
    return data


@router.put("/config/main")
async def update_main_config(data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    try:
        current = await _read_json_async(CONFIG_FILE)
        merged = _merge_main_config_patch(current, data)
        await _write_json_async(CONFIG_FILE, merged)
        await _log_operation(token, "update_main_config", "main_config", "main", _scrub_relay_secret(current), _scrub_relay_secret(data))
        return _strip_bootstrap_config(merged)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# repo 会拼进本地词表路径，因此这里同时承担路径穿越防护：只允许 owner/name 两段，
# 字符集限定为 HF 仓库名的合法字符，"..", "/", "\" 一律无法通过。
_HF_REPO_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,95}/[A-Za-z0-9][A-Za-z0-9._-]{0,95}$")


def _positive_float(value, default: float) -> float:
    """转正浮点数；非法或非正时回退默认值。"""
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _clean_tokenizer_rules(data) -> list[dict]:
    """校验并归一化分词规则。

    注意：本函数是「保存即真相」——未被这里显式保留的字段会被丢弃。新增规则字段时
    必须同步在此登记，否则管理端配好一保存就丢。
    """
    if not isinstance(data, list):
        raise HTTPException(status_code=400, detail="tokenizer rules 必须是数组")
    cleaned = []
    for idx, item in enumerate(data):
        if not isinstance(item, dict):
            raise HTTPException(status_code=400, detail=f"第 {idx + 1} 条规则必须是对象")
        name = str(item.get("name") or f"rule-{idx + 1}").strip()
        pattern = str(item.get("pattern") or "").strip()
        if not pattern:
            raise HTTPException(status_code=400, detail=f"第 {idx + 1} 条规则缺少 pattern")
        try:
            re.compile(pattern)
        except re.error as e:
            raise HTTPException(status_code=400, detail=f"第 {idx + 1} 条规则正则无效: {e}")
        tokenizer_type = str(item.get("type") or "chars").strip().lower()
        rule = {"name": name, "enabled": item.get("enabled") is not False, "pattern": pattern, "type": tokenizer_type}
        if tokenizer_type in ("tiktoken", "huggingface"):
            rule["encoding"] = str(item.get("encoding") or "cl100k_base").strip() or "cl100k_base"
            if tokenizer_type == "huggingface":
                repo = str(item.get("repo") or "").strip()
                if not repo:
                    raise HTTPException(status_code=400, detail=f"第 {idx + 1} 条规则 type=huggingface 需要填写 repo")
                if not _HF_REPO_RE.match(repo):
                    raise HTTPException(status_code=400, detail=f"第 {idx + 1} 条规则 repo 格式应为 owner/name：{repo}")
                rule["repo"] = repo
        elif tokenizer_type == "chars":
            rule["chars_per_token"] = _positive_float(item.get("chars_per_token"), 2)
            if item.get("cjk_chars_per_token") is not None:
                rule["cjk_chars_per_token"] = _positive_float(item.get("cjk_chars_per_token"), 2)
        else:
            raise HTTPException(status_code=400, detail=f"第 {idx + 1} 条规则 type 只能是 chars、tiktoken 或 huggingface")
        # 校准系数与结构开销对所有类型通用。
        if item.get("calibration") is not None:
            calibration = _positive_float(item.get("calibration"), 1.0)
            if calibration > 10:
                raise HTTPException(status_code=400, detail=f"第 {idx + 1} 条规则校准系数超出合理范围（应 ≤ 10）")
            rule["calibration"] = calibration
        if item.get("structural") is not None:
            rule["structural"] = item.get("structural") is True
        cleaned.append(rule)
    return cleaned


@router.get("/config/main/tokenizer-rules")
async def get_tokenizer_rules(token: str = Header(None, alias="Authorization")):
    """只读返回内置 tokenizer 策略摘要：模型族 → 估算方式。

    Token 规则已收回后端维护，用户不可编辑。旧 config 里的 rules 被忽略，
    仅用于兼容旧前端读取，不再参与运行时估算。
    """
    await _require_admin(token)
    summary = [
        {"family": rule.get("name"), "pattern": rule.get("pattern"), "type": rule.get("type"),
         "encoding": rule.get("encoding"), "repo": rule.get("repo")}
        for rule in usage_utils._BUILTIN_TOKENIZER_RULES if rule.get("enabled") is not False
    ]
    return {"mode": "built_in", "rules": summary, "editable": False,
            "note": "Token 统计策略由系统按模型自动选择，无需用户配置"}


@router.put("/config/main/tokenizer-rules")
async def update_tokenizer_rules(data: dict, token: str = Header(None, alias="Authorization")):
    """Tokenizer 规则由后端内置模型族映射维护，用户不可编辑。"""
    await _require_admin(token)
    return JSONResponse(
        status_code=400,
        content={"ok": False, "error": "Tokenizer 规则由系统内置模型族识别维护，用户不可编辑"},
    )


@router.post("/config/main/tokenizer-rules/warmup")
async def warmup_tokenizer_vocab(data: dict, token: str = Header(None, alias="Authorization")):
    """下载 HuggingFace 词表并写入 PostgreSQL（所有实例共享）。

    必须由管理端主动触发：估算函数在请求热路径上（token 预占 / 超限拦截），
    绝不能在那里联网下载几十 MB 的词表。运行时只读进程内预载的内存快照。
    """
    await _require_admin(token)
    repo = str((data or {}).get("repo") or "").strip()
    if not repo:
        return JSONResponse(status_code=400, content={"ok": False, "error": "缺少 repo"})
    if not _HF_REPO_RE.match(repo):
        return JSONResponse(status_code=400, content={"ok": False, "error": f"repo 格式应为 owner/name：{repo}"})

    mirror = str((data or {}).get("mirror") or "").strip().rstrip("/") or "https://hf-mirror.com"
    if not mirror.startswith(("http://", "https://")):
        return JSONResponse(status_code=400, content={"ok": False, "error": "mirror 必须是 http(s) 地址"})

    if not PostgresClient.pool:
        return JSONResponse(status_code=400, content={"ok": False, "error": "数据库未就绪，无法保存词表"})

    url = f"{mirror}/{repo}/resolve/main/tokenizer.json"
    import aiohttp

    etag = None
    try:
        timeout = aiohttp.ClientTimeout(total=300, sock_read=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return JSONResponse(
                        status_code=400,
                        content={"ok": False, "error": f"下载失败 HTTP {response.status}：{url}"},
                    )
                etag = (response.headers.get("ETag") or "").strip('"') or None
                payload = await response.read()
    except Exception as e:
        return JSONResponse(status_code=400, content={"ok": False, "error": f"下载失败: {type(e).__name__}: {e}"})

    # 先校验再入库：写坏内容会让运行时每次都解析失败。
    try:
        json.loads(payload.decode("utf-8"))
    except Exception as e:
        return JSONResponse(status_code=400, content={"ok": False, "error": f"词表不是合法 JSON：{e}"})

    try:
        await PostgresClient.upsert_tokenizer_vocab(
            repo, payload.decode("utf-8"), etag=etag, size=len(payload), mirror=mirror
        )
    except Exception as e:
        return JSONResponse(status_code=400, content={"ok": False, "error": f"写入数据库失败: {e}"})

    # 刷新本实例内存快照与缓存，自检可用性。
    usage_utils.invalidate_hf_tokenizer_cache(repo)
    await usage_utils.load_hf_tokenizer_from_pg(repo)
    loaded = usage_utils.load_hf_tokenizer(repo) is not None
    # 刷新内存状态（current / etag / bytes），供状态接口立即反映新值。
    import tokenizer_vocab_check
    tokenizer_vocab_check.record_download(repo, etag=etag, size=len(payload), mirror=mirror)
    # 通知其它实例重载该词表。
    await runtime_sync.publish(runtime_sync.EVENT_TOKENIZER_VOCAB, repo)
    await _log_operation(token, "warmup_tokenizer_vocab", "main_config", "tokenizer", None, {"repo": repo, "bytes": len(payload)})
    return {
        "ok": True,
        "repo": repo,
        "bytes": len(payload),
        "path": "pg:tokenizer_vocabs",
        "loadable": loaded,
        "warning": None if loaded else "词表已保存到数据库，但本地缺少 tokenizers 库，运行时仍会降级",
    }


@router.get("/config/main/tokenizer-vocab/status")
async def get_tokenizer_vocab_status(token: str = Header(None, alias="Authorization")):
    """内置 HF 词表的状态（存 PG，所有实例共享；反映本实例的检查结果）。"""
    await _require_admin(token)
    import tokenizer_vocab_check
    return {
        "enabled": config.Config.tokenizer_vocab_check_enabled(),
        "interval_hours": config.Config.tokenizer_vocab_check_interval_hours(),
        "mirror": config.Config.tokenizer_vocab_mirror(),
        "items": tokenizer_vocab_check.current_status(),
    }


@router.post("/config/main/tokenizer-vocab/check")
async def check_tokenizer_vocab_now(token: str = Header(None, alias="Authorization")):
    """立即跑一轮版本检查（只比对 ETag，不下载）。"""
    await _require_admin(token)
    import tokenizer_vocab_check
    await tokenizer_vocab_check.run_check_once()
    return {"ok": True, "items": tokenizer_vocab_check.current_status()}


@router.post("/config/main/tokenizer-rules/test")
async def test_tokenizer_rules(data: dict, token: str = Header(None, alias="Authorization")):
    """用真实历史日志试算候选规则，返回 估算/真实 的偏差分布。

    地面真值取 request_logs.prompt_tokens（上游真实回报），请求体从 ClickHouse
    按 attempt_key 取回。候选规则不落盘。
    """
    await _require_admin(token)
    raw_rules = (data or {}).get("rules")
    if raw_rules is not None and not isinstance(raw_rules, list):
        return JSONResponse(status_code=400, content={"ok": False, "error": "rules 必须是数组"})
    try:
        candidate_rules = _clean_tokenizer_rules(raw_rules) if raw_rules is not None else None
    except HTTPException as e:
        return JSONResponse(status_code=400, content={"ok": False, "error": str(e.detail)})

    try:
        limit = max(1, min(500, int((data or {}).get("limit") or 100)))
    except (TypeError, ValueError):
        limit = 100
    model_filter = str((data or {}).get("model") or "").strip()

    if not PostgresClient.pool:
        return JSONResponse(status_code=400, content={"ok": False, "error": "数据库未就绪，无法试算"})

    from request_payload_store import fetch_payloads

    # 只取真实成功、上游给了 prompt_tokens 的日志；测试类端点会被上游注入系统提示，
    # 真实值远大于我方请求体，会污染统计，必须排除。
    sql = """
        SELECT attempt_key, model, actual_model, endpoint, prompt_tokens
        FROM request_logs
        WHERE success AND prompt_tokens > 0
          AND created_at > now() - interval '7 days'
          AND (endpoint IS NULL OR endpoint NOT LIKE '/admin/%')
    """
    params: list = []
    if model_filter:
        params.append(f"%{model_filter}%")
        sql += f" AND (model ILIKE ${len(params)} OR actual_model ILIKE ${len(params)})"
    params.append(limit)
    sql += f" ORDER BY created_at DESC LIMIT ${len(params)}"

    async with PostgresClient.pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
    if not rows:
        return {"ok": True, "samples": 0, "rows": [], "summary": None, "note": "近 7 天没有符合条件的日志"}

    payloads, payload_status = await fetch_payloads([r["attempt_key"] for r in rows])
    if payload_status != "available" or not payloads:
        return {
            "ok": True,
            "samples": 0,
            "rows": [],
            "summary": None,
            "note": f"请求体不可用（ClickHouse {payload_status}），无法试算",
        }

    # 候选规则通过参数显式下传，绝不改全局 getter：那是进程级状态，会污染此刻正在
    # 服务的真实请求（预占与超限拦截都读它）。rules=None 时等价于跑当前生效配置。
    samples = []
    for row in rows:
        payload = payloads.get(row["attempt_key"]) or {}
        body = payload.get("router_request_body") or payload.get("request_body")
        if not isinstance(body, dict) or not body.get("messages"):
            continue
        real = int(row["prompt_tokens"] or 0)
        if real <= 0:
            continue
        samples.append((row["attempt_key"], row["actual_model"] or row["model"] or "", body, real))

    def _run_estimates() -> list[dict]:
        out = []
        for attempt_key, model, body, real in samples:
            estimate = int(usage_utils.estimate_input_tokens(model, body, "upstream", candidate_rules) or 0)
            out.append({
                "attempt_key": attempt_key,
                "model": model,
                "rule": usage_utils.resolve_tokenizer_rule(model, candidate_rules).get("name") or "(默认兜底)",
                "real_prompt_tokens": real,
                "estimated_prompt_tokens": estimate,
                "ratio": round(estimate / real, 4),
            })
        return out

    # 几百个大请求体的分词是纯 CPU 且耗时可达数十秒，留在事件循环里会卡住整个代理服务。
    results = await asyncio.to_thread(_run_estimates)

    if not results:
        return {"ok": True, "samples": 0, "rows": [], "summary": None, "note": "取到日志但请求体为空，无法试算"}

    ratios = sorted(item["ratio"] for item in results)

    def _pct(values: list[float], pct: float) -> float:
        if not values:
            return 0.0
        idx = min(len(values) - 1, max(0, int(round((len(values) - 1) * pct))))
        return round(values[idx], 4)

    # 中位比值是核心指标：1.00 表示估算与上游真实口径一致。
    median = _pct(ratios, 0.5)
    summary = {
        "samples": len(ratios),
        "median_ratio": median,
        "p10_ratio": _pct(ratios, 0.1),
        "p90_ratio": _pct(ratios, 0.9),
        "worst_ratio": round(ratios[-1], 4),
        # 把中位数拉到 1.00 所需的系数，直接填进规则的 calibration 即可。
        "suggested_calibration": round(1 / median, 4) if median > 0 else None,
    }
    return {
        "ok": True,
        "samples": len(results),
        "summary": summary,
        "rows": sorted(results, key=lambda item: item["ratio"], reverse=True)[:50],
    }


@router.get("/config/thinking-global")
async def get_thinking_global_config(token: str = Header(None, alias="Authorization")):
    """获取推理思考全局配置"""
    await _require_admin(token)
    return {
        "reasoning_owned_by": config.Config.get_reasoning_owned_by(),
        "thinking_global_enabled": config.Config.get_thinking_global_enabled(),
        "thinking_defaults": config.Config.get_thinking_defaults(),
        "reasoning_defaults": config.Config.get_reasoning_defaults(),
        "simulated_client_defaults": config.Config.get_simulated_client_defaults(),
    }


@router.put("/config/thinking-global")
async def update_thinking_global_config(data: dict, token: str = Header(None, alias="Authorization")):
    """更新推理思考全局配置（只能修改 reasoning_owned_by 和 thinking_global_enabled）"""
    await _require_admin(token)
    if "reasoning_owned_by" in data:
        owned_by = data["reasoning_owned_by"]
        if not isinstance(owned_by, list):
            raise HTTPException(status_code=400, detail="reasoning_owned_by 必须是数组")
        config.Config.set_reasoning_owned_by(owned_by)
    if "thinking_global_enabled" in data:
        enabled = data["thinking_global_enabled"]
        if not isinstance(enabled, bool):
            raise HTTPException(status_code=400, detail="thinking_global_enabled 必须是 boolean")
        config.Config.set_thinking_global_enabled(enabled)
    if "simulated_client_defaults" in data:
        sc = data["simulated_client_defaults"]
        if not isinstance(sc, dict):
            raise HTTPException(status_code=400, detail="simulated_client_defaults 必须是对象")
        config.Config.set_simulated_client_defaults(sc)
    await _log_operation(token, "update_thinking_global", "main_config", "thinking_global", None, data)
    return {"ok": True}


@router.get("/config/owned-by-list")
async def get_owned_by_list(token: str = Header(None, alias="Authorization")):
    """获取所有模型元数据的 owned_by 列表（去重）"""
    await _require_admin(token)
    models = (await _mm.list_metadata_async()).get("models") or []
    owned_by_set = set()
    for m in models:
        owned_by = (m.get("owned_by") or "").strip()
        if owned_by:
            owned_by_set.add(owned_by)
    return {"owned_by": sorted(owned_by_set)}


@router.put("/config/main/server")
async def update_server_config(host: str = "0.0.0.0", port: int = 8001, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cfg = _read_json(CONFIG_FILE)
    cfg.setdefault("server", {})["host"] = host
    cfg["server"]["port"] = port
    _write_json(CONFIG_FILE, cfg)
    await _log_operation(token, "update_server_config", "main_config", "server", None, {"host": host, "port": port})
    return {"ok": True}


@router.put("/config/main/logging")
async def update_logging_config(level: str = "DEBUG", token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cfg = _read_json(CONFIG_FILE)
    cfg.setdefault("logging", {})["level"] = level.upper()
    _write_json(CONFIG_FILE, cfg)
    await _log_operation(token, "update_logging_config", "main_config", "logging", None, {"level": level.upper()})
    return {"ok": True}


@router.put("/config/main/model-refresh")
async def update_model_refresh(interval_minutes: int = 30, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cfg = _read_json(CONFIG_FILE)
    cfg.setdefault("model_refresh", {})["interval_minutes"] = interval_minutes
    _write_json(CONFIG_FILE, cfg)
    await _log_operation(token, "update_model_refresh", "main_config", "model_refresh", None, {"interval_minutes": interval_minutes})
    return {"ok": True}


@router.put("/config/main/message-delete")
async def update_message_delete(enabled: bool, interval_minutes: int = 30, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cfg = _read_json(CONFIG_FILE)
    cfg.setdefault("message_delete", {})["enabled"] = enabled
    cfg["message_delete"]["interval_minutes"] = interval_minutes
    _write_json(CONFIG_FILE, cfg)
    await _log_operation(token, "update_message_delete", "main_config", "message_delete", None, {"enabled": enabled, "interval_minutes": interval_minutes})
    return {"ok": True}


@router.put("/config/main/log-retention")
async def update_log_retention(max_entries: int = 0, token: str = Header(None, alias="Authorization")):
    """设置日志条数上限。0 = 不按条数删数据库日志（只按天数保留）。

    这个值有两个消费者，量纲差好几个数量级，所以不再共用一个取值区间：
    - 数据库删除阈值（db.cleanup_request_logs）：0 表示关闭按条数删，上界不限；
      默认 0——按条数裁剪会删掉尚未聚合进小时统计表的原始日志，历史永久缺失；
    - 内存最近日志环形缓冲（main._recent_logs）：常驻内存且只被 /recent-logs 读，
      仍夹在 10~10000，否则填 0 会让缓冲永远为空、填几百万会吃掉内存。
    """
    await _require_admin(token)
    if max_entries < 0:
        raise HTTPException(status_code=400, detail="max_entries 不能为负数（0 = 不按条数删）")
    cfg = _read_json(CONFIG_FILE)
    cfg.setdefault("log_retention", {})["max_entries"] = max_entries
    _write_json(CONFIG_FILE, cfg)
    # 同步到运行时：内存缓冲用自己的安全区间，与数据库阈值解耦。
    from main import _recent_logs
    import main as _main
    memory_cap = min(max(max_entries or 200, 10), 10000)
    _main._MAX_LOG_ENTRIES = memory_cap
    while len(_recent_logs) > memory_cap:
        _recent_logs.popleft()
    await _log_operation(token, "update_log_retention", "main_config", "log_retention", None, {"max_entries": max_entries})
    return {"ok": True, "max_entries": max_entries}


@router.put("/config/main/data-retention")
async def update_data_retention_config(
    log_days: int = 30,
    cleanup_hour: int | None = None,
    token: str = Header(None, alias="Authorization"),
):
    await _require_admin(token)
    if log_days < 1:
        raise HTTPException(status_code=400, detail="log_days 最小为 1")
    cfg = _read_json(CONFIG_FILE)
    retention = cfg.setdefault("data_retention", {})
    retention["log_days"] = log_days
    # cleanup_hour 可选：省略时保留原值；传入时限定 0-23（每日清理触发的本地整点）。
    if cleanup_hour is not None:
        if cleanup_hour < 0 or cleanup_hour > 23:
            raise HTTPException(status_code=400, detail="cleanup_hour 范围: 0-23")
        retention["cleanup_hour"] = cleanup_hour
    # 归档与响应体保留已下线，移除旧键避免残留策略生效
    retention.pop("keep_response_hours", None)
    retention.pop("archive_keep_entries", None)
    retention.pop("keep_full_entries_per_provider", None)
    _write_json(CONFIG_FILE, cfg)
    await _log_operation(token, "update_data_retention", "main_config", "data_retention", None, {"log_days": log_days, "cleanup_hour": retention.get("cleanup_hour")})
    return {
        "ok": True,
        "log_days": log_days,
        "cleanup_hour": retention.get("cleanup_hour"),
    }


# 手动清理在途任务。全表 DELETE 分批跑可能几十秒，请求内 await 会超时，
# 所以后台执行、立即返回；并发点击只复用同一个任务，避免多个批量 DELETE 互相锁等。
_manual_log_cleanup_task: asyncio.Task | None = None


@router.post("/config/main/cleanup-logs")
async def cleanup_logs_now(token: str = Header(None, alias="Authorization")):
    """立即按当前保留配置清理请求日志（后台异步执行，结果见通知中心）。

    清理口径与每日定时任务完全一致：按 data_retention.log_days 删旧行，
    并在 log_retention.max_entries>0 时删超额最老行。
    """
    await _require_admin(token)
    global _manual_log_cleanup_task
    if _manual_log_cleanup_task is not None and not _manual_log_cleanup_task.done():
        return {
            "ok": True,
            "started": False,
            "detail": "已有清理任务在执行中，请等待完成后再试",
            "keep_days": config.Config.get_log_retention_days(),
            "max_entries": config.Config.get_log_retention_max_entries(),
        }
    _manual_log_cleanup_task = asyncio.create_task(ModelClientPool.clean_response_data("manual"))
    keep_days = config.Config.get_log_retention_days()
    max_entries = config.Config.get_log_retention_max_entries()
    await _log_operation(token, "cleanup_logs_now", "main_config", "data_retention", None, {"keep_days": keep_days, "max_entries": max_entries})
    return {"ok": True, "started": True, "keep_days": keep_days, "max_entries": max_entries}


def _clean_non_retryable_rules(raw) -> dict:
    defaults = config.DEFAULT_NON_RETRYABLE_PARAMETER_ERRORS
    raw = raw if isinstance(raw, dict) else {}

    def clean_str_list(key: str) -> list[str]:
        value = raw.get(key, defaults.get(key, []))
        if isinstance(value, str):
            value = [x.strip() for x in value.split(",")]
        if not isinstance(value, list):
            value = defaults.get(key, [])
        result: list[str] = []
        for item in value:
            text = str(item or "").strip().lower()
            if text and text not in result:
                result.append(text)
        return result

    status_codes_raw = raw.get("status_codes", defaults.get("status_codes", []))
    if isinstance(status_codes_raw, (int, str)):
        status_codes_raw = [status_codes_raw]
    if not isinstance(status_codes_raw, list):
        status_codes_raw = defaults.get("status_codes", [])
    status_codes: list[int] = []
    for item in status_codes_raw:
        try:
            code = int(item)
        except (TypeError, ValueError):
            continue
        if 100 <= code <= 599 and code not in status_codes:
            status_codes.append(code)

    return {
        "status_codes": status_codes,
        "types": clean_str_list("types"),
        "codes": clean_str_list("codes"),
        "params": clean_str_list("params"),
        "markers": clean_str_list("markers"),
    }


def _clean_output_interception_rules(raw) -> dict:
    """归一异常输出拦截规则，并按 match_type 校验 pattern。

    regex 规则的 pattern 必须能编译；text 规则按字面量匹配，不解析正则元字符。
    与 Config.get_output_interception_rules 的口径保持一致，确保写存与读取一致。
    """
    import re
    defaults = config.DEFAULT_OUTPUT_INTERCEPTION_RULES
    raw = raw if isinstance(raw, dict) else {}
    enabled = raw.get("enabled", defaults["enabled"])
    if not isinstance(enabled, bool):
        enabled = defaults["enabled"]
    rules_raw = raw.get("rules")
    if not isinstance(rules_raw, list):
        rules_raw = defaults["rules"]
    rules: list[dict] = []
    for item in rules_raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        pattern = str(item.get("pattern") or "").strip()
        if not name or not pattern:
            continue
        item_enabled = item.get("enabled", True)
        if not isinstance(item_enabled, bool):
            item_enabled = True
        match_type = item.get("match_type", "regex")
        if match_type not in {"regex", "text"}:
            match_type = "regex"
        if match_type == "regex":
            try:
                re.compile(pattern)
            except re.error:
                continue
        rules.append({
            "name": name,
            "enabled": item_enabled,
            "match_type": match_type,
            "pattern": pattern,
        })
    return {"enabled": enabled, "rules": rules}


@router.put("/config/main/retry")
async def update_retry_config(data: dict | None = None, max_retries: int = 3, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    if isinstance(data, dict) and "max_retries" in data:
        max_retries = data.get("max_retries")
    try:
        max_retries = int(max_retries)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="max_retries 必须是整数")
    if max_retries < 0 or max_retries > 10:
        raise HTTPException(status_code=400, detail="范围: 0-10")
    cfg = _read_json(CONFIG_FILE)
    retry = cfg.setdefault("retry", {})
    retry["max_retries"] = max_retries
    if isinstance(data, dict) and "context_overflow_not_retryable_enabled" in data:
        retry["context_overflow_not_retryable_enabled"] = data.get("context_overflow_not_retryable_enabled") is not False
    if isinstance(data, dict) and "non_retryable_parameter_errors" in data:
        retry["non_retryable_parameter_errors"] = _clean_non_retryable_rules(data.get("non_retryable_parameter_errors"))
    _write_json(CONFIG_FILE, cfg)
    await _log_operation(token, "update_retry_config", "main_config", "retry", None, {"max_retries": max_retries, "context_overflow_not_retryable_enabled": retry.get("context_overflow_not_retryable_enabled"), "non_retryable_parameter_errors": retry.get("non_retryable_parameter_errors")})
    return {"ok": True, "max_retries": max_retries, "context_overflow_not_retryable_enabled": retry.get("context_overflow_not_retryable_enabled"), "non_retryable_parameter_errors": retry.get("non_retryable_parameter_errors")}


@router.put("/config/main/stream")
async def update_stream_config(data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cfg = _read_json(CONFIG_FILE)
    cfg.setdefault("stream", {})["incomplete_error_enabled"] = data.get("incomplete_error_enabled") is not False
    _write_json(CONFIG_FILE, cfg)
    await _log_operation(token, "update_stream_config", "main_config", "stream", None, {"incomplete_error_enabled": cfg["stream"]["incomplete_error_enabled"]})
    return {"ok": True, "incomplete_error_enabled": cfg["stream"]["incomplete_error_enabled"]}


@router.put("/config/main/rate-limit")
async def update_rate_limit_config(data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    raw_codes = data.get("status_codes")
    if isinstance(raw_codes, (int, str)):
        raw_codes = [raw_codes]
    elif not isinstance(raw_codes, list):
        raw_codes = []
    codes: list[int] = []
    for item in raw_codes:
        try:
            code = int(item)
        except (TypeError, ValueError):
            continue
        if 100 <= code <= 599 and code not in codes:
            codes.append(code)
    if not codes:
        codes = [429]
    try:
        cooldown = int(data.get("cooldown_seconds", 60))
    except (TypeError, ValueError):
        cooldown = 60
    cooldown = max(0, cooldown)
    try:
        exception_cooldown = int(data.get("exception_cooldown_seconds", 30))
    except (TypeError, ValueError):
        exception_cooldown = 30
    exception_cooldown = max(0, exception_cooldown)
    cfg = _read_json(CONFIG_FILE)
    # 该端点整体重建 rate_limit，未随请求提交的字段必须显式保留，否则「允许 token 预占触线
    # 放行」等开关会被这次保存静默清掉（缺失即回退默认值）。
    existing_rate_limit = cfg.get("rate_limit") or {}
    if "allow_token_reservation_overflow" in data:
        allow_overflow = data.get("allow_token_reservation_overflow") is not False
    else:
        allow_overflow = existing_rate_limit.get(
            "allow_token_reservation_overflow", config.DEFAULT_ALLOW_TOKEN_RESERVATION_OVERFLOW
        ) is not False
    cfg["rate_limit"] = {
        "status_codes": codes,
        "cooldown_seconds": cooldown,
        "exception_cooldown_seconds": exception_cooldown,
        "allow_token_reservation_overflow": allow_overflow,
    }
    _write_json(CONFIG_FILE, cfg)
    from config import Config
    Config.reload()
    await _log_operation(token, "update_rate_limit_config", "main_config", "rate_limit", None, {"status_codes": codes, "cooldown_seconds": cooldown, "exception_cooldown_seconds": exception_cooldown, "allow_token_reservation_overflow": allow_overflow})
    return {"ok": True, "status_codes": codes, "cooldown_seconds": cooldown, "exception_cooldown_seconds": exception_cooldown, "allow_token_reservation_overflow": allow_overflow}


@router.put("/config/main/system")
async def update_system_config(debug: bool = False, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cfg = _read_json(CONFIG_FILE)
    cfg.setdefault("system", {})["debug"] = debug
    _write_json(CONFIG_FILE, cfg)
    await _log_operation(token, "update_system_config", "main_config", "system", None, {"debug": debug})
    return {"ok": True, "debug": debug}


def _clean_test_types(raw) -> list[dict]:
    """校验并归一账号测试类型列表。非法结构直接 400，避免脏配置写入。

    每项：key（非空、唯一）、label、operation（None 或 image/video/tts_generation）、
    messages（数组）、body（对象）。空列表回退后端默认类型。
    """
    if raw is None:
        return [dict(t) for t in config.DEFAULT_TEST_TYPES]
    if not isinstance(raw, list):
        raise HTTPException(status_code=400, detail="account_test.types 必须是数组")
    allowed_ops = {"image", "video", "tts_generation"}
    seen: set[str] = set()
    cleaned: list[dict] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            raise HTTPException(status_code=400, detail=f"第 {idx + 1} 个测试类型必须是对象")
        key = str(item.get("key") or "").strip()
        if not key:
            raise HTTPException(status_code=400, detail=f"第 {idx + 1} 个测试类型缺少 key")
        if key in seen:
            raise HTTPException(status_code=400, detail=f"测试类型 key 重复: {key}")
        seen.add(key)
        operation = item.get("operation")
        if operation is not None:
            operation = str(operation).strip() or None
        if operation is not None and operation not in allowed_ops:
            raise HTTPException(status_code=400, detail=f"「{key}」的 operation 非法: {operation}")
        messages = item.get("messages")
        if messages is None:
            messages = []
        if not isinstance(messages, list):
            raise HTTPException(status_code=400, detail=f"「{key}」的 messages 必须是数组")
        body = item.get("body")
        if body is None:
            body = {}
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail=f"「{key}」的 body 必须是对象")
        cleaned.append({
            "key": key,
            "label": str(item.get("label") or key),
            "operation": operation,
            "messages": messages,
            "body": body,
        })
    return cleaned or [dict(t) for t in config.DEFAULT_TEST_TYPES]


@router.put("/config/main/account-test")
async def update_account_test_config(data: dict, token: str = Header(None, alias="Authorization")):
    """账号测试类型列表。只改 account_test 段，与其它主配置段互不覆盖。"""
    await _require_admin(token)
    types = _clean_test_types(data.get("types"))
    cfg = _read_json(CONFIG_FILE)
    cfg.setdefault("account_test", {})["types"] = types
    _write_json(CONFIG_FILE, cfg)
    from config import Config
    Config.reload()
    await _log_operation(token, "update_account_test_config", "main_config", "account_test", None, {"types_count": len(types)})
    return {"ok": True, "types": types}


# ==================== API Keys ====================

@router.get("/config/api-keys")
async def get_api_keys(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    # 列表只取展示列（id/name/disabled/parent_id/expires_at/key 等），不拉
    # rate_limit/whitelist/thinking_config/usage_limit/group_ids 等大 JSONB——
    # key 多时这些是列表的主要传输/反序列化成本。编辑弹窗点开时前端单独拉
    # /config/api-keys/{id} 详情拿全字段。
    keys = await PostgresClient.list_api_keys_lite()
    return {"enabled": config.Config.api_keys_enabled(), "keys": keys}


@router.put("/config/api-keys/enabled")
async def toggle_api_keys(enabled: bool, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cfg = await _read_json_async(CONFIG_FILE)
    cfg.setdefault("api_keys", {})["enabled"] = enabled
    await _write_json_async(CONFIG_FILE, cfg)
    await _log_operation(token, "toggle_api_keys", "api_key", "enabled", None, {"enabled": enabled})
    return {"ok": True}


@router.put("/config/api-keys/add")
async def add_api_key(data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    row = await PostgresClient.add_api_key(data)
    await config.Config.refresh_api_keys_cache()
    row["rate_limit"] = json.loads(row["rate_limit"]) if isinstance(row["rate_limit"], str) else (row["rate_limit"] or {})
    await _log_operation(token, "add_api_key", "api_key", str(row.get("id")), None, _scrub_relay_secret(row))
    from monkeycode_compat.notify_core import emit_notification_background
    emit_notification_background(
        "api_key.created",
        params={"api_key_id": str(row.get("id")), "name": row.get("name") or ""},
        owner_type="platform",
        severity="info",
    )
    return {"ok": True, "key": row}


@router.put("/config/api-keys/{key_id}")
async def update_api_key(key_id: int, data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    # 审计快照只需这一行的旧值，直接按 id 取；原实现 list_api_keys() 全量拉所有 key
    # （含明文）再线性找一条，每次改/删都把整张表搬一遍。
    old_row = await PostgresClient.get_api_key_by_id(key_id)
    try:
        row = await PostgresClient.update_api_key(key_id, data)
    except ValueError as exc:
        # 子 Key 编辑越权（白/黑名单/配额/过期超出父范围）→ 400 并带具体原因。
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not row:
        raise HTTPException(status_code=404, detail="key 不存在")
    await config.Config.refresh_api_keys_cache()
    row["rate_limit"] = json.loads(row["rate_limit"]) if isinstance(row["rate_limit"], str) else (row["rate_limit"] or {})
    await _log_operation(token, "update_api_key", "api_key", str(key_id), _scrub_relay_secret(old_row) if old_row else None, _scrub_relay_secret(row))
    from monkeycode_compat.notify_core import emit_notification_background
    emit_notification_background(
        "api_key.modified",
        params={"api_key_id": str(key_id), "name": row.get("name") or ""},
        owner_type="platform",
        severity="info",
    )
    return {"ok": True, "key": row}


@router.put("/config/api-keys/{key_id}/copy")
async def copy_api_key(key_id: int, data: dict | None = None, token: str = Header(None, alias="Authorization")):
    """按父子关系复制一枚 Key（副本）。

    副本继承父 Key 的限流/渠道/模型/策略/归属，`parent_id` 指向根 Key；用量按
    `api_key_id OR api_key_parent_id` 汇总回父 Key。仅签发新的明文，不复制父 Key 密文。
    """
    await _require_admin(token)
    try:
        row = await PostgresClient.copy_api_key(key_id, data or {})
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not row:
        raise HTTPException(status_code=404, detail="key 不存在")
    await config.Config.refresh_api_keys_cache()
    row["rate_limit"] = json.loads(row["rate_limit"]) if isinstance(row["rate_limit"], str) else (row["rate_limit"] or {})
    await _log_operation(token, "copy_api_key", "api_key", str(key_id), None, {"new_key_id": row.get("id"), "parent_id": row.get("parent_id")})
    return {"ok": True, "key": row}


@router.delete("/config/api-keys/{key_id}")
async def delete_api_key(key_id: int, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    old_row = await PostgresClient.get_api_key_by_id(key_id)
    ok = await PostgresClient.delete_api_key(key_id)
    if not ok:
        raise HTTPException(status_code=404, detail="key 不存在")
    await config.Config.refresh_api_keys_cache()
    await _log_operation(token, "delete_api_key", "api_key", str(key_id), _scrub_relay_secret(old_row) if old_row else None, None)
    return {"ok": True}


# ==================== API Key 速率使用 ====================

@router.get("/config/api-keys/usage")
async def get_api_keys_usage(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    from rate_limiter import RateLimiter
    keys = await PostgresClient.list_api_keys()
    # 并发取每一个 Key 的运行时限流余量 + 用量明细；原实现串行循环，N 个 key 就
    # 叠加 N 次 Redis + N 次 DB 往返，key 多时直接超时。
    async def _usage(k: dict) -> dict:
        usage = await RateLimiter.get_usage(k.get("key", ""))
        ledger_usage = await PostgresClient.api_key_usage_totals(
            k["id"], include_children=bool(k.get("parent_id") is None)
        )
        k["rate_limit"] = json.loads(k["rate_limit"]) if isinstance(k["rate_limit"], str) else (k["rate_limit"] or {})
        return {**k, "usage": usage, "ledger_usage": ledger_usage}
    return await asyncio.gather(*[_usage(k) for k in keys])


@router.get("/config/api-keys/{key_id}")
async def get_api_key_detail(key_id: int, token: str = Header(None, alias="Authorization")):
    """单个 API Key 全字段详情，供编辑弹窗按需拉取（列表已裁字段）。

    声明在 ``/config/api-keys/usage`` 之后，避免 ``/{key_id}`` 先把字面量
    ``usage`` 当 id 捕获。复用 ``get_api_key_by_id``（update/delete 也用它做
    审计快照），返回经 ``_api_key_row`` 归一化的全字段行。
    """
    await _require_admin(token)
    row = await PostgresClient.get_api_key_by_id(key_id)
    if row is None:
        raise HTTPException(status_code=404, detail="key 不存在")
    return {"ok": True, "key": row}


@router.put("/config/api-keys/{key_id}/disable")
async def disable_api_key(key_id: int, disabled: bool = True, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    row = await PostgresClient.toggle_api_key_disabled(key_id, disabled)
    if not row:
        raise HTTPException(status_code=404, detail="key 不存在")
    await config.Config.refresh_api_keys_cache()
    await _log_operation(token, "disable_api_key", "api_key", str(key_id), None, {"disabled": disabled})
    return {"ok": True}


# ==================== 模型路由 / 模型组 ====================


async def _routing_models_metadata(enabled_groups: dict | None = None) -> list[dict]:
    return await ModelClientPool.get_admin_model_route_options(enabled_groups)


async def _real_model_ids() -> set[str]:
    return {
        m.get("id") for m in await _routing_models_metadata()
        if m.get("id") and m.get("type") != "model_group" and m.get("owned_by") != "model-group"
    }


def _api_key_label(k: dict) -> str:
    name = (k.get("name") or "").strip()
    if name:
        return name
    key = k.get("key") or ""
    return f"{key[:8]}...{key[-4:]}" if len(key) > 12 else key


def _sanitize_model_group_provider_filters(group: dict, group_name: str):
    """校验模型组/方案的渠道标签白/黑名单格式：只去空白 + 去重，**不按存在性丢弃**。

    标签是运行时筛选维度——名单存的是"意图"，选路时按渠道当前 tags 现取现判
    （rate_limiter._select_and_reserve_candidates 与 Config.model_group_allows_provider）。
    故先建组、后给渠道打同义标签也能在运行时命中，无需重存组；反之先建组、后删
    渠道的某标签，选路时也会自然失效。
    """
    for fld in ("provider_whitelist", "provider_blacklist"):
        raw = group.get(fld)
        if raw is None:
            continue
        if not isinstance(raw, list):
            raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' {fld} 必须是数组")
        values: list[str] = []
        seen: set[str] = set()
        for value in raw:
            if not isinstance(value, str) or not value.strip():
                raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' {fld} 包含非法值")
            tag = value.strip()
            if tag not in seen:
                seen.add(tag)
                values.append(tag)
        group[fld] = values


def _validate_model_group_schemes(group: dict, group_name: str):
    """校验并清洗自定义模型的多套方案（schemes）。

    方案的**身份是 id**（不是 name）：id 必填、组内唯一——重复直接拒绝。name 只是展示标签，
    允许为空、允许重复、允许改名，都不影响 active_scheme 指向。每套方案 models 至少一个；
    白/黑名单走与顶层一致的标签格式校验（不按存在性丢弃）。active_scheme 若给出必须命中某方案 id。
    schemes 缺省（旧组）时不处理，由 normalize_model_group 用顶层字段合成。
    """
    raw = group.get("schemes")
    if raw is None:
        return
    if not isinstance(raw, list):
        raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' schemes 必须是数组")
    seen_ids: set[str] = set()
    for scheme in raw:
        if not isinstance(scheme, dict):
            raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' 方案必须是对象")
        scheme_id = str(scheme.get("id") or "").strip()
        if not scheme_id:
            raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' 方案缺少 id")
        if scheme_id in seen_ids:
            raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' 方案 id 重复: {scheme_id}")
        seen_ids.add(scheme_id)
        scheme_label = str(scheme.get("name") or "").strip() or scheme_id
        if _SCHEME_GROUP_SEP in scheme_label:
            raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' 方案名不能包含保留串 '{_SCHEME_GROUP_SEP}'")
        models = scheme.get("models", [])
        if not isinstance(models, list) or not [m for m in models if isinstance(m, str) and m.strip()]:
            raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' 方案 '{scheme_label}' 至少需要一个模型")
        scheme["id"] = scheme_id
        _sanitize_model_group_provider_filters(scheme, f"{group_name}/{scheme_label}")
    active_scheme = group.get("active_scheme")
    if active_scheme is not None:
        if not isinstance(active_scheme, str):
            raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' active_scheme 必须是字符串")
        active_scheme = active_scheme.strip()
        if active_scheme and active_scheme not in seen_ids:
            raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' active_scheme '{active_scheme}' 不是已定义的方案 id")
        group["active_scheme"] = active_scheme


async def _validate_model_groups(groups: dict):
    """校验自定义模型（model_groups）。

    允许组主名/别名与真实模型或其它组重名（收敛后由运行时按“组优先、创建时间”解析）。
    选择策略与 API Key 范围已移出组，这里不再处理。
    """
    if not isinstance(groups, dict):
        raise HTTPException(status_code=400, detail="model_groups.groups 必须是对象")

    real_model_ids: set[str] | None = None  # 惰性加载：仅当有组指定 metadata_model 时才拉取
    group_names = set(groups.keys())
    normalized_group_names = set()

    for group_name, group in groups.items():
        if not group_name or not isinstance(group_name, str):
            raise HTTPException(status_code=400, detail="模型组名称必填")
        normalized_group_name = group_name.strip()
        if not normalized_group_name:
            raise HTTPException(status_code=400, detail="模型组名称不能为空")
        if _SCHEME_GROUP_SEP in normalized_group_name:
            raise HTTPException(status_code=400, detail=f"模型组名称不能包含保留串 '{_SCHEME_GROUP_SEP}'")
        if normalized_group_name in normalized_group_names:
            raise HTTPException(status_code=400, detail=f"模型组名称重复: {normalized_group_name}")
        normalized_group_names.add(normalized_group_name)
        if not isinstance(group, dict):
            raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' 配置必须是对象")
        if str(group.get("kind") or "custom") == "real":
            continue
        members = group.get("models", [])
        has_top_models = isinstance(members, list) and [m for m in members if isinstance(m, str) and m.strip()]
        # 允许仅提供 schemes（不带顶层 models）：顶层字段由 normalize_model_group 从激活方案投影。
        schemes_raw = group.get("schemes")
        has_scheme_models = isinstance(schemes_raw, list) and any(
            isinstance(s, dict) and isinstance(s.get("models"), list)
            and [m for m in s.get("models") if isinstance(m, str) and m.strip()]
            for s in schemes_raw
        )
        if not has_top_models and not has_scheme_models:
            raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' 至少需要一个模型")
        aliases_raw = group.get("aliases", [])
        if aliases_raw is not None and not isinstance(aliases_raw, list):
            raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' aliases 必须是数组")
        if isinstance(aliases_raw, list):
            for alias in aliases_raw:
                if isinstance(alias, str) and _SCHEME_GROUP_SEP in alias:
                    raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' 别名不能包含保留串 '{_SCHEME_GROUP_SEP}'")
        backup_raw = group.get("backup_model_group")
        if backup_raw is None:
            backup_raw = group.get("backup_group")
        backup_name = ""
        if backup_raw is not None:
            if not isinstance(backup_raw, str):
                raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' backup_model_group 必须是字符串")
            backup_name = backup_raw.strip()
            if backup_name:
                if backup_name == normalized_group_name:
                    raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' backup_model_group 不能指向自身")
                if backup_name not in group_names:
                    raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' backup_model_group '{backup_name}' 不存在")
                # 防止备份链成环：沿备份链回溯，若发现目标已存在链中则拒绝
                seen = {normalized_group_name}
                cursor = backup_name
                while cursor and cursor in group_names:
                    if cursor in seen:
                        raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' backup_model_group 链中存在环: {', '.join(list(seen) + [cursor])}")
                    seen.add(cursor)
                    nxt = groups.get(cursor)
                    if not isinstance(nxt, dict):
                        break
                    nxt_raw = nxt.get("backup_model_group")
                    if nxt_raw is None:
                        nxt_raw = nxt.get("backup_group")
                    cursor = str(nxt_raw or "").strip()
            group["backup_model_group"] = backup_name
            group.pop("backup_group", None)
        metadata_model_raw = group.get("metadata_model")
        if metadata_model_raw is not None:
            if not isinstance(metadata_model_raw, str):
                raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' metadata_model 必须是字符串")
            metadata_model = metadata_model_raw.strip()
            if metadata_model:
                if real_model_ids is None:
                    real_model_ids = await _real_model_ids()
                if metadata_model not in real_model_ids:
                    raise HTTPException(status_code=400, detail=f"模型组 '{group_name}' metadata_model '{metadata_model}' 不是有效的真实模型")
            group["metadata_model"] = metadata_model
        _sanitize_model_group_provider_filters(group, group_name)
        _validate_model_group_schemes(group, group_name)


@router.get("/model-routing")
async def get_model_routing(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    provider_configs, api_keys, groups = await asyncio.gather(
        config.Config.get_providers(),
        PostgresClient.list_api_keys(),
        PostgresClient.list_model_groups(),
    )
    # /model-routing 只展示自定义路由组（kind=custom）；real 元数据行由 /model-metadata
    # 管理，不进路由页。list_model_groups 已为每行带上 kind+metadata，供前端区分。
    groups = {
        name: group for name, group in (groups or {}).items()
        if isinstance(group, dict) and str(group.get("kind") or "custom") != "real"
    }
    providers = []
    for name in _provider_names(provider_configs):
        pcfg = provider_configs.get(name, {})
        providers.append({
            "name": name,
            "remark": pcfg.get("remark") or "",
            "tags": _normalize_provider_tags(pcfg.get("tags")),
            "enabled": pcfg.get("enabled", True),
            "accounts": [acc.get("username") for acc in pcfg.get("accounts", []) if acc.get("username")],
            "models": _provider_models(name),
        })
    enabled_groups = {
        name: group for name, group in (groups or {}).items()
        if isinstance(group, dict) and group.get("enabled", True) and isinstance(group.get("models"), list) and group.get("models")
    }
    models = await _routing_models_metadata(enabled_groups)
    return {
        "model_groups": {"groups": groups},
        "providers": providers,
        "models": models,
        "api_keys": [{"id": k.get("id"), "name": k.get("name"), "label": _api_key_label(k), "disabled": k.get("disabled", False)} for k in api_keys],
    }


async def _after_metadata_write(name: str):
    """模型元数据变更：重载本实例目录、置脏 /v1/models 并广播具名事件。"""
    import model_catalog

    changed = await model_catalog.reload_from_db(reason=f"admin:metadata:{name}")
    if changed:
        ModelClientPool._mark_models_dirty()
    try:
        await runtime_sync.publish(runtime_sync.EVENT_METADATA, name)
    except Exception as exc:
        logger.warning(f"[admin] 广播 metadata/{name} 事件失败: {exc}")


async def _reload_provider_models_local(name: str):
    """渠道模型行变更：只重灌本渠道 Channel.models + 置脏 _models + 广播 model 事件。

    不再全局 refresh_models（打上游 + 全站重拉）；其它实例经 model 事件重灌该渠道。
    """
    try:
        rows = await PostgresClient.list_provider_models(name)
        ModelClientPool.reload_channel_models(name, rows)
    except Exception as exc:
        logger.warning(f"[admin] 重灌渠道 {name} 模型失败: {exc}")
    try:
        await runtime_sync.publish(runtime_sync.EVENT_MODEL, name)
    except Exception:
        pass


async def _after_model_routing_write(name: str, *, old_name: str | None = None):
    """模型组变更：重载本实例目录、置脏 /v1/models 并广播具名事件。"""
    import model_catalog

    changed = await model_catalog.reload_from_db(reason=f"admin:group:{name}")
    if changed:
        ModelClientPool._mark_models_dirty()
    extra = {"old_name": old_name} if old_name and old_name != name else None
    try:
        await runtime_sync.publish(runtime_sync.EVENT_GROUP, name, extra=extra)
    except Exception as exc:
        logger.warning(f"[admin] 广播 group/{name} 事件失败: {exc}")


async def _validate_model_group_write(name: str, group: dict) -> dict:
    # 这里只校验自定义路由组之间的关系。model_groups 表里还有 kind='real' 的真实模型
    # 元数据行，不能拿来套「至少一个模型 / 备用组链」等 custom 规则。
    groups = {
        group_name: item
        for group_name, item in (await PostgresClient.list_model_groups()).items()
        if isinstance(item, dict) and str(item.get("kind") or "custom") != "real"
    }
    groups.pop(name, None)
    candidate = dict(group or {})
    candidate["name"] = candidate.get("name") or name
    # /model-groups 只管自定义路由组（kind=custom）；real 元数据行由 /model-metadata
    # 专用入口管理。这里强制 custom 并丢弃 metadata，避免路由写入误改 real 行。
    candidate["kind"] = "custom"
    candidate.pop("metadata", None)
    groups[candidate["name"]] = candidate
    await _validate_model_groups(groups)
    return candidate


@router.post("/model-groups")
async def create_model_group(data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    name = (data or {}).get("name")
    if not isinstance(name, str) or not name.strip():
        raise HTTPException(status_code=400, detail="model group name required")
    name = name.strip()
    existing = await PostgresClient.get_model_group(name)
    if existing:
        if str(existing.get("kind") or "custom") == "real":
            raise HTTPException(
                status_code=400,
                detail=f"真实模型 '{name}' 不是自定义模型组；请在模型元数据页管理它",
            )
        raise HTTPException(status_code=409, detail="model group already exists")
    try:
        payload = await _validate_model_group_write(name, data)
        group = await PostgresClient.upsert_model_group(name, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    await _after_model_routing_write(group.get("name", name))
    await _log_operation(token, "create_model_group", "model_group", group.get("name", name), None, group)
    return {"ok": True, "model_group": group}


@router.put("/model-groups/{name}")
async def update_model_group(name: str, data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    old_group = await PostgresClient.get_model_group(name)
    if not old_group:
        raise HTTPException(status_code=404, detail="model group not found")
    if str(old_group.get("kind") or "custom") == "real":
        # real 元数据行不是自定义路由组；对它做整组 PUT 应走 /model-metadata，拒绝以免覆盖元数据。
        raise HTTPException(status_code=400, detail="真实模型元数据行请通过模型元数据接口编辑")
    new_name = (data or {}).get("name", name)
    if not isinstance(new_name, str) or not new_name.strip():
        raise HTTPException(status_code=400, detail="model group name required")
    new_name = new_name.strip()
    if new_name != name and await PostgresClient.get_model_group(new_name):
        raise HTTPException(status_code=409, detail="model group already exists")
    payload = dict(data or {})
    payload["name"] = new_name
    # 整组 PUT 是覆盖写。若调用方没带 schemes/active_scheme（例如只想改 enabled），
    # 回源现有值，避免用空 schemes 把 DB 里已有的多套方案整体覆盖成默认方案。
    if payload.get("schemes", None) is None:
        payload["schemes"] = old_group.get("schemes", [])
    if not str(payload.get("active_scheme") or "").strip():
        payload["active_scheme"] = old_group.get("active_scheme", "")
    try:
        payload = await _validate_model_group_write(name, payload)
        group = await PostgresClient.upsert_model_group(name, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    await _after_model_routing_write(group.get("name", new_name), old_name=name)
    await _log_operation(token, "update_model_group", "model_group", name, old_group, group)
    return {"ok": True, "model_group": group}


@router.delete("/model-groups/{name}")
async def delete_model_group(name: str, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    old_group = await PostgresClient.get_model_group(name)
    if not old_group:
        raise HTTPException(status_code=404, detail="model group not found")
    if str(old_group.get("kind") or "custom") == "real":
        raise HTTPException(status_code=400, detail="真实模型元数据行请通过模型元数据接口删除")
    deleted = await PostgresClient.delete_model_group(name)
    if not deleted:
        raise HTTPException(status_code=404, detail="model group not found")
    await _after_model_routing_write(name)
    await _log_operation(token, "delete_model_group", "model_group", name, old_group, None)
    return {"ok": True}


@router.put("/model-routing")
async def update_model_routing(data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    groups = data.get("model_groups", {}).get("groups", data.get("groups", {}))
    await _validate_model_groups(groups)

    existing_groups = await PostgresClient.list_model_groups()
    existing_group_names = set(existing_groups.keys())

    normalized_groups = {}
    for name, group in groups.items():
        # 只做标签名格式规范化（去空白/去重），不按渠道存在性丢弃：标签是运行时筛选维度，
        # 选路时按渠道当前 tags 现取现判。_validate_model_groups 已对 kind=custom 组做过
        # 同样的清洗，这里幂等重跑一次以覆盖它跳过的 kind=real 行。
        _sanitize_model_group_provider_filters(group, name)
        whitelist = group.get("provider_whitelist") or []
        blacklist = group.get("provider_blacklist") or []
        existing_group = existing_groups.get(name.strip(), {})
        backup_raw = group.get("backup_model_group")
        if backup_raw is None:
            backup_raw = group.get("backup_group")
        backup_name = str(backup_raw or "").strip() if backup_raw is not None else (existing_group.get("backup_model_group") or existing_group.get("backup_group") or "")
        normalized_groups[name.strip()] = {
            "enabled": group.get("enabled", existing_group.get("enabled", True)),
            "remark": group.get("remark", existing_group.get("remark", "")),
            "models": [m.strip() for m in group.get("models", []) if isinstance(m, str) and m.strip()],
            "aliases": [a.strip() for a in group.get("aliases", []) if isinstance(a, str) and a.strip()],
            "provider_whitelist": whitelist,
            "provider_blacklist": blacklist,
            "backup_model_group": backup_name,
            "response_model": (str(group.get("response_model")) if group.get("response_model") is not None else existing_group.get("response_model", "")).strip(),
            "metadata_model": (str(group.get("metadata_model")) if group.get("metadata_model") is not None else existing_group.get("metadata_model", "")).strip(),
            "schemes": group.get("schemes") if group.get("schemes") is not None else existing_group.get("schemes", []),
            "active_scheme": group.get("active_scheme") if group.get("active_scheme") is not None else existing_group.get("active_scheme", ""),
        }

    replace_groups = getattr(PostgresClient, "replace_model_groups", None)
    if callable(replace_groups):
        await replace_groups(normalized_groups)
    else:
        for name, group in normalized_groups.items():
            await PostgresClient.upsert_model_group(name, {"name": name, **group})
        for name in existing_group_names - set(normalized_groups.keys()):
            await PostgresClient.delete_model_group(name)

    await _after_model_routing_write("__all__")
    old_data = {"model_groups": {"groups": existing_groups}}
    new_data = {"model_groups": {"groups": normalized_groups}}
    await _log_operation(token, "update_model_routing", "model_routing", "model_routing", old_data, new_data)
    return {"ok": True, **new_data}


# ==================== Provider 查询接口 ====================


def _provider_names(provider_configs: dict | None = None) -> list[str]:
    names = set((provider_configs or {}).keys())
    names.update(ModelClientPool.get_provider_names())
    return sorted(names)


def _provider_models(name: str) -> list[str]:
    """返回该渠道的模型名列表（仅 id）。"""
    return list(dict.fromkeys(
        route.get("model_id") for route in ModelClientPool.get_provider_routes(name)
        if route.get("model_id")
    ))


def _build_provider_models_map() -> dict[str, list[str]]:
    """遍历各渠道 Channel.models，构建 {provider_name: [model_id, ...]} 映射。"""
    mapping: dict[str, list[str]] = {}
    for provider_name in ModelClientPool.get_provider_names():
        model_ids = [
            route.get("model_id")
            for route in ModelClientPool.get_provider_routes(provider_name)
            if route.get("model_id")
        ]
        if model_ids:
            mapping[provider_name] = model_ids
    return mapping


def _provider_base_response(name: str, cfg: dict) -> dict:
    chat_protocols = cfg.get("chat_protocols") or _normalize_chat_protocols(cfg)
    response = {
        "id": name,
        "name": name,
        "enabled": cfg.get("enabled", True),
        "rate_limit": {"concurrent_per_account": 0, **(cfg.get("rate_limit", {}) or {})},
        "billing_mode": cfg.get("billing_mode", "token"),
        "clear_conversation": cfg.get("clear_conversation", {}),
        "remark": cfg.get("remark", ""),
        "tags": _normalize_provider_tags(cfg.get("tags")),
        "website_url": cfg.get("website_url", ""),
        "icon": cfg.get("icon", ""),
        # 代码渠道的 Python 源码：编辑态要能回显才能改。仅 admin 门禁内可见，
        # 不进渠道列表摘要，避免大字段污染列表响应。
        "code": cfg.get("code", ""),
        "price_remark": cfg.get("price_remark", ""),
        "type": cfg.get("type", "custom" if _is_custom_provider_config(cfg) else "builtin"),
        "custom_channel": _is_custom_provider_config(cfg),
        "builtin_type": cfg.get("builtin_type", ""),
        "base_url": cfg.get("base_url"),
        "models_path": cfg.get("models_path"),
        "image_path": cfg.get("image_path"),
        "video_path": cfg.get("video_path"),
        "speech_path": cfg.get("speech_path"),
        "endpoint_configs": chat_protocols,
        "chat_protocols": chat_protocols,
        "chat_protocols_error": "" if chat_protocols else "未配置对话协议 chat_protocols",
        "supports_image_generation": cfg.get("supports_image_generation", False),
        "supports_video_generation": cfg.get("supports_video_generation", False),
        "supports_tts": cfg.get("supports_tts", False),
        "account_priority": cfg.get("account_priority", 0),
        "account_weight": cfg.get("account_weight", 0),
        "auto_update_models": cfg.get("auto_update_models", False),
        # 读取路径用宽松规范化：历史脏数据/坏正则不能让 GET 500，严格校验只在写入侧。
        "model_id_rewrite_rules": normalize_model_id_rewrite_rules(cfg.get("model_id_rewrite_rules")),
        "timeout": cfg.get("timeout", 120),
        "retry_count": cfg.get("retry_count"),
        "extra_retry_status_codes": _coerce_extra_retry_status_codes(cfg.get("extra_retry_status_codes")),
        "health_check": cfg.get("health_check", {}),
        "scheduled_test": cfg.get("scheduled_test", {}),
        "last_scheduled_test_at": ModelClientPool.last_scheduled_test_triggered_at(name),
        "models": _provider_models(name),
        "model_rows": ModelClientPool.get_provider_routes(name),
    }
    return response


def _memory_account_cooldown(client) -> dict | None:
    """Build the account-level cooldown dict from the in-memory freeze bucket.

    Mirrors the shape Redis used to return ({remaining, until, reason, permanent})
    so downstream consumers (_provider_accounts_response, _freeze_items, status
    endpoint) stay unchanged. Single-process deployment: the memory bucket is
    the authoritative runtime freeze state; Redis only seeds it on startup.
    """
    if client is None:
        return None
    now = time.time()
    acc = getattr(client, "_freeze_state", {}).get("account") or {}
    until = acc.get("until", 0)
    if until is None:
        remaining = None
        until_out = None
        permanent = True
    elif until and until > now:
        remaining = max(1, int(until - now))
        until_out = int(until)
        permanent = False
    else:
        # 过期或 0：内存桶里不算冻结，不进列表（与 Redis TTL<=0 不计冻结一致）。
        return None
    return {
        "remaining": remaining,
        "until": until_out,
        "reason": str(acc.get("reason") or ""),
        "permanent": permanent,
    }


def _memory_model_cooldowns(client) -> list[dict]:
    """Build the per-model cooldown list from the in-memory freeze bucket."""
    if client is None:
        return []
    now = time.time()
    out: list[dict] = []
    models = getattr(client, "_freeze_state", {}).get("models") or {}
    for model_id, entry in models.items():
        if not isinstance(entry, dict):
            continue
        until = entry.get("until", 0)
        if until is None:
            remaining = None
            until_out = None
            permanent = True
        elif until and until > now:
            remaining = max(1, int(until - now))
            until_out = int(until)
            permanent = False
        else:
            continue
        out.append({
            "model": str(model_id),
            "remaining": remaining,
            "until": until_out,
            "reason": str(entry.get("reason") or ""),
            "permanent": permanent,
        })
    return out


def _freeze_items(
    account_status: dict | None = None,
    model_statuses: list[dict] | None = None,
) -> list[dict]:
    """Return structured freeze entries; presentation labels belong to clients."""
    items: list[dict] = []
    if account_status:
        items.append({
            "scope": "account",
            "model": None,
            "reason": str(account_status.get("reason") or ""),
            "remaining": account_status.get("remaining"),
            "until": account_status.get("until"),
            "permanent": bool(account_status.get("permanent")),
        })
    for status in model_statuses or []:
        if not isinstance(status, dict):
            continue
        items.append({
            "scope": "account_model",
            "model": str(status.get("model") or ""),
            "reason": str(status.get("reason") or ""),
            "remaining": status.get("remaining"),
            "until": status.get("until"),
            "permanent": bool(status.get("permanent")),
        })
    return items


async def _provider_accounts_response(name: str, cfg: dict) -> list[dict]:
    pool = ModelClientPool.get_provider_pool(name)
    clients = pool.clients if pool else []
    client_by_username = {c.username: c for c in clients}
    rate_limit = cfg.get("rate_limit", {}) or {}
    effective_policy = await get_effective_provider_policy(name)
    channel_rpd_limit = int(effective_policy.get("account_rpd") or 0)
    channel = getattr(pool, "channel", None) if pool else None
    channel_priority = getattr(channel, "priority", cfg.get("account_priority", cfg.get("priority", 0)))
    channel_weight = getattr(channel, "weight", cfg.get("account_weight", cfg.get("weight", 1)))

    # 批量预取 RPD 已用值（Redis 固定窗口，账号级）。account_key 与 LimitSubject.account_key 一致。
    rpd_account_keys = [f"account:{name}:{u}" for u in client_by_username.keys()]
    rpd_used_map = await RedisLimitBackend.batch_get_account_rpd(rpd_account_keys)
    # 预占式计量的权威用量来自 reservation ledger：失败请求已回滚，因此这里读到的是
    # 真实占额。本地 _requests/_token_usages 只是选路评分信号（预占即 append、失败不回退），
    # 不能作为面板口径；仅在 ledger 无数据（未配限额 / Redis 不可用）时兜底。
    # 每账号一条 pipeline 且彼此独立：并发发出，把 N 次串行 RTT 压成≈1 次墙钟耗时
    # （账号多的渠道详情页原先在这里逐账号等一个来回，是主要卡点）。
    usernames = list(client_by_username.keys())
    ledger_results = await asyncio.gather(*[
        RedisLimitBackend.batch_get_account_window_usage(
            f"account:{name}:{u}", metrics=("rpm", "rpd", "tpm")
        )
        for u in usernames
    ])
    ledger_usage_map: dict[str, dict[str, int]] = {
        u: (res or {}) for u, res in zip(usernames, ledger_results)
    }
    # 冻结改纯内存：每账号的 cooldown / model_cooldowns 直接从 _freeze_state 取，
    # 替代原先两次 Redis 批量扫描。rpd / ledger 仍读 Redis（用量只存 Redis）。
    account_cooldowns_map: dict[str, dict] = {
        u: cd for u, c in client_by_username.items()
        if (cd := _memory_account_cooldown(c)) is not None
    }
    model_cooldowns_map: dict[str, list[dict]] = {
        u: _memory_model_cooldowns(c) for u, c in client_by_username.items()
    }

    accounts = []
    seen_usernames: set[str] = set()
    proxies = _read_proxies()
    proxy_url_by_ref: dict[str, str] = {}
    proxy_mode_by_ref: dict[str, str] = {}
    for proxy in proxies:
        proxy_url = (proxy.get("url") or "").strip()
        mode = proxy_mode(proxy)
        for ref in (proxy.get("id"), proxy.get("name"), proxy.get("url")):
            ref_value = (ref or "").strip()
            if ref_value:
                proxy_url_by_ref[ref_value] = proxy_url
                proxy_mode_by_ref[ref_value] = mode
    for acc in cfg.get("accounts", []):
        if not isinstance(acc, dict):
            continue
        username = (acc.get("username") or "").strip()
        if not username or username in seen_usernames:
            continue
        seen_usernames.add(username)

        account_info = dict(acc)
        proxy_ref = (account_info.get("proxy_id") or account_info.get("proxy") or "").strip()
        account_info["proxy_url"] = proxy_url_by_ref.get(proxy_ref, (account_info.get("proxy") or "").strip())
        account_info["proxy_mode"] = proxy_mode_by_ref.get(proxy_ref, "network")
        client = client_by_username.get(username)
        if client is None:
            account_info.update({
                "username": username,
                "switch": acc.get("switch") is not False,
                "auth": False,
                "auth_checked_at": None,
                "auth_error": None,
                "rpm_limit": acc.get("rpm_limit", rate_limit.get("rpm_per_account", rate_limit.get("requests_per_minute_per_account", 0))),
                "tpm_limit": acc.get("tpm_limit", rate_limit.get("tpm_per_account", 0)),
                "concurrent_limit": acc.get("concurrent_limit", acc.get("concurrent_per_account", rate_limit.get("concurrent_per_account", 0))),
                "concurrent_used": 0,
                "rpm_used": 0,
                "rpm_available": 0,
                "tpm_used": 0,
                "tpm_available": acc.get("tpm_limit", rate_limit.get("tpm_per_account", 0)),
                "rpd_limit": channel_rpd_limit,
                "rpd_used": 0,
                "rpd_available": channel_rpd_limit,
                "disabled": True,
                "disable_reason": "账号开关已关闭" if acc.get("switch") is False else "账号未加载",
                "state": "disabled",
                "priority": channel_priority,
                "weight": channel_weight,
                "cooldown": False,
                "cooldown_reason": "",
                "cooldown_remaining": 0,
                "cooldown_until": 0,
                "cooldown_scope": "none",
                "model_cooldowns": [],
                "freeze_items": [],
            })
            accounts.append(account_info)
            continue

        now = time.time()
        local_rpm = len([t for t in client._requests if now - t < 60])
        local_tpm = sum(n for t, n in client._token_usages if now - t < 60)
        ledger = ledger_usage_map.get(client.username) or {}
        # 优先用 reservation ledger 的权威 used；只有结果里完全没有该 metric（Redis 不可用）
        # 才回退本地信号。0 是有效权威值（可能刚把失败请求回滚成 0），不能用 ``or`` 覆盖。
        rpm_used = int(ledger["rpm"]) if "rpm" in ledger else local_rpm
        tpm_used = int(ledger["tpm"]) if "tpm" in ledger else local_tpm
        rpd_used = (
            int(ledger["rpd"])
            if "rpd" in ledger
            else rpd_used_map.get(f"account:{name}:{client.username}", 0)
        )
        # Redis 仅作冻结兜底显示；运行中的冻结真相是 AccountClient 内存桶。
        cooldown_info = account_cooldowns_map.get(client.username)
        cooldown_remaining = int(cooldown_info.get("remaining") or 0) if cooldown_info else 0
        cooldown_reason = str(cooldown_info.get("reason") or "") if cooldown_info else ""
        account_cooldown = bool(client.is_frozen) or cooldown_remaining > 0
        # 兼容 Redis 回灌尚未完成的瞬间：不要用空 Redis 结果覆盖内存冻结。
        model_cooldowns = model_cooldowns_map.get(client.username, [])
        memory_models = getattr(client, "_freeze_state", {}).get("models", {}) or {}
        # Redis is the fallback for the runtime freeze mirror; expose one stable
        # structured list even during the short memory/Redis synchronization gap.
        model_cooldown_by_name = {
            str(item.get("model")): item
            for item in model_cooldowns
            if isinstance(item, dict) and item.get("model")
        }
        for model_id, entry in memory_models.items():
            if model_id in model_cooldown_by_name:
                continue
            until = entry.get("until", 0) if isinstance(entry, dict) else 0
            if until is not None and (not until or until <= now):
                continue
            remaining = None if until is None else max(1, int(until - now))
            model_cooldowns.append({
                "model": model_id,
                "remaining": remaining,
                "until": None if until is None else int(until),
                "reason": str(entry.get("reason") or "") if isinstance(entry, dict) else "",
                "permanent": until is None,
            })
        has_model_cooldown = bool(model_cooldowns) or bool(memory_models)
        memory_remaining = int(client.cooldown_remaining() or 0)
        account_status = cooldown_info
        if account_cooldown and account_status is None:
            account_status = {
                "remaining": None if memory_remaining <= 0 else memory_remaining,
                "until": None if memory_remaining <= 0 else int(now + memory_remaining),
                "reason": client.freeze_reason(),
                "permanent": memory_remaining <= 0,
            }
        freeze_items = _freeze_items(account_status, model_cooldowns)
        if account_cooldown and has_model_cooldown:
            cooldown_scope = "mixed"
        elif account_cooldown:
            cooldown_scope = "account"
        elif has_model_cooldown:
            cooldown_scope = "model"
        else:
            cooldown_scope = "none"

        account_info.update({
            "username": username,
            "switch": acc.get("switch") is not False,
            "auth": client.auth_ok,
            "auth_checked_at": client.auth_checked_at,
            "auth_error": client.auth_error,
            "rpm_limit": client.rpm_limit,
            "tpm_limit": client.tpm_limit,
            "concurrent_limit": client.concurrent_limit,
            "concurrent_used": client.concurrent_used,
            "rpm_used": rpm_used,
            "rpm_available": max(0, client.rpm_limit - rpm_used),
            "tpm_used": tpm_used,
            "tpm_available": max(0, client.tpm_limit - tpm_used) if client.tpm_limit > 0 else 0,
            "rpd_limit": client.rpd_limit,
            "rpd_used": rpd_used,
            "rpd_available": max(0, client.rpd_limit - rpd_used) if client.rpd_limit > 0 else 0,
            "disabled": client.disabled,
            "disable_reason": client.disable_reason,
            "priority": client.priority,
            "weight": client.weight,
            "cooldown": account_cooldown or has_model_cooldown,
            "state": client.state().value,
            "is_frozen": bool(client.is_frozen),
            "cooldown_reason": cooldown_reason or client.freeze_reason(),
            "cooldown_remaining": cooldown_remaining if cooldown_remaining > 0 else 0,
            "cooldown_until": int(now + cooldown_remaining) if cooldown_remaining > 0 else 0,
            "cooldown_scope": cooldown_scope,
            "model_cooldowns": model_cooldowns,
            "freeze_items": freeze_items,
        })

        provider = client.provider
        if hasattr(provider, 'access_token') and hasattr(provider, '_access_token_expires_at'):
            account_info["has_access_token"] = bool(provider.access_token)
            account_info["token_expires_at"] = provider._access_token_expires_at
        elif getattr(provider, 'access_token', None):
            account_info["has_access_token"] = True
        token_expires_at_ms = getattr(provider, 'access_token_expires_at_ms', None)
        if token_expires_at_ms:
            try:
                account_info["token_expires_at"] = float(token_expires_at_ms) / 1000
            except (TypeError, ValueError):
                pass
        if hasattr(provider, 'cookies') and provider.cookies:
            account_info["has_cookies"] = True

        accounts.append(account_info)
    return accounts


def _configured_account_usernames(cfg: dict) -> list[str]:
    usernames: list[str] = []
    seen: set[str] = set()
    for account in cfg.get("accounts", []):
        if not isinstance(account, dict):
            continue
        username = str(account.get("username") or "").strip()
        if username and username not in seen:
            seen.add(username)
            usernames.append(username)
    return usernames


async def _provider_lite_accounts_response(name: str, cfg: dict) -> list[dict]:
    usernames = _configured_account_usernames(cfg)
    accounts_by_username: dict[str, dict] = {}
    for account in cfg.get("accounts", []):
        if not isinstance(account, dict):
            continue
        username = str(account.get("username") or "").strip()
        if username and username not in accounts_by_username:
            accounts_by_username[username] = account
    # 冻结改纯内存：_freeze_state 是单进程权威，不再 batch 读 Redis。
    pool = ModelClientPool.get_provider_pool(name)
    client_by_username = {c.username: c for c in (pool.clients if pool else [])}
    result = []
    for username in usernames:
        client = client_by_username.get(username)
        memory_frozen = bool(getattr(client, "is_frozen", False)) or bool(
            _memory_account_cooldown(client)
        ) or bool(_memory_model_cooldowns(client))
        switch_on = accounts_by_username[username].get("switch") is not False
        if client is None or not switch_on:
            state = AccountState.DISABLED.value
        elif memory_frozen:
            state = AccountState.COOLING.value
        else:
            state = client.state().value
        result.append({
            "username": username,
            "switch": switch_on,
            "cooldown": memory_frozen,
            "state": state,
        })
    return result


def _provider_copy_accounts_response(cfg: dict) -> list[dict]:
    accounts_by_username: dict[str, dict] = {}
    for account in cfg.get("accounts", []):
        if not isinstance(account, dict):
            continue
        username = str(account.get("username") or "").strip()
        if username and username not in accounts_by_username:
            accounts_by_username[username] = account
    return [
        {
            "username": username,
            "password": account.get("password"),
            "api_key": account.get("api_key"),
            "key": account.get("key"),
        }
        for username, account in accounts_by_username.items()
    ]


def _provider_account_status_response(
    name: str,
    account_cooldowns: dict[str, dict] | None = None,
    model_cooldowns: dict[str, list[dict]] | None = None,
) -> dict:
    account_cooldowns = account_cooldowns or {}
    model_cooldowns = model_cooldowns or {}
    usernames = sorted(set(account_cooldowns.keys()) | set(model_cooldowns.keys()))
    accounts = []
    for username in usernames:
        account_status = account_cooldowns.get(username)
        model_statuses = model_cooldowns.get(username, []) or []
        accounts.append({
            "username": username,
            "cooldown": True,
            "cooldown_scope": "mixed" if account_status and model_statuses else ("account" if account_status else "model"),
            "cooldown_reason": str((account_status or {}).get("reason") or ""),
            "cooldown_remaining": int((account_status or {}).get("remaining") or 0),
            "cooldown_until": int((account_status or {}).get("until") or 0),
            "account_cooldown": account_status,
            "model_cooldowns": model_statuses,
            "freeze_items": _freeze_items(account_status, model_statuses),
        })
    return {"ok": True, "provider": name, "accounts": accounts}


# 可用性诊断标签：把 LimitDecision.reason 映射为中文说明（与 rate_limiter._format_skip_reasons 口径一致）。
# 仅收录“账号静态状态”维度；请求级维度（excluded_account/selection_returned_none/
# reservation_denied/concurrent_limit/redis_uncertain）刻意不在此视图出现——它们是重试排除或
# 预占瞬时结果，不是账号此刻的可用性，展示会误导。
_AVAILABILITY_REASON_LABELS = {
    "available": "可用",
    "provider_filtered": "模型组 provider 白/黑名单过滤",
    "no_model_route": "该模型未在此渠道注册路由",
    "account_disabled": "账号已禁用",
    "account_frozen": "账号被持久冻结",
    "channel_disabled": "渠道已禁用",
    "provider_not_initialized": "账号尚未完成初始化",
    "account_cooldown": "账号处于冷却期",
    "account_model_cooldown": "账号的当前模型处于冷却期",
    "provider_quota_cooldown": "上游配额处于冷却期",
    "provider_daily_quota_exhausted": "上游每日配额耗尽",
    "provider_quota_exhausted": "上游配额耗尽",
    "provider_message_quota_exceeded": "消息级上游配额不足",
}


async def _provider_accounts_availability(name: str, cfg: dict, model_id: str) -> dict:
    """按目标模型 / 模型组诊断该渠道每个账号此刻能否被路由选中。

    只读：复用 ``LimitManager.account_available_locked``（账号级权威判定，不 reserve、不占并发）
    + 模型组 provider 白/黑名单复判（account_available_locked 覆盖不到的一层，发生在候选收集时的
    账号循环之前）。model 可传真实模型名或模型组名；组则遍历成员模型取“任一可用即可用”。
    """
    from limits.manager import LimitManager

    pool = ModelClientPool.get_provider_pool(name)
    clients = pool.clients if pool else []
    channel = getattr(pool, "channel", None) if pool else None

    is_group = await config.Config.is_model_group(model_id)
    if is_group:
        member_models = await config.Config.get_model_group_models(model_id)
        group_whitelist, group_blacklist = await config.Config.get_model_group_provider_filter(model_id)
    else:
        member_models = [model_id]
        group_whitelist, group_blacklist = set(), set()

    # provider 级过滤（对齐 _collect_candidates 顺序，只做静态维度）
    provider_status: str | None = None
    if group_whitelist and name not in group_whitelist:
        provider_status = "provider_filtered"
        provider_reason = "模型组白名单未包含该 provider"
    elif group_blacklist and name in group_blacklist:
        provider_status = "provider_filtered"
        provider_reason = "模型组黑名单已排除该 provider"
    else:
        # 成员模型是否在该渠道注册了路由（channel.models 命中）
        registered = bool(channel) and any(
            row.get("model_id") in member_models for row in channel.models
        )
        if not registered:
            provider_status = "no_model_route"
            provider_reason = "该模型未在此渠道注册路由"

    accounts: list[dict] = []
    for client in clients:
        if provider_status is not None:
            accounts.append({
                "username": client.username,
                "status": provider_status,
                "reason": provider_reason,
                "retry_after": 0,
                "cooldown_scope": "none",
            })
            continue

        # 账号级：组内成员“任一可用即可用”，否则取最后一个成员的拒绝原因作为代表。
        decision = None
        for member in member_models:
            decision = await LimitManager.account_available_locked(
                client, member, messages=None, is_test=False
            )
            if decision.allowed:
                break
        if decision is not None and decision.allowed:
            status = "available"
            reason = _AVAILABILITY_REASON_LABELS["available"]
            retry_after = 0
        else:
            reason_key = (decision.reason if decision else "") or "unavailable"
            status = reason_key
            reason = _AVAILABILITY_REASON_LABELS.get(reason_key, reason_key)
            retry_after = int(getattr(decision, "retry_after", 0) or 0) if decision else 0
        scope = "model" if status == "account_model_cooldown" else ("account" if status == "account_cooldown" else "none")
        accounts.append({
            "username": client.username,
            "status": status,
            "reason": reason,
            "retry_after": retry_after,
            "cooldown_scope": scope,
        })

    return {
        "ok": True,
        "provider": name,
        "model": model_id,
        "is_group": is_group,
        "member_models": member_models,
        "accounts": accounts,
    }


def _provider_lite_response(name: str, cfg: dict, models: list[str]) -> dict:
    return {
        "id": name,
        "name": name,
        "remark": cfg.get("remark", ""),
        "tags": _normalize_provider_tags(cfg.get("tags")),
        "enabled": cfg.get("enabled", True),
        "custom_channel": _is_custom_provider_config(cfg),
        "builtin_type": cfg.get("builtin_type", ""),
        "protocol": cfg.get("protocol"),
        "models": models,
    }


def _provider_summary_response(name: str, cfg: dict, models: list[str] | None = None) -> dict:
    pool = ModelClientPool.get_provider_pool(name)
    clients = pool.clients if pool else []
    accounts_cfg = cfg.get("accounts", []) or []
    enabled_accounts = [a for a in accounts_cfg if a.get("switch") is not False]
    # 冻结计数改纯内存：AccountClient._freeze_state 是单进程部署下的权威运行态，
    # 不再为列表徽章对全部渠道做 Redis 全量扫描。账号级 is_frozen 或任一模型级
    # 冻结的账号都计入，与原先 (account_cooldowns ∪ model_cooldowns) 同口径。
    now = time.time()
    frozen_usernames: set[str] = set()
    for c in clients:
        if bool(getattr(c, "is_frozen", False)):
            frozen_usernames.add(c.username)
            continue
        models_state = getattr(c, "_freeze_state", {}).get("models") or {}
        for mid, entry in models_state.items():
            if not isinstance(entry, dict):
                continue
            until = entry.get("until", 0)
            if until is None or (until and until > now):
                frozen_usernames.add(c.username)
                break
    cooldown_account_count = len(frozen_usernames)
    # 认证口径与账号明细（/accounts 的 auth 字段）统一到 client.auth_ok 真实体检结果，
    # 而非 is_init()（后者只表示"凭据字段存在"，token/cookie 过期时仍为 True，会让
    # 徽章误报"正常"）。auth_ok: True=认证通过 / False=认证失败 / None=尚未体检。
    # 只统计已启用（未 switch off）账号，避免禁用账号污染徽章判断。
    active_clients = [c for c in clients if not getattr(c, "disabled", False)]
    auth_account_count = sum(1 for c in active_clients if getattr(c, "auth_ok", None) is True)
    auth_failed_account_count = sum(1 for c in active_clients if getattr(c, "auth_ok", None) is False)
    checking_account_count = sum(1 for c in active_clients if getattr(c, "auth_ok", None) is None)
    # 渠道能否自动刷新 token：可自动刷新的渠道账号过期只是暂时状态（定时任务/下次
    # 请求会自愈），前端显示"已过期"（黄）；只能人工更新凭据的渠道显示"异常"（红）。
    supports_token_auto_refresh = any(
        getattr(c.provider, "SUPPORTS_TOKEN_AUTO_REFRESH", False) for c in clients
    )
    return {
        "id": name,
        "name": name,
        "enabled": cfg.get("enabled", True),
        "remark": cfg.get("remark", ""),
        "tags": _normalize_provider_tags(cfg.get("tags")),
        "website_url": cfg.get("website_url", ""),
        "icon": cfg.get("icon", ""),
        "type": cfg.get("type", "custom" if _is_custom_provider_config(cfg) else "builtin"),
        "custom_channel": _is_custom_provider_config(cfg),
        "builtin_type": cfg.get("builtin_type", ""),
        "protocol": cfg.get("protocol"),
        "base_url": cfg.get("base_url"),
        "account_count": len(accounts_cfg),
        "enabled_account_count": len(enabled_accounts),
        "auth_account_count": auth_account_count,
        "auth_failed_account_count": auth_failed_account_count,
        "checking_account_count": checking_account_count,
        "supports_token_auto_refresh": supports_token_auto_refresh,
        "requesting_account_count": sum(1 for c in clients if getattr(c, "_in_flight", 0) > 0),
        "cooldown_account_count": cooldown_account_count,
        "disabled_account_count": len(accounts_cfg) - len(enabled_accounts),
        "retry_count": cfg.get("retry_count"),
        "extra_retry_status_codes": _coerce_extra_retry_status_codes(cfg.get("extra_retry_status_codes")),
        "updated_at_ts": _provider_updated_at_ts(cfg),
        "models": models if models is not None else _provider_models(name),
    }


@router.get("/providers")
async def list_providers(
    lite: bool = False,
    token: str = Header(None, alias="Authorization"),
):
    await _require_admin(token)
    provider_configs = await config.Config.get_providers()
    # 仅返回已在 Postgres 持久化（即真正“已创建”）的渠道。
    # 不要并入 ModelClientPool 运行时注册的内置渠道模板，否则未创建的渠道也会出现在列表中。
    names = sorted((provider_configs or {}).keys())
    models_map = _build_provider_models_map()
    if lite:
        return [
            _provider_lite_response(name, provider_configs.get(name, {}), models_map.get(name, []))
            for name in names
        ]
    # 冻结计数走每个渠道的内存 _freeze_state（见 _provider_summary_response），
    # 不再对全部渠道做 Redis 全量扫描——单进程下内存即权威，Redis 仅启动回灌用。
    return [
        _provider_summary_response(
            name, provider_configs.get(name, {}),
            models=models_map.get(name, []),
        )
        for name in names
    ]


@router.delete("/providers/{name}")
async def delete_provider(name: str, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cfg = await _read_provider_config(name)

    main_cfg = await _read_json_async(CONFIG_FILE)
    old_data = {
        "provider": cfg,
        "models": await PostgresClient.list_provider_models(name),
        "model_routes": main_cfg.get("model_routes", {}),
        "model_groups": main_cfg.get("model_groups", {}),
    }
    _write_delete_backup("provider", name, old_data)
    routing_changed = _remove_provider_from_routing_config(main_cfg, name)
    if routing_changed:
        await _write_json_async(CONFIG_FILE, main_cfg)
    api_key_filters_changed = await PostgresClient.remove_provider_from_api_key_filters(name)
    if api_key_filters_changed:
        await config.Config.refresh_api_keys_cache()

    await CONFIG_STORE.delete_provider_async(name)
    await _publish_channel_event(name)
    await _remove_provider_runtime(name)
    await _log_operation(token, "delete_provider", "provider", name, old_data, None)
    return {"ok": True}


@router.get("/providers/{name}/base")
async def get_provider_base_config(name: str, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cfg = await _read_provider_base_config(name)
    return _provider_base_response(name, cfg)


def _normalize_account_schema(name: str, cfg: dict, schema: dict | None) -> dict:
    base = {
        "provider_name": name,
        "display_name": cfg.get("remark") or name,
        "add_methods": ["manual_form"],
        "fields": [],
        "auth_start": {"enabled": False},
        "add_guidance": "",
        "metadata_badges": [],
    }
    if isinstance(schema, dict):
        base.update(schema)
    override = cfg.get("account_schema")
    if isinstance(override, dict):
        base.update(override)
    base["provider_name"] = name
    base["display_name"] = base.get("display_name") or cfg.get("remark") or name
    if not isinstance(base.get("add_methods"), list):
        base["add_methods"] = ["manual_form"]
    base["add_methods"] = [str(m) for m in base.get("add_methods") or ["manual_form"] if m]
    fields = []
    for field in base.get("fields") or []:
        if not isinstance(field, dict) or not field.get("key"):
            continue
        fields.append({
            "type": "text",
            "required": False,
            "readonly": False,
            "secret": False,
            "span": 1,
            **field,
            "key": str(field.get("key")),
            "label": str(field.get("label") or field.get("key")),
        })
    base["fields"] = fields
    if not isinstance(base.get("auth_start"), dict):
        base["auth_start"] = {"enabled": False}
    # completion 归一化：渠道声明「授权怎么才算完成」，前端据此决定 UI 形态。
    #   poll     —— 纯轮询：用户登录完，服务端轮询到结果自动认领（如 GitHub 设备码）
    #   callback —— 上游回调服务端 redirect_uri，无需用户回粘任何地址
    #   loopback —— 上游只回调 127.0.0.1:{port}（桌面上游模型）：同机部署回调直接命中；
    #               跨机时浏览器停在打不开的本机地址，用户把地址栏 URL 粘回来补投
    # 未声明时按 mode + loopback 钩子推导默认值，存量渠道零改动。
    auth_start = base["auth_start"]
    if not str(auth_start.get("completion") or "").strip():
        mode = str(auth_start.get("mode") or "").strip().lower()
        if mode in ("oauth_callback", "callback", "popup"):
            auth_start["completion"] = "callback"
        elif mode == "device_code" and _schema_declares_loopback(auth_start):
            auth_start["completion"] = "loopback"
        else:
            auth_start["completion"] = "poll"
    if not isinstance(base.get("metadata_badges"), list):
        base["metadata_badges"] = []
    return base


def _schema_declares_loopback(auth_start: dict) -> bool:
    """schema 里有没有 loopback 声明：显式 completion='loopback' 或 loopback.enabled。"""
    if str(auth_start.get("completion") or "").strip().lower() == "loopback":
        return True
    loopback = auth_start.get("loopback")
    return isinstance(loopback, dict) and bool(loopback.get("enabled"))


def _provider_account_schema(name: str, cfg: dict) -> dict:
    provider_class = _get_provider_class(name, cfg)
    schema = None
    try:
        schema_fn = getattr(provider_class, "account_schema", None)
        if schema_fn:
            schema = schema_fn()
    except Exception as e:
        logger.warning("获取渠道 %s 账号 schema 失败: %s", name, e)
    return _normalize_account_schema(name, cfg, schema)


def _provider_display_name(provider_class, fallback: str) -> str:
    """渠道的中文展示名：spec 的 ``account_schema()["display_name"]``，取不到回落渠道 id。

    回调落地页给用户看，显示「华为云 CodeArts Agent」比内部 id ``codearts-agent`` 可读。
    schema 求值可能抛（作者写挂了），失败不能影响回调结果渲染。
    """
    try:
        schema_fn = getattr(provider_class, "account_schema", None)
        if schema_fn:
            display = (schema_fn() or {}).get("display_name")
            if isinstance(display, str) and display.strip():
                return display.strip()
    except Exception as e:  # noqa: BLE001 - 展示名取不到不算错，回落 id
        logger.debug(f"[admin] display_name 取用失败 provider={fallback}: {e}")
    return fallback


_AUTH_CALLBACK_AUTO_CLOSE_SECONDS = 60


def _account_auth_callback_html(ok: bool | None, message: str, provider_name: str, username: str = "") -> str:
    """授权回调落地页：结果详情 + 返回首页按钮，成功页倒计时结束才自动关窗。

    这一页要能独立看懂——跨机手工补投、或把回调地址直接粘进浏览器时没有父页，只有它。
    故不再 900ms 闪关（用户只看到窗口一闪而过），改为展示渠道/账号/结果后留
    ``_AUTH_CALLBACK_AUTO_CLOSE_SECONDS`` 秒倒计时；失败页不自动关，否则错误一闪即失。

    ``ok=None`` = 中性页（没有进行中的授权 / 无人认领这条回调）。这**不是**对任何一个
    会话的判决，所以不 postMessage：父页收到 ok:false 会当成"本次授权失败"停掉轮询，
    把一个还能靠 ticket 轮询救回来的会话打死。标题也不写"失败"。

    ``message`` 可能带上游原文（渠道 spec 的 error 串），一律经 JSON + textContent 注入，
    并把 ``<`` 转义成 ``\\u003c``，防 ``</script>`` 提前闭合脚本块。
    """
    neutral = ok is None
    payload = {"ok": bool(ok), "provider": provider_name, "username": username,
               "message": message, "notify": not neutral}
    payload_json = json.dumps(payload, ensure_ascii=False).replace("<", "\\u003c")
    title = "没有进行中的授权" if neutral else ("授权成功" if ok else "授权失败")
    color = "#64748b" if neutral else ("#16a34a" if ok else "#dc2626")
    countdown = _AUTH_CALLBACK_AUTO_CLOSE_SECONDS if ok else 0
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
:root {{ color-scheme: light dark; }}
body {{ margin: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 24px; background: #f8fafc; color: #0f172a; font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }}
.card {{ width: 100%; max-width: 420px; padding: 28px; border: 1px solid #e2e8f0; border-radius: 12px; background: #fff; box-shadow: 0 1px 3px rgba(15, 23, 42, .08); }}
h1 {{ margin: 0 0 18px; font-size: 20px; color: {color}; }}
dl {{ margin: 0 0 16px; display: grid; grid-template-columns: auto 1fr; gap: 6px 14px; font-size: 13px; }}
dt {{ color: #64748b; }} dd {{ margin: 0; word-break: break-all; }}
.msg {{ margin: 0 0 22px; font-size: 14px; line-height: 1.6; color: #475569; }}
.row {{ display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }}
button {{ font: inherit; font-size: 14px; padding: 8px 18px; border: 0; border-radius: 8px; background: #0f172a; color: #fff; cursor: pointer; }}
.hint {{ font-size: 12px; color: #94a3b8; }}
@media (prefers-color-scheme: dark) {{
  body {{ background: #0b1120; color: #e2e8f0; }}
  .card {{ background: #111a2e; border-color: #1e293b; }}
  .msg {{ color: #94a3b8; }} button {{ background: #e2e8f0; color: #0f172a; }}
}}
</style>
</head>
<body>
  <div class="card">
    <h1>{title}</h1>
    <dl id="meta">
      <dt>渠道</dt><dd id="provider"></dd>
    </dl>
    <p class="msg" id="message"></p>
    <div class="row">
      <button id="back" type="button">返回首页</button>
      <span class="hint" id="hint"></span>
    </div>
  </div>
  <script>
    const payload = {payload_json};
    const countdown = {countdown};
    if (payload.provider) {{
      document.getElementById('provider').textContent = payload.provider;
    }} else {{
      document.getElementById('meta').remove();   // 中性页没有具体渠道，别显示占位行
    }}
    document.getElementById('message').textContent = payload.message || '';
    if (payload.username) {{
      const dt = document.createElement('dt');
      dt.textContent = '账号';
      const dd = document.createElement('dd');
      dd.textContent = payload.username;
      document.getElementById('meta').append(dt, dd);
    }}
    // 只有对某个会话的真实判决才通知父页（notify=false 是中性页：无人认领 / 没有进行中
    // 的授权）。父页收到 ok:false 会停掉状态轮询，中性页若也发就会打死正常会话。
    // 注意 localhost 与 127.0.0.1 不同源，本机回调常落在后者，父页那侧的 origin 校验会
    // 丢弃本消息——那种情况下父页靠自己轮询 auth/status 拿终态，不依赖这条。
    if (payload.notify && window.opener) {{
      window.opener.postMessage({{ type: 'ai-lubricant-account-auth', ...payload }}, '*');
    }}
    // 按钮 = 回首页；倒计时到点 = 关窗。window.close() 只对脚本开出来的窗口有效，
    // 直接粘地址打开的关不掉，200ms 后没关成就退回首页兜底。
    const goHome = () => {{ location.replace('/'); }};
    const closeOrHome = () => {{ window.close(); setTimeout(goHome, 200); }};
    document.getElementById('back').addEventListener('click', goHome);
    const hint = document.getElementById('hint');
    if (countdown > 0) {{
      let left = countdown;
      hint.textContent = left + ' 秒后自动关闭';
      const timer = setInterval(() => {{
        left -= 1;
        if (left <= 0) {{ clearInterval(timer); closeOrHome(); return; }}
        hint.textContent = left + ' 秒后自动关闭';
      }}, 1000);
    }} else {{
      hint.textContent = '请手动关闭本页，或返回首页重试';
    }}
  </script>
</body>
</html>"""


def _normalize_auth_origin(value: Any) -> str | None:
    """Normalize a browser origin to ``scheme://host[:port]`` only.

    The browser supplies the public origin when the API is behind a reverse proxy;
    never concatenate an unchecked value into an OAuth redirect URI.
    """
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        parsed = urlsplit(raw)
        if parsed.scheme.lower() not in ("http", "https"):
            return None
        if parsed.username is not None or parsed.password is not None:
            return None
        if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
            return None
        host = parsed.hostname
        if not host:
            return None
        port = parsed.port
    except (ValueError, UnicodeError):
        return None
    host = host.lower()
    # Bracket IPv6 literals when rebuilding a netloc.
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    origin = f"{parsed.scheme.lower()}://{host}"
    if port is not None:
        origin += f":{port}"
    return origin


def _provider_account_auth_redirect_uri(request: Request, name: str, origin: str | None = None) -> str:
    """Build the framework-owned callback URI from a browser origin."""
    if origin:
        return f"{origin}/admin/providers/{quote(name, safe='')}/accounts/auth/callback"
    try:
        return str(request.url_for("account_auth_callback", name=name))
    except Exception:
        base = str(request.base_url).rstrip("/")
        return f"{base}/admin/providers/{quote(name, safe='')}/accounts/auth/callback"


def _auth_origin_from_request(request: Request, data: dict) -> str:
    """Resolve the browser-visible origin, with a legacy server-side fallback.

    Priority: ``Origin`` header (set by the browser, not forgeable by page JS)
    → ``Referer``'s scheme+netloc → body ``origin`` field → ``request.base_url``.
    Body and header disagreement resolves to the header (logged) — the body field
    is untrusted input on an API with no global CSRF protection.
    """
    origin_header = request.headers.get("origin")
    referer_header = request.headers.get("referer")
    body_value = data.get("origin") if isinstance(data, dict) else None

    def _from_candidate(value: Any) -> str | None:
        if not isinstance(value, str) or not value.strip():
            return None
        return _normalize_auth_origin(value.strip())

    normalized = _from_candidate(origin_header)
    if not normalized and referer_header:
        try:
            ref = urlsplit(referer_header.strip())
            normalized = _normalize_auth_origin(f"{ref.scheme}://{ref.netloc}")
        except (ValueError, UnicodeError):
            normalized = None
    if not normalized:
        normalized = _from_candidate(body_value)
    if not normalized:
        normalized = _from_candidate(str(request.base_url).rstrip("/"))
    if not normalized:
        raise HTTPException(status_code=400, detail="无法确定浏览器回调地址，请从管理端页面重新发起授权")
    if origin_header and _from_candidate(origin_header) and normalized != _from_candidate(origin_header):
        logger.warning(f"[admin] auth origin from body/referer disagrees with Origin header "
                       f"({normalized!r} != {_from_candidate(origin_header)!r}); using header value")
        return _from_candidate(origin_header)
    return normalized


def _build_auth_context(request: Request, data: dict, name: str, state: str) -> dict:
    origin = _auth_origin_from_request(request, data)
    callback_path = f"/admin/providers/{quote(name, safe='')}/accounts/auth/callback"
    parsed = urlsplit(origin)
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    return {
        "provider": name,
        "state": state,
        "origin": origin,
        "scheme": parsed.scheme,
        "host": parsed.hostname or "",
        "port": port,
        "callback_path": callback_path,
        "callback_url": f"{origin}{callback_path}",
        "loopback_callback_path": "/oauth/callback",
    }


def _account_from_auth_result(result: dict) -> dict:
    account = result.get("account") if isinstance(result, dict) else None
    if isinstance(account, dict):
        return dict(account)
    if isinstance(result, dict):
        return dict(result)
    return {}


async def _finalize_authorized_auth(name: str, state: str, record: dict) -> dict:
    """把 authorized 的授权结果真正落库并标记 completed；幂等。

    三个入口共用（扫描器 authorized、loopback 回调 authorized、/auth/status 读到遗留
    authorized）。record 已 completed 就直接返回已存账号——loopback 回调与扫描器可能
    抢跑，账号 upsert 本身按 username 合并是安全的，护栏挡的是状态行被回写覆盖。
    """
    if record.get("completed"):
        return record.get("account") or {}
    account_data = dict(record.get("account_data") or {})
    account_data.setdefault("username", account_data.get("user_name") or account_data.get("user_email") or account_data.get("account_id") or f"{name}-user")
    account_data.setdefault("password", account_data.get("github_token") or account_data.get("refresh_token") or "")
    account_data.setdefault("switch", True)
    account_data.setdefault("add_method", "device_code")

    cfg = await _read_provider_config(name)
    account, old_account = _upsert_account_in_config(cfg, account_data)
    account = _normalize_account_proxy_ref(account, _read_proxies())
    await _persist_account(name, account)
    _reload_account_into_pool(name, account, _provider_rpm(cfg), _provider_extra(name, cfg))
    await _log_operation("", "device_code_auth_complete", "account", account.get("username"), {"provider": name, "account": old_account}, {"provider": name, "account": account})

    # 标记完成：写回 Postgres，扫描器据 status!=pending 自然停止轮询。
    record["status"] = "completed"
    record["completed"] = True
    record["account"] = account
    record["message"] = "设备码授权成功，已写入账号列表"
    await _set_account_auth_state(state, record)
    logger.info(f"[admin] auth finalize provider={name} state={state} username={account.get('username')!r}")
    return account


def _upsert_account_in_config(cfg: dict, incoming: dict) -> tuple[dict, dict | None]:
    account = dict(incoming or {})
    username = (
        account.get("username")
        or account.get("user_name")
        or account.get("user_email")
        or account.get("account_id")
        or account.get("email")
        or ""
    ).strip()
    if not username:
        raise HTTPException(status_code=400, detail="回调未返回账号用户名")
    account["username"] = username
    account.setdefault("switch", True)
    account.setdefault("add_method", "callback")
    accounts = cfg.setdefault("accounts", [])
    old_account = None
    for index, existing in enumerate(accounts):
        if isinstance(existing, dict) and existing.get("username") == username:
            old_account = dict(existing)
            merged = dict(existing)
            merged.update({k: v for k, v in account.items() if v is not None})
            accounts[index] = merged
            return merged, old_account
    accounts.append(account)
    return account, None


@router.get("/providers/{name}/account-schema")
async def get_provider_account_schema(name: str, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cfg = await _read_provider_config(name)
    return _provider_account_schema(name, cfg)


@router.get("/providers/code-channel/docs")
async def get_code_channel_docs(token: str = Header(None, alias="Authorization")):
    """代码渠道「使用说明」+ 可粘贴样例，来自仓库里的真实文件（docs/providers/*）。

    前端弹框不再硬编码说明与样例：说明读 code-channel.md，样例读 samples/*.py，
    三处（文档 / 测试 / 前端）共用同一份文件，改一处即处处生效。
    """
    await _require_admin(token)
    from providers.code_docs import get_code_channel_docs as _docs
    return _docs()


def _build_auth_runtime_instance(name: str, cfg: dict, account: dict):
    """Instantiate a provider for an auth-only call (device flow / loopback callback)."""
    provider_class = _get_provider_class(name, cfg)
    runtime_acc = dict(account or {})
    if not runtime_acc.get("username"):
        runtime_acc["username"] = f"{name}-auth"
    runtime_acc["proxy"] = _resolve_account_proxy(runtime_acc)
    runtime_acc["url_prefix"] = _resolve_account_url_prefix(runtime_acc)
    extra = _provider_extra(name, cfg)
    extra.update({k: v for k, v in runtime_acc.items() if k not in ("username", "password", "proxy", "url_prefix", "proxy_id", "switch")})
    return provider_class(
        username=runtime_acc.get("username"),
        password=runtime_acc.get("password", ""),
        proxy=runtime_acc.get("proxy"),
        url_prefix=runtime_acc.get("url_prefix"),
        **extra,
    )


async def _invoke_begin_device_flow(provider_class, name: str, cfg: dict, account: dict, auth_context: dict) -> dict:
    """Call ``begin_device_flow`` on a throwaway instance and validate its shape."""
    begin = getattr(provider_class, "begin_device_flow", None)
    if not begin:
        raise HTTPException(status_code=501, detail=f"渠道 {name} 未实现设备码授权（begin_device_flow）")
    provider = _build_auth_runtime_instance(name, cfg, account)
    if isinstance(begin, classmethod):
        bound = begin.__func__
        if _accepts_extra_arg(bound, 2):
            result = await bound(provider_class, provider, auth_context)
        else:
            result = await bound(provider_class, provider)
    else:
        bound = provider.begin_device_flow
        result = (await bound(auth_context)
                  if _accepts_extra_arg(bound, 0)
                  else await bound())
    if not isinstance(result, dict):
        raise HTTPException(status_code=500, detail="provider.begin_device_flow 返回格式错误")
    if result.get("task_type") != "device_code":
        raise HTTPException(status_code=500, detail="provider.begin_device_flow 未声明 task_type=device_code")
    return result


def _apply_device_code_state(state_data: dict, result: dict) -> tuple[int, int]:
    """Write device-code scanner fields into ``state_data``; returns (interval, expires_in)."""
    now = time.time()
    interval = max(int(result.get("interval") or 5), 1)
    expires_in = int(result.get("expires_in") or 900)
    state_data.update({
        "task_type": "device_code",
        "status": "pending",
        "interval": interval,
        "poll_params": result.get("poll_params") or {},
        "next_poll_at": now + interval,  # 第一次 tick 就开始轮询
        "expires_at": now + expires_in,
        "user_code": result.get("user_code", ""),
        "verification_uri": result.get("verification_uri", ""),
    })
    return interval, expires_in


def _shape_device_code_response(result: dict, interval: int, completion: str = "poll") -> dict:
    auth_url = result.get("auth_url") or result.get("verification_uri") or result.get("login_url")
    if auth_url:
        result.setdefault("auth_url", auth_url)
    result.setdefault("mode", "device_code")
    result.setdefault("poll_status", True)
    result.setdefault("poll_interval", interval)
    # completion 透传给前端：poll = 只等轮询；loopback = 同机等回调、跨机可粘 URL 补投。
    result.setdefault("completion", completion)
    # provider 内部 state（broker state）与后端管理态 state 是两个东西，前端不要混
    if "state" in result:
        result["provider_state"] = result.pop("state")
    return result


@router.post("/providers/{name}/accounts/auth/start")
async def start_provider_account_auth(name: str, data: dict, request: Request, token: str = Header(None, alias="Authorization")):
    """启动账号授权（callback / device-code 统一入口）。

    分发依据是 ``account_schema()["auth_start"]["mode"]``：``oauth_callback`` 走
    ``build_account_auth_start``，``device_code`` 走 ``begin_device_flow``；两者都没声明的
    老渠道保留「先试 callback、撞 501 再退设备码」的兼容路径。

    回调地址由框架用「浏览器 origin + 框架路径」拼好后作为 ``redirect_uri`` /
    ``auth_context`` 交给渠道，渠道不自己决定服务端地址。
    """
    admin_token = await _require_admin(token)
    cfg = await _read_provider_config(name)
    provider_class = _get_provider_class(name, cfg)
    account = data.get("account") if isinstance(data.get("account"), dict) else data
    account = _merge_saved_account_for_auth(dict(account or {}), cfg)
    state = secrets.token_urlsafe(32)
    auth_context = _build_auth_context(request, data if isinstance(data, dict) else {}, name, state)
    redirect_uri = auth_context["callback_url"]
    auth_start = (_provider_account_schema(name, cfg).get("auth_start") or {})
    mode = str(auth_start.get("mode") or "").strip().lower()
    completion = str(auth_start.get("completion") or "").strip().lower()
    if completion not in ("poll", "callback", "loopback"):
        completion = "callback" if mode in ("oauth_callback", "callback", "popup") else "poll"
    state_data = {
        "provider": name,
        "account": account,
        "admin": admin_token[:16],
        "created_at": time.time(),
        "redirect_uri": redirect_uri,
        "auth_context": auth_context,
        "completion": completion,
    }
    await _set_account_auth_state(state, state_data)
    try:
        if mode == "device_code":
            result = await _invoke_begin_device_flow(provider_class, name, cfg, account, auth_context)
            interval, expires_in = _apply_device_code_state(state_data, result)
            await _set_account_auth_state(state, state_data)
            result = _shape_device_code_response(result, interval, completion)
            logger.info(
                f"[admin] auth/start device_code provider={name} state={state} "
                f"completion={completion} origin={auth_context['origin']} "
                f"interval={interval}s expires_in={expires_in}s"
            )
        elif mode in ("oauth_callback", "callback", "popup"):
            result = await provider_class.build_account_auth_start(
                name, account, redirect_uri, state, cfg, auth_context=auth_context)
            # callback 型：依赖 callback 端点写 completed，不写 task_type，扫描器跳过
            if isinstance(result, dict):
                result.setdefault("completion", "callback")
            logger.info(f"[admin] auth/start {mode} provider={name} state={state} redirect_uri={redirect_uri}")
        else:
            # 未声明 mode 的老渠道：先试 callback，只有 501（未实现）才回退设备码。
            try:
                result = await provider_class.build_account_auth_start(
                    name, account, redirect_uri, state, cfg, auth_context=auth_context)
                if isinstance(result, dict):
                    result.setdefault("completion", "callback")
            except HTTPException as e:
                if e.status_code != 501 or not getattr(provider_class, "begin_device_flow", None):
                    raise
                result = await _invoke_begin_device_flow(provider_class, name, cfg, account, auth_context)
                interval, expires_in = _apply_device_code_state(state_data, result)
                await _set_account_auth_state(state, state_data)
                result = _shape_device_code_response(result, interval, completion)
                logger.info(
                    f"[admin] auth/start device_code (legacy probe) provider={name} state={state} "
                    f"completion={completion} origin={auth_context['origin']} "
                    f"interval={interval}s expires_in={expires_in}s"
                )
    except HTTPException as e:
        await _del_account_auth_state(state)
        logger.warning(
            f"[admin] auth/start 失败 provider={name} status={e.status_code} detail={str(e.detail)[:300]}"
        )
        raise
    except Exception as e:
        await _del_account_auth_state(state)
        import aiohttp as _aiohttp
        logger.error(f"[admin] auth/start 未预期异常 provider={name}: {type(e).__name__}: {e}")
        if isinstance(e, (_aiohttp.ClientError, _aiohttp.ClientConnectorError, _aiohttp.ClientConnectorDNSError)):
            raise HTTPException(
                status_code=502,
                detail=f"无法连接授权服务（{name}）：{type(e).__name__}。请检查网络或为该渠道配置代理后重试。",
            ) from e
        raise
    if not isinstance(result, dict):
        await _del_account_auth_state(state)
        raise HTTPException(status_code=500, detail="渠道授权入口返回格式错误")
    await _log_operation(admin_token, "start_account_auth", "account", account.get("username") or name, None, {"provider": name, "state": state})
    payload = {"ok": True, "state": state, **result}
    # redirect_uri 只对回调式授权有意义；device_code 模式不回传，避免前端误用。
    if state_data.get("task_type") != "device_code":
        payload["redirect_uri"] = redirect_uri
    return payload


@router.get("/providers/{name}/accounts/auth/callback", name="account_auth_callback")
async def account_auth_callback(name: str, request: Request):
    params = dict(request.query_params)
    state = (params.get("state") or "").strip()
    if not state:
        return HTMLResponse(_account_auth_callback_html(False, "缺少 state 参数", name), status_code=400)
    state_data = await _get_account_auth_state(state)
    if not state_data or state_data.get("provider") != name:
        return HTMLResponse(_account_auth_callback_html(False, "授权状态已过期或不匹配，请重新发起授权", name), status_code=400)
    try:
        cfg = await _read_provider_config(name)
        provider_class = _get_provider_class(name, cfg)
        result = await provider_class.handle_account_auth_callback(name, params, state_data, cfg)
        account = _account_from_auth_result(result)
        account, old_account = _upsert_account_in_config(cfg, account)
        account = _normalize_account_proxy_ref(account, _read_proxies())
        await _persist_account(name, account)
        _reload_account_into_pool(name, account, _provider_rpm(cfg), _provider_extra(name, cfg))
        await _log_operation("", "account_auth_callback", "account", account.get("username"), {"provider": name, "account": old_account}, {"provider": name, "account": account})
        await _del_account_auth_state(state)
        message = "账号授权成功，已写入账号列表"
        return HTMLResponse(_account_auth_callback_html(True, message, name, account.get("username") or ""))
    except Exception as e:
        detail = getattr(e, "detail", str(e))
        status = getattr(e, "status_code", 500)
        return HTMLResponse(_account_auth_callback_html(False, str(detail), name), status_code=status)


_LOOPBACK_NEUTRAL_MESSAGE = (
    "本地址仅用于桌面上游的本机登录回调。如果你看到此页面，说明这条回调没有匹配到进行中的"
    "授权会话——若管理端仍显示「授权进行中」，服务端会继续用轮询兜底，请回管理端观察结果；"
    "否则回账号页重新发起授权。")


def _loopback_neutral_page() -> HTMLResponse:
    """中性提示页：无会话与无人认领返回同一份文案，不泄漏是否有授权在进行。

    ``ok=None`` 而非 False：这不是对某次授权的判决，不能 postMessage 让父页停掉轮询
    （见 ``_account_auth_callback_html``）。provider 传空串，页面不渲染「渠道」行。
    """
    return HTMLResponse(_account_auth_callback_html(None, _LOOPBACK_NEUTRAL_MESSAGE, "", ""), status_code=400)


def _validate_loopback_redirect(url: Any) -> str | None:
    """claimed 307 目标只允许渠道返回的 http(s) 绝对 URL，防开放重定向。"""
    if not isinstance(url, str) or not url.strip():
        return None
    try:
        parsed = urlsplit(url.strip())
    except (ValueError, UnicodeError):
        return None
    if parsed.scheme.lower() not in ("http", "https") or not parsed.hostname:
        return None
    return url.strip()


def _parse_callback_url_params(raw: Any) -> dict[str, str]:
    """从用户粘贴的回调地址里取 query 参数。

    跨机部署时浏览器最终落在 ``http://127.0.0.1:{port}/oauth/callback?...``——那台机器上
    没有我们的服务，页面打不开，但**地址栏里的参数是完整的**。允许管理员把整条地址粘回来，
    等价于把那次回调补投给服务端。

    宽松接受三种形态：完整 URL、``?code=..&secret=..``、裸 ``code=..&secret=..``。
    """
    if not isinstance(raw, str):
        return {}
    text = raw.strip()
    if not text:
        return {}
    query = urlsplit(text).query if "://" in text else text.lstrip("?")
    if not query and "=" not in text:
        return {}
    parsed = parse_qs(query or text.lstrip("?"), keep_blank_values=False)
    return {k: v[0] for k, v in parsed.items() if v and isinstance(v[0], str)}


async def _provider_declares_loopback(provider_class) -> bool:
    return "handle_loopback_callback" in vars(provider_class)


@loopback_router.get("/oauth/callback")
async def oauth_loopback_callback(request: Request):
    """根级本机回调：上游只回调 127.0.0.1:{port}/oauth/callback 的桌面客户端型 OAuth。

    上游回调不带我们的 state（它带的是自己那套 secret/redirect/code），所以这里按
    「枚举 pending 会话 → 渠道钩子认领」处理：只遍历 device_code + pending 且声明了
    handle_loopback_callback 的会话（通常 0~几个），认领与否由渠道拿自己 poll_params
    里的票据比对决定，框架不猜。跨机部署回调打不进来时，扫描器 ticket 轮询照常兜底，
    管理端也可把浏览器地址栏里的整条回调地址粘回 auth/replay 手工补投。
    """
    result = await _process_loopback_params(dict(request.query_params))
    kind = result["kind"]
    if kind == "redirect":
        return RedirectResponse(url=result["redirect_url"], status_code=307)
    if kind == "authorized":
        return HTMLResponse(_account_auth_callback_html(
            True, result["message"], result["provider"], result.get("username") or ""))
    if kind == "error":
        return HTMLResponse(_account_auth_callback_html(
            False, result["message"], result.get("provider") or "oauth"),
            status_code=result.get("status_code", 400))
    return _loopback_neutral_page()


@router.post("/providers/{name}/accounts/auth/replay")
async def replay_provider_account_auth(name: str, data: dict, token: str = Header(None, alias="Authorization")):
    """手工补投回调地址：把浏览器地址栏里的整条 URL 粘回来，当作那次本机回调处理。

    跨机部署时上游回调落在用户自己机器的 127.0.0.1 上（那台机器没有本服务，页面打不开），
    但地址栏里的参数是完整的（code / secret / redirect 都在）。管理员复制整条地址粘进
    管理端，服务端按与 ``GET /oauth/callback`` 完全相同的「认领 → 换码 / 回填」逻辑补投。
    只处理本渠道的会话；与 ticket 轮询双通道幂等（_finalize_authorized_auth 有 completed 护栏）。
    """
    admin_token = await _require_admin(token)
    raw = data.get("callback_url") if isinstance(data, dict) else None
    params = _parse_callback_url_params(raw)
    if not params:
        raise HTTPException(status_code=400, detail="回调地址里没有可识别的参数，请粘贴浏览器地址栏的完整 URL")
    # 整条 URL 也透传给渠道钩子：需要 fragment（#token=…）或非常规参数的 spec 自己解析。
    result = await _process_loopback_params(
        params, only_provider=name, callback_url=raw if isinstance(raw, str) else "")
    kind = result["kind"]
    logger.info(f"[admin] auth/replay provider={name} kind={kind} keys={sorted(params)}")
    if kind == "authorized":
        await _log_operation(admin_token, "replay_account_auth", "account",
                             result.get("username") or name, None, {"provider": name})
        return {"ok": True, "status": "completed", "message": result["message"],
                "username": result.get("username") or ""}
    if kind == "redirect":
        # 第一阶段：secret 已回填，扫描器下一轮就会用新参数拿到凭证；无需用户再操作。
        return {"ok": True, "status": "pending",
                "message": "回调已补投，登录票据已更新，正在等待服务端换取凭证…"}
    if kind == "error":
        raise HTTPException(status_code=result.get("status_code", 400), detail=result["message"])
    raise HTTPException(status_code=400,
                        detail="没有渠道认领这条回调地址：可能授权会话已过期或地址不完整，请重新发起授权")


async def _process_loopback_params(params: dict, only_provider: str | None = None,
                                   callback_url: str = "") -> dict:
    """本机回调的统一处理：浏览器直打 GET /oauth/callback 与管理端手工补投共用。

    ``callback_url`` 是手工补投时的整条地址（query+fragment 都在）；浏览器直打的真实回调
    只有 query 参数，传空串。渠道钩子按签名决定是否接收它。

    返回 ``{"kind": "redirect"|"authorized"|"error"|"unclaimed", ...}``，渲染成 HTML 页
    还是 JSON 由调用方决定。
    """
    # 无人认领时要能查为什么：记参数名和候选会话数，不记值（code/secret/token 是凭据）。
    param_names = sorted(params or {})
    try:
        tasks = await _list_pending_device_code_tasks()
    except Exception as e:  # noqa: BLE001 - 列不出会话按未认领处理
        logger.error(f"[admin] loopback callback list states failed: {e}")
        return {"kind": "unclaimed"}

    # 回调自带 state（spec 把它塞进 auth_callback_url，portal 若原样带回来）→ 直接命中
    # 那个会话，不遍历。portal 不带 state（没保留 query 或旧身份无 auth_callback_url）
    # 时 tasks 不动，回落遍历兜底。
    state_hint = (params.get("state") or "").strip()
    if state_hint:
        matched = [(s, r) for s, r in tasks if s == state_hint]
        if matched:
            tasks = matched

    considered = 0
    for state, record in tasks:
        provider_name = record.get("provider", "")
        if not provider_name:
            continue
        if only_provider and provider_name != only_provider:
            continue
        considered += 1
        try:
            cfg = await _read_provider_config(provider_name)
            provider_class = _get_provider_class(provider_name, cfg)
            if not await _provider_declares_loopback(provider_class):
                continue
            inst = _build_scanner_instance(
                provider_name, cfg, record.get("account") or {},
                cache_key=f"{provider_name}:{state}")
            hook = getattr(inst, "handle_loopback_callback", None)
            if hook is None:
                continue
            poll_params = record.get("poll_params") or {}
            if callback_url and _accepts_extra_arg(hook, 2):
                coro = hook(params, poll_params, callback_url)
            else:
                coro = hook(params, poll_params)
            outcome = await asyncio.wait_for(coro, timeout=30)
        except asyncio.TimeoutError:
            logger.warning(f"[admin] loopback callback timeout provider={provider_name} state={state}")
            continue
        except Exception as e:  # noqa: BLE001 - 单个会话失败不影响其它会话认领
            logger.error(f"[admin] loopback callback provider={provider_name} state={state}: {type(e).__name__}: {e}")
            continue
        if outcome is None:
            continue  # 渠道说不是自己的回调，试下一个

        status = outcome.get("status") if isinstance(outcome, dict) else None
        if status == "claimed":
            # 第一阶段：渠道认领并回填 poll_params（如 portal 下发的真 secret），
            # 扫描器下一轮自然用上新参数；307 到渠道给的后续地址。
            new_poll = outcome.get("poll_params")
            if isinstance(new_poll, dict) and new_poll:
                record["poll_params"] = {**(record.get("poll_params") or {}), **new_poll}
                await _set_account_auth_state(state, record)
            redirect = _validate_loopback_redirect(outcome.get("redirect"))
            if not redirect:
                logger.warning(f"[admin] loopback claimed without valid redirect provider={provider_name} state={state}")
                return {"kind": "unclaimed"}
            logger.info(f"[admin] loopback claimed provider={provider_name} state={state} → 307 {redirect[:120]}")
            return {"kind": "redirect", "redirect_url": redirect, "provider": provider_name}

        if status == "authorized":
            # 第二阶段：渠道已用回调参数换到凭证，直接落库完成。
            record["status"] = "authorized"
            record["account_data"] = outcome.get("account_data") or {}
            await _set_account_auth_state(state, record)
            display = _provider_display_name(provider_class, provider_name)
            try:
                account = await _finalize_authorized_auth(provider_name, state, record)
            except Exception as e:  # noqa: BLE001
                logger.error(f"[admin] loopback finalize failed provider={provider_name} state={state}: {e}")
                return {"kind": "error", "provider": display,
                        "message": f"授权完成但写账号失败：{e}", "status_code": 500}
            return {"kind": "authorized", "provider": display,
                    "message": "账号授权成功，已写入账号列表",
                    "username": account.get("username") or ""}

        if status == "error":
            return {"kind": "error", "provider": _provider_display_name(provider_class, provider_name),
                    "message": str(outcome.get("error") or "授权失败"), "status_code": 400}

        logger.warning(f"[admin] loopback callback unknown outcome provider={provider_name} state={state}: {outcome!r}")

    logger.warning(
        f"[admin] loopback callback unclaimed params={param_names} "
        f"considered={considered} total_pending={len(tasks)}"
    )
    return {"kind": "unclaimed"}


@router.get("/providers/{name}/accounts/auth/status")
async def get_provider_account_auth_status(name: str, state: str, token: str = Header(None, alias="Authorization")):
    """查询账号授权状态（统一 callback 和 device-code）。

    所有状态从 Postgres account_auth_states 的 state_data 读取，不再读 provider 类/实例属性。
    404 = state 不存在（过期或被 cancel 删）= 前端应显示"已过期/取消"。
    """
    await _require_admin(token)
    state_data = await _get_account_auth_state(state)
    logger.info(
        f"[admin] auth/status ENTER provider={name} state={state} "
        f"state_found={state_data is not None} "
        f"state_provider={(state_data or {}).get('provider')!r} "
        f"task_type={(state_data or {}).get('task_type')!r} "
        f"status={(state_data or {}).get('status')!r}"
    )
    if not state_data or state_data.get("provider") != name:
        logger.warning(
            f"[admin] auth/status 404 provider={name} state={state} "
            f"reason={'missing' if not state_data else 'provider_mismatch'}"
        )
        raise HTTPException(status_code=404, detail="授权状态已过期或不存在")

    task_status = state_data.get("status", "idle")
    result: dict = {"status": task_status, "state": state}

    # callback 型完成（由 account_auth_callback 端点写入 completed）
    if state_data.get("completed"):
        return {
            "status": "completed",
            "account": state_data.get("account"),
            "message": state_data.get("message", "授权已完成"),
        }

    # device_code 型：读 state_data 里的进度
    if state_data.get("task_type") == "device_code":
        if task_status == "pending":
            result["user_code"] = state_data.get("user_code", "")
            result["verification_uri"] = state_data.get("verification_uri", "")
            created_at = state_data.get("created_at") or state_data.get("next_poll_at") or time.time()
            result["elapsed"] = int(time.time() - created_at)
        elif task_status == "authorized":
            # 正常路径下扫描器已在 _poll_one_auth_task 里落库；这里只兜底处理
            # 落库失败/旧数据残留的 authorized 行（幂等）。
            logger.info(f"[admin] auth/status AUTHORIZED provider={name} state={state} 走兜底落库")
            try:
                account = await _finalize_authorized_auth(name, state, state_data)
            except Exception as e:
                logger.error(f"[admin] device-code auth 落库失败 provider={name} state={state}: {e}")
                account = dict(state_data.get("account_data") or {})

            result["status"] = "completed"
            result["account"] = account
            result["message"] = "设备码授权成功"
        elif task_status == "error":
            result["error"] = state_data.get("error", "授权失败")
        elif task_status == "expired":
            result["message"] = "授权已过期，请重新发起"

    logger.info(
        f"[admin] auth/status RETURN provider={name} state={state} "
        f"result.status={result.get('status')} keys={list(result.keys())} "
        f"result={_mask_auth_dict(result)}"
    )
    return result


@router.post("/providers/{name}/accounts/auth/cancel")
async def cancel_provider_account_auth(name: str, state: str, token: str = Header(None, alias="Authorization")):
    """取消进行中的账号授权：删除 account_auth_states 里的 state 行。

    state 删除后扫描器自然不再轮询，前端下次查 /auth/status 得 404 = 已取消。
    """
    await _require_admin(token)
    state_data = await _get_account_auth_state(state)
    if state_data and state_data.get("provider") == name:
        await _del_account_auth_state(state)
        logger.info(f"[admin] auth/cancel provider={name} state={state} deleted")
        await _log_operation(token, "cancel_account_auth", "account", state, {"provider": name}, None)
    return {"ok": True}


@router.post("/providers/{name}/accounts/{username}/refresh-auth")
async def refresh_provider_account_auth(name: str, username: str, token: str = Header(None, alias="Authorization")):
    admin_token = await _require_admin(token)
    cfg = await _read_provider_config(name)
    accounts = cfg.get("accounts", [])
    target_index = None
    target = None
    for index, account in enumerate(accounts):
        if isinstance(account, dict) and account.get("username") == username:
            target_index = index
            target = account
            break
    if target_index is None or target is None:
        raise HTTPException(status_code=404, detail=f"账号 '{username}' 不存在")

    pool = ModelClientPool.get_provider_pool(name)
    provider = None
    if pool:
        for client in pool.clients:
            if client.username == username:
                provider = client.provider
                break
    if provider is None:
        provider_class = _get_provider_class(name, cfg)
        runtime_acc = dict(target)
        runtime_acc["proxy"] = _resolve_account_proxy(target)
        runtime_acc["url_prefix"] = _resolve_account_url_prefix(target)
        extra = _provider_extra(name, cfg)
        extra.update({k: v for k, v in runtime_acc.items() if k not in ("username", "password", "proxy", "url_prefix", "proxy_id", "switch")})
        provider = provider_class(
            username=runtime_acc.get("username", username),
            password=runtime_acc.get("password", ""),
            proxy=runtime_acc.get("proxy"),
            url_prefix=runtime_acc.get("url_prefix"),
            **extra,
        )

    updates = await provider.refresh_account_auth(dict(target), cfg)
    if not isinstance(updates, dict):
        raise HTTPException(status_code=500, detail="刷新授权返回格式错误")
    merged = dict(target)
    merged.update({k: v for k, v in updates.items() if v is not None})
    merged = _normalize_account_proxy_ref(merged, _read_proxies())
    await _persist_account(name, merged)
    _reload_account_into_pool(name, merged, _provider_rpm(cfg), _provider_extra(name, cfg))
    await _log_operation(admin_token, "refresh_account_auth", "account", username, {"provider": name, "account": target}, {"provider": name, "account": merged})
    return {"ok": True, "username": merged.get("username", username)}


@router.get("/providers/{name}/accounts")
async def get_provider_accounts(
    name: str,
    view: Literal["full", "lite", "copy"] = "full",
    token: str = Header(None, alias="Authorization"),
):
    await _require_admin(token)
    cfg = await _read_provider_config(name)
    if view == "lite":
        return await _provider_lite_accounts_response(name, cfg)
    if view == "copy":
        return _provider_copy_accounts_response(cfg)
    return await _provider_accounts_response(name, cfg)


@router.get("/providers/{name}/accounts/duplicate-tokens")
async def get_provider_duplicate_tokens(name: str, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cfg = await _read_provider_config(name)
    duplicates = _find_duplicate_account_tokens(cfg.get("accounts", []))
    return {"ok": True, "duplicates": duplicates}


@router.get("/providers/{name}/accounts/status")
async def get_provider_account_status(name: str, token: str = Header(None, alias="Authorization")):
    """返回渠道下所有冻结账号状态（账号级 + 账号-模型级）。"""
    await _require_admin(token)
    cfg = await _read_provider_config(name)
    # 冻结改纯内存：_freeze_state 是单进程权威，不再两次批量读 Redis。
    pool = ModelClientPool.get_provider_pool(name)
    client_by_username = {c.username: c for c in (pool.clients if pool else [])}
    account_cooldowns = {
        u: cd for u, c in client_by_username.items()
        if (cd := _memory_account_cooldown(c)) is not None
    }
    model_cooldowns = {
        u: _memory_model_cooldowns(c) for u, c in client_by_username.items()
    }
    return _provider_account_status_response(name, account_cooldowns, model_cooldowns)


@router.get("/providers/{name}/accounts/availability")
async def get_provider_accounts_availability(
    name: str,
    model: str = "",
    token: str = Header(None, alias="Authorization"),
):
    """按目标模型 / 模型组诊断该渠道每个账号此刻能否被路由选中（只读）。

    区分“模型组配置过滤”（provider_filtered）与“运行态冷却/冻结”，
    帮助定位“账号没冻结但模型组不可用”的场景。
    """
    await _require_admin(token)
    model_id = (model or "").strip()
    if not model_id:
        raise HTTPException(status_code=400, detail="model 参数必填（真实模型名或模型组名）")
    cfg = await _read_provider_config(name)
    return await _provider_accounts_availability(name, cfg, model_id)


def _apply_policy_to_runtime(provider_name: str, policy: dict):
    pool = ModelClientPool.get_provider_pool(provider_name)
    if not pool:
        return
    policy_enabled = policy.get("enabled", True) is not False
    for client in pool.clients:
        client.rpm_limit = int(policy.get("account_rpm") or 0) if policy_enabled else 0
        client.tpm_limit = int(policy.get("account_tpm") or 0) if policy_enabled else 0
        client.concurrent_limit = int(policy.get("account_concurrent") or 0) if policy_enabled else 0
        # 到量冻结维度：每小时次数/每小时 tokens/每天次数/每天 tokens（触线即冻结账号）
        client.rph_limit = int(policy.get("account_rph") or 0) if policy_enabled else 0
        client.tph_limit = int(policy.get("account_tph") or 0) if policy_enabled else 0
        client.rpd_limit = int(policy.get("account_rpd") or 0) if policy_enabled else 0
        client.tpd_limit = int(policy.get("account_tpd") or 0) if policy_enabled else 0
        client.provider.limits.config = {**getattr(client.provider.limits, "config", {}), **policy}


@router.get("/providers/{name}/limit-policy")
async def get_provider_limit_policy(name: str, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    if not await _provider_config_exists(name):
        raise HTTPException(status_code=404, detail=f"provider '{name}' 不存在")
    return await get_effective_provider_policy(name, refresh=True)


@router.put("/providers/{name}/limit-policy")
async def update_provider_limit_policy(name: str, data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    if not await _provider_config_exists(name):
        raise HTTPException(status_code=404, detail=f"provider '{name}' 不存在")
    old_data = await get_effective_provider_policy(name, refresh=True)
    payload = dict(data or {})
    payload["name"] = payload.get("name") or "default"
    try:
        payload["cooldown_policy"] = validate_cooldown_policy(payload.get("cooldown_policy") or {})
        payload["freeze_policy"] = validate_freeze_policy(payload.get("freeze_policy") or {})
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    policy = await PostgresClient.upsert_provider_limit_policy(name, payload)
    normalized_policy = dict(policy)
    normalized_policy["cooldown_policy"] = validate_cooldown_policy(normalized_policy.get("cooldown_policy") or {})
    normalized_policy["freeze_policy"] = validate_freeze_policy(normalized_policy.get("freeze_policy") or {})
    clear_policy_cache(name)
    await get_effective_provider_policy(name, refresh=True)
    _apply_policy_to_runtime(name, normalized_policy)
    await _log_operation(token, "update_limit_policy", "provider", name, old_data, normalized_policy)
    return {"ok": True, "policy": normalized_policy}


# ==================== Provider 配置 (供弹窗编辑用) ====================


@router.get("/config/providers/{name}")
async def get_provider_raw_config(name: str, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    return await _read_provider_base_config(name)


def _normalize_chat_protocols(data: dict) -> list[dict]:
    """规范化对话协议列表 chat_protocols（只服务对话，无 kind 字段）。

    字段：id/enabled/protocol/path/upstream_stream/client_preset/header_template/models。
    不再从顶层 protocol/chat_path/upstream_stream/client_preset 兜底——这些顶层字段已废弃，
    渠道必须显式配置 chat_protocols（管理端保存时强制非空）。
    旧配置兼容：若存在旧 endpoint_configs，只取其中 kind=chat 的行作为初始来源。
    """
    raw = data.get("chat_protocols")
    if not isinstance(raw, list):
        raw = []
    if not raw:
        # 兼容上一版 endpoint_configs：只保留 kind=chat 行
        legacy_ec = data.get("endpoint_configs")
        if isinstance(legacy_ec, list) and legacy_ec:
            mapped: list[dict] = []
            for item in legacy_ec:
                if not isinstance(item, dict):
                    continue
                if str(item.get("kind") or "chat").lower() != "chat":
                    continue
                mapped.append(item)
            if mapped:
                raw = mapped
    if not isinstance(raw, list):
        raw = []
    if not raw:
        # 不再从顶层字段兜底；空列表就是空列表，由调用方判断是否拒绝保存。
        return []
    seen_ids: set[str] = set()
    result: list[dict] = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            continue
        p = str(item.get("protocol") or "openai").lower()
        path = str(item.get("path") or "").strip()
        # gemini 的聊天地址按 {model}:generateContent 动态拼接（见 gemini_proto.py），
        # 无固定 path，故不强制填写；其余协议运行时依赖 path 解析 chat_url，必填。
        if not path and p != "gemini":
            raise HTTPException(status_code=400, detail=f"chat_protocols[{index}].path 必填")
        cp_id = str(item.get("id") or f"{p}-chat-{index}").strip()
        base_id = cp_id
        suffix = 1
        while cp_id in seen_ids:
            suffix += 1
            cp_id = f"{base_id}-{suffix}"
        seen_ids.add(cp_id)
        models = item.get("models")
        if not isinstance(models, list):
            models = []
        models = [str(m).strip() for m in models if isinstance(m, str) and m.strip()]
        result.append({
            "id": cp_id,
            "enabled": bool(item.get("enabled", True)),
            "protocol": p,
            "path": path,
            "upstream_stream": normalize_upstream_stream(item.get("upstream_stream")),
            "client_preset": str(item.get("client_preset") or "none").lower(),
            "header_template": str(item.get("header_template") or "").strip(),
            "system_type": str(item.get("system_type") or "auto").lower(),
            "send_reasoning_content": item.get("send_reasoning_content") is not False,
            "models": models,
        })
    return result


def _chat_path_from_protocols(chat_protocols: list[dict]) -> dict:
    """deprecated：顶层兜底字段已废弃，保留空实现以兼容旧调用点。"""
    return {}


def _normalize_provider_website_url(value) -> str:
    """渠道跳转地址：完整 HTTP/HTTPS URL，否则落空串（表示不跳转）。"""
    if not isinstance(value, str):
        return ""
    text = value.strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text)
    except Exception:
        return ""
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        return ""
    return text


_CHANNEL_ICON_KEYS = {
    "bot", "brain", "cloud", "code", "cpu", "database", "flame", "globe",
    "json", "layers", "message", "rocket", "server", "sparkles", "terminal", "wand",
}


def _normalize_provider_icon(value) -> str:
    """渠道图标：内置符号键、完整 HTTP/HTTPS 图片 URL 或 base64 data URL，否则落空串。"""
    if not isinstance(value, str):
        return ""
    text = value.strip()
    if not text:
        return ""
    if text in _CHANNEL_ICON_KEYS:
        return text
    if text.lower().startswith("data:image/"):
        return text
    try:
        parsed = urlparse(text)
    except Exception:
        return ""
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        return ""
    return text


def _normalize_provider_tags(value, *, strict: bool = False) -> list[str]:
    """规范化渠道展示标签：去空、精确去重并保留用户输入顺序。"""
    if value is None:
        return []
    if not isinstance(value, list):
        if strict:
            raise HTTPException(status_code=400, detail="tags 必须是字符串数组")
        return []
    seen: set[str] = set()
    result: list[str] = []
    for item in value:
        if not isinstance(item, str):
            if strict:
                raise HTTPException(status_code=400, detail="tags 只能包含字符串")
            continue
        tag = item.strip()
        if tag and tag not in seen:
            seen.add(tag)
            result.append(tag)
    return result


def _normalize_extra_retry_status_codes(value) -> list[int]:
    """规范化渠道额外重试状态码：仅整数 HTTP 状态码 100-599，去重并稳定排序。"""
    if value is None:
        return []
    if isinstance(value, (int, str)):
        value = [value]
    if not isinstance(value, list):
        raise HTTPException(status_code=400, detail="extra_retry_status_codes 必须是数组")
    seen: set[int] = set()
    result: list[int] = []
    for item in value:
        if isinstance(item, bool):
            raise HTTPException(status_code=400, detail="extra_retry_status_codes 不能包含布尔值")
        try:
            code = int(item)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail=f"extra_retry_status_codes 含非法状态码: {item!r}")
        if not (100 <= code <= 599):
            raise HTTPException(status_code=400, detail=f"extra_retry_status_codes 状态码需在 100-599 之间: {code}")
        if code not in seen:
            seen.add(code)
            result.append(code)
    return sorted(result)


def _normalize_model_id_rewrite_rules(value) -> list[dict]:
    """规范化渠道的 model_id 改写条目，并在写库前编译校验每条正则。

    条目可以是内联规则，也可以是对全局模版的引用（kind="template"）。坏正则在
    这里就拒掉（400），不能放过去等模型同步时才炸——那时一条坏规则会让整轮
    refresh_models 白跑。条目顺序敏感（模版与内联规则同列排序），保留用户输入
    顺序不排序。引用的模版是否存在不在这里校验：模版可以后建。
    """
    if value is None:
        return []
    if not isinstance(value, list):
        raise HTTPException(status_code=400, detail="model_id_rewrite_rules 必须是数组")
    for item in value:
        if not isinstance(item, dict):
            raise HTTPException(status_code=400, detail="model_id_rewrite_rules 只能包含对象")
    rules, errors = compile_model_id_rewrite_rules(value)
    if errors:
        raise HTTPException(status_code=400, detail="model_id 改写规则不合法：" + "；".join(errors))
    return rules


def _coerce_extra_retry_status_codes(value) -> list[int]:
    """读取路径用的宽松规范化：静默丢弃非法/越界/布尔值，去重并稳定排序。

    写入路径用 _normalize_extra_retry_status_codes 严格报错；读取路径（详情/列表响应）
    用本函数防御性收窄，避免历史脏数据导致 GET 500。
    """
    if value is None:
        return []
    if isinstance(value, (int, str)):
        value = [value]
    if not isinstance(value, list):
        return []
    seen: set[int] = set()
    result: list[int] = []
    for item in value:
        if isinstance(item, bool):
            continue
        try:
            code = int(item)
        except (TypeError, ValueError):
            continue
        if not (100 <= code <= 599):
            continue
        if code not in seen:
            seen.add(code)
            result.append(code)
    return sorted(result)


def _validate_provider_requires_base_url(name: str, cfg: dict) -> None:
    """渠道地址必填校验。

    「渠道地址」是前端让用户填的字段，配置驱动渠道（自定义 / 代码渠道回落实现）缺它就
    发不出请求，所以建/改渠道时必须在写库前 400 掉，而不是留到跑请求时才炸。
    代码渠道的 spec 类若完全自管请求或压根不打外网（如 Echo 样例），在 spec 上写
    ``REQUIRES_BASE_URL = False`` 即豁免——真相源是 Provider 类的类属性，不是这里的白名单。
    """
    if (cfg.get("base_url") or "").strip():
        return
    provider_class = _get_provider_class(name, cfg)
    if not getattr(provider_class, "REQUIRES_BASE_URL", False):
        return
    raise HTTPException(status_code=400, detail="渠道地址（base_url）不能为空")


def _custom_provider_config_from_payload(data: dict) -> dict:
    chat_protocols = _normalize_chat_protocols(data)
    if not chat_protocols:
        raise HTTPException(status_code=400, detail="chat_protocols 不能为空：至少配置一条对话协议行")
    cfg = {
        "enabled": data.get("enabled", True),
        "type": "custom",
        "custom_channel": True,
        "remark": data.get("remark", ""),
        "tags": _normalize_provider_tags(data.get("tags"), strict=True),
        "website_url": _normalize_provider_website_url(data.get("website_url")),
        "icon": _normalize_provider_icon(data.get("icon")),
        "base_url": data.get("base_url", ""),
        "models_path": data.get("models_path", "/v1/models"),
        "image_path": data.get("image_path", "/v1/images/generations"),
        "video_path": data.get("video_path", "/v1/videos/generations"),
        "speech_path": data.get("speech_path", "/v1/audio/speech"),
        "supports_image_generation": bool(data.get("supports_image_generation", False)),
        "supports_video_generation": bool(data.get("supports_video_generation", False)),
        "supports_tts": bool(data.get("supports_tts", False)),
        "auto_update_models": data.get("auto_update_models", False),
        "model_id_rewrite_rules": _normalize_model_id_rewrite_rules(data.get("model_id_rewrite_rules")),
        "timeout": data.get("timeout", 120),
        "retry_count": data.get("retry_count"),
        "extra_retry_status_codes": _normalize_extra_retry_status_codes(data.get("extra_retry_status_codes")),
        "account_priority": data.get("account_priority", 0),
        "account_weight": data.get("account_weight", 0),
        "health_check": data.get("health_check", {"enabled": False, "interval_minutes": 30, "test_model": ""}),
        "scheduled_test": data.get("scheduled_test", {"enabled": False}),
        "chat_protocols": chat_protocols,
        "rate_limit": data.get("rate_limit", {"rpm_per_account": 0, "tpm_per_account": 0, "tpm_per_model": 0}),
        "billing_mode": (str(data.get("billing_mode") or "token").lower()) if str(data.get("billing_mode") or "").lower() in ("token", "request") else "token",
        "accounts": [_normalize_account_proxy_ref(a, _read_proxies()) for a in data.get("accounts", []) if isinstance(a, dict)],
        "clear_conversation": data.get("clear_conversation", {"enabled": False, "max_age_hours": 2}),
    }
    return cfg


async def _fetch_unsaved_custom_provider_models(data: dict) -> list[dict]:
    cfg = _custom_provider_config_from_payload(data)
    base_url = (cfg.get("base_url") or "").strip()
    if not base_url:
        raise HTTPException(status_code=400, detail="base_url 必填")
    accounts = [a for a in cfg.get("accounts", []) if isinstance(a, dict) and (a.get("password") or a.get("api_key") or a.get("key"))]
    if not accounts:
        raise HTTPException(status_code=400, detail="至少需要填写一个账号 API Key")
    acc = _runtime_accounts([accounts[0]])[0]
    extra = {k: v for k, v in _provider_extra(data.get("name") or "custom-preview", cfg).items() if k != "proxies"}
    provider = CustomProvider(
        username=acc.get("username") or "preview",
        password=acc.get("password") or acc.get("api_key") or acc.get("key") or "",
        proxy=acc.get("proxy"),
        **extra,
    )
    models = await provider.fetch_upstream_model_list()
    if not models:
        fetch_err = getattr(provider, "_last_fetch_models_error", "") or ""
        if fetch_err:
            raise HTTPException(status_code=502, detail=f"上游模型列表获取失败: {fetch_err}")
        # 上游成功返回空列表时，空列表本身是正常结果。
        return []
    return models


@router.post("/custom-providers/detect")
async def detect_custom_provider_input(data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    return await _detect_quick_custom_provider(data.get("raw", ""))


@router.post("/custom-providers")
async def create_custom_provider(data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)

    builtin_type = (data.get("builtin_type") or "").strip()
    existing_configs = await config.Config.get_providers()
    existing_names = set(existing_configs.keys()) | set(ModelClientPool.get_provider_names())
    remark = (data.get("remark") or "").strip()

    if builtin_type:
        # 内置渠道模板创建（可多次创建）
        default_cfg = _builtin_provider_default_config(builtin_type)
        if not default_cfg:
            raise HTTPException(status_code=400, detail=f"未知的内置渠道类型: {builtin_type}")
        if not remark:
            remark = default_cfg.get("remark", builtin_type)
        name = _auto_generate_name(remark, existing_names)
        cfg = dict(default_cfg)
        cfg["remark"] = remark
        cfg["builtin_type"] = builtin_type  # 记录模板类型，供运行时匹配 Provider 类
        # 允许用户覆盖目录定义中的可编辑字段；builtin_type 仍决定 Provider 类与认证方式。
        for field in (
            "accounts", "enabled", "base_url", "models_path", "image_path", "video_path",
            "speech_path", "timeout", "retry_count", "extra_retry_status_codes", "rate_limit",
            "billing_mode", "auto_update_models", "model_id_rewrite_rules",
            "account_priority", "account_weight", "website_url", "icon",
            "code",
        ):
            if field in data:
                cfg[field] = data[field]
        if "chat_protocols" in data:
            rows = _normalize_chat_protocols(data)
            if not rows:
                raise HTTPException(status_code=400, detail="chat_protocols 不能为空：至少配置一条对话协议行")
            cfg["chat_protocols"] = rows
        cfg["tags"] = _normalize_provider_tags(data.get("tags"), strict=True)
    else:
        # 自定义渠道创建
        name = (data.get("name") or "").strip()
        if not name:
            name = _auto_generate_name(remark, existing_names)
        else:
            _validate_provider_name(name)
            # 名称冲突时自动生成新名称
            if name in existing_names:
                name = _auto_generate_name(remark or name, existing_names)

        if not data.get("accounts"):
            raise HTTPException(status_code=400, detail="自定义渠道必须至少添加一个账号")

        cfg = _custom_provider_config_from_payload(data)

    cfg["remark"] = cfg.get("remark") or remark or name
    _validate_provider_requires_base_url(name, cfg)
    initial_models = data.get("models")
    await _write_provider_config(name, cfg)
    if isinstance(initial_models, list):
        await PostgresClient.bulk_replace_provider_models(name, initial_models)
    # 新渠道立刻落一份显式限流策略行：冻结策略取全局配置的「新增渠道默认冻结策略」快照，
    # 之后改全局默认不影响本渠道。限流字段必须按 rate_limit 派生——显式策略行会屏蔽
    # _legacy_effective_policy 的派生路径，不派生就会把创建时填的 rpm/tpm 静默清零。
    await PostgresClient.upsert_provider_limit_policy(name, {
        "name": "default",
        "enabled": True,
        **policy_fields_from_rate_limit(cfg.get("rate_limit") or {}),
        "freeze_policy": get_default_freeze_policy(),
    })
    clear_policy_cache(name)
    await _load_provider_runtime(name, cfg)
    await _log_operation(token, "create_provider", "provider", name, None, cfg)
    from monkeycode_compat.notify_core import emit_notification_background
    emit_notification_background(
        "channel.created",
        params={"provider_name": name, "remark": cfg.get("remark") or ""},
        owner_type="platform",
        severity="info",
        dedupe_key=f"channel.created:{name}",
    )
    return {"ok": True, "name": name}


@router.post("/custom-providers/upstream-models")
async def preview_custom_provider_upstream_models(data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    models = await _fetch_unsaved_custom_provider_models(data)
    upstream_models = []
    for model in models:
        upstream_id = (model.get("id") or "").strip()
        if not upstream_id:
            continue
        upstream_models.append({
            "upstream_model_id": upstream_id,
            "name": model.get("name") or upstream_id,
            "tracked": False,
            "current_model_id": upstream_id,
            "raw": model.get("raw"),
        })
    return {"ok": True, "provider": data.get("name") or "custom-preview", "upstream_models": upstream_models, "models": upstream_models}


# 所有支持的内置渠道类型及其默认信息
# CLI 逆向渠道（copilot/codebuddy/atomcode/eaichat/qoder）已下架为「代码渠道」形态，
# 不再作为内置模板出现在加渠道目录里——spec 源码由使用者自行粘贴与分发。
_BUILTIN_PROVIDER_TYPES: dict[str, dict] = {
    "cloudflare": {"remark": "Cloudflare Workers AI", "description": "Cloudflare Workers AI", "doc_url": ""},
}


def _channel_catalog_builtin_entries() -> list[dict]:
    """Build system channel entries for the unified add-channel catalog."""
    entries: list[dict] = []
    from monkeycode_compat.marketplace.channel_catalog import _preset_from_config
    for name, info in _BUILTIN_PROVIDER_TYPES.items():
        cfg = _builtin_provider_default_config(name) or {}
        entries.append({
            "id": name,
            "name": str(info.get("remark") or name),
            "description": str(info.get("description") or ""),
            "category": "模型服务",
            "tags": [],
            "builtin_type": name,
            "preset": _preset_from_config(cfg),
        })
    return entries


@router.get("/channel-catalog")
async def get_channel_catalog(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    from monkeycode_compat.marketplace.channel_catalog import get_catalog
    return await get_catalog()


@router.post("/channel-catalog/refresh")
async def refresh_channel_catalog(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    from monkeycode_compat.marketplace.channel_catalog import refresh
    return await refresh()


@router.post("/providers/{name}/apply-template")
async def apply_provider_channel_template(
    name: str,
    data: dict,
    token: str = Header(None, alias="Authorization"),
):
    """Apply a marketplace channel template without reading or writing accounts."""
    operator = await _require_admin(token)
    template_id = str((data or {}).get("template_id") or "").strip()
    if not template_id:
        raise HTTPException(status_code=400, detail="template_id 必填")

    from channel_template_service import apply_channel_template
    from monkeycode_compat.marketplace import config as marketplace_config
    from monkeycode_compat.marketplace import urls as marketplace_urls
    from monkeycode_compat.marketplace.validator import safe_item_id

    settings = marketplace_config.consumer_settings
    if not settings.enabled or "channels" not in settings.modules:
        raise HTTPException(status_code=503, detail="消费侧渠道模板仓库未启用")
    safe_id = safe_item_id(template_id.replace("/", "."))
    if not safe_id:
        raise HTTPException(status_code=400, detail="template_id 非法")
    raw_url = marketplace_urls.consumer_raw_url(f"modules/channels/items/{safe_id}.json")
    try:
        # 与市场消费侧同一条出口：经 proxy_manager 用资源中心配置的 proxy_id 出网，
        # 避免「配了代理但只有部分链路生效」（见 marketplace/routes.py 消费侧注释）。
        import aiohttp
        from providers.proxy_manager import get_proxy_manager

        timeout = aiohttp.ClientTimeout(total=20)
        resp = await get_proxy_manager().request(
            url=raw_url,
            method="GET",
            headers={"User-Agent": "ai-lubricant-channel-template"},
            timeout=timeout,
            proxy_config_id=marketplace_config.settings.proxy_id or None,
        )
        if resp.status != 200:
            raise HTTPException(status_code=404, detail=f"渠道模板下载失败: HTTP {resp.status}")
        manifest = await resp.json()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"渠道模板下载失败: {exc}") from exc

    current = await _read_provider_base_config(name)
    old_config = dict(current)
    try:
        next_config, template_freeze_policy = apply_channel_template(current, manifest)
        normalized_freeze_policy = validate_freeze_policy(template_freeze_policy)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # 基础配置窄写明确不触碰 provider_accounts；账号数量只通过 COUNT 获取。
    await _write_provider_base(name, next_config)
    old_policy = await get_effective_provider_policy(name, refresh=True)
    policy_payload = dict(old_policy)
    policy_payload["name"] = policy_payload.get("name") or "default"
    policy_payload["freeze_policy"] = normalized_freeze_policy
    policy = await PostgresClient.upsert_provider_limit_policy(name, policy_payload)
    clear_policy_cache(name)
    await get_effective_provider_policy(name, refresh=True)
    _apply_policy_to_runtime(name, policy)

    pool = ModelClientPool.get_provider_pool(name)
    if pool and pool.channel is not None:
        pool.channel.apply(_provider_extra(name, next_config))

    accounts_count = await PostgresClient.count_provider_accounts(name)
    await _log_operation(
        operator,
        "apply_channel_template",
        "provider",
        name,
        old_config,
        {
            "template_id": next_config.get("channel_template_id"),
            "template_version": next_config.get("channel_template_version"),
            "channel_config_revision": next_config.get("channel_config_revision"),
            "accounts_preserved": True,
            "accounts_count": accounts_count,
        },
    )
    return {
        "ok": True,
        "provider": name,
        "channel_template": {
            "id": next_config.get("channel_template_id"),
            "version": next_config.get("channel_template_version"),
            "applied_at": next_config.get("channel_template_applied_at"),
        },
        "channel_config_revision": next_config.get("channel_config_revision"),
        "accounts_preserved": True,
        "accounts_count": accounts_count,
    }


@router.get("/config/versions")
async def get_configuration_versions(token: str = Header(None, alias="Authorization")):
    """Aggregate channel configuration versions without returning account data."""
    await _require_admin(token)
    providers = await config.Config.get_providers()
    items = []
    for name in sorted(providers):
        try:
            base = await _read_provider_base_config(name)
        except HTTPException:
            continue
        items.append({
            "provider": name,
            "display_name": base.get("remark") or name,
            "channel_template": {
                "id": base.get("channel_template_id") or "",
                "version": base.get("channel_template_version") or "",
                "applied_at": base.get("channel_template_applied_at") or "",
            },
            "channel_config_revision": int(base.get("channel_config_revision") or 0),
            "config_updated_at": base.get("updated_at") or "",
            "models_count": await PostgresClient.count_provider_models(name),
            "accounts_count": await PostgresClient.count_provider_accounts(name),
        })
    return {"providers": items}


@router.get("/builtin-providers")
async def list_builtin_providers(token: str = Header(None, alias="Authorization")):
    """返回所有可供创建的内置渠道类型，标记已创建状态。"""
    await _require_admin(token)
    existing = await config.Config.get_providers()
    result = []
    for name, info in _BUILTIN_PROVIDER_TYPES.items():
        cfg = existing.get(name)
        created = bool(cfg and not _is_custom_provider_config(cfg))
        result.append({**info, "name": name, "created": created})
    return result


@router.get("/custom-providers")
async def list_custom_providers(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    providers_config = await config.Config.get_providers()
    return [
        {"name": name, **cfg}
        for name, cfg in providers_config.items()
        if _is_custom_provider_config(cfg)
    ]




def _scrub_relay_secret(value):
    if isinstance(value, dict):
        scrubbed = {}
        for k, v in value.items():
            key = str(k).lower()
            secret_key = (
                key in {"key", "cookies", "_cookies", "authorization", "new-api-user", "access_token", "user_id"}
                or any(word in key for word in ("password", "token", "api_key", "apikey", "secret", "cookie", "authorization"))
            )
            if secret_key:
                scrubbed[k] = _mask_secret(v) if not isinstance(v, (dict, list)) else "********"
            else:
                scrubbed[k] = _scrub_relay_secret(v)
        return scrubbed
    if isinstance(value, list):
        return [_scrub_relay_secret(v) for v in value]
    return value


# ==================== Provider 操作 (热更新) ====================


async def _check_provider_accounts_in_background(name: str, pool) -> None:
    try:
        await pool.check_account(force=True)
    except Exception:
        logger.exception(f"渠道 {name} 启用后的账号状态检查失败")


async def _init_and_check_provider_in_background(name: str, pool) -> None:
    """渠道从禁用改启用后的后台初始化 + 体检。

    重新启用前账号可能从未 init_auth（init_all/check_account 在渠道禁用时整体跳过），
    auth_ok 停在 None → 前端显示「待检查」久久不消。仅 check_account 对人工凭据渠道
    只跑 health_check、对 is_init()==False 的账号直接判失败，不会触发 init_auth。
    这里先 init_all（与启动期路径一致）把未初始化账号补上 init_auth，再 check_account
    体检，使 auth_ok 从 None 翻成 True/False，状态滑出「待检查」。
    """
    try:
        await pool.init_all(use_invitation_interval=False)
        await pool.check_account(force=True)
    except Exception:
        logger.exception(f"渠道 {name} 启用后的账号初始化/体检失败")


@router.put("/providers/{name}/enabled")
async def toggle_provider(name: str, enabled: bool, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cfg = await _read_provider_config(name)
    old_cfg = dict(cfg)
    was_enabled = cfg.get("enabled", True) is not False
    cfg["enabled"] = enabled
    await _write_provider_base(name, cfg)
    # 只翻转渠道启用状态：运行池账号不重建，正式发送/定时任务从 pool.channel.enabled 读取。
    pool = ModelClientPool.get_provider_pool(name)
    if pool and pool.channel is not None:
        pool.channel.apply(_provider_extra(name, cfg))
    if pool and not was_enabled and enabled:
        # 重新启用：先补 init_auth 再体检，避免账号停在「待检查」（auth_ok=None）。
        asyncio.create_task(_init_and_check_provider_in_background(name, pool))
    await _log_operation(token, "toggle_provider", "provider", name, old_cfg, {"enabled": enabled})
    return {"ok": True}


@router.get("/providers/{name}/models")
async def list_provider_models_endpoint(
    name: str,
    lite: bool = False,
    authorization: Optional[str] = Header(None),
):
    """读取该渠道在 provider_models 表中的全部行。"""
    await _require_admin(authorization)
    rows = await PostgresClient.list_provider_models(name, lite=lite)
    return {"ok": True, "provider": name, "models": rows}


@router.put("/providers/{name}/models")
async def replace_provider_models_endpoint(name: str, data: dict, authorization: Optional[str] = Header(None)):
    """事务整覆盖该渠道的模型行。

    body: {"models": [{"upstream_model_id": str, "model_id": str}, ...]}
    覆盖后立即触发 refresh_models() 重建内存表（不打上游）。

    models 字段缺失时跳过模型表更新（前端模型列表未加载完成时用此语义，
    避免用空 state 误清表）；models 为数组（含空数组）时正常整覆盖。
    """
    await _require_admin(authorization)
    if "models" not in data:
        # 前端未拿到权威模型数据，本次只算配置保存，不动 provider_models 表。
        return {"ok": True, "count": None, "skipped": True}
    payload = data["models"]
    if not isinstance(payload, list):
        raise HTTPException(status_code=400, detail="models 必须是数组")
    rows: list[dict] = []
    seen: set[str] = set()
    for item in payload:
        if not isinstance(item, dict):
            continue
        upstream_id = (item.get("upstream_model_id") or "").strip()
        if not upstream_id or upstream_id in seen:
            continue
        seen.add(upstream_id)
        model_id = (item.get("model_id") or "").strip() or upstream_id
        rows.append({
            "upstream_model_id": upstream_id,
            "model_id": model_id,
            "extra_config": item.get("extra_config") if isinstance(item.get("extra_config"), dict) else {},
        })
    await PostgresClient.bulk_replace_provider_models(name, rows)
    await _reload_provider_models_local(name)
    await _log_operation(authorization, "replace_provider_models", "provider", name, None, {"count": len(rows), "model_ids": [r["model_id"] for r in rows]})
    return {"ok": True, "count": len(rows)}



@router.post("/providers/{name}/models")
async def upsert_provider_model_endpoint(name: str, data: dict, authorization: Optional[str] = Header(None)):
    """新增或重命名单行。body: {upstream_model_id, model_id?}"""
    await _require_admin(authorization)
    upstream_id = (data.get("upstream_model_id") or "").strip()
    if not upstream_id:
        raise HTTPException(status_code=400, detail="upstream_model_id 必填")
    model_id = (data.get("model_id") or "").strip() or upstream_id
    await PostgresClient.upsert_provider_model(
        name,
        upstream_id,
        model_id,
        extra_config=data.get("extra_config") if isinstance(data.get("extra_config"), dict) else {},
    )
    await _reload_provider_models_local(name)
    await _log_operation(authorization, "upsert_provider_model", "provider", name, None, {"upstream_model_id": upstream_id, "model_id": model_id})
    return {"ok": True}


@router.delete("/providers/{name}/models/{upstream_model_id:path}")
async def delete_provider_model_endpoint(name: str, upstream_model_id: str, authorization: Optional[str] = Header(None)):
    await _require_admin(authorization)
    ok = await PostgresClient.delete_provider_model(name, upstream_model_id)
    await _reload_provider_models_local(name)
    await _log_operation(authorization, "delete_provider_model", "provider", name, {"upstream_model_id": upstream_model_id}, None)
    return {"ok": ok}


@router.post("/providers/{name}/refresh-models")
async def refresh_provider_models(name: str, authorization: Optional[str] = Header(None)):
    """手动刷新模型列表"""
    await _require_admin(authorization)
    if not await _ensure_provider_runtime(name):
        raise HTTPException(status_code=404, detail=f"渠道 '{name}' 不存在")
    upstream = await ModelClientPool.fetch_provider_upstream_models(name)
    await ModelClientPool.refresh_models()
    await _log_operation(authorization, "refresh_provider_models", "provider", name, None, {"count": upstream.get("count") if isinstance(upstream, dict) else None})
    return {"ok": True, **upstream}


@router.get("/providers/{name}/upstream-models")
async def get_provider_upstream_models(name: str, authorization: Optional[str] = Header(None)):
    """获取渠道上游全量模型列表，用于管理白名单。

    每行同时返回 model_id（套改写规则后的短名）与 raw_model_id（原始上游名），
    前端按开关在两者间切换显示，无需重新拉取上游。
    """
    await _require_admin(authorization)
    if not await _ensure_provider_runtime(name):
        raise HTTPException(status_code=404, detail=f"渠道 '{name}' 不存在")
    return {"ok": True, **await ModelClientPool.fetch_provider_upstream_models(name)}


@router.post("/providers/{name}/health-check")
async def health_check_provider(name: str, authorization: Optional[str] = Header(None)):
    """手动检测渠道可用性"""
    await _require_admin(authorization)
    if not await _ensure_provider_runtime(name):
        raise HTTPException(status_code=404, detail=f"渠道 '{name}' 不存在")
    pool = ModelClientPool.get_provider_pool(name)
    if not pool:
        raise HTTPException(status_code=404, detail=f"渠道 '{name}' 不存在")
    results = []
    for c in pool.clients:
        ok = await c.provider.check_auth()
        c.auth_ok = ok
        c.auth_checked_at = time.time()
        c.auth_error = "" if ok else "检测失败"
        results.append({"username": c.username, "ok": ok, "error": c.auth_error})
    await _log_operation(authorization, "health_check_provider", "provider", name, None, {"accounts": results})
    return {"ok": True, "accounts": results}


@router.post("/providers/{name}/scheduled-test/run-now")
async def trigger_scheduled_test_now(name: str, body: dict = Body(default_factory=dict), token: str = Header(None, alias="Authorization")):
    """立即触发一次定时检测：把当前表单参数覆盖到已保存配置上，算出候选账号并入检测队列，
    由 scheduled_test_loop 的 worker 异步消费，不阻塞调用方。返回入队账号数与触发时间戳。
    """
    await _require_admin(token)
    if not await _ensure_provider_runtime(name):
        raise HTTPException(status_code=404, detail=f"渠道 '{name}' 不存在")
    try:
        enqueued, triggered_at = await ModelClientPool.trigger_scheduled_test_now(name, body or {})
    except Exception as e:
        return {"ok": False, "enqueued": 0, "triggered_at": None, "detail": f"触发失败: {e}"}
    if enqueued < 0:
        return {"ok": False, "enqueued": 0, "triggered_at": None, "detail": "检测队列尚未就绪，请稍后重试"}
    await _log_operation(token, "scheduled_test_run_now", "provider", name, None, {"enqueued": enqueued})
    detail = "已加入检测队列" if enqueued > 0 else "当前无候选账号（按检测范围与账号状态筛选后为空）"
    return {"ok": True, "enqueued": enqueued, "triggered_at": triggered_at, "detail": detail}


@router.post("/fetch-models")
async def fetch_models_endpoint(data: dict, token: str = Header(None, alias="Authorization")):
    """从上游 API 临时拉取模型列表（创建自定义渠道向导用）。"""
    await _require_admin(token)
    base_url = (data.get("base_url") or "").rstrip("/")
    models_path = data.get("models_path", "/v1/models")
    api_key = data.get("api_key", "")
    if not base_url:
        raise HTTPException(status_code=400, detail="base_url 必填")
    import aiohttp
    from providers.base import make_insecure_connector, format_aiohttp_error
    try:
        async with aiohttp.ClientSession(connector=make_insecure_connector()) as session:
            headers = {}
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"
            async with session.get(base_url + models_path, headers=headers, timeout=aiohttp.ClientTimeout(total=30), ssl=False) as response:
                text = await response.text()
                if response.status != 200:
                    raise HTTPException(status_code=502, detail=f"上游返回 HTTP {response.status}: body_len={len(text)}")
                result = json.loads(text)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"请求失败: {format_aiohttp_error(e, base_url + models_path)}") from e
    raw_models = result.get("data") if isinstance(result, dict) else result
    models = []
    for item in (raw_models or []):
        if isinstance(item, str):
            models.append({"id": item, "name": item})
        elif isinstance(item, dict):
            mid = item.get("id") or item.get("name") or item.get("model")
            if mid:
                models.append({"id": mid, "name": item.get("name", mid)})
    return {"ok": True, "models": models}


@router.put("/providers/{name}/custom")
async def update_custom_provider_config(name: str, data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    try:
        cfg = await _read_provider_config(name)
        old_cfg = _scrub_relay_secret(dict(cfg))
        if "supports_stream" in data and "upstream_stream" not in data:
            data["upstream_stream"] = data["supports_stream"]
        changed_keys: set[str] = set()
        if "retry_count" in data:
            if data.get("retry_count") is None:
                data.pop("retry_count", None)
            else:
                try:
                    v = int(data["retry_count"])
                except (TypeError, ValueError):
                    raise HTTPException(status_code=400, detail="retry_count 必须为整数")
                if v < 0 or v > 10:
                    raise HTTPException(status_code=400, detail="retry_count 需在 0-10 之间")
                data["retry_count"] = v
        if "extra_retry_status_codes" in data:
            data["extra_retry_status_codes"] = _normalize_extra_retry_status_codes(data.get("extra_retry_status_codes"))
        if "model_id_rewrite_rules" in data:
            data["model_id_rewrite_rules"] = _normalize_model_id_rewrite_rules(data.get("model_id_rewrite_rules"))
        if "tags" in data:
            data["tags"] = _normalize_provider_tags(data.get("tags"), strict=True)
        if "website_url" in data:
            data["website_url"] = _normalize_provider_website_url(data.get("website_url"))
        if "icon" in data:
            data["icon"] = _normalize_provider_icon(data.get("icon"))
        for key in (
            "remark", "tags", "price_remark", "base_url", "models_path", "image_path", "video_path", "speech_path",
            "supports_image_generation", "supports_video_generation", "supports_tts",
            "account_priority", "account_weight", "auto_update_models", "model_id_rewrite_rules",
            "timeout", "retry_count", "extra_retry_status_codes", "health_check", "scheduled_test", "chat_protocols", "rate_limit", "billing_mode", "balance",
            "website_url", "icon", "code", "clear_conversation",
        ):
            if key in data:
                cfg[key] = data[key]
                changed_keys.add(key)
        if "chat_protocols" in changed_keys:
            cfg["chat_protocols"] = _normalize_chat_protocols(cfg)
            # 顶层 protocol/chat_path/upstream_stream/client_preset 不再写入；协议行才是真相源。
            for legacy_key in ("protocol", "chat_path", "upstream_stream", "client_preset", "supports_stream"):
                cfg.pop(legacy_key, None)
        # 渠道地址必填校验：改了 base_url / code 都可能让渠道从「有地址」变「没地址」，写库前拦。
        if {"base_url", "code"} & changed_keys:
            _validate_provider_requires_base_url(name, cfg)
        # 只改渠道基础字段，不碰 accounts 表——窄写 provider_configs 行。
        await _write_provider_base(name, cfg)
        if "billing_mode" in changed_keys:
            ModelClientPool._provider_billing_mode[name] = cfg.get("billing_mode", "token")

        # 热更新：只更新现有 provider/AccountClient 字段，不重建账号池
        # 保留 RPM/TPM 窗口、冷却、in_flight、auth 缓存等运行时状态。
        # 标签是运行时选路的筛选维度（rate_limiter 按 channel.tags 现取现判），
        # 改标签必须刷新本机 pool.channel 快照，否则本机要等 60s 对账才生效——
        # 故不再把 tags 排除在热更新之外。
        if changed_keys:
            # 代码渠道改 code = 换 Provider 类（loader exec 出新类），
            # _hot_update_custom_provider 的就地字段更新处理不了"换类"，必须完整重载。
            needs_full_reload = "code" in changed_keys and cfg.get("builtin_type") == "code"
            pool = ModelClientPool.get_provider_pool(name)
            if pool and not needs_full_reload:
                _hot_update_custom_provider(name, cfg, changed_keys)
            else:
                # 就地更新处理不了 / 渠道还没初始化 / 换了 code 类 —— 走完整加载
                await _load_provider_runtime(name, cfg)
        await _log_operation(token, "update_custom_provider", "provider", name, old_cfg, _scrub_relay_secret(dict(cfg)))
        return {"ok": True}
    except HTTPException as e:
        # uvicorn 访问日志只记状态码，HTTPException.detail 只进响应体、不落控制台。
        # 这里显式打印 detail + 提交字段，方便定位 400 具体原因。
        logger.warning(
            f"[update_custom_provider] name={name} 失败 {e.status_code}: {e.detail} | "
            f"payload_keys={sorted(data.keys())} | "
            f"chat_protocols={json.dumps(data.get('chat_protocols'), ensure_ascii=False)}"
        )
        raise
    except Exception:
        logger.exception(f"[update_custom_provider] name={name} 未预期异常 | payload_keys={sorted(data.keys())}")
        raise


# ==================== 账号操作 (热更新) ====================


@router.put("/providers/{name}/accounts/add")
async def add_account(name: str, data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cfg = await _read_provider_config(name)
    data.setdefault("switch", True)
    data = _normalize_account_proxy_ref(data, _read_proxies())
    await _persist_account(name, data)

    if ModelClientPool.get_provider_pool(name):
        _reload_account_into_pool(name, data, _provider_rpm(cfg), _provider_extra(name, cfg))
    else:
        # 渠道配置存在但运行池缺失（如历史配置、热更新遗漏）— 走完整加载
        await _load_provider_runtime(name, cfg)
    await _log_operation(token, "add_account", "account", data.get("username"), None, {"provider": name, "account": data})
    from monkeycode_compat.notify_core import emit_notification_background
    emit_notification_background(
        "account.created",
        params={"provider_name": name, "account_username": data.get("username") or ""},
        owner_type="platform",
        severity="info",
    )
    return {"ok": True, "username": data.get("username")}


def _demask_custom_account_credentials(provider_cls, acc: dict, data: dict, merged: dict) -> None:
    """消解 CustomProvider 系渠道 DB 行里的陈旧 api_key/key 遮蔽。

    CustomProvider init 取值优先级 api_key > key > password（providers/custom.py:275）。
    前端编辑账号时新凭据写入 password 字段，但旧 api_key/key 会作为陈旧回显被连带回传，
    或被 update_account 的 secret 保留逻辑补回，于是 DB 行同时留有新 password 和旧 api_key。
    重启初始化取 api_key → 新 password 被永久遮蔽（表现为「改了密钥、重启仍不生效」）。
    这里在持久化前：password 确有变化、且没有真正新填的 api_key/key 时，清掉陈旧的
    api_key/key，让 password 成为运行时凭据。OAuth 系（password=refresh_token）无 api_key，
    pop 为空操作，安全。"""
    if not issubclass(provider_cls, CustomProvider):
        return
    new_pw = (data.get("password") or "").strip()
    old_pw = acc.get("password") or ""
    explicit_key = (data.get("api_key") or data.get("key") or "").strip()
    old_key = acc.get("api_key") or acc.get("key") or ""
    if new_pw and new_pw != old_pw and (not explicit_key or explicit_key == old_key):
        merged.pop("api_key", None)
        merged.pop("key", None)


@router.put("/providers/{name}/accounts/{username}")
async def update_account(name: str, username: str, data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cfg = await _read_provider_config(name)
    proxy_was_explicitly_cleared = (
        ("proxy" in data or "proxy_id" in data)
        and not (data.get("proxy_id") or data.get("proxy") or "").strip()
    )
    data = _normalize_account_proxy_ref(data, _read_proxies())
    secret_keys = {
        "password",
        "token",
        "access_token",
        "refresh_token",
        "id_token",
        "github_token",
        "api_key",
        "authorization",
        "cookie",
        "cookies",
        "session",
        "access_token_expires_at_ms",
        "secure_1psid",
        "secure_1psidts",
    }
    for i, acc in enumerate(cfg.get("accounts", [])):
        if acc.get("username") == username:
            merged = dict(acc)
            merged.update(data)
            # 前端编辑账号时通常不会回传 password/token 等 secret 字段；
            # 不能用表单数据整体替换账号，否则 OAuth/设备码授权得到的 token 会被清空。
            for key in secret_keys:
                if acc.get(key) not in (None, "") and data.get(key) in (None, ""):
                    merged[key] = acc.get(key)
            if proxy_was_explicitly_cleared:
                merged.pop("proxy_id", None)
                merged.pop("proxy", None)
            # 自定义系渠道：消解 DB 里的陈旧 api_key/key 遮蔽，避免重启后 password 被覆盖。
            _demask_custom_account_credentials(_get_provider_class(name, cfg), acc, data, merged)
            # 改名：先删旧账号行（DB + 内存 + 广播），再写新账号行。
            if merged.get("username") != username:
                await _remove_account_row(name, username)
                _remove_account_from_pool(name, username)
            await _persist_account(name, merged)
            _reload_account_into_pool(name, merged, _provider_rpm(cfg), _provider_extra(name, cfg))
            await _log_operation(token, "update_account", "account", username, _scrub_relay_secret(dict(acc)), _scrub_relay_secret(dict(merged)))
            return {"ok": True}
    raise HTTPException(status_code=404, detail=f"账号 '{username}' 不存在")


@router.delete("/providers/{name}/accounts/{username}")
async def delete_account(name: str, username: str, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cfg = await _read_provider_config(name)
    accounts = cfg.get("accounts", [])
    removed_account = next((a for a in accounts if a.get("username") == username), None)
    if removed_account is None:
        raise HTTPException(status_code=404, detail=f"账号 '{username}' 不存在")
    # 删除前落备份：被删账号 + 当时完整渠道配置（含账号列表），供误删恢复。
    _write_delete_backup("account", f"{name}-{username}", {
        "provider": name,
        "username": username,
        "account": removed_account,
        "provider_config": cfg,
    })
    await _remove_account_row(name, username)
    _remove_account_from_pool(name, username)
    await _cleanup_account_routes(name, username)
    await _log_operation(token, "delete_account", "account", username, {"provider": name, "account": accounts}, None)
    return {"ok": True}


@router.put("/providers/{name}/accounts/{username}/switch")
async def toggle_account_switch(name: str, username: str, switch: bool, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cfg = await _read_provider_config(name)
    found = None
    for acc in cfg.get("accounts", []):
        if acc.get("username") == username:
            acc["switch"] = switch
            found = dict(acc)
            break
    if found is None:
        raise HTTPException(status_code=404, detail=f"账号 '{username}' 不存在")
    await _persist_account(name, found)

    # switch 只翻转账号发送门禁，不移除运行时账号；手动测试/拉模型仍可直接使用。
    if not _set_account_disabled(name, username, not switch):
        _reload_account_into_pool(name, found, _provider_rpm(cfg), _provider_extra(name, cfg))
    await _log_operation(token, "toggle_account_switch", "account", username, {"provider": name}, {"switch": switch})
    return {"ok": True}


@router.post("/providers/{name}/check-accounts")
async def check_provider_accounts(name: str, authorization: Optional[str] = Header(None)):
    """手动检查渠道所有已启用账号的状态"""
    await _require_admin(authorization)
    pool = ModelClientPool.get_provider_pool(name)
    if not pool:
        raise HTTPException(status_code=404, detail=f"渠道 '{name}' 不存在")
    await pool.check_account(force=True)
    await _log_operation(authorization, "check_provider_accounts", "provider", name, None, {"ok": True})
    return {"ok": True, "message": "账号状态检查已完成"}


def _format_admin_error(error) -> str:
    def _format_message(message) -> str:
        text = str(message or "")
        if text == "Expecting value: line 1 column 1 (char 0)":
            return "上游响应无法解析为 JSON：响应体为空或 SSE data 后没有有效 JSON（原始错误：Expecting value: line 1 column 1 (char 0)）"
        return text

    if isinstance(error, dict):
        nested = error.get("error")
        if isinstance(nested, dict):
            return _format_message(nested.get("message") or nested.get("detail") or nested)
        return _format_message(error.get("message") or error.get("detail") or error)
    return _format_message(error)


def _redact_test_diagnostics(value):
    """遮蔽 body 内的凭据字段。

    只认精确键名：body 里 max_tokens/prompt_tokens 等含 "token" 的字段是用量数字，
    子串匹配会把它们也遮成星号。header 走 _redact_test_headers。
    """
    sensitive_keys = ("authorization", "cookie", "token", "api-key", "apikey", "secret", "credential", "password")
    if isinstance(value, dict):
        return {
            str(key): "***"
            if str(key).lower() in sensitive_keys
            else _redact_test_diagnostics(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_test_diagnostics(item) for item in value]
    return value


def _redact_test_headers(headers):
    """遮蔽请求头 —— 认证头保留首尾片段，便于认出用的是哪把 key。

    与渠道请求日志同一套判定（security.is_sensitive_header），各上游协议的认证头
    名字不同（x-api-key / x-goog-api-key / Authorization）都能覆盖。
    """
    from security import is_sensitive_header, sanitize_header_value

    if not isinstance(headers, dict):
        return _redact_test_diagnostics(headers)
    return {
        str(key): sanitize_header_value(key, item)
        if is_sensitive_header(key)
        else item
        for key, item in headers.items()
    }


def _test_failure_diagnostics(built: dict, route_info: dict, error: str) -> dict:
    return {
        "request": _redact_test_diagnostics(built.get("body") or {}),
        "request_headers": _redact_test_headers(built.get("headers") or {}),
        "router_request_path": route_info.get("router_request_path"),
        "router_request": _redact_test_diagnostics(route_info.get("router_request_body")),
        "router_response": _redact_test_diagnostics(route_info.get("router_response_body")),
        "upstream_status": route_info.get("upstream_status"),
        "error": error,
    }


# 不同服务商在「超出上下文长度」时报错时的常见文本模式，统一抽出 (model max context length) -> intercept number
_CONTEXT_LIMIT_PATTERNS = [
    re.compile(r"(?:maximum |max\.? )?context length[^0-9]{0,40}?(\d{4,})\s*tokens?", re.I),
    re.compile(r"max(?:imum)?[^0-9]{0,12}?(\d{4,})[^0-9]{0,4}?tokens?", re.I),
    re.compile(r"(\d{4,})\s*tokens?[^0-9]{0,4}?limit", re.I),
    re.compile(r"context[_\s-]*window[^0-9]{0,40}?(\d{4,})", re.I),
    re.compile(r"(?:supports?|supports up to)\s+(\d{4,})\s+tokens?", re.I),
    re.compile(r"(?:令牌|上下文|最大)[^\d]{0,18}?(\d{4,})", re.I),
]


# 窗口真实大小测试：用户选择/输入的「测试窗口大小」即本次实际请求的目标 token 数。
# 不再用声称上下文的相对比例（声称值可能虚标），直接按指定窗口发送请求验证模型能否处理。
# 保留 4k/8k/16k/32k 旧键以向前兼容老客户端，同时扩展更大窗口预设。
WINDOW_SIZES: dict[str, int] = {
    "4k": 4000,
    "8k": 8000,
    "16k": 16000,
    "32k": 32000,
    "64k": 64000,
    "128k": 128000,
    "200k": 200000,
    "256k": 256000,
    "512k": 512000,
    "1m": 1_000_000,
}

# 窗口测试 token 数的安全边界：低于下限没意义，高于上限会拼出过大请求体拖垮上游/数据库。
_WINDOW_MIN_TOKENS = 1000
_WINDOW_MAX_TOKENS = 1_000_000


def _parse_window_tokens(raw) -> tuple[int | None, str | None]:
    """解析窗口大小输入为正整数 token 数。

    接受预设键（4k/8k/.../1m，大小写不敏感）、纯数字字符串/整数（如 32768）、
    以及带 k/m 后缀的写法（如 256k、1.5m 暂不支持小数，仅整数倍）。
    返回 (tokens, error)：成功时 error=None；失败时 tokens=None、error 为中文原因。
    """
    if raw is None:
        return None, "未提供测试窗口大小"
    text = str(raw).strip().lower()
    if not text:
        return None, "未提供测试窗口大小"

    # 预设键优先
    if text in WINDOW_SIZES:
        return WINDOW_SIZES[text], None

    # 带后缀：256k / 1m
    if text.endswith("k") or text.endswith("m"):
        suffix = text[-1]
        digits = text[:-1].replace(",", "").strip()
        if not digits.isdigit():
            return None, "窗口大小格式无法识别"
        base = int(digits)
        tokens = base * (1000 if suffix == "k" else 1_000_000)
    elif text.replace(",", "").isdigit():
        tokens = int(text.replace(",", ""))
    else:
        return None, "窗口大小必须是数字或预设键（如 8k、128k、1m）"

    if tokens < _WINDOW_MIN_TOKENS:
        return None, f"窗口大小不能小于 {_WINDOW_MIN_TOKENS} tokens"
    if tokens > _WINDOW_MAX_TOKENS:
        return None, f"窗口大小不能超过 {_WINDOW_MAX_TOKENS:,} tokens"
    return tokens, None


def _extract_context_limit(error_text) -> int | None:
    """从上游错误文本中提取实际允许的最大 token 数；找不到返回 None。"""
    text = _format_admin_error(error_text) if not isinstance(error_text, str) else str(error_text or "")
    if not text:
        return None
    for pat in _CONTEXT_LIMIT_PATTERNS:
        m = pat.search(text)
        if m:
            try:
                value = int(m.group(1))
            except (TypeError, ValueError):
                continue
            if value >= 1024:
                return value
    return None


def _collect_text_parts(value, parts: list[str], max_parts: int = 80) -> None:
    if len(parts) >= max_parts or value is None:
        return
    if isinstance(value, str):
        text = value.strip()
        if len(text) >= 20:
            parts.append(text[:12000])
        return
    if isinstance(value, list):
        for item in value:
            _collect_text_parts(item, parts, max_parts)
            if len(parts) >= max_parts:
                break
        return
    if isinstance(value, dict):
        for key in ("content", "text", "input", "prompt", "message"):
            if key in value:
                _collect_text_parts(value.get(key), parts, max_parts)
        messages = value.get("messages")
        if isinstance(messages, list):
            for message in messages:
                if isinstance(message, dict):
                    _collect_text_parts(message.get("content"), parts, max_parts)
                    if len(parts) >= max_parts:
                        break


async def _build_context_probe_messages(target_tokens: int) -> list[dict]:
    """按约 2 字符/token 拼接自然的大消息；这是跨模型 tokenizer 的近似目标值。"""
    target_chars = max(12000, target_tokens * 2)
    snippets: list[str] = []
    if PostgresClient.pool:
        async with PostgresClient.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT request_body FROM request_logs
                WHERE request_body IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 200
                """
            )
        for row in rows:
            body = row["request_body"]
            if isinstance(body, str):
                try:
                    body = json.loads(body)
                except Exception:
                    pass
            _collect_text_parts(body, snippets)
            if sum(len(s) for s in snippets) >= target_chars:
                break

    if not snippets:
        snippets = [
            "请阅读下面的项目资料、接口记录、用户反馈和排查笔记，并在最后给出结构化总结。资料包含多段历史上下文，请不要忽略任何细节。",
            "背景：系统需要在多个模型渠道之间做路由、日志追踪、错误归因、用量统计和管理端展示。下面是若干条历史请求、排查记录和配置说明的混合文本。",
            "任务要求：逐段归纳关键事实，识别重复信息，保留所有数字、模型名、接口路径、错误码、时间、账号和渠道相关线索。",
            "分析时请按模块拆分：渠道配置、模型映射、请求日志、用量统计、重试策略、前端交互、数据库字段、测试覆盖、风险点和后续建议。",
        ]

    chunks: list[str] = []
    total = 0
    i = 0
    while total < target_chars:
        src = snippets[i % len(snippets)]
        block = f"资料片段 {i + 1}:\n{src}\n\n请结合前后文继续分析这一片段中的配置、模型、接口、错误和用量线索。"
        chunks.append(block)
        total += len(block)
        i += 1

    messages = [{"role": "system", "content": "你是一个严谨的技术分析助手，需要处理大量真实项目资料并给出准确总结。"}]
    buffer: list[str] = []
    buffer_len = 0
    for block in chunks:
        buffer.append(block)
        buffer_len += len(block)
        if buffer_len >= 16000:
            messages.append({"role": "user", "content": "\n\n".join(buffer)})
            buffer = []
            buffer_len = 0
    if buffer:
        messages.append({"role": "user", "content": "\n\n".join(buffer)})
    return messages


async def _run_fallback_probe(client, model_id: str, target_tokens: int, fallback_size: str, provider_name: str, username: str):
    """降级探测：用「确定能处理」的正常大小请求验证模型连通性。

    超大探测未能解析出上下文上限时调用。流式、max_tokens 1024，chat 循环正常
    结束即视为「模型能处理该大小」。返回降级结果字典。

    日志与主探测同口径：先插入 requesting 行、结束后 update，api_key_name 统一为
    admin-detect-model（不做区分），与普通请求日志一致。
    """
    fb_messages = await _build_context_probe_messages(target_tokens)
    fb_request_body = {
        "model": model_id,
        "messages": fb_messages,
        "max_tokens": 1024,
        "stream": True,
    }
    fb_log_data = {
        "request_id": f"req_{uuid.uuid4().hex[:12]}",
        "api_key_name": "admin-detect-model",
        "provider_name": provider_name,
        "account_username": username,
        "model": model_id,
        "actual_model": model_id,
        "endpoint": f"/admin/providers/{provider_name}/accounts/detect-model",
        "success": False,
        "status": "requesting",
        "stream": True,
        "request_body": fb_request_body,
    }
    fb_log_id = await PostgresClient.insert_request_log(fb_log_data)

    fb_upstream_headers: dict = {}
    fb_router_request_headers: dict = {}
    fb_router_request_body: dict | None = None
    fb_router_response_body = None

    def _fb_resp_headers(headers):
        fb_upstream_headers.clear()
        if isinstance(headers, dict):
            fb_upstream_headers.update(headers)
        status = (headers or {}).get(":status") if isinstance(headers, dict) else None
        if status:
            fb_log_data["upstream_status"] = str(status)

    def _fb_req_headers(headers):
        from security import sanitize_header_value

        fb_router_request_headers.clear()
        if isinstance(headers, dict):
            fb_router_request_headers.update({
                str(k): sanitize_header_value(k, v)
                for k, v in headers.items()
            })

    def _fb_req_body(payload):
        nonlocal fb_router_request_body
        fb_router_request_body = payload

    def _fb_resp_body(payload):
        nonlocal fb_router_response_body
        if fb_router_response_body is None:
            fb_router_response_body = payload
        elif isinstance(fb_router_response_body, list):
            fb_router_response_body.append(payload)
        else:
            fb_router_response_body = [fb_router_response_body, payload]

    fb_start = time.time()
    fb_ok = False
    fb_err = ""
    fb_sample = ""
    try:
        async for chunk in client.provider.chat(
            model_id, fb_messages, stream=True,
            response_headers_callback=_fb_resp_headers,
            router_request_headers_callback=_fb_req_headers,
            router_request_body_callback=_fb_req_body,
            router_response_body_callback=_fb_resp_body,
        ):
            if isinstance(chunk, dict):
                if not fb_sample:
                    fb_sample = _extract_sample_text(chunk)
                fb_router_response_body = chunk
            elif isinstance(chunk, str):
                # 流式 chunk 是 SSE 文本，作为成功佐证保留，不当作错误
                if not fb_sample:
                    fb_sample = chunk[:500]
                fb_router_response_body = chunk
        # 循环正常结束：模型能处理该大小
        fb_ok = True
    except HTTPException as e:
        fb_err = _format_admin_error(getattr(e, "detail", str(e)) or str(e))
    except Exception as e:
        fb_err = _format_admin_error(getattr(e, "message", str(e)) or str(e))

    fb_duration = int((time.time() - fb_start) * 1000)
    fallback: dict = {
        "size": fallback_size,
        "target_tokens": target_tokens,
        "ok": fb_ok,
        "duration_ms": fb_duration,
    }
    if fb_err:
        fallback["error_message"] = fb_err
    if fb_sample:
        fallback["sample"] = fb_sample[:500]

    fb_final: dict = {
        "success": fb_ok,
        "status": "ok" if fb_ok else "error",
        "duration_ms": fb_duration,
        "actual_model": model_id,
    }
    if fb_ok:
        fb_final["response_body"] = {"size": fallback_size, "target_tokens": target_tokens, "ok": True, "sample": fallback.get("sample")}
    else:
        fb_final["error"] = fb_err
        fb_final["response_body"] = {"size": fallback_size, "target_tokens": target_tokens, "ok": False, "error": fb_err}
    if fb_upstream_headers:
        fb_final["response_headers"] = dict(fb_upstream_headers)
    if fb_router_request_headers:
        fb_final["router_request_headers"] = dict(fb_router_request_headers)
    if fb_router_request_body is not None:
        fb_final["router_request_body"] = fb_router_request_body
    if fb_router_response_body is not None:
        fb_final["router_response_body"] = fb_router_response_body
    if fb_log_data.get("upstream_status"):
        fb_final["upstream_status"] = fb_log_data["upstream_status"]
    await PostgresClient.update_request_log(fb_log_id, fb_final)

    return fallback


def _extract_sample_text(chunk) -> str:
    """从 chat 返回的 chunk 中提取少量文本用于成功佐证。"""
    if isinstance(chunk, str):
        return chunk[:500]
    if isinstance(chunk, dict):
        # 常见结构：choices[].delta.content / choices[].message.content / content
        choices = chunk.get("choices")
        if isinstance(choices, list):
            for ch in choices:
                if isinstance(ch, dict):
                    msg = ch.get("message") or ch.get("delta")
                    if isinstance(msg, dict):
                        c = msg.get("content")
                        if isinstance(c, str) and c.strip():
                            return c[:500]
        c = chunk.get("content")
        if isinstance(c, str) and c.strip():
            return c[:500]
    return ""


# 账号测试：按协议构造测试请求体。caller 负责设置 stream。
# 返回 (messages, body_extra)；body_extra 为该测试类型在指定协议下的特有字段（不含 model/messages/stream）。
# 客户端模拟不再由 test_type 承载 —— 改由前端独立传 client_type（见 _TEST_CLIENT_TYPES）。
_TOOL_OPENAI = [{
    "type": "function",
    "function": {
        "name": "get_weather",
        "description": "Get the current weather for a city",
        "parameters": {
            "type": "object",
            "properties": {"city": {"type": "string", "description": "City name"}},
            "required": ["city"],
        },
    },
}]
_TOOL_ANTHROPIC = [{
    "name": "get_weather",
    "description": "Get the current weather for a city",
    "input_schema": {
        "type": "object",
        "properties": {"city": {"type": "string", "description": "City name"}},
        "required": ["city"],
    },
}]
_TOOL_RESPONSES = [{
    "type": "function",
    "name": "get_weather",
    "description": "Get the current weather for a city",
    "parameters": {
        "type": "object",
        "properties": {"city": {"type": "string", "description": "City name"}},
        "required": ["city"],
    },
}]
_TOOL_GEMINI = [{
    "function_declarations": [{
        "name": "get_weather",
        "description": "Get the current weather for a city",
        "parameters": {
            "type": "object",
            "properties": {"city": {"type": "string", "description": "City name"}},
            "required": ["city"],
        },
    }],
}]
_TOOL_PROMPT = "What's the weather in Beijing? Use the get_weather tool."
_THINKING_PROMPT = "Think step by step: prove 17 is prime."
_MULTI_MESSAGES = [
    {"role": "user", "content": "Hi"},
    {"role": "assistant", "content": "Hello! How can I help?"},
    {"role": "user", "content": "Say bye."},
]


def _build_test_body(test_type: str, protocol: str, model: str) -> tuple[list[dict], dict]:
    """构造账号测试请求体。返回 (messages, body_extra)。

    body_extra 不含 model/messages/stream，由 caller 合并。协议字段差异：
    - openai/chat：tools+tool_choice、reasoning_effort
    - anthropic：tools(input_schema)、thinking={type,budget_tokens}
    - responses：tools、reasoning={effort}
    - gemini：tools(function_declarations)、无原生 thinking（fallback system 指令）
    """
    proto = (protocol or "openai").lower()
    if proto not in ("openai", "chat", "anthropic", "responses", "gemini"):
        proto = "openai"

    if test_type == "tool":
        messages = [{"role": "user", "content": _TOOL_PROMPT}]
        if proto in ("openai", "chat"):
            return messages, {"tools": _TOOL_OPENAI, "tool_choice": "auto"}
        if proto == "anthropic":
            return messages, {"tools": _TOOL_ANTHROPIC}
        if proto == "responses":
            return messages, {"tools": _TOOL_RESPONSES, "tool_choice": "auto"}
        if proto == "gemini":
            return messages, {"tools": _TOOL_GEMINI, "tool_choice": "auto"}
        return messages, {}

    if test_type == "thinking":
        messages = [{"role": "user", "content": _THINKING_PROMPT}]
        if proto in ("openai", "chat"):
            return messages, {"reasoning_effort": "medium"}
        if proto == "responses":
            return messages, {"reasoning": {"effort": "medium"}}
        if proto == "anthropic":
            return messages, {"thinking": {"type": "enabled", "budget_tokens": 1024}}
        if proto == "gemini":
            # gemini 无原生 thinking 字段：退化为 system 指令消息，由 gemini_proto 转成 systemInstruction。
            return [
                {"role": "system", "content": "Think step by step before answering."},
                {"role": "user", "content": _THINKING_PROMPT},
            ], {}
        return messages, {}

    if test_type == "multi":
        return list(_MULTI_MESSAGES), {}

    # chat / stream / 未知类型默认聊天
    messages = [{"role": "user", "content": "hi"}]
    return messages, {}


# 前端「客户端类型」值 → CustomProvider.CLIENT_PRESETS / 目标协议 的映射。
# "none" = 通用测试，不带任何客户端头、body 走所选协议的通用形态。
# 其余值让测试请求「长得就像那个客户端发来的」：headers 用真实客户端头（服务端据此
# 探测 client_type），body 按该客户端的目标协议形态构造。不下发 client_preset override
# ——伪装与否由渠道自己的 client_preset 配置决定（client_type==preset 时服务端直通）。
_TEST_CLIENT_TYPES = {
    "none": None,
    "claude-code": "claude-code",
    "codex-cli": "codex-cli",
    "codex-tui": "codex-tui",
    "codex-openai": "codex-openai",
    "opencode": "opencode",
    "workbuddy": "workbuddy",
}


def _find_test_type(test_type: str) -> dict | None:
    """在可配置测试类型列表里按 key 查条目；找不到返回 None。"""
    key = (test_type or "").strip().lower()
    for item in config.Config.get_test_types():
        if str(item.get("key") or "").strip().lower() == key:
            return item
    return None


def _is_default_test_type(entry: dict | None) -> bool:
    """该测试类型条目是否与后端默认值完全一致（未被管理员改动）。

    默认条目仍走 _build_test_body 的协议感知逻辑（保留 anthropic/gemini/responses
    的 tools/thinking 原生形态改写）；被改动的条目才走扁平 messages+body。
    """
    if not isinstance(entry, dict):
        return False
    key = str(entry.get("key") or "").strip().lower()
    for default in config.DEFAULT_TEST_TYPES:
        if str(default.get("key") or "").strip().lower() != key:
            continue
        return (
            entry.get("operation") == default.get("operation")
            and entry.get("messages") == default.get("messages")
            and entry.get("body") == default.get("body")
        )
    return False


def _select_test_endpoint_config(provider_cfg: dict, requested_protocol: str) -> dict | None:
    """测试协议只选择已配置的协议行；返回原行以锁定其 path。"""
    if not requested_protocol:
        return None
    raw_chat_protocols = provider_cfg.get("chat_protocols")
    rows = [c for c in (raw_chat_protocols if isinstance(raw_chat_protocols, list) else []) if isinstance(c, dict)]
    return next((
        row for row in rows
        if row.get("enabled", True)
        and str(row.get("protocol") or "").lower() == requested_protocol
    ), None)


# 自定义测试模板 messages/body 支持的运行时变量（每次测试、每个账号独立求值）。
# 变量集与渲染语义**与 Header 模板完全一致**，且直接复用 providers/custom.py 的同一份
# 实现（build_request_template_variables + render_account_template_paths）：
#   - 内置标量：{{uuid}} {{time}} {{timestamp}} {{timestamp_ms}}
#               {{client_session_id}} {{client_request_id}}
#   - 账号上下文：{{account.username}} {{account.provider}} {{account.metadata.<字段>}}
#               （密钥类字段不暴露，见 _account_template_context）
#   - 测试独有：{{model}} {{username}} {{provider}}（本次测试的被测模型/账号/渠道）
# 未识别的 {{...}} 原样保留（不误伤 body 里合法的双花括号）；account.* 缺值渲染为空串。
# 默认类型未被改动时不走模板下发，变量不生效。


def _test_template_variables(*, model: str, username: str, provider_name: str, headers: dict | None = None) -> dict:
    """构造本次测试的模板变量表。

    内置标量取 Header 模板那一份实现（uuid/时间类逐次求值，每次测试不同）；
    client_session_id/client_request_id 从**本次测试构造出的模拟客户端 headers** 里
    提取——与 dispatch_entry 给出站 Header 模板用的是同一个提取函数，两边渲染同值。
    model/username/provider 取自本次测试上下文（被测模型、账号、渠道）。
    """
    session_context: dict = {}
    if headers:
        # 模拟客户端才有会话头（通用测试 headers 为空，此时不必导入主链路）。
        from main import _extract_client_session_context
        session_context = _extract_client_session_context(headers)
    variables = build_request_template_variables(session_context)
    variables.update({
        "model": str(model or ""),
        "username": str(username or ""),
        "provider": str(provider_name or ""),
    })
    return variables


def _render_test_template_vars(value, variables: dict, kwargs: dict | None = None):
    """递归渲染模板 messages/body 里的 {{...}} 变量：只改字符串叶子，
    dict/list 结构原样保留；未识别的 {{...}} 不动（避免误伤 body 里合法的双花括号内容）。

    字符串叶子先替换内置/测试标量，再交给 Header 模板那一份 account.* 点路径渲染器，
    因此 {{account.metadata.<字段>}} 这类开放路径与出站 Header 行为一致。
    """
    if isinstance(value, str):
        for name, rendered in variables.items():
            value = value.replace("{{" + name + "}}", rendered)
        return render_account_template_paths(value, kwargs)
    if isinstance(value, list):
        return [_render_test_template_vars(item, variables, kwargs) for item in value]
    if isinstance(value, dict):
        return {k: _render_test_template_vars(v, variables, kwargs) for k, v in value.items()}
    return value


def _test_template_renderer(*, model: str, username: str, provider_name: str,
                            headers: dict | None, account_client=None):
    """本次测试的模板渲染器（单参可调用）：变量表只求值一次，同一次测试里
    所有 messages/body 叶子共享同一份 {{uuid}}/{{timestamp}}（与真实请求一致）。"""
    variables = _test_template_variables(
        model=model, username=username, provider_name=provider_name, headers=headers,
    )
    # account.* 走运行态账号对象（AccountClient），与出站 Header 模板同源；
    # 缺账号（池外/单测）时渲染为空串，不报错。
    account_kwargs = {"account_client": account_client} if account_client is not None else {}
    return lambda value: _render_test_template_vars(value, variables, account_kwargs)


def _build_test_dispatch_input(
    *,
    test_type: str,
    client_type: str,
    protocol: str | None,
    channel_protocol: str,
    model: str,
    username: str,
    provider_name: str,
    data: dict,
    selected_endpoint_config: dict | None,
    account_client=None,
) -> dict:
    """一次性构造测试请求的完整下发入参（就像目标客户端真实发来的请求）。

    返回 dispatch_entry 所需的全部字段；下发调用方原样透传，不做任何二次判断。
    与真实请求的唯一差异是筛选层（由调用方置 is_test=True + 白名单锁定目标），
    请求本身的形态（headers/body/协议）在这里构造完整，不靠 dispatch 特判。

    ``account_client``：本次被测账号的运行态对象，只用于模板 {{account.*}} 取值
    （与出站 Header 模板同源）；缺省时这类变量渲染为空串。

    不在此构造：client_preset override（删除——真实链路不下发，由渠道配置决定伪装）、
    is_test（下发层置 True）。
    """
    from providers.custom import CustomProvider

    preset_key = _TEST_CLIENT_TYPES.get(client_type)
    proto = (protocol or channel_protocol or "openai").lower()

    # headers：模拟客户端时用该客户端的真实头（含 User-Agent + session 头），
    # 服务端 _detect_client_type 据 User-Agent 探测出 client_type。通用测试用空头。
    headers = dict(CustomProvider.CLIENT_PRESETS.get(preset_key, {})) if preset_key else {}

    # extra_kwargs：只带协议行（自定义渠道按所选协议选的 chat_protocols 行），不带 client_preset。
    extra_kwargs: dict = {}
    if selected_endpoint_config:
        extra_kwargs["_endpoint_config"] = selected_endpoint_config

    # 可配置测试类型：按 key 查配置条目。命中且被管理员自定义（≠默认值）则走
    # 「扁平 messages+body」；默认未改动或未命中的 key 仍回退协议感知逻辑，
    # 保留 anthropic/gemini/responses 测试线对 tools/thinking 的原生形态改写。
    entry = _find_test_type(test_type)
    entry_customized = bool(entry) and not _is_default_test_type(entry)
    # 自定义模板的 messages/body 先渲染 {{...}} 变量再下发；默认未改动类型不走模板内容。
    render = _test_template_renderer(
        model=model, username=username, provider_name=provider_name,
        headers=headers, account_client=account_client,
    ) if entry_customized else None

    # ── 媒体分支：operation 非空的测试类型（无客户端模拟）──
    media_operation = None
    if entry and entry.get("operation"):
        media_operation = entry["operation"]
    elif test_type in ("image", "image_generation"):
        media_operation = "image"
    elif test_type in ("video", "video_generation"):
        media_operation = "video"
    elif test_type in ("tts", "tts_generation", "speech"):
        media_operation = "tts_generation"

    if media_operation:
        media_body: dict = {"model": model}
        if entry_customized:
            # 自定义媒体类型：用配置 body（渲染变量后）；同名字段允许 data 覆盖（交互覆盖优先）。
            for k, v in (entry.get("body") or {}).items():
                media_body[k] = data.get(k, render(v))
        elif media_operation == "image":
            media_body.update({
                "prompt": data.get("prompt") or "A simple test image of a red apple on a desk.",
                "n": data.get("n") or 1,
                "size": data.get("size") or "1024x1024",
            })
        elif media_operation == "video":
            media_body["prompt"] = data.get("prompt") or "A short test video of ocean waves."
        else:  # tts_generation
            media_body.update({
                "input": data.get("input") or data.get("text") or "Hello, this is an account test.",
                "voice": data.get("voice") or "alloy",
                "response_format": data.get("response_format") or "mp3",
            })
        return {
            "operation": media_operation,
            "body": media_body,
            "headers": headers,
            "stream": False,
            "request_protocol": proto,
            "chat_method": _test_chat_method_for_protocol(proto),
            "provider_whitelist": {provider_name},
            "account_whitelist": {username},
            "extra_kwargs": extra_kwargs,
        }

    # ── 对话分支 ──
    entry_body = entry.get("body") if isinstance(entry, dict) and isinstance(entry.get("body"), dict) else {}
    stream = test_type == "stream" or bool(data.get("stream", False)) or bool(entry_body.get("stream"))

    if preset_key:
        # 模拟客户端：body 按该客户端目标协议的真实形态构造。
        body, body_proto = _build_client_sim_body(preset_key, model, stream, data)
        proto = body_proto
    elif entry_customized:
        # 自定义/覆盖的测试类型：扁平 messages + body（渲染变量后），不做目标协议改写。
        messages = render(entry.get("messages") or [{"role": "user", "content": "hi"}])
        body = {"model": model, "messages": messages, "stream": stream}
        for k, v in render(entry_body).items():
            if k == "stream":
                continue
            body[k] = v
        if protocol:
            body["protocol"] = protocol
    else:
        # 默认/未知类型：按所选协议构造通用 body（协议感知回退）。
        messages, body_extra = _build_test_body(test_type, proto, model)
        body = {"model": model, "messages": messages, "stream": stream}
        body.update(body_extra)
        if protocol:
            body["protocol"] = protocol

    return {
        "operation": None,
        "body": body,
        "headers": headers,
        "stream": stream,
        "request_protocol": proto,
        "chat_method": _test_chat_method_for_protocol(proto),
        "provider_whitelist": {provider_name},
        "account_whitelist": {username},
        "extra_kwargs": extra_kwargs,
    }


def _test_chat_method_for_protocol(proto: str) -> str:
    if proto == "responses":
        return "chat_responses"
    if proto == "anthropic":
        return "chat_anthropic"
    return "chat"


# 模拟客户端的最小消息（body 主体由 _build_client_sim_body 按协议形态包装）。
_SIM_USER_TEXT = "hi"


# 模拟客户端的真实请求样本字段（用户提供，按真实形态逐字段构造）。
# 仅取结构性字段：大段固定 instructions/system prompt 用渠道已有常量，不整段硬编码进 body，
# 避免测试请求体过载；字段形状（key 名/嵌套）与真实样本一致即达成「长得像那个客户端」。

# Codex client_metadata（按真实样本结构，值用固定测试常量）
_SIM_CODEX_CLIENT_METADATA = {
    "turn_id": "test-turn-id",
    "thread_id": "test-thread-id",
    "session_id": "test-thread-id",
    "x-codex-window-id": "test-thread-id:0",
    "x-codex-installation-id": "test-installation-id",
}
_SIM_CODEX_TURN_METADATA = (
    '{"installation_id":"test-installation-id",'
    '"session_id":"test-thread-id","thread_id":"test-thread-id",'
    '"turn_id":"test-turn-id","window_id":"test-thread-id:0",'
    '"request_kind":"turn","thread_source":"user",'
    '"sandbox":"none","workspace_kind":"projectless"}'
)


# Codex 输入结构（按真实样本）：一条 developer（permissions/app-context 系统指令占位）+ 一条 user。
def _sim_codex_input(user_text: str) -> list[dict]:
    return [
        {
            "role": "developer",
            "type": "message",
            "content": [{"text": "<permissions instructions>test</permissions instructions>", "type": "input_text"}],
        },
        {
            "role": "user",
            "type": "message",
            "content": [{"text": user_text, "type": "input_text"}],
        },
    ]


def _build_client_sim_body(preset_key: str, model: str, stream: bool, data: dict) -> tuple[dict, str]:
    """按客户端类型构造「就像那个客户端发来的」请求体，返回 (body, request_protocol)。

    按用户提供的真实请求样本逐字段构造（结构一致，大段固定文案用渠道常量简化）：
    - codex-cli / codex-tui → responses：input(developer+user input_text)/instructions/store/include/
                    reasoning/tool_choice/client_metadata/prompt_cache_key/text.verbosity/parallel_tool_calls
    - codex-openai→ openai chat：messages+store/include/reasoning/tool_choice/client_metadata/
                    prompt_cache_key/parallel_tool_calls（Codex 的 OpenAI 端点变体）
    - claude-code → anthropic：system/messages/tools/thinking=adaptive/max_tokens/stop_sequences/
                    context_management/metadata.user_id
    - opencode    → openai chat：messages(system+user)/tools/stream_options/tool_choice/max_tokens
    """
    from providers.custom import CustomProvider

    target_proto = CustomProvider.PRESET_TARGET_PROTOCOL.get(preset_key, "openai")
    user_text = data.get("prompt") or _SIM_USER_TEXT

    if target_proto == "responses":
        # Codex CLI（responses 协议）：按真实样本结构构造。
        body = {
            "model": model,
            "instructions": CustomProvider.CODEX_INSTRUCTIONS_PREFIX,
            "input": _sim_codex_input(user_text),
            "store": False,
            "tools": [],
            "stream": stream,
            "include": ["reasoning.encrypted_content"],
            "reasoning": {"effort": "medium"},
            "tool_choice": "auto",
            "parallel_tool_calls": True,
            "text": {"verbosity": "low"},
            "prompt_cache_key": "test-prompt-cache-key",
            "client_metadata": dict(_SIM_CODEX_CLIENT_METADATA, **{
                "x-codex-turn-metadata": _SIM_CODEX_TURN_METADATA,
            }),
        }
        return body, "responses"

    if target_proto == "anthropic":
        # Claude Code（anthropic 协议）：按真实样本结构构造。
        metadata_user_id = {
            "device_id": "test-device-id",
            "account_uuid": "",
            "session_id": "test-session-id",
        }
        claude_tool = {
            "name": "Agent",
            "description": "Launch a new agent to handle complex, multi-step tasks.",
            "input_schema": {
                "type": "object",
                "required": ["description", "prompt"],
                "properties": {
                    "description": {"type": "string"},
                    "prompt": {"type": "string"},
                    "model": {"type": "string"},
                    "isolation": {"type": "string"},
                    "subagent_type": {"type": "string"},
                    "run_in_background": {"type": "boolean"},
                },
                "additionalProperties": False,
            },
        }
        body = {
            "model": model,
            "system": "You are Claude Code, Anthropic's official CLI for Claude.",
            "messages": [{"role": "user", "content": user_text}],
            "tools": [claude_tool],
            "thinking": {"type": "adaptive"},
            "max_tokens": 64000,
            "stop_sequences": ["</block>"],
            "context_management": {"edits": [{"keep": "all", "type": "clear_thinking_20251015"}]},
            "metadata": {"user_id": json.dumps(metadata_user_id, ensure_ascii=False)},
            "stream": stream,
        }
        return body, "anthropic"

    # openai 形态：opencode / workbuddy / codex-openai 各自的字段集合。
    if preset_key == "opencode":
        # OpenCode：openai chat，按真实样本带 system（agent-identity）+ user + tools/stream_options。
        opencode_tool = {
            "type": "function",
            "function": {
                "name": "ast_grep_replace",
                "description": "Rewrite code by AST pattern (25 languages). Dry-run by default.",
                "parameters": {
                    "type": "object",
                    "required": ["pattern", "rewrite", "lang"],
                    "properties": {
                        "pattern": {"type": "string"},
                        "rewrite": {"type": "string"},
                        "lang": {"type": "string"},
                        "paths": {"type": "array", "items": {"type": "string"}},
                        "globs": {"type": "array", "items": {"type": "string"}},
                        "dryRun": {"type": "boolean"},
                    },
                    "additionalProperties": False,
                },
            },
        }
        body = {
            "model": model,
            "messages": [
                {"role": "system", "content": (
                    "<agent-identity>\nYou are a helpful assistant.\n</agent-identity>"
                )},
                {"role": "user", "content": user_text},
            ],
            "tools": [opencode_tool],
            "stream": stream,
            "stream_options": {"include_usage": True},
            "tool_choice": "auto",
            "max_tokens": 32000,
        }
        return body, "openai"

    # workbuddy：WorkBuddy（腾讯 CodeBuddy 桌面客户端）走标准 OpenAI chat 形态。
    # 其特殊识别特征都在 header 层（user-agent / x-codebuddy-request / x-ide-name，
    # 由 _TEST_CLIENT_TYPES → CLIENT_PRESETS 静态 preset 带上），请求体就是普通 chat
    # messages，不带 codex 那套 store/include/reasoning/client_metadata。
    if preset_key == "workbuddy":
        body = {
            "model": model,
            "messages": [{"role": "user", "content": user_text}],
            "stream": stream,
        }
        return body, "openai"

    # codex-openai：Codex 走 OpenAI 端点的变体，带 store/include/reasoning/client_metadata。
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": CustomProvider.CODEX_INSTRUCTIONS_PREFIX},
            {"role": "user", "content": user_text},
        ],
        "store": False,
        "include": ["reasoning.encrypted_content"],
        "reasoning": {"effort": "medium"},
        "tool_choice": "auto",
        "tools": [],
        "parallel_tool_calls": True,
        "prompt_cache_key": "test-prompt-cache-key",
        "client_metadata": dict(_SIM_CODEX_CLIENT_METADATA, **{
            "x-codex-turn-metadata": _SIM_CODEX_TURN_METADATA,
        }),
        "stream": stream,
    }
    return body, "openai"


@router.post("/providers/{name}/accounts/test")
async def test_provider_accounts(name: str, data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    # main -> admin 是启动期导入，反向引用统一入口必须延迟导入，避免模块加载期循环依赖。
    # _detect_client_type：与真实端点同源地从测试请求 headers/body 探测 client_type，
    # 保证「测试请求 == 真实客户端请求」闭环（不再写死 admin-test）。
    if not await _ensure_provider_runtime(name):
        raise HTTPException(status_code=404, detail=f"渠道 '{name}' 不存在")
    pool = ModelClientPool.get_provider_pool(name)
    if not pool:
        raise HTTPException(status_code=404, detail=f"渠道 '{name}' 不存在")
    return await _run_provider_test(name, pool, data)


async def _run_provider_test(name: str, pool, data: dict, *, is_probe: bool = False) -> dict:
    """跑一次渠道测试。端点 /accounts/test 与定时循环共用本函数，保证「定时测试 == 手动测试」。

    data 字段：username（str 单选 / list 多选 / 留空=该渠道全部账号）、model、test_type、
    client_type、protocol、retain_failed_logs。手动测试（is_probe=False）失败不冻结、不影响健康度；定时检测
    （is_probe=True）失败走与正常请求完全一致的冻结/健康度收口——「失败刷新冻结周期」是渠道级
    配置，在冻结写入点判定，不再由本函数透传。retain_failed_logs 仅对定时检测生效；关闭后
    定时检测（成功与失败）都不写请求日志，不影响手动测试与正常用户请求，也不改变冻结/健康度行为。
    两者成功均解除临时/永久冻结，但不改变账号 switch 或渠道 enabled（禁用不是冻结）。
    """
    from main import dispatch_entry, _detect_client_type

    # username 兼容三种形态：str 单选 / list 多选 / None=全部账号。
    username_field = data.get("username")
    if isinstance(username_field, list):
        usernames = {u for u in username_field if isinstance(u, str) and u.strip()} or None
    elif isinstance(username_field, str) and username_field.strip():
        usernames = {username_field.strip()}
    else:
        usernames = None
    # 模型兜底必须落在**该渠道自己的**模型表里：拿全局目录第 0 个模型会选不到路
    # （该模型多半不属于本渠道）→ NoAvailableAccountError → 请求日志一行都不写，
    # 检测看起来「没跑」。渠道一个模型都没有时下面提前返回，不进 dispatch。
    channel_models = _provider_models(name)
    model = data.get("model") or (channel_models[0] if channel_models else "")
    if not model:
        logger.warning(f"[provider_account_test] provider={name} 无任何模型，跳过检测")
        return {"ok": True, "results": [{
            "username": c.username,
            "ok": False,
            "error": f"渠道 {name} 未配置任何模型，无法测试",
            "duration_ms": 0,
        } for c in pool.clients if usernames is None or c.username in usernames]}
    test_type = (data.get("test_type") or "chat").strip().lower()
    client_type = (data.get("client_type") or "none").strip().lower()
    if client_type not in _TEST_CLIENT_TYPES:
        client_type = "none"
    provider_cfg = await _read_provider_config(name)
    # 测试选择协议只决定使用哪条已配置的 chat_protocols 行；该行的 path 必须原样下发，
    # 客户端模拟随后即便改变 request_protocol/body 形态，也绝不能改变这条 path。
    requested_protocol = (data.get("protocol") or "").strip().lower()
    if requested_protocol and requested_protocol not in ("openai", "chat", "anthropic", "responses", "gemini"):
        requested_protocol = ""
    channel_protocol = (provider_cfg.get("protocol") or "openai").lower()
    selected_endpoint_config = _select_test_endpoint_config(provider_cfg, requested_protocol)
    endpoint = f"/admin/providers/{name}/accounts/test"
    results = []

    def _media_preview(value) -> str:
        if isinstance(value, (bytes, bytearray)):
            return f"audio bytes: {len(value)} bytes"
        if isinstance(value, dict):
            data_items = value.get("data")
            if isinstance(data_items, list) and data_items:
                first = data_items[0] if isinstance(data_items[0], dict) else {"value": data_items[0]}
                for key in ("url", "b64_json", "revised_prompt"):
                    if first.get(key):
                        return str(first.get(key))[:1000]
                return json.dumps(first, ensure_ascii=False)[:1000]
            return json.dumps(value, ensure_ascii=False)[:1000]
        return str(value)[:1000]

    for c in pool.clients:
        if usernames is not None and c.username not in usernames:
            continue
        start_time = time.time()
        built: dict = {}
        last_route_info: dict = {}
        try:
            # 手动测试属于 Path A：不看渠道 enabled / 账号 switch；但若定时 init 被跳过，这里按需强制初始化。
            is_init = getattr(c.provider, "is_init", lambda: True)
            if not is_init():
                ok = await asyncio.wait_for(c.provider.init_auth(True), timeout=30)
                c.auth_ok = bool(ok)
                c.auth_checked_at = time.time()
                c.auth_error = "" if ok else "初始化失败"
                if not ok:
                    raise HTTPException(status_code=400, detail="账号初始化失败")
            if c.auth_ok is False:
                raise HTTPException(
                    status_code=400,
                    detail=f"账号认证异常：{c.auth_error or '认证未通过'}",
                )

            # 一次性把入参构造成「目标客户端真实发来的请求」形态：headers/body/协议全在这里定死，
            # 下发不再有任何 test_type 特判。测试与真实的唯一差异 = is_test 跳过筛选 + 白名单锁定目标。
            built = _build_test_dispatch_input(
                test_type=test_type,
                client_type=client_type,
                protocol=requested_protocol or None,
                channel_protocol=channel_protocol,
                model=model,
                username=c.username,
                provider_name=name,
                data=data,
                selected_endpoint_config=selected_endpoint_config,
                # account.* 模板变量取运行态账号（与出站 Header 模板同源）。
                account_client=c,
            )
            operation = built["operation"]
            stream = built["stream"]
            # client_type 与真实端点同源探测：先看构造出的客户端 headers（User-Agent），
            # 再看 body 系统提示 —— 通用测试（none）无客户端头时回落到协议名，与真实链路一致。
            detected_client_type = _detect_client_type(
                built["body"], built["headers"], endpoint,
            )
            dispatch = await dispatch_entry(
                endpoint=endpoint,
                body=built["body"],
                headers=built["headers"],
                api_key="admin-check" if is_probe else "admin-test",
                api_key_name="admin-check" if is_probe else "admin-test",
                provider_whitelist=built["provider_whitelist"],
                provider_blacklist=set(),
                account_whitelist=built["account_whitelist"],
                is_test=not is_probe,
                is_probe=is_probe,
                # 仅定时探测可关闭日志；手动测试即使伪造该字段也始终正常记日志。
                is_save_log=(data.get("retain_failed_logs") is not False) if is_probe else True,
                request_protocol=built["request_protocol"],
                chat_method=built["chat_method"],
                operation=operation,
                client_type=detected_client_type,
                extra_kwargs=built["extra_kwargs"],
            )

            if operation:
                result_payload = dispatch.get("result")
                # 测试成功即证明账号可达：解除临时/永久冻结；账号禁用开关不受影响。
                pool.clear_runtime_freeze_on_success(c.username, model)
                results.append({
                    "username": c.username,
                    "ok": True,
                    "preview": _media_preview(result_payload),
                    "duration_ms": int((time.time() - start_time) * 1000),
                    "protocol": built["request_protocol"],
                    "test_type": test_type,
                })
                continue

            chunks = []
            preview_full = False
            usage_info: dict | None = None
            accumulated_usage = {}
            stream_had_content = False
            nonstream_had_content = False
            response_body = None
            async for chunk in dispatch["generator"]:
                if isinstance(chunk, dict) and "_last_route_info" in chunk:
                    last_route_info = chunk["_last_route_info"] or {}
                    continue
                if isinstance(chunk, dict) and chunk.get("_passthrough_done"):
                    if chunk.get("usage"):
                        usage_info = merge_usage(usage_info, chunk["usage"])
                    continue
                chunk_has_content = _has_first_token_content(chunk)
                if stream:
                    if chunk_has_content:
                        stream_had_content = True
                    for payload in _admin_stream_payloads(chunk):
                        usage = _admin_extract_usage_payload(payload)
                        if usage is not None:
                            accumulated_usage = merge_usage(accumulated_usage, usage)
                            usage_info = merge_usage(usage_info, usage)
                else:
                    if chunk_has_content:
                        nonstream_had_content = True
                    _admin_validate_upstream_usage_payload(chunk, had_content=nonstream_had_content)
                if not preview_full:
                    chunks.append(str(chunk)[:500])
                    if len(chunks) >= 3:
                        preview_full = True
                response_body = chunk
                if isinstance(chunk, dict) and chunk.get("usage"):
                    usage_info = merge_usage(usage_info, chunk["usage"])
            # 有真实输出时，零 completion usage 只是不可靠的上游统计；无真实输出才判失败。
            if stream and accumulated_usage and not stream_had_content:
                normalized_acc = normalize_usage(accumulated_usage)
                if normalized_acc["completion_tokens"] <= 0:
                    raise HTTPException(status_code=502, detail=UPSTREAM_ZERO_COMPLETION_MESSAGE)
            # 测试成功即证明账号可达：解除临时/永久冻结；账号禁用开关不受影响。
            pool.clear_runtime_freeze_on_success(c.username, model)
            result = {
                "username": c.username,
                "ok": True,
                "preview": "\n".join(chunks)[:1000] if chunks else _media_preview(response_body),
                "duration_ms": int((time.time() - start_time) * 1000),
                "protocol": built["request_protocol"],
            }
            if usage_info:
                result["usage"] = usage_info
            results.append(result)
        except NoAvailableAccountError as e:
            # 选路阶段就没候选：请求从未发出（dispatch 里 raise 在建日志行之前），
            # 所以不写请求日志（写了会污染真实请求统计），只在结果与服务日志里标注。
            # 阶段四修好模型选择后此分支应大幅减少。
            error_info = {
                "username": c.username,
                "ok": False,
                "no_candidate": True,
                "error": _format_admin_error(getattr(e, "detail", str(e)) or "无可用账号"),
                "duration_ms": int((time.time() - start_time) * 1000),
            }
            logger.warning(
                f"[provider_account_test] 无候选 provider={name}, account={c.username}, "
                f"model={model}, detail={error_info['error']}"
            )
            results.append(error_info)
        except Exception as e:
            error_info = {"username": c.username, "ok": False}
            if hasattr(e, 'status_code'):
                error_info["status_code"] = e.status_code
                error_info["error"] = _format_admin_error(getattr(e, 'detail', str(e)) or str(e))
            elif hasattr(e, 'status'):
                error_info["status_code"] = e.status
                error_info["error"] = _format_admin_error(getattr(e, 'message', str(e)) or str(e))
            else:
                error_info["error"] = _format_admin_error(str(e))
            error_info["duration_ms"] = int((time.time() - start_time) * 1000)
            diagnostics = _test_failure_diagnostics(built, last_route_info, error_info["error"])
            error_info["diagnostics"] = diagnostics
            logger.warning(
                f"[provider_account_test] provider={name}, account={c.username}, "
                f"model={model}, test_type={test_type}, error={error_info['error']}"
            )
            results.append(error_info)
    return {"ok": True, "results": results}


@router.post("/providers/{name}/accounts/detect-model")
async def detect_provider_model(name: str, data: dict, token: str = Header(None, alias="Authorization")):
    """测试指定渠道模型能否真实处理用户选择/输入的上下文窗口大小。

    window_tokens（兼容旧字段 fallback_size）就是本次实际请求的目标 token 数；后端不会
    再按 metadata 自动放大，也不会在失败后追加第二次降级请求。metadata 声明窗口仅作
    结果对照展示。消息体按约 2 字符/token 构造，因此 token 数是跨模型 tokenizer 的近似值。
    """
    await _require_admin(token)
    pool = ModelClientPool.get_provider_pool(name)
    if not pool:
        raise HTTPException(status_code=404, detail=f"渠道 '{name}' 不存在")
    if not pool.clients:
        raise HTTPException(status_code=404, detail=f"渠道 '{name}' 无可用账号")

    # username 为空时随机选一个账号，不是全部账号
    username = (data.get("username") or "").strip()
    if username:
        usernames = [username]
    else:
        usernames = [random.choice(pool.clients).username]

    upstream_model_id = (data.get("upstream_model_id") or data.get("model") or "").strip()
    if not upstream_model_id:
        raise HTTPException(status_code=400, detail="必须提供 upstream_model_id（渠道模型 id）")
    # 探测强制流式：超大请求体走流式不会被 SDK/HTTP 超时 guard 拦截，
    # 也无需为探测单独设置超时（上游对超大请求通常会立即拒绝）。
    stream = True

    # 窗口真实大小测试：用户选择/输入的窗口 token 数即本次请求目标，只做这一次，
    # 不叠加降级请求、不依赖 metadata 自动扩容。
    window_raw = data.get("window_tokens", data.get("fallback_size", "8k"))
    target_tokens, size_err = _parse_window_tokens(window_raw)
    if size_err:
        raise HTTPException(status_code=400, detail=f"窗口大小无效：{size_err}")
    target_size = str(window_raw).strip().lower() or "8k"  # 保留原始输入展示

    # 声明窗口仅作结果对照展示，不参与探测目标计算；用 model_metadata 当前真相源而非旧表。
    declared_limit: int | None = None
    try:
        from model_metadata import get_model_metadata
        model_metadata, _ = await get_model_metadata(upstream_model_id)
        declared_limit = model_metadata.get("max_context_tokens")
        if not isinstance(declared_limit, int) or declared_limit <= 0:
            declared_limit = None
    except Exception:
        declared_limit = None

    # 构造探测消息体；固定输出预算让误差收敛到窗口大小本身。
    probe_messages = await _build_context_probe_messages(target_tokens)
    max_tokens = 1024
    probe_request_body = {
        "model": upstream_model_id,
        "messages": probe_messages,
        "max_tokens": max_tokens,
        "stream": stream,
    }

    results = []
    for c in pool.clients:
        if c.username not in usernames:
            continue
        start_time = time.time()
        log_data = {
            "request_id": f"req_{uuid.uuid4().hex[:12]}",
            "api_key_name": "admin-detect-model",
            "provider_name": name,
            "account_username": c.username,
            "model": upstream_model_id,
            "actual_model": upstream_model_id,
            "endpoint": f"/admin/providers/{name}/accounts/detect-model",
            "success": False,
            "status": "requesting",
            "stream": stream,
            "request_body": probe_request_body,
        }
        # 与普通请求一致：先插入 requesting 行，探测结束后再 update 结果。
        log_id = await PostgresClient.insert_request_log(log_data)
        upstream_headers: dict = {}
        upstream_request_headers: dict = {}
        router_request_body: dict | None = None
        router_response_body = None

        def _response_headers_callback(headers):
            upstream_headers.clear()
            if isinstance(headers, dict):
                upstream_headers.update(headers)
            status = (headers or {}).get(":status") if isinstance(headers, dict) else None
            if status:
                log_data["upstream_status"] = str(status)

        def _router_request_headers_callback(headers):
            from security import sanitize_header_value

            upstream_request_headers.clear()
            if isinstance(headers, dict):
                upstream_request_headers.update({
                    str(k): sanitize_header_value(k, v)
                    for k, v in headers.items()
                })

        def _router_request_body_callback(payload):
            nonlocal router_request_body
            router_request_body = payload

        def _router_response_body_callback(payload):
            nonlocal router_response_body
            if router_response_body is None:
                router_response_body = []
            if isinstance(router_response_body, list):
                router_response_body.append(payload)
            else:
                router_response_body = [router_response_body, payload]

        chat_kwargs = {
            "response_headers_callback": _response_headers_callback,
            "router_request_headers_callback": _router_request_headers_callback,
            "router_request_body_callback": _router_request_body_callback,
            "router_response_body_callback": _router_response_body_callback,
        }

        raw_error = ""
        response_sample = ""
        succeeded = False
        try:
            async for chunk in c.provider.chat(
                upstream_model_id, probe_messages,
                stream=True, max_tokens=max_tokens, **chat_kwargs,
            ):
                # 直接窗口测试：正常走完即视为该窗口可处理；异常才落入 except 分支记录错误。
                if isinstance(chunk, dict):
                    router_response_body = chunk
                elif isinstance(chunk, str):
                    if not response_sample:
                        response_sample = chunk[:2000]
                    router_response_body = chunk
            succeeded = True
        except HTTPException as e:
            raw_error = _format_admin_error(getattr(e, "detail", str(e)) or str(e))
        except Exception as e:
            raw_error = _format_admin_error(getattr(e, "message", str(e)) or str(e))

        duration_ms = int((time.time() - start_time) * 1000)
        result: dict = {
            "username": c.username,
            "ok": succeeded,
            "target_tokens": target_tokens,
            "target_size": target_size,
            "duration_ms": duration_ms,
        }
        if declared_limit is not None:
            result["declared_limit"] = declared_limit
        if succeeded:
            result["verified_tokens"] = target_tokens
        if response_sample:
            result["sample"] = response_sample[:500]
        if raw_error:
            result["error_message"] = raw_error
        final_log: dict = {
            "success": succeeded,
            "status": "ok" if succeeded else "error",
            "duration_ms": duration_ms,
            "actual_model": upstream_model_id,
        }
        if succeeded:
            final_log["response_body"] = {"verified": True, "verified_tokens": target_tokens, "target_tokens": target_tokens, "target_size": target_size, "declared_limit": declared_limit, "sample": response_sample[:500] if response_sample else None}
        else:
            final_log["error"] = raw_error
            final_log["response_body"] = {"verified": False, "error": raw_error, "target_tokens": target_tokens, "target_size": target_size, "declared_limit": declared_limit}
        if upstream_headers:
            final_log["response_headers"] = dict(upstream_headers)
        if upstream_request_headers:
            final_log["router_request_headers"] = dict(upstream_request_headers)
        if router_request_body is not None:
            final_log["router_request_body"] = router_request_body
        if router_response_body is not None:
            final_log["router_response_body"] = router_response_body
        if log_data.get("upstream_status"):
            final_log["upstream_status"] = log_data["upstream_status"]
        await PostgresClient.update_request_log(log_id, final_log)
        results.append(result)
    return {"ok": True, "results": results}


@router.get("/accounts/{username}/daily-usage")
async def get_account_daily_usage(username: str, provider_name: str = "", token: str = Header(None, alias="Authorization")):
    """获取指定账号今日在各渠道的使用次数

    可选 provider_name 用于精确指定渠道，避免同名 username 跨渠道首匹配命中错误的 provider。
    """
    await _require_admin(token)
    usage = await PostgresClient.account_daily_usage(username, provider_name or None)
    daily_remaining = None
    daily_limit = None
    model_daily_remaining: dict = {}
    model_daily_quota: dict = {}
    matched_provider: str | None = None
    for pool_name, pool in ModelClientPool._provider_pools.items():
        if provider_name and pool_name != provider_name:
            continue
        for c in pool.clients:
            if c.username == username:
                get_quota_snapshot = getattr(c.provider, "get_quota_snapshot", None)
                if get_quota_snapshot:
                    quota_result = get_quota_snapshot()
                    quota_snapshot = await quota_result if inspect.isawaitable(quota_result) else quota_result
                    daily_remaining = quota_snapshot.get("daily_requests_remaining")
                    daily_limit = quota_snapshot.get("daily_requests_limit")
                    if daily_remaining is None or daily_limit is None:
                        for dim in quota_snapshot.get("dimensions", []) or quota_snapshot.get("_dimensions", []) or []:
                            if dim.get("name") == "requests_daily":
                                daily_remaining = dim.get("remaining")
                                daily_limit = dim.get("limit")
                                break
                    model_daily_remaining = quota_snapshot.get("model_daily_remaining") or {}
                    for dim in quota_snapshot.get("dimensions", []) or quota_snapshot.get("_dimensions", []) or []:
                        dim_name = dim.get("name")
                        if isinstance(dim_name, str) and dim_name.startswith("requests_daily_model:"):
                            model_daily_quota[dim_name.split(":", 1)[1]] = {
                                "limit": dim.get("limit"),
                                "remaining": dim.get("remaining"),
                                "used": dim.get("used"),
                            }
                matched_provider = pool_name
                break
        if matched_provider:
            break
    return {
        "ok": True,
        "usage": usage,
        "daily_remaining": daily_remaining,
        "daily_limit": daily_limit,
        "model_daily_remaining": model_daily_remaining,
        "model_daily_quota": model_daily_quota,
        "provider_name": matched_provider,
    }


@router.post("/providers/{name}/accounts/{username}/init")
async def init_account(name: str, username: str, authorization: Optional[str] = Header(None)):
    """手动初始化/重新认证单个账号"""
    await _require_admin(authorization)
    if not await _ensure_provider_runtime(name):
        raise HTTPException(status_code=404, detail=f"渠道 '{name}' 不存在")
    pool = ModelClientPool.get_provider_pool(name)
    if not pool:
        raise HTTPException(status_code=404, detail=f"渠道 '{name}' 不存在")
    for c in pool.clients:
        if c.username == username:
            now = time.time()
            try:
                ok = await c.provider.init_auth(True)
                c.auth_ok = ok
                c.auth_checked_at = now
                c.auth_error = "" if ok else "初始化返回失败"
                await _log_operation(authorization, "init_account", "account", username, {"provider": name}, {"ok": ok})
                return {"ok": ok, "username": username}
            except Exception as e:
                c.auth_ok = False
                c.auth_checked_at = now
                c.auth_error = f"初始化异常: {e}"
                raise HTTPException(status_code=500, detail=f"初始化失败: {e}")
    raise HTTPException(status_code=404, detail=f"账号 '{username}' 未在运行池中")


@router.post("/providers/{name}/accounts/{username}/clear-cooldown")
async def clear_account_cooldown(name: str, username: str, token: str = Header(None, alias="Authorization")):
    """清除账号冷却状态（解冻）"""
    await _require_admin(token)
    pool = ModelClientPool.get_provider_pool(name)
    if not pool:
        raise HTTPException(status_code=404, detail=f"渠道 '{name}' 不存在")
    pool.clear_account_cooldown(username)
    await _log_operation(token, "clear_cooldown", "account", username, {"provider": name}, None)
    return {"ok": True, "username": username}


@router.post("/providers/{name}/accounts/clear-all-cooldowns")
async def clear_all_cooldowns(name: str, token: str = Header(None, alias="Authorization")):
    """批量清除渠道下所有账号的冻结状态（账号级 + 模型级冷却）。"""
    await _require_admin(token)
    pool = ModelClientPool.get_provider_pool(name)
    if not pool:
        raise HTTPException(status_code=404, detail=f"渠道 '{name}' 不存在")
    cleared = pool.clear_all_cooldowns()
    await _log_operation(token, "clear_all_cooldowns", "provider", name, {"provider": name, "cleared": cleared}, None)
    return {"ok": True, "cleared": cleared}


@router.put("/providers/{name}/accounts/batch-switch")
async def batch_set_account_switch(name: str, data: dict, token: str = Header(None, alias="Authorization")):
    """批量开启/关闭账号开关（switch）。

    body: {"switch": true|false, "usernames": ["a","b"]?}  # usernames 为空时作用于全部账号。
    """
    await _require_admin(token)
    switch_flag = bool(data.get("switch"))
    usernames = data.get("usernames") or []
    if usernames and not isinstance(usernames, list):
        raise HTTPException(status_code=400, detail="usernames 必须为数组")
    target_set = {str(u) for u in usernames} if usernames else None

    cfg = await _read_provider_config(name)
    accounts = cfg.get("accounts", [])
    changed: list[dict] = []
    for acc in accounts:
        uname = acc.get("username")
        if not uname:
            continue
        if target_set is not None and uname not in target_set:
            continue
        if acc.get("switch") is not switch_flag:
            changed.append({"username": uname, "switch": switch_flag})
            acc["switch"] = switch_flag

    if not changed:
        return {"ok": True, "changed": 0}

    rpm = _provider_rpm(cfg)
    extra = _provider_extra(name, cfg)
    for item in changed:
        raw = next((a for a in accounts if a.get("username") == item["username"]), None)
        if raw:
            await _persist_account(name, raw)  # 单账号窄写 switch 列 + 广播
        if not _set_account_disabled(name, item["username"], not item["switch"]):
            if raw:
                _reload_account_into_pool(name, raw, rpm, extra)
    await _log_operation(token, "batch_switch", "account", name, {"provider": name, "changed": changed}, None)
    return {"ok": True, "changed": len(changed)}


@router.post("/providers/{name}/accounts/batch-add")
async def batch_add_accounts(name: str, data: dict, token: str = Header(None, alias="Authorization")):
    """批量新增账号。所有行先校验，任一行失败时不写入账号。"""
    await _require_admin(token)
    rows = data.get("accounts") if isinstance(data, dict) else None
    if not isinstance(rows, list) or not rows:
        raise HTTPException(status_code=400, detail="accounts 必须为非空数组")

    cfg = await _read_provider_config(name)
    existing = {str(a.get("username")) for a in cfg.get("accounts", []) if a.get("username")}
    seen: set[str] = set()
    proxies = _read_proxies()
    prepared: list[dict] = []
    for index, raw in enumerate(rows, 1):
        if not isinstance(raw, dict):
            raise HTTPException(status_code=400, detail=f"第 {index} 行必须为对象")
        account = dict(raw)
        username = str(account.get("username") or "").strip()
        if not username:
            raise HTTPException(status_code=400, detail=f"第 {index} 行缺少用户名")
        if username in existing or username in seen:
            raise HTTPException(status_code=400, detail=f"第 {index} 行账号 '{username}' 已存在或重复")
        account["username"] = username
        account.setdefault("switch", True)
        prepared.append(_normalize_account_proxy_ref(account, proxies))
        seen.add(username)

    persisted: list[str] = []
    try:
        for account in prepared:
            await _persist_account(name, account)
            persisted.append(account["username"])
    except Exception as exc:
        for username in persisted:
            try:
                await _remove_account_row(name, username)
            except Exception:
                logger.exception("batch account add rollback failed: %s", username)
        raise HTTPException(status_code=500, detail=f"批量新增失败: {exc}") from exc

    rpm = _provider_rpm(cfg)
    extra = _provider_extra(name, cfg)
    if ModelClientPool.get_provider_pool(name):
        for account in prepared:
            _reload_account_into_pool(name, account, rpm, extra, proxies)
    else:
        # 批量写入后重读，避免用写入前的 cfg 启动缺少新账号的运行池。
        await _load_provider_runtime(name, await _read_provider_config(name))
    await _log_operation(token, "batch_add_account", "account", name, None, {"provider": name, "accounts": _scrub_relay_secret(prepared)})
    await _publish_channel_event(name)
    return {"ok": True, "added": persisted, "count": len(persisted)}


@router.post("/providers/{name}/accounts/batch-delete")
async def batch_delete_accounts(name: str, data: dict, token: str = Header(None, alias="Authorization")):
    """批量删除账号，返回逐项结果并为每个成功删除项保留备份。"""
    await _require_admin(token)
    usernames = data.get("usernames") if isinstance(data, dict) else None
    if not isinstance(usernames, list) or not usernames:
        raise HTTPException(status_code=400, detail="usernames 必须为非空数组")
    cfg = await _read_provider_config(name)
    accounts = cfg.get("accounts", [])
    results: list[dict] = []
    deleted: list[str] = []
    for raw_username in usernames:
        username = str(raw_username or "").strip()
        account = next((a for a in accounts if a.get("username") == username), None)
        if not username or account is None:
            results.append({"username": username, "ok": False, "error": "账号不存在"})
            continue
        try:
            _write_delete_backup("account", f"{name}-{username}", {
                "provider": name,
                "username": username,
                "account": account,
                "provider_config": cfg,
            })
            removed = await _remove_account_row(name, username)
            if not removed:
                raise RuntimeError("账号未被删除")
            _remove_account_from_pool(name, username)
            await _cleanup_account_routes(name, username)
            deleted.append(username)
            results.append({"username": username, "ok": True})
        except Exception as exc:
            results.append({"username": username, "ok": False, "error": str(exc)})
    if deleted:
        await _log_operation(token, "batch_delete_account", "account", name, {"provider": name, "usernames": deleted}, None)
        await _publish_channel_event(name)
    failed = [item for item in results if not item["ok"]]
    return {"ok": not failed, "deleted": deleted, "failed": failed, "results": results, "count": len(deleted)}


@router.post("/providers/{name}/accounts/batch-proxy")
async def batch_update_account_proxy(name: str, data: dict, token: str = Header(None, alias="Authorization")):
    """批量修改账号代理；proxy 为空表示清除代理。"""
    await _require_admin(token)
    usernames = data.get("usernames") if isinstance(data, dict) else None
    if not isinstance(usernames, list) or not usernames:
        raise HTTPException(status_code=400, detail="usernames 必须为非空数组")
    proxy_value = data.get("proxy")
    if proxy_value is not None and not isinstance(proxy_value, str):
        raise HTTPException(status_code=400, detail="proxy 必须为字符串或 null")
    cfg = await _read_provider_config(name)
    accounts = cfg.get("accounts", [])
    by_username = {a.get("username"): a for a in accounts}
    normalized_usernames = list(dict.fromkeys(str(u or "").strip() for u in usernames))
    missing = [u for u in normalized_usernames if not u or u not in by_username]
    if missing:
        raise HTTPException(status_code=400, detail=f"账号不存在: {', '.join(missing)}")

    proxies = _read_proxies()
    proxy_ref = (proxy_value or "").strip()
    if proxy_ref and not _find_proxy(proxies, proxy_ref):
        raise HTTPException(status_code=400, detail="代理不存在或已从代理池移除")
    old_accounts = {u: dict(by_username[u]) for u in normalized_usernames}
    updated: list[str] = []
    try:
        for username in normalized_usernames:
            merged = dict(by_username[username])
            if proxy_ref:
                # 先清掉旧 proxy_id，否则 _normalize_account_proxy_ref 会优先取
                # 旧 proxy_id、把刚写入的新 proxy 当字面量丢弃——已绑代理的账号
                # 批量换代理会静默不生效。与清空路径（下方 pop 两个字段）对称。
                merged.pop("proxy_id", None)
                merged["proxy"] = proxy_ref
            else:
                merged.pop("proxy", None)
                merged.pop("proxy_id", None)
            merged = _normalize_account_proxy_ref(merged, proxies)
            await _persist_account(name, merged)
            by_username[username].clear()
            by_username[username].update(merged)
            _reload_account_into_pool(name, merged, _provider_rpm(cfg), _provider_extra(name, cfg), proxies)
            updated.append(username)
    except Exception as exc:
        for username in updated:
            try:
                await _persist_account(name, old_accounts[username])
                _reload_account_into_pool(name, old_accounts[username], _provider_rpm(cfg), _provider_extra(name, cfg), proxies)
            except Exception:
                logger.exception("batch proxy rollback failed: %s", username)
        raise HTTPException(status_code=500, detail=f"批量修改代理失败: {exc}") from exc

    await _log_operation(token, "batch_update_account_proxy", "account", name, None, {"provider": name, "usernames": updated, "proxy": proxy_ref or None})
    await _publish_channel_event(name)
    return {"ok": True, "updated": updated, "count": len(updated)}


# ==================== OAuth Device Flow (已废弃) ====================
# 旧的基于运行池实例 + provider._pending_device_auth 的 device flow 已移除，
# 统一走 /providers/{name}/accounts/auth/{start,status,cancel}（Redis + 中央扫描器）。

_OAUTH_DEPRECATED_DETAIL = "该接口已废弃，请使用 /providers/{name}/accounts/auth/start|status|cancel"


@router.post("/providers/{name}/oauth/start")
async def start_oauth_device_flow(name: str, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    raise HTTPException(status_code=410, detail=_OAUTH_DEPRECATED_DETAIL)


@router.get("/providers/{name}/oauth/status")
async def get_oauth_status(name: str, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    raise HTTPException(status_code=410, detail=_OAUTH_DEPRECATED_DETAIL)


@router.post("/providers/{name}/oauth/cancel")
async def cancel_oauth_device_flow(name: str, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    raise HTTPException(status_code=410, detail=_OAUTH_DEPRECATED_DETAIL)


# ==================== 代理池管理 ====================
# 节点 Release 下载源（release_owner/repo/tag + github_proxy_id）已整体作废：
# 节点程序版本改由市场 node-versions 模块发布与下载，市场仓库地址在
# /api/v1/marketplace/admin/source-config（资源中心「配置」Tab）统一管理。

@router.get("/config/proxies")
async def get_proxies(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    return _normalize_proxies(_read_json(CONFIG_FILE).get("proxies", []))


@router.put("/config/proxies")
async def update_proxies(data: list[dict], token: str = Header(None, alias="Authorization")):
    """全量更新代理池 [{name, url}]"""
    await _require_admin(token)
    cfg = _read_json(CONFIG_FILE)
    old_proxies = cfg.get("proxies", [])
    proxies = _normalize_proxies(data)
    cfg["proxies"] = proxies
    # 异步等待持久化 + 配置缓存刷新完成，再原地刷新运行态代理并广播跨实例事件。
    await _write_json_async(CONFIG_FILE, cfg)
    update_result = _hot_update_proxy_pool(proxies)
    # 同步本实例共享 ProxyManager 的配置缓存（含剪掉已删除条目 + 关闭残留实例），
    # 否则改代理 url/密码/模式在本实例要等 Redis 自收 EVENT_PROXY 或 60s 对账才生效；
    # Redis 不可用时会最长滞后 60s。其它实例仍由 _publish_proxy_event 兜底。
    try:
        from proxy_utils import sync_proxy_manager_configs
        await sync_proxy_manager_configs(proxies)
    except Exception as exc:
        logger.warning(f"[admin] ProxyManager 代理配置同步失败: {exc}")
    await _publish_proxy_event()
    await _log_operation(token, "update_proxies", "proxy", "proxies", _scrub_relay_secret(old_proxies), _scrub_relay_secret(proxies))
    return {"ok": True, "count": len(proxies), "hot_reloaded": update_result["changed"]}


# ==================== 全局配置聚合（各 Tab 独立读写） ====================
# 市场仓库源配置不属于这里：由 monkeycode marketplace 的
# /api/v1/marketplace/admin/source-config 统一管理。

@router.get("/global-config/run-mode")
async def get_global_run_mode_config(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cfg = _read_json(CONFIG_FILE)
    return {"debug": bool((cfg.get("system") or {}).get("debug", False))}


@router.put("/global-config/run-mode")
async def update_global_run_mode_config(data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    if not isinstance(data, dict) or not isinstance(data.get("debug"), bool):
        raise HTTPException(status_code=400, detail="debug 必须是 boolean")
    result = await update_system_config(debug=data["debug"], token=token)
    return result


@router.get("/global-config/proxy-pool")
async def get_global_proxy_pool_config(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    return {"proxies": _normalize_proxies(_read_json(CONFIG_FILE).get("proxies", []))}


@router.put("/global-config/proxy-pool")
async def update_global_proxy_pool_config(data: list[dict], token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    result = await update_proxies(data, token=token)
    return {"ok": result.get("ok", True), "count": result.get("count", 0)}


@router.get("/global-config/channels")
async def get_global_channels_config(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cfg = _strip_bootstrap_config(_read_json(CONFIG_FILE))
    retry = dict(cfg.get("retry") or {})
    if "context_overflow_not_retryable_enabled" not in retry:
        retry["context_overflow_not_retryable_enabled"] = config.DEFAULT_CONTEXT_OVERFLOW_NOT_RETRYABLE_ENABLED
    if "output_interception_rules" not in retry:
        retry["output_interception_rules"] = config.DEFAULT_OUTPUT_INTERCEPTION_RULES
    if "output_interception_rules" in retry:
        retry["output_interception_rules"] = _clean_output_interception_rules(retry["output_interception_rules"])
    account_test = dict(cfg.get("account_test") or {})
    account_test["types"] = _clean_test_types(account_test.get("types"))
    rate_limit = dict(cfg.get("rate_limit") or {})
    if "allow_token_reservation_overflow" not in rate_limit:
        rate_limit["allow_token_reservation_overflow"] = config.DEFAULT_ALLOW_TOKEN_RESERVATION_OVERFLOW
    return {
        "scheduled": {"model_refresh": dict(cfg.get("model_refresh") or {}), "message_delete": dict(cfg.get("message_delete") or {})},
        "scheduled_test": {
            "concurrency": config.Config.scheduled_test_concurrency(),
            "skip_if_requested_within_seconds": config.Config.scheduled_test_skip_if_requested_within(),
        },
        "retry": retry,
        "stream": dict(cfg.get("stream") or {}),
        "rate_limit": rate_limit,
        "default_freeze_policy": get_default_freeze_policy(),
        "account_test": account_test,
        "header_templates": await PostgresClient.get_header_templates(),
        "model_rule_templates": await PostgresClient.get_model_rule_templates(),
    }


@router.put("/global-config/channels")
async def update_global_channels_config(data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="渠道配置必须是对象")
    cfg = await _read_json_async(CONFIG_FILE)
    if isinstance(data.get("scheduled"), dict):
        scheduled = data["scheduled"]
        for key in ("model_refresh", "message_delete"):
            if isinstance(scheduled.get(key), dict):
                cfg[key] = dict(cfg.get(key) or {}) | scheduled[key]
    if isinstance(data.get("scheduled_test"), dict):
        # 定时检测的并发度与「最近请求过则跳过」是全局配置：一个消费者池给所有渠道共用，
        # 所以不落在任何渠道的 scheduled_test 里，只写顶层 config.json。
        raw = data["scheduled_test"]
        existing = dict(cfg.get("scheduled_test") or {})
        if "concurrency" in raw:
            try:
                existing["concurrency"] = max(1, int(raw["concurrency"]))
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="检测并发数必须是正整数")
        if "skip_if_requested_within_seconds" in raw:
            try:
                existing["skip_if_requested_within_seconds"] = max(0, int(raw["skip_if_requested_within_seconds"]))
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="跳过窗口秒数必须是非负整数")
        cfg["scheduled_test"] = existing
    for key in ("retry", "stream", "rate_limit"):
        if isinstance(data.get(key), dict):
            if key == "retry":
                patch = dict(data[key])
                if "non_retryable_parameter_errors" in patch:
                    patch["non_retryable_parameter_errors"] = _clean_non_retryable_rules(patch["non_retryable_parameter_errors"])
                if "output_interception_rules" in patch:
                    patch["output_interception_rules"] = _clean_output_interception_rules(patch["output_interception_rules"])
                cfg[key] = dict(cfg.get(key) or {}) | patch
            elif key == "rate_limit":
                # Reuse the existing normalizer and preserve the isolated Tab semantics.
                raw = data[key]
                codes = raw.get("status_codes", (cfg.get(key) or {}).get("status_codes", [429]))
                if isinstance(codes, (int, str)): codes = [codes]
                codes = [int(x) for x in codes if str(x).lstrip("-").isdigit() and 100 <= int(x) <= 599]
                existing = cfg.get(key) or {}
                allow_overflow = raw.get(
                    "allow_token_reservation_overflow",
                    existing.get("allow_token_reservation_overflow", config.DEFAULT_ALLOW_TOKEN_RESERVATION_OVERFLOW),
                ) is not False
                cfg[key] = {
                    "status_codes": list(dict.fromkeys(codes)) or [429],
                    "cooldown_seconds": max(0, int(raw.get("cooldown_seconds", existing.get("cooldown_seconds", 60)))),
                    "exception_cooldown_seconds": max(0, int(raw.get("exception_cooldown_seconds", existing.get("exception_cooldown_seconds", 30)))),
                    "allow_token_reservation_overflow": allow_overflow,
                }
            else:
                cfg[key] = dict(cfg.get(key) or {}) | data[key]
    if isinstance(data.get("account_test"), dict) and "types" in data["account_test"]:
        cfg.setdefault("account_test", {})["types"] = _clean_test_types(data["account_test"]["types"])
    # 新增渠道默认冻结策略：只作为创建渠道时的快照来源，改这里不动任何已建渠道。
    if isinstance(data.get("default_freeze_policy"), dict):
        try:
            cfg["default_freeze_policy"] = validate_freeze_policy(data["default_freeze_policy"])
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    await _write_json_async(CONFIG_FILE, cfg)
    # 聚合 channels PUT 历史上不刷新内存缓存，导致这里保存的开关（如 allow_token_reservation_overflow）
    # 要等 Redis 300s TTL 过期才生效。显式 reload 让「渠道运行」tab 的改动立即进入热路径。
    await config.Config.reload_async()
    if isinstance(data.get("header_templates"), list):
        saved = await PostgresClient.set_header_templates(_normalize_header_templates(data["header_templates"]))
        ModelClientPool.set_header_template_cache(saved)
        await runtime_sync.publish(runtime_sync.EVENT_HEADER_TEMPLATES, "__default__")
    if isinstance(data.get("model_rule_templates"), list):
        saved = await PostgresClient.set_model_rule_templates(_normalize_model_rule_templates(data["model_rule_templates"]))
        set_model_rule_template_cache(saved)
        await runtime_sync.publish(runtime_sync.EVENT_MODEL_RULE_TEMPLATES, "__default__")
    await _log_operation(token, "update_global_channels_config", "global_config", "channels", None, {k: data[k] for k in data if k not in ("header_templates", "model_rule_templates")})
    return {"ok": True}


@router.get("/global-config/models")
async def get_global_models_config(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    tokenizer = _read_json(CONFIG_FILE).get("tokenizer") or {}
    thinking = await get_thinking_global_config(token=token)
    metadata = await _mm.list_metadata_from_db_async()
    detection = tokenizer.get("context_detection_enabled")
    # tokenizer 规则已由后端内置模型族映射维护，不可编辑。仍回传内置策略摘要供前端只读展示。
    tokenizer_policy = [
        {"family": rule.get("name"), "pattern": rule.get("pattern"), "type": rule.get("type"),
         "encoding": rule.get("encoding"), "repo": rule.get("repo")}
        for rule in usage_utils._BUILTIN_TOKENIZER_RULES if rule.get("enabled") is not False
    ]
    import tokenizer_vocab_check
    return {
        "tokenizer_policy": {"mode": "built_in", "editable": False, "rules": tokenizer_policy,
                             "note": "Token 统计策略由系统按模型自动选择，无需用户配置"},
        "tokenizer_vocab": {
            "enabled": config.Config.tokenizer_vocab_check_enabled(),
            "interval_hours": config.Config.tokenizer_vocab_check_interval_hours(),
            "mirror": config.Config.tokenizer_vocab_mirror(),
            "items": tokenizer_vocab_check.current_status(),
        },
        "context_detection_enabled": detection if isinstance(detection, bool) else config.DEFAULT_CONTEXT_TOKEN_DETECTION_ENABLED,
        "thinking": thinking,
        "owned_by_options": (await get_owned_by_list(token=token)).get("owned_by", []),
        "default_metadata": metadata.get("default"),
    }


@router.put("/global-config/models")
async def update_global_models_config(data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    # tokenizer_rules 已不可编辑：忽略旧前端整包 PUT 里可能携带的字段，不写回主配置。
    if "context_detection_enabled" in data:
        enabled = data["context_detection_enabled"]
        if not isinstance(enabled, bool):
            raise HTTPException(status_code=400, detail="context_detection_enabled 必须是 boolean")
        cfg = await _read_json_async(CONFIG_FILE)
        cfg.setdefault("tokenizer", {})["context_detection_enabled"] = enabled
        await _write_json_async(CONFIG_FILE, cfg)
        # 超限拦截在请求热路径上读该开关，必须立即让内存配置生效。
        await config.Config.reload_async()
        await _log_operation(token, "update_context_token_detection", "main_config", "tokenizer", None, {"context_detection_enabled": enabled})
    if isinstance(data.get("tokenizer_vocab"), dict):
        # 词表版本检查的开关/间隔/镜像。只影响后台检查任务，不碰请求热路径。
        raw = data["tokenizer_vocab"]
        cfg = await _read_json_async(CONFIG_FILE)
        section = dict(cfg.get("tokenizer_vocab_check") or {})
        if "enabled" in raw:
            if not isinstance(raw["enabled"], bool):
                raise HTTPException(status_code=400, detail="tokenizer_vocab.enabled 必须是 boolean")
            section["enabled"] = raw["enabled"]
        if "interval_hours" in raw:
            try:
                hours = int(raw["interval_hours"])
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="tokenizer_vocab.interval_hours 必须是整数")
            if hours < 1 or hours > 720:
                raise HTTPException(status_code=400, detail="tokenizer_vocab.interval_hours 需在 1~720 小时之间")
            section["interval_hours"] = hours
        if "mirror" in raw:
            mirror = str(raw["mirror"] or "").strip().rstrip("/")
            if mirror and not mirror.startswith(("http://", "https://")):
                raise HTTPException(status_code=400, detail="tokenizer_vocab.mirror 必须是 http(s) 地址")
            section["mirror"] = mirror
        cfg["tokenizer_vocab_check"] = section
        await _write_json_async(CONFIG_FILE, cfg)
        await _log_operation(token, "update_tokenizer_vocab_check", "main_config", "tokenizer_vocab_check", None, section)
    if isinstance(data.get("thinking"), dict):
        await update_thinking_global_config(data["thinking"], token=token)
    if isinstance(data.get("default_metadata"), dict):
        await update_model_metadata_default(data["default_metadata"], token=token)
    return {"ok": True}



@router.get("/notifications")
async def query_notifications(
    page: int = 1,
    page_size: int = 20,
    status: str = "",
    severity: str = "",
    kind: str = "",
    event_type: str = "",
    q: str = "",
    token: str = Header(None, alias="Authorization"),
):
    await _require_admin(token)
    page = max(1, int(page or 1))
    page_size = max(1, min(int(page_size or 20), 100))
    offset = (page - 1) * page_size
    filters = {
        "status": status,
        "severity": severity,
        "kind": kind,
        "event_type": event_type,
        "owner_type": "platform",
        "user_id": None,
        "q": q,
    }
    data = await PostgresClient.query_notifications(filters, page_size, offset)
    return {
        "items": data.get("rows") or [],
        "total": data.get("total") or 0,
        "page": page,
        "page_size": page_size,
        "unread_count": data.get("unread_count") or 0,
    }


@router.get("/notifications/summary")
async def get_notifications_summary(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    return await PostgresClient.notification_summary()


@router.put("/notifications/read-all")
async def mark_all_notifications_read(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    updated = await PostgresClient.mark_all_notifications_read()
    return {"ok": True, "updated": updated}


# ==================== 通知订阅（平台级，带参数过滤） ====================
# 订阅规则 = 事件类型 + 出站渠道 + 参数过滤（只盯某些渠道/账号）。平台级规则归属
# 固定哨兵 owner_id（表 owner_id NOT NULL），语义由 owner_type='platform' 承载。
_PLATFORM_OWNER_ID = "00000000-0000-0000-0000-0000000009f1"


@router.get("/notifications/event-types")
async def list_notify_event_types(token: str = Header(None, alias="Authorization")):
    """事件目录（含一级分类 category 与可配范围 owner_scope）。"""
    await _require_admin(token)
    from monkeycode_compat.notify_service import notify_service
    return {"items": notify_service.list_event_types()}


@router.get("/notifications/subscription-rules")
async def list_notify_subscription_rules(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    from monkeycode_compat.notify_service import notify_service
    rows = await notify_service.list_rules(_PLATFORM_OWNER_ID, owner_type="platform")
    return {"items": rows}


@router.post("/notifications/subscription-rules")
async def create_notify_subscription_rule(data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    from monkeycode_compat.notify_service import notify_service
    row = await notify_service.create_rule(_PLATFORM_OWNER_ID, data, owner_type="platform")
    if row is None:
        raise HTTPException(status_code=400, detail="订阅事件或通知渠道无效")
    await _log_operation(token, "create_notify_rule", "notify_rule", str(row.get("id")), None, data)
    return {"ok": True, "rule": row}


@router.put("/notifications/subscription-rules/{rule_id}")
async def update_notify_subscription_rule(rule_id: str, data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    from monkeycode_compat.notify_service import notify_service
    ok = await notify_service.update_rule(_PLATFORM_OWNER_ID, rule_id, data, owner_type="platform")
    if not ok:
        raise HTTPException(status_code=404, detail="订阅规则不存在或无效")
    await _log_operation(token, "update_notify_rule", "notify_rule", rule_id, None, data)
    return {"ok": True}


@router.delete("/notifications/subscription-rules/{rule_id}")
async def delete_notify_subscription_rule(rule_id: str, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    from monkeycode_compat.notify_service import notify_service
    ok = await notify_service.delete_rule(_PLATFORM_OWNER_ID, rule_id, owner_type="platform")
    if not ok:
        raise HTTPException(status_code=404, detail="订阅规则不存在")
    await _log_operation(token, "delete_notify_rule", "notify_rule", rule_id, None, None)
    return {"ok": True}


@router.get("/notifications/events")
async def list_notify_events_admin(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    from monkeycode_compat.notify_service import notify_service
    return {"items": await notify_service.list_events(_PLATFORM_OWNER_ID, owner_type="platform")}


@router.post("/notifications/events")
async def create_notify_event_admin(data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    from monkeycode_compat.notify_service import notify_service
    row = await notify_service.create_event(_PLATFORM_OWNER_ID, data, owner_type="platform")
    if row is None:
        raise HTTPException(status_code=400, detail="事件类型或参数无效")
    await _log_operation(token, "create_notify_event", "notify_event", str(row.get("id")), None, data)
    return {"ok": True, "event": row}


@router.put("/notifications/events/{event_id}")
async def update_notify_event_admin(event_id: str, data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    from monkeycode_compat.notify_service import notify_service
    ok = await notify_service.update_event(_PLATFORM_OWNER_ID, event_id, data, owner_type="platform")
    if not ok:
        raise HTTPException(status_code=404, detail="事件不存在或参数无效")
    await _log_operation(token, "update_notify_event", "notify_event", event_id, None, data)
    return {"ok": True}


@router.delete("/notifications/events/{event_id}")
async def delete_notify_event_admin(event_id: str, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    from monkeycode_compat.notify_service import notify_service
    ok = await notify_service.delete_event(_PLATFORM_OWNER_ID, event_id, owner_type="platform")
    if not ok:
        raise HTTPException(status_code=404, detail="事件不存在")
    await _log_operation(token, "delete_notify_event", "notify_event", event_id, None, None)
    return {"ok": True}


@router.get("/notifications/channels")
async def list_notify_channels_admin(token: str = Header(None, alias="Authorization")):
    """平台级出站渠道列表（供订阅配置选择目标渠道）。"""
    await _require_admin(token)
    from monkeycode_compat.notify_service import notify_service
    return {"items": await notify_service.list_channels(_PLATFORM_OWNER_ID, owner_type="platform")}


@router.post("/notifications/channels")
async def create_notify_channel_admin(data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    from monkeycode_compat.notify_service import notify_service
    try:
        row = await notify_service.create_channel(_PLATFORM_OWNER_ID, data, owner_type="platform")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="创建通知渠道失败") from exc
    await _log_operation(token, "create_notify_channel", "notify_channel", str(row.get("id")), None, data)
    return {"ok": True, "channel": row}


@router.put("/notifications/channels/{channel_id}")
async def update_notify_channel_admin(channel_id: str, data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    from monkeycode_compat.notify_service import notify_service
    ok = await notify_service.update_channel(_PLATFORM_OWNER_ID, channel_id, data, owner_type="platform")
    if not ok:
        raise HTTPException(status_code=404, detail="渠道不存在")
    await _log_operation(token, "update_notify_channel", "notify_channel", channel_id, None, data)
    return {"ok": True}


@router.delete("/notifications/channels/{channel_id}")
async def delete_notify_channel_admin(channel_id: str, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    from monkeycode_compat.notify_service import notify_service
    ok = await notify_service.delete_channel(_PLATFORM_OWNER_ID, channel_id, owner_type="platform")
    if not ok:
        raise HTTPException(status_code=404, detail="渠道不存在")
    await _log_operation(token, "delete_notify_channel", "notify_channel", channel_id, None, None)
    return {"ok": True}


@router.post("/notifications/channels/{channel_id}/test")
async def test_notify_channel_admin(channel_id: str, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    from monkeycode_compat.notify_service import notify_service
    result = await notify_service.test_channel(_PLATFORM_OWNER_ID, channel_id, owner_type="platform")
    if result is None:
        raise HTTPException(status_code=404, detail="渠道不存在")
    ok, error = result
    if not ok:
        return {"ok": False, "message": f"测试消息发送失败：{error}"}
    return {"ok": True, "message": "测试消息已发送"}


@router.post("/notifications/batch-delete")
async def batch_delete_notifications(data: dict, token: str = Header(None, alias="Authorization")):
    """批量删除平台通知。声明在 /{notification_id} 之前，保持静态子路由优先。"""
    await _require_admin(token)
    raw_ids = data.get("ids") if isinstance(data, dict) else None
    if not isinstance(raw_ids, list) or not raw_ids:
        raise HTTPException(status_code=400, detail="ids 必须为非空数组")
    if len(raw_ids) > 500:
        raise HTTPException(status_code=400, detail="一次最多删除 500 条通知")
    try:
        ids = [int(i) for i in raw_ids]
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="ids 必须为整数数组")
    deleted = await PostgresClient.delete_notifications_by_ids(ids)
    if deleted:
        await _log_operation(token, "batch_delete_notification", "notification", None, {"ids": ids}, None)
    return {"ok": True, "deleted": deleted}


@router.post("/notifications/clear-read")
async def clear_read_notifications(token: str = Header(None, alias="Authorization")):
    """清空平台通知里所有已读条目（未读保留）。"""
    await _require_admin(token)
    deleted = await PostgresClient.delete_notifications_by_filter(status="read")
    if deleted:
        await _log_operation(token, "clear_read_notification", "notification", None, {"deleted": deleted}, None)
    return {"ok": True, "deleted": deleted}


@router.post("/notifications/clear-all")
async def clear_all_notifications(token: str = Header(None, alias="Authorization")):
    """清空全部平台通知（含未读）。"""
    await _require_admin(token)
    deleted = await PostgresClient.delete_notifications_by_filter(status=None)
    if deleted:
        await _log_operation(token, "clear_all_notification", "notification", None, {"deleted": deleted}, None)
    return {"ok": True, "deleted": deleted}


@router.get("/notifications/{notification_id}")
async def get_notification_detail(notification_id: int, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    row = await PostgresClient.get_notification_detail(notification_id)
    if not row:
        raise HTTPException(status_code=404, detail="通知不存在")
    return row


@router.put("/notifications/{notification_id}/read")
async def mark_notification_read(notification_id: int, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    ok = await PostgresClient.mark_notification_read(notification_id)
    if not ok:
        raise HTTPException(status_code=404, detail="通知不存在")
    return {"ok": True}


@router.delete("/notifications/{notification_id}")
async def delete_notification(notification_id: int, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    ok = await PostgresClient.delete_notification(notification_id)
    if not ok:
        raise HTTPException(status_code=404, detail="通知不存在")
    await _log_operation(token, "delete_notification", "notification", str(notification_id), None, None)
    return {"ok": True}


# ==================== Agent / 聊天 对话列表（管理端） ====================
# 数据存 ClickHouse（agent/conversation_store），与项目对话（SQLite mc_*）并列。
# 前端管理端对话页通过 tab 切换查看三类对话。游标分页口径与 monkeycode audit 一致。

@router.get("/agent/conversations")
async def admin_list_agent_conversations(
    cursor: str | None = None,
    limit: int = 20,
    token: str = Header(None, alias="Authorization"),
):
    await _require_admin(token)
    from agent import conversation_store
    if not conversation_store.is_ready():
        raise HTTPException(status_code=503, detail="ClickHouse 未启用，Agent 对话不可用")
    return await conversation_store.list_conversations_admin(
        kind="agent", limit=min(max(limit, 1), 100), cursor=cursor
    )


@router.get("/chat/conversations")
async def admin_list_chat_conversations(
    cursor: str | None = None,
    limit: int = 20,
    token: str = Header(None, alias="Authorization"),
):
    await _require_admin(token)
    from agent import conversation_store
    if not conversation_store.is_ready():
        raise HTTPException(status_code=503, detail="ClickHouse 未启用，聊天对话不可用")
    return await conversation_store.list_conversations_admin(
        kind="chat", limit=min(max(limit, 1), 100), cursor=cursor
    )


@router.get("/agent/conversations/{conv_id}")
async def admin_get_agent_conversation(
    conv_id: str,
    token: str = Header(None, alias="Authorization"),
):
    """管理端跨用户读取单条 Agent 对话 + 消息（admin token 鉴权）。

    用户侧 /agent/conversations/{id} 走 session cookie，管理员用 session 登录时
    会被按 user_id 过滤，读别人的对话会 404；这里用 admin token 直接读，不过滤。
    """
    await _require_admin(token)
    from agent import conversation_store
    if not conversation_store.is_ready():
        raise HTTPException(status_code=503, detail="ClickHouse 未启用，Agent 对话不可用")
    conv = await conversation_store.get_conversation(conv_id)
    if conv is None or conv.get("status") == "deleted":
        raise HTTPException(status_code=404, detail="对话不存在")
    messages = await conversation_store.get_messages(conv_id)
    return {"conversation": conv, "messages": messages}


@router.get("/chat/conversations/{conv_id}")
async def admin_get_chat_conversation(
    conv_id: str,
    token: str = Header(None, alias="Authorization"),
):
    """管理端跨用户读取单条聊天对话 + 消息（admin token 鉴权）。"""
    await _require_admin(token)
    from agent import conversation_store
    if not conversation_store.is_ready():
        raise HTTPException(status_code=503, detail="ClickHouse 未启用，聊天对话不可用")
    conv = await conversation_store.get_conversation(conv_id)
    if conv is None or conv.get("status") == "deleted":
        raise HTTPException(status_code=404, detail="对话不存在")
    messages = await conversation_store.get_messages(conv_id)
    return {"conversation": conv, "messages": messages}


# ==================== 最近请求日志 ====================

@router.get("/recent-logs")
async def get_recent_logs(limit: int = 50, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    logs = await PostgresClient.recent_logs(min(limit, 500))
    return logs


async def _attach_provider_remark(rows: list[dict]) -> None:
    """给请求日志行附带 provider_remark（渠道备注/显示名）。

    请求日志页只展示渠道名，不再前端拉全量渠道列表做 id→remark 反查；remark 由这里
    从配置内存快照补到每行。渠道已删除（快照里没有）时留空，前端回落到 provider_name
    （即渠道 id）。一次调用只读一次 get_providers()，列表/详情均 O(行数)。
    """
    if not rows:
        return
    try:
        provider_configs = await config.Config.get_providers() or {}
    except Exception:
        provider_configs = {}
    for row in rows:
        name = row.get("provider_name")
        if not name:
            continue
        cfg = provider_configs.get(name) if isinstance(provider_configs, dict) else None
        remark = ((cfg or {}).get("remark") or "")
        row["provider_remark"] = remark.strip() if isinstance(remark, str) else ""


@router.get("/request-logs")
async def query_request_logs(
    provider_name: str = "",
    account_username: str = "",
    api_key_name: str = "",
    model: str = "",
    success: Optional[bool] = None,
    status: str = "",
    stream: Optional[bool] = None,
    client_type: str = "",
    session_id: str = "",
    editor_id: str = "",
    editor_session_id: str = "",
    start_time: Optional[float] = None,
    end_time: Optional[float] = None,
    source: str = "live",
    limit: int = 100,
    offset: int = 0,
    token: str = Header(None, alias="Authorization"),
):
    t0 = time.perf_counter()
    await _require_admin(token)
    t1 = time.perf_counter()
    filters = {
        "provider_name": provider_name,
        "account_username": account_username,
        "api_key_name": api_key_name,
        "model": model,
        "success": success,
        "status": status,
        "stream": stream,
        "client_type": client_type,
        "session_id": session_id,
        "editor_id": editor_id,
        "editor_session_id": editor_session_id,
        "start_time": start_time,
        "end_time": end_time,
        # 日志归档已下线，source 只查实时主表；保留字段向后兼容前端默认值。
        "source": "live",
    }
    stage = "query"
    try:
        result = await PostgresClient.query_request_logs(filters, min(limit, 500), offset)
        t2 = time.perf_counter()
        stage = "remark"
        await _attach_provider_remark(result.get("rows", []))
        t3 = time.perf_counter()
    except (Exception, asyncio.CancelledError):
        t_err = time.perf_counter()
        logger.warning(
            "[req-logs] raised@{} admin={:.0f}ms elapsed={:.0f}ms",
            stage, (t1 - t0) * 1000, (t_err - t0) * 1000,
        )
        raise
    try:
        rows_n = len(result.get("rows", []))
        total_n = result.get("total", 0)
        logger.info(
            "[req-logs] admin={:.0f}ms query={:.0f}ms remark={:.0f}ms total={:.0f}ms (rows={}/{})",
            (t1 - t0) * 1000, (t2 - t1) * 1000, (t3 - t2) * 1000, (t3 - t0) * 1000,
            rows_n, total_n,
        )
    except Exception:
        pass
    return result


@router.get("/request-logs/{log_id}")
async def get_request_log_detail(log_id: int, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    row = await PostgresClient.get_request_log_detail(log_id)
    if not row:
        raise HTTPException(status_code=404, detail="日志不存在")
    # 顶部行 + 同 request_id 的兄弟 attempts 一次性补 remark，详情弹窗的渠道列与
    # AttemptsView 都能直接显示备注名，无需前端再查渠道列表。
    await _attach_provider_remark([row, *(row.get("attempts") or [])])
    return row


@router.get("/dashboard-stats")
async def get_dashboard_stats(
    provider_name: str = "",
    account_username: str = "",
    api_key_name: str = "",
    provider: str = "",
    account: str = "",
    api_key: str = "",
    model: str = "",
    grain: str = "hour",
    section: str = "all",
    metric: str = "",
    start: Optional[float] = None,
    end: Optional[float] = None,
    start_time: Optional[float] = None,
    end_time: Optional[float] = None,
    token: str = Header(None, alias="Authorization"),
):
    await _require_admin(token)
    return await PostgresClient.dashboard_stats({
        "provider_name": provider_name or provider,
        "account_username": account_username or account,
        "api_key_name": api_key_name or api_key,
        "model": model,
        "grain": grain,
        "section": metric or section,
        "start_time": start_time if start_time is not None else start,
        "end_time": end_time if end_time is not None else end,
    })


# ==================== 统计 ====================

@router.get("/stats")
async def get_stats(include_token_stats: bool = False, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    now = time.time()
    provider_list = []
    for name in ModelClientPool.get_provider_names():
        pool = ModelClientPool.get_provider_pool(name)
        if pool:
            total = len(pool.clients)
            auth_count = sum(1 for c in pool.clients if c.provider.is_init())
            cooldown_count = sum(
                1 for c in pool.clients
                if c.is_frozen
            )
            provider_list.append({
                "name": name,
                "total_accounts": total,
                "auth_ok": auth_count,
                "cooldown": cooldown_count,
            })

    result = {
        "providers": provider_list,
        "models_count": len(ModelClientPool._models),
        "total_requests": await PostgresClient.total_successful_request_count(),
    }
    if include_token_stats:
        result["token_stats"] = await PostgresClient.token_stats()
    return result


# ==================== 模型元数据 ====================

import model_metadata as _mm


_METADATA_FIELDS = (
    "name", "owned_by", "object", "created",
    "max_tokens", "max_context_tokens",
    "input_modalities", "output_modalities", "multimodal",
    "function_calling", "auto_search", "auto_thinking", "is_thinking",
    "capabilities", "icon_url",
    # real 行的按模型渠道标签过滤（写入 model_groups 顶层列，不进 metadata JSONB）。
    "provider_whitelist", "provider_blacklist",
)


def _clean_metadata_payload(data: dict) -> dict:
    cleaned = {}
    for key in _METADATA_FIELDS:
        if key not in data:
            continue
        value = data[key]
        if value is None:
            continue
        if key in ("provider_whitelist", "provider_blacklist"):
            # 与模型组渠道过滤同规则：仅去空白 + 去重，不按存在性丢弃（运行时现取现判）。
            if not isinstance(value, list):
                raise HTTPException(status_code=400, detail=f"{key} 必须是数组")
            values: list[str] = []
            seen: set[str] = set()
            for item in value:
                if not isinstance(item, str) or not item.strip():
                    raise HTTPException(status_code=400, detail=f"{key} 包含非法值")
                tag = item.strip()
                if tag not in seen:
                    seen.add(tag)
                    values.append(tag)
            value = values
        cleaned[key] = value
    return cleaned


def _model_market_payload(metadata_payload: dict) -> dict:
    models = list(metadata_payload.get("models") or [])
    models.sort(key=lambda m: (m.get("model_id") or m.get("id") or ""))
    metadata_by_id = {m.get("model_id") or m.get("id"): m for m in models if m.get("model_id") or m.get("id")}
    runtime_models = []
    route_count = 0
    for model_id, routes in sorted(ModelClientPool.get_runtime_routes_snapshot().items()):
        model_routes = [dict(route) for route in routes]
        route_count += len(model_routes)
        providers = sorted({r.get("provider") for r in model_routes if r.get("provider")})
        metadata = metadata_by_id.get(model_id)
        runtime_models.append({
            "model_id": model_id,
            "routes": model_routes,
            "providers": providers,
            "has_metadata": bool(metadata),
            "available": bool(metadata),
            "metadata": metadata,
            "reason": None if metadata else "missing_metadata",
        })
    return {
        "default": metadata_payload.get("default") or {},
        "models": models,
        "runtime_models": runtime_models,
        "summary": {
            "runtime_model_count": len(runtime_models),
            "route_count": route_count,
            "metadata_model_count": len(models),
            "missing_metadata_count": sum(1 for m in runtime_models if not m["has_metadata"]),
        }
    }


@router.get("/model-metadata")
async def list_model_metadata(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    payload = await _mm.list_metadata_from_db_async()
    return _model_market_payload(payload)


@router.put("/model-metadata/default")
async def update_model_metadata_default(data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    cleaned = _clean_metadata_payload(data)
    # 渠道过滤只属于具体 real 模型；默认元数据不参与选路。
    cleaned.pop("provider_whitelist", None)
    cleaned.pop("provider_blacklist", None)
    new_default = await _mm.update_default_async(cleaned)
    await _after_metadata_write("__default__")
    await _log_operation(token, "update_model_metadata_default", "model_metadata", "default", None, new_default)
    return {"ok": True, "default": new_default}


# ==================== Header 模板库（系统配置） ====================

def _normalize_header_templates(templates: list) -> list[dict]:
    """校验并规范化 header 模板列表：每条需有唯一 id、name，headers 为对象。"""
    if not isinstance(templates, list):
        return []
    seen_ids: set[str] = set()
    result: list[dict] = []
    for index, item in enumerate(templates):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            raise HTTPException(status_code=400, detail=f"header_templates[{index}].name 必填")
        tg_id = str(item.get("id") or "").strip() or f"tpl-{index}"
        base_id = tg_id
        suffix = 1
        while tg_id in seen_ids:
            suffix += 1
            tg_id = f"{base_id}-{suffix}"
        seen_ids.add(tg_id)
        headers = item.get("headers")
        if not isinstance(headers, dict):
            headers = {}
        headers = {str(k): str(v) for k, v in headers.items() if v is not None}
        result.append({"id": tg_id, "name": name, "headers": headers})
    return result


@router.get("/header-templates")
async def list_header_templates(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    return {"templates": await PostgresClient.get_header_templates()}


@router.put("/header-templates")
async def update_header_templates(payload: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    templates = _normalize_header_templates(payload.get("templates") if isinstance(payload, dict) else payload)
    saved = await PostgresClient.set_header_templates(templates)
    ModelClientPool.set_header_template_cache(saved)
    await runtime_sync.publish(runtime_sync.EVENT_HEADER_TEMPLATES, "__default__")
    await _log_operation(token, "update_header_templates", "header_templates", "default", None, {"templates": saved})
    return {"ok": True, "templates": saved}


# ==================== 模型规则模版（全局，供渠道引用） ====================

def _normalize_model_rule_templates(templates: list) -> list[dict]:
    """校验并规范化模型规则模版列表：每条需有唯一 id、name，rules 为内联规则数组。

    模版内只允许内联规则，不允许再引用模版（避免环）；引用型条目在这里被 400 拒。
    每条模版的正则都过 compile 校验，坏正则连模版都存不进去。
    """
    if not isinstance(templates, list):
        return []
    seen_ids: set[str] = set()
    result: list[dict] = []
    for index, item in enumerate(templates):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            raise HTTPException(status_code=400, detail=f"model_rule_templates[{index}].name 必填")
        tg_id = str(item.get("id") or "").strip() or f"tpl-{index}"
        base_id = tg_id
        suffix = 1
        while tg_id in seen_ids:
            suffix += 1
            tg_id = f"{base_id}-{suffix}"
        seen_ids.add(tg_id)
        raw_rules = item.get("rules")
        if raw_rules is None:
            raw_rules = []
        if not isinstance(raw_rules, list):
            raise HTTPException(status_code=400, detail=f"模版「{name}」的 rules 必须是数组")
        if any(is_model_id_template_entry(r) for r in raw_rules if isinstance(r, dict)):
            raise HTTPException(status_code=400, detail=f"模版「{name}」不能再引用其他模版")
        rules, errors = compile_model_id_rewrite_rules(raw_rules)
        if errors:
            raise HTTPException(status_code=400, detail=f"模版「{name}」规则不合法：" + "；".join(errors))
        result.append({"id": tg_id, "name": name, "rules": rules})
    return result


@router.get("/model-rule-templates")
async def list_model_rule_templates(token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    return {"templates": await PostgresClient.get_model_rule_templates()}


@router.put("/model-rule-templates")
async def update_model_rule_templates(payload: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    templates = _normalize_model_rule_templates(payload.get("templates") if isinstance(payload, dict) else payload)
    saved = await PostgresClient.set_model_rule_templates(templates)
    set_model_rule_template_cache(saved)
    await runtime_sync.publish(runtime_sync.EVENT_MODEL_RULE_TEMPLATES, "__default__")
    await _log_operation(token, "update_model_rule_templates", "model_rule_templates", "default", None, {"templates": saved})
    return {"ok": True, "templates": saved}


@router.get("/model-metadata/openrouter/fetch")
async def fetch_openrouter_catalog(token: str = Header(None, alias="Authorization")):
    """拉取 OpenRouter 全量模型供前端挑选导入。"""
    await _require_admin(token)
    try:
        raw = await _mm.fetch_openrouter_models()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"拉取 OpenRouter 失败: {e}")
    items = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        rid = row.get("id")
        if not rid:
            continue
        normalized = _mm.normalize_openrouter_model(row)
        items.append({
            "id": rid,
            "name": row.get("name") or rid,
            "context_length": row.get("context_length"),
            "created": row.get("created"),
            "description": (row.get("description") or "")[:300],
            "normalized": normalized,
        })
    items.sort(key=lambda x: x["id"])
    return {"ok": True, "count": len(items), "items": items}


@router.post("/model-metadata/openrouter/sync")
async def sync_from_openrouter(data: dict, token: str = Header(None, alias="Authorization")):
    """批量把 OpenRouter 选中的模型导入到本地元数据库。

    Body:
        targets: [{openrouter_id: str, model_id?: str}]
                 或 [str]  # 直接给 openrouter_id 列表
    """
    await _require_admin(token)
    targets_raw = data.get("targets") or []
    targets = []
    for entry in targets_raw:
        if isinstance(entry, str):
            targets.append({"openrouter_id": entry})
        elif isinstance(entry, dict):
            targets.append(entry)
    if not targets:
        raise HTTPException(status_code=400, detail="targets 不能为空")

    try:
        raw = await _mm.fetch_openrouter_models()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"拉取 OpenRouter 失败: {e}")
    openrouter_index = {item.get("id"): item for item in raw if isinstance(item, dict) and item.get("id")}

    items = []
    results = []
    for entry in targets:
        openrouter_id = (entry.get("openrouter_id") or entry.get("id") or "").strip()
        if not openrouter_id:
            continue
        source = openrouter_index.get(openrouter_id)
        if not source:
            results.append({"openrouter_id": openrouter_id, "ok": False, "error": "OpenRouter 未找到该模型"})
            continue
        normalized = _mm.normalize_openrouter_model(source)
        model_id = (entry.get("model_id") or openrouter_id).strip()
        item = dict(normalized)
        item["model_id"] = model_id
        items.append(item)
        results.append({"openrouter_id": openrouter_id, "model_id": model_id, "ok": True})

    imported = await _mm.bulk_import_async(items) if items else 0
    await _after_metadata_write("__all__")
    await _log_operation(token, "sync_model_metadata_openrouter", "model_metadata", "sync", None, {"imported": imported, "results": results})
    return {"ok": True, "imported": imported, "results": results}


@router.get("/model-metadata/modelsdev/fetch")
async def fetch_modelsdev_catalog(token: str = Header(None, alias="Authorization")):
    """拉取 models.dev 全量模型供前端挑选导入。"""
    await _require_admin(token)
    try:
        raw = await _mm.fetch_modelsdev()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"拉取 models.dev 失败: {e}")
    items = []
    for entry in _mm.flatten_modelsdev(raw):
        provider_key = entry["provider"]
        mid = entry["id"]
        src = entry["item"]
        normalized = _mm.normalize_modelsdev_model(provider_key, src)
        limit = src.get("limit") or {}
        items.append({
            "provider": provider_key,
            "id": mid,
            "name": src.get("name") or mid,
            "family": src.get("family") or "",
            "context_length": limit.get("context"),
            "output_limit": limit.get("output"),
            "release_date": src.get("release_date"),
            "normalized": normalized,
        })
    items.sort(key=lambda x: (x["provider"], x["id"]))
    providers = sorted({it["provider"] for it in items})
    return {"ok": True, "count": len(items), "providers": providers, "items": items}


@router.post("/model-metadata/modelsdev/sync")
async def sync_from_modelsdev(data: dict, token: str = Header(None, alias="Authorization")):
    """批量把 models.dev 选中的模型导入到本地元数据库。

    Body:
        targets: [{provider: str, source_id: str, model_id?: str}]
    """
    await _require_admin(token)
    targets_raw = data.get("targets") or []
    targets = [entry for entry in targets_raw if isinstance(entry, dict)]
    if not targets:
        raise HTTPException(status_code=400, detail="targets 不能为空")

    try:
        raw = await _mm.fetch_modelsdev()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"拉取 models.dev 失败: {e}")
    index = {(e["provider"], e["id"]): e["item"] for e in _mm.flatten_modelsdev(raw)}

    items = []
    results = []
    for entry in targets:
        provider_key = (entry.get("provider") or "").strip()
        source_id = (entry.get("source_id") or entry.get("id") or "").strip()
        if not provider_key or not source_id:
            continue
        src = index.get((provider_key, source_id))
        if not src:
            results.append({"provider": provider_key, "source_id": source_id, "ok": False, "error": "models.dev 未找到该模型"})
            continue
        normalized = _mm.normalize_modelsdev_model(provider_key, src)
        model_id = (entry.get("model_id") or source_id).strip()
        item = dict(normalized)
        item["model_id"] = model_id
        items.append(item)
        results.append({"provider": provider_key, "source_id": source_id, "model_id": model_id, "ok": True})

    imported = await _mm.bulk_import_async(items) if items else 0
    await _after_metadata_write("__all__")
    await _log_operation(token, "sync_model_metadata_modelsdev", "model_metadata", "sync", None, {"imported": imported, "results": results})
    return {"ok": True, "imported": imported, "results": results}


@router.get("/model-metadata/llm-metadata/fetch")
async def fetch_llm_metadata_catalog(token: str = Header(None, alias="Authorization")):
    """拉取 llm-metadata 全量模型供前端挑选导入。

    llm-metadata 的 dist/api/all.json 是 models.dev 目录与社区 overrides 合并后的构建
    产物，结构与 models.dev 同形（{provider: {models: {id: item}}}），但字段更全且带
    provider 名称/iconURL。这里 flatten 成候选列表并附加 normalized metadata。
    """
    await _require_admin(token)
    try:
        raw = await _mm.fetch_llm_metadata()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"拉取 llm-metadata 失败: {e}")
    items = []
    for entry in _mm.flatten_llm_metadata(raw):
        provider_key = entry["provider"]
        mid = entry["id"]
        src = entry["item"]
        normalized = _mm.normalize_llm_metadata_model(provider_key, src)
        limit = src.get("limit") or {}
        provider_meta = raw.get(provider_key) if isinstance(raw, dict) else None
        items.append({
            "provider": provider_key,
            "id": mid,
            "name": src.get("name") or mid,
            "family": src.get("family") or "",
            "context_length": limit.get("context"),
            "output_limit": limit.get("output"),
            "release_date": src.get("release_date"),
            "normalized": normalized,
        })
    items.sort(key=lambda x: (x["provider"], x["id"]))
    providers = sorted({it["provider"] for it in items})
    return {"ok": True, "count": len(items), "providers": providers, "items": items}


@router.post("/model-metadata/llm-metadata/sync")
async def sync_from_llm_metadata(data: dict, token: str = Header(None, alias="Authorization")):
    """批量把 llm-metadata 选中的模型导入到本地元数据库。

    Body:
        targets: [{provider: str, source_id: str, model_id?: str}]
    """
    await _require_admin(token)
    targets_raw = data.get("targets") or []
    targets = [entry for entry in targets_raw if isinstance(entry, dict)]
    if not targets:
        raise HTTPException(status_code=400, detail="targets 不能为空")

    try:
        raw = await _mm.fetch_llm_metadata()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"拉取 llm-metadata 失败: {e}")
    index = {(e["provider"], e["id"]): e["item"] for e in _mm.flatten_llm_metadata(raw)}

    items = []
    results = []
    for entry in targets:
        provider_key = (entry.get("provider") or "").strip()
        source_id = (entry.get("source_id") or entry.get("id") or "").strip()
        if not provider_key or not source_id:
            continue
        src = index.get((provider_key, source_id))
        if not src:
            results.append({"provider": provider_key, "source_id": source_id, "ok": False, "error": "llm-metadata 未找到该模型"})
            continue
        normalized = _mm.normalize_llm_metadata_model(provider_key, src)
        model_id = (entry.get("model_id") or source_id).strip()
        item = dict(normalized)
        item["model_id"] = model_id
        items.append(item)
        results.append({"provider": provider_key, "source_id": source_id, "model_id": model_id, "ok": True})

    imported = await _mm.bulk_import_async(items) if items else 0
    await _after_metadata_write("__all__")
    await _log_operation(token, "sync_model_metadata_llm_metadata", "model_metadata", "sync", None, {"imported": imported, "results": results})
    return {"ok": True, "imported": imported, "results": results}


@router.put("/model-metadata/{model_id}")
async def upsert_model_metadata_entry(model_id: str, data: dict, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    model_id = model_id.strip()
    if not model_id:
        raise HTTPException(status_code=400, detail="model_id 必填")
    cleaned = _clean_metadata_payload(data)
    try:
        merged = await _mm.upsert_metadata_async(model_id, cleaned)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    await _after_metadata_write(model_id)
    await _log_operation(token, "update_model_metadata", "model_metadata", model_id, None, merged)
    return {"ok": True, "model": merged}


def _sanitize_real_model_routing_payload(data: dict) -> tuple[list[str], list[str], list[dict] | None, str | None]:
    """校验真实模型选路负载。

    返回 (白, 黑, schemes | None, active_scheme | None)。带 schemes 时走方案模式：
    方案数组过与自定义模型同构的校验（id 必填唯一、is_backup 布尔），顶层两列由
    激活方案投影得出，负载顶层对忽略；不带 schemes 时按单对模式只收两个数组。
    """
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="请求体必须是对象")
    raw_schemes = data.get("schemes")
    if raw_schemes is None:
        cleaned = _clean_metadata_payload({
            "provider_whitelist": data.get("provider_whitelist", []),
            "provider_blacklist": data.get("provider_blacklist", []),
        })
        whitelist = cleaned.get("provider_whitelist")
        blacklist = cleaned.get("provider_blacklist")
        if not isinstance(whitelist, list) or not isinstance(blacklist, list):
            raise HTTPException(status_code=400, detail="provider_whitelist/provider_blacklist 必须是数组")
        return whitelist, blacklist, None, None
    if not isinstance(raw_schemes, list):
        raise HTTPException(status_code=400, detail="schemes 必须是数组")
    probe: dict = {"schemes": raw_schemes, "name": data.get("model_id") or "real"}
    _validate_model_group_schemes(probe, str(probe["name"]))
    active_scheme = str(data.get("active_scheme") or "").strip() or None
    return [], [], raw_schemes, active_scheme


@router.put("/model-metadata/{model_id}/routing")
async def update_real_model_routing(model_id: str, data: dict, token: str = Header(None, alias="Authorization")):
    """真实模型选路窄写：只改路由相关列（渠道过滤 / schemes），不触碰元数据。

    带 schemes 时为方案模式：支持多套方案与 is_backup 降级标记，顶层过滤取激活方案投影；
    不带时为单对模式（兼容旧入口）。
    """
    await _require_admin(token)
    model_id = model_id.strip()
    if not model_id:
        raise HTTPException(status_code=400, detail="model_id 必填")
    whitelist, blacklist, schemes, active_scheme = _sanitize_real_model_routing_payload(data)
    row = await PostgresClient.update_real_model_provider_filter(
        model_id, whitelist, blacklist, schemes=schemes, active_scheme=active_scheme,
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"真实模型 '{model_id}' 不存在")
    await _after_metadata_write(model_id)
    await _log_operation(token, "update_model_routing", "model_metadata", model_id, None, {
        "provider_whitelist": list(row.get("provider_whitelist") or []),
        "provider_blacklist": list(row.get("provider_blacklist") or []),
        "schemes": row.get("schemes") or [],
        "active_scheme": row.get("active_scheme") or "",
    })
    return {
        "ok": True,
        "model_id": model_id,
        "provider_whitelist": list(row.get("provider_whitelist") or []),
        "provider_blacklist": list(row.get("provider_blacklist") or []),
        "schemes": row.get("schemes") or [],
        "active_scheme": row.get("active_scheme") or "",
    }


@router.delete("/model-metadata/{model_id}")
async def delete_model_metadata_entry(model_id: str, token: str = Header(None, alias="Authorization")):
    await _require_admin(token)
    ok = await _mm.delete_metadata_async(model_id)
    if not ok:
        raise HTTPException(status_code=404, detail=f"模型 '{model_id}' 不存在")
    await _after_metadata_write(model_id)
    await _log_operation(token, "delete_model_metadata", "model_metadata", model_id, None, None)
    return {"ok": True}


# ==================== 安全模块管理 ====================

@router.get("/security/config")
async def get_security_config(token: str = Header(None, alias="Authorization")):
    """获取安全配置"""
    await _require_admin(token)
    cfg = config.Config._load().get("security", {})
    return {
        "enabled": cfg.get("enabled", True),
        "block_on_high": cfg.get("block_on_high", True),
        "masking_enabled": cfg.get("masking_enabled", True),
        "detectors": cfg.get("detectors", {
            "credential_leak": True,
            "prompt_injection": True,
            "input_validation": True,
        }),
        "input_limits": cfg.get("input_limits", {
            "max_message_size_bytes": 512000,
            "max_messages": 500,
        }),
    }


@router.put("/security/config")
async def update_security_config(data: dict, token: str = Header(None, alias="Authorization")):
    """更新安全配置"""
    await _require_admin(token)
    main_cfg = await config.CONFIG_STORE.read_main_async() if hasattr(config.CONFIG_STORE, "read_main_async") else config.Config._load()
    if not isinstance(main_cfg, dict):
        main_cfg = {}
    security = main_cfg.get("security", {})
    if not isinstance(security, dict):
        security = {}

    for key in ("enabled", "block_on_high", "masking_enabled"):
        if key in data:
            security[key] = bool(data[key])
    if "detectors" in data and isinstance(data["detectors"], dict):
        detectors = security.get("detectors", {})
        if not isinstance(detectors, dict):
            detectors = {}
        detectors.update(data["detectors"])
        security["detectors"] = detectors
    if "input_limits" in data and isinstance(data["input_limits"], dict):
        limits = security.get("input_limits", {})
        if not isinstance(limits, dict):
            limits = {}
        limits.update(data["input_limits"])
        security["input_limits"] = limits

    main_cfg["security"] = security
    if hasattr(config.CONFIG_STORE, "write_main_async"):
        await config.CONFIG_STORE.write_main_async(main_cfg)
    else:
        config.CONFIG_STORE.write_main(main_cfg)

    await _log_operation(token, "update_security_config", "security", "config", None, security)
    return {"ok": True, "security": security}


def _serialize_security_event(row: dict) -> dict:
    """统一序列化 security_events 行：event_time 转 ISO 字符串、detail jsonb 解析为对象。

    asyncpg 读 jsonb 列默认返回 JSON 字符串而非对象，若不解析，前端
    row.detail.prefix 会恒为 undefined，安全事件表格的“详情”列将永远空白。
    """
    item = dict(row)
    if item.get("event_time"):
        item["event_time"] = item["event_time"].isoformat()
    detail = item.get("detail")
    if isinstance(detail, str):
        try:
            item["detail"] = json.loads(detail) if detail else {}
        except (ValueError, TypeError):
            item["detail"] = {}
    elif detail is None:
        item["detail"] = {}
    return item


@router.get("/security/events")
async def get_security_events(
    token: str = Header(None, alias="Authorization"),
    page: int = 1,
    page_size: int = 50,
    severity: Optional[str] = None,
    tag: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
):
    """查询安全事件日志（分页）"""
    await _require_admin(token)

    if not PostgresClient.pool:
        return {"items": [], "total": 0, "page": page, "page_size": page_size}

    conditions = []
    params = []
    idx = 1

    if severity:
        conditions.append(f"severity = ${idx}")
        params.append(severity)
        idx += 1
    if tag:
        conditions.append(f"tag LIKE ${idx}")
        params.append(f"%{tag}%")
        idx += 1
    if start_time:
        conditions.append(f"event_time >= ${idx}")
        params.append(start_time)
        idx += 1
    if end_time:
        conditions.append(f"event_time <= ${idx}")
        params.append(end_time)
        idx += 1

    where = " WHERE " + " AND ".join(conditions) if conditions else ""

    async with PostgresClient.pool.acquire() as conn:
        count_row = await conn.fetchrow(f"SELECT COUNT(*) as total FROM security_events{where}", *params)
        total = count_row["total"] if count_row else 0

        offset = (page - 1) * page_size
        rows = await conn.fetch(
            f"SELECT * FROM security_events{where} ORDER BY event_time DESC LIMIT ${idx} OFFSET ${idx + 1}",
            *params, page_size, offset,
        )

    items = [_serialize_security_event(r) for r in rows]

    return {"items": items, "total": total, "page": page, "page_size": page_size}


@router.get("/security/stats")
async def get_security_stats(
    token: str = Header(None, alias="Authorization"),
    hours: int = 24,
):
    """安全事件统计"""
    await _require_admin(token)

    if not PostgresClient.pool:
        return {"total": 0, "by_severity": {}, "by_tag": {}, "recent": []}

    async with PostgresClient.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) as total FROM security_events WHERE event_time >= NOW() - make_interval(hours => $1)",
            hours,
        )
        total = row["total"] if row else 0

        severity_rows = await conn.fetch(
            "SELECT severity, COUNT(*) as count FROM security_events WHERE event_time >= NOW() - make_interval(hours => $1) GROUP BY severity ORDER BY count DESC",
            hours,
        )
        by_severity = {r["severity"]: r["count"] for r in severity_rows}

        tag_rows = await conn.fetch(
            "SELECT tag, COUNT(*) as count FROM security_events WHERE event_time >= NOW() - make_interval(hours => $1) GROUP BY tag ORDER BY count DESC LIMIT 20",
            hours,
        )
        by_tag = {r["tag"]: r["count"] for r in tag_rows}

        recent_rows = await conn.fetch(
            "SELECT * FROM security_events WHERE event_time >= NOW() - make_interval(hours => $1) ORDER BY event_time DESC LIMIT 10",
            hours,
        )
        recent = [_serialize_security_event(r) for r in recent_rows]

    return {
        "total": total,
        "by_severity": by_severity,
        "by_tag": by_tag,
        "recent": recent,
        "hours": hours,
    }


# ==================== 敏感数据规则管理 ====================

async def _read_main_cfg() -> dict:
    if hasattr(config.CONFIG_STORE, "read_main_async"):
        main_cfg = await config.CONFIG_STORE.read_main_async()
    else:
        main_cfg = config.Config._load()
    return main_cfg if isinstance(main_cfg, dict) else {}


async def _write_main_cfg(main_cfg: dict) -> None:
    if hasattr(config.CONFIG_STORE, "write_main_async"):
        await config.CONFIG_STORE.write_main_async(main_cfg)
    else:
        config.CONFIG_STORE.write_main(main_cfg)


@router.get("/security/rules")
async def get_security_rules(token: str = Header(None, alias="Authorization")):
    """获取敏感数据规则（首次访问时播种内置规则）"""
    await _require_admin(token)
    from security.sensitive_rules import seed_sensitive_rules, normalize_rules_for_storage, refresh_cache

    main_cfg = await _read_main_cfg()
    security = main_cfg.get("security", {})
    if not isinstance(security, dict):
        security = {}
    rules = security.get("sensitive_rules")
    if not isinstance(rules, list) or not rules:
        # 首次访问：播种内置规则并落盘
        rules = normalize_rules_for_storage(seed_sensitive_rules())
        security["sensitive_rules"] = rules
        main_cfg["security"] = security
        await _write_main_cfg(main_cfg)
        refresh_cache()
    else:
        rules = normalize_rules_for_storage([r for r in rules if isinstance(r, dict)])

    return {"rules": rules}


@router.put("/security/rules")
async def update_security_rules(data: dict, token: str = Header(None, alias="Authorization")):
    """全量替换敏感数据规则（处理增删改 + 调序）"""
    await _require_admin(token)
    from security.sensitive_rules import normalize_rules_for_storage, validate_rule, refresh_cache

    raw_rules = data.get("rules")
    if not isinstance(raw_rules, list):
        raise HTTPException(status_code=400, detail="rules 必须是数组")

    # 逐条校验，任一不合法则整体拒绝
    errors = []
    normalized = []
    for idx, raw in enumerate(raw_rules):
        if not isinstance(raw, dict):
            errors.append({"index": idx, "error": "规则必须是对象"})
            continue
        err = validate_rule(raw)
        if err:
            errors.append({"index": idx, "id": raw.get("id"), "error": err})
            continue
        normalized.append(normalize_rules_for_storage([raw])[0])
    if errors:
        return JSONResponse(status_code=400, content={"ok": False, "errors": errors})

    # id 唯一性检查
    ids = [r["id"] for r in normalized]
    dup = {x for x in ids if ids.count(x) > 1}
    if dup:
        return JSONResponse(status_code=400, content={"ok": False, "errors": [{"error": f"id 重复: {','.join(sorted(dup))}"}]})

    main_cfg = await _read_main_cfg()
    security = main_cfg.get("security", {})
    if not isinstance(security, dict):
        security = {}
    old_rules = security.get("sensitive_rules")
    security["sensitive_rules"] = normalized
    main_cfg["security"] = security
    await _write_main_cfg(main_cfg)
    refresh_cache()

    await _log_operation(token, "update_security_rules", "security", "sensitive_rules", old_rules, normalized)
    return {"ok": True, "rules": normalized}


@router.post("/security/rules/test")
async def test_security_rules(data: dict, token: str = Header(None, alias="Authorization")):
    """测试正则规则：输入示例文本，返回命中的规则与片段"""
    await _require_admin(token)
    from security.sensitive_rules import (
        get_sensitive_rules,
        normalize_rule,
        validate_rule,
        _compile_one,
    )

    text = str(data.get("text") or "")
    raw_rule = data.get("rule")

    matches: list[dict] = []

    if isinstance(raw_rule, dict) and raw_rule:
        # 测试单条候选规则（不落盘）
        err = validate_rule(raw_rule)
        if err:
            return JSONResponse(status_code=400, content={"ok": False, "error": err})
        try:
            rule = _compile_one(normalize_rule(raw_rule))
        except Exception as e:
            return JSONResponse(status_code=400, content={"ok": False, "error": f"正则无效: {e}"})
        rules = [rule]
    else:
        rules = get_sensitive_rules()

    for rule in rules:
        fragments = []
        for m in rule.pattern.finditer(text):
            fragments.append({"match": m.group(0), "start": m.start(), "end": m.end()})
        if fragments:
            matches.append({
                "rule_id": rule.id,
                "label": rule.label,
                "severity": rule.severity,
                "fragments": fragments,
            })

    return {"ok": True, "matches": matches}