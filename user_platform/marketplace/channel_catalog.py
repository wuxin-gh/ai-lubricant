"""预同步并缓存统一渠道目录。

GitHub 仅是渠道定义发布源；添加渠道时前端只读本模块的本地快照。远端失败时
保留 PostgreSQL 中最后一次成功快照，系统内置渠道与通用渠道始终可用。
"""
from __future__ import annotations

import asyncio
import copy
import datetime as dt
import json
from typing import Any, Callable

import aiohttp
from loguru import logger

from channel import default_config_with_chat_protocols
from db import PostgresClient

from . import config as mp_config
from .validator import validate_manifest

SNAPSHOT_KEY = "channel_catalog_snapshot"
MAX_FILE_BYTES = 256 * 1024
FETCH_TIMEOUT_SECONDS = 20
DEFAULT_SYNC_SECONDS = 600

_lock = asyncio.Lock()
_snapshot: dict[str, Any] = {"remote_items": [], "updated_at": "", "stale": True}
_builtin_loader: Callable[[], list[dict[str, Any]]] | None = None


def configure_builtin_loader(loader: Callable[[], list[dict[str, Any]]]) -> None:
    global _builtin_loader
    _builtin_loader = loader


def _default_protocol_path(protocol: str) -> str:
    return {
        "openai": "/v1/chat/completions",
        "anthropic": "/v1/messages",
        "responses": "/v1/responses",
        "gemini": "/v1beta/models/{model}:{method}",
    }.get(protocol, "/v1/chat/completions")


def _preset_from_config(cfg: dict[str, Any]) -> dict[str, Any]:
    normalized = default_config_with_chat_protocols(copy.deepcopy(cfg)) or {}
    rows = normalized.get("chat_protocols")
    if not isinstance(rows, list) or not rows:
        proto = str(cfg.get("protocol") or "openai").lower()
        rows = [{
            "enabled": True,
            "protocol": proto,
            "path": str(cfg.get("chat_path") or _default_protocol_path(proto)),
            "upstream_stream": cfg.get("upstream_stream", True),
            "client_preset": str(cfg.get("client_preset") or "none"),
            "system_type": "auto",
            "send_reasoning_content": cfg.get("send_reasoning_content") is not False,
            "models": [],
        }]
    return {
        # manifest 的 channel 用 name 存展示名（build_manifest 里 channel.name = remark），
        # 渠道基础配置用 remark。两侧同义，这里都认，否则选模板预填时渠道名会丢、回退成「自定义」。
        "remark": str(cfg.get("remark") or cfg.get("name") or ""),
        "tags": list(cfg.get("tags") or []),
        "enabled": cfg.get("enabled", True) is not False,
        "base_url": str(cfg.get("base_url") or ""),
        "models_path": str(cfg.get("models_path") or ""),
        "image_path": str(cfg.get("image_path") or "/v1/images/generations"),
        "video_path": str(cfg.get("video_path") or "/v1/videos/generations"),
        "speech_path": str(cfg.get("speech_path") or "/v1/audio/speech"),
        "website_url": str(cfg.get("website_url") or ""),
        "icon": str(cfg.get("icon") or ""),
        "timeout": int(cfg.get("timeout") or 120),
        "retry_count": cfg.get("retry_count"),
        "extra_retry_status_codes": list(cfg.get("extra_retry_status_codes") or []),
        "billing_mode": str(cfg.get("billing_mode") or "token"),
        "chat_protocols": rows,
        "rate_limit": dict(cfg.get("rate_limit") or {}),
        "account_priority": int(cfg.get("account_priority") or 0),
        "account_weight": int(cfg.get("account_weight") or 1),
        "auto_update_models": bool(cfg.get("auto_update_models", False)),
        "model_id_rewrite_rules": copy.deepcopy(cfg.get("model_id_rewrite_rules") or []),
        "freeze_policy": copy.deepcopy(cfg.get("freeze_policy") or {"enabled": True, "rules": []}),
    }


def _generic_entry() -> dict[str, Any]:
    return {
        "id": "custom",
        "name": "通用渠道",
        "description": "配置任意 OpenAI、Anthropic、Responses 或 Gemini 兼容服务",
        "category": "通用",
        "tags": ["兼容接口"],
        "builtin_type": "",
        "preset": {
            "remark": "",
            "enabled": True,
            "billing_mode": "token",
            "base_url": "",
            "timeout": 120,
            "chat_protocols": [{
                "enabled": True,
                "protocol": "openai",
                "path": "/v1/chat/completions",
                "upstream_stream": "auto",
                "client_preset": "none",
                "system_type": "auto",
                "send_reasoning_content": True,
                "models": [],
            }],
            "models_path": "/v1/models",
            "image_path": "/v1/images/generations",
            "video_path": "/v1/videos/generations",
            "speech_path": "/v1/audio/speech",
        },
    }


def _code_entry() -> dict[str, Any]:
    """代码渠道：贴一个 spec 类（普通类 + @staticmethod 钩子）即造一个完整渠道。

    preset.code 是默认模板，建渠道时回填到编辑器，作者改写或替换。
    没写的钩子回落 CustomProvider 配置驱动实现；账号体系（池/冻结/冷却/重试/登录态存
    redis）与授权能力（account_schema 声明字段、设备码/回调授权、定时刷新）全复用现有机制。
    """
    # 代码渠道最小样例（EchoChannel）。完整方法 / 字段 / 参数说明在前端「源码」Tab
    # 点「使用说明」弹框查看，或见 docs/providers/code-channel.md。
    # - 写一个普通类（不继承任何基类），方法用 @staticmethod、首参 p = 渠道实例。
    # - 只写想改的钩子；没写的回落 CustomProvider 配置驱动实现。
    # - 不要写 PROVIDER_NAME：loader 强制覆盖为渠道 id（对齐 redis_prefix / 日志）。
    # 与前端 Channels.tsx::CODE_CHANNEL_SAMPLE 保持一致，改一处记得同步另一处。
    sample_code = (
        'class EchoChannel:\n'
        '    """最小样例：把用户最后一条消息原样回吐。完整说明点「使用说明」。"""\n'
        '\n'
        '    # 本样例不打外网，所以豁免「渠道地址」必填校验。真实渠道删掉这行：\n'
        '    # 渠道地址由用户在渠道配置里填，spec 用 p.base_url 读取，别写死域名。\n'
        '    REQUIRES_BASE_URL = False\n'
        '\n'
        '    @staticmethod\n'
        '    async def init_auth(p, is_check=False):\n'
        '        return True\n'
        '\n'
        '    @staticmethod\n'
        '    async def fetch_models(p):\n'
        '        return [{"id": "echo", "name": "Echo"}]\n'
        '\n'
        '    @staticmethod\n'
        '    async def stream_chat(p, model_id, messages, **kwargs):\n'
        '        msg = messages[-1].get("content", "") if messages else ""\n'
        '        yield {}\n'
        '        yield {"content": msg, "thinking": "", "tool_calls": []}\n'
        '\n'
        '    # 不写 non_stream_chat：框架自动把流式帧聚合成非流式响应。\n'
        '    # 不写 usage：框架按内容自动估算；要自定义就 yield {"usage": {...}}。\n'
    )
    return {
        "id": "code",
        "name": "自定义渠道",
        "description": "贴一个 spec 类（普通类 + @staticmethod 钩子）即造一个完整渠道，支持自定义登录/请求/解析/授权。仅授权管理员，可执行任意代码。",
        "category": "通用",
        "tags": ["自定义", "代码"],
        "builtin_type": "code",
        "preset": {
            "remark": "",
            "enabled": True,
            "billing_mode": "token",
            "base_url": "",
            "timeout": 120,
            "builtin_type": "code",
            "code": sample_code,
            "chat_protocols": [{
                "enabled": True,
                "protocol": "openai",
                "path": "/v1/chat/completions",
                "upstream_stream": "auto",
                "client_preset": "none",
                "system_type": "auto",
                "send_reasoning_content": True,
                "models": [],
            }],
            "models_path": "/v1/models",
        },
    }


def _system_entries() -> list[dict[str, Any]]:
    if _builtin_loader is None:
        return []
    try:
        return copy.deepcopy(_builtin_loader())
    except Exception as exc:
        logger.warning("[channel-catalog] build system entries failed: {}", exc)
        return []


def _resolve_header_names(items: list[dict[str, Any]], templates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_name: dict[str, list[str]] = {}
    for template in templates:
        name = str(template.get("name") or "").strip()
        template_id = str(template.get("id") or "").strip()
        if name and template_id:
            by_name.setdefault(name, []).append(template_id)
    result = copy.deepcopy(items)
    for item in result:
        warnings: list[str] = []
        rows = (item.get("preset") or {}).get("chat_protocols") or []
        for row in rows:
            name = str(row.pop("header_template_name", "") or "").strip()
            if not name:
                continue
            matches = by_name.get(name, [])
            if len(matches) == 1:
                row["header_template"] = matches[0]
            else:
                row["header_template"] = ""
                warnings.append(f"未解析到唯一的本地 Header 模板「{name}」，请手动选择")
        if warnings:
            item.setdefault("preset", {})["template_warnings"] = warnings
    return result


async def _remote_items() -> tuple[list[dict[str, Any]], str, bool]:
    """远端渠道模板条目：store 是编辑真相源，未填充时回落 raw 快照。

    store 已填充（本部署代管市场编辑）时直接投影 PG 里 published 的 channels
    manifest——管理端保存即对「添加渠道」弹窗生效，不必等 GitHub 发布与 raw/CDN
    传播。只读部署 / bootstrap 完成前 store 为空，继续用 ``_snapshot``（定时
    sync_loop 拉 raw 的最后一次成功结果）。
    """
    try:
        import marketplace_store as store

        if await store.is_populated():
            manifests = await store.list_manifests("channels", include_hidden=False)
            items: list[dict[str, Any]] = []
            for manifest in manifests:
                if not isinstance(manifest, dict):
                    continue
                try:
                    items.append(_manifest_entry(manifest))
                except Exception as exc:  # noqa: BLE001 - 单条坏模板不该整表不可用
                    logger.warning("[channel-catalog] skip bad stored manifest {}: {}", manifest.get("id"), exc)
            return items, dt.datetime.now(dt.timezone.utc).isoformat(), False
    except Exception as exc:  # noqa: BLE001 - store 不可用时退回快照，不影响加渠道
        logger.warning("[channel-catalog] read store failed; falling back to snapshot: {}", exc)
    return (
        copy.deepcopy(_snapshot.get("remote_items") or []),
        str(_snapshot.get("updated_at") or ""),
        bool(_snapshot.get("stale", True)),
    )


async def get_catalog() -> dict[str, Any]:
    remote, remote_updated_at, remote_stale = await _remote_items()
    merged: dict[str, dict[str, Any]] = {
        "custom": _generic_entry(),
        "code": _code_entry(),
    }
    for item in _system_entries():
        merged[str(item.get("id") or item.get("builtin_type"))] = item
    for item in remote:
        builtin_type = str(item.get("builtin_type") or "")
        key = builtin_type or str(item.get("id") or "")
        if not key:
            continue
        if builtin_type and key in merged:
            base = merged[key]
            preset = {**(base.get("preset") or {}), **(item.get("preset") or {})}
            merged[key] = {**base, **item, "id": key, "builtin_type": builtin_type, "preset": preset}
        else:
            merged[key] = item
    templates = await PostgresClient.get_header_templates()
    items = _resolve_header_names(list(merged.values()), templates)
    return {
        "items": items,
        "updated_at": remote_updated_at,
        "stale": remote_stale,
    }


async def load_snapshot() -> None:
    global _snapshot
    try:
        stored = await PostgresClient.get_config(SNAPSHOT_KEY)
    except Exception as exc:
        logger.warning("[channel-catalog] load snapshot failed: {}", exc)
        return
    if isinstance(stored, dict) and isinstance(stored.get("remote_items"), list):
        _snapshot = copy.deepcopy(stored)
        logger.info("[channel-catalog] loaded {} remote channels from snapshot", len(_snapshot["remote_items"]))


def _raw_url(path: str) -> str:
    """消费侧 raw 直链：渠道目录是服务端消费（加渠道弹窗读本地缓存），按
    consumer_settings 的平台渲染——配了 Gitee 镜像即从 Gitee 拉取，未配时坐标
    回落生产仓库、行为与从前一致。"""
    from . import urls

    return urls.consumer_raw_url(path)


async def _fetch_json(path: str) -> Any:
    """经 proxy_manager 拉 raw JSON（用资源中心配置的 proxy_id，空=直连）。

    不再用裸 aiohttp session：浏览器/前端都收口到服务端代理后，后台同步也走同一条
    出口，避免「配了代理但只有部分链路生效」。proxy_manager 内部已做连接复用，
    并发拉取仍高效。
    """
    from providers.proxy_manager import get_proxy_manager

    resp = await get_proxy_manager().request(
        url=_raw_url(path),
        method="GET",
        headers={"User-Agent": "ai-lubricant-channel-catalog"},
        timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT_SECONDS),
        proxy_config_id=mp_config.settings.proxy_id or None,
    )
    if resp.status != 200:
        raise RuntimeError(f"{path} returned HTTP {resp.status}")
    raw = await resp.read()
    if len(raw) > MAX_FILE_BYTES:
        raise RuntimeError(f"{path} exceeds {MAX_FILE_BYTES} bytes")
    return json.loads(raw.decode("utf-8"))


def _manifest_entry(manifest: dict[str, Any]) -> dict[str, Any]:
    resource = manifest.get("resource") or {}
    channel = copy.deepcopy(resource.get("channel") or {})
    builtin_type = str(channel.pop("builtin_type", "") or "").strip()
    channel["freeze_policy"] = copy.deepcopy(resource.get("freeze_policy") or {"enabled": True, "rules": []})
    # manifest 的 tags 在顶层，渠道基础配置读 preset.tags；不补进来选模板时标签就丢了。
    if not channel.get("tags"):
        channel["tags"] = list(manifest.get("tags") or [])
    # manifest 顶层 icon 是模板图标真相源，预填到创建弹窗；website_url 在 resource.channel 层，
    # 已随 channel 透传，无需再补。
    if not channel.get("icon"):
        channel["icon"] = str(manifest.get("icon") or "")
    # channel 用 name 存展示名，渠道基础配置读 preset.remark；不补 remark 选模板预填时
    # 渠道名会空、回退成「自定义」。优先用 channel.name，没有则用 manifest 展示名。
    if not channel.get("remark"):
        channel["remark"] = str(channel.get("name") or manifest.get("display_name") or manifest.get("name") or "")
    return {
        "id": str(manifest.get("id") or ""),
        "name": str(manifest.get("display_name") or manifest.get("name") or manifest.get("id") or ""),
        "description": str(manifest.get("summary") or manifest.get("description") or ""),
        "category": str(manifest.get("category") or ""),
        "tags": list(manifest.get("tags") or []),
        "icon": str(manifest.get("icon") or "").strip(),
        "builtin_type": builtin_type,
        "preset": channel,
    }


async def apply_authoritative_manifests(
    manifests: list[dict[str, Any]], *, publish: bool = True
) -> dict[str, Any]:
    """Apply manifests just written through GitHub Contents API to the local cache.

    The import path already holds authoritative payloads, so it must not wait for
    raw/CDN propagation. Only supplied IDs are replaced; other cached entries are
    retained until the normal periodic refresh observes external repository changes.
    """
    global _snapshot
    if not isinstance(manifests, list) or not manifests:
        raise ValueError("没有可应用的渠道模板")

    entries: dict[str, dict[str, Any]] = {}
    for manifest in manifests:
        if not isinstance(manifest, dict):
            raise ValueError("渠道模板 manifest 必须是对象")
        errors = validate_manifest("channels", manifest)
        if errors:
            item_id = str(manifest.get("id") or "(missing id)")
            raise ValueError(f"{item_id}: {'; '.join(errors)}")
        entry = _manifest_entry(manifest)
        item_id = str(entry.get("id") or "")
        if not item_id:
            raise ValueError("渠道模板 id 不能为空")
        entries[item_id] = entry

    async with _lock:
        merged = [
            copy.deepcopy(item)
            for item in (_snapshot.get("remote_items") or [])
            if isinstance(item, dict) and str(item.get("id") or "") not in entries
        ]
        merged.extend(copy.deepcopy(entry) for entry in entries.values())
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        next_snapshot = {"remote_items": merged, "updated_at": now, "stale": False}
        await PostgresClient.set_config(SNAPSHOT_KEY, next_snapshot)
        _snapshot = copy.deepcopy(next_snapshot)

    if publish:
        import runtime_sync
        await runtime_sync.publish(runtime_sync.EVENT_CHANNEL_CATALOG, "__all__")
    logger.info("[channel-catalog] applied {} authoritative channels", len(entries))
    return {"ok": True, **await get_catalog()}


async def refresh(*, publish: bool = True) -> dict[str, Any]:
    global _snapshot
    # writable 部署 store 是真相源：禁止 raw 全量刷新覆盖本地最新编辑（publish/CDN 尚未
    # 传播时会把旧数据拉回来）。只读部署 / bootstrap 前 store 为空才走旧同步。
    try:
        import marketplace_store as store

        if await store.is_populated():
            return {"ok": True, "skipped": "store source", **await get_catalog()}
    except Exception:  # noqa: BLE001 - store 不可用时按旧 raw 路径降级
        pass
    # 渠道目录是消费侧行为：启停与坐标都看 consumer_settings（默认回落生产配置）。
    settings = mp_config.consumer_settings
    if not settings.enabled or "channels" not in settings.modules:
        return {"ok": False, "error": "渠道目录源未启用", **await get_catalog()}
    async with _lock:
        try:
            index = await _fetch_json(f"modules/channels/{settings.index_name}")
            rows = index.get("items") if isinstance(index, dict) else None
            if not isinstance(rows, list):
                raise RuntimeError("channels index.items must be an array")
            visible = [row for row in rows if isinstance(row, dict) and row.get("status") not in ("hidden", "deleted", "draft")]
            semaphore = asyncio.Semaphore(6)

            async def load(row: dict[str, Any]) -> dict[str, Any]:
                raw_id = str(row.get("id") or "")
                path = str(row.get("item_path") or f"modules/channels/items/{raw_id.replace('/', '.')}.json")
                async with semaphore:
                    manifest = await _fetch_json(path)
                errors = validate_manifest("channels", manifest)
                if errors:
                    raise RuntimeError(f"{raw_id}: {'; '.join(errors)}")
                return _manifest_entry(manifest)

            remote_items = await asyncio.gather(*(load(row) for row in visible))
            now = dt.datetime.now(dt.timezone.utc).isoformat()
            next_snapshot = {"remote_items": remote_items, "updated_at": now, "stale": False}
            await PostgresClient.set_config(SNAPSHOT_KEY, next_snapshot)
            _snapshot = copy.deepcopy(next_snapshot)
            if publish:
                import runtime_sync
                await runtime_sync.publish(runtime_sync.EVENT_CHANNEL_CATALOG, "__all__")
            logger.info("[channel-catalog] synced {} remote channels", len(remote_items))
            return {"ok": True, **await get_catalog()}
        except Exception as exc:
            _snapshot["stale"] = True
            logger.warning("[channel-catalog] refresh failed; keeping last snapshot: {}", exc)
            return {"ok": False, "error": str(exc), **await get_catalog()}


async def reload_from_db() -> None:
    await load_snapshot()


async def sync_loop() -> None:
    """渠道目录每小时同步一次，不受开关影响。

    渠道模板属于固定仓库布局的一部分，统一每小时拉取一次；管理端不再暴露开关与周期，
    也不再读取 ``auto_sync_enabled`` / ``sync_interval_minutes``。
    """
    while True:
        try:
            await refresh()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[channel-catalog] sync cycle failed: {}", exc)
        await asyncio.sleep(3600)
