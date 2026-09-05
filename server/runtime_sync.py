"""跨实例运行时同步：Redis pub/sub + 60s 增量对账兜底。

设计（见重构方案 Part 基础 / Part 三）：
- 事件模型：发布 ``{type, name, version}`` 到单一频道；订阅端按 ``type`` 分发到注册的
  handler。channel/model 等事件可按 ``name`` 重读单条；metadata/group 事件携带具名变更
  （``__all__`` / ``__default__`` / 模型或组名），接收端据此记录原因并原子重载目录快照。
- 丢消息兜底：Redis pub/sub 发后即忘、断线期间的消息全丢。60s 对账循环按版本/更新时间
  拉回漏掉的变更，pub/sub 管"快"、对账管"最终一致"。
- 乱序：消息带 version（updated_at 或时间戳），收到旧于内存已见版本的直接丢弃。
- 自收自发：handler 重读一遍天然幂等，无害，不特判。
- 启动空窗：调用方负责"先订阅、再全量加载"（见 main.py lifespan）。
- Redis 不可用：publish/订阅均降级为 no-op，单实例靠直接内存更新照常工作。

事件类型：channel / account / model / metadata / group / apikey / cooldown / proxy / main_config / header_templates / tokenizer_vocab。
"""

from __future__ import annotations

import asyncio
import json
import time
import traceback
from typing import Awaitable, Callable

from loguru import logger

# 单一同步频道。手动带上 redis 前缀，避免与共享同一 Redis 的其它部署串台
# （publish/pubsub 是 coredis 原生方法，不经过 RedisJdbc.get_key 自动加前缀）。
_CHANNEL_BASE = "ai-lubricant:runtime-sync"

# 事件类型常量
EVENT_CHANNEL = "channel"
EVENT_ACCOUNT = "account"
EVENT_MODEL = "model"
EVENT_METADATA = "metadata"
EVENT_GROUP = "group"
EVENT_APIKEY = "apikey"
EVENT_COOLDOWN = "cooldown"
EVENT_PROXY = "proxy"
EVENT_MAIN_CONFIG = "main_config"
EVENT_HEADER_TEMPLATES = "header_templates"
EVENT_MODEL_RULE_TEMPLATES = "model_rule_templates"
EVENT_CHANNEL_CATALOG = "channel_catalog"
EVENT_NODE_RELEASE = "node_release"
EVENT_MOBILE_RELEASE = "mobile_release"
EVENT_DEVICE_CONTROL_RELEASE = "device_control_release"
EVENT_TOKENIZER_VOCAB = "tokenizer_vocab"

# type -> handler(name: str | None, payload: dict)；name 可为实体名或 __all__/__default__ 哨兵
Handler = Callable[[str | None, dict], Awaitable[None]]
_handlers: dict[str, list[Handler]] = {}

# 对账回调：无参 async fn，周期性全量比对内存与 DB
Reconciler = Callable[[], Awaitable[None]]
_reconcilers: list[Reconciler] = []

# 每个 (type, name) 最近见过的版本，用于丢弃乱序旧消息
_last_version: dict[tuple[str, str], float] = {}

# 本实例唯一 id，用于（可选）识别自己发的消息
_INSTANCE_ID = f"{time.time()}:{id(object())}"

_subscriber_task: asyncio.Task | None = None
_reconcile_task: asyncio.Task | None = None
_subscribed = asyncio.Event()
_started = False


def _redis():
    try:
        from rd import JdbcClient
        return JdbcClient.redis
    except Exception:
        return None


def _channel() -> str:
    redis = _redis()
    prefix = getattr(redis, "prefix_key", None)
    return f"{prefix}:{_CHANNEL_BASE}" if prefix else _CHANNEL_BASE


def register_handler(event_type: str, handler: Handler) -> None:
    """注册某类事件的内存更新 handler。可多次注册（多个消费方）。"""
    _handlers.setdefault(event_type, []).append(handler)


def register_reconciler(reconciler: Reconciler) -> None:
    """注册 60s 对账回调。"""
    _reconcilers.append(reconciler)


async def publish(event_type: str, name: str | None = None, version: float | None = None,
                  extra: dict | None = None) -> None:
    """发布一条同步事件。Redis 不可用时静默 no-op。

    - ``name`` 作为实体的标识（ID/主键），接收方应凭借它去 DB/Redis 重读最新权威状态。
    - ``extra`` 仅做冗余/透传辅助信号，不应被当作权威数据直接落内存。
    - ``version`` 缺省用当前时间戳；调用方若有 updated_at 应显式传入以支持乱序丢弃。
    """
    redis = _redis()
    if redis is None:
        return
    payload = {
        "type": event_type,
        "name": name,
        "version": version if version is not None else time.time(),
        "origin": _INSTANCE_ID,
    }
    if extra:
        payload["extra"] = extra
    try:
        await redis.publish(_channel(), json.dumps(payload, ensure_ascii=False))
    except Exception as exc:
        logger.warning(f"[runtime_sync] publish {event_type}/{name} 失败: {exc}")


async def _dispatch(payload: dict) -> None:
    event_type = payload.get("type")
    name = payload.get("name")
    version = payload.get("version")
    if not event_type:
        return
    # 乱序丢弃：同 (type, name) 收到不新于已见版本的消息忽略
    if version is not None and name is not None:
        key = (event_type, str(name))
        seen = _last_version.get(key)
        if seen is not None and float(version) <= seen:
            return
        _last_version[key] = float(version)
    for handler in _handlers.get(event_type, []):
        try:
            await handler(name, payload)
        except Exception as exc:
            logger.warning(f"[runtime_sync] handler {event_type}/{name} 失败: {exc}")


async def _subscribe_loop() -> None:
    redis = _redis()
    if redis is None:
        logger.info("[runtime_sync] 无 Redis，跳过订阅（单实例模式）")
        _subscribed.set()
        return
    channel = _channel()
    while True:
        try:
            ps = redis.pubsub(ignore_subscribe_messages=True)
            try:
                await ps.subscribe(channel)
                # Pub/Sub is intentionally idle while no event is published.  The
                # shared command client has a finite socket_timeout for ordinary
                # requests; inheriting it here would turn a quiet subscription into
                # a reconnect every stream_timeout seconds.  This connection is
                # dedicated to Pub/Sub, so disable only its response timeout.
                if ps.connection is not None:
                    ps.connection.socket_timeout = None
                _subscribed.set()
                logger.info(f"[runtime_sync] 已订阅同步频道 {channel}")
                async for message in ps.listen():
                    if message is None:
                        continue
                    if message.get("type") != "message":
                        continue
                    data = message.get("data")
                    if isinstance(data, bytes):
                        data = data.decode("utf-8", errors="replace")
                    try:
                        payload = json.loads(data)
                    except Exception:
                        continue
                    await _dispatch(payload)
            finally:
                await ps.aclose()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            # coredis pubsub 把 reader/keepalive/consumer 的异常包成 ExceptionGroup 抛出，
            # {exc} 的 str() 只显示 "unhandled errors in a TaskGroup (N sub-exception)"，
            # 真正的子异常被吞掉、无法定位根因。这里用 repr 展开子异常 + 打全 traceback。
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            logger.warning(f"[runtime_sync] 订阅中断，3s 后重连: {exc!r}\n{tb}")
            await asyncio.sleep(3)


async def _reconcile_loop(interval_seconds: int = 60) -> None:
    while True:
        try:
            await asyncio.sleep(interval_seconds)
            for reconciler in _reconcilers:
                try:
                    await reconciler()
                except Exception as exc:
                    logger.warning(f"[runtime_sync] 对账回调失败: {exc}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"[runtime_sync] 对账循环异常: {exc}")


async def start(reconcile_interval_seconds: int = 60) -> None:
    """启动订阅 + 对账后台任务。应在全量加载**之前**调用（先订阅后加载，避免空窗）。"""
    global _subscriber_task, _reconcile_task, _started
    if _started:
        return
    _started = True
    _subscribed.clear()
    _subscriber_task = asyncio.create_task(_subscribe_loop())
    _reconcile_task = asyncio.create_task(_reconcile_loop(reconcile_interval_seconds))
    try:
        await asyncio.wait_for(_subscribed.wait(), timeout=5.0)
    except asyncio.TimeoutError:
        logger.warning("[runtime_sync] 等待 Redis 订阅就绪超时，继续启动并依赖重连/对账")
    logger.info("[runtime_sync] 已启动订阅与对账任务")


async def stop() -> None:
    global _subscriber_task, _reconcile_task, _started
    for task in (_subscriber_task, _reconcile_task):
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
    _subscriber_task = None
    _reconcile_task = None
    _subscribed.clear()
    _started = False
