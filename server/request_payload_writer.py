"""Request payload writer for ClickHouse (best-effort, async, isolated).

Reads started/finalized events from a background queue and writes them to
ClickHouse. Failures are surfaced via metrics only; they never touch the main
request path, reservations, retries, billing, or the PostgreSQL writer.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from loguru import logger

from clickhouse_config import get_settings

_PAYLOAD_FIELDS = (
    "request_body",
    "request_headers",
    "router_request_body",
    "router_request_headers",
    "router_response_body",
    "response_body",
    "response_headers",
)

_HEADER_FIELDS = frozenset({"request_headers", "router_request_headers", "response_headers"})

# body 内的凭据字段：精确匹配，避免 max_tokens/prompt_tokens 之类被误判。
_SENSITIVE_KEYS = {"authorization", "cookie", "x-api-key", "proxy-authorization", "token", "password", "secret"}


@dataclass
class PayloadEvent:
    kind: str
    request_id: str
    attempt_key: str
    attempt_no: int
    payload: dict[str, Any]
    version: int = 0
    created_at: str = ""


class RequestPayloadWriter:
    """Independent background writer for ClickHouse request payloads."""

    def __init__(self, *, max_events: int = 5000, flush_size: int = 100, flush_interval: float = 0.05):
        self.max_events = max(100, max_events)
        self.flush_size = max(1, flush_size)
        self.flush_interval = max(0.01, flush_interval)
        self._events: deque[PayloadEvent] = deque()
        self._wake = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._accepting = False
        self._stopping = False
        self._client = None
        self._metrics: dict[str, int | float] = {
            "events_enqueued": 0,
            "events_flushed": 0,
            "events_dropped": 0,
            "events_failed": 0,
            "events_retried": 0,
            "high_water_events": 0,
        }

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._accepting = True
        self._stopping = False
        self._task = asyncio.create_task(self._run(), name="request-payload-writer")
        logger.info("[request-payload-writer] started max_events={}", self.max_events)

    async def stop(self) -> None:
        self._stopping = True
        self._accepting = False
        self._wake.set()
        task = self._task
        if task is not None:
            try:
                await asyncio.wait_for(task, timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning("[request-payload-writer] stop timeout")
            finally:
                self._task = None
        await self._close_client()

    def enqueue(self, event: PayloadEvent) -> None:
        if not self._accepting:
            self._metrics["events_dropped"] += 1
            return
        self._events.append(event)
        self._metrics["events_enqueued"] += 1
        self._metrics["high_water_events"] = max(int(self._metrics["high_water_events"]), len(self._events))
        if len(self._events) > self.max_events:
            self._drop_oldest()
        self._wake.set()

    def status(self) -> dict[str, int | float | bool]:
        return {
            **self._metrics,
            "accepting": self._accepting,
            "worker_running": bool(self._task and not self._task.done()),
            "queued_events": len(self._events),
        }

    def _drop_oldest(self) -> None:
        if not self._events:
            return
        self._events.popleft()
        self._metrics["events_dropped"] += 1

    async def _run(self) -> None:
        from integrations.clickhouse import ClickHousePayloadClient
        from clickhouse_config import get_settings

        ch = get_settings()
        try:
            client = ClickHousePayloadClient(
                addr=ch.addr,
                database=ch.database,
                username=ch.username,
                password=ch.password,
            )
            await client.connect()
            await client.ensure_schema(ttl_days=ch.ttl_days)
            self._client = client
        except Exception:
            logger.exception("[request-payload-writer] failed to init ClickHouse client; events dropped")
            while not self._stopping:
                self._events.clear()
                self._wake.clear()
                await self._wake.wait()
            return

        while not self._stopping or self._events:
            if not self._events:
                self._wake.clear()
                await self._wake.wait()
                continue
            batch = self._collect_batch()
            if not batch:
                await asyncio.sleep(self.flush_interval)
                continue
            try:
                await self._flush_batch(batch)
                self._metrics["events_flushed"] += len(batch)
            except Exception:
                logger.exception("[request-payload-writer] flush failed; batch dropped")
                self._metrics["events_failed"] += len(batch)
            await asyncio.sleep(self.flush_interval)

    def _collect_batch(self) -> list[PayloadEvent]:
        batch: list[PayloadEvent] = []
        while len(batch) < self.flush_size and self._events:
            batch.append(self._events.popleft())
        return batch

    async def _flush_batch(self, batch: list[PayloadEvent]) -> None:
        from integrations.clickhouse import ClickHousePayloadEvent

        events = []
        for event in batch:
            metadata = {k: v for k, v in event.payload.items() if k not in _PAYLOAD_FIELDS}
            payload = {k: event.payload.get(k) for k in _PAYLOAD_FIELDS if k in event.payload}
            payload = self._sanitize_payload(payload)
            # 归一保证 json.dumps 不会因 bytes/datetime 等非 JSON 原生类型抛 TypeError
            # （否则整批 flush_size 条无关请求的 payload 会被一起丢弃）。
            metadata = self._coerce_json_safe(metadata)
            payload = self._coerce_json_safe(payload)
            events.append(
                ClickHousePayloadEvent(
                    kind=event.kind,
                    request_id=event.request_id,
                    attempt_key=event.attempt_key,
                    attempt_no=event.attempt_no,
                    version=event.version,
                    created_at=event.created_at or time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                    metadata=metadata,
                    payload=payload,
                )
            )
        await self._client.insert_events(events)

    def _sanitize_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            key: self._sanitize_dict(value)
            if key in _HEADER_FIELDS and isinstance(value, dict)
            else self._sanitize_value(key, value)
            for key, value in payload.items()
        }

    @staticmethod
    def _coerce_json_safe(value: Any) -> Any:
        """把 payload 树里的非 JSON 原生类型降级成字符串。

        payload 是尽力而为的观测数据，而 _flush_batch 的失败粒度是整批：单个不可
        序列化的值（bytes / datetime / set / 自定义对象）会让 json.dumps 抛
        TypeError，同批最多 flush_size 条无关请求的 payload 一起被丢弃。这里在
        序列化前统一降级，把「一个坏值毒死一整批」变成「那个字段退化成字符串」。
        """
        if isinstance(value, (bytes, bytearray)):
            return bytes(value).decode("utf-8", errors="replace")
        if isinstance(value, dict):
            return {str(k): RequestPayloadWriter._coerce_json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [RequestPayloadWriter._coerce_json_safe(item) for item in value]
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return repr(value)

    def _sanitize_value(self, key: str, value: Any) -> Any:
        """遮蔽 body 内的凭据字段 —— 整体 ***，且只认精确键名。

        与 header 的处理有意不同：
        - 整体遮蔽：body 里的凭据是「被传输的内容」而非「本次用的哪把 key」，
          排查渠道不需要认出它，保留片段只是白送泄露面。
        - 精确键名：body 里 ``max_tokens``/``prompt_tokens`` 等含 "token" 但不是凭据，
          子串匹配会把用量数字也遮成星号。
        """
        if key.lower() in _SENSITIVE_KEYS:
            return "***"
        if isinstance(value, dict):
            return {k: self._sanitize_value(k, v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._sanitize_value(key, item) for item in value]
        return value

    def _sanitize_dict(self, data: dict[str, Any]) -> dict[str, Any]:
        """遮蔽 header 字典 —— 按 header 名子串判定，覆盖各上游协议的认证头。

        各协议认证头名字不统一（anthropic 用 x-api-key、gemini 用 x-goog-api-key、
        openai/responses 用 Authorization，spec 渠道还能自定义），穷举列表必漏，
        故这里统一交给 security 的敏感头判定。
        """
        from security import is_sensitive_header, sanitize_header_value

        return {
            k: sanitize_header_value(k, v) if is_sensitive_header(k) else v
            for k, v in data.items()
        }

    async def _close_client(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            try:
                await client.close()
            except Exception:
                logger.exception("[request-payload-writer] client close failed")


payload_writer = RequestPayloadWriter()
