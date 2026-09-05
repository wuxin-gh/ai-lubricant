"""Non-blocking request-log lifecycle writer.

Request handlers only enqueue start/final snapshots. A single managed worker persists
those snapshots in order, so PostgreSQL latency never participates in routing/retry.
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Literal

from loguru import logger

from db import PostgresClient


_PAYLOAD_FIELDS = (
    "request_body",
    "request_headers",
    "router_request_body",
    "router_request_headers",
    "router_response_body",
    "response_body",
    "response_headers",
)


@dataclass
class RequestLogEvent:
    kind: Literal["started", "finalized"]
    request_id: str
    attempt_key: str
    attempt_no: int
    payload: dict
    enqueued_at_monotonic: float = field(default_factory=time.monotonic)


@dataclass
class AttemptWriteState:
    request_id: str
    attempt_key: str
    attempt_no: int
    started_payload: dict | None = None
    final_payload: dict | None = None
    inserted: bool = False
    start_dropped: bool = False
    retry_count: int = 0
    next_retry_at: float = 0.0
    created_monotonic: float = field(default_factory=time.monotonic)


class RequestLogWriter:
    """One-process, bounded request-log writer with lifecycle ordering.

    The writer deliberately trades observability fidelity for application availability
    during a prolonged database outage. Admission never awaits and never performs DB
    work. Large bodies/headers never enter this queue (they are fanned out to
    ClickHouse first), so queue pressure only sheds whole attempts:

    1. drop oldest failed attempts;
    2. at the hard cap, drop oldest remaining attempts.
    """

    def __init__(
        self,
        *,
        max_events: int = 5_000,
        level1_events: int = 3_500,
        level2_events: int = 4_250,
        flush_size: int = 100,
        flush_interval: float = 0.05,
        pressure_batch_size: int = 100,
        retry_max_delay: float = 5.0,
    ) -> None:
        self.max_events = max(100, max_events)
        self.level1_events = min(max(1, level1_events), self.max_events)
        self.level2_events = min(max(self.level1_events, level2_events), self.max_events)
        self.flush_size = max(1, flush_size)
        self.flush_interval = max(0.01, flush_interval)
        self.pressure_batch_size = max(1, pressure_batch_size)
        self.retry_max_delay = max(0.1, retry_max_delay)

        self._events: deque[RequestLogEvent] = deque()
        self._states: OrderedDict[str, AttemptWriteState] = OrderedDict()
        self._wake = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._accepting = False
        self._stopping = False
        self._last_success_monotonic: float | None = None
        self._last_error: str = ""
        # 写完 finalized 行后回调（如补记 API Key token 计费）。注入式，避免反向依赖 main。
        self._on_finalized: Callable[[dict], Awaitable[None]] | None = None

        self._metrics: dict[str, int | float] = {
            "events_enqueued": 0,
            "events_flushed": 0,
            "starts_inserted": 0,
            "finals_updated": 0,
            "finals_coalesced": 0,
            "retries": 0,
            "permanent_failures": 0,
            "failed_attempts_dropped": 0,
            "hard_cap_attempts_dropped": 0,
            "starts_dropped": 0,
            "finals_dropped": 0,
            "finalization_loss": 0,
            "shutdown_abandoned": 0,
            "high_water_events": 0,
        }

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._accepting = True
        self._stopping = False
        self._task = asyncio.create_task(self._run(), name="request-log-writer")
        logger.info(
            "[request-log-writer] started max_events={} level1={} level2={} flush={}/{}ms",
            self.max_events,
            self.level1_events,
            self.level2_events,
            self.flush_size,
            int(self.flush_interval * 1000),
        )

    def set_on_finalized(self, cb: Callable[[dict], Awaitable[None]] | None) -> None:
        """注入 finalized 回调；worker 成功 finalize 一行后调用，回调异常不阻断写库。"""
        self._on_finalized = cb

    def enqueue_started(self, *, request_id: str, attempt_key: str, attempt_no: int, payload: dict) -> None:
        self._enqueue(RequestLogEvent("started", request_id, attempt_key, attempt_no, dict(payload)))

    def enqueue_finalized(self, *, request_id: str, attempt_key: str, attempt_no: int, payload: dict) -> None:
        self._enqueue(RequestLogEvent("finalized", request_id, attempt_key, attempt_no, dict(payload)))

    def _enqueue(self, event: RequestLogEvent) -> None:
        if not self._accepting:
            self._count_drop(event, "writer_stopped")
            return
        self._fanout_payload(event)
        event.payload = self._metadata_only(event.payload)
        self._events.append(event)
        self._metrics["events_enqueued"] += 1
        self._metrics["high_water_events"] = max(
            int(self._metrics["high_water_events"]), self._buffered_event_count()
        )
        self._apply_pressure()
        self._wake.set()

    def _buffered_event_count(self) -> int:
        return len(self._events) + len(self._states)

    @staticmethod
    def _metadata_only(payload: dict) -> dict:
        """Keep large request/response payloads exclusively in ClickHouse."""
        return {key: value for key, value in payload.items() if key not in _PAYLOAD_FIELDS}

    @staticmethod
    def _fanout_payload(event: RequestLogEvent) -> None:
        """Best-effort payload fan-out to the authoritative payload store."""
        try:
            from clickhouse_config import get_settings

            ch = get_settings()
            # enabled 是唯一开关：关闭时大字段既不进 ClickHouse、PG 侧也只留 metadata
            # （fan-out 整个跳过）。默认 false，开启前需完成隐私评估（见 governance）。
            if not ch.enabled:
                return
            from request_payload_writer import PayloadEvent, payload_writer

            payload_writer.enqueue(
                PayloadEvent(
                    kind=event.kind,
                    request_id=event.request_id,
                    attempt_key=event.attempt_key,
                    attempt_no=event.attempt_no,
                    payload=dict(event.payload),
                    version=1 if event.kind == "started" else 2,
                )
            )
        except Exception:
            # Logging fan-out must never affect queue admission or routing.
            return

    def _apply_pressure(self) -> None:
        # Payloads are stripped to metadata before admission (bodies live in
        # ClickHouse), so there is no large-field stripping stage anymore.
        if self._buffered_event_count() >= self.level2_events:
            self._drop_oldest_failed_attempts()
        if self._buffered_event_count() >= self.max_events:
            self._drop_oldest_attempts()

    @staticmethod
    def _state_failed(state: AttemptWriteState) -> bool:
        payload = state.final_payload
        return bool(payload and (not payload.get("success") or payload.get("status") not in ("ok", "requesting")))

    def _drop_state(self, key: str, *, reason: str) -> None:
        state = self._states.pop(key, None)
        if state is None:
            return
        if state.inserted and state.final_payload is not None:
            self._metrics["finalization_loss"] += 1
        if state.started_payload is not None:
            self._metrics["starts_dropped"] += 1
        if state.final_payload is not None:
            self._metrics["finals_dropped"] += 1
        if reason == "failed":
            self._metrics["failed_attempts_dropped"] += 1
        else:
            self._metrics["hard_cap_attempts_dropped"] += 1

    def _drop_oldest_failed_attempts(self) -> None:
        dropped = 0
        for key, state in list(self._states.items()):
            if dropped >= self.pressure_batch_size:
                break
            if self._state_failed(state):
                self._drop_state(key, reason="failed")
                dropped += 1
        if dropped:
            logger.warning("[request-log-writer] queue pressure dropped {} oldest failed attempts", dropped)

    def _drop_oldest_attempts(self) -> None:
        target = max(self.level2_events, self.max_events - self.pressure_batch_size)
        dropped = 0
        while self._buffered_event_count() > target and dropped < self.pressure_batch_size:
            if self._states:
                key = next(iter(self._states))
                self._drop_state(key, reason="hard_cap")
            elif self._events:
                event = self._events.popleft()
                self._count_drop(event, "hard_cap")
            else:
                break
            dropped += 1
        if dropped:
            logger.error(
                "[request-log-writer] hard cap dropped {} oldest attempts/events; buffered={}",
                dropped,
                self._buffered_event_count(),
            )

    def _count_drop(self, event: RequestLogEvent, reason: str) -> None:
        if event.kind == "started":
            self._metrics["starts_dropped"] += 1
        else:
            self._metrics["finals_dropped"] += 1
        logger.warning(
            "[request-log-writer] dropped {} event reason={} request_id={} attempt={}",
            event.kind,
            reason,
            event.request_id,
            event.attempt_no,
        )

    def _consume_events(self) -> None:
        while self._events:
            event = self._events.popleft()
            state = self._states.get(event.attempt_key)
            if state is None:
                state = AttemptWriteState(event.request_id, event.attempt_key, event.attempt_no)
                self._states[event.attempt_key] = state
            if event.kind == "started":
                if state.started_payload is None:
                    state.started_payload = event.payload
                continue
            if state.final_payload is not None:
                self._metrics["finals_coalesced"] += 1
            state.final_payload = event.payload

    async def _flush(self) -> None:
        self._consume_events()
        if not self._states:
            return
        now = time.monotonic()
        starts = [
            state.started_payload
            for state in self._states.values()
            if state.started_payload is not None and not state.inserted and state.next_retry_at <= now
        ][: self.flush_size]
        if starts:
            try:
                inserted_keys = await PostgresClient.insert_request_log_batch(starts)
            except Exception as exc:
                self._defer_states(starts, exc)
                return
            inserted_set = set(inserted_keys)
            for state in self._states.values():
                if state.attempt_key in inserted_set:
                    state.inserted = True
                    self._metrics["starts_inserted"] += 1

        finals = [
            state.final_payload
            for state in self._states.values()
            if state.inserted and state.final_payload is not None and state.next_retry_at <= now
        ][: self.flush_size]
        if finals:
            try:
                finalized_keys = await PostgresClient.finalize_request_log_batch(finals)
            except Exception as exc:
                self._defer_states(finals, exc)
                return
            on_finalized = self._on_finalized
            for key in finalized_keys:
                state = self._states.pop(key, None)
                self._metrics["finals_updated"] += 1
                if state is None or on_finalized is None or state.final_payload is None:
                    continue
                try:
                    await on_finalized(state.final_payload)
                except Exception:
                    logger.exception("[request-log-writer] on_finalized callback failed")
        self._metrics["events_flushed"] += len(starts) + len(finals)
        self._last_success_monotonic = time.monotonic()
        self._last_error = ""

    def _defer_states(self, payloads: list[dict], exc: Exception) -> None:
        self._last_error = f"{type(exc).__name__}: {exc}"
        keys = {str(payload.get("attempt_key") or "") for payload in payloads}
        now = time.monotonic()
        for key in keys:
            state = self._states.get(key)
            if state is None:
                continue
            state.retry_count += 1
            delay = min(self.retry_max_delay, 0.1 * (2 ** min(state.retry_count, 8)))
            state.next_retry_at = now + delay + random.uniform(0, delay * 0.2)
            self._metrics["retries"] += 1
        logger.warning("[request-log-writer] deferred {} log writes after DB error: {}", len(keys), exc)

    async def _run(self) -> None:
        try:
            while not self._stopping or self._events or self._states:
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self.flush_interval)
                except asyncio.TimeoutError:
                    pass
                self._wake.clear()
                await self._flush()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._last_error = f"worker crashed: {type(exc).__name__}: {exc}"
            logger.exception("[request-log-writer] worker crashed")

    async def stop(self, drain_timeout: float = 10.0) -> None:
        self._accepting = False
        self._stopping = True
        self._wake.set()
        task = self._task
        if task is None:
            return
        try:
            await asyncio.wait_for(task, timeout=drain_timeout)
        except asyncio.TimeoutError:
            abandoned = self._buffered_event_count()
            self._metrics["shutdown_abandoned"] += abandoned
            logger.warning("[request-log-writer] drain timeout; abandoning {} buffered events", abandoned)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        finally:
            self._task = None

    def status(self) -> dict:
        oldest = None
        if self._states:
            oldest = next(iter(self._states.values())).created_monotonic
        elif self._events:
            oldest = self._events[0].enqueued_at_monotonic
        return {
            **self._metrics,
            "accepting": self._accepting,
            "worker_running": bool(self._task and not self._task.done()),
            "buffered_events": self._buffered_event_count(),
            "pending_attempts": len(self._states),
            "oldest_event_age_ms": int((time.monotonic() - oldest) * 1000) if oldest else 0,
            "last_success_age_ms": int((time.monotonic() - self._last_success_monotonic) * 1000)
            if self._last_success_monotonic
            else None,
            "last_error": self._last_error,
        }


request_log_writer = RequestLogWriter()
