"""Silence Hypercorn's HTTP/2 stream-teardown races.

Hypercorn's h2 protocol handler looks streams up as ``self.streams[stream_id]``
while dispatching a batch of h2 events. When a peer resets or ends a stream and
a frame for that same stream is still in flight, the stream is already gone from
the dict and the lookup raises ``KeyError``. The exception escapes through the
``TCPServer`` TaskGroup and asyncio prints a full
``Task exception was never retrieved`` traceback, even though nothing is broken:
that one connection is simply over.

Our long-lived ``NodeConnect`` bidi streams (h2c) hit this on every node
reconnect, so the log fills with tracebacks that carry no information. This
module wraps ``H2Protocol._handle_events`` so events are dispatched one at a
time and a ``KeyError`` for an already-closed stream is swallowed with a debug
line instead of tearing down the batch.

Dispatching one event at a time preserves ordering and keeps every other event
in the batch working; only the event for the vanished stream is dropped, which
is exactly what upstream already does for ``StreamEnded`` (it has a ``try/except
KeyError: pass`` there — this extends the same tolerance to the other stream
events).

Call :func:`apply` once before starting the server. It is idempotent.
"""
from __future__ import annotations

from loguru import logger

_APPLIED = False


def apply() -> bool:
    """Patch Hypercorn's h2 event dispatch. Returns True when the patch is live.

    Never raises: a Hypercorn version whose internals moved just leaves the
    noisy-but-harmless tracebacks in place rather than breaking startup.
    """
    global _APPLIED
    if _APPLIED:
        return True
    try:
        from hypercorn.protocol.h2 import H2Protocol
    except Exception as exc:  # noqa: BLE001 - hypercorn absent/renamed
        logger.debug("[h2patch] hypercorn h2 protocol unavailable: {}", exc)
        return False

    original = getattr(H2Protocol, "_handle_events", None)
    if original is None or getattr(original, "_h2_keyerror_guarded", False):
        _APPLIED = original is not None
        return _APPLIED

    async def _handle_events(self, events) -> None:  # type: ignore[no-untyped-def]
        # One event per call: a KeyError from a closed stream drops only that
        # event instead of aborting the rest of the batch.
        for event in events:
            try:
                await original(self, [event])
            except KeyError as exc:
                logger.debug(
                    "[h2patch] dropped {} for closed stream {}",
                    type(event).__name__,
                    exc.args[0] if exc.args else "?",
                )

    _handle_events._h2_keyerror_guarded = True  # type: ignore[attr-defined]
    H2Protocol._handle_events = _handle_events  # type: ignore[assignment]
    _APPLIED = True
    logger.info("[h2patch] hypercorn h2 stream-teardown KeyError suppressed")
    return True
