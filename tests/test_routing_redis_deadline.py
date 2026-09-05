import asyncio
import gc
import time

import pytest

from limits.manager import routing_redis_call


@pytest.mark.asyncio
async def test_routing_redis_call_returns_without_waiting_for_cancel_cleanup():
    cleanup_finished = asyncio.Event()

    async def cancellation_slow_operation():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await asyncio.sleep(0.2)
            cleanup_finished.set()
            return "late"

    started = time.monotonic()
    result = await routing_redis_call(
        cancellation_slow_operation(),
        stage="test_hard_deadline",
        timeout=0.01,
        default=None,
        skip_when_degraded=False,
    )
    elapsed = time.monotonic() - started

    assert result is None
    assert elapsed < 0.1
    await asyncio.wait_for(cleanup_finished.wait(), timeout=1)


@pytest.mark.asyncio
async def test_routing_redis_call_consumes_predegraded_gather_cancellation():
    from limits.manager import begin_routing_redis_scope, end_routing_redis_scope

    loop = asyncio.get_running_loop()
    unhandled = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: unhandled.append(context))
    token = begin_routing_redis_scope()
    try:
        async def slow_operation():
            await asyncio.sleep(10)

        first = await routing_redis_call(
            slow_operation(),
            stage="trigger_degraded",
            timeout=0.01,
            default=None,
        )
        assert first is None

        skipped_gather = asyncio.gather(slow_operation())
        second = await routing_redis_call(
            skipped_gather,
            stage="skipped_after_degraded",
            default="fallback",
        )
        assert second == "fallback"

        for _ in range(10):
            if skipped_gather.done():
                break
            await asyncio.sleep(0)
        assert skipped_gather.done()
        del skipped_gather
        gc.collect()
        await asyncio.sleep(0)
        assert unhandled == []
    finally:
        end_routing_redis_scope(token)
        loop.set_exception_handler(previous_handler)
