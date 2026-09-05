import os
import sys
from unittest.mock import patch, MagicMock

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from agent.config import AgentConfig
from agent.runner import AgentTaskRunner


@pytest.fixture(autouse=True)
def _reset_runner():
    """Ensure AgentTaskRunner state is clean before/after each test."""
    AgentTaskRunner._started = False
    if hasattr(AgentTaskRunner, "_scheduler"):
        delattr(AgentTaskRunner, "_scheduler")
    yield
    AgentTaskRunner._started = False
    if hasattr(AgentTaskRunner, "_scheduler"):
        delattr(AgentTaskRunner, "_scheduler")


def test_start_respects_scheduler_enabled_false():
    config = AgentConfig(scheduler_enabled=False)
    AgentTaskRunner.start(config=config)
    assert AgentTaskRunner._started is False


def test_start_calls_scheduler_start():
    config = AgentConfig(scheduler_enabled=True)
    with patch("agent.runner.AgentScheduler") as MockScheduler:
        mock_instance = MockScheduler.return_value
        AgentTaskRunner.start(config=config)
        assert AgentTaskRunner._started is True
        mock_instance.start.assert_called_once()


def test_start_is_idempotent():
    config = AgentConfig(scheduler_enabled=True)
    with patch("agent.runner.AgentScheduler") as MockScheduler:
        AgentTaskRunner.start(config=config)
        AgentTaskRunner.start(config=config)
        # AgentScheduler() constructor called only once
        MockScheduler.assert_called_once()


@pytest.mark.asyncio
async def test_stop_calls_scheduler_shutdown():
    config = AgentConfig(scheduler_enabled=True)
    with patch("agent.runner.AgentScheduler") as MockScheduler:
        mock_instance = MockScheduler.return_value
        AgentTaskRunner.start(config=config)
        await AgentTaskRunner.stop()
        mock_instance.shutdown.assert_called_once_with(wait=True)
        assert AgentTaskRunner._started is False


@pytest.mark.asyncio
async def test_stop_noop_when_not_started():
    await AgentTaskRunner.stop()
    assert AgentTaskRunner._started is False
