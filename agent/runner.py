"""后台调度器生命周期管理。

实际调度逻辑由 APScheduler + AgentScheduler 处理，本模块仅负责
在 main.py lifespan 中启停 AgentScheduler。
"""
from __future__ import annotations

import asyncio

from loguru import logger

from agent.config import AgentConfig
from agent.scheduler import AgentScheduler


class AgentTaskRunner:
    """Class-level singleton，管理 AgentScheduler 的生命周期。"""

    _started: bool = False

    @classmethod
    def start(cls, config: AgentConfig | None = None) -> None:
        """启动调度器。scheduler_enabled=False 时不启动。"""
        if config and not config.scheduler_enabled:
            logger.info("定时任务调度器未启用 (scheduler_enabled=False)")
            return
        if cls._started:
            return
        scheduler = AgentScheduler()
        scheduler.start()
        cls._started = True
        cls._scheduler = scheduler
        logger.info("定时任务调度器启动")

    @classmethod
    async def stop(cls) -> None:
        """停止调度器。"""
        if not cls._started:
            return
        if hasattr(cls, "_scheduler"):
            # wait=False：关闭时不阻塞等待正在执行的定时任务跑完。wait=True 是
            # 同步阻塞，会占住事件循环，连 lifespan 里的超时兜底都无法中断，
            # 导致 Ctrl+C 挂死。
            cls._scheduler.shutdown(wait=False)
        cls._started = False
        logger.info("定时任务调度器已停止")
