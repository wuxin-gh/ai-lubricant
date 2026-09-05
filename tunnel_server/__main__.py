"""Run the standalone tunnel runtime service with Hypercorn + h2c."""
from __future__ import annotations

import asyncio
from pathlib import Path

# 业务模块收在 server/ 目录下（根目录只保留入口与本文件绑定的配置）。
import sys as _sys

_sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "server"))

from dotenv_loader import load_project_env

load_project_env()

from .config import settings


async def _serve() -> None:
    from hypercorn.asyncio import serve
    from hypercorn.config import Config

    import hypercorn_h2_patch

    from .app import app

    hypercorn_h2_patch.apply()
    cfg = Config()
    cfg.bind = [f"{settings.host}:{settings.port}"]
    cfg.alpn_protocols = ["h2", "http/1.1"]
    cfg.keep_alive_timeout = 3600
    await serve(app, cfg)


if __name__ == "__main__":
    asyncio.run(_serve())
