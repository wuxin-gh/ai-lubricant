"""Desktop ASGI shell: serves the SPA's absolute-path assets, then delegates.

Why this exists: the Vite build emits absolute asset URLs
(``<script src="/assets/index-*.js">`` — user-frontend/dist/index.html:45-46),
but main.py only mounts ``/static`` and ``/admin-static`` (main.py:656, 6022).
``/assets/*`` is not a registered mount and is not in ``_API_PREFIXES``, so the
catch-all at main.py:6025 returns ``index.html`` for it — the browser then
loads HTML as JavaScript and the page is blank. Production hides this behind
nginx (``try_files``); a desktop build has no nginx.

Rather than editing main.py, this wraps it: any request whose path maps to a
real file under ``user-frontend/dist`` is served from disk, everything else
falls through to the unmodified main app. This covers ``/assets/*`` plus the
root-level loose files (``/iconfont.js``, ``/ai-lubricant.svg``, ``/robots.txt``,
logos) and the ``captcha/`` and ``ppt/`` subdirectories.

Docker deployments keep running ``main:app`` directly — their behaviour is
unchanged.
"""
from __future__ import annotations

from pathlib import Path

from desktop import env_bootstrap, paths

# Must precede importing main: main.py calls load_project_env() at import time.
env_bootstrap.apply_env()

from starlette.responses import FileResponse  # noqa: E402
from starlette.types import Receive, Scope, Send  # noqa: E402

from main import app as main_app  # noqa: E402

# Paths that must always reach the backend, never the static resolver. These
# mirror main.py:6019 ``_API_PREFIXES``; the existence check below would already
# reject them, but skipping the filesystem probe keeps the hot path cheap.
_API_PREFIXES = (
    "admin", "agent", "mcp", "v1", "api", "static",
    "docs", "project-docs", "openapi", "redoc",
)


class StaticFirst:
    """ASGI wrapper: serve real files from ``dist``, else delegate downstream."""

    def __init__(self, app, dist: Path) -> None:
        self.app = app
        self.dist = dist.resolve() if dist.exists() else None

    def _resolve(self, url_path: str) -> Path | None:
        """Map a URL path to a file inside ``dist``, or None.

        Rejects traversal by requiring the resolved path to stay under ``dist``.
        ``index.html`` is deliberately NOT served here — SPA routing stays with
        main.py's catch-all so its API-prefix handling keeps working.
        """
        if self.dist is None:
            return None
        rel = url_path.lstrip("/")
        if not rel or rel == "index.html":
            return None
        first = rel.split("/", 1)[0]
        if first in _API_PREFIXES:
            return None
        candidate = (self.dist / rel).resolve()
        if not candidate.is_relative_to(self.dist):
            return None
        if not candidate.is_file():
            return None
        return candidate

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and scope.get("method") in ("GET", "HEAD"):
            target = self._resolve(scope.get("path", ""))
            if target is not None:
                await FileResponse(target)(scope, receive, send)
                return
        await self.app(scope, receive, send)


shell = StaticFirst(main_app, paths.dist_dir())
