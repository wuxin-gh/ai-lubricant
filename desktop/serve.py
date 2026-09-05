"""Uvicorn entry for the main service inside the desktop build.

Runs ``desktop.shell_asgi:shell`` (the static-first wrapper) instead of the
bare ``main:app`` so ``/assets/*`` and other absolute-path SPA resources are
served without nginx. Launched as a child process by the supervisor, or run
directly for development:

    python -m desktop.serve
"""
from __future__ import annotations

import os


def main() -> None:
    import uvicorn

    host = os.environ.get("DESKTOP_MAIN_HOST", "127.0.0.1")
    port = int(os.environ.get("DESKTOP_MAIN_PORT", "8001"))
    # Pass the app as an import string is unnecessary (no reload); hand the
    # object directly so env_bootstrap (run at shell_asgi import) is applied
    # in-process.
    from desktop.shell_asgi import shell

    uvicorn.run(shell, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
