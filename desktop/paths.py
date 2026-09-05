"""Runtime path resolution for the desktop build.

Works in both source mode (running from the repo) and PyInstaller frozen mode
(``sys.frozen`` set, resources unpacked next to the exe under ``_internal``).

User-writable data (``.env``, logs, downloaded node/tunnel binaries) never lives
inside the install directory — it goes to ``%LOCALAPPDATA%\\AiLubricant`` so a
Program Files install stays read-only and no UAC prompt is triggered.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path


def is_frozen() -> bool:
    """True when running inside a PyInstaller bundle."""
    return bool(getattr(sys, "frozen", False))


def resource_dir() -> Path:
    """Directory holding bundled resources (source tree root, or _internal).

    In frozen mode PyInstaller sets ``sys._MEIPASS`` (onefile) or lays files
    beside the exe (onedir). ``_MEIPASS`` is defined for both, pointing at the
    directory that ``--add-data`` files were extracted/placed into.
    """
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        return Path(meipass)
    # Source mode: repo root is the parent of the desktop/ package.
    return Path(__file__).resolve().parent.parent


def user_data_dir() -> Path:
    """Per-user writable data directory, created on first access."""
    base = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA")
    if base:
        root = Path(base) / "AiLubricant"
    else:
        # Non-Windows / unusual env: fall back to home.
        root = Path.home() / ".ailubricant"
    root.mkdir(parents=True, exist_ok=True)
    return root


def env_file_path() -> Path:
    """Path to the user-writable ``.env`` consumed by all processes.

    ``DESKTOP_ENV_FILE`` overrides it — handy for development, where you want
    the desktop shell to reuse the repo's existing ``.env`` instead of the
    per-user one.
    """
    override = os.environ.get("DESKTOP_ENV_FILE")
    if override:
        return Path(override)
    return user_data_dir() / ".env"


def logs_dir() -> Path:
    d = user_data_dir() / "logs"
    d.mkdir(parents=True, exist_ok=True)
    return d


def node_bin_dir() -> Path:
    d = user_data_dir() / "node-bin"
    d.mkdir(parents=True, exist_ok=True)
    return d


def tunnel_bin_dir() -> Path:
    d = user_data_dir() / "mc-tunnel-bins"
    d.mkdir(parents=True, exist_ok=True)
    return d


def dist_dir() -> Path:
    """Frontend build output (user-frontend/dist)."""
    return resource_dir() / "user-frontend" / "dist"
