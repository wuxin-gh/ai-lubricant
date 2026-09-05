"""Bootstrap environment for the desktop build.

MUST run before ``main`` / ``node_server`` / ``tunnel_server`` are imported,
because main.py calls ``load_project_env()`` at import time (main.py:14) and
node_server generates + writes back security keys on first start
(node_server/wiring.py:25-67).

What this does, without touching any existing source file:

1. Relocate the writable ``.env`` to ``%LOCALAPPDATA%\\AiLubricant\\.env`` (the
   frozen install directory is read-only / not UAC-friendly).
2. Seed ``os.environ`` from that ``.env`` (override=True) BEFORE main's own
   import-time ``load_project_env()`` runs. main's call is a no-op when its
   frozen-path default ``.env`` is absent, and uses ``override=False`` so it
   never clobbers what we already set.
3. Generate ``NODE_CONTROL_TOKEN``, ``NODE_CREDENTIAL_ENCRYPTION_KEY`` and
   ``AGENT_ATTACHMENT_SIGNING_KEY`` once, persist them to the user ``.env``.
   node_server / attachment_signing read them from env at first start and
   therefore NEVER trigger their own write-back path (wiring.py:59-67,
   attachment_signing.ensure_signing_key) — sidestepping the "frozen dir is
   unwritable → RuntimeError" trap entirely.
4. Apply desktop defaults (127.0.0.1 binds to avoid the Windows firewall
   prompt; feature flags on; bin dirs under user data).
"""
from __future__ import annotations

import os
import secrets
from pathlib import Path

from dotenv import load_dotenv

from desktop import paths


def _generate_token() -> str:
    """Mirror node_server/wiring.py:63 (secrets.token_urlsafe(32))."""
    return secrets.token_urlsafe(32)


def _generate_master_key() -> str:
    """Mirror node_server/wiring.py:54 (secrets.token_hex(32))."""
    return secrets.token_hex(32)


def _persist_to_env(updates: dict[str, str]) -> None:
    """Append missing keys to the user ``.env`` (does not overwrite existing)."""
    env_file = paths.env_file_path()
    env_file.parent.mkdir(parents=True, exist_ok=True)
    existing: dict[str, str] = {}
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            existing[k.strip()] = v

    # Collect first, write second: an empty result must not append a bare
    # section header (doing so on every launch grows the file without end).
    pending: list[str] = []
    for key, value in updates.items():
        if existing.get(key):
            continue  # respect user config
        pending.append(f"{key}={value}")
        os.environ[key] = value
    if not pending:
        return

    lines: list[str] = []
    if env_file.exists() and env_file.stat().st_size:
        lines.append("")
        lines.append("# desktop defaults")
    lines.extend(pending)
    with env_file.open("a", encoding="utf-8", newline="\n") as fh:
        fh.write("\n".join(lines) + "\n")


def _set_default(key: str, value: str) -> None:
    """Set an env var only if unset; also persist to .env via batch call."""
    if not os.environ.get(key):
        os.environ[key] = value


def apply_env() -> Path:
    """Load user .env into os.environ and apply desktop defaults.

    Returns the path to the user .env so callers (wizard, supervisor) can pass
    it explicitly to ``dotenv_loader`` helpers.
    """
    env_file = paths.env_file_path()
    if env_file.exists():
        load_dotenv(dotenv_path=env_file, override=True)

    # Security keys: generate once, persist, inject. node_server reads these at
    # first start and skips its own generation/write-back.
    keys: dict[str, str] = {}
    if not os.environ.get("NODE_CONTROL_TOKEN"):
        keys["NODE_CONTROL_TOKEN"] = _generate_token()
    if not os.environ.get("NODE_CREDENTIAL_ENCRYPTION_KEY"):
        keys["NODE_CREDENTIAL_ENCRYPTION_KEY"] = _generate_master_key()
    if not os.environ.get("AGENT_ATTACHMENT_SIGNING_KEY"):
        # Mirror attachment_signing.ensure_signing_key (secrets.token_urlsafe(48)).
        keys["AGENT_ATTACHMENT_SIGNING_KEY"] = secrets.token_urlsafe(48)
    if keys:
        _persist_to_env(keys)
        for k, v in keys.items():
            os.environ[k] = v

    # Desktop defaults. Bound to 127.0.0.1 to avoid the Windows firewall prompt
    # on a desktop product; users wanting LAN/remote reach edit .env manually.
    defaults = {
        # 旧部署可能只设了 MONKEYCODE_COMPAT_ENABLED；显式 false 时不能被下面的
        # 默认 true 覆盖，否则改名后兼容层会被意外打开。
        "AI_LUBRICANT_COMPAT_ENABLED": os.environ.get("MONKEYCODE_COMPAT_ENABLED") or "true",
        "AGENT_COMPOSE_NODE_SERVER_ENABLED": "true",
        "TUNNEL_RUNTIME_ENABLED": "true",
        "AGENT_COMPOSE_BASE_URL": "http://127.0.0.1:8003",
        "NODE_CONTROL_HOST": "127.0.0.1",
        "NODE_CONTROL_PORT": "8003",
        "TUNNEL_RUNTIME_HOST": "127.0.0.1",
        "TUNNEL_RUNTIME_PORT": "8004",
        "MC_TUNNEL_BIN_DIR": str(paths.tunnel_bin_dir()),
        "AGENT_COMPOSE_NODE_BIN_DIR": str(paths.node_bin_dir()),
    }
    _persist_to_env(defaults)  # persists missing ones + sets os.environ
    for key, value in defaults.items():
        _set_default(key, value)

    return env_file
