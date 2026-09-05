"""Load and persist the project's standard dotenv configuration."""
from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
from typing import Mapping

from dotenv import load_dotenv

from project_paths import env_file

# 本模块位于 server/，而 .env 在仓库根：必须走 project_paths 解析，
# 否则 Path(__file__).parent 会指向 server/.env（不存在）。
ENV_FILE = env_file()
_KEY_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?:.*)?$")


def load_project_env(path: Path = ENV_FILE) -> None:
    """Load ``.env`` without overriding variables supplied by the process."""
    load_dotenv(dotenv_path=path, override=False)


def update_env_vars(mapping: Mapping[str, str], path: Path = ENV_FILE) -> None:
    """Atomically update dotenv keys while preserving comments and ordering."""
    updates = {str(key): str(value) for key, value in mapping.items()}
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    seen: set[str] = set()
    output: list[str] = []
    for line in lines:
        match = _KEY_RE.match(line)
        if match and match.group(1) in updates:
            key = match.group(1)
            output.append(f"{key}={updates[key]}")
            seen.add(key)
        else:
            output.append(line)
    for key, value in updates.items():
        if key not in seen:
            if output and output[-1].strip():
                output.append("")
            output.append(f"{key}={value}")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write("\n".join(output) + "\n")
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise
    for key, value in updates.items():
        os.environ[key] = value
