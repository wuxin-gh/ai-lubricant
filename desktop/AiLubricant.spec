# PyInstaller spec for the Ai Lubricant desktop build (Windows, onedir).
#
# Build:
#     pyinstaller desktop/AiLubricant.spec --noconfirm
#
# Produces dist/AiLubricant/AiLubricant.exe plus _internal/ holding the Python
# runtime, all dependencies, the source tree and the bundled data files. The
# target machine needs no Python — but it DOES need external PostgreSQL and
# Redis (>= 6.0); the first-run wizard collects those connection details.
#
# Key hazards handled here (see desktop/ docstrings for the why):
#   * Tortoise ORM registers models by STRING module path, which PyInstaller's
#     static analysis cannot follow -> every model module is a hiddenimport.
#   * Hypercorn picks its protocol handler by ALPN at runtime -> h2/h11 stacks
#     are hiddenimports.
#   * node_server/templates/*.tmpl are read with open() at request time -> they
#     must ship as datas, not as code.
#   * protobuf hard-fails on a gencode/runtime version mismatch
#     (agentcompose_v2_pb2.py calls ValidateProtobufRuntimeVersion), so the
#     build must pin protobuf exactly — currently 7.35.1. Keep
#     requirements-desktop.txt in sync.

from pathlib import Path

from PyInstaller.utils.hooks import collect_submodules

# spec 现位于 desktop/，SPECPATH 即 desktop/ 目录；仓库根要上溯一级。
PROJECT_ROOT = Path(SPECPATH).parent


def _tree(rel_path: str) -> tuple[str, str]:
    """(source, dest) datas entry preserving the relative layout."""
    return (str(PROJECT_ROOT / rel_path), rel_path.replace("/", "\\"))


datas = [
    # Frontend build output — served by desktop/shell_asgi.py and main.py.
    # user-frontend is a submodule (the AGPL-licensed portal; see NOTICE there).
    _tree("user-frontend/dist"),
    # Backend static assets and docs surfaces mounted by main.py.
    _tree("static"),
    _tree("docs"),
    _tree("sql"),
    # Read with open() at request time by node_server/scripts.py:75.
    _tree("node_server/templates"),
    # Agent SOP markdown loaded at runtime.
    _tree("agent/sop"),
    # Vendored built-in MCP plugins.
    _tree("mcp_builtin"),
    # Startup configuration template. The former root-level JSON configs
    # (config.json / model_metadata.json / channel_remarks.json) are gone: main
    # config and model metadata now live in PostgreSQL (app_config / model_groups).
    # The live env.ini (real credentials) is intentionally NOT shipped; the
    # packaged app writes its .env into the per-user data dir at first run.
    (str(PROJECT_ROOT / "env.ini.example"), "."),
]

hiddenimports = [
    # ── Tortoise ORM: models registered by string path ────────────────────
    # user_platform/database.py:15-31 MODEL_MODULES
    "user_platform.models",
    "user_platform.models_task",
    "user_platform.models_project",
    "user_platform.models_git",
    "user_platform.models_skill",
    "user_platform.models_notify",
    "user_platform.models_team_admin",
    "user_platform.models_resources",
    "user_platform.models_webhook",
    "user_platform.models_webhook_event",
    "user_platform.models_review",
    "user_platform.models_tunnel",
    # node_server/database.py:17 — the node ledger app
    "node_server.store",
    # Tortoise selects its dialect dynamically
    "tortoise.backends.asyncpg",
    "tortoise.backends.asyncpg.client",
    "tortoise.backends.base_postgres",
    "tortoise.backends.base_postgres.client",
    "pypika_tortoise",
    # ── Hypercorn: protocol chosen by ALPN at runtime ─────────────────────
    "hypercorn.asyncio",
    "hypercorn.protocol",
    "hypercorn.protocol.h2",
    "hypercorn.protocol.h11",
    "h2",
    "h2.connection",
    "hpack",
    "hyperframe",
    "priority",
    "wsproto",
    # ── Providers (explicit re-exports; listed so a lazy path can't miss) ──
    "providers.base",
    "providers.custom",
    "providers.edgeone_ai",
    "providers.cloudflare",
    "providers.proxy_manager",
    # ── Desktop child entry points (launched via runpy) ───────────────────
    "desktop.serve",
    "desktop.shell_asgi",
    "desktop.config_wizard",
    "node_server.__main__",
    "tunnel_server.__main__",
    # ── MCP runtime builtin plugins ───────────────────────────────────────
    # mcp_runtime/registry.py:35,42 resolve adapter modules with
    # import_module(spec.adapter_module) — a string path PyInstaller cannot see.
    "mcp_runtime.builtin_plugins",
    "mcp_runtime.builtin_plugins.cdp_bridge_plugin",
    "mcp_runtime.builtin_plugins.mail_plugin",
    "mcp_runtime.builtin_plugins.marketplace_plugin",
    "mcp_runtime.builtin_plugins.issue_workflow_plugin",
    "mcp_runtime.builtin_plugins.review_result_plugin",
    # ── Misc runtime-resolved deps ────────────────────────────────────────
    "asyncpg.pgproto",
    "clickhouse_connect.driverc",
    "bcrypt",
    "json_repair",
]

# uvicorn resolves its loop/protocol implementations by name at runtime;
# coredis and tortoise have plenty of lazily imported submodules.
for pkg in ("uvicorn", "coredis", "tortoise", "hypercorn", "clickhouse_connect"):
    hiddenimports += collect_submodules(pkg)

# Trim obvious build-time-only weight. Nothing in the runtime path needs these.
excludes = [
    "tkinter",
    "matplotlib",
    "pytest",
    "IPython",
    "notebook",
]


a = Analysis(
    # 入口用绝对路径：spec 从仓库根以 `pyinstaller desktop/AiLubricant.spec` 调用时，
    # PyInstaller 会把相对路径拼在 SPECPATH（=desktop/）后面，得出 desktop\desktop\main_window.py。
    [str(PROJECT_ROOT / "desktop" / "main_window.py")],
    pathex=[str(PROJECT_ROOT), str(PROJECT_ROOT / "server")],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=excludes,
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="AiLubricant",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,  # windowed app; child logs go to %LOCALAPPDATA%\AiLubricant\logs
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=str(PROJECT_ROOT / "user-frontend" / "electron" / "icon.png"),
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name="AiLubricant",
)
