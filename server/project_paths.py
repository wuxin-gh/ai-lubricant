"""仓库根目录与关键资源路径的统一解析。

**任何需要定位仓库内文件的代码都必须走这里**，不要再写
``Path(__file__).parent / "xxx"`` —— 那种写法把模块位置和资源位置绑死，
模块一旦挪进子目录（例如业务模块收进 ``server/``）就会静默指向错误的目录：
读不到 ``.env``、找不到 ``sql/init.sql``、把缓存写到 ``server/data/`` 下面。

根目录从本文件位置推导（``server/project_paths.py`` → 上一级即仓库根），
不依赖当前工作目录，所以从任何 cwd、任何入口（``main.py`` /
``python -m tunnel_server`` / pytest / PyInstaller）启动都解析到同一处。

只用 ``pathlib``，不 import 任何项目模块，可以被最早期的启动代码安全引用。
"""
from __future__ import annotations

from pathlib import Path

# server/project_paths.py -> server/ -> 仓库根
REPO_ROOT = Path(__file__).resolve().parent.parent
SERVER_DIR = REPO_ROOT / "server"


def repo_root() -> Path:
    """仓库根目录（``main.py`` 所在目录）。"""
    return REPO_ROOT


def server_dir() -> Path:
    """业务模块目录 ``server/``。"""
    return SERVER_DIR


def path(*parts: str) -> Path:
    """拼一个仓库内路径：``project_paths.path("sql", "init.sql")``。"""
    return REPO_ROOT.joinpath(*parts)


# ── 具名资源：调用点用这些而不是自己拼字符串，改目录时只动这一处 ──────────

def env_file() -> Path:
    """启动配置 ``.env``（DB / Redis 连接与各类密钥）。"""
    return REPO_ROOT / ".env"


def data_dir() -> Path:
    """运行期数据目录 ``data/``（tokenizer 词表、agent 资源等）。"""
    return REPO_ROOT / "data"


def sql_dir() -> Path:
    """建库脚本目录 ``sql/``。"""
    return REPO_ROOT / "sql"


def static_dir() -> Path:
    """后端静态资源目录 ``static/``。"""
    return REPO_ROOT / "static"


def docs_dir() -> Path:
    """项目文档目录 ``docs/``。"""
    return REPO_ROOT / "docs"


def deleted_backups_dir() -> Path:
    """删除渠道/账号前的快照备份目录 ``deleted_backups/``。"""
    return REPO_ROOT / "deleted_backups"


def model_metadata_json() -> Path:
    """模型元数据种子文件（真相源在 DB，此文件仅供全新库首次导入）。"""
    return REPO_ROOT / "model_metadata.json"


def frontend_dist_dir() -> Path:
    """前端构建产物 ``user-frontend/dist``（submodule 内，由 pnpm build 产出）。"""
    return REPO_ROOT / "user-frontend" / "dist"


__all__ = [
    "REPO_ROOT",
    "SERVER_DIR",
    "repo_root",
    "server_dir",
    "path",
    "env_file",
    "data_dir",
    "sql_dir",
    "static_dir",
    "docs_dir",
    "deleted_backups_dir",
    "model_metadata_json",
    "frontend_dist_dir",
]
