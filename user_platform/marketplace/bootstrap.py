"""store 存量导入：GitHub 仓库 → marketplace_items 的一次性迁移与手动 resync。

改造前 GitHub 仓库是唯一真相源，已有的部署升级后 store 是空的——bootstrap_once
在启动时把仓库现有内容全量导入 store（不 enqueue：仓库已是该状态），此后编辑走
store、publisher 异步镜像回仓库。

resync_from_repo 是 bootstrap 的手动版：直改 GitHub（如 script/publish_channel_templates.py
脚本写入）不再是权威，要采纳仓库内容时由管理员显式触发，merge 语义——仓库有的
条目覆盖 store 行，仓库没有的保留（store 里新编辑、还没发布出去的行不能被抹掉）。
"""
from __future__ import annotations

from typing import Any

from loguru import logger

from . import config as mp_config
from .github import MarketplaceGitHub
from .validator import index_path, item_path, safe_item_id

BOOTSTRAP_FLAG_KEY = "marketplace_store_bootstrapped"


def _client() -> MarketplaceGitHub:
    return MarketplaceGitHub(mp_config.settings)


async def _fetch_repo_items(client: MarketplaceGitHub) -> list[tuple[str, dict]]:
    """读仓库全部模块的 index + 每条 manifest。读失败/缺文件按空处理。"""
    from .validator import empty_index

    out: list[tuple[str, dict]] = []
    modules = [*mp_config.settings.modules, "node-versions", "mobile-versions"]
    for module in dict.fromkeys(modules):
        try:
            got = await client.read_json_or_none(
                index_path(module, mp_config.settings.index_name)
            )
        except Exception:  # noqa: BLE001 - 单模块失败不拖垮其余
            continue
        if got is None or not isinstance(got[0], dict):
            continue
        index = got[0]
        for row in (index.get("items") or []):
            if not isinstance(row, dict):
                continue
            raw_id = str(row.get("id") or "")
            safe = safe_item_id(raw_id.replace("/", "."))
            if not safe:
                continue
            path = row.get("item_path") or item_path(module, safe)
            try:
                manifest, _sha = await client.read_json(path)
            except Exception:  # noqa: BLE001
                continue
            if isinstance(manifest, dict):
                out.append((module, manifest))
    return out


async def bootstrap_once() -> dict[str, Any]:
    """启动时的一次性导入：writable 且 store 为空才跑，幂等（app_config flag 双保险）。"""
    from db import PostgresClient
    import marketplace_store as store

    if not mp_config.settings.writable:
        return {"ok": False, "skipped": "not writable"}
    try:
        flag = await PostgresClient.get_config(BOOTSTRAP_FLAG_KEY)
    except Exception:  # noqa: BLE001 - PG 不可用：只读路径继续走旧 GitHub 读取
        return {"ok": False, "error": "pg unavailable"}
    if flag:
        return {"ok": True, "skipped": "already bootstrapped"}
    if await store.is_populated():
        # 有数据但没 flag（resync 后从未置位等场景）：直接置 flag，不重复导入。
        await PostgresClient.set_config(BOOTSTRAP_FLAG_KEY, {"done": True})
        return {"ok": True, "skipped": "store already populated"}
    client = _client()
    items = await _fetch_repo_items(client)
    if items:
        # DO NOTHING：导入窗口内管理员已保存的同 id 行（store 是真相源）绝不能被
        # 仓库旧值覆盖；只把 store 还没有的条目补进来。
        count = await store.bootstrap_items_no_overwrite(items)
    else:
        count = 0
    await PostgresClient.set_config(BOOTSTRAP_FLAG_KEY, {"done": True, "items": count})
    logger.info("[marketplace-bootstrap] imported {} item(s) from repo", count)
    return {"ok": True, "imported": count}


async def resync_from_repo() -> dict[str, Any]:
    """手动采纳仓库直改：merge 仓库内容进 store，不删 store 独有条目。

    采纳后可选触发一次 publish_once 把 store 状态镜像回去（caller 决定）。
    """
    import marketplace_store as store

    if not mp_config.settings.writable:
        return {"ok": False, "error": "marketplace admin not configured"}
    client = _client()
    items = await _fetch_repo_items(client)
    count = await store.bootstrap_items(items)
    logger.info("[marketplace-resync] merged {} item(s) from repo", count)
    return {"ok": True, "merged": count}
