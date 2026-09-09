"""外部榜单候选池存储——**委托层**（统一资源池切换）。

历史实现直接读写 ``marketplace_leaderboard_items`` 旧表。统一资源池重构后，榜单
条目改落 ``resources`` 表（source_type=leaderboard_sync|manual），真正实现在
:mod:`leaderboard_resource_store`。本模块保留原有 import 路径与全部函数名/签名，
逐一委托到新实现——所有调用方（leaderboard_sync / publisher / verify / routes /
converters / marketplace_plugin）零改动。旧表不再写入，清理见
``script/cleanup_old_resource_data.sql``。

分类语义变化：旧 ``target_modules`` 多值 → 新 ``resource_type`` 单选（探针在场用
``derive_primary_type``，否则 board 推导）。``derive_target_modules`` 相应改为返回
至多一项。``TARGET_MODULES`` 常量扩入 ``skills`` 集合类型。
"""
from __future__ import annotations

from leaderboard_resource_store import (  # noqa: F401  (re-export 兼容旧 import 路径)
    apply_overrides,
    count_items,
    create_manual_item,
    delete_items,
    derive_target_module,
    derive_target_modules,
    enqueue_publish_jobs,
    find_by_repo,
    get_item,
    list_items,
    list_pending_launch_spec,
    list_published_for_consumer,
    load_probe_reuse_hints,
    merge_external_data_probe,
    publish_items,
    purge_all,
    set_launch_spec,
    unpublish_items,
    update_curation,
    upsert_item,
)

# 我们市场的安装形态（单选主类型）。skills=技能集合（≥2 个不同目录的 skill 条目）。
# 榜单 board（上游主题）与之不等价——见 leaderboard_resource_store.derive_target_modules。
TARGET_MODULES = frozenset({"mcp", "skill", "skills", "prompt", "plugin"})

BOARD_TO_MODULE = {
    "mcp": "mcp",
    "skills": "skill",
    "prompts": "prompt",
}


def row_to_dict(row) -> dict:
    """兼容保留：旧调用方偶有直接把 asyncpg 行喂进来的用法。

    统一资源池后行是 ``resources`` 表结构，交给新实现的映射函数收口成榜单 item。
    """
    from leaderboard_resource_store import _decode_row, _resource_to_item

    return _resource_to_item(_decode_row(row))
