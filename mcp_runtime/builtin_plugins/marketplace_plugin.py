"""Marketplace management MCP built-in.

The caller identity comes exclusively from the runtime token. Tool arguments never
carry a user id or role, and the GitHub write token remains inside marketplace
configuration.
"""
from __future__ import annotations

from typing import Any

import builtin_tool_store
from mcp_runtime.plugin_loader import PluginContext, PluginRegistrar, current_request_token
from monkeycode_compat.marketplace import config as mp_config
from monkeycode_compat.marketplace.routes import (
    catalog,
    delete_item,
    export,
    get_channel_import_job,
    get_item,
    import_current_channels,
    import_data,
    list_exportable_channels,
    start_channel_import_job,
    status,
    batch_delete_channel_templates,
    upsert,
)
from monkeycode_compat.marketplace.validator import validate_manifest
from monkeycode_compat.models import User


async def _admin() -> User:
    if not mp_config.settings.writable:
        raise PermissionError("marketplace management is not writable")
    token = current_request_token.get().strip()
    resolved = await builtin_tool_store.resolve_token(token)
    if not resolved or resolved.get("kind") != "identity":
        raise PermissionError("marketplace management requires an authenticated Agent identity")
    row = resolved.get("token") or {}
    if row.get("target_type") != "user":
        raise PermissionError("marketplace management requires a user identity")
    target_id = str(row.get("target_id") or "")
    if target_id == "__admin__":
        return type("AdminActor", (), {"role": "admin", "id": target_id})()
    user = await User.get_or_none(id=target_id)
    if user is None or user.role != "admin" or user.is_deleted or user.is_blocked:
        raise PermissionError("marketplace management requires an active platform administrator")
    return user


def _module(args: dict) -> str:
    module = str(args.get("module") or "").strip()
    if module not in mp_config.settings.modules:
        raise ValueError(f"unknown module: {module}")
    return module


async def _status(_args: dict, _ctx: PluginContext) -> dict:
    await _admin()
    return await status()


async def _list(args: dict, _ctx: PluginContext) -> dict:
    user = await _admin()
    return await catalog(_module(args), str(args.get("q") or "") or None, str(args.get("kind") or "") or None, user)


async def _get(args: dict, _ctx: PluginContext) -> dict:
    user = await _admin()
    return await get_item(_module(args), str(args.get("item") or ""), user)


async def _validate(args: dict, _ctx: PluginContext) -> dict:
    await _admin()
    module = _module(args)
    manifest = args.get("manifest")
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be an object")
    errors = validate_manifest(module, manifest)
    return {"valid": not errors, "module": module, "errors": errors}


async def _upsert(args: dict, _ctx: PluginContext) -> dict:
    user = await _admin()
    module = _module(args)
    manifest = args.get("manifest")
    if not isinstance(manifest, dict):
        raise ValueError("manifest must be an object")
    return await upsert({"module": module, "manifest": manifest}, user)


async def _import(args: dict, _ctx: PluginContext) -> dict:
    user = await _admin()
    body = args.get("data")
    if not isinstance(body, dict):
        raise ValueError("data must be an import object")
    mode = str(args.get("mode") or "merge")
    return await import_data({**body, "mode": mode}, user)


async def _hide(args: dict, _ctx: PluginContext) -> dict:
    user = await _admin()
    return await delete_item(_module(args), str(args.get("item") or ""), False, user)


async def _delete(args: dict, _ctx: PluginContext) -> dict:
    user = await _admin()
    return await delete_item(_module(args), str(args.get("item") or ""), True, user)


async def _export(args: dict, _ctx: PluginContext) -> dict:
    user = await _admin()
    module = str(args.get("module") or "").strip() or None
    if module is not None:
        module = _module({"module": module})
    return await export(module, user)


async def _list_exportable_channels(_args: dict, _ctx: PluginContext) -> dict:
    return await list_exportable_channels(await _admin())


async def _import_current_channels(args: dict, _ctx: PluginContext) -> dict:
    return await import_current_channels({"providers": args.get("providers") or []}, await _admin())


async def _start_channel_job(args: dict, _ctx: PluginContext) -> dict:
    return await start_channel_import_job(
        {"providers": args.get("providers") or [], "overwrite": bool(args.get("overwrite", False))},
        await _admin(),
    )


async def _get_channel_job(args: dict, _ctx: PluginContext) -> dict:
    return await get_channel_import_job(str(args.get("job_id") or ""), await _admin())


async def _batch_delete_channels(args: dict, _ctx: PluginContext) -> dict:
    return await batch_delete_channel_templates({"ids": args.get("ids") or []}, await _admin())


async def _unhide(args: dict, _ctx: PluginContext) -> dict:
    user = await _admin()
    module = _module(args)
    item = str(args.get("item") or "")
    manifest = await get_item(module, item, user)
    return await upsert({"module": module, "manifest": {**manifest, "status": "published"}}, user)


# ── 外部榜单草稿（marketplace_leaderboard_*）──────────────────────────────────
# 与 REST 管理端同源（marketplace_leaderboard_store），只是给市场管理场景的
# Agent 一个不用 HTTP 的调用面。每个工具都先过 _admin()：token 身份必须仍是
# 在职平台管理员。

def _item_id(args: dict) -> int:
    try:
        return int(args.get("item_id"))
    except (TypeError, ValueError):
        raise ValueError("item_id 必须是榜单条目的数字 id") from None


def _item_ids(args: dict) -> list[int]:
    raw = args.get("item_ids")
    if not isinstance(raw, list) or not raw:
        raise ValueError("item_ids 必须是非空的条目 id 数组")
    try:
        return [int(v) for v in raw]
    except (TypeError, ValueError):
        raise ValueError("item_ids 必须全是数字 id") from None


async def _leaderboard_list(args: dict, _ctx: PluginContext) -> dict:
    await _admin()
    import marketplace_leaderboard_store as store

    installable = args.get("installable")
    rows = await store.list_items(
        board=str(args.get("board") or "") or None,
        status=str(args.get("status") or "") or None,
        target_module=str(args.get("target_module") or "") or None,
        installable=bool(installable) if isinstance(installable, bool) else None,
        launch_status=str(args.get("launch_status") or "") or None,
        query=str(args.get("q") or ""),
        limit=int(args.get("limit") or 50),
        offset=int(args.get("offset") or 0),
    )
    return {"items": rows, "count": len(rows)}


async def _leaderboard_get(args: dict, _ctx: PluginContext) -> dict:
    await _admin()
    import marketplace_leaderboard_store as store

    row = await store.get_item(_item_id(args))
    if row is None:
        raise ValueError("榜单条目不存在")
    return row


async def _leaderboard_update(args: dict, _ctx: PluginContext) -> dict:
    await _admin()
    import marketplace_leaderboard_store as store

    patch = args.get("patch")
    if not isinstance(patch, dict) or not patch:
        raise ValueError("patch 必须是非空对象")
    from monkeycode_compat.marketplace.source_config import get_source_config_async
    source = await get_source_config_async()
    row = await store.update_curation(
        _item_id(args), patch,
        require_verified=bool(source.get("leaderboard_require_verified")),
    )
    if row is None:
        raise ValueError("榜单条目不存在")
    return row


async def _leaderboard_publish(args: dict, _ctx: PluginContext) -> dict:
    user = await _admin()
    import marketplace_leaderboard_store as store
    from monkeycode_compat.marketplace.source_config import get_source_config_async

    source = await get_source_config_async()
    return await store.publish_items(
        _item_ids(args),
        operator=str(getattr(user, "id", "") or "admin"),
        require_verified=bool(source.get("leaderboard_require_verified")),
    )


async def _leaderboard_unpublish(args: dict, _ctx: PluginContext) -> dict:
    await _admin()
    import marketplace_leaderboard_store as store

    unpublished = await store.unpublish_items(_item_ids(args))
    return {"unpublished": unpublished, "count": len(unpublished)}


async def _leaderboard_set_sort(args: dict, _ctx: PluginContext) -> dict:
    await _admin()
    import marketplace_leaderboard_store as store

    sort_order = args.get("sort_order")
    if sort_order is not None and str(sort_order).strip() != "":
        try:
            sort_order = int(str(sort_order).strip())
        except (TypeError, ValueError):
            raise ValueError("排序需为非负整数；null/空 = 取消固定排最后") from None
        if sort_order < 0:
            raise ValueError("排序需为非负整数；null/空 = 取消固定排最后")
    row = await store.update_curation(_item_id(args), {"sort_order": sort_order})
    if row is None:
        raise ValueError("榜单条目不存在")
    return row


_SYNC_FIELD_VALUES = (
    "name", "display_name", "description", "publisher", "version",
    "categories", "tags", "modules", "install_spec", "launch_spec",
)


async def _leaderboard_sync_field(args: dict, _ctx: PluginContext) -> dict:
    """把一个资源字段拉回上游值（external_data 派生），立即落库。

    与编辑弹框的逐项「同步」按钮同口径：分类按 board/上游分类重新推导，
    子分类=use_cases、标签=topics、名称/显示名=仓库短名、发布者=owner、
    版本=上游更新日期；install_spec/launch_spec 用确定性探针
    （``external_data.probe`` → ``derive_specs_from_probe``）重派生。
    改完回读整行，方便 Agent 复述新值。
    """
    await _admin()
    import marketplace_leaderboard_store as store
    from monkeycode_compat.marketplace import leaderboard_probe

    field = str(args.get("field") or "").strip()
    if field not in _SYNC_FIELD_VALUES:
        raise ValueError(f"field 只支持：{', '.join(_SYNC_FIELD_VALUES)}")
    row = await store.get_item(_item_id(args))
    if row is None:
        raise ValueError("榜单条目不存在")
    external = row.get("external_data") or {}
    if not isinstance(external, dict):
        external = {}
    full_name = str(external.get("repo_full_name") or row.get("repo_full_name") or "")
    short = full_name.split("/")[-1] if full_name else ""
    owner = full_name.split("/")[0] if "/" in full_name else ""
    if field == "name":
        patch: dict = {"name": short}
    elif field == "display_name":
        patch = {"display_name": short or row.get("display_name") or ""}
    elif field == "description":
        patch = {"description": str(external.get("description") or "")}
    elif field == "publisher":
        patch = {"publisher": owner}
    elif field == "version":
        patch = {"version": str(external.get("version") or "latest")}
    elif field == "categories":
        patch = {"categories": [str(c) for c in (external.get("use_cases") or []) if str(c).strip()]}
    elif field == "tags":
        patch = {"tags": [str(t) for t in (external.get("topics") or []) if str(t).strip()]}
    elif field in ("install_spec", "launch_spec"):
        # 确定性重派生：探针数据随同步保鲜（external_data.probe），这里只做纯派生+落库。
        probe = external.get("probe") if isinstance(external.get("probe"), dict) else {}
        if not probe or probe.get("error"):
            raise ValueError("无探针数据或上次探针失败——请先重新同步（或手动添加后重试）")
        modules = store.derive_target_modules(
            str(external.get("board") or row.get("board") or ""),
            str(external.get("upstream_category") or row.get("upstream_category") or ""),
        )
        derived = leaderboard_probe.derive_specs_from_probe(probe, modules, full_name)
        if field == "install_spec":
            patch = {"install_spec": derived["install_spec"]}
        else:
            if not derived["launch_spec"]:
                raise ValueError("探针没有 launch_spec 证据（仓库无 .mcp.json / README 启动命令），请手填或让 agent 识别")
            # update_curation 的 launch_spec 分支会把 launch_spec_status 置 filled——
            # 管理员显式拉上游=视为已核对；verified 是 verify 的显式动作，不在这翻。
            patch = {"launch_spec": derived["launch_spec"]}
    else:  # modules
        modules = store.derive_target_modules(
            str(external.get("board") or row.get("board") or ""),
            str(external.get("upstream_category") or row.get("upstream_category") or ""),
        )
        patch = {"target_modules": modules, "installable": bool(modules)}
    return await store.update_curation(int(row["id"]), patch)


async def _leaderboard_add_github(args: dict, _ctx: PluginContext) -> dict:
    """手动添加一个 GitHub 仓库为榜单草稿（source=manual）。

    与 REST ``POST /admin/leaderboard/items`` 同链路：find_by_repo 去重 →
    fetch_repo_metadata 拉元数据 → manual_item 预分类 → create_manual_item 落库。
    已存在时返回既有条目并标 created=false，不报错。
    """
    await _admin()
    from monkeycode_compat.marketplace import leaderboard_sync
    import marketplace_leaderboard_store as store

    raw = str(args.get("repo") or "").strip().removeprefix("https://github.com/").strip("/")
    if "/" not in raw:
        raise ValueError("请提供 owner/repo（或仓库完整地址）")
    full_name = "/".join(part for part in raw.split("/") if part)
    if len(full_name.split("/")) != 2:
        raise ValueError("仓库标识形如 owner/repo")

    existing = await store.find_by_repo(full_name)
    if existing is not None:
        return {"item": existing, "created": False, "detail": "该仓库已在候选池中"}

    try:
        meta = await leaderboard_sync.fetch_repo_metadata(full_name)
    except Exception as exc:  # noqa: BLE001 — 出网失败要给人话
        raise ValueError(f"读取 GitHub 仓库信息失败：{exc}") from exc
    item = leaderboard_sync.manual_item(full_name, meta)
    if item is None:
        raise ValueError("无法解析仓库标识")
    # 手动添加也跑确定性探针（与同步链路同一份 attach_probe）：仓库有 .mcp.json/SKILL.md
    # 等结构化清单时 install_spec/launch_spec 直接派生。探针失败不挡创建。
    from monkeycode_compat.marketplace import leaderboard_probe
    item = await leaderboard_probe.attach_probe(item)
    row, created = await store.create_manual_item(item)
    if row is None:
        raise ValueError("创建失败")
    return {"item": row, "created": created, "detail": ""}


async def _leaderboard_verify(args: dict, _ctx: PluginContext) -> dict:
    """重新验证一条条目的 launch_spec（可选 install_spec），写回 verified/failed。"""
    await _admin()
    from monkeycode_compat.marketplace import leaderboard_verify

    return await leaderboard_verify.verify_item(
        _item_id(args), include_install=bool(args.get("include_install", False)),
    )


_OBJECT = {"type": "object", "additionalProperties": True}
_MODULE = {"type": "string", "enum": ["mcp", "plugins", "skills", "prompts", "channels", "node-versions"]}
_ITEM_ID = {"type": "integer"}
_ITEM_IDS = {"type": "array", "items": {"type": "integer"}, "minItems": 1}
_LEADERBOARD_MODULE = {"type": "string", "enum": ["mcp", "skill", "plugin", "prompt"]}
_SYNC_FIELD = {"type": "string", "enum": list(_SYNC_FIELD_VALUES)}


def register(reg: PluginRegistrar) -> None:
    reg.tool(name="marketplace_get_status", description="Read marketplace availability and configured modules.", params={"type": "object", "properties": {}})(_status)
    reg.tool(name="marketplace_list_items", description="List and search marketplace resources.", params={"type": "object", "properties": {"module": _MODULE, "q": {"type": "string"}, "kind": {"type": "string"}}, "required": ["module"]})(_list)
    reg.tool(name="marketplace_get_item", description="Read one complete marketplace manifest.", params={"type": "object", "properties": {"module": _MODULE, "item": {"type": "string"}}, "required": ["module", "item"]})(_get)
    reg.tool(name="marketplace_validate_manifest", description="Validate a proposed MCP, plugin, or Skill marketplace manifest without writing it.", params={"type": "object", "properties": {"module": _MODULE, "manifest": _OBJECT}, "required": ["module", "manifest"]})(_validate)
    reg.tool(name="marketplace_upsert_item", description="Create or update one validated marketplace manifest in GitHub.", params={"type": "object", "properties": {"module": _MODULE, "manifest": _OBJECT}, "required": ["module", "manifest"]})(_upsert)
    reg.tool(name="marketplace_import_items", description="Import a marketplace export package using merge or replace mode.", params={"type": "object", "properties": {"data": _OBJECT, "mode": {"type": "string", "enum": ["merge", "replace"]}}, "required": ["data"]})(_import)
    reg.tool(name="marketplace_hide_item", description="Hide a marketplace item without deleting its manifest.", params={"type": "object", "properties": {"module": _MODULE, "item": {"type": "string"}}, "required": ["module", "item"]})(_hide)
    reg.tool(name="marketplace_unhide_item", description="Restore one hidden marketplace item to published status.", params={"type": "object", "properties": {"module": _MODULE, "item": {"type": "string"}}, "required": ["module", "item"]})(_unhide)
    reg.tool(name="marketplace_delete_item", description="Permanently delete a marketplace item and its manifest.", params={"type": "object", "properties": {"module": _MODULE, "item": {"type": "string"}}, "required": ["module", "item"]})(_delete)
    reg.tool(name="marketplace_export", description="Export marketplace indexes and manifests for one module or all modules.", params={"type": "object", "properties": {"module": _MODULE}})(_export)
    reg.tool(name="marketplace_list_exportable_channels", description="List current platform channels that may be published as templates.", params={"type": "object", "properties": {}})(_list_exportable_channels)
    reg.tool(name="marketplace_import_current_channels", description="Publish selected current platform channels as marketplace templates.", params={"type": "object", "properties": {"providers": {"type": "array", "items": {"type": "string"}}}, "required": ["providers"]})(_import_current_channels)
    reg.tool(name="marketplace_start_channel_import", description="Start a background import of selected channel templates.", params={"type": "object", "properties": {"providers": {"type": "array", "items": {"type": "string"}}, "overwrite": {"type": "boolean"}}, "required": ["providers"]})(_start_channel_job)
    reg.tool(name="marketplace_get_channel_import", description="Read one channel import job state.", params={"type": "object", "properties": {"job_id": {"type": "string"}}, "required": ["job_id"]})(_get_channel_job)
    reg.tool(name="marketplace_batch_delete_channels", description="Permanently delete selected channel templates in one batch.", params={"type": "object", "properties": {"ids": {"type": "array", "items": {"type": "string"}}}, "required": ["ids"]})(_batch_delete_channels)
    # ── 外部榜单草稿（marketplace_leaderboard_*）：marketplace-status 是场景保留 MCP，
    # 只在市场管理场景的对话里临时获权，普通 Agent MCP 选择器不暴露（见 agent/api.list_available_mcp）。
    reg.tool(name="marketplace_leaderboard_list", description="查询外部榜单候选池（草稿+已发布），支持按榜单/状态/分类/可安装/启动方式状态过滤与搜索。", params={"type": "object", "properties": {"board": {"type": "string"}, "status": {"type": "string", "enum": ["draft", "published"]}, "target_module": _LEADERBOARD_MODULE, "installable": {"type": "boolean"}, "launch_status": {"type": "string", "enum": ["unfilled", "pending", "filled", "failed", "verified"]}, "q": {"type": "string"}, "limit": {"type": "integer"}, "offset": {"type": "integer"}}})(_leaderboard_list)
    reg.tool(name="marketplace_leaderboard_get", description="读取一条榜单条目的完整字段（含 external_data 上游快照与安装配置）。", params={"type": "object", "properties": {"item_id": _ITEM_ID}, "required": ["item_id"]})(_leaderboard_get)
    reg.tool(name="marketplace_leaderboard_update", description="修改榜单草稿的资源字段（名称/显示名/摘要/发布者/版本/子分类/标签/分类多选）与安装配置、状态；发布=状态选 published。", params={"type": "object", "properties": {"item_id": _ITEM_ID, "patch": _OBJECT}, "required": ["item_id", "patch"]})(_leaderboard_update)
    reg.tool(name="marketplace_leaderboard_publish", description="批量发布榜单条目（对用户可见）。空分类会按上游榜单自动推导，推不出的条目报错返回原因。", params={"type": "object", "properties": {"item_ids": _ITEM_IDS}, "required": ["item_ids"]})(_leaderboard_publish)
    reg.tool(name="marketplace_leaderboard_unpublish", description="批量撤回榜单条目（回草稿，用户侧立即不可见）。", params={"type": "object", "properties": {"item_ids": _ITEM_IDS}, "required": ["item_ids"]})(_leaderboard_unpublish)
    reg.tool(name="marketplace_leaderboard_set_sort", description="设置榜单条目的排序（资源中心索引）；留空=null 取消固定，排最后按热度。", params={"type": "object", "properties": {"item_id": _ITEM_ID, "sort_order": {"type": ["integer", "null"], "minimum": 0}}, "required": ["item_id"]})(_leaderboard_set_sort)
    reg.tool(name="marketplace_leaderboard_sync_field", description="把某个资源字段同步回上游值（从 external_data 派生）；分类按 board/上游分类重新推导。", params={"type": "object", "properties": {"item_id": _ITEM_ID, "field": _SYNC_FIELD}, "required": ["item_id", "field"]})(_leaderboard_sync_field)
    reg.tool(name="marketplace_leaderboard_verify", description="重新验证一条榜单条目的启动方式（remote 握手 / stdio registry HEAD）与安装资源存在性，写回 verified/failed。", params={"type": "object", "properties": {"item_id": _ITEM_ID, "include_install": {"type": "boolean"}}, "required": ["item_id"]})(_leaderboard_verify)
    reg.tool(name="marketplace_leaderboard_add_github", description="手动添加一个 GitHub 仓库为榜单草稿：读取仓库元数据、按 topics 预分类；已存在时返回既有条目。", params={"type": "object", "properties": {"repo": {"type": "string"}}, "required": ["repo"]})(_leaderboard_add_github)
