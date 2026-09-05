"""launch_spec / install_spec 验证（解决"能看不可用"的最后一公里）。

探测（``leaderboard_probe``）只认仓库形状——``.mcp.json`` 存在就是确定性证据，
但不代表那个 MCP 真能连。验证这一步补上"真的能用吗"：

- remote MCP：复用 ``mcp_runtime.sse_client.SSEClient`` 做 JSON-RPC ``initialize``
  握手（``list_tools()`` 即完整握手，``api_my_services._sync_my_service_tools``
  是同款 probe-then-mark 先例）。transport=streamable-http 也走 SSEClient——
  现行用户侧服务就是用 SSEClient 连 streamable-http 的（``api_my_services``
  创建时 transport 标 streamable-http 但 kind=sse）。
- stdio MCP：npx/uvx 的包做 registry HEAD（npm/pypi 存在性，不 spawn 进程）。
- skill / plugin：Contents API / download_url HEAD 存在性。

验证结果写回 ``launch_spec_status``（新增 ``verified`` 值，schema 无 CHECK 约束
直接可用）与 ``external_data.probe.install_verify``（informational 徽标）。发布门禁
（软+开关，默认 OFF）在 ``marketplace_leaderboard_store`` 里查这两个状态。

**永不抛**——所有失败路径都写成 ``failed`` + error 文案，由调用方落库。
"""
from __future__ import annotations

import asyncio
from typing import Any

_VERIFY_TIMEOUT_SECONDS = 30


def _split_pkg_from_npx(command: str, args: list) -> str:
    """从 npx 命令里取出包名：``npx -y @scope/pkg`` → ``@scope/pkg``。"""
    cmd = (command or "").strip()
    if cmd not in ("npx", "npx-cli"):
        return ""
    parts = [str(a) for a in (args or []) if str(a).strip()]
    for arg in parts:
        if arg.startswith("-"):
            continue
        return arg
    return ""


async def _head_ok(url: str, *, headers: dict | None = None) -> tuple[bool, str]:
    """经 proxy_manager 做 HEAD/GET，返回 (ok, error)。200/302 → ok。"""
    from providers.proxy_manager import get_proxy_manager
    import aiohttp

    manager = get_proxy_manager()
    try:
        resp = await manager.request(
            url=url, method="GET", headers=headers or {},
            timeout=aiohttp.ClientTimeout(total=_VERIFY_TIMEOUT_SECONDS),
            proxy_config_id=None,
        )
    except Exception as exc:  # noqa: BLE001
        return False, f"请求失败: {exc}"
    if resp.status in (200, 301, 302, 204):
        return True, ""
    return False, f"HTTP {resp.status}"


async def verify_launch_spec(item: dict) -> dict:
    """验证 item 的 launch_spec。返回 ``{"ok": bool, "status": "verified"|"failed"|"", "error": str}``。

    kind=remote（含 transport=streamable-http）：SSEClient.list_tools() 握手。
    kind=stdio：npx→npm registry HEAD；uvx→pypi HEAD；docker→不支持远程校验。
    kind=none/空：无验证对象，status=""。
    """
    spec = item.get("launch_spec") if isinstance(item, dict) else None
    if not isinstance(spec, dict):
        return {"ok": True, "status": "", "error": ""}
    kind = str(spec.get("kind") or "none").strip().lower()
    if kind in ("", "none"):
        return {"ok": True, "status": "", "error": ""}

    try:
        if kind == "remote":
            return await asyncio.wait_for(
                _verify_remote(spec), timeout=_VERIFY_TIMEOUT_SECONDS,
            )
        if kind == "stdio":
            return await asyncio.wait_for(
                _verify_stdio(spec), timeout=_VERIFY_TIMEOUT_SECONDS,
            )
        return {"ok": False, "status": "failed", "error": f"不支持的 launch_spec kind: {kind}"}
    except asyncio.TimeoutError:
        return {"ok": False, "status": "failed", "error": "验证超时"}
    except Exception as exc:  # noqa: BLE001 — 验证绝不抛给调用方
        return {"ok": False, "status": "failed", "error": str(exc)[:200]}


async def _verify_remote(spec: dict) -> dict:
    url = str(spec.get("url") or "").strip()
    if not url or not url.startswith("https://"):
        return {"ok": False, "status": "failed", "error": "remote MCP 需要 HTTPS 地址"}
    from mcp_runtime.sse_client import SSEClient, SSEClientError

    try:
        client = SSEClient(url, headers={})
        await client.list_tools()
    except SSEClientError as exc:
        return {"ok": False, "status": "failed", "error": f"握手失败: {exc}"}
    return {"ok": True, "status": "verified", "error": ""}


async def _verify_stdio(spec: dict) -> dict:
    command = str(spec.get("command") or "").strip()
    args = spec.get("args") if isinstance(spec.get("args"), list) else []
    if command in ("npx", "npx-cli"):
        pkg = _split_pkg_from_npx(command, args)
        if not pkg:
            return {"ok": False, "status": "failed", "error": "npx 启动缺少包名参数"}
        ok, err = await _head_ok(f"https://registry.npmjs.org/{pkg}")
        if ok:
            return {"ok": True, "status": "verified", "error": ""}
        return {"ok": False, "status": "failed", "error": f"npm 包不存在或不可达: {pkg}（{err}）"}
    if command in ("uvx", "uv"):
        parts = [str(a) for a in args if str(a).strip()]
        pkg = next((p for p in parts if not p.startswith("-")), "")
        if not pkg:
            return {"ok": False, "status": "failed", "error": "uvx 启动缺少包名参数"}
        ok, err = await _head_ok(f"https://pypi.org/simple/{pkg}/")
        if ok:
            return {"ok": True, "status": "verified", "error": ""}
        return {"ok": False, "status": "failed", "error": f"pypi 包不存在或不可达: {pkg}（{err}）"}
    if command == "docker":
        return {"ok": False, "status": "failed", "error": "docker 镜像需本地拉取验证，暂不支持远程校验"}
    return {"ok": False, "status": "failed", "error": f"无法识别的启动命令: {command}"}


async def verify_install_spec(item: dict) -> dict:
    """验证 skill entries / plugin download_url 存在性。

    skill entries 由探针派生时本就已验过路径存在；这里只兜底老行（probe 缺失时）
    按 entry 跑 Contents HEAD。plugin 永远 HEAD download_url。
    """
    install = item.get("install_spec") if isinstance(item, dict) else None
    if not isinstance(install, dict):
        return {"ok": True, "install_verify": {}, "error": ""}
    result: dict[str, Any] = {"plugin": None, "skill": []}
    full_name = str(item.get("repo_full_name") or "")
    external = item.get("external_data") if isinstance(item, dict) else None
    probe = external.get("probe") if isinstance(external, dict) and isinstance(external.get("probe"), dict) else {}
    ref = str((probe or {}).get("ref") or "main")

    plugin = install.get("plugin") if isinstance(install.get("plugin"), dict) else None
    if plugin:
        url = str(plugin.get("download_url") or "").strip()
        if url:
            ok, err = await _head_ok(url)
            result["plugin"] = {"ok": ok, "error": err}

    skill = install.get("skill") if isinstance(install.get("skill"), dict) else None
    if isinstance(skill, dict) and isinstance(skill.get("entries"), list):
        for entry in skill["entries"]:
            if not isinstance(entry, dict):
                continue
            path = str(entry.get("path") or "").strip().strip("/")
            entry_file = str(entry.get("entry") or "SKILL.md").strip()
            full_path = f"{path}/{entry_file}" if path else entry_file
            if not full_name or not full_path:
                result["skill"].append({"name": entry.get("name"), "ok": False, "error": "路径不完整"})
                continue
            ok, err = await _head_ok(f"https://api.github.com/repos/{full_name}/contents/{full_path}?ref={ref}")
            result["skill"].append({"name": entry.get("name"), "ok": ok, "error": err})

    all_ok = (result["plugin"] is None or result["plugin"]["ok"]) and all(s["ok"] for s in result["skill"])
    return {"ok": all_ok, "install_verify": result, "error": "" if all_ok else "部分安装资源不可达"}


async def verify_item(item_id: int, *, include_install: bool = False) -> dict:
    """入口：load item → 验证 → 写回。返回汇总。"""
    import marketplace_leaderboard_store as store

    row = await store.get_item(item_id)
    if row is None:
        return {"id": item_id, "error": "榜单条目不存在"}

    launch = await verify_launch_spec(row)
    if launch["status"]:
        await store.set_launch_spec(
            item_id, spec=row.get("launch_spec") or {},
            status=launch["status"],
            error=(f"verify: {launch['error']}") if launch["error"] else "",
        )

    install: dict = {}
    if include_install:
        install = await verify_install_spec(row)
        if install.get("install_verify"):
            await store.merge_external_data_probe(item_id, {"install_verify": install["install_verify"]})

    return {"id": item_id, "launch": launch, "install": install}


async def verify_published_batch(limit: int = 200, *, sleep_seconds: float = 0.5) -> dict:
    """已发布条目的 launch_spec 巡检（remote/stdio 便宜校验，不执行任意代码）。

    只巡已发布 + 分类含 mcp + kind in (remote, stdio) + 状态 in (filled, verified) 的
    条目——remote 是"看起来对其实挂了"的重灾区，握手一次就识破；stdio 的 npm/pypi
    registry HEAD 也很便宜。验证结果写回 launch_spec_status（verified/failed），
    前端徽标即见。挂在 sync_loop 的每次同步之后跑（同步未启用时空转）。
    """
    import marketplace_leaderboard_store as store

    rows = await store.list_items(status="published", limit=max(1, min(1000, limit)))
    checked = verified = failed = 0
    for row in rows:
        modules = row.get("target_modules") or []
        if "mcp" not in (modules if isinstance(modules, list) else []):
            continue
        spec = row.get("launch_spec") if isinstance(row.get("launch_spec"), dict) else {}
        if str((spec or {}).get("kind") or "none") not in ("remote", "stdio"):
            continue
        if str(row.get("launch_spec_status") or "") not in ("filled", "verified"):
            continue
        result = await verify_launch_spec(row)
        checked += 1
        if result["status"] == "verified":
            verified += 1
        elif result["status"] == "failed":
            failed += 1
        if result["status"]:
            await store.set_launch_spec(
                int(row["id"]), spec=row.get("launch_spec") or {},
                status=result["status"],
                error=(f"verify: {result['error']}") if result["error"] else "",
            )
        if sleep_seconds > 0:
            await asyncio.sleep(sleep_seconds)
    return {"checked": checked, "verified": verified, "failed": failed}
