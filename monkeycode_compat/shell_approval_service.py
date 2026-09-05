"""Persistent shell-command authorization policy storage.

Node policies are the active runtime source: platform administrators configure
one execution node and every user or Agent using that node shares the result.
The older user-policy helpers remain only for rolling compatibility with the
legacy ``/api/v1/users/shell-approval`` routes; the Agent runtime does not read
them and no data is copied between the two scopes.
"""
from __future__ import annotations

from .models_team_admin import NodeShellApprovalPolicy, UserShellApprovalPolicy

_SHELL_FLAVORS = ("posix", "powershell", "cmd", "unknown")


def infer_node_shell_flavor(node: dict | None) -> str:
    """Derive the effective shell from trusted control-plane node telemetry."""
    node = node or {}
    capabilities = node.get("capabilities") or {}
    os_name = str(capabilities.get("os") or node.get("platform") or "").strip().lower()
    if os_name == "windows" or os_name.startswith("windows/"):
        return "powershell"
    if os_name:
        return "posix"
    return "unknown"


def _normalize_shell(shell: str) -> str:
    value = (shell or "").strip().lower()
    if value not in _SHELL_FLAVORS:
        raise ValueError("shell 必须为 posix/powershell/cmd/unknown")
    return value


async def list_node_policy(node_id: str) -> list[dict]:
    rows = await NodeShellApprovalPolicy.filter(node_id=str(node_id)).order_by("-updated_at")
    return [_node_row_to_dict(row) for row in rows]


async def list_node_auto_allow_keys(node_id: str, shell_flavor: str) -> set[str]:
    flavor = _normalize_shell(shell_flavor)
    rows = await NodeShellApprovalPolicy.filter(
        node_id=str(node_id), shell_flavor=flavor,
    ).only("command_key")
    return {row.command_key for row in rows}


async def upsert_node_policy(
    node_id: str,
    command_key: str,
    shell_flavor: str,
    note: str | None = None,
) -> dict:
    key = (command_key or "").strip().lower()
    if not key:
        raise ValueError("command_key 不能为空")
    flavor = _normalize_shell(shell_flavor)
    obj, _ = await NodeShellApprovalPolicy.update_or_create(
        node_id=str(node_id),
        command_key=key,
        shell_flavor=flavor,
        defaults={"note": note},
    )
    return _node_row_to_dict(obj)


async def delete_node_policy(node_id: str, command_key: str, shell_flavor: str) -> bool:
    key = (command_key or "").strip().lower()
    flavor = _normalize_shell(shell_flavor)
    deleted = await NodeShellApprovalPolicy.filter(
        node_id=str(node_id), command_key=key, shell_flavor=flavor,
    ).delete()
    return bool(deleted)


async def clear_node_policy(node_id: str, shell_flavor: str | None = None) -> int:
    query = NodeShellApprovalPolicy.filter(node_id=str(node_id))
    if shell_flavor is not None:
        query = query.filter(shell_flavor=_normalize_shell(shell_flavor))
    return await query.delete()


# Legacy user-policy compatibility helpers. These remain callable by the old
# user routes but are intentionally not a fallback for node runtime policy.
async def list_user_policy(user_id: str) -> list[dict]:
    rows = await UserShellApprovalPolicy.filter(user_id=str(user_id)).order_by("-updated_at")
    return [_user_row_to_dict(row) for row in rows]


async def list_auto_allow_keys(user_id: str, shell_flavor: str) -> set[str]:
    flavor = _normalize_shell(shell_flavor)
    rows = await UserShellApprovalPolicy.filter(
        user_id=str(user_id), shell_flavor=flavor,
    ).only("command_key")
    return {row.command_key for row in rows}


async def upsert_policy(
    user_id: str,
    command_key: str,
    shell_flavor: str,
    note: str | None = None,
) -> dict:
    key = (command_key or "").strip().lower()
    if not key:
        raise ValueError("command_key 不能为空")
    flavor = _normalize_shell(shell_flavor)
    obj, _ = await UserShellApprovalPolicy.update_or_create(
        user_id=str(user_id),
        command_key=key,
        shell_flavor=flavor,
        defaults={"note": note},
    )
    return _user_row_to_dict(obj)


async def delete_policy(user_id: str, command_key: str, shell_flavor: str) -> bool:
    key = (command_key or "").strip().lower()
    flavor = _normalize_shell(shell_flavor)
    deleted = await UserShellApprovalPolicy.filter(
        user_id=str(user_id), command_key=key, shell_flavor=flavor,
    ).delete()
    return bool(deleted)


async def clear_policy(user_id: str) -> int:
    return await UserShellApprovalPolicy.filter(user_id=str(user_id)).delete()


def _node_row_to_dict(row: NodeShellApprovalPolicy) -> dict:
    return {
        "node_id": row.node_id,
        "command_key": row.command_key,
        "shell_flavor": row.shell_flavor,
        "note": row.note,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _user_row_to_dict(row: UserShellApprovalPolicy) -> dict:
    return {
        "command_key": row.command_key,
        "shell_flavor": row.shell_flavor,
        "note": row.note,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


__all__ = [
    "infer_node_shell_flavor",
    "list_node_policy",
    "list_node_auto_allow_keys",
    "upsert_node_policy",
    "delete_node_policy",
    "clear_node_policy",
    "list_user_policy",
    "list_auto_allow_keys",
    "upsert_policy",
    "delete_policy",
    "clear_policy",
]
