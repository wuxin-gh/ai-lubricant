"""Manifest-driven configuration resources for built-in MCP services.

The frontend and generic API operate on resources/views/actions.  Physical
storage (SQL rows, service env, or runtime state) is hidden behind registered
adapters; manifests may only reference allow-listed binding names.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlparse
import re

import mcp_plugin_store


class ConfigurationError(ValueError):
    pass


class ConfigurationConflict(ConfigurationError):
    pass


@dataclass(frozen=True)
class ResourceContext:
    service: dict
    definition: dict

    @property
    def service_id(self) -> int:
        return int(self.service["id"])


class ResourceAdapter(Protocol):
    capabilities: frozenset[str]

    async def list(self, ctx: ResourceContext, query: dict) -> list[dict]: ...
    async def get(self, ctx: ResourceContext, item_id: Any | None = None) -> dict | None: ...
    async def create(self, ctx: ResourceContext, value: dict) -> dict: ...
    async def update(self, ctx: ResourceContext, item_id: Any | None, patch: dict) -> dict | None: ...
    async def delete(self, ctx: ResourceContext, item_id: Any) -> bool: ...


def validate_resource_value(definition: dict, value: dict, *, partial: bool = False) -> dict:
    schema = definition.get("schema") or {}
    properties = schema.get("properties") or {}
    if not isinstance(value, dict):
        raise ConfigurationError("资源值必须是对象")
    unknown = set(value) - set(properties) - {"parent_id", "expected_revision"}
    if unknown:
        raise ConfigurationError(f"未知字段: {sorted(unknown)}")
    if not partial:
        missing = [key for key in schema.get("required") or [] if value.get(key) in (None, "")]
        if missing:
            raise ConfigurationError(f"缺少必填字段: {missing}")
    for key, raw in value.items():
        field = properties.get(key)
        if field is None:
            continue
        if field.get("readOnly"):
            raise ConfigurationError(f"字段 {key} 为只读")
        expected = field.get("type")
        valid = (
            expected == "string" and isinstance(raw, str)
            or expected == "boolean" and isinstance(raw, bool)
            or expected == "integer" and isinstance(raw, int) and not isinstance(raw, bool)
            or expected == "number" and isinstance(raw, (int, float)) and not isinstance(raw, bool)
            or expected == "array" and isinstance(raw, list)
        )
        if raw is not None and expected and not valid:
            raise ConfigurationError(f"字段 {key} 类型应为 {expected}")
        # array 元素类型校验（用于 user_ids / agent_ids 这类 integer id 列表）。
        if expected == "array" and isinstance(raw, list):
            item_type = (field.get("items") or {}).get("type")
            if item_type == "integer":
                for element in raw:
                    if not isinstance(element, int) or isinstance(element, bool):
                        raise ConfigurationError(f"字段 {key} 的元素类型应为 integer")
            elif item_type == "string":
                for element in raw:
                    if not isinstance(element, str):
                        raise ConfigurationError(f"字段 {key} 的元素类型应为 string")
        if field.get("enum") and raw not in field["enum"]:
            raise ConfigurationError(f"字段 {key} 不在允许值中")
        if raw is None or raw == "" or raw == []:
            continue
        minimum = field.get("minimum")
        maximum = field.get("maximum")
        if minimum is not None and isinstance(raw, (int, float)) and raw < minimum:
            raise ConfigurationError(f"字段 {key} 不能小于 {minimum}")
        if maximum is not None and isinstance(raw, (int, float)) and raw > maximum:
            raise ConfigurationError(f"字段 {key} 不能大于 {maximum}")
        if field.get("format") == "email" and isinstance(raw, str):
            if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", raw):
                raise ConfigurationError(f"字段 {key} 不是有效邮箱地址")
        if field.get("format") == "uri" and isinstance(raw, str):
            parsed = urlparse(raw)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                raise ConfigurationError(f"字段 {key} 不是有效 HTTP(S) 地址")
    return value


def _normalize_id_list(raw: Any, *, fallback: Any = None) -> list[int]:
    """把存储里的 id 列表规整成去重的 int 列表；兼容旧的单值 fallback。"""
    items = raw if isinstance(raw, list) else None
    if items is None and fallback not in (None, ""):
        items = [fallback]
    out: list[int] = []
    for item in items or []:
        try:
            value = int(item)
        except (TypeError, ValueError):
            continue
        if value not in out:
            out.append(value)
    return out


class ResourceTypeHandler(Protocol):
    def prepare(self, value: dict, *, partial: bool = False) -> dict: ...
    def present(self, value: dict | None) -> dict | None: ...


class ObjectTypeHandler:
    def prepare(self, value: dict, *, partial: bool = False) -> dict:
        return dict(value)

    def present(self, value: dict | None) -> dict | None:
        return dict(value) if value is not None else None


class MailAccountTypeHandler(ObjectTypeHandler):
    def prepare(self, value: dict, *, partial: bool = False) -> dict:
        result = dict(value)
        for key in ("display_name", "username", "base_url"):
            if key in result:
                result[key] = str(result[key]).strip()
        if "base_url" in result:
            result["base_url"] = result["base_url"].rstrip("/")
        if "user_ids" in result:
            result["user_ids"] = _normalize_id_list(result.get("user_ids"))
        return result

    def present(self, value: dict | None) -> dict | None:
        if value is None:
            return None
        result = dict(value)
        states = dict(result.get("_secrets") or {})
        for key in ("password", "secret_key"):
            result.pop(key, None)
            states.setdefault(key, {"state": "unset"})
        result["_secrets"] = states
        result["user_ids"] = _normalize_id_list(result.get("user_ids"))
        result["mailbox_type"] = str(result.get("mailbox_type") or "TempMail")
        return result


class MailAddressTypeHandler(ObjectTypeHandler):
    def prepare(self, value: dict, *, partial: bool = False) -> dict:
        result = dict(value)
        for key in ("address", "source_address"):
            if key in result:
                result[key] = str(result[key]).strip().lower()
        return result


class CdpClientTypeHandler(ObjectTypeHandler):
    def prepare(self, value: dict, *, partial: bool = False) -> dict:
        result = dict(value)
        if "name" in result:
            result["name"] = str(result["name"]).strip()
        return result

    def present(self, value: dict | None) -> dict | None:
        if value is None:
            return None
        result = dict(value)
        # A token is never part of a stored row. It is injected only into the
        # immediate create/rotate response by SharedConfigAdapter.
        result.pop("token_hash", None)
        result["client_id"] = str(result.get("instance_key") or result.get("id") or "")
        # 兼容旧数据：单 user_id → user_ids 列表；补齐缺省 agent_ids。
        result["user_ids"] = _normalize_id_list(result.get("user_ids"), fallback=result.get("user_id"))
        result["agent_ids"] = _normalize_id_list(result.get("agent_ids"))
        result.pop("user_id", None)
        return result


TYPE_HANDLERS: dict[str, ResourceTypeHandler] = {
    "object": ObjectTypeHandler(),
    "mail-account": MailAccountTypeHandler(),
    "mail-address": MailAddressTypeHandler(),
    "cdp-client": CdpClientTypeHandler(),
}


class SharedConfigAdapter:
    """One allowlisted adapter for every editable runtime config type."""

    capabilities = frozenset({"list", "read", "create", "update", "delete", "filter", "relate", "replace", "runtime-apply"})

    def _storage(self, ctx: ResourceContext) -> dict:
        storage = ctx.definition.get("storage") or {}
        config_type = str(storage.get("configType") or "")
        if config_type not in mcp_plugin_store.ALLOWED_RUNTIME_CONFIG_TYPES:
            raise ConfigurationError(f"未允许的配置类型: {config_type}")
        return storage

    def _handler(self, ctx: ResourceContext) -> ResourceTypeHandler:
        type_name = str(self._storage(ctx).get("handler") or "object")
        handler = TYPE_HANDLERS.get(type_name)
        if handler is None:
            raise ConfigurationError(f"未注册的配置类型处理器: {type_name}")
        return handler

    async def _validate_users_agents(self, ctx: ResourceContext, prepared: dict) -> None:
        """校验 cdp_client / mail_account 的可操作用户与可操作 agent 列表。

        方向已翻转：不再要求用户预先获得服务授权——由本实例的 user_ids 反过来
        决定该服务的授权用户集（保存后由 recompute_service_users_from_resources 回写）。
        这里只校验 user_ids/agent_ids 引用的对象存在且已启用。
        """
        config_type = self._storage(ctx)["configType"]
        if config_type not in ("cdp_client", "mail_account"):
            return
        if "user_ids" in prepared:
            user_ids = _normalize_id_list(prepared["user_ids"])
            for user_id in user_ids:
                user = await mcp_plugin_store.get_mcp_user(user_id)
                if not user or not user.get("enabled"):
                    raise ConfigurationError(f"可操作用户不存在或未启用: {user_id}")
            prepared["user_ids"] = user_ids
        if config_type == "cdp_client" and "agent_ids" in prepared:
            agent_ids = _normalize_id_list(prepared["agent_ids"])
            if agent_ids:
                found = set(await mcp_plugin_store.list_agent_ids(agent_ids))
                missing = [aid for aid in agent_ids if aid not in found]
                if missing:
                    raise ConfigurationError(f"可操作 agent 不存在或未启用: {missing}")
            prepared["agent_ids"] = agent_ids

    async def list(self, ctx: ResourceContext, query: dict) -> list[dict]:
        storage = self._storage(ctx)
        rows = await mcp_plugin_store.list_runtime_configs(ctx.service_id, storage["configType"])
        parent_type = storage.get("parentConfigType")
        if parent_type:
            parent_id = query.get("parent_id")
            try:
                parent = await mcp_plugin_store.get_runtime_config(int(parent_id))
            except (TypeError, ValueError):
                parent = None
            if not parent or parent.get("config_type") != parent_type or int(parent["service_id"]) != ctx.service_id:
                raise ConfigurationError("父配置不存在")
            rows = [row for row in rows if row.get("parent_instance_key") == parent["instance_key"]]
        return [self._handler(ctx).present(row) for row in rows]

    async def get(self, ctx: ResourceContext, item_id: Any | None = None) -> dict | None:
        storage = self._storage(ctx)
        if ctx.definition.get("cardinality") == "singleton":
            row = await mcp_plugin_store.get_runtime_config_by_key(ctx.service_id, storage["configType"], "singleton")
        elif item_id is not None:
            if storage.get("idField") == "instance_key":
                row = await mcp_plugin_store.get_runtime_config_by_key(ctx.service_id, storage["configType"], str(item_id))
            else:
                try:
                    row = await mcp_plugin_store.get_runtime_config(int(item_id))
                except (TypeError, ValueError):
                    row = None
            if row and (int(row["service_id"]) != ctx.service_id or row["config_type"] != storage["configType"]):
                row = None
        else:
            row = None
        return self._handler(ctx).present(row)

    async def create(self, ctx: ResourceContext, value: dict) -> dict:
        storage, handler = self._storage(ctx), self._handler(ctx)
        prepared = handler.prepare({key: val for key, val in value.items() if key not in {"parent_id", "expected_revision"}})
        await self._validate_users_agents(ctx, prepared)
        parent_type = storage.get("parentConfigType")
        if parent_type:
            try:
                parent = await mcp_plugin_store.get_runtime_config(int(value.get("parent_id")))
            except (TypeError, ValueError):
                parent = None
            if not parent or parent.get("config_type") != parent_type or int(parent["service_id"]) != ctx.service_id:
                raise ConfigurationError("父配置不存在")
            prepared["parent_instance_key"] = parent["instance_key"]
        instance_key = prepared.get(storage.get("instanceKeyField")) if storage.get("instanceKeyField") else None
        if ctx.definition.get("cardinality") == "singleton":
            row = await mcp_plugin_store.upsert_runtime_singleton(ctx.service_id, storage["configType"], prepared, defaults=storage.get("defaults"))
        elif storage["configType"] == "cdp_client":
            token = mcp_plugin_store.generate_runtime_token()
            row = await mcp_plugin_store.create_runtime_config(
                ctx.service_id, "cdp_client", prepared, instance_key=instance_key,
                token_hash=mcp_plugin_store.hash_runtime_token(token),
                token_hint=mcp_plugin_store.runtime_token_hint(token),
            )
            presented = handler.present(row) or {}
            presented["token"] = token
            return presented
        else:
            row = await mcp_plugin_store.create_runtime_config(ctx.service_id, storage["configType"], prepared, instance_key=instance_key)
        return handler.present(row) or {}

    async def update(self, ctx: ResourceContext, item_id: Any | None, patch: dict) -> dict | None:
        storage, handler = self._storage(ctx), self._handler(ctx)
        expected_revision = patch.get("expected_revision")
        prepared = handler.prepare({key: val for key, val in patch.items() if key not in {"parent_id", "expected_revision"}}, partial=True)
        await self._validate_users_agents(ctx, prepared)
        try:
            if ctx.definition.get("cardinality") == "singleton":
                row = await mcp_plugin_store.upsert_runtime_singleton(
                    ctx.service_id, storage["configType"], prepared,
                    defaults=storage.get("defaults"), expected_revision=expected_revision,
                )
            else:
                current = await self.get(ctx, item_id)
                if not current:
                    return None
                row = await mcp_plugin_store.update_runtime_config(
                    int(current["id"]), prepared, expected_revision=expected_revision,
                )
        except ValueError as exc:
            if "revision conflict" in str(exc).lower():
                raise ConfigurationConflict("配置已被其他操作修改，请刷新后重试") from exc
            raise
        return handler.present(row)

    async def rotate_token(self, ctx: ResourceContext, item_id: Any, expected_revision: int | None = None) -> dict | None:
        storage, handler = self._storage(ctx), self._handler(ctx)
        if storage["configType"] != "cdp_client":
            raise ConfigurationError("该资源不支持 token 轮换")
        current = await self.get(ctx, item_id)
        if not current:
            return None
        token = mcp_plugin_store.generate_runtime_token()
        try:
            row = await mcp_plugin_store.update_runtime_config(
                int(current["id"]),
                {"token_hash": mcp_plugin_store.hash_runtime_token(token), "token_hint": mcp_plugin_store.runtime_token_hint(token), "enabled": True},
                expected_revision=expected_revision, replace_token=True,
            )
        except ValueError as exc:
            if "revision conflict" in str(exc).lower():
                raise ConfigurationConflict("配置已被其他操作修改，请刷新后重试") from exc
            raise
        result = handler.present(row)
        if result is not None:
            result["token"] = token
        return result

    async def revoke_token(self, ctx: ResourceContext, item_id: Any, expected_revision: int | None = None) -> dict | None:
        storage, handler = self._storage(ctx), self._handler(ctx)
        if storage["configType"] != "cdp_client":
            raise ConfigurationError("该资源不支持 token 撤销")
        current = await self.get(ctx, item_id)
        if not current:
            return None
        try:
            row = await mcp_plugin_store.update_runtime_config(
                int(current["id"]), {"enabled": False, "token_hash": None, "token_hint": None},
                expected_revision=expected_revision, replace_token=True,
            )
        except ValueError as exc:
            if "revision conflict" in str(exc).lower():
                raise ConfigurationConflict("配置已被其他操作修改，请刷新后重试") from exc
            raise
        return handler.present(row)

    async def delete(self, ctx: ResourceContext, item_id: Any, expected_revision: int | None = None) -> bool:
        if ctx.definition.get("cardinality") == "singleton":
            return False
        row = await self.get(ctx, item_id)
        if not row:
            return False
        try:
            return await mcp_plugin_store.delete_runtime_config(int(row["id"]), expected_revision=expected_revision)
        except ValueError as exc:
            if "revision conflict" in str(exc).lower():
                raise ConfigurationConflict("配置已被其他操作修改，请刷新后重试") from exc
            raise

    async def delete_with_query(self, ctx: ResourceContext, item_id: Any, query: dict, expected_revision: int | None = None) -> bool:
        rows = await self.list(ctx, query)
        row = next((item for item in rows if str(item.get("id")) == str(item_id) or item.get("instance_key") == str(item_id)), None)
        if not row:
            return False
        try:
            return await mcp_plugin_store.delete_runtime_config(int(row["id"]), expected_revision=expected_revision)
        except ValueError as exc:
            if "revision conflict" in str(exc).lower():
                raise ConfigurationConflict("配置已被其他操作修改，请刷新后重试") from exc
            raise

    async def replace(self, ctx: ResourceContext, values: list[dict]) -> list[dict]:
        storage, handler = self._storage(ctx), self._handler(ctx)
        prepared = [handler.prepare(value) for value in values]
        return await mcp_plugin_store.replace_runtime_configs(ctx.service_id, storage["configType"], prepared)


_SHARED_ADAPTER = SharedConfigAdapter()


RESOURCE_ADAPTERS: dict[str, ResourceAdapter] = {
    "shared": _SHARED_ADAPTER,
}


def get_resource_definition(manifest: dict, resource_key: str) -> dict:
    definition = ((manifest.get("config") or {}).get("resources") or {}).get(resource_key)
    if not isinstance(definition, dict):
        raise ConfigurationError(f"未知配置资源: {resource_key}")
    binding = definition.get("binding")
    adapter = RESOURCE_ADAPTERS.get(str(binding))
    if adapter is None:
        raise ConfigurationError(f"未注册的配置资源 binding: {binding}")
    requested = set(definition.get("capabilities") or [])
    unsupported = requested - set(adapter.capabilities)
    if unsupported:
        raise ConfigurationError(f"资源 {resource_key} 声明了 adapter 不支持的能力: {sorted(unsupported)}")
    return definition


def get_resource_adapter(manifest: dict, resource_key: str) -> tuple[dict, ResourceAdapter]:
    definition = get_resource_definition(manifest, resource_key)
    return definition, RESOURCE_ADAPTERS[str(definition["binding"])]


def configuration_contract(manifest: dict) -> dict:
    config = manifest.get("config") or {}
    resources = config.get("resources") or {}
    for key in resources:
        get_resource_definition(manifest, key)
    return {
        "schema": int(manifest.get("schema") or 1),
        "service_name": manifest.get("name"),
        "resources": resources,
        "views": config.get("views") or {},
        "actions": config.get("actions") or {},
        "panels": manifest.get("panels") or {},
    }
