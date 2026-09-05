"""sync_agent_sops / list_for_agent per-Agent binding semantics tests.

Covers the regression fixed in this round: the previous implementation synced every
enabled catalog SOP to every Agent (wrong global sharing). The correct behavior:

- built-in catalog SOPs are unconditionally synced to every Agent (runtime default);
- custom catalog SOPs are synced only when an AgentSopBinding row exists for that
  (agent_id, sop_id) pair (per-Agent binding, not global);
- Agent-private distilled SOPs under memory/sop/ are preserved (never deleted);
- disabling / unbinding a custom SOP removes its catalog file from the Agent.

The Tortoise ORM (AgentSop / AgentSopBinding) is monkeypatched with in-memory
fakes so the tests run without Postgres. file_memory's root is pinned to a tmp
directory so the real data/agents tree is never touched.
"""
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from agent import sop_service, file_memory as fm


# ---------------------------------------------------------------------------
# Fake Tortoise ORM (supports .filter / .all / .values / .update_or_create)
# ---------------------------------------------------------------------------


class _FakeRow(SimpleNamespace):
    """A catalog/binding row: attribute access mirrors a Tortoise model instance."""

    async def delete(self) -> None:  # pragma: no cover - exercised by unbind
        self._store.remove(self)


class _FakeQuery:
    """Awaitable queryset fake supporting .filter / .all / .values / .first.

    Each filter key may be a plain attribute (``is_builtin=True``) or a Tortoise
    lookup (``id__in=[...]``). Awaiting the query returns the matching rows;
    ``.values(*fields)`` returns a coroutine resolving to list-of-dicts.
    """

    def __init__(self, store: list[_FakeRow], filters: dict[str, Any] | None = None) -> None:
        self._store = store
        self._filters = filters or {}

    def _match(self, row: _FakeRow) -> bool:
        for key, value in self._filters.items():
            if key == "id__in":
                if getattr(row, "id", None) not in value:
                    return False
            elif key == "sop_id__in":
                if getattr(row, "sop_id", None) not in value:
                    return False
            else:
                if getattr(row, key, None) != value:
                    return False
        return True

    def filter(self, **kwargs) -> "_FakeQuery":
        merged = {**self._filters, **kwargs}
        return _FakeQuery(self._store, merged)

    def all(self) -> "_FakeQuery":
        return self

    def __await__(self):
        async def _list() -> list[_FakeRow]:
            return [r for r in self._store if self._match(r)]

        return _list().__await__()

    async def values(self, *fields: str) -> list[dict[str, Any]]:
        return [
            {field: getattr(r, field) for field in fields}
            for r in self._store
            if self._match(r)
        ]

    async def first(self) -> _FakeRow | None:
        for r in self._store:
            if self._match(r):
                return r
        return None


class _FakeModel:
    """Stand-in for a Tortoise model class with a shared row store."""

    def __init__(self) -> None:
        self.store: list[_FakeRow] = []

    def add(self, **fields) -> _FakeRow:
        row = _FakeRow(**fields)
        row._store = self.store
        self.store.append(row)
        return row

    def filter(self, **kwargs) -> _FakeQuery:
        return _FakeQuery(self.store, kwargs)

    def all(self) -> _FakeQuery:
        return _FakeQuery(self.store)

    async def update_or_create(self, defaults: dict | None = None, **lookup) -> tuple[_FakeRow, bool]:
        for r in self.store:
            if all(getattr(r, k, None) == v for k, v in lookup.items()):
                if defaults:
                    for k, v in defaults.items():
                        setattr(r, k, v)
                return r, False
        merged = {**lookup, **(defaults or {})}
        return self.add(**merged), True

    async def get_or_none(self, **lookup) -> _FakeRow | None:
        for r in self.store:
            if all(getattr(r, k, None) == v for k, v in lookup.items()):
                return r
        return None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


BUILTIN_SOP_BODY = "# Built-in SOP\nGA default behavior.\n"
CUSTOM_SOP_BODY = "# Custom SOP\nuser-defined procedure.\n"


@pytest.fixture()
def sop_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Isolate SOP source dir + Agent memory root + ORM models."""
    source_root = tmp_path / "source"
    source_root.mkdir()
    (source_root / "builtin").mkdir()
    (source_root / "custom").mkdir()
    (source_root / "builtin" / "memory_management_sop.md").write_text(BUILTIN_SOP_BODY, encoding="utf-8")
    (source_root / "custom" / "deploy_procedure.md").write_text(CUSTOM_SOP_BODY, encoding="utf-8")

    agents_root = tmp_path / "agents"
    agents_root.mkdir()

    monkeypatch.setattr(sop_service, "_SOP_SOURCE_ROOT", source_root)
    monkeypatch.setattr(fm, "_MEMORY_ROOT", agents_root)

    fake_sops = _FakeModel()
    fake_bindings = _FakeModel()
    builtin_id = UUID(int=1)
    custom_id = UUID(int=2)
    fake_sops.add(
        id=builtin_id,
        name="Memory Management",
        description="Built-in GenericAgent SOP",
        file_ref="builtin/memory_management_sop.md",
        scope_type="global",
        scope_id="global",
        created_by=None,
        enabled=True,
        is_builtin=True,
        revision=1,
        updated_at=None,
    )
    fake_sops.add(
        id=custom_id,
        name="Deploy Procedure",
        description="user-defined procedure",
        file_ref="custom/deploy_procedure.md",
        scope_type="global",
        scope_id="global",
        created_by=None,
        enabled=True,
        is_builtin=False,
        revision=1,
        updated_at=None,
    )
    monkeypatch.setattr(sop_service, "AgentSop", fake_sops)
    monkeypatch.setattr(sop_service, "AgentSopBinding", fake_bindings)

    return SimpleNamespace(
        source_root=source_root,
        agents_root=agents_root,
        sops=fake_sops,
        bindings=fake_bindings,
        builtin_id=builtin_id,
        custom_id=custom_id,
    )


# ---------------------------------------------------------------------------
# sync_agent_sops
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_builtin_sop_unconditionally_synced(sop_env) -> None:
    """Built-in SOP enters every Agent even with zero bindings."""
    await sop_service.sync_agent_sops(1)

    sop_file = sop_env.agents_root / "1" / "memory" / "sop" / "memory_management_sop.md"
    assert sop_file.is_file()
    assert sop_file.read_text(encoding="utf-8") == BUILTIN_SOP_BODY
    # Built-in SOPs are routable from L1. The index is a GA-style routing table
    # (``<stem>(trigger words)`` grouped on the L3 line), not one full-path line
    # per SOP, so assert on the stem plus the L0/L3 scaffolding.
    l1 = (sop_env.agents_root / "1" / "memory" / "global_mem_insight.txt").read_text(encoding="utf-8")
    assert "memory_management_sop" in l1
    assert "L0(META-SOP):" in l1
    assert "L3:" in l1


@pytest.mark.asyncio
async def test_unbound_custom_sop_not_synced(sop_env) -> None:
    """Unbound custom SOP must NOT auto-sync to all Agents (regression)."""
    await sop_service.sync_agent_sops(1)

    custom_file = sop_env.agents_root / "1" / "memory" / "sop" / "deploy_procedure.md"
    assert not custom_file.is_file(), "unbound custom SOP leaked into the Agent"


@pytest.mark.asyncio
async def test_bound_custom_sop_synced(sop_env) -> None:
    """Custom SOP bound via AgentSopBinding is synced to that Agent."""
    sop_env.bindings.add(
        id=uuid4(),
        agent_id=1,
        sop_id=sop_env.custom_id,
        enabled=True,
    )
    await sop_service.sync_agent_sops(1)

    custom_file = sop_env.agents_root / "1" / "memory" / "sop" / "deploy_procedure.md"
    assert custom_file.is_file()
    assert custom_file.read_text(encoding="utf-8") == CUSTOM_SOP_BODY


@pytest.mark.asyncio
async def test_bound_custom_not_leaked_to_other_agent(sop_env) -> None:
    """A binding for Agent 1 must not sync the custom SOP to Agent 2."""
    sop_env.bindings.add(id=uuid4(), agent_id=1, sop_id=sop_env.custom_id, enabled=True)
    await sop_service.sync_agent_sops(1)
    await sop_service.sync_agent_sops(2)

    agent1 = sop_env.agents_root / "1" / "memory" / "sop" / "deploy_procedure.md"
    agent2 = sop_env.agents_root / "2" / "memory" / "sop" / "deploy_procedure.md"
    assert agent1.is_file()
    assert not agent2.is_file(), "binding for Agent 1 leaked into Agent 2"


@pytest.mark.asyncio
async def test_disabled_binding_does_not_sync(sop_env) -> None:
    """A disabled binding must not sync the custom SOP."""
    sop_env.bindings.add(id=uuid4(), agent_id=1, sop_id=sop_env.custom_id, enabled=False)
    await sop_service.sync_agent_sops(1)

    custom_file = sop_env.agents_root / "1" / "memory" / "sop" / "deploy_procedure.md"
    assert not custom_file.is_file()


@pytest.mark.asyncio
async def test_private_distilled_sop_preserved(sop_env) -> None:
    """Agent-private distilled SOPs under memory/sop/ are never deleted by sync."""
    sop_root = sop_env.agents_root / "1" / "memory" / "sop"
    sop_root.mkdir(parents=True, exist_ok=True)
    private = sop_root / "mcp.mail.md"
    private.write_text("# Private distilled SOP\n", encoding="utf-8")

    await sop_service.sync_agent_sops(1)

    assert private.is_file(), "private distilled SOP was deleted by sync"
    assert private.read_text(encoding="utf-8").startswith("# Private distilled SOP")


@pytest.mark.asyncio
async def test_unbinding_removes_catalog_sop_file(sop_env) -> None:
    """Removing the binding drops the custom SOP file on the next sync."""
    sop_env.bindings.add(id=uuid4(), agent_id=1, sop_id=sop_env.custom_id, enabled=True)
    await sop_service.sync_agent_sops(1)
    custom_file = sop_env.agents_root / "1" / "memory" / "sop" / "deploy_procedure.md"
    assert custom_file.is_file()

    # Remove the binding and re-sync.
    sop_env.bindings.store.clear()
    await sop_service.sync_agent_sops(1)
    assert not custom_file.is_file(), "unbound custom SOP file was not cleaned up"
    # Private distilled SOPs are still preserved.
    # (none here, but builtin remains because built-ins are unconditional.)


# ---------------------------------------------------------------------------
# list_for_agent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_for_agent_builtin_always_effective(sop_env) -> None:
    """Built-in SOP is effective for an Agent with no bindings."""
    result = await sop_service.list_for_agent(1)
    names = {row["name"] for row in result["available"]}
    assert "Memory Management" in names
    # Unbound custom SOP is not effective.
    assert "Deploy Procedure" not in names


@pytest.mark.asyncio
async def test_list_for_agent_bound_custom_effective(sop_env) -> None:
    sop_env.bindings.add(id=uuid4(), agent_id=1, sop_id=sop_env.custom_id, enabled=True)
    result = await sop_service.list_for_agent(1)
    names = {row["name"] for row in result["available"]}
    assert {"Memory Management", "Deploy Procedure"}.issubset(names)


@pytest.mark.asyncio
async def test_list_for_agent_includes_private_distilled(sop_env) -> None:
    sop_root = sop_env.agents_root / "1" / "memory" / "sop"
    sop_root.mkdir(parents=True, exist_ok=True)
    (sop_root / "mcp.mail.md").write_text("# private\n", encoding="utf-8")
    result = await sop_service.list_for_agent(1)
    private = [r for r in result["available"] if r["filename"].startswith("private/")]
    assert any(r["name"] == "mcp.mail" for r in private)


# ---------------------------------------------------------------------------
# bind_sop / unbind_sop (backend capability, no UI yet)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bind_sop_creates_binding(sop_env) -> None:
    await sop_service.bind_sop(1, str(sop_env.custom_id))
    bindings = await sop_service.list_bound_sop_ids(1)
    assert str(sop_env.custom_id) in bindings


@pytest.mark.asyncio
async def test_bind_sop_rejects_builtin(sop_env) -> None:
    with pytest.raises(ValueError, match="auto-effective"):
        await sop_service.bind_sop(1, str(sop_env.builtin_id))


@pytest.mark.asyncio
async def test_unbind_sop_removes_binding(sop_env) -> None:
    await sop_service.bind_sop(1, str(sop_env.custom_id))
    removed = await sop_service.unbind_sop(1, str(sop_env.custom_id))
    assert removed is True
    bindings = await sop_service.list_bound_sop_ids(1)
    assert str(sop_env.custom_id) not in bindings
