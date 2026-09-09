"""Regression coverage for the user-facing node capability projection."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

import user_platform.nodes_service as nodes_service_module
from user_platform.node_client import NodeServerUnavailable
from user_platform.node_client.normalization import normalize_node_info
from user_platform.nodes_service import NodesService


def _link(node_id: str, role: str = "execution") -> SimpleNamespace:
    return SimpleNamespace(
        group_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        node_id=node_id,
        node_name=f"saved-{node_id}",
        node_role=role,
        created_at=datetime(2026, 8, 4, tzinfo=UTC),
    )


def _live_node(
    node_id: str,
    *,
    role: str = "execution",
    manager_node_id: str = "",
    passive: bool = False,
) -> dict:
    role_proto = "NODE_ROLE_PASSIVE_MANAGEMENT" if passive else f"NODE_ROLE_{role.upper()}"
    raw = {
        "nodeId": node_id,
        "nodeName": f"live-{node_id}",
        "role": role_proto,
        "status": "NODE_STATUS_APPROVED",
        "startupMethod": "NODE_STARTUP_METHOD_DOCKER",
        "managerNodeId": manager_node_id,
        "connected": True,
        "online": True,
        "lastHeartbeatAt": "2026-08-04T10:20:30Z",
        "activeSessionIds": ["session-1"],
        "capabilities": {
            "os": "linux",
            "arch": "amd64",
            "labels": {
                "hostname": "build-host",
                "cpu": "Example CPU",
                "cpu_cores": "8",
                "memory_total": "17179869184",
                "client_version": "260804-deadbee",
                "future_machine_fact": "preserved",
            },
        },
    }
    return normalize_node_info(raw)


def test_binding_projection_preserves_all_normalized_machine_facts() -> None:
    service = NodesService()
    live = _live_node("node-1")
    live.update(
        capacity={"max_sessions": 4, "cpu_total": 8, "memory_total": 17179869184},
        active_sessions=1,
        editor_occupancy=2,
    )

    row = service._binding_dict(_link("node-1"), live)

    assert row["capabilities"] == live["capabilities"]
    assert row["capabilities"] == {
        "hostname": "build-host",
        "cpu": "Example CPU",
        "cpu_cores": "8",
        "memory_total": "17179869184",
        "client_version": "260804-deadbee",
        "future_machine_fact": "preserved",
        "os": "linux",
        "arch": "amd64",
    }
    for field in (
        "role",
        "startup_method",
        "status",
        "connected",
        "online",
        "last_heartbeat_at",
        "active_session_ids",
        "manager_node_id",
    ):
        assert row[field] == live[field]
    assert row["capacity"] == live["capacity"]
    assert row["active_sessions"] == 1
    assert row["editor_occupancy"] == 2


def test_binding_projection_keeps_last_reported_facts_while_offline() -> None:
    service = NodesService()
    live = _live_node("node-offline")
    live.update(connected=False, online=False)

    row = service._binding_dict(_link("node-offline"), live)

    assert row["connected"] is False
    assert row["online"] is False
    assert row["last_heartbeat_at"] == "2026-08-04T10:20:30Z"
    assert row["capabilities"]["client_version"] == "260804-deadbee"


def test_missing_live_node_has_explicit_unknown_state_without_fabricated_facts() -> None:
    row = NodesService()._binding_dict(_link("missing"), None)

    assert row["status"] == "unknown"
    assert row["connected"] is False
    assert row["online"] is False
    assert row["capabilities"] == {}
    assert row["last_heartbeat_at"] == ""


def test_management_grant_projects_child_machine_facts() -> None:
    service = NodesService()
    manager_link = _link("manager-1", "management")
    child = _live_node("child-1", manager_node_id="manager-1")

    rows = service._derived_child_dicts(
        [manager_link],
        {"child-1": child},
        {"manager-1"},
    )

    assert len(rows) == 1
    assert rows[0]["node_id"] == "child-1"
    assert rows[0]["manager_node_id"] == "manager-1"
    assert rows[0]["capabilities"] == child["capabilities"]


def test_display_only_parent_redacts_runtime_facts() -> None:
    service = NodesService()
    child = service._binding_dict(
        _link("child-1"),
        _live_node("child-1", manager_node_id="manager-1"),
    )
    manager = _live_node("manager-1", role="management")

    rows = service._context_manager_dicts([child], {"manager-1": manager})

    assert len(rows) == 1
    assert rows[0]["display_only"] is True
    assert rows[0]["capabilities"] == {}
    assert rows[0]["connected"] is False
    assert rows[0]["online"] is False
    assert rows[0]["last_heartbeat_at"] == ""


def test_passive_manager_is_identified_without_fabricated_client_facts() -> None:
    service = NodesService()
    live = _live_node("container-1", role="management", passive=True)
    live["capabilities"] = {}

    row = service._binding_dict(_link("container-1", "management"), live)

    assert row["node_role"] == "management"
    assert row["is_passive"] is True
    assert row["capabilities"] == {}


class _OrderedLinks:
    def __init__(self, links: list[SimpleNamespace]) -> None:
        self.links = links

    def order_by(self, _field: str) -> "_OrderedLinks":
        return self

    def __await__(self):
        async def resolve() -> list[SimpleNamespace]:
            return self.links

        return resolve().__await__()


@pytest.mark.asyncio
async def test_list_my_nodes_deduplicates_bindings_without_dropping_telemetry(monkeypatch) -> None:
    service = NodesService()
    duplicate_links = [_link("node-1"), _link("node-1")]
    live = _live_node("node-1")

    async def user_group_ids(_user_id: str) -> list[uuid.UUID]:
        return [duplicate_links[0].group_id]

    async def live_node_map_ex() -> tuple[dict[str, dict], bool]:
        return {"node-1": live}, True

    async def attach_editor_occupancy(_live_map: dict[str, dict]) -> None:
        return None

    class FakeGroupNode:
        @staticmethod
        def filter(**_kwargs) -> _OrderedLinks:
            return _OrderedLinks(duplicate_links)

    monkeypatch.setattr(nodes_service_module, "GroupNode", FakeGroupNode)
    monkeypatch.setattr(service, "_user_group_ids", user_group_ids)
    monkeypatch.setattr(service, "_live_node_map_ex", live_node_map_ex)
    monkeypatch.setattr(service, "_attach_editor_occupancy", attach_editor_occupancy)

    result = await service.list_my_nodes("22222222-2222-2222-2222-222222222222")

    assert len(result["nodes"]) == 1
    assert result["nodes"][0]["capabilities"] == live["capabilities"]
    assert result["control_plane_online"] is True


@pytest.mark.asyncio
async def test_list_my_nodes_reports_control_plane_offline_when_node_service_is_down(monkeypatch) -> None:
    """控制面不可达时节点列表是降级快照，必须如实上报 control_plane_online=false，
    好让 C 端把「查不到节点」归因为节点服务离线，而不是断言节点不存在。"""
    service = NodesService()
    link = _link("node-1")

    async def user_group_ids(_user_id: str) -> list[uuid.UUID]:
        return [link.group_id]

    async def live_node_map_ex() -> tuple[dict[str, dict], bool]:
        return {}, False

    async def attach_editor_occupancy(_live_map: dict[str, dict]) -> None:
        return None

    class FakeGroupNode:
        @staticmethod
        def filter(**_kwargs) -> _OrderedLinks:
            return _OrderedLinks([link])

    monkeypatch.setattr(nodes_service_module, "GroupNode", FakeGroupNode)
    monkeypatch.setattr(service, "_user_group_ids", user_group_ids)
    monkeypatch.setattr(service, "_live_node_map_ex", live_node_map_ex)
    monkeypatch.setattr(service, "_attach_editor_occupancy", attach_editor_occupancy)

    result = await service.list_my_nodes("22222222-2222-2222-2222-222222222222")

    # 直接绑定的节点仍会以 unknown 状态返回（本地绑定行），但降级标志必须为 false。
    assert len(result["nodes"]) == 1
    assert result["nodes"][0]["status"] == "unknown"
    assert result["control_plane_online"] is False


class _DeleteQuery:
    def __init__(self, owner, criteria: dict) -> None:
        self.owner = owner
        self.criteria = criteria

    async def delete(self) -> int:
        self.owner.deleted.append(self.criteria)
        return 1


@pytest.mark.asyncio
async def test_delete_node_removes_local_group_bindings(monkeypatch) -> None:
    service = NodesService()

    class FakeClient:
        async def delete_node(self, node_id: str) -> dict:
            return {"deleted": node_id == "node-1"}

    class FakeGroupNode:
        deleted: list[dict] = []

        @classmethod
        def filter(cls, **criteria):
            return _DeleteQuery(cls, criteria)

    monkeypatch.setattr(nodes_service_module, "get_local_node_client", lambda: FakeClient())
    monkeypatch.setattr(nodes_service_module, "GroupNode", FakeGroupNode)

    result = await service.delete_node("node-1")

    assert result == {"deleted": True}
    assert FakeGroupNode.deleted == [{"node_id": "node-1"}]


@pytest.mark.asyncio
async def test_reconcile_bindings_deletes_orphans_and_refreshes_snapshots(monkeypatch) -> None:
    service = NodesService()
    orphan = _link("old-node")
    orphan.id = uuid.uuid4()
    current = _link("node-1")
    current.id = uuid.uuid4()
    saved_fields: list[list[str]] = []

    async def save(*, update_fields: list[str]) -> None:
        saved_fields.append(update_fields)

    current.save = save

    class FakeClient:
        async def list_nodes(self) -> list[dict]:
            return [_live_node("node-1", role="management")]

    class FakeGroupNode:
        deleted: list[dict] = []

        @staticmethod
        async def all():
            return [orphan, current]

        @classmethod
        def filter(cls, **criteria):
            return _DeleteQuery(cls, criteria)

    monkeypatch.setattr(nodes_service_module, "get_local_node_client", lambda: FakeClient())
    monkeypatch.setattr(nodes_service_module, "GroupNode", FakeGroupNode)

    result = await service.reconcile_bindings()

    assert result == {"skipped": False, "deleted": 1, "updated": 1}
    assert FakeGroupNode.deleted == [{"id": orphan.id}]
    assert current.node_role == "management"
    assert current.node_name == "live-node-1"
    assert saved_fields == [["node_role", "node_name"]]


@pytest.mark.asyncio
async def test_reconcile_bindings_never_deletes_when_control_plane_is_unavailable(monkeypatch) -> None:
    service = NodesService()

    class FakeClient:
        async def list_nodes(self) -> list[dict]:
            raise NodeServerUnavailable("offline")

    class FakeGroupNode:
        touched = False

        @staticmethod
        async def all():
            FakeGroupNode.touched = True
            return []

    monkeypatch.setattr(nodes_service_module, "get_local_node_client", lambda: FakeClient())
    monkeypatch.setattr(nodes_service_module, "GroupNode", FakeGroupNode)

    result = await service.reconcile_bindings()

    assert result == {"skipped": True, "deleted": 0, "updated": 0}
    assert FakeGroupNode.touched is False
