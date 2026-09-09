from __future__ import annotations

import pytest
import pytest_asyncio


@pytest_asyncio.fixture
async def db():
    from tortoise import Tortoise

    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"user_platform": ["user_platform.models_team_admin"]},
        use_tz=False,
    )
    await Tortoise.generate_schemas()
    try:
        yield
    finally:
        await Tortoise.close_connections()


@pytest.mark.asyncio
async def test_node_policy_upsert_is_idempotent_and_normalized(db):
    from user_platform import shell_approval_service as service

    first = await service.upsert_node_policy("node-a", "Git:Reset", "posix", "first")
    second = await service.upsert_node_policy("node-a", "git:reset", "posix", "second")

    assert first["command_key"] == "git:reset"
    assert second["note"] == "second"
    rows = await service.list_node_policy("node-a")
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_node_and_shell_scopes_are_isolated(db):
    from user_platform import shell_approval_service as service

    await service.upsert_node_policy("node-a", "rm", "posix")
    await service.upsert_node_policy("node-a", "remove-item", "powershell")
    await service.upsert_node_policy("node-b", "git:reset", "posix")

    assert await service.list_node_auto_allow_keys("node-a", "posix") == {"rm"}
    assert await service.list_node_auto_allow_keys("node-a", "powershell") == {"remove-item"}
    assert await service.list_node_auto_allow_keys("node-b", "posix") == {"git:reset"}
    assert await service.list_node_auto_allow_keys("node-b", "powershell") == set()


@pytest.mark.asyncio
async def test_clear_shell_and_clear_node_do_not_cross_scopes(db):
    from user_platform import shell_approval_service as service

    await service.upsert_node_policy("node-a", "rm", "posix")
    await service.upsert_node_policy("node-a", "remove-item", "powershell")
    await service.upsert_node_policy("node-b", "rm", "posix")

    assert await service.clear_node_policy("node-a", "posix") == 1
    assert await service.list_node_auto_allow_keys("node-a", "posix") == set()
    assert await service.list_node_auto_allow_keys("node-a", "powershell") == {"remove-item"}
    assert await service.list_node_auto_allow_keys("node-b", "posix") == {"rm"}

    assert await service.clear_node_policy("node-a") == 1
    assert await service.list_node_policy("node-a") == []
    assert await service.list_node_auto_allow_keys("node-b", "posix") == {"rm"}


@pytest.mark.asyncio
async def test_invalid_shell_is_rejected(db):
    from user_platform import shell_approval_service as service

    with pytest.raises(ValueError, match="shell"):
        await service.upsert_node_policy("node-a", "rm", "bash")
    with pytest.raises(ValueError, match="shell"):
        await service.list_node_auto_allow_keys("node-a", "bash")


def test_shell_flavor_uses_trusted_node_telemetry():
    from user_platform.shell_approval_service import infer_node_shell_flavor

    assert infer_node_shell_flavor({"capabilities": {"os": "windows"}}) == "powershell"
    assert infer_node_shell_flavor({"capabilities": {"os": "linux"}}) == "posix"
    assert infer_node_shell_flavor({}) == "unknown"
