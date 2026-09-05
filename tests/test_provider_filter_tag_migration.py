import asyncio
from contextlib import asynccontextmanager

from db import PostgresClient


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Connection:
    def __init__(self, *, applied=False):
        self.applied = applied
        self.commands: list[tuple[str, tuple]] = []

    def transaction(self):
        return _Transaction()

    async def execute(self, sql, *args):
        self.commands.append((" ".join(sql.split()), args))
        if sql.lstrip().startswith("UPDATE api_keys"):
            return "UPDATE 2"
        if sql.lstrip().startswith("UPDATE model_groups"):
            return "UPDATE 3"
        return "SELECT 1" if sql.lstrip().startswith("SELECT") else "INSERT 0 1"

    async def fetchval(self, sql, *args):
        self.commands.append((" ".join(sql.split()), args))
        return 1 if self.applied else None


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    @asynccontextmanager
    async def acquire(self):
        yield self.conn


def test_provider_filter_migration_clears_once_and_records_flag(monkeypatch):
    conn = _Connection()
    monkeypatch.setattr(PostgresClient, "pool", _Pool(conn))

    result = asyncio.run(PostgresClient.migrate_provider_filters_to_tags())

    assert result == {"applied": True, "api_keys": 2, "model_groups": 3}
    statements = [sql for sql, _ in conn.commands]
    assert any("pg_advisory_xact_lock" in sql for sql in statements)
    assert any(sql.startswith("UPDATE api_keys") for sql in statements)
    group_sql = next(sql for sql in statements if sql.startswith("UPDATE model_groups"))
    assert "jsonb_array_elements" in group_sql
    assert "provider_whitelist" in group_sql and "provider_blacklist" in group_sql
    assert any(sql.startswith("INSERT INTO schema_migrations") for sql in statements)


def test_provider_filter_migration_skips_after_flag(monkeypatch):
    conn = _Connection(applied=True)
    monkeypatch.setattr(PostgresClient, "pool", _Pool(conn))

    result = asyncio.run(PostgresClient.migrate_provider_filters_to_tags())

    assert result == {"applied": False, "api_keys": 0, "model_groups": 0}
    statements = [sql for sql, _ in conn.commands]
    assert not any(sql.startswith("UPDATE api_keys") for sql in statements)
    assert not any(sql.startswith("UPDATE model_groups") for sql in statements)
    assert not any(sql.startswith("INSERT INTO schema_migrations") for sql in statements)
