"""Legacy MCP principal token-hash migration tests."""
import asyncio
import hashlib

from db import PostgresClient


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _FakeConnection:
    def __init__(self, rows):
        self.rows = rows
        self.fetch_sql = ""
        self.updates = None
        self.executed = []

    def transaction(self):
        return _Transaction()

    async def fetch(self, sql):
        self.fetch_sql = sql
        return self.rows

    async def executemany(self, sql, values):
        self.updates = (sql, values)

    async def execute(self, sql):
        self.executed.append(sql)


def test_mcp_user_token_hash_migration_uses_python_sha256():
    conn = _FakeConnection([
        {"id": 7, "token": "mcp_short"},
        {"id": 9, "token": "mcp_1234567890abcdef"},
    ])

    asyncio.run(PostgresClient._migrate_mcp_user_token_hashes(conn))

    assert "token_hash IS NULL" in conn.fetch_sql
    assert conn.updates == (
        "UPDATE mcp_users SET token_hash=$1, token_hint=$2 WHERE id=$3",
        [
            (hashlib.sha256(b"mcp_short").hexdigest(), "mcp_shor...hort", 7),
            (
                hashlib.sha256(b"mcp_1234567890abcdef").hexdigest(),
                "mcp_1234...cdef",
                9,
            ),
        ],
    )
    assert len(conn.executed) == 1
    assert "mcp_users_token_hashed" in conn.executed[0]
    assert "digest(" not in conn.fetch_sql.lower()
    assert "digest(" not in conn.updates[0].lower()


def test_mcp_user_token_hash_migration_marks_empty_batch_complete():
    conn = _FakeConnection([])

    asyncio.run(PostgresClient._migrate_mcp_user_token_hashes(conn))

    assert conn.updates is None
    assert len(conn.executed) == 1
    assert "mcp_users_token_hashed" in conn.executed[0]
