"""市场候选池技术栈 facet（stack/stack_tags）的口径测试。

覆盖的是产品硬约束：
* 探针把识别结果写进 item.stack/stack_tags（喂已抓 files，零新增网络）
* 识别失败绝不拖垮探针主链路（只记 probe.stack_error）
* upsert 落 stack/stack_tags 列，冲突时随 external_data 一并刷新
* 管理端/消费端 stack_tag 过滤都走 stack_tags @> jsonb（GIN 索引同列）
"""
from __future__ import annotations

import json
import sys

import pytest

_proj = "/".join(__file__.replace("\\", "/").split("/")[:-2])
if _proj not in sys.path:
    sys.path.insert(0, _proj)
sys.path.insert(0, _proj + "/server")

import monkeycode_compat.marketplace.leaderboard_probe as probe_mod  # noqa: E402


# ── attach_probe：stack 识别随探针顺带产出 ──────────────────────────────────

def _base_item(**kw):
    item = {
        "repo_full_name": "o/r",
        "board": "mcp",
        "external_data": {"repo_full_name": "o/r"},
        "target_module": "mcp",
        "target_modules": ["mcp"],
        "installable": True,
    }
    item.update(kw)
    return item


async def _fake_probe(result):
    async def fake(full_name, ref="main"):
        return result
    return fake


@pytest.mark.asyncio
async def test_attach_probe_writes_stack_from_fetched_files(monkeypatch):
    """探针已抓的 package.json + 树路径喂引擎：语言分布+框架命中，零新增网络。"""
    pr = {
        "ref": "main", "fetched_at": "t", "tree_count": 3, "truncated": False,
        "blob_paths": ["package.json", "src/main.ts", "src/util.ts", "README.md"],
        "tree_paths": ["package.json", "README.md"],
        "files": {"package.json": {"name": "x", "dependencies": {"react": "18"}}},
        "hints": {}, "error": "",
    }
    monkeypatch.setattr(probe_mod, "probe_repo_shape", await _fake_probe(pr))
    item = await probe_mod.attach_probe(_base_item())
    assert item["stack"]["primary_language"] == "typescript"
    assert "react" in item["stack"]["frameworks"]
    # stack_tags = 主语言 + 框架 + 形态的扁平投影（小写）。
    assert "typescript" in item["stack_tags"]
    assert "react" in item["stack_tags"]


@pytest.mark.asyncio
async def test_attach_probe_pyproject_text_passthrough(monkeypatch):
    """files 里存原文的 pyproject.toml 直接可用；scripts → cli 形态。"""
    pr = {
        "ref": "main", "fetched_at": "t", "tree_count": 2, "truncated": False,
        "blob_paths": ["pyproject.toml", "main.py"],
        "tree_paths": ["pyproject.toml"],
        "files": {
            "pyproject.toml": "[project]\nname='x'\n[project.scripts]\nx='x:main'\n"
        },
        "hints": {}, "error": "",
    }
    monkeypatch.setattr(probe_mod, "probe_repo_shape", await _fake_probe(pr))
    item = await probe_mod.attach_probe(_base_item())
    assert item["stack"]["primary_language"] == "python"
    assert "cli" in item["stack"]["project_types"]
    assert "python" in item["stack_tags"]


@pytest.mark.asyncio
async def test_attach_probe_truncated_hint_passthrough(monkeypatch):
    """树被截断时 profile.truncated 必须亮——宁警告勿静默漏判。"""
    pr = {
        "ref": "main", "fetched_at": "t", "tree_count": 5, "truncated": True,
        "blob_paths": ["a.py"], "tree_paths": [], "files": {}, "hints": {}, "error": "",
    }
    monkeypatch.setattr(probe_mod, "probe_repo_shape", await _fake_probe(pr))
    item = await probe_mod.attach_probe(_base_item())
    assert item["stack"]["truncated"] is True


@pytest.mark.asyncio
async def test_attach_probe_stack_failure_does_not_break_probe(monkeypatch):
    """识别抛异常 → 只记 probe.stack_error；spec 派生照常、item 照常返回。"""
    pr = {
        "ref": "main", "fetched_at": "t", "tree_count": 1, "truncated": False,
        "blob_paths": ["SKILL.md"], "tree_paths": ["SKILL.md"],
        "files": {"SKILL.md": "---\nname: x\n---\nbody"},
        "hints": {}, "error": "",
    }
    monkeypatch.setattr(probe_mod, "probe_repo_shape", await _fake_probe(pr))

    async def boom(*_a, **_k):
        raise RuntimeError("engine exploded")

    import monkeycode_compat.stack_detector as sd
    monkeypatch.setattr(sd, "detect_stack", boom)
    # attach_probe 里是函数内 import，改模块属性即可命中。
    item = await probe_mod.attach_probe(_base_item())
    assert "stack" not in item or item.get("stack") in (None, {})
    probe = item["external_data"]["probe"]
    assert "engine exploded" in probe["stack_error"]
    # 主链路没断：skill entries 仍派生出来。
    assert "skill" in (item.get("install_spec") or {})


@pytest.mark.asyncio
async def test_attach_probe_no_repo_name_is_noop():
    assert await probe_mod.attach_probe({"repo_full_name": ""}) == {"repo_full_name": ""}


# ── store：stack/stack_tags 落库与过滤 ──────────────────────────────────────

class _FakeConn:
    """照 tests/test_marketplace_store.py 的模式：记录 SQL + 参数。"""

    def __init__(self):
        self.sqls: list[str] = []
        self.args: list[tuple] = []

    async def fetchrow(self, sql, *args):
        self.sqls.append(sql)
        self.args.append(args)
        return {"id": 1}

    async def fetch(self, sql, *args):
        self.sqls.append(sql)
        self.args.append(args)
        return []


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        conn = self._conn

        class _Ctx:
            async def __aenter__(self_inner):
                return conn

            async def __aexit__(self_inner, *exc):
                return None
        return _Ctx()


@pytest.fixture
def fake_pool(monkeypatch):
    import db as db_mod

    conn = _FakeConn()
    monkeypatch.setattr(db_mod.PostgresClient, "pool", _FakePool(conn))
    return conn


def test_upsert_item_persists_stack_and_tags(fake_pool):
    import asyncio

    import marketplace_leaderboard_store as store

    asyncio.run(store.upsert_item({
        "board": "mcp", "repo_full_name": "o/r", "source": "agent-leaderboard",
        "stack": {"primary_language": "python", "frameworks": ["fastapi"]},
        "stack_tags": ["python", "fastapi", "web_backend"],
    }))
    sql = fake_pool.sqls[-1]
    assert "stack" in sql and "stack_tags" in sql
    # 冲突更新也刷新 stack（它是仓库派生事实，不是人工资源字段）。
    assert sql.count("stack_tags = EXCLUDED.stack_tags") == 1
    # 参数里的 jsonb 序列化正确（stack/stack_tags 紧跟 external_data 之后）。
    args = fake_pool.args[-1]
    assert json.loads(args[-8])["primary_language"] == "python"
    assert json.loads(args[-7]) == ["python", "fastapi", "web_backend"]


def test_list_items_stack_tag_filter(fake_pool):
    import asyncio

    import marketplace_leaderboard_store as store

    asyncio.run(store.list_items(stack_tag="python"))
    sql = fake_pool.sqls[-1]
    assert "stack_tags @> " in sql and "::jsonb" in sql
    assert '["python"]' in list(fake_pool.args[-1])


def test_list_items_without_stack_tag_no_clause(fake_pool):
    import asyncio

    import marketplace_leaderboard_store as store

    asyncio.run(store.list_items())
    assert "stack_tags" not in fake_pool.sqls[-1]


def test_list_published_for_consumer_stack_tag_filter(fake_pool):
    import asyncio

    import marketplace_leaderboard_store as store

    asyncio.run(store.list_published_for_consumer(stack_tag="web_frontend"))
    sql = fake_pool.sqls[-1]
    # 消费端同口径：stack_tags @>，且 SELECT 投影带 stack/stack_tags。
    assert "stack_tags @> " in sql
    assert "stack, stack_tags" in sql
    assert '["web_frontend"]' in list(fake_pool.args[-1])


def test_list_published_for_consumer_pin_status_published(fake_pool):
    import asyncio

    import marketplace_leaderboard_store as store

    asyncio.run(store.list_published_for_consumer(stack_tag="go"))
    assert "status = 'published'" in fake_pool.sqls[-1]
