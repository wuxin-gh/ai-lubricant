"""外部榜单同步与候选池发布的口径测试。

覆盖的是产品硬约束，不是实现细节：
* 同步默认关闭（opt-in），未配置的部署不跑
* 归类按「安装形态」而非上游 board
* 仅浏览条目不可发布
* 批量发布的 published/skipped/failed 语义
* agent 补全只写草稿，绝不改发布状态
"""
from __future__ import annotations

import user_platform.marketplace.leaderboard_sync as sync
import user_platform.marketplace.leaderboard_launch_agent as launch_agent
from user_platform.marketplace.source_config import _normalize


# ── 配置：默认关闭 ────────────────────────────────────────────────────────────


def test_leaderboard_sync_disabled_by_default():
    """没配过的部署必须是关的——绝大多数部署不该去抓外部榜单。"""
    cfg = _normalize({})
    assert cfg["leaderboard_sync_enabled"] is False
    # 专用 agent 配置已移除（探针替代）；确认字段不再出现
    assert "leaderboard_launch_agent_id" not in cfg


def test_leaderboard_sync_enabled_only_when_explicitly_true():
    assert _normalize({"leaderboard_sync_enabled": True})["leaderboard_sync_enabled"] is True
    # 字符串/1 之类的含糊值不算开启，避免误配就开始抓
    assert _normalize({"leaderboard_sync_enabled": "true"})["leaderboard_sync_enabled"] is False
    assert _normalize({"leaderboard_sync_enabled": 1})["leaderboard_sync_enabled"] is False


def test_interval_hours_clamped():
    # 0/空 视为未配置，回落默认 24h（与其它 marketplace 配置项一致的口径）
    assert _normalize({"leaderboard_sync_interval_hours": 0})["leaderboard_sync_interval_hours"] == 24
    assert _normalize({})["leaderboard_sync_interval_hours"] == 24
    # 显式值按上下界收敛，防止误配成每分钟抓或几年一次
    assert _normalize({"leaderboard_sync_interval_hours": -5})["leaderboard_sync_interval_hours"] == 1
    assert _normalize({"leaderboard_sync_interval_hours": 99999})["leaderboard_sync_interval_hours"] == 720
    assert _normalize({"leaderboard_sync_interval_hours": 24})["leaderboard_sync_interval_hours"] == 24


def test_resolve_boards_filters_unknown():
    boards = sync.resolve_boards({"leaderboard_boards": "skills,mcp,bogus"})
    assert boards == ["skills", "mcp"]
    # 留空回落内置默认（全同步五个）
    assert sync.resolve_boards({}) == ["skills", "mcp", "prompts", "frameworks", "research"]


# ── 归类：按安装形态，不按 board ──────────────────────────────────────────────


def test_awesome_directory_is_browse_only():
    """awesome 列表是一篇 README，不是可装物——即便在 mcp 榜里也不可安装。"""
    verdict = sync.classify("mcp", {
        "full_name": "punkpeye/awesome-mcp-servers",
        "description": "A collection of MCP servers.",
    })
    assert verdict["installable"] is False
    assert verdict["target_module"] == ""
    assert verdict["target_modules"] == []


def test_frameworks_and_research_are_browse_only():
    """langchain 这类开发库不是给编辑器装的扩展，四类里没有它的位置。"""
    for board in ("frameworks", "research"):
        verdict = sync.classify(board, {"full_name": "langchain-ai/langchain", "description": "LLM framework"})
        assert verdict["installable"] is False, board
        assert verdict["target_module"] == "", board


def test_real_mcp_server_gets_mcp_module():
    verdict = sync.classify("mcp", {
        "full_name": "github/github-mcp-server",
        "description": "GitHub's official MCP Server",
    })
    assert verdict["target_module"] == "mcp"
    assert verdict["target_modules"] == ["mcp"]
    assert verdict["installable"] is True


def test_skills_board_maps_to_skill_module():
    """skills 榜一比一归为 skill——上游榜单就是类型，不再留空让管理员手填。"""
    verdict = sync.classify("skills", {
        "full_name": "obra/superpowers",
        "description": "An agentic skills framework & software development methodology that works.",
    })
    assert verdict["target_module"] == "skill"
    assert verdict["installable"] is True


def test_prompts_board_maps_to_prompt_module():
    verdict = sync.classify("prompts", {
        "full_name": "someone/awesome-prompt-pack",
        "description": "A curated prompt.",
    })
    # 注意：含 awesome 目录特征的会先被判为仅浏览，这里用不含该特征的样本。
    verdict2 = sync.classify("prompts", {"full_name": "someone/prompt-pack", "description": "prompt"})
    assert verdict2["target_module"] == "prompt"
    assert verdict2["installable"] is True


def test_to_item_maps_upstream_fields():
    item = sync.to_item("mcp", {
        "full_name": "github/github-mcp-server",
        "url": "https://github.com/github/github-mcp-server",
        "description": "GitHub's official MCP Server",
        "stars": 29976,
        "forks": 100,
        "language": "Go",
        "topics": ["mcp"],
        "category": "official",
        "use_cases": ["代码工具"],
        "updated_at": "2026-08-26T02:50:48Z",
    }, rank=3)
    assert item is not None
    assert item["repo_full_name"] == "github/github-mcp-server"
    assert item["stars"] == 29976
    assert item["target_module"] == "mcp"
    assert item["upstream_rank"] == 3
    assert item["upstream_updated_at"] is not None


def test_to_item_rank_defaults_to_none():
    """不传 rank（旧调用方）不得硬塞 0——0 会被当成「榜首」参与排序。"""
    item = sync.to_item("skills", {"full_name": "obra/superpowers", "description": "skills"})
    assert item is not None
    assert item["upstream_rank"] is None


def test_to_item_prefills_install_spec_and_external_data():
    """同步即建完整草稿：external_data 先落上游原文，再派生全部资源字段。"""
    item = sync.to_item("skills", {
        "full_name": "obra/superpowers",
        "description": "An agentic skills framework.",
        "stars": 1200,
        "topics": ["skill", "agent"],
        "category": "skill",
        "use_cases": ["开发流程"],
    }, rank=1)
    assert item is not None
    assert item["target_modules"] == ["skill"]
    # skill 预填 github_clone 默认
    assert item["install_spec"]["skill"]["install_method"] == "github_clone"
    assert item["install_spec"]["skill"]["ref"] == "main"
    # external_data 是唯一上游真相：含 source/原文
    assert item["external_data"]["source"] == "agent-leaderboard"
    assert item["external_data"]["board"] == "skills"
    assert item["external_data"]["repo_full_name"] == "obra/superpowers"
    assert item["external_data"]["stars"] == 1200
    assert item["external_data"]["raw"]["category"] == "skill"
    # 资源字段按确认口径派生：use_cases→子分类、topics→标签、名称/发布者/排序
    assert item["categories"] == ["开发流程"]
    assert item["tags"] == ["skill", "agent"]
    assert item["name"] == "superpowers"
    assert item["display_name"] == "superpowers"
    assert item["publisher"] == "obra"
    assert item["sort_order"] == 1
    assert item["version"]  # 上游没给更新时间 → latest 兜底
    assert item["version"] == "latest"


def test_to_item_multi_module_prefills_both_install_specs():
    """一条既是 skill 又是 plugin：两个安装配置都预填。"""
    item = sync.to_item("skills", {
        "full_name": "a/b", "description": "x", "category": "skill plugin",
    })
    assert item is not None
    assert set(item["target_modules"]) == {"skill", "plugin"}
    assert "skill" in item["install_spec"]
    assert item["install_spec"]["plugin"]["download_url"].endswith("/archive/refs/heads/main.zip")


def test_browse_only_item_has_empty_install_spec():
    """仅浏览（frameworks）无分类，不预填安装参数。"""
    item = sync.to_item("frameworks", {"full_name": "langchain-ai/langchain", "description": "LLM framework"})
    assert item is not None
    assert item["target_modules"] == []
    assert item["install_spec"] == {}


def test_manual_item_source_and_classification():
    """手动添加：source=manual、board=manual，按 topics/描述预分类。"""
    meta = {
        "html_url": "https://github.com/owner/repo",
        "description": "an mcp server",
        "stargazers_count": 42,
        "forks_count": 3,
        "language": "Python",
        "topics": ["mcp"],
        "updated_at": "2026-08-26T02:50:48Z",
    }
    item = sync.manual_item("owner/repo", meta)
    assert item is not None
    assert item["source"] == "manual"
    assert item["board"] == "manual"
    assert item["repo_full_name"] == "owner/repo"
    assert item["stars"] == 42
    # topics 拼进 category → 命中 mcp
    assert "mcp" in item["target_modules"]


def test_manual_item_rejects_malformed():
    assert sync.manual_item("no-slash", {}) is None


def test_to_item_rejects_malformed_repo():
    assert sync.to_item("mcp", {"full_name": ""}) is None
    assert sync.to_item("mcp", {"full_name": "no-slash"}) is None


# ── agent 补全：只写草稿 ──────────────────────────────────────────────────────


def test_normalize_spec_stdio():
    spec = launch_agent.normalize_spec({
        "kind": "stdio", "command": "npx", "args": ["-y", "server"],
        "env": ["API_KEY"], "confidence": "high", "notes": "ok",
    })
    assert spec["kind"] == "stdio"
    assert spec["command"] == "npx"
    assert spec["args"] == ["-y", "server"]
    assert spec["env"] == ["API_KEY"]


def test_normalize_spec_repository_info():
    """批量识别扩展：描述/子分类/tags 收敛后保留，非法形态不影响仓库信息。"""
    spec = launch_agent.normalize_spec({
        "kind": "none",
        "description": "  一段中文介绍  ",
        "categories": ["代码辅助", "研发流程", ""],
        "tags": ["agent", "skill", ""],
    })
    assert spec["description"] == "一段中文介绍"
    assert spec["categories"] == ["代码辅助", "研发流程"]
    assert spec["tags"] == ["agent", "skill"]


def test_normalize_spec_remote_rejects_non_https_and_bad_transport():
    spec = launch_agent.normalize_spec({
        "kind": "remote", "url": "http://insecure.example", "transport": "websocket",
    })
    # 非 HTTPS 与非法 transport 一律丢弃，不硬塞进库
    assert spec["url"] == ""
    assert spec["transport"] == ""


def test_normalize_spec_unknown_kind_becomes_none():
    assert launch_agent.normalize_spec({"kind": "magic"})["kind"] == "none"
    assert launch_agent.normalize_spec({})["kind"] == "none"


def test_is_usable_requires_concrete_launch_info():
    assert launch_agent._is_usable({"kind": "stdio", "command": "npx"}) is True
    assert launch_agent._is_usable({"kind": "stdio", "command": ""}) is False
    assert launch_agent._is_usable({"kind": "remote", "url": "https://x", "transport": "sse"}) is True
    assert launch_agent._is_usable({"kind": "remote", "url": "https://x", "transport": ""}) is False
    assert launch_agent._is_usable({"kind": "none"}) is False


def test_extract_json_from_fenced_and_bare_output():
    fenced = [{"content": 'blah\n```json\n{"kind":"stdio","command":"npx"}\n```\ntail'}]
    assert launch_agent._extract_json(fenced)["command"] == "npx"
    bare = [{"content": '{"kind":"remote","url":"https://a","transport":"sse"}'}]
    assert launch_agent._extract_json(bare)["kind"] == "remote"
    assert launch_agent._extract_json([{"content": "no json here"}]) is None
    assert launch_agent._extract_json([]) is None


def test_launch_agent_requires_configured_agent(monkeypatch):
    """未配置专用 agent 时必须拒绝，而不是随便找个 agent 顶上。"""
    import asyncio

    async def fake_config():
        return {"leaderboard_launch_agent_id": 0}

    monkeypatch.setattr(launch_agent, "get_source_config_async", fake_config)
    try:
        asyncio.run(launch_agent.resolve_agent_id())
    except launch_agent.LaunchAgentUnavailable as exc:
        assert "专用 agent" in str(exc)
    else:
        raise AssertionError("未配置 agent 时应拒绝")


# ── 发布：批量语义 + 仅浏览拦截 ───────────────────────────────────────────────


class _FakeConn:
    """最小 PG 桩：只支持 publish_items 用到的 fetchrow/execute。"""

    def __init__(self, rows: dict[int, dict]):
        self.rows = rows
        self.executed: list[tuple] = []

    async def fetchrow(self, sql, *args):
        return self.rows.get(args[0])

    async def execute(self, sql, *args):
        self.executed.append((sql, args))
        item_id = args[0]
        if item_id in self.rows:
            self.rows[item_id]["status"] = "published"
        return "UPDATE 1"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return self._conn


def _publish(rows: dict[int, dict], ids: list[int]):
    import asyncio
    import marketplace_leaderboard_store as store
    from db import PostgresClient

    conn = _FakeConn(rows)
    original = PostgresClient.pool
    PostgresClient.pool = _FakePool(conn)
    try:
        result = asyncio.run(store.publish_items(ids, operator="tester"))
        # execute 入参 [id, operator, target_module]；返回前记录，供测试断言
        # 「随发布落库的形态」——row 桩是普通 dict，execute 里不会真改它。
        result["_execute_args"] = [args for _, args in conn.executed]
        return result
    finally:
        PostgresClient.pool = original


def test_publish_allows_browse_only_items():
    """发布=可见,与能不能装是两回事。仅浏览条目有发现价值,照样能上架。"""
    rows = {1: {"id": 1, "status": "draft", "installable": False, "target_module": "", "board": "research", "upstream_category": ""}}
    result = _publish(rows, [1])
    assert result["published"] == [1]
    assert result["failed"] == []


def test_publish_derives_module_from_board_when_missing():
    """空归类不再拦发布:external_data/board 已隐含安装形态,就地推出并随发布落库。

    覆盖两类场景:board 直接给出形态(mcp 榜→mcp),以及榜单类型打底 + category 追加
    (skills 榜 + category 含 plugin → skill+plugin，主分类=榜单类型 skill)。
    """
    rows = {
        1: {"id": 1, "status": "draft", "installable": True,
            "target_module": "", "board": "mcp", "upstream_category": ""},
        2: {"id": 2, "status": "draft", "installable": True,
            "target_module": "", "board": "skills", "upstream_category": "editor plugin"},
    }
    result = _publish(rows, [1, 2])
    assert sorted(result["published"]) == [1, 2]
    assert result["failed"] == []
    assert result["_execute_args"][0][2] == "mcp"    # board 推出
    assert result["_execute_args"][1][2] == "skill"  # 榜单类型打底=主分类
    import json as _json
    assert _json.loads(result["_execute_args"][1][3]) == ["skill", "plugin"]


def test_publish_rejects_installable_but_unclassified():
    """声明可装且上游确实无类型线索(frameworks/research/awesome),才要人工收口。"""
    rows = {1: {"id": 1, "status": "draft", "installable": True,
                "target_module": "", "board": "frameworks", "upstream_category": "library"}}
    result = _publish(rows, [1])
    assert result["published"] == []
    assert "分类" in result["failed"][0]["error"]


def test_publish_batch_mixed_outcomes():
    """批量里的失败不影响其余条目,且已发布的算幂等 skipped。"""
    rows = {
        1: {"id": 1, "status": "draft", "installable": True, "target_module": "mcp"},
        2: {"id": 2, "status": "published", "installable": True, "target_module": "skill"},
        3: {"id": 3, "status": "draft", "installable": False, "target_module": ""},
        4: {"id": 4, "status": "draft", "installable": True, "target_module": "plugin"},
    }
    # 5 可装但无形态,且 board 是 frameworks(推不出形态)——被拦下要求人工。
    rows[5] = {"id": 5, "status": "draft", "installable": True,
               "target_module": "", "board": "frameworks", "upstream_category": ""}
    result = _publish(rows, [1, 2, 3, 4, 5, 999])
    # 3 是仅浏览条目,可发布;5 声明可装且上游无线索,被拦下
    assert sorted(result["published"]) == [1, 3, 4]
    assert result["skipped"] == [2]
    failed_ids = {f["id"] for f in result["failed"]}
    assert failed_ids == {5, 999}


def test_publish_missing_item_reported_not_raised():
    result = _publish({}, [42])
    assert result["published"] == []
    assert result["failed"][0]["id"] == 42


# ── 删除：连带清掉孤儿推送 job ─────────────────────────────────────────────────


class _DeleteConn:
    """delete_items 桩：记下事务内执行的两条 DELETE（job 清理 + 行删除），fetch 回行 id。"""

    def __init__(self):
        self.executed: list[tuple] = []

    async def execute(self, sql, *args):
        self.executed.append((sql, args))
        return "DELETE 0"

    async def fetch(self, sql, *args):
        self.executed.append((sql, args))
        # 行 DELETE 的参数是 bigint[]，回这些 id 模拟「都删成功了」。
        ids = args[0] if args and isinstance(args[0], list) else []
        return [{"id": i} for i in ids]

    class _Tx:
        async def __aenter__(self):
            return None

        async def __aexit__(self, *exc):
            return False

    def transaction(self):
        return self._Tx()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def test_delete_items_clears_pending_push_jobs():
    """删除条目要连带清掉 pending/pushing 的榜单推送 job：行都没了，留着 job
    只会让 worker 对「条目不存在」空转重试、刷满失败列表。"""
    import asyncio
    import marketplace_leaderboard_store as store
    from db import PostgresClient

    conn = _DeleteConn()
    original = PostgresClient.pool
    PostgresClient.pool = _FakePool(conn)
    try:
        removed = asyncio.run(store.delete_items([1, 2]))
    finally:
        PostgresClient.pool = original
    assert sorted(removed) == [1, 2]
    job_deletes = [(sql, args) for sql, args in conn.executed if "marketplace_publish_jobs" in sql]
    assert len(job_deletes) == 1, "应清一次孤儿 job"
    sql, args = job_deletes[0]
    assert "module='leaderboard'" in sql
    assert "'pending'" in sql and "'pushing'" in sql
    assert sorted(args[0]) == ["1", "2"]  # 文本 ids（job 表 item_id 是 text）
    row_deletes = [sql for sql, _ in conn.executed if "marketplace_leaderboard_items" in sql and "DELETE" in sql]
    assert len(row_deletes) == 1


# ── 归类校验 ──────────────────────────────────────────────────────────────────


def test_update_curation_rejects_unknown_module():
    import asyncio
    import marketplace_leaderboard_store as store
    from db import PostgresClient

    original = PostgresClient.pool
    PostgresClient.pool = _FakePool(_FakeConn({}))
    try:
        asyncio.run(store.update_curation(1, {"target_module": "nonsense"}))
    except ValueError as exc:
        assert "target_module" in str(exc)
    else:
        raise AssertionError("非法 target_module 应被拒绝")
    finally:
        PostgresClient.pool = original


def test_target_modules_are_exactly_the_four():
    import marketplace_leaderboard_store as store

    assert store.TARGET_MODULES == frozenset({"mcp", "skill", "prompt", "plugin"})


# ── 多选分类：一条可以同时是 mcp + skill + plugin ──────────────────────────────


def test_derive_target_modules_board_mandatory_category_appends():
    """榜单类型必选打底，上游 category 只负责追加（保序去重）。"""
    import marketplace_leaderboard_store as store

    # skills 榜打底 skill，category 里的 mcp 追加 → [skill, mcp]
    assert store.derive_target_modules("skills", "mcp server and skill package") == ["skill", "mcp"]
    # mcp 榜 + category 含 plugin/extension → mcp + plugin（extension 也映射 plugin）
    assert store.derive_target_modules("mcp", "MCP tools extension") == ["mcp", "plugin"]
    assert store.derive_target_modules("mcp", "") == ["mcp"]
    assert store.derive_target_modules("skills", "") == ["skill"]
    # 上游 category 没有可识别形态时榜单类型仍然保留（榜单本身就是类型声明）
    assert store.derive_target_modules("skills", "editor tool") == ["skill"]
    # frameworks/research/manual 没有对应形态且 category 无线索 → 空（仅浏览）
    assert store.derive_target_modules("frameworks", "library") == []


def test_derive_target_module_wrapper_returns_first():
    import marketplace_leaderboard_store as store

    # 榜单类型打底在前 → 第一个是 skill
    assert store.derive_target_module("skills", "mcp and skill") == "skill"
    assert store.derive_target_module("frameworks", "") == ""


def test_update_curation_target_modules_multi_write():
    """多选分类落 target_modules 数组,主分类(target_module)同步成第一个。"""
    conn = _curation({"target_modules": ["skill", "mcp"], "installable": True})
    assert "target_modules = " in conn.sql
    assert "target_module = " in conn.sql
    blob = next((a for a in conn.args if isinstance(a, str) and a.startswith("[") and "skill" in a), None)
    assert blob is not None, "应有 target_modules 的 JSON 数组参数"
    import json as _json
    assert _json.loads(blob) == ["skill", "mcp"]
    # 主分类 = 数组第一个
    primary = conn.args[conn.args.index(blob) + 1]
    assert primary == "skill"


def test_update_curation_target_modules_rejects_unknown():
    try:
        _curation({"target_modules": ["mcp", "nonsense"]})
    except ValueError as exc:
        assert "nonsense" in str(exc)
    else:
        raise AssertionError("非法分类应被拒绝")


def test_update_curation_install_spec_replaces_whole():
    """install_spec 整体替换:skill/plugin/prompt 各自的安装配置写进同一 JSON 列。"""
    import json as _json
    conn = _curation({"install_spec": {"skill": {"path": "skills", "ref": "main"},
                                        "plugin": {"download_url": "https://x/y.tar.gz"}}})
    assert "install_spec = " in conn.sql
    blob = next((a for a in conn.args if isinstance(a, str) and a.startswith("{") and "download_url" in a), None)
    assert blob is not None
    assert _json.loads(blob)["skill"]["ref"] == "main"


def test_classify_multi_module_upstream_prefill():
    """同步预分类：榜单类型（skill）打底，category 追加（mcp）。"""
    verdict = sync.classify("skills", {
        "full_name": "a/b",
        "description": "an mcp server that also ships skills",
        "category": "mcp skill",
    })
    assert verdict["target_modules"] == ["skill", "mcp"]
    assert verdict["target_module"] == "skill"
    assert verdict["installable"] is True
    # 纯 skill
    plain = sync.classify("skills", {"full_name": "a/c", "description": "skills"})
    assert plain["target_modules"] == ["skill"]
    assert plain["installable"] is True


def test_list_filter_targets_module_uses_array_contains():
    """分类过滤改用数组包含(target_modules @>):既是 mcp 又是 skill 的条目在两个 tab 都出现。"""
    import asyncio
    import marketplace_leaderboard_store as store
    from db import PostgresClient

    conn = _FakeConnFetch()

    class _Pool:
        def acquire(self):
            return conn

    original = PostgresClient.pool
    PostgresClient.pool = _Pool()
    try:
        asyncio.run(store.list_items(target_module="mcp"))
    finally:
        PostgresClient.pool = original
    assert "target_modules @>" in conn.last_sql


def test_pending_launch_spec_uses_array_contains():
    """待补清单按数组包含取:分类里带 mcp 的(哪怕主分类是 skill)都要补启动方式。"""
    import asyncio
    import marketplace_leaderboard_store as store
    from db import PostgresClient

    class _Conn(_FakeConnFetch):
        async def fetch(self, sql, *args):
            self.last_sql = sql
            return []

    conn = _Conn()

    class _Pool:
        def acquire(self):
            return conn

    original = PostgresClient.pool
    PostgresClient.pool = _Pool()
    try:
        asyncio.run(store.list_pending_launch_spec())
    finally:
        PostgresClient.pool = original
    assert 'target_modules @> \'["mcp"]\'::jsonb' in conn.last_sql


# ── 手改启动方式：只写人工字段,同步不覆盖 ──────────────────────────────────


class _CurationConn:
    """记下 update_curation 的 UPDATE SQL 与参数,供断言分支是否触发。"""

    def __init__(self):
        self.sql = ""
        self.args: tuple = ()

    async def fetchrow(self, sql, *args):
        self.sql = sql
        self.args = args
        # 返回一个 dict 让 row_to_dict 走通;测试只断言 SQL/参数,不看返回值内容。
        return {"id": args[-1] if args else 1, "status": "draft", "target_module": "mcp"}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _curation(patch: dict):
    import asyncio
    import marketplace_leaderboard_store as store
    from db import PostgresClient

    conn = _CurationConn()
    original = PostgresClient.pool
    PostgresClient.pool = _FakePool(conn)
    try:
        asyncio.run(store.update_curation(1, patch))
    finally:
        PostgresClient.pool = original
    return conn


def test_update_curation_writes_launch_spec_and_marks_filled():
    """手改 launch_spec 要落到 launch_spec 列,并把状态标 filled(人工已核对),
    清掉旧 error——这是 agent 补全与人工核对共用同一列、但互不踩的口径。"""
    conn = _curation({"launch_spec": {"kind": "stdio", "command": "npx"}})
    assert "launch_spec = $" in conn.sql
    assert "launch_spec_status = $" in conn.sql
    assert "launch_spec_error = $" in conn.sql
    # args 顺序:spec(jsonb 字符串), "filled", ""(清 error), id
    assert "filled" in conn.args
    assert any("stdio" in str(a) for a in conn.args)


def test_update_curation_rejects_bad_launch_spec_kind():
    """非法 kind 不该被静默写成 none——那等于把脏数据默默改写行为,与 set_launch_spec 的
    normalize 口径一致:非法即拒绝。"""
    try:
        _curation({"launch_spec": {"kind": "magic"}})
    except ValueError as exc:
        assert "kind" in str(exc)
    else:
        raise AssertionError("非法 launch_spec.kind 应被拒绝")


def test_update_curation_rejects_non_dict_launch_spec():
    try:
        _curation({"launch_spec": "not-an-object"})
    except ValueError as exc:
        assert "launch_spec" in str(exc)
    else:
        raise AssertionError("launch_spec 非对象应被拒绝")


# ── 排序：资源中心索引 ─────────────────────────────────────────────────────────


def test_update_curation_sort_order_writes_index():
    """手动排序写 sort_order（资源中心索引），数字越小越靠前。"""
    conn = _curation({"sort_order": 7})
    assert "sort_order = $" in conn.sql
    assert 7 in conn.args


def test_update_curation_empty_sort_order_clears():
    """清空排序 = NULL，列表排最后再按热度。"""
    conn = _curation({"sort_order": None})
    assert "sort_order = $" in conn.sql
    assert None in conn.args


def test_update_curation_rejects_bad_sort_order():
    """非整数/负数不做静默截断。"""
    for bad in ("abc", 12.5, "12.5", -3):
        try:
            _curation({"sort_order": bad})
        except ValueError:
            pass
        else:
            raise AssertionError(f"非法排序 {bad!r} 应被拒绝")


# ── 编辑不含发布：status 一律忽略，发布走推送队列 ─────────────────────────────


def test_update_curation_ignores_status():
    """编辑保存绝不发布：patch 里的 status 不进 UPDATE 列，published_at 不动。

    「先编辑后推送」的核心口径——发布是显式推送动作（enqueue_publish_jobs →
    publisher worker 门禁 → 翻状态），编辑链路静默吞掉 status 且不报错（旧客户端
    误传也只是保存）。"""
    conn = _curation({"description": "只改描述", "status": "published"})
    assert "description = $" in conn.sql
    assert "status = $" not in conn.sql
    assert "published_at" not in conn.sql


class _PushConn:
    """enqueue_publish_jobs 桩：fetchrow 回行状态，execute 记录 _enqueue 的 INSERT。"""

    def __init__(self, rows: dict[int, str]):
        self.rows = rows  # id -> status
        self.executed: list[tuple] = []

    async def fetchrow(self, sql, *args):
        status = self.rows.get(args[0])
        return None if status is None else {"id": args[0], "status": status}

    async def execute(self, sql, *args):
        self.executed.append((sql, args))
        return "INSERT 0 1"

    class _Tx:
        async def __aenter__(self):
            return None

        async def __aexit__(self, *exc):
            return False

    def transaction(self):
        return self._Tx()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def _enqueue_publish(rows: dict[int, str], ids: list[int]):
    import asyncio
    import marketplace_leaderboard_store as store
    from db import PostgresClient

    conn = _PushConn(rows)
    original = PostgresClient.pool
    PostgresClient.pool = _FakePool(conn)
    try:
        return asyncio.run(store.enqueue_publish_jobs(ids, operator="tester")), conn
    finally:
        PostgresClient.pool = original


def test_enqueue_publish_jobs_skips_published_and_missing():
    """已发布（幂等）与不存在的条目不入队归 skipped，只有草稿入队。"""
    (enqueued, skipped), conn = _enqueue_publish({1: "draft", 2: "published"}, [1, 2, 3])
    assert enqueued == [1]
    assert sorted(skipped) == [2, 3]
    inserts = [(sql, args) for sql, args in conn.executed if "marketplace_publish_jobs" in sql]
    assert len(inserts) == 1
    # 复用市场发布 outbox：module='leaderboard'、action='publish'、payload 带发布人。
    assert "leaderboard" in inserts[0][0] or "leaderboard" in inserts[0][1]


def test_enqueue_publish_jobs_no_pool_yields_all_skipped():
    import asyncio
    import marketplace_leaderboard_store as store
    from db import PostgresClient

    original = PostgresClient.pool
    PostgresClient.pool = None
    try:
        enqueued, skipped = asyncio.run(store.enqueue_publish_jobs([7, 8]))
    finally:
        PostgresClient.pool = original
    assert enqueued == []
    assert sorted(skipped) == [7, 8]


def test_upsert_conflict_updates_only_external_data_projection():
    """同步已存在条目只刷新 external_data 与其列表投影缓存，不碰资源字段。

    ON CONFLICT 中不能出现 name/description/categories/tags/target_modules/install_spec 等
    资源字段赋值；新条目 INSERT 则必须含完整资源字段。external_data 采用 probe 保留规则：
    带新 probe 用新值，不带（探针复用/预算跳过/限流恢复路径）则把行里旧 probe 移植进
    新 external_data。
    """
    import asyncio
    import marketplace_leaderboard_store as store
    from db import PostgresClient

    captured = {}

    class _Conn:
        async def fetchrow(self, sql, *args):
            captured["sql"] = sql
            captured["args"] = args
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    original = PostgresClient.pool
    PostgresClient.pool = _FakePool(_Conn())
    try:
        asyncio.run(store.upsert_item({
            "board": "mcp", "repo_full_name": "a/b", "upstream_rank": 1,
            "name": "b", "version": "2026.09.01", "categories": ["开发"],
            "tags": ["mcp"], "target_modules": ["mcp"],
            "external_data": {"source": "agent-leaderboard", "board": "mcp"},
        }))
    finally:
        PostgresClient.pool = original
    sql = captured["sql"]
    assert "name, display_name, publisher, version, categories, tags" in sql
    conflict = sql.split("ON CONFLICT", 1)[1]
    # probe 保留规则：带 probe 键用新值；没有则移植行里旧 probe（不存在时不动）。
    assert "EXCLUDED.external_data ? 'probe'" in conflict
    assert "jsonb_set(EXCLUDED.external_data, '{probe}'" in conflict
    assert "marketplace_leaderboard_items.external_data->'probe'" in conflict
    assert "name = EXCLUDED.name" not in conflict
    assert "description = EXCLUDED.description" not in conflict
    assert "categories = EXCLUDED.categories" not in conflict
    assert "tags = EXCLUDED.tags" not in conflict
    assert "target_modules =" not in conflict
    assert "install_spec =" not in conflict


def test_load_probe_reuse_hints_returns_lean_projection():
    """复用快照只查轻量投影（probe 健康位/fetched_at/上游 updated_at/stack），不带 probe 本体。"""
    import asyncio
    import marketplace_leaderboard_store as store
    from db import PostgresClient

    captured = {}

    class _Conn:
        async def fetch(self, sql, *args):
            captured["sql"] = sql
            return [
                # asyncpg 回 JSONB 为 str；外部行没探过时 probe_* 为 None。
                {"source": "agent-leaderboard", "board": "skills", "repo_full_name": "a/b",
                 "probe_error": None, "probe_fetched_at": None,
                 "upstream_updated_at": "2026-01-01T00:00:00+00:00",
                 "stack": '{"languages": {"Python": 10}}', "stack_tags": '["python"]'},
                {"source": "agent-leaderboard", "board": "skills", "repo_full_name": "c/d",
                 "probe_error": "", "probe_fetched_at": "2026-09-07T00:00:00+00:00",
                 "upstream_updated_at": None, "stack": None, "stack_tags": None},
            ]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    original = PostgresClient.pool
    PostgresClient.pool = _FakePool(_Conn())
    try:
        hints = asyncio.run(store.load_probe_reuse_hints())
    finally:
        PostgresClient.pool = original
    # 只查投影，不搬 probe 本体（可能数百 KB/条）。
    assert "external_data->'probe'->>'error'" in captured["sql"]
    assert "->'probe'->>'fetched_at'" in captured["sql"]
    assert "->'probe' AS" not in captured["sql"]
    assert hints[("agent-leaderboard", "skills", "a/b")]["stack"] == {"languages": {"Python": 10}}
    assert hints[("agent-leaderboard", "skills", "a/b")]["stack_tags"] == ["python"]
    # JSONB str 已解好；None 探针行照常返回（调用方视作不可复用）。
    assert hints[("agent-leaderboard", "skills", "c/d")]["fetched_at"] == "2026-09-07T00:00:00+00:00"
    assert hints[("agent-leaderboard", "skills", "c/d")]["stack"] == {}


def test_list_orders_by_sort_order_before_stars():
    """列表排序口径：sort_order（资源中心索引）优先于热度，NULL 排最后。"""
    import asyncio
    import marketplace_leaderboard_store as store
    from db import PostgresClient

    conn = _FakeConnFetch()

    class _Pool:
        def acquire(self):
            return conn

    original = PostgresClient.pool
    PostgresClient.pool = _Pool()
    try:
        asyncio.run(store.list_items())
    finally:
        PostgresClient.pool = original
    assert "ORDER BY sort_order ASC NULLS LAST, stars DESC, id ASC" in conn.last_sql


# ── 资源字段：直接写列（同步已存在条目只更新 external_data） ────────────────


def test_update_curation_writes_resource_columns_directly():
    """description/categories/tags 是资源字段，编辑直接写列；同步不会再冲掉。"""
    conn = _curation({
        "description": "管理员改写的描述",
        "categories": ["代码辅助", "研发流程"],
        "tags": ["agent", "skill"],
    })
    assert "description = $" in conn.sql
    assert "categories = $" in conn.sql
    assert "tags = $" in conn.sql
    assert "admin_overrides" not in conn.sql
    import json as _json
    json_args = [_json.loads(a) for a in conn.args if isinstance(a, str) and a.startswith("[")]
    assert ["代码辅助", "研发流程"] in json_args
    assert ["agent", "skill"] in json_args


def test_update_curation_empty_description_writes_empty_column():
    conn = _curation({"description": ""})
    assert "description = $" in conn.sql
    assert "" in conn.args


def test_update_curation_rejects_non_list_categories_tags():
    for field in ("categories", "tags"):
        try:
            _curation({field: "not-a-list"})
        except ValueError as exc:
            assert field in str(exc)
        else:
            raise AssertionError(f"{field} 非数组应被拒绝")


def test_apply_overrides_shadows_upstream_values():
    """读取侧:admin_overrides 里的值盖在上游原值上,没覆盖的字段保持上游值。"""
    import marketplace_leaderboard_store as store

    row = {
        "description": "上游描述",
        "language": "Go",
        "topics": ["x"],
        "admin_overrides": {"description": "改过的", "topics": ["y", "z"]},
    }
    out = store.apply_overrides(dict(row))
    assert out["description"] == "改过的"   # 覆盖生效
    assert out["topics"] == ["y", "z"]        # 覆盖生效
    assert out["language"] == "Go"            # 没覆盖,保持上游


def test_overridable_fields_exclude_identity():
    """repo_full_name / repo_url 绝不可覆盖——它们是条目身份,改了等于换仓库、
    还会让下次同步认不出这一行而插重复。"""
    import marketplace_leaderboard_store as store

    assert "repo_full_name" not in store.OVERRIDABLE_FIELDS
    assert "repo_url" not in store.OVERRIDABLE_FIELDS
    assert "description" in store.OVERRIDABLE_FIELDS
    # 资源身份覆盖（编辑弹窗里的显示名/发布者）也在白名单里——它们没有原列,
    # 走 admin_overrides 是唯一存储。
    assert "display_name" in store.OVERRIDABLE_FIELDS
    assert "publisher" in store.OVERRIDABLE_FIELDS


def test_apply_overrides_injects_resource_identity_keys():
    """display_name / publisher 榜单行没有对应原列——覆盖存在时读取侧要凭空
    盖出这两个键（资源编辑弹窗据此预填「管理员改过的显示名」而非仓库名）。"""
    import marketplace_leaderboard_store as store

    row = {
        "repo_full_name": "owner/repo",
        "description": "上游描述",
        "admin_overrides": {"display_name": "改过的名字", "publisher": "改过的发布者"},
    }
    out = store.apply_overrides(dict(row))
    assert out["display_name"] == "改过的名字"
    assert out["publisher"] == "改过的发布者"
    # 没覆盖时这两个键不被注入——前端回落仓库名/owner 推导。
    plain = store.apply_overrides({"repo_full_name": "owner/repo", "admin_overrides": {}})
    assert "display_name" not in plain
    assert "publisher" not in plain


# ── 消费侧：草稿永不泄漏 ──────────────────────────────────────────────────────


class _FakeConnFetch:
    """记下 SQL，让测试断言「一定带 status='published'」。永远返回空行。"""

    def __init__(self):
        self.last_sql = ""

    async def fetch(self, sql, *args):
        self.last_sql = sql
        return []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def test_consumer_query_always_filters_published():
    """消费侧 SQL 必须钉死 status='published'——这是草稿不漏给用户的最后一道闸。"""
    import asyncio
    import marketplace_leaderboard_store as store
    from db import PostgresClient

    conn = _FakeConnFetch()

    class _Pool:
        def acquire(self):
            return conn

    original = PostgresClient.pool
    PostgresClient.pool = _Pool()
    try:
        asyncio.run(store.list_published_for_consumer())
    finally:
        PostgresClient.pool = original
    assert "status = 'published'" in conn.last_sql


def test_consumer_projection_returns_resource_fields():
    """消费侧行直接是资源字段（统一资源池 resources 行投影）：名称/分类/标签等
    从 source_data 投影，内部结构（admin_overrides/labels 不在投影里）。
    分类 target_module(s) 必须透传——前端市场卡片按它渲染徽章与安装分支。"""
    import asyncio
    import json

    import marketplace_leaderboard_store as store
    from db import PostgresClient

    # asyncpg 回 JSONB 是 str——用 str 模拟才真实（_decode_row 负责解析）。
    class _Row(dict):
        pass

    source_data = {
        "source": "agent-leaderboard",
        "board": "mcp",
        "repo_full_name": "owner/repo",
        "repo_url": "https://github.com/owner/repo",
        "description": "上游描述",
        "stars": 10,
        "forks": 1,
        "language": "Go",
        "topics": ["x"],
        "upstream_category": "official",
        "use_cases": [],
        "upstream_rank": 3,
        "installable": True,
        "categories": ["开发"],
        "tags": ["mcp"],
        "publisher": "owner",
    }
    row = _Row(
        id=1,
        source_type="leaderboard_sync",
        resource_type="mcp",
        resource_data="{}",
        source_data=json.dumps(source_data),
        association="[]",
        editors="[]",
        probe_data="{}",
        name="repo",
        display_name="改过的名字",
        description="改过的描述",
        version="2026.09.01",
        status="published",
        sort_order=1,
    )

    class _Conn:
        async def fetch(self, sql, *args):
            return [row]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    class _Pool:
        def acquire(self):
            return _Conn()

    original = PostgresClient.pool
    PostgresClient.pool = _Pool()
    try:
        items = asyncio.run(store.list_published_for_consumer())
    finally:
        PostgresClient.pool = original
    assert len(items) == 1
    item = items[0]
    assert item["description"] == "改过的描述"
    assert item["display_name"] == "改过的名字"
    assert item["categories"] == ["开发"]
    assert item["tags"] == ["mcp"]
    assert item["version"] == "2026.09.01"
    assert item["sort_order"] == 1
    assert item["repo_full_name"] == "owner/repo"
    assert item["stars"] == 10
    # 分类透传（消费查询已把 skill 扩成 skill+skills，前端按 target_modules 渲染徽章）。
    assert item["target_module"] == "mcp"
    assert item["target_modules"] == ["mcp"]
    # mcp 的安装形态走 launch_spec，install_spec 恒空。
    assert item["install_spec"] == {}
    # 内部结构不进用户侧响应：admin_overrides 随旧表删除不再投影；
    # labels 现从 source_data 透传（未设置时为空数组）。
    assert "admin_overrides" not in item
    assert item["labels"] == []


def test_consumer_projection_skills_collection_entries():
    """技能集（resource_type=skills）：install_spec.skill.entries 全量透传——
    市场卡片展开子技能列表、按子技能名搜索都依赖这个契约。"""
    import asyncio
    import json

    import marketplace_leaderboard_store as store
    from db import PostgresClient

    class _Row(dict):
        pass

    resource_data = {
        "install_method": "github_clone",
        "ref": "main",
        "entries": [
            {"name": "docx", "path": "skills/docx", "entry": "SKILL.md", "editors": ["claude"]},
            {"name": "pdf", "path": "skills/pdf", "entry": "SKILL.md", "editors": ["claude"]},
        ],
    }
    row = _Row(
        id=2,
        source_type="leaderboard_sync",
        resource_type="skills",
        resource_data=json.dumps(resource_data),
        source_data=json.dumps({
            "source": "agent-leaderboard",
            "board": "skills",
            "repo_full_name": "anthropics/skills",
            "repo_url": "https://github.com/anthropics/skills",
            "installable": True,
        }),
        association="[]",
        editors="[]",
        probe_data="{}",
        name="anthropics/skills",
        display_name="anthropics/skills",
        description="官方技能集合",
        status="published",
        sort_order=2,
    )

    class _Conn:
        async def fetch(self, sql, *args):
            return [row]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    class _Pool:
        def acquire(self):
            return _Conn()

    original = PostgresClient.pool
    PostgresClient.pool = _Pool()
    try:
        items = asyncio.run(store.list_published_for_consumer(target_module="skill"))
    finally:
        PostgresClient.pool = original
    assert len(items) == 1
    item = items[0]
    assert item["repo_full_name"] == "anthropics/skills"
    # 技能集是复数 "skills"——前端安装/徽章分支据此区分集合与单技能。
    assert item["target_module"] == "skills"
    assert item["target_modules"] == ["skills"]
    spec = item["install_spec"]
    assert spec["skill"]["ref"] == "main"
    assert [e["name"] for e in spec["skill"]["entries"]] == ["docx", "pdf"]

