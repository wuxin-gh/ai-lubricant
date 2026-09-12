"""Pure-function tests for the GitHub recognition helpers.

No network: parse_github_input, build_*_manifest, pick_skill_entry, and the
probe's derive_primary_type (fed with synthetic probe dicts). The probe itself
(recognize_repo) hits GitHub and is covered by the recognize endpoint in plan
verification, not here.
"""
from user_platform.github_recognize import (
    build_plugin_manifest,
    build_skill_manifest,
    build_skills_collection_manifest,
    parse_github_input,
    pick_skill_entry,
)
from user_platform.marketplace.leaderboard_probe import derive_primary_type


def test_parse_github_input_accepts_owner_repo_shorthand():
    assert parse_github_input("owner/repo") == ("owner/repo", "", "")


def test_parse_github_input_accepts_https_url():
    assert parse_github_input("https://github.com/owner/repo") == ("owner/repo", "", "")
    assert parse_github_input("https://github.com/owner/repo.git") == ("owner/repo", "", "")


def test_parse_github_input_accepts_tree_url_with_subpath():
    assert parse_github_input(
        "https://github.com/owner/repo/tree/main/skills/foo"
    ) == ("owner/repo", "main", "skills/foo")


def test_parse_github_input_accepts_blob_url():
    assert parse_github_input(
        "https://github.com/owner/repo/blob/v1.2/SKILL.md"
    ) == ("owner/repo", "v1.2", "SKILL.md")


def test_parse_github_input_accepts_ssh_url():
    assert parse_github_input("git@github.com:owner/repo.git") == ("owner/repo", "", "")


def test_parse_github_input_rejects_non_github_host():
    assert parse_github_input("https://gitee.com/owner/repo") == ("", "", "")
    assert parse_github_input("not a url") == ("", "", "")


def test_build_skill_manifest_has_github_clone_coords_resolve_reads():
    """resolve_reference_specs skill branch reads install_method (→should_skip_mirror),
    resource.url/path/ref, and source_url. All must be present and github_clone."""
    entry = {"name": "My Skill", "path": "skills/my", "entry": "SKILL.md", "editors": ["claude"]}
    m = build_skill_manifest("owner/repo", "main", entry)
    assert m["install_method"] == "github_clone"
    assert m["source_url"] == "https://github.com/owner/repo.git"
    assert m["resource"] == {
        "source": "github",
        "url": "https://github.com/owner/repo.git",
        "path": "skills/my",
        "ref": "main",
    }
    assert m["id"] == "github:owner/repo:skills/my"
    assert m["name"] == "My Skill"
    assert m["version"] == "main"


def test_build_skill_manifest_root_skill_has_no_subpath_in_market_id():
    m = build_skill_manifest("owner/repo", "dev", {"path": "", "name": ""})
    assert m["resource"]["path"] == ""
    assert m["name"] == "repo"  # falls back to repo short name
    assert m["id"] == "github:owner/repo"


def test_build_plugin_manifest_exposes_download_url_resolve_reads():
    plugin_spec = {"download_url": "https://github.com/o/r/archive/refs/heads/main.zip",
                   "provider": "claude", "entry": ".claude-plugin/marketplace.json"}
    m = build_plugin_manifest("o/r", "main", plugin_spec, version="abc123")
    assert m["download_url"] == plugin_spec["download_url"]
    assert m["version"] == "abc123"
    assert m["id"] == "github:o/r"


def test_build_plugin_manifest_synthesizes_archive_url_when_probe_found_no_manifest():
    m = build_plugin_manifest("o/r", "main", {})
    assert m["download_url"] == "https://github.com/o/r/archive/refs/heads/main.zip"


def test_build_skill_manifest_overrides_name_and_description():
    """识别后用户可编辑名称/描述——编辑值必须覆盖探针默认值。"""
    entry = {"name": "probed-name", "path": "", "entry": "SKILL.md", "editors": ["claude"]}
    m = build_skill_manifest("o/r", "main", entry, name="我改的名", display_name="显示名", description="我改的描述")
    assert m["name"] == "我改的名"
    assert m["display_name"] == "显示名"
    assert m["description"] == "我改的描述"
    # 坐标不受 override 影响
    assert m["resource"]["url"] == "https://github.com/o/r.git"
    assert m["install_method"] == "github_clone"


def test_build_plugin_manifest_overrides_name_and_description():
    plugin_spec = {"download_url": "https://github.com/o/r/archive/refs/heads/main.zip", "provider": "claude"}
    m = build_plugin_manifest("o/r", "main", plugin_spec, name="改名", description="新描述")
    assert m["name"] == "改名"
    assert m["display_name"] == "改名"
    assert m["description"] == "新描述"
    assert m["download_url"] == plugin_spec["download_url"]


def test_pick_skill_entry_defaults_to_first():
    entries = [{"name": "a"}, {"name": "b"}]
    assert pick_skill_entry(entries, None)["name"] == "a"
    assert pick_skill_entry(entries, 1)["name"] == "b"


def test_pick_skill_entry_empty_or_bad_index_returns_none_or_first():
    assert pick_skill_entry([], 0) is None
    # out-of-range index falls back to 0
    assert pick_skill_entry([{"name": "a"}], 99)["name"] == "a"


# ── derive_primary_type：单选主类型（plugin > skill > mcp > prompt）──

def _probe(tree_paths, files=None, launch=None):
    return {"tree_paths": tree_paths, "files": files or {}, "launch_spec": launch or {}}


def test_primary_type_multi_dir_skills_beats_plugin():
    """anthropics/skills 形态：多个子目录 SKILL.md + 插件清单 → plugin 容器（原 skills
    集合已归入 plugin：技能集本质是多技能的插件包）。"""
    probe = _probe(
        ["skills/pdf/SKILL.md", "skills/slides/SKILL.md", ".claude-plugin/marketplace.json"],
        files={".claude-plugin/marketplace.json": {"plugins": []}},
    )
    assert derive_primary_type(probe, "anthropics/skills") == "plugin"


def test_primary_type_single_dir_multi_editor_is_skill_not_skills():
    """同目录 SKILL.md + AGENTS.md（一个技能的多编辑器形态）→ skill，不是集合。"""
    probe = _probe(["SKILL.md", "AGENTS.md"])
    assert derive_primary_type(probe, "o/r") == "skill"


def test_primary_type_root_skill_with_cursor_rules_is_skill_not_skills():
    """obra/superpowers 形态：根 SKILL.md + AGENTS.md + .cursor/rules/*.mdc 都是
    同一个根技能的多编辑器形态，.cursor/rules 归一到根目录，判 skill 不是 skills。"""
    probe = _probe([
        "SKILL.md", "AGENTS.md",
        ".cursor/rules/main.mdc", ".cursor/rules/extra.mdc",
    ])
    assert derive_primary_type(probe, "obra/superpowers") == "skill"


def test_group_skill_entries_cursor_rules_normalized_to_root():
    """.cursor/rules/*.mdc 的 path 归一到 ""（不是 .cursor/rules），避免顶成多目录集合。"""
    from user_platform.marketplace.leaderboard_probe import _group_skill_entries

    probe = _probe(["SKILL.md", ".cursor/rules/main.mdc"])
    entries = _group_skill_entries(probe, "superpowers")
    assert {e["path"] for e in entries} == {""}, entries


def test_primary_type_plugin_when_one_entry_and_manifest():
    probe = _probe(
        ["SKILL.md", ".claude-plugin/marketplace.json"],
        files={".claude-plugin/marketplace.json": {"plugins": []}},
    )
    assert derive_primary_type(probe, "o/r") == "plugin"


def test_primary_type_mcp_from_launch_spec():
    # _derive_launch_spec 从 files[".mcp.json"] 解析 servers，不从外部传 launch。
    probe = _probe(
        [".mcp.json"],
        files={".mcp.json": {"mcpServers": {"s": {"command": "npx", "args": ["-y", "x"]}}}},
    )
    assert derive_primary_type(probe, "o/r") == "mcp"


def test_primary_type_prompt_from_claude_md():
    probe = _probe(["CLAUDE.md", "README.md"])
    assert derive_primary_type(probe, "o/r") == "prompt"


def test_primary_type_empty_when_nothing_recognized():
    assert derive_primary_type(_probe(["README.md"]), "o/r") == ""


def test_primary_type_plugin_for_multi_skill_repo_without_manifest():
    """无 marketplace.json 的多技能仓库（≥2 个不同父目录）也归 plugin 容器。"""
    probe = _probe(["skills/pdf/SKILL.md", "skills/slides/SKILL.md"])
    assert derive_primary_type(probe, "o/r") == "plugin"


def test_primary_type_skill_when_single_entry_without_manifest():
    """单技能仓库（无 marketplace.json）仍识别为 skill，不是容器。"""
    probe = _probe(["skills/pdf/SKILL.md"])
    assert derive_primary_type(probe, "o/r") == "skill"


# ── 插件容器 resolve（resource_store.resolve_specs 的 entries 展开）──────────
# resolve_specs 是 async——统一 asyncio.run 跑。

def _resolve(rows, bindings):
    import asyncio

    from server.resource_store import resolve_specs

    return asyncio.run(resolve_specs(rows, bindings))


def _ref_row(rtype: str, data: dict) -> dict:
    return {
        "id": "ref-1",
        "display_name": "容器",
        "version": "",
        "params": {},
        "resource": {
            "resource_type": rtype,
            "resource_data": data,
            "display_name": "容器",
            "name": "o/r",
            "source_data": {"repo_full_name": "o/r"},
            "version": "",
        },
    }


def test_resolve_specs_expands_plugin_container_entries():
    """plugin 容器（带 entries）在 skill 通道按子技能展开（装配跟着编辑器走）。"""
    rows = [_ref_row("plugin", {
        "clone_url": "https://github.com/o/r.git",
        "ref": "main",
        "download_url": "https://github.com/o/r/archive/refs/heads/main.zip",
        "entries": [
            {"name": "pdf", "path": "skills/pdf"},
            {"name": "slides", "path": "skills/slides"},
        ],
    })]
    specs = _resolve(rows, [{"reference_id": "ref-1"}])
    assert [s["name"] for s in specs] == ["o/r/pdf", "o/r/slides"]
    assert specs[0]["url"] == "https://github.com/o/r.git"
    assert specs[0]["path"] == "skills/pdf"


def test_resolve_specs_plugin_container_filters_by_binding_entries():
    """绑定带 entries → 只展开勾中的子技能（任务期勾选语义不变）。"""
    rows = [_ref_row("plugin", {
        "clone_url": "https://github.com/o/r.git",
        "ref": "main",
        "entries": [
            {"name": "pdf", "path": "skills/pdf"},
            {"name": "slides", "path": "skills/slides"},
        ],
    })]
    specs = _resolve(rows, [{"reference_id": "ref-1", "entries": ["slides"]}])
    assert [s["name"] for s in specs] == ["o/r/slides"]


def test_resolve_specs_plain_plugin_stays_zip_spec():
    """纯 zip 插件（无 entries）不展开，走 download_url 整包。"""
    rows = [_ref_row("plugin", {
        "download_url": "https://github.com/o/r/archive/refs/heads/main.zip",
    })]
    specs = _resolve(rows, [{"reference_id": "ref-1"}])
    assert specs == [{
        "name": "容器",
        "url": "https://github.com/o/r/archive/refs/heads/main.zip",
        "version": "",
    }]


def test_resolve_specs_legacy_skills_row_expands_like_container():
    """存量 skills 集合行读侧兼容：与 plugin 容器同一展开口径。"""
    rows = [_ref_row("skills", {
        "clone_url": "https://github.com/o/r.git",
        "ref": "main",
        "entries": [{"name": "pdf", "path": "skills/pdf"}],
    })]
    specs = _resolve(rows, [{"reference_id": "ref-1"}])
    assert [s["name"] for s in specs] == ["o/r/pdf"]


# ── 插件容器 manifest（原 skills 集合，已归入 plugin 容器）──────────────────────

def test_build_skills_collection_manifest_full_entries_and_default_name():
    entries = [
        {"name": "pdf", "path": "skills/pdf", "entry": "SKILL.md", "editors": ["claude"]},
        {"name": "slides", "path": "skills/slides", "entry": "SKILL.md", "editors": ["claude"]},
    ]
    m = build_skills_collection_manifest("anthropics/skills", "main", entries)
    # skills 集合现归 plugin 容器：type=plugin，同时带 entries（技能展开）与 download_url
    # （zip 整包）。
    assert m["type"] == "plugin"
    assert m["name"] == "anthropics/skills"  # 容器名默认用仓库全名
    assert m["resource"] == {"source": "github", "url": "https://github.com/anthropics/skills.git", "path": "", "ref": "main"}
    assert m["entries"][0]["name"] == "pdf"
    assert len(m["entries"]) == 2
    assert m["install_method"] == "github_clone"
    assert m["download_url"]  # 容器同时带 zip 整包地址
    assert m["id"] == "github:anthropics/skills"  # 仓库级 market_id，与单技能 github:o/r:子路径 互不冲突


def test_build_skills_collection_manifest_pin_commit_overrides_ref():
    entries = [{"name": "a", "path": "x/a", "entry": "SKILL.md"}]
    m = build_skills_collection_manifest("o/r", "main", entries, pin_commit="abc1234def")
    assert m["resource"]["ref"] == "abc1234def"  # 安装：钉 commit
    assert m["version"] == "abc1234def"


def test_build_skills_collection_manifest_overrides_and_filters_bad_entries():
    entries = [
        {"name": "good", "path": "x/good", "entry": "SKILL.md"},
        {"name": "", "path": "x/noname", "entry": "SKILL.md"},  # 无名 → 剔除
    ]
    m = build_skills_collection_manifest("o/r", "main", entries, name="我的集合", description="描述")
    assert m["name"] == "我的集合"
    assert m["description"] == "描述"
    assert len(m["entries"]) == 1
    assert m["entries"][0]["name"] == "good"
