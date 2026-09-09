"""install_method（下载方式）口径测试。

覆盖三件事：
* validator：合法取值放行、非法拒绝、缺省/旧条目兼容
* should_skip_mirror：用户选了「GitHub 直连」就强制直连，即使有 ready 镜像也不用
* 边界：install_method 只在 skills 模块校验，mcp/plugin 不带它不该报错
"""
from __future__ import annotations

from user_platform.marketplace.validator import validate_manifest
from user_platform.resource_reference_service import should_skip_mirror


def _skill(**overrides) -> dict:
    base = {
        "id": "owner.pkg",
        "name": "pkg",
        "display_name": "Pkg",
        "version": "v1",
        "summary": "a skill",
        "kind": "skill",
        "source_url": "https://github.com/owner/repo",
        "resource": {"type": "skill_package", "entry": "SKILL.md"},
    }
    base.update(overrides)
    return base


# ── validator ──────────────────────────────────────────────────────────────


def test_install_method_valid_values_pass():
    for value in ("github_clone", "server_mirror", ""):
        assert validate_manifest("skills", _skill(install_method=value)) == [], value


def test_install_method_absent_is_compatible_with_old_entries():
    """旧条目没有这个字段,必须照常通过——否则历史市场数据全部校验失败。"""
    assert validate_manifest("skills", _skill()) == []


def test_install_method_rejects_unknown_value():
    errors = validate_manifest("skills", _skill(install_method="ftp_direct"))
    assert any("install_method" in e for e in errors)


def test_install_method_only_validated_for_skills():
    """mcp/plugin 不带 install_method 不该报错——这个字段目前只对 skill 有意义。"""
    mcp = {
        "id": "m", "name": "m", "display_name": "M", "version": "v", "summary": "s",
        "kind": "mcp",
        "resource": {"type": "remote_mcp", "url": "https://x.example/mcp", "transport": "sse"},
    }
    assert validate_manifest("mcp", mcp) == []


# ── should_skip_mirror ─────────────────────────────────────────────────────


def test_github_clone_forces_direct_even_with_mirror_available():
    """用户明确选了直连：有镜像也不该改写——这条约束交给 resolve 的调用方,
    这里只锁定判定本身。"""
    assert should_skip_mirror({"install_method": "github_clone"}) is True


def test_server_mirror_falls_back_to_automatic():
    assert should_skip_mirror({"install_method": "server_mirror"}) is False


def test_absent_install_method_is_automatic():
    """缺省 = 服务端自动（有镜像走服务端，否则直连），不强制跳过。"""
    assert should_skip_mirror({}) is False


def test_empty_string_install_method_is_automatic():
    assert should_skip_mirror({"install_method": ""}) is False


def test_bogus_value_does_not_silently_force_direct():
    """非法值不该被当成 github_clone 而强制直连——那等于把脏数据默默改写行为。
    合法性由 validator 兜住,这里只保证非法值不会误判成「要跳过镜像」。"""
    assert should_skip_mirror({"install_method": "bogus"}) is False
