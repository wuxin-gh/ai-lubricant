"""resource mirror 的纯函数单测：打包、digest、路径安全。

不依赖 DB / 网络：只验证 _pack_tar_gz 与 _safe_name/_archive_path 的行为，
这些是镜像正确性与安全性的关键点（节点按 digest 做缓存键，路径穿越会写出目录外）。
"""
from __future__ import annotations

import hashlib
import tarfile
from pathlib import Path

import resources_api as api


def _make_repo(tmp_path: Path) -> Path:
    """造一个像 skill 仓库的目录：含 SKILL.md、子目录、以及应被排除的 .git。"""
    repo = tmp_path / "repo"
    (repo / "skills" / "demo").mkdir(parents=True)
    (repo / "skills" / "demo" / "SKILL.md").write_text("# demo skill\n", encoding="utf-8")
    (repo / "README.md").write_text("readme\n", encoding="utf-8")
    git = repo / ".git"
    git.mkdir()
    (git / "config").write_text("[core]\n", encoding="utf-8")
    return repo


def test_pack_excludes_git_and_reports_digest(tmp_path: Path):
    repo = _make_repo(tmp_path)
    out = tmp_path / "out" / "demo.tar.gz"

    digest, size = api._pack_tar_gz(repo, out)

    assert out.is_file()
    assert size > 0
    # digest 必须是存档内容的 sha256（节点用它做缓存键与完整性校验）
    assert digest == hashlib.sha256(out.read_bytes()).hexdigest()

    with tarfile.open(out, "r:gz") as tf:
        names = tf.getnames()
    # .git 被排除，业务内容保留
    assert not any(n.startswith(".git") or "/.git/" in n for n in names)
    assert "README.md" in names
    assert any(n.endswith("SKILL.md") for n in names)


def test_pack_subtree_only(tmp_path: Path):
    """resource.path 指定子路径时，只镜像该子树（对齐 skill manifest 的 path 语义）。"""
    repo = _make_repo(tmp_path)
    out = tmp_path / "out" / "sub.tar.gz"

    api._pack_tar_gz(repo / "skills", out)

    with tarfile.open(out, "r:gz") as tf:
        names = tf.getnames()
    # 只有 skills/ 下的内容，仓库根的 README 不在
    assert "README.md" not in names
    assert any(n.endswith("SKILL.md") for n in names)


def test_pack_is_deterministic_for_same_content(tmp_path: Path):
    """同内容重复打包 digest 应一致，否则节点缓存会每次失效。"""
    repo = _make_repo(tmp_path)
    a, _ = api._pack_tar_gz(repo, tmp_path / "a.tar.gz")
    b, _ = api._pack_tar_gz(repo, tmp_path / "b.tar.gz")
    assert a == b


def test_safe_name_strips_path_traversal():
    assert "/" not in api._safe_name("../../etc/passwd")
    assert "\\" not in api._safe_name("..\\..\\windows")
    # 常见 market_id 形态（含点、连字符）要保留可读性
    assert api._safe_name("obra.superpowers") == "obra.superpowers"
    assert api._safe_name("code-yeongyu.lazycodex") == "code-yeongyu.lazycodex"


def test_archive_path_stays_under_mirror_root():
    """恶意 market_id / version 不能把存档写到镜像根目录之外。"""
    p = api._archive_path("skills", "../../../evil", "../x")
    assert str(p.resolve()).startswith(str(api._MIRROR_ROOT.resolve()))


def test_archive_path_includes_version_tag():
    with_ver = api._archive_path("skills", "demo", "1.2.0")
    without = api._archive_path("skills", "demo", "")
    assert with_ver != without
    assert "1.2.0" in with_ver.name


# ── 节点契约 ──────────────────────────────────────────────────────────────────
# 下面两组锁的是「服务端下发形态」与「节点消费形态」之间的约定。这两处曾经不一致：
# 节点发 Authorization: Bearer，服务端只读 ?token=，结果每次拉取都 404 却看不出原因。


def test_fetch_accepts_bearer_authorization():
    """节点用 Bearer 头发 token，fetch 路由必须能解出来（Go 侧 downloadAndExtract）。"""
    import asyncio
    import inspect

    seen: dict = {}

    async def fake_verify(module, market_id, token):
        seen["token"] = token
        return None  # 返回 None 让路由继续走 admin 分支，我们只关心解出的 token

    async def fake_admin(_authorization):
        raise RuntimeError("stop-here")  # 到这里已经验证完 token 解析，不必真跑下去

    orig_verify = api.store.verify_fetch_token
    orig_admin = api._require_admin
    api.store.verify_fetch_token = fake_verify
    api._require_admin = fake_admin
    try:
        fn = api.fetch_mirror
        assert inspect.iscoroutinefunction(fn)
        try:
            asyncio.run(fn("skills", "demo", version="", token="", authorization="Bearer rmf_abc123"))
        except RuntimeError as exc:
            assert "stop-here" in str(exc)
    finally:
        api.store.verify_fetch_token = orig_verify
        api._require_admin = orig_admin

    assert seen["token"] == "rmf_abc123", "Bearer 头里的 fetch_token 没被解析"


def test_query_token_still_works():
    """?token= 仍要支持（人工 curl 排障用）。"""
    import asyncio

    seen: dict = {}

    async def fake_verify(module, market_id, token):
        seen["token"] = token
        return None

    async def fake_admin(_authorization):
        raise RuntimeError("stop-here")

    orig_verify = api.store.verify_fetch_token
    orig_admin = api._require_admin
    api.store.verify_fetch_token = fake_verify
    api._require_admin = fake_admin
    try:
        try:
            asyncio.run(api.fetch_mirror("skills", "demo", version="", token="rmf_q", authorization=None))
        except RuntimeError:
            pass
    finally:
        api.store.verify_fetch_token = orig_verify
        api._require_admin = orig_admin

    assert seen["token"] == "rmf_q"
