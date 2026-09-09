"""Recursive tree pagination for the git clients.

gitea 1.24+ paginates the ``git/trees`` recursive endpoint (``per_page``/``page``),
and gitlab/gitee tree listings have always been paginated. A single-page fetch
silently returns only the first 100 entries — which for a repo whose first page
is dominated by one alphabetically-early junk directory (the discovered case:
a committed Chrome user-data dir) yields an all-junk entry set. These tests pin
the walk-until-exhausted behavior at the ``_get_json`` mock boundary (no network).
"""
from __future__ import annotations

import os
import sys

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from user_platform import git_clients


def _git_entries(n: int, first_sha: str) -> list[dict]:
    """gitea/gitee git/trees shaped entries with unique paths/shas."""
    return [
        {"path": f"f{i:04d}.py", "mode": "100644", "type": "blob", "sha": f"{first_sha}-{i:04d}", "size": 1}
        for i in range(n)
    ]


def _gl_entries(n: int, first_id: str) -> list[dict]:
    """gitlab repository/tree shaped entries."""
    return [
        {"name": f"f{i:04d}.py", "path": f"f{i:04d}.py", "type": "blob", "id": f"{first_id}-{i:04d}", "mode": "100644"}
        for i in range(n)
    ]


class _MockGet:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls: list[dict] = []

    async def __call__(self, url, *, headers, params=None):
        self.calls.append({"url": url, "headers": headers, "params": params or {}})
        if self._responses:
            return self._responses.pop(0)
        return [], {}


def _install(monkeypatch, responses):
    mock = _MockGet(responses)
    monkeypatch.setattr(git_clients, "_get_json", mock)
    return mock


OPTS = git_clients.RepositoryOptions(token="t")
PAGE = git_clients._FETCH_ALL_PAGE_SIZE


# ── gitea ────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_gitea_recursive_tree_paginates_to_exhaustion(monkeypatch):
    mock = _install(monkeypatch, [
        ({"tree": _git_entries(PAGE, "a")}, {}),
        ({"tree": _git_entries(PAGE, "b")}, {}),
        ({"tree": _git_entries(37, "c")}, {}),
    ])
    entries = await git_clients.fetch_tree("gitea", "o/r", OPTS, ref="master", path="", recursive=True)
    assert len(entries) == 2 * PAGE + 37
    assert len(mock.calls) == 3
    assert [c["params"]["page"] for c in mock.calls] == [1, 2, 3]
    assert all(c["params"]["per_page"] == PAGE and c["params"]["recursive"] == "true" for c in mock.calls)
    # 首条 sha 来自第三页：整棵树按序拼接，不是只有第一页。
    assert entries[2 * PAGE].path == "f0000.py"


@pytest.mark.asyncio
async def test_gitea_recursive_tree_stops_on_partial_page(monkeypatch):
    mock = _install(monkeypatch, [
        ({"tree": _git_entries(PAGE, "a")}, {}),
        ({"tree": _git_entries(40, "b")}, {}),
    ])
    entries = await git_clients.fetch_tree("gitea", "o/r", OPTS, ref="", path="", recursive=True)
    assert len(entries) == PAGE + 40
    assert len(mock.calls) == 2  # 不满页即末页，不再发第 3 次


@pytest.mark.asyncio
async def test_gitea_recursive_tree_whole_tree_response_is_not_refetched(monkeypatch):
    # 老版 gitea 无视 per_page 整树返回（250 条）：append 全量后止步，不重复翻页。
    mock = _install(monkeypatch, [({"tree": _git_entries(250, "a")}, {})])
    entries = await git_clients.fetch_tree("gitea", "o/r", OPTS, ref="", path="", recursive=True)
    assert len(entries) == 250
    assert len(mock.calls) == 1


@pytest.mark.asyncio
async def test_gitea_recursive_tree_stops_when_page_param_ignored(monkeypatch):
    # 服务端每页都返回同一批（不认 page 参数）：止步，不把同一页抓 50 次。
    page_body = {"tree": _git_entries(PAGE, "same")}
    mock = _install(monkeypatch, [(page_body, {}), (dict(page_body), {}), (dict(page_body), {})])
    entries = await git_clients.fetch_tree("gitea", "o/r", OPTS, ref="", path="", recursive=True)
    assert len(entries) == PAGE
    assert len(mock.calls) == 2


@pytest.mark.asyncio
async def test_gitea_single_level_tree_keeps_contents_endpoint(monkeypatch):
    mock = _install(monkeypatch, [([{"name": "a.py", "path": "a.py", "type": "file", "sha": "s"}], {})])
    entries = await git_clients.fetch_tree("gitea", "o/r", OPTS, ref="", path="", recursive=False)
    assert len(entries) == 1
    assert len(mock.calls) == 1
    assert "/contents/" in mock.calls[0]["url"]


# ── gitee ────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_gitee_recursive_tree_paginates_to_exhaustion(monkeypatch):
    mock = _install(monkeypatch, [
        ({"tree": _git_entries(PAGE, "a")}, {}),
        ({"tree": _git_entries(12, "b")}, {}),
    ])
    entries = await git_clients.fetch_tree("gitee", "o/r", OPTS, ref="master", path="", recursive=True)
    assert len(entries) == PAGE + 12
    assert len(mock.calls) == 2
    assert [c["params"]["page"] for c in mock.calls] == [1, 2]


# ── gitlab ───────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_gitlab_recursive_tree_paginates_to_exhaustion(monkeypatch):
    mock = _install(monkeypatch, [(_gl_entries(PAGE, "a"), {}), (_gl_entries(55, "b"), {})])
    entries = await git_clients.fetch_tree("gitlab", "g/p", OPTS, ref="main", path="", recursive=True)
    assert len(entries) == PAGE + 55
    assert len(mock.calls) == 2
    assert [c["params"]["page"] for c in mock.calls] == [1, 2]
    assert all(c["params"]["recursive"] == "true" for c in mock.calls)


@pytest.mark.asyncio
async def test_gitlab_tree_single_page_makes_one_call(monkeypatch):
    mock = _install(monkeypatch, [(_gl_entries(3, "a"), {})])
    entries = await git_clients.fetch_tree("gitlab", "g/p", OPTS, ref="", path="", recursive=False)
    assert len(entries) == 3
    assert len(mock.calls) == 1
