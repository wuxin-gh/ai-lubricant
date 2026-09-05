from pathlib import Path

import pytest

from agent import file_memory as fm


def test_first_use_creates_private_ga_layout(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(fm, "_MEMORY_ROOT", tmp_path / "memory")

    root = fm.ensure_agent_memory(12)

    assert root == tmp_path / "memory" / "12" / "memory"
    index = fm.agent_l1_path(12).read_text(encoding="utf-8")
    # L1 is GA-shaped: a header, an aggregated L3 routing table keyed by SOP stem
    # (with scenario triggers in parentheses), then [LEARNED] and [RULES].
    assert index.startswith("# [Global Memory Insight]")
    assert "memory_management_sop" in index
    assert "[RULES]" in index
    assert fm.agent_l2_path(12).read_text(encoding="utf-8").startswith("# Verified Environment Facts")
    assert fm.agent_sop_root(12).is_dir()
    assert fm.read_sop(12, "memory_management_sop.md").startswith("# Memory Management SOP")


def test_first_use_indexes_builtin_sops(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(fm, "_MEMORY_ROOT", tmp_path / "memory")
    monkeypatch.setattr(fm, "_BUILTIN_SOP_ROOT", tmp_path / "builtin")
    source = tmp_path / "builtin"
    source.mkdir()
    (source / "alpha_sop.md").write_text("# Alpha SOP\n", encoding="utf-8")
    (source / "beta_sop.md").write_text("# Beta SOP\n", encoding="utf-8")

    fm.ensure_agent_memory(12)

    index = fm.read_l1(12)
    # Built-in SOPs are routed by stem on the aggregated "L3:" line, not by full path.
    assert "alpha_sop" in index
    assert "beta_sop" in index
    assert index.count("L3:") == 1
    assert fm.read_sop(12, "alpha_sop.md") == "# Alpha SOP\n"


def test_agent_memory_is_isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(fm, "_MEMORY_ROOT", tmp_path / "memory")

    fm.append_l2_fact(1, "root", "/one", verified=True)
    fm.append_l2_fact(2, "root", "/two", verified=True)

    assert "/one" in fm.read_l2(1) and "/two" not in fm.read_l2(1)
    assert "/two" in fm.read_l2(2) and "/one" not in fm.read_l2(2)


def test_builtin_sops_seed_once(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(fm, "_MEMORY_ROOT", tmp_path / "memory")
    source = tmp_path / "builtin"
    source.mkdir()
    (source / "base.md").write_text("# Base v1\n", encoding="utf-8")

    fm.ensure_agent_memory(5, source)
    (fm.agent_sop_root(5) / "base.md").write_text("# User edit\n", encoding="utf-8")
    (source / "base.md").write_text("# Base v2\n", encoding="utf-8")
    fm.ensure_agent_memory(5, source)

    assert fm.read_sop(5, "base.md") == "# User edit\n"


def test_sop_traversal_is_rejected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(fm, "_MEMORY_ROOT", tmp_path / "memory")
    fm.ensure_agent_memory(1)

    with pytest.raises(PermissionError):
        fm.read_sop(1, "../secret.md")
    with pytest.raises(PermissionError):
        fm.write_sop(1, "/absolute.md", "x")


def test_distilled_sop_updates_l1(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(fm, "_MEMORY_ROOT", tmp_path / "memory")

    fm.write_sop(9, "deploy.md", "# Deploy\n")
    fm.upsert_l1_pointer(9, "deploy", "sop/deploy.md")

    assert fm.read_sop(9, "deploy.md") == "# Deploy\n"
    assert "sop/deploy.md" in fm.read_l1(9)
