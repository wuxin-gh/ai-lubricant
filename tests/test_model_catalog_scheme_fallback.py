"""显式标记 is_backup 的非激活方案在内存快照里派生为组条目、并串进 backup_group 链。

DB 里一个组永远只有一行（含 schemes 数组）；catalog 构建期把**显式勾选「作为备用方案」
（is_backup=True）**的非激活方案按数组顺序派生成额外的内存组条目（名形如
``主名#scheme:序号``），主组 backup 指向第一个派生条目，派生条目依次相连，链尾接回主组原本
的 backup_group。引擎沿 backup 链降级即得「方案降级」。

方案身份是 id（不是 name）：激活方案不派生；未勾选 is_backup 的非激活方案只能手动切换为
激活方案，不进降级链（不派生）。派生条目自身不再二次备用。
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from model_catalog import SCHEME_GROUP_SEP, _build_snapshot


def _group(name, *, schemes, active, backup="", **extra):
    # 顶层三字段取激活方案（模拟 _model_group_row 的投影）。
    act = next((s for s in schemes if s["id"] == active), schemes[0])
    return {
        "name": name,
        "enabled": True,
        "remark": extra.get("remark", ""),
        "models": list(act["models"]),
        "aliases": extra.get("aliases", []),
        "provider_whitelist": list(act["provider_whitelist"]),
        "provider_blacklist": list(act["provider_blacklist"]),
        "selection_strategy": "intelligent",
        "backup_group": backup,
        "response_model": extra.get("response_model", ""),
        "metadata_model": extra.get("metadata_model", ""),
        "schemes": schemes,
        "active_scheme": active,
        "created_at": 0,
    }


def _scheme(scheme_id, name, models, wl=None, bl=None, is_backup=False):
    return {
        "id": scheme_id,
        "name": name,
        "models": models,
        "provider_whitelist": wl or [],
        "provider_blacklist": bl or [],
        "is_backup": is_backup,
    }


def test_two_fallback_schemes_derive_entries_and_chain():
    group = _group(
        "opus",
        active="s-fast",
        schemes=[
            _scheme("s-fast", "快", ["m-a"], wl=["p1"]),
            _scheme("s-steady", "稳", ["m-b"], wl=["p2"], is_backup=True),
            _scheme("s-cheap", "省", ["m-c"], bl=["p3"], is_backup=True),
        ],
    )
    snap = _build_snapshot({"groups": {"opus": group}, "metadata": {}, "default": {}}, generation=1)

    d1 = f"opus{SCHEME_GROUP_SEP}1"
    d2 = f"opus{SCHEME_GROUP_SEP}2"
    # 主组 + 2 个派生条目（激活方案「快」不派生）。
    assert set(snap.group_index) == {"opus", d1, d2}
    # 链：主组 → d1 → d2 → 空（无用户 backup）。
    assert snap.group_index["opus"]["backup_group"] == d1
    assert snap.group_index[d1]["backup_group"] == d2
    assert snap.group_index[d2]["backup_group"] == ""
    # 派生条目字段 = 对应非激活方案（按数组顺序：稳、省）。
    assert snap.group_index[d1]["models"] == ("m-b",)
    assert snap.group_index[d1]["provider_whitelist"] == ("p2",)
    assert snap.group_index[d2]["models"] == ("m-c",)
    assert snap.group_index[d2]["provider_blacklist"] == ("p3",)


def test_derived_chain_tail_points_to_user_backup():
    groups = {
        "opus": _group(
            "opus", active="s-fast", backup="haiku",
            schemes=[_scheme("s-fast", "快", ["m-a"]), _scheme("s-steady", "稳", ["m-b"], is_backup=True)],
        ),
        "haiku": _group("haiku", active="s-def", schemes=[_scheme("s-def", "默认", ["m-h"])]),
    }
    snap = _build_snapshot({"groups": groups, "metadata": {}, "default": {}}, generation=1)
    d1 = f"opus{SCHEME_GROUP_SEP}1"
    assert snap.group_index["opus"]["backup_group"] == d1
    # 派生链尾接回用户原配的 backup（haiku）。
    assert snap.group_index[d1]["backup_group"] == "haiku"


def test_single_scheme_group_has_no_derived_entries():
    group = _group("opus", active="s-def", schemes=[_scheme("s-def", "默认", ["m-a"])])
    snap = _build_snapshot({"groups": {"opus": group}, "metadata": {}, "default": {}}, generation=1)
    assert set(snap.group_index) == {"opus"}
    assert snap.group_index["opus"]["backup_group"] == ""


def test_derived_entry_inherits_group_identity():
    group = _group(
        "opus", active="s-fast",
        remark="Opus 组", response_model="claude-opus", metadata_model="m-meta",
        schemes=[_scheme("s-fast", "快", ["m-a"]), _scheme("s-steady", "稳", ["m-b"], is_backup=True)],
    )
    snap = _build_snapshot({"groups": {"opus": group}, "metadata": {}, "default": {}}, generation=1)
    d1 = snap.group_index[f"opus{SCHEME_GROUP_SEP}1"]
    # 降级后对外身份/元数据仍是主组。
    assert d1["response_model"] == "claude-opus"
    assert d1["metadata_model"] == "m-meta"
    assert d1["remark"] == "Opus 组"
    # 派生条目自身不再带 schemes/aliases，避免二次派生与别名污染
    # （快照经 _freeze，list 会成为 tuple）。
    assert tuple(d1["schemes"]) == ()
    assert tuple(d1["aliases"]) == ()


def test_derived_entries_not_reachable_by_alias_and_active_excluded():
    group = _group(
        "opus", active="s-steady",  # 激活的是第二套
        aliases=["opus-latest"],
        schemes=[
            _scheme("s-fast", "快", ["m-a"], is_backup=True),
            _scheme("s-steady", "稳", ["m-b"]),
            _scheme("s-cheap", "省", ["m-c"], is_backup=True),
        ],
    )
    snap = _build_snapshot({"groups": {"opus": group}, "metadata": {}, "default": {}}, generation=1)
    # 激活方案「稳」不派生；「快」「省」各派生一条。
    derived = [k for k in snap.group_index if SCHEME_GROUP_SEP in k]
    assert len(derived) == 2
    # 别名指向主组本身，不指向任何派生条目。
    assert snap.group_index["opus-latest"] is snap.group_index["opus"]


def test_duplicate_scheme_names_still_derive_by_id():
    """两套方案同名（name 可重复），仍按 id 各自派生——不会被 name 去重吞掉。"""
    group = _group(
        "opus", active="s-1",
        schemes=[
            _scheme("s-1", "同名", ["m-a"]),
            _scheme("s-2", "同名", ["m-b"], is_backup=True),
            _scheme("s-3", "同名", ["m-c"], is_backup=True),
        ],
    )
    snap = _build_snapshot({"groups": {"opus": group}, "metadata": {}, "default": {}}, generation=1)
    derived = [k for k in snap.group_index if SCHEME_GROUP_SEP in k]
    assert len(derived) == 2  # 激活 s-1 不派生，s-2/s-3 都标了备用各派生一条


def test_non_active_scheme_without_is_backup_does_not_derive():
    """非激活方案未勾选 is_backup 时不进降级链——只能手动切换为激活，不自动兜底。"""
    group = _group(
        "opus", active="s-fast",
        schemes=[
            _scheme("s-fast", "快", ["m-a"]),
            _scheme("s-manual", "手动备选", ["m-b"]),  # 非激活但未标 is_backup
            _scheme("s-backup", "自动兜底", ["m-c"], is_backup=True),
        ],
    )
    snap = _build_snapshot({"groups": {"opus": group}, "metadata": {}, "default": {}}, generation=1)
    d1 = f"opus{SCHEME_GROUP_SEP}1"
    # 只有标了 is_backup 的 s-backup 派生一条；s-manual 不派生。
    assert set(snap.group_index) == {"opus", d1}
    assert snap.group_index[d1]["models"] == ("m-c",)
    assert snap.group_index["opus"]["backup_group"] == d1
    assert snap.group_index[d1]["backup_group"] == ""


def test_backup_order_follows_array_order_skipping_non_backup():
    """降级顺序按数组顺序，跳过激活方案与未标 is_backup 的方案。"""
    group = _group(
        "opus", active="s-fast",
        schemes=[
            _scheme("s-fast", "快", ["m-a"]),
            _scheme("s-b1", "备1", ["m-b"], is_backup=True),
            _scheme("s-manual", "手动", ["m-x"]),  # 不进链
            _scheme("s-b2", "备2", ["m-c"], is_backup=True),
        ],
    )
    snap = _build_snapshot({"groups": {"opus": group}, "metadata": {}, "default": {}}, generation=1)
    d1 = f"opus{SCHEME_GROUP_SEP}1"
    d2 = f"opus{SCHEME_GROUP_SEP}2"
    assert set(snap.group_index) == {"opus", d1, d2}
    # 链：主组 → 备1 → 备2 → 空；s-manual 被跳过。
    assert snap.group_index["opus"]["backup_group"] == d1
    assert snap.group_index[d1]["models"] == ("m-b",)
    assert snap.group_index[d1]["backup_group"] == d2
    assert snap.group_index[d2]["models"] == ("m-c",)
    assert snap.group_index[d2]["backup_group"] == ""


def test_active_scheme_marked_backup_still_not_derived():
    """激活方案即便被标了 is_backup 也不派生（它就是主组投影，不能自我兜底）。"""
    group = _group(
        "opus", active="s-fast",
        schemes=[
            _scheme("s-fast", "快", ["m-a"], is_backup=True),  # 激活且被标备用
            _scheme("s-b", "备", ["m-b"], is_backup=True),
        ],
    )
    snap = _build_snapshot({"groups": {"opus": group}, "metadata": {}, "default": {}}, generation=1)
    derived = [k for k in snap.group_index if SCHEME_GROUP_SEP in k]
    assert len(derived) == 1  # 只有 s-b 派生
    assert snap.group_index[f"opus{SCHEME_GROUP_SEP}1"]["models"] == ("m-b",)


def test_fingerprint_stable_across_identical_builds():
    group = _group(
        "opus", active="s-fast",
        schemes=[_scheme("s-fast", "快", ["m-a"]), _scheme("s-steady", "稳", ["m-b"])],
    )
    source = {"groups": {"opus": group}, "metadata": {}, "default": {}}
    a = _build_snapshot(source, generation=1)
    b = _build_snapshot(source, generation=2)
    assert a.fingerprint == b.fingerprint
