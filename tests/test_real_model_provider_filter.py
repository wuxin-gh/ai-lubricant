"""真实模型（model_groups kind='real'）的按模型渠道标签过滤。

覆盖三层：
1. Config.get_real_model_provider_filter —— 从快照 groups 读 real 行过滤，custom 行不读；
2. db._real_metadata_normalized / admin._clean_metadata_payload —— 过滤字段清洗与白名单；
3. rate_limiter 选路 —— 非模型组请求（直连真实模型）应用 real 行过滤选渠道。

背景见 docs/project-system/whitepapers/model-routing.md「模型专用线路配置应围绕目标
模型筛选支持渠道」。
"""
import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config


def _snapshot_with_groups(groups: dict):
    """构造带 groups 映射的最小快照对象（config._catalog_field 按 getattr 读取）。"""
    return SimpleNamespace(groups=groups)


# ==================== Config.get_real_model_provider_filter ====================

def test_real_model_filter_reads_real_row_from_snapshot():
    snap = _snapshot_with_groups({
        "m1": {"name": "m1", "kind": "real", "provider_whitelist": ["tagA", "tagB"], "provider_blacklist": ["tagC"]},
    })
    whitelist, blacklist = asyncio.run(config.Config.get_real_model_provider_filter("m1", snapshot=snap))
    assert whitelist == {"tagA", "tagB"}
    assert blacklist == {"tagC"}


def test_real_model_filter_ignores_custom_rows_and_missing_models():
    snap = _snapshot_with_groups({
        "g1": {"name": "g1", "kind": "custom", "models": ["m1"], "provider_whitelist": ["tagA"]},
        "m2": {"name": "m2", "kind": "real", "provider_whitelist": []},
    })
    # custom 行即便带过滤也不读（组过滤走 get_model_group_provider_filter）
    wl, bl = asyncio.run(config.Config.get_real_model_provider_filter("g1", snapshot=snap))
    assert wl == set() and bl == set()
    # real 行无过滤 → 空集合（不限制）
    wl, bl = asyncio.run(config.Config.get_real_model_provider_filter("m2", snapshot=snap))
    assert wl == set() and bl == set()
    # 未配置的模型 → 空集合
    wl, bl = asyncio.run(config.Config.get_real_model_provider_filter("m3", snapshot=snap))
    assert wl == set() and bl == set()
    # kind 缺省（视作 custom）不读
    wl, bl = asyncio.run(config.Config.get_real_model_provider_filter("m2", snapshot=_snapshot_with_groups({"m2": {"name": "m2"}})))
    assert wl == set() and bl == set()


def test_real_model_filter_drops_non_string_entries():
    snap = _snapshot_with_groups({
        "m1": {"name": "m1", "kind": "real", "provider_whitelist": ["tagA", 42, None, ""], "provider_blacklist": ["", "tagB"]},
    })
    whitelist, blacklist = asyncio.run(config.Config.get_real_model_provider_filter("m1", snapshot=snap))
    assert whitelist == {"tagA"}
    assert blacklist == {"tagB"}


# ==================== db._real_metadata_normalized ====================

def test_real_metadata_normalized_moves_filters_out_of_metadata_jsonb():
    from db import PostgresClient

    out = PostgresClient._real_metadata_normalized("m1", {
        "name": "M1", "max_tokens": 8192,
        "provider_whitelist": [" tagA ", "tagA", "tagB"],
        "provider_blacklist": ["tagC"],
    })
    # 过滤字段进顶层列（清洗：去空白 + 去重），不留在 metadata JSONB 负载里
    assert out["kind"] == "real"
    assert out["provider_whitelist"] == ["tagA", "tagB"]
    assert out["provider_blacklist"] == ["tagC"]
    assert "provider_whitelist" not in (out["metadata"] or {})
    assert "provider_blacklist" not in (out["metadata"] or {})
    assert out["metadata"]["max_tokens"] == 8192


def test_real_metadata_normalized_defaults_filters_to_empty():
    from db import PostgresClient

    out = PostgresClient._real_metadata_normalized("m1", {"name": "M1"})
    assert out["provider_whitelist"] == []
    assert out["provider_blacklist"] == []


def test_normalize_model_group_real_branch_honors_filters():
    from db import PostgresClient

    out = PostgresClient.normalize_model_group("m1", {
        "kind": "real",
        "provider_whitelist": ["tagA"],
        "provider_blacklist": ["tagB"],
        "metadata": {"name": "M1"},
    })
    assert out["kind"] == "real"
    assert out["provider_whitelist"] == ["tagA"]
    assert out["provider_blacklist"] == ["tagB"]


# ==================== admin._clean_metadata_payload ====================

def test_clean_metadata_payload_accepts_and_dedupes_filters():
    from admin import _clean_metadata_payload

    cleaned = _clean_metadata_payload({
        "name": "M1",
        "provider_whitelist": [" tagA ", "tagA", "tagB"],
        "provider_blacklist": [],
    })
    assert cleaned["provider_whitelist"] == ["tagA", "tagB"]
    assert cleaned["provider_blacklist"] == []


def test_clean_metadata_payload_rejects_invalid_filters():
    from fastapi import HTTPException

    from admin import _clean_metadata_payload

    with pytest.raises(HTTPException) as exc:
        _clean_metadata_payload({"provider_whitelist": "tagA"})
    assert exc.value.status_code == 400

    with pytest.raises(HTTPException):
        _clean_metadata_payload({"provider_blacklist": ["tagA", 42]})

    with pytest.raises(HTTPException):
        _clean_metadata_payload({"provider_whitelist": ["  "]})


def test_sanitize_real_model_routing_payload_normalizes():
    """选路窄写端点的负载清洗：单对模式与元数据保存同规则（去空白+去重），只取两个过滤字段。"""
    from admin import _sanitize_real_model_routing_payload

    whitelist, blacklist, schemes, active = _sanitize_real_model_routing_payload({
        "provider_whitelist": [" tagA ", "tagA"],
        "provider_blacklist": ["tagB"],
        # 无关字段必须被忽略——窄写不碰元数据
        "name": "x", "max_tokens": 1,
    })
    assert whitelist == ["tagA"]
    assert blacklist == ["tagB"]
    assert schemes is None
    assert active is None


def test_sanitize_real_model_routing_payload_rejects_invalid():
    from fastapi import HTTPException

    from admin import _sanitize_real_model_routing_payload

    with pytest.raises(HTTPException):
        _sanitize_real_model_routing_payload({"provider_whitelist": "tagA"})

    # 空字符串标签与模型组过滤同规则：拒绝而非静默丢弃
    with pytest.raises(HTTPException):
        _sanitize_real_model_routing_payload({"provider_blacklist": ["tagB", ""]})


# ==================== rate_limiter 选路应用 real 行过滤 ====================

class _StubStats:
    def record_pick(self, now):
        return None


class _StubAccountClient:
    def __init__(self, username, provider_name="p1"):
        self.username = username
        self.weight = 1
        self.priority = 0
        self._cooldown_until = 0
        self.is_frozen = False
        self.disabled = False
        self.rpm_limit = 0
        self.tpm_limit = 0
        self.reserved_for = []
        self._stats = _StubStats()
        self._is_test = False
        self.provider = type("P", (), {"PROVIDER_NAME": provider_name})()
        self.last_route_info = {}

    @property
    def daily_requests_remaining(self):
        return None

    @property
    def daily_requests_limit(self):
        return None

    @property
    def model_daily_remaining(self):
        return {}

    async def can_accept(self, model, messages):
        return True

    def is_model_frozen(self, model):
        return False

    def cooldown_remaining(self, now):
        return 0

    async def reserve(self, model, messages):
        self.reserved_for.append(model)
        return True

    async def reserve_with_decision(self, model, messages):
        from limits.rules import LimitDecision, LimitLease, LimitReservation, LimitSubject
        lease = LimitLease(LimitSubject(model=model), f"test:{self.username}")
        return LimitReservation(lease, LimitDecision(True, ""))

    async def release(self):
        return None


def _pool(provider_name, tag):
    pool = SimpleNamespace()
    pool.clients = [_StubAccountClient(f"u-{provider_name}", provider_name=provider_name)]
    pool.channel = SimpleNamespace(
        enabled=True,
        tags=frozenset({tag}) if tag else frozenset(),
        get_chat_protocol_candidates=lambda model, protocol: [None],
    )
    return pool


def _setup_real_model(monkeypatch, real_filter):
    """非模型组请求 m1：两个渠道（p1/tagA、p2/tagB）都承载 m1，real 行过滤决定用哪个。"""
    from rate_limiter import ModelClientPool

    async def _ret(value):
        return value

    monkeypatch.setattr(config.Config, "is_model_group", classmethod(lambda cls, m, **kw: _ret(False)))
    monkeypatch.setattr(config.Config, "get_real_model_provider_filter", classmethod(lambda cls, m, **kw: _ret(real_filter)))
    monkeypatch.setattr(config.Config, "get_model_group_provider_filter", classmethod(lambda cls, m, **kw: _ret((set(), set()))))
    monkeypatch.setattr(config.Config, "get_api_key_strategy", classmethod(lambda cls, api_key, **kw: _ret("sequential")))
    monkeypatch.setattr(config.Config, "get_api_key_provider_filter", classmethod(lambda cls, api_key, **kw: _ret((set(), set()))))
    monkeypatch.setattr(config.Config, "get_providers", classmethod(lambda cls, **kw: _ret({})))

    routes = {"m1": [
        {"provider": "p1", "upstream_model_id": "m1"},
        {"provider": "p2", "upstream_model_id": "m1"},
    ]}

    def _iter(cls, model_id):
        for route in routes.get(model_id, []) or []:
            pool = cls._provider_pools.get(route.get("provider"))
            if pool is not None:
                yield route.get("provider"), pool, route

    monkeypatch.setattr(ModelClientPool, "iter_model_candidates", classmethod(_iter))
    monkeypatch.setattr(ModelClientPool, "has_model_route", classmethod(lambda cls, m: m in routes))
    monkeypatch.setattr(ModelClientPool, "_provider_pools", {"p1": _pool("p1", "tagA"), "p2": _pool("p2", "tagB")})
    monkeypatch.setattr(ModelClientPool, "_model_tpm_available", classmethod(lambda cls, m: _ret(True)))
    monkeypatch.setattr(ModelClientPool, "_model_provider_quota_available", classmethod(lambda cls, m, p: True))
    return ModelClientPool


def test_real_model_whitelist_routes_to_matching_tag_provider(monkeypatch):
    ModelClientPool = _setup_real_model(monkeypatch, ({"tagB"}, set()))
    _, provider_name, client = asyncio.run(ModelClientPool._get_available_client_with_provider("m1", messages=[]))
    assert provider_name == "p2"
    assert client.username == "u-p2"


def test_real_model_blacklist_excludes_tagged_provider(monkeypatch):
    ModelClientPool = _setup_real_model(monkeypatch, (set(), {"tagA"}))
    _, provider_name, _ = asyncio.run(ModelClientPool._get_available_client_with_provider("m1", messages=[]))
    assert provider_name == "p2"


def test_real_model_without_filter_uses_all_providers(monkeypatch):
    ModelClientPool = _setup_real_model(monkeypatch, (set(), set()))
    # 无过滤时 sequential 取首个候选（p1）
    _, provider_name, _ = asyncio.run(ModelClientPool._get_available_client_with_provider("m1", messages=[]))
    assert provider_name == "p1"


def test_real_model_filter_exhausted_raises_no_available(monkeypatch):
    from fastapi import HTTPException

    ModelClientPool = _setup_real_model(monkeypatch, ({"tagC"}, set()))  # 两个渠道都不带 tagC
    with pytest.raises(HTTPException):
        asyncio.run(ModelClientPool._get_available_client_with_provider("m1", messages=[]))


# ==================== 真实模型多方案降级链 ====================

def _catalog_snapshot_with_schemes(groups: dict, generation: int = 1):
    """构造带 real 方案行的 catalog 快照（用真正的 ModelCatalogSnapshot，避免缺 .generation）。"""
    from types import MappingProxyType

    import model_catalog

    frozen_groups = {name: MappingProxyType(dict(group)) for name, group in groups.items()}
    return model_catalog.ModelCatalogSnapshot(
        generation=generation,
        fingerprint=str(generation),
        groups=MappingProxyType(frozen_groups),
        group_index=MappingProxyType({}),
        metadata=MappingProxyType({}),
        default=MappingProxyType({}),
        group_metadata=MappingProxyType({}),
    )


def _real_row(schemes, active_scheme, **top):
    row = {
        "name": "m1", "kind": "real", "enabled": True, "models": [],
        "provider_whitelist": top.get("provider_whitelist", []),
        "provider_blacklist": top.get("provider_blacklist", []),
        "schemes": schemes, "active_scheme": active_scheme,
    }
    return row


def _real_model_filter_nodes(model_id, snapshot):
    """直接调节点解析（不经过 monkeypatch 过的 Config）。"""
    from rate_limiter import ModelClientPool

    return asyncio.run(ModelClientPool._real_model_filter_nodes(model_id, is_group=False, is_test=False, is_probe=False, snapshot=snapshot))


def test_real_model_filter_nodes_returns_none_without_multiple_schemes():
    # 单方案（合成默认）→ None：退回单节点路径，行为与原实现一致
    snap = _catalog_snapshot_with_schemes({"m1": _real_row([], "")})
    assert _real_model_filter_nodes("m1", snap) is None
    # 只有激活方案、无 is_backup → None
    snap = _catalog_snapshot_with_schemes({"m1": _real_row(
        [{"id": "s1", "name": "a", "models": ["m1"], "provider_whitelist": ["tagA"], "provider_blacklist": [], "is_backup": False}], "s1",
    )})
    assert _real_model_filter_nodes("m1", snap) is None
    # 非 real 行 → None
    snap = _catalog_snapshot_with_schemes({"m1": {"name": "m1", "kind": "custom", "models": ["m1"], "provider_whitelist": ["tagA"]}})
    assert _real_model_filter_nodes("m1", snap) is None


def test_real_model_filter_nodes_orders_active_then_backup_schemes():
    snap = _catalog_snapshot_with_schemes({"m1": _real_row([
        {"id": "s1", "name": "高端", "models": ["m1"], "provider_whitelist": ["tagA"], "provider_blacklist": [], "is_backup": False},
        {"id": "s2", "name": "普通", "models": ["m1"], "provider_whitelist": ["tagB"], "provider_blacklist": [], "is_backup": True},
        {"id": "s3", "name": "兜底", "models": ["m1"], "provider_whitelist": [], "provider_blacklist": [], "is_backup": True},
    ], "s1")})
    nodes = _real_model_filter_nodes("m1", snap)
    assert nodes == [({"tagA"}, set()), ({"tagB"}, set()), (set(), set())]


def test_real_model_scheme_fallback_degrades_when_active_channels_unavailable(monkeypatch):
    """激活方案渠道账号冻结 → 无候选 → 降级到备用方案 → 命中备用渠道。"""
    ModelClientPool = _setup_real_model(monkeypatch, ({"tagA"}, set()))  # 主行过滤=激活投影，仅作单节点兜底
    ModelClientPool._provider_pools["p1"].clients[0].is_frozen = True  # p1/tagA 账号冻结
    snap = _catalog_snapshot_with_schemes({"m1": _real_row(
        [
            {"id": "s1", "name": "高端", "models": ["m1"], "provider_whitelist": ["tagA"], "provider_blacklist": [], "is_backup": False},
            {"id": "s2", "name": "普通", "models": ["m1"], "provider_whitelist": ["tagB"], "provider_blacklist": [], "is_backup": True},
        ],
        "s1",
        provider_whitelist=["tagA"],
    )})
    _, provider_name, client = asyncio.run(
        ModelClientPool._get_available_client_with_provider("m1", messages=[], snapshot=snap)
    )
    assert provider_name == "p2"  # 降级到备用方案 tagB
    assert client.username == "u-p2"


def test_real_model_scheme_fallback_skips_unmarked_backup_schemes(monkeypatch):
    """未标记 is_backup 的方案不参与自动降级：激活方案不可用时直接报错，不走未标记方案。"""
    from fastapi import HTTPException

    ModelClientPool = _setup_real_model(monkeypatch, ({"tagA"}, set()))
    ModelClientPool._provider_pools["p1"].clients[0].is_frozen = True
    snap = _catalog_snapshot_with_schemes({"m1": _real_row(
        [
            {"id": "s1", "name": "高端", "models": ["m1"], "provider_whitelist": ["tagA"], "provider_blacklist": [], "is_backup": False},
            # 未标 is_backup —— 仅手动切换可用，不进降级链
            {"id": "s2", "name": "普通", "models": ["m1"], "provider_whitelist": ["tagB"], "provider_blacklist": [], "is_backup": False},
        ],
        "s1",
        provider_whitelist=["tagA"],
    )})
    with pytest.raises(HTTPException):
        asyncio.run(ModelClientPool._get_available_client_with_provider("m1", messages=[], snapshot=snap))


# ==================== 方案落库：_real_metadata_normalized ====================

def test_real_metadata_normalized_projects_active_scheme_to_top_level():
    from db import PostgresClient

    out = PostgresClient._real_metadata_normalized("m1", {
        "schemes": [
            {"id": "s1", "name": "高端", "models": ["m1"], "provider_whitelist": ["tagA"], "provider_blacklist": [], "is_backup": False},
            {"id": "s2", "name": "普通", "models": ["m1"], "provider_whitelist": ["tagB"], "provider_blacklist": [], "is_backup": True},
        ],
        "active_scheme": "s1",
    })
    assert out["kind"] == "real"
    assert out["schemes"][0]["id"] == "s1"
    assert out["active_scheme"] == "s1"
    # 顶层 = 激活方案投影
    assert out["provider_whitelist"] == ["tagA"]
    assert out["provider_blacklist"] == []
    # schemes/active_scheme 不进 metadata JSONB
    assert "schemes" not in (out["metadata"] or {})
    assert "active_scheme" not in (out["metadata"] or {})


def test_real_metadata_normalized_patching_top_pair_writes_active_scheme():
    """行已有 schemes、负载只带顶层白名单对（旧单对入口）：写进激活方案，而非投影覆盖。"""
    from db import PostgresClient

    out = PostgresClient._real_metadata_normalized("m1", {
        "provider_whitelist": ["tagNew"],
        "schemes": [
            {"id": "s1", "name": "高端", "models": ["m1"], "provider_whitelist": ["tagA"], "provider_blacklist": [], "is_backup": False},
        ],
        "active_scheme": "s1",
    })
    assert out["provider_whitelist"] == ["tagNew"]
    assert out["schemes"][0]["provider_whitelist"] == ["tagNew"]


# ==================== 方案落库：admin 窄写校验 ====================

def test_sanitize_real_model_routing_payload_accepts_schemes_mode():
    from admin import _sanitize_real_model_routing_payload

    whitelist, blacklist, schemes, active = _sanitize_real_model_routing_payload({
        "schemes": [
            {"id": "s1", "name": "高端", "models": ["m1"], "provider_whitelist": ["tagA"], "provider_blacklist": [], "is_backup": False},
            {"id": "s2", "name": "普通", "models": ["m1"], "provider_whitelist": ["tagB"], "provider_blacklist": [], "is_backup": True},
        ],
        "active_scheme": "s1",
        # 无关字段被忽略
        "name": "x", "max_tokens": 1,
    })
    # 方案模式下顶层对返回空（由后端按激活方案投影）
    assert whitelist == [] and blacklist == []
    assert schemes is not None and len(schemes) == 2
    assert active == "s1"


def test_sanitize_real_model_routing_payload_rejects_duplicate_scheme_ids():
    from fastapi import HTTPException

    from admin import _sanitize_real_model_routing_payload

    with pytest.raises(HTTPException):
        _sanitize_real_model_routing_payload({
            "schemes": [
                {"id": "s1", "name": "a", "models": ["m1"], "provider_whitelist": ["tagA"], "provider_blacklist": [], "is_backup": False},
                {"id": "s1", "name": "b", "models": ["m1"], "provider_whitelist": [], "provider_blacklist": [], "is_backup": True},
            ],
        })


def test_sanitize_real_model_routing_payload_rejects_scheme_without_models():
    from fastapi import HTTPException

    from admin import _sanitize_real_model_routing_payload

    with pytest.raises(HTTPException):
        _sanitize_real_model_routing_payload({
            "schemes": [
                {"id": "s1", "name": "a", "models": [], "provider_whitelist": ["tagA"], "provider_blacklist": [], "is_backup": False},
            ],
        })
