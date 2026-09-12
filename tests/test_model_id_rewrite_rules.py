"""model_id 改写规则：上游模型 ID → 对外 model_id。

契约要点（改动前先看这里）：
1. 切 "owner/" 前缀发生在规则之前，规则跑在切完之后的结果上。
2. 顺序敏感且不是命中即停——多条规则自上而下叠加。
3. 坏正则在保存侧被 compile 校验拒掉；运行时兜底跳过该条，
   不能让一条坏规则打断整轮 refresh_models。
4. 结果为空时回退到改写前的值，绝不把 model_id 抹成空串。
5. 条目列表是混合的：kind="rule"（缺省）是内联规则，kind="template" 是对
   全局模版的活引用，在其所在位置就地展开。模版与内联规则同列排序，位置即
   优先级，二者之间没有高低之分。
6. 落库判定（_resolve_target_model_id）：规则搜到的行改名保留，撞名不让位——
   渠道允许多个同名 model_id，deepseek-v4-flash-free 改名成 deepseek-v4-flash
   和原版撞名是正常的。只有渠道配了生效规则时才重算已跟踪行，否则保留库里的
   手工改名。
7. 只搜索规则（search_only=True）：纯过滤条件，按**全量名**（含 owner/ 前缀，
   未切）匹配，命中即保留该模型，不参与改名。
"""
import pytest

from channel import (
    Channel,
    apply_model_id_rewrite_rules,
    compile_model_id_rewrite_rules,
    expand_model_id_rewrite_rules,
    model_id_matches_search_rules,
    normalize_model_id_rewrite_rules,
    set_model_rule_template_cache,
    strip_model_owner_prefix,
)


def _rule(pattern: str, replacement: str = "", **kw) -> dict:
    return {"pattern": pattern, "replacement": replacement, **kw}


def _ref(template_id: str, **kw) -> dict:
    return {"kind": "template", "template_id": template_id, **kw}


@pytest.fixture(autouse=True)
def _clear_template_cache():
    """模版缓存是模块级全局，用例间必须互相隔离。"""
    set_model_rule_template_cache([])
    yield
    set_model_rule_template_cache([])


# ── 切前缀 ─────────────────────────────────────────────────────────────────
def test_strip_prefix_keeps_tail():
    assert strip_model_owner_prefix("1111/glm-5.2") == "glm-5.2"


def test_strip_prefix_noop_without_slash():
    assert strip_model_owner_prefix("gpt-4o") == "gpt-4o"


def test_strip_prefix_keeps_original_when_tail_empty():
    """后半为空时原样返回，避免把显式 model_id 改坏。"""
    assert strip_model_owner_prefix("owner/") == "owner/"


def test_strip_prefix_only_splits_first_slash():
    assert strip_model_owner_prefix("a/b/c") == "b/c"


# ── 规则应用 ───────────────────────────────────────────────────────────────
def test_prefix_stripped_before_rules_run():
    """规则看到的是切完前缀的值：^glm 能命中 "1111/glm-5.2" 的尾段。"""
    assert apply_model_id_rewrite_rules("1111/glm-5.2", [_rule(r"^glm", "GLM")]) == "GLM-5.2"


def test_rules_apply_in_order_and_accumulate():
    """不是命中即停：两条规则都要作用到结果上。"""
    rules = [_rule(r"-latest$"), _rule(r"^gpt", "openai-gpt")]
    assert apply_model_id_rewrite_rules("gpt-4o-latest", rules) == "openai-gpt-4o"


def test_order_matters():
    """交换顺序得到不同结果——顺序是规则语义的一部分。"""
    first = [_rule(r"^a", "b"), _rule(r"^b", "c")]
    second = [_rule(r"^b", "c"), _rule(r"^a", "b")]
    assert apply_model_id_rewrite_rules("a-1", first) == "c-1"
    assert apply_model_id_rewrite_rules("a-1", second) == "b-1"


def test_capture_group_backreference():
    rules = [_rule(r"^(.+)-preview$", r"\1")]
    assert apply_model_id_rewrite_rules("claude-3.5-sonnet-preview", rules) == "claude-3.5-sonnet"


def test_disabled_rule_skipped():
    rules = [_rule(r"^gpt", "X", enabled=False)]
    assert apply_model_id_rewrite_rules("gpt-4o", rules) == "gpt-4o"


def test_rule_emptying_result_falls_back():
    """规则把整个名字替空时回退，不产出空 model_id。"""
    assert apply_model_id_rewrite_rules("1111/abc", [_rule(r".*")]) == "abc"


def test_bad_regex_skipped_but_later_rules_still_apply():
    """一条坏正则不能拖垮整轮同步，也不能吃掉后续规则。"""
    rules = [_rule("["), _rule(r"c$", "C")]
    assert apply_model_id_rewrite_rules("abc", rules) == "abC"


def test_no_rules_is_prefix_strip_only():
    """无配置时行为与历史 _strip_owner_prefix 完全一致。"""
    assert apply_model_id_rewrite_rules("1111/glm-5.2", []) == "glm-5.2"
    assert apply_model_id_rewrite_rules("1111/glm-5.2", None) == "glm-5.2"


def test_strip_prefix_can_be_disabled():
    assert apply_model_id_rewrite_rules("1111/glm", [], strip_prefix=False) == "1111/glm"


# ── 归一化 / 编译校验 ──────────────────────────────────────────────────────
def test_normalize_drops_junk_rows():
    """非 dict 与空 pattern 的行被丢弃，不进运行时。"""
    raw = ["nope", None, {"pattern": "  "}, {"no_pattern": 1}, _rule("ok")]
    rules = normalize_model_id_rewrite_rules(raw)
    assert [r["pattern"] for r in rules] == ["ok"]


def test_normalize_defaults_enabled_true():
    assert normalize_model_id_rewrite_rules([_rule("x")])[0]["enabled"] is True


def test_normalize_non_list_is_empty():
    assert normalize_model_id_rewrite_rules({"pattern": "x"}) == []
    assert normalize_model_id_rewrite_rules(None) == []


def test_compile_reports_bad_regex_with_label():
    """错误信息带规则名，便于前端定位是哪一条坏了。"""
    _, errors = compile_model_id_rewrite_rules([_rule("[", name="脏前缀")])
    assert len(errors) == 1
    assert "脏前缀" in errors[0]


def test_compile_falls_back_to_index_label():
    _, errors = compile_model_id_rewrite_rules([_rule("[")])
    assert "第 1 条" in errors[0]


def test_compile_accepts_valid_rules():
    rules, errors = compile_model_id_rewrite_rules([_rule(r"-latest$")])
    assert errors == []
    assert len(rules) == 1


# ── 渠道集成 ───────────────────────────────────────────────────────────────
def test_channel_public_model_id_uses_configured_rules():
    ch = Channel("demo", {"model_id_rewrite_rules": [_rule(r"-latest$")]})
    assert ch.public_model_id("1111/gpt-4o-latest") == "gpt-4o"


def test_channel_without_rules_only_strips_prefix():
    ch = Channel("demo", {})
    assert ch.public_model_id("1111/gpt-4o") == "gpt-4o"


def test_channel_apply_swaps_rules_hot():
    """热更新换掉规则后立即生效——配置改动不需要重启。"""
    ch = Channel("demo", {"model_id_rewrite_rules": [_rule(r"^gpt", "A")]})
    assert ch.public_model_id("gpt-4o") == "A-4o"
    ch.apply({"model_id_rewrite_rules": [_rule(r"^gpt", "B")]})
    assert ch.public_model_id("gpt-4o") == "B-4o"


def test_channel_ignores_malformed_rules_config():
    """库里是脏数据时退化为只切前缀，不抛异常。"""
    ch = Channel("demo", {"model_id_rewrite_rules": "not-a-list"})
    assert ch.public_model_id("1111/gpt-4o") == "gpt-4o"


# ── 全局模版引用 ───────────────────────────────────────────────────────────
def test_legacy_rules_without_kind_unchanged():
    """存量数据（无 kind）行为不变：归一结果不带 kind 键，这是零迁移的底线。

    search_only 是后加的字段，归一时一律补齐成布尔（存量行补 False），语义不变。
    """
    entries = normalize_model_id_rewrite_rules([_rule(r"-latest$", name="去后缀")])
    assert entries == [
        {"name": "去后缀", "enabled": True, "pattern": r"-latest$", "replacement": "", "search_only": False},
    ]


def test_template_expands_in_place():
    """rule → template → rule：展开结果与手写等价扁平列表逐条一致。"""
    set_model_rule_template_cache([
        {"id": "tpl", "name": "通用", "rules": [_rule(r":free$"), _rule(r"-latest$")]},
    ])
    mixed = [_rule(r"^openai\.", "gpt-"), _ref("tpl"), _rule(r"^gpt", "GPT")]
    flat = [_rule(r"^openai\.", "gpt-"), _rule(r":free$"), _rule(r"-latest$"), _rule(r"^gpt", "GPT")]
    assert expand_model_id_rewrite_rules(mixed) == normalize_model_id_rewrite_rules(flat)
    assert apply_model_id_rewrite_rules("openai.4o-latest:free", mixed) == "GPT-4o"


def test_template_position_is_priority():
    """交换内联规则与模版引用的顺序得到不同结果——顺序即语义。"""
    set_model_rule_template_cache([{"id": "tpl", "name": "t", "rules": [_rule(r"^b", "c")]}])
    template_last = [_rule(r"^a", "b"), _ref("tpl")]
    template_first = [_ref("tpl"), _rule(r"^a", "b")]
    assert apply_model_id_rewrite_rules("a-1", template_last) == "c-1"
    assert apply_model_id_rewrite_rules("a-1", template_first) == "b-1"


def test_disabled_template_ref_skips_whole_template():
    set_model_rule_template_cache([{"id": "tpl", "name": "t", "rules": [_rule(r"-latest$")]}])
    assert apply_model_id_rewrite_rules("gpt-4o-latest", [_ref("tpl", enabled=False)]) == "gpt-4o-latest"


def test_missing_template_is_skipped_not_raised():
    """模版可能后建，或随渠道模板跨实例导入时尚未同步——跳过而不是炸。"""
    assert apply_model_id_rewrite_rules("1111/gpt-4o", [_ref("nope")]) == "gpt-4o"


def test_template_ref_without_id_dropped():
    assert normalize_model_id_rewrite_rules([_ref("  ")]) == []


def test_template_ref_survives_normalize_roundtrip():
    assert normalize_model_id_rewrite_rules([_ref("tpl")]) == [
        {"kind": "template", "template_id": "tpl", "enabled": True},
    ]


def test_compile_skips_template_refs():
    """引用条目不校验模版是否存在，只校验内联规则的正则。"""
    entries, errors = compile_model_id_rewrite_rules([_ref("nope"), _rule(r"-latest$")])
    assert errors == []
    assert len(entries) == 2


def test_compile_still_rejects_bad_regex_alongside_ref():
    _, errors = compile_model_id_rewrite_rules([_ref("tpl"), _rule("[", name="坏的")])
    assert len(errors) == 1
    assert "坏的" in errors[0]


def test_template_cache_hot_swap():
    """活引用：渠道配置不动，改模版内容即时改变改写结果。"""
    ch = Channel("demo", {"model_id_rewrite_rules": [_ref("tpl")]})
    set_model_rule_template_cache([{"id": "tpl", "name": "t", "rules": [_rule(r"^gpt", "A")]}])
    assert ch.public_model_id("gpt-4o") == "A-4o"
    set_model_rule_template_cache([{"id": "tpl", "name": "t", "rules": [_rule(r"^gpt", "B")]}])
    assert ch.public_model_id("gpt-4o") == "B-4o"


def test_nested_template_in_cache_is_ignored():
    """保存侧拒绝模版嵌模版；缓存侧再兜一层，脏数据不会递归。"""
    set_model_rule_template_cache([
        {"id": "tpl", "name": "t", "rules": [_ref("tpl"), _rule(r"^gpt", "A")]},
    ])
    assert apply_model_id_rewrite_rules("gpt-4o", [_ref("tpl")]) == "A-4o"


# ── has_model_id_rewrite_rules：区分「显式规则」与「默认切前缀」 ────────────
def test_has_rules_false_without_config():
    assert Channel("demo", {}).has_model_id_rewrite_rules() is False


def test_has_rules_true_with_inline_rule():
    assert Channel("demo", {"model_id_rewrite_rules": [_rule(r"-latest$")]}).has_model_id_rewrite_rules() is True


def test_has_rules_false_when_all_disabled():
    """全部停用等于没规则——不然一个停用的列表也会触发重算。"""
    ch = Channel("demo", {"model_id_rewrite_rules": [_rule(r"-latest$", enabled=False)]})
    assert ch.has_model_id_rewrite_rules() is False


def test_has_rules_follows_template_content():
    """看展开后的结果：模版有内容才算有规则，模版被删/停用等于没规则。"""
    ch = Channel("demo", {"model_id_rewrite_rules": [_ref("tpl")]})
    assert ch.has_model_id_rewrite_rules() is False  # 模版不存在
    set_model_rule_template_cache([{"id": "tpl", "name": "t", "rules": []}])
    assert ch.has_model_id_rewrite_rules() is False  # 模版存在但空
    set_model_rule_template_cache([{"id": "tpl", "name": "t", "rules": [_rule(r"^gpt", "A")]}])
    assert ch.has_model_id_rewrite_rules() is True


# ── 落库判定：规则搜到就改名保留，撞名不让位 ───────────────────────────────
def _resolve(channel_cfg, upstream_id, raw_model_id, rows, monkeypatch):
    """按渠道配置与 provider_models 现状算出目标 model_id。"""
    from rate_limiter import ModelClientPool

    pool = type("_Pool", (), {"channel": Channel("prov", channel_cfg)})()
    monkeypatch.setattr(ModelClientPool, "get_provider_pool", classmethod(lambda cls, name: pool))
    return ModelClientPool._resolve_target_model_id(
        "prov",
        upstream_id,
        raw_model_id,
        {r["upstream_model_id"]: r["model_id"] for r in rows},
    )


def test_tracked_row_follows_rules(monkeypatch):
    """核心诉求：改一版规则，已入库的 xxx:free 跟着变成真实名字。"""
    target = _resolve(
        {"model_id_rewrite_rules": [_rule(r":free$")]},
        "acme/glm:free",
        "acme/glm:free",
        [{"upstream_model_id": "acme/glm:free", "model_id": "glm:free"}],
        monkeypatch,
    )
    assert target == "glm"


def test_tracked_row_keeps_manual_rename_without_rules(monkeypatch):
    """没配规则时不重算：切前缀是默认形态，不能把手工改名冲回去。"""
    target = _resolve(
        {},
        "acme/kept",
        "acme/kept",
        [{"upstream_model_id": "acme/kept", "model_id": "custom-kept"}],
        monkeypatch,
    )
    assert target == "custom-kept"


def test_new_model_always_uses_rewritten_name(monkeypatch):
    target = _resolve(
        {"model_id_rewrite_rules": [_rule(r":free$")]},
        "acme/new:free",
        "acme/new:free",
        [],
        monkeypatch,
    )
    assert target == "new"


def test_regex_hit_renames_even_when_target_name_collides(monkeypatch):
    """规则搜到的行照样改名，目标名被别的上游模型占着也不让位。

    渠道 model_id 只有普通索引、允许多个同名；deepseek-v4-flash-free 改名成
    deepseek-v4-flash 后和原版撞名是正常的，两行 upstream_model_id 不同各管各的。
    """
    target = _resolve(
        {"model_id_rewrite_rules": [_rule(r":free$")]},
        "acme/glm:free",
        "acme/glm:free",
        [
            {"upstream_model_id": "acme/glm:free", "model_id": "glm:free"},
            {"upstream_model_id": "acme/glm", "model_id": "glm"},  # 已占用目标名
        ],
        monkeypatch,
    )
    assert target == "glm"


def test_idempotent_when_target_equals_own_current(monkeypatch):
    """目标名就是自己这行的值：照常返回。"""
    target = _resolve(
        {"model_id_rewrite_rules": [_rule(r":free$")]},
        "acme/glm:free",
        "acme/glm:free",
        [{"upstream_model_id": "acme/glm:free", "model_id": "glm"}],
        monkeypatch,
    )
    assert target == "glm"


# ── 只搜索规则：按全量名匹配的纯过滤条件 ───────────────────────────────────
# 普通规则跑在切完 "owner/" 前缀的结果上，pattern 带前缀（如
# deepseek-ai/deepseek-v4）永远搜不到。只搜索规则拿全量名匹配：命中即保留
# 该模型，不参与改名——名字沿用默认形态（切前缀 + 其它改写规则）。
def test_search_only_rule_matches_full_name_with_owner_prefix():
    rules = [_rule(r"^deepseek-ai/deepseek-v4$", "", search_only=True)]
    assert model_id_matches_search_rules("deepseek-ai/deepseek-v4", rules) is True
    assert model_id_matches_search_rules("openai/gpt-4o", rules) is False


def test_search_only_rule_uses_re_search_semantics():
    """命中口径与 re.sub 一致：pattern 在名字里出现即命中，不要求锚定。"""
    rules = [_rule(r"deepseek-v4", "", search_only=True)]
    assert model_id_matches_search_rules("deepseek-ai/deepseek-v4-chat", rules) is True


def test_search_only_rule_never_rewrites():
    """只搜索规则不参与改名：改写链整条跳过，结果仍是切完前缀的默认名。"""
    rules = [_rule(r"deepseek", "renamed", search_only=True)]
    assert apply_model_id_rewrite_rules("deepseek-ai/deepseek-v4", rules) == "deepseek-v4"


def test_search_rule_runs_alongside_rewrite_rules():
    """搜索规则过滤、改写规则改名，各管各的：命中搜索保留，名字由改写规则定。"""
    rules = [
        _rule(r"^deepseek-ai/", "", search_only=True),
        _rule(r"^deepseek-", "ds-"),
    ]
    assert model_id_matches_search_rules("deepseek-ai/deepseek-v4", rules) is True
    assert apply_model_id_rewrite_rules("deepseek-ai/deepseek-v4", rules) == "ds-v4"


def test_disabled_search_rule_ignored():
    rules = [_rule(r"x", "", search_only=True, enabled=False)]
    assert model_id_matches_search_rules("x-model", rules) is False


def test_bad_search_regex_skipped_not_raised():
    """坏正则跳过该条不炸，后面的规则照常生效——口径与改写侧一致。"""
    rules = [_rule(r"[", "", search_only=True), _rule(r"ok", "", search_only=True)]
    assert model_id_matches_search_rules("an-ok-model", rules) is True


def test_search_only_counts_as_having_rules():
    """只配搜索规则也算「配了规则」——自动更新要按它过滤，不能视同没配置。"""
    ch = Channel("demo", {"model_id_rewrite_rules": [_rule(r"^glm", "", search_only=True)]})
    assert ch.has_model_id_rewrite_rules() is True


def test_channel_matches_search_rules_helper():
    ch = Channel("demo", {"model_id_rewrite_rules": [_rule(r"^1111/", "", search_only=True)]})
    assert ch.matches_search_rules("1111/glm-5.2") is True
    assert ch.matches_search_rules("2222/glm-5.2") is False


def test_normalize_carries_search_only():
    entries = normalize_model_id_rewrite_rules([_rule(r"x", search_only=True), _rule(r"y")])
    assert entries[0]["search_only"] is True
    assert entries[1]["search_only"] is False


def test_search_rule_inside_template_expands():
    """模版里的只搜索规则照样就地展开生效（活引用）。"""
    set_model_rule_template_cache([{"id": "tpl", "name": "t", "rules": [_rule(r"^acme/", "", search_only=True)]}])
    assert model_id_matches_search_rules("acme/glm", [_ref("tpl")]) is True
    assert model_id_matches_search_rules("other/glm", [_ref("tpl")]) is False
