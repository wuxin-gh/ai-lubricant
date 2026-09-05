"""敏感数据规则管理 API 测试"""

import asyncio

import pytest
from fastapi import HTTPException
from fastapi.responses import JSONResponse

import admin
import security.sensitive_rules as sr


def run(coro):
    return asyncio.run(coro)


@pytest.fixture
def fake_admin(monkeypatch):
    """绕过鉴权与操作日志，使用内存配置存储"""
    store = {"security": {}}

    async def fake_require_admin(token):
        if token != "Bearer admin":
            raise HTTPException(status_code=401, detail="需要登录")
        return token

    async def fake_log(*args, **kwargs):
        return None

    async def fake_read():
        # 返回深拷贝避免端点内修改污染存储，模拟真实读写语义
        import copy
        return copy.deepcopy(store)

    async def fake_write(cfg):
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(admin, "_require_admin", fake_require_admin)
    monkeypatch.setattr(admin, "_log_operation", fake_log)
    monkeypatch.setattr(admin, "_read_main_cfg", fake_read)
    monkeypatch.setattr(admin, "_write_main_cfg", fake_write)
    sr.refresh_cache()
    return store


def test_requires_admin(fake_admin):
    with pytest.raises(HTTPException) as exc:
        run(admin.get_security_rules(token=None))
    assert exc.value.status_code == 401


def test_get_seeds_builtin_rules_when_absent(fake_admin):
    res = run(admin.get_security_rules(token="Bearer admin"))
    rules = res["rules"]
    assert len(rules) == 10
    # 落盘后再次读取应直接返回已存规则
    assert isinstance(fake_admin["security"]["sensitive_rules"], list)
    assert len(fake_admin["security"]["sensitive_rules"]) == 10
    # eth-private-key 默认不参与检测
    eth = next(r for r in rules if r["id"] == "eth-private-key")
    assert eth["detect_enabled"] is False
    assert eth["mask_enabled"] is True


def test_get_does_not_reseed_existing(fake_admin):
    fake_admin["security"]["sensitive_rules"] = [
        {"id": "only", "label": "Only", "pattern": "abc", "fake": "X"}
    ]
    res = run(admin.get_security_rules(token="Bearer admin"))
    assert [r["id"] for r in res["rules"]] == ["only"]


def test_put_replaces_and_reorders(fake_admin):
    rules = [
        {"id": "a", "label": "A", "pattern": "aaa+", "fake": "FA"},
        {"id": "b", "label": "B", "pattern": "bbb+", "fake": "FB"},
    ]
    res = run(admin.update_security_rules({"rules": rules}, token="Bearer admin"))
    assert res["ok"] is True
    assert [r["id"] for r in res["rules"]] == ["a", "b"]

    # 调序：全量替换
    reordered = list(reversed(res["rules"]))
    res2 = run(admin.update_security_rules({"rules": reordered}, token="Bearer admin"))
    assert [r["id"] for r in res2["rules"]] == ["b", "a"]


def test_put_rejects_invalid_regex_without_saving(fake_admin):
    fake_admin["security"]["sensitive_rules"] = [
        {"id": "keep", "label": "Keep", "pattern": "ok+", "fake": "F"}
    ]
    bad = [{"id": "bad", "label": "Bad", "pattern": "(unclosed", "fake": "F"}]
    res = run(admin.update_security_rules({"rules": bad}, token="Bearer admin"))
    assert isinstance(res, JSONResponse)
    assert res.status_code == 400
    # 未落盘：原规则保持不变
    assert fake_admin["security"]["sensitive_rules"][0]["id"] == "keep"


def test_put_rejects_duplicate_ids(fake_admin):
    dup = [
        {"id": "x", "label": "X1", "pattern": "a+", "fake": "F"},
        {"id": "x", "label": "X2", "pattern": "b+", "fake": "F"},
    ]
    res = run(admin.update_security_rules({"rules": dup}, token="Bearer admin"))
    assert isinstance(res, JSONResponse)
    assert res.status_code == 400


def test_put_rejects_non_list(fake_admin):
    with pytest.raises(HTTPException) as exc:
        run(admin.update_security_rules({"rules": "nope"}, token="Bearer admin"))
    assert exc.value.status_code == 400


def test_delete_builtin_via_full_replace(fake_admin):
    run(admin.get_security_rules(token="Bearer admin"))  # 播种 10 条
    stored = fake_admin["security"]["sensitive_rules"]
    remaining = [r for r in stored if r["id"] != "anthropic-key"]
    res = run(admin.update_security_rules({"rules": remaining}, token="Bearer admin"))
    ids = [r["id"] for r in res["rules"]]
    assert "anthropic-key" not in ids
    assert len(ids) == 9


def test_test_endpoint_returns_fragments_for_saved_rules(fake_admin, monkeypatch):
    # 测试端点的「全部已存规则」分支走加载器，需直接打补丁加载器
    saved = [
        {"id": "ak", "label": "Anthropic", "pattern": r"sk-ant-[A-Za-z0-9_-]{20,}", "fake": "F", "detect_enabled": True, "mask_enabled": True, "enabled": True},
    ]
    monkeypatch.setattr(sr, "_load_raw_rules", lambda: saved)
    sr.refresh_cache()
    text = "key sk-ant-api03-xKd9Lm2Qp8Rv5Tx1Zb6Nc4Gg0Js_aBcDeFgHiJkLmNoP here"
    res = run(admin.test_security_rules({"text": text}, token="Bearer admin"))
    assert res["ok"] is True
    assert len(res["matches"]) == 1
    assert res["matches"][0]["rule_id"] == "ak"
    frag = res["matches"][0]["fragments"][0]
    assert frag["match"].startswith("sk-ant-")
    assert frag["end"] > frag["start"]


def test_test_endpoint_candidate_rule(fake_admin):
    res = run(admin.test_security_rules(
        {"text": "abc123abc", "rule": {"id": "c", "label": "C", "pattern": "abc", "fake": "F"}},
        token="Bearer admin",
    ))
    assert res["ok"] is True
    assert res["matches"][0]["rule_id"] == "c"
    assert len(res["matches"][0]["fragments"]) == 2


def test_test_endpoint_rejects_invalid_candidate(fake_admin):
    res = run(admin.test_security_rules(
        {"text": "x", "rule": {"id": "c", "label": "C", "pattern": "(unclosed", "fake": "F"}},
        token="Bearer admin",
    ))
    assert isinstance(res, JSONResponse)
    assert res.status_code == 400
