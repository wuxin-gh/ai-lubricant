"""可配置敏感数据规则加载器测试"""

import pytest

import security.sensitive_rules as sr
from security.detectors.credential_leak import detect
from security.masking import mask_credentials
from security.types import ScanRequest


ANTHROPIC_KEY = "sk-ant-api03-xKd9Lm2Qp8Rv5Tx1Zb6Nc4Gg0Js_aBcDeFgHiJkLmNoPqRsTuVwXyZ123456789"


@pytest.fixture(autouse=True)
def _clean_cache():
    """每个用例前后清空规则缓存，避免相互污染"""
    sr.refresh_cache()
    yield
    sr.refresh_cache()


def _patch_rules(monkeypatch, raw_rules):
    """让加载器返回指定规则列表"""
    monkeypatch.setattr(sr, "_load_raw_rules", lambda: raw_rules)
    sr.refresh_cache()


class TestSeed:
    def test_seed_count_matches_builtin(self):
        seeded = sr.seed_sensitive_rules()
        assert len(seeded) == len(sr.BUILTIN_RULES)
        assert len(seeded) == 10

    def test_eth_private_key_detect_disabled_by_default(self):
        seeded = {r["id"]: r for r in sr.seed_sensitive_rules()}
        assert seeded["eth-private-key"]["detect_enabled"] is False
        assert seeded["eth-private-key"]["mask_enabled"] is True

    def test_non_eth_rules_detect_enabled(self):
        seeded = {r["id"]: r for r in sr.seed_sensitive_rules()}
        assert seeded["anthropic-key"]["detect_enabled"] is True
        assert seeded["openai-key"]["detect_enabled"] is True


class TestCompile:
    def test_invalid_regex_skipped_not_crash(self):
        raw = [
            {"id": "bad", "label": "坏规则", "pattern": "([unclosed", "fake": "X"},
            {"id": "good", "label": "好规则", "pattern": "secret-[0-9]+", "fake": "X"},
        ]
        compiled = sr.compile_rules(raw)
        ids = [r.id for r in compiled]
        assert "bad" not in ids
        assert "good" in ids

    def test_ignorecase_flag_applied(self):
        raw = [{"id": "ci", "label": "ci", "pattern": "abc", "ignorecase": True, "fake": "X"}]
        compiled = sr.compile_rules(raw)
        assert compiled[0].pattern.search("ABC") is not None


class TestCache:
    def test_cache_returns_same_instance_for_unchanged_config(self, monkeypatch):
        raw = [{"id": "a", "label": "a", "pattern": "abc", "fake": "X"}]
        _patch_rules(monkeypatch, raw)
        first = sr.get_sensitive_rules()
        second = sr.get_sensitive_rules()
        assert first is second

    def test_refresh_cache_picks_up_changes(self, monkeypatch):
        _patch_rules(monkeypatch, [{"id": "a", "label": "a", "pattern": "abc", "fake": "X"}])
        first = sr.get_sensitive_rules()
        _patch_rules(monkeypatch, [{"id": "b", "label": "b", "pattern": "xyz", "fake": "X"}])
        second = sr.get_sensitive_rules()
        assert first is not second
        assert [r.id for r in second] == ["b"]


class TestFiltering:
    def test_detection_filters_enabled_and_detect(self, monkeypatch):
        _patch_rules(monkeypatch, [
            {"id": "d1", "label": "d1", "pattern": "a", "fake": "X", "detect_enabled": True, "mask_enabled": False, "enabled": True},
            {"id": "d2", "label": "d2", "pattern": "b", "fake": "X", "detect_enabled": False, "mask_enabled": True, "enabled": True},
            {"id": "d3", "label": "d3", "pattern": "c", "fake": "X", "detect_enabled": True, "mask_enabled": True, "enabled": False},
        ])
        det_ids = [r.id for r in sr.get_detection_rules()]
        mask_ids = [r.id for r in sr.get_masking_rules()]
        assert det_ids == ["d1"]
        assert mask_ids == ["d2"]

    def test_masking_preserves_order(self, monkeypatch):
        _patch_rules(monkeypatch, [
            {"id": "first", "label": "first", "pattern": "a", "fake": "X"},
            {"id": "second", "label": "second", "pattern": "b", "fake": "Y"},
        ])
        assert [r.id for r in sr.get_masking_rules()] == ["first", "second"]


class TestValidate:
    def test_valid_rule_returns_none(self):
        assert sr.validate_rule({"id": "x", "label": "x", "pattern": "a+", "fake": "X"}) is None

    def test_empty_id_rejected(self):
        assert sr.validate_rule({"id": "", "label": "x", "pattern": "a", "fake": "X"})

    def test_empty_pattern_rejected(self):
        assert sr.validate_rule({"id": "x", "label": "x", "pattern": "", "fake": "X"})

    def test_invalid_regex_rejected(self):
        assert sr.validate_rule({"id": "x", "label": "x", "pattern": "([bad", "fake": "X"})

    def test_invalid_severity_normalized_not_rejected(self):
        # severity 非法会被 normalize 成 high，因此校验通过
        assert sr.validate_rule({"id": "x", "label": "x", "pattern": "a", "severity": "bogus", "fake": "X"}) is None

    def test_group_out_of_range_rejected(self):
        # 正则没有捕获组，group=1 越界
        err = sr.validate_rule({"id": "x", "label": "x", "pattern": "abc", "group": 1, "fake": "X"})
        assert err and "group" in err

    def test_group_in_range_ok(self):
        assert sr.validate_rule({"id": "x", "label": "x", "pattern": "(abc)", "group": 1, "fake": "X"}) is None


class TestIntegrationWithDetectAndMask:
    def test_custom_rule_detected_and_masked(self, monkeypatch):
        _patch_rules(monkeypatch, [{
            "id": "my-secret", "label": "我的密钥",
            "pattern": r"MYSEC-[A-Za-z0-9]{10,}", "fake": "MYSEC-REDACTED",
            "severity": "warn", "detect_enabled": True, "mask_enabled": True, "enabled": True,
        }])
        text = "token is MYSEC-abcdefghij1234"
        tags = detect(ScanRequest(messages=[{"role": "user", "content": text}]))
        assert any(t.tag == "secret:my-secret" and t.severity == "warn" for t in tags)

        masked = mask_credentials({"messages": [{"role": "user", "content": text}]})
        assert "MYSEC-abcdefghij1234" not in str(masked.masked_body)
        assert "MYSEC-REDACTED" in str(masked.masked_body)

    def test_disabled_rule_not_used(self, monkeypatch):
        _patch_rules(monkeypatch, [{
            "id": "off", "label": "off", "pattern": r"MYSEC-[A-Za-z0-9]{10,}",
            "fake": "X", "enabled": False,
        }])
        tags = detect(ScanRequest(messages=[{"role": "user", "content": "MYSEC-abcdefghij1234"}]))
        assert not any(t.tag == "secret:off" for t in tags)

    def test_builtin_anthropic_still_works_via_loader(self):
        # 不打补丁，走真实播种规则
        sr.refresh_cache()
        tags = detect(ScanRequest(messages=[{"role": "user", "content": ANTHROPIC_KEY}]))
        assert any(t.tag == "secret:anthropic-key" for t in tags)
