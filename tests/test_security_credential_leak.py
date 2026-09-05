"""凭据泄露检测器测试"""

from security.detectors.credential_leak import detect
from security.types import ScanRequest


def _req(messages: list[dict]) -> ScanRequest:
    return ScanRequest(messages=messages)


class TestCredentialLeak:
    def test_detect_anthropic_key(self):
        req = _req([{"role": "user", "content": "用这个 key: sk-ant-api03-xKd9Lm2Qp8Rv5Tx1Zb6Nc4Gg0Js_aBcDeFgHiJkLmNoPqRsTuVwXyZ123456789"}])
        tags = detect(req)
        assert any(t.tag == "secret:anthropic-key" for t in tags)

    def test_detect_openai_key(self):
        req = _req([{"role": "user", "content": "my key is sk-proj-T3BlbkFJH2kPq7Rj4tXcV8B1mYwE6sZ0dLfG5uIoA3eK7D3ShQ2NvGxRm"}])
        tags = detect(req)
        assert any(t.tag == "secret:openai-key" for t in tags)

    def test_detect_bearer_token(self):
        req = _req([{"role": "user", "content": "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"}])
        tags = detect(req)
        assert any(t.tag == "secret:bearer-token" for t in tags)

    def test_detect_aws_key(self):
        req = _req([{"role": "user", "content": "aws key: AKIAIOSFODNN7EXAMPLE"}])
        tags = detect(req)
        assert any(t.tag == "secret:aws-akid" for t in tags)

    def test_detect_github_pat(self):
        req = _req([{"role": "user", "content": "token: ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef12"}])
        tags = detect(req)
        assert any(t.tag == "secret:github-pat" for t in tags)

    def test_no_false_positive_on_short_strings(self):
        req = _req([{"role": "user", "content": "sk-123"}])
        tags = detect(req)
        assert len(tags) == 0

    def test_severity_is_high(self):
        req = _req([{"role": "user", "content": "sk-ant-api03-xKd9Lm2Qp8Rv5Tx1Zb6Nc4Gg0Js_aBcDeFgHiJkLmNoPqRsTuVwXyZ123456789"}])
        tags = detect(req)
        assert all(t.severity == "high" for t in tags)

    def test_prefix_only_in_detail(self):
        req = _req([{"role": "user", "content": "sk-ant-api03-xKd9Lm2Qp8Rv5Tx1Zb6Nc4Gg0Js_aBcDeFgHiJkLmNoPqRsTuVwXyZ123456789"}])
        tags = detect(req)
        for t in tags:
            if t.tag.startswith("secret:"):
                assert len(t.detail.get("prefix", "")) <= 6

    def test_empty_messages(self):
        req = _req([])
        tags = detect(req)
        assert len(tags) == 0

    def test_content_parts_array(self):
        req = _req([{
            "role": "user",
            "content": [
                {"type": "text", "text": "key: sk-ant-api03-xKd9Lm2Qp8Rv5Tx1Zb6Nc4Gg0Js_aBcDeFgHiJkLmNoPqRsTuVwXyZ123456789"},
            ]
        }])
        tags = detect(req)
        assert any(t.tag == "secret:anthropic-key" for t in tags)

    def test_extra_text_scanned(self):
        req = ScanRequest(messages=[], extra_text="sk-ant-api03-xKd9Lm2Qp8Rv5Tx1Zb6Nc4Gg0Js_aBcDeFgHiJkLmNoPqRsTuVwXyZ123456789")
        tags = detect(req)
        assert any(t.tag == "secret:anthropic-key" for t in tags)

    def test_multiple_secrets_detected(self):
        req = _req([{"role": "user", "content": "anthropic: sk-ant-api03-xKd9Lm2Qp8Rv5Tx1Zb6Nc4Gg0Js_aBcDeFgHiJkLmNoPqRsTuVwXyZ123456789 and aws: AKIAIOSFODNN7EXAMPLE"}])
        tags = detect(req)
        tag_names = {t.tag for t in tags}
        assert "secret:anthropic-key" in tag_names
        assert "secret:aws-akid" in tag_names

    def test_assistant_messages_scanned(self):
        """凭据泄露检测扫描所有消息（不限 role）"""
        req = _req([{"role": "assistant", "content": "key: sk-ant-api03-xKd9Lm2Qp8Rv5Tx1Zb6Nc4Gg0Js_aBcDeFgHiJkLmNoPqRsTuVwXyZ123456789"}])
        tags = detect(req)
        assert any(t.tag == "secret:anthropic-key" for t in tags)
