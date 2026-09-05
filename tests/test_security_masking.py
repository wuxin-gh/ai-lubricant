"""凭据遮蔽/还原测试"""

from security.masking import (
    is_sensitive_header,
    mask_credentials,
    restore_credentials,
    restore_response_body,
    sanitize_header_value,
)
from security.masking_rules import BUILTIN_RULES


class TestSanitizeHeaderValue:
    def test_api_key_header_keeps_head_and_tail(self):
        assert sanitize_header_value("x-api-key", "sk-II2KB0abcdefgHemfn") == "sk-I...emfn"

    def test_api_key_alias_headers_also_partial(self):
        for key in ("X-Api-Key", "api-key", "x-goog-api-key", "apikey"):
            assert sanitize_header_value(key, "sk-II2KB0abcdefgHemfn") == "sk-I...emfn"

    def test_short_credential_fully_starred(self):
        assert sanitize_header_value("x-api-key", "sk-123") == "******"

    def test_authorization_keeps_scheme(self):
        assert sanitize_header_value("Authorization", "Bearer sk-II2KB0abcdefgHemfn") == "Bearer sk-I...emfn"

    def test_cookie_stays_fully_masked(self):
        assert sanitize_header_value("Cookie", "session=abcdefghijklmnop") == "***"

    def test_non_sensitive_header_passthrough(self):
        assert sanitize_header_value("content-type", "application/json") == "application/json"

    def test_is_sensitive_header(self):
        assert is_sensitive_header("x-api-key")
        assert is_sensitive_header("Authorization")
        assert is_sensitive_header("cookie")
        assert not is_sensitive_header("content-type")

    def test_per_protocol_auth_headers_all_partial(self):
        """各上游协议的认证头名字不同，都要保留首尾片段而非整体遮蔽。"""
        key = "sk-II2KB0abcdefgHemfn"
        # anthropic / gemini(api_key_header) / gemini(bearer) / openai+responses / proxy
        assert sanitize_header_value("x-api-key", key) == "sk-I...emfn"
        assert sanitize_header_value("x-goog-api-key", key) == "sk-I...emfn"
        assert sanitize_header_value("Authorization", f"Bearer {key}") == "Bearer sk-I...emfn"
        assert sanitize_header_value("proxy-authorization", f"Basic {key}") == "Basic sk-I...emfn"
        for name in ("x-api-key", "x-goog-api-key", "Authorization", "proxy-authorization"):
            assert "***" not in sanitize_header_value(name, key)

    def test_protocol_metadata_headers_not_masked(self):
        """协议元数据头不是凭据，不能被误遮，否则排查时看不到协议形态。"""
        assert sanitize_header_value("anthropic-version", "2023-06-01") == "2023-06-01"
        assert sanitize_header_value("anthropic-beta", "context-1m-2025-08-07") == "context-1m-2025-08-07"
        assert sanitize_header_value("x-goog-api-client", "gl-node/22.9.0") == "gl-node/22.9.0"


class TestMaskCredentials:
    def test_mask_anthropic_key(self):
        body = {"messages": [{"role": "user", "content": "key: sk-ant-api03-xKd9Lm2Qp8Rv5Tx1Zb6Nc4Gg0Js_aBcDeFgHiJkLmNoPqRsTuVwXyZ123456789"}]}
        result = mask_credentials(body)
        assert "sk-ant-api03-xKd9Lm2Qp8Rv5Tx1Zb6Nc4Gg0Js_aBcDeFgHiJkLmNoPqRsTuVwXyZ123456789" not in str(result.masked_body)
        assert len(result.restore_map) > 0

    def test_restore_reverses_masking(self):
        real_key = "sk-ant-api03-xKd9Lm2Qp8Rv5Tx1Zb6Nc4Gg0Js_aBcDeFgHiJkLmNoPqRsTuVwXyZ123456789"
        body = {"messages": [{"role": "user", "content": f"key: {real_key}"}]}
        result = mask_credentials(body)

        masked_content = result.masked_body["messages"][0]["content"]
        restored_content = restore_credentials(masked_content, result.restore_map)
        assert real_key in restored_content

    def test_mask_preserves_non_secret_text(self):
        body = {"messages": [{"role": "user", "content": "hello world, no secrets here"}]}
        result = mask_credentials(body)
        assert result.masked_body["messages"][0]["content"] == "hello world, no secrets here"
        assert len(result.restore_map) == 0

    def test_mask_content_parts_array(self):
        body = {"messages": [{"role": "user", "content": [
            {"type": "text", "text": "key: sk-ant-api03-xKd9Lm2Qp8Rv5Tx1Zb6Nc4Gg0Js_aBcDeFgHiJkLmNoPqRsTuVwXyZ123456789"},
            {"type": "text", "text": "normal text"},
        ]}]}
        result = mask_credentials(body)
        parts = result.masked_body["messages"][0]["content"]
        assert "sk-ant-api03-xKd9Lm2Qp8Rv5Tx1Zb6Nc4Gg0Js_aBcDeFgHiJkLmNoPqRsTuVwXyZ123456789" not in str(parts)
        assert parts[1]["text"] == "normal text"

    def test_mask_openai_key(self):
        body = {"messages": [{"role": "user", "content": "sk-proj-T3BlbkFJH2kPq7Rj4tXcV8B1mYwE6sZ0dLfG5uIoA3eK7D3ShQ2NvGxRm"}]}
        result = mask_credentials(body)
        assert "sk-proj-T3BlbkFJH2kPq7Rj4tXcV8B1mYwE6sZ0dLfG5uIoA3eK7D3ShQ2NvGxRm" not in str(result.masked_body)

    def test_mask_aws_key(self):
        body = {"messages": [{"role": "user", "content": "aws: AKIAIOSFODNN7EXAMPLE"}]}
        result = mask_credentials(body)
        assert "AKIAIOSFODNN7EXAMPLE" not in str(result.masked_body)

    def test_multiple_secrets_same_type_unique_fakes(self):
        """同类型多个不同密钥应分配不同的 fake"""
        body = {"messages": [
            {"role": "user", "content": "key1: sk-ant-api03-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
            {"role": "user", "content": "key2: sk-ant-api03-BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"},
        ]}
        result = mask_credentials(body)
        assert len(result.restore_map) >= 2

    def test_deep_copy_original_unchanged(self):
        original_content = "key: sk-ant-api03-xKd9Lm2Qp8Rv5Tx1Zb6Nc4Gg0Js_aBcDeFgHiJkLmNoPqRsTuVwXyZ123456789"
        body = {"messages": [{"role": "user", "content": original_content}]}
        mask_credentials(body)
        assert body["messages"][0]["content"] == original_content

    def test_restore_response_body(self):
        real_key = "sk-ant-api03-xKd9Lm2Qp8Rv5Tx1Zb6Nc4Gg0Js_aBcDeFgHiJkLmNoPqRsTuVwXyZ123456789"
        body = {"messages": [{"role": "user", "content": f"key: {real_key}"}]}
        result = mask_credentials(body)

        fake = list(result.restore_map.keys())[0]
        response_body = {"choices": [{"message": {"role": "assistant", "content": f"your key is {fake}"}}]}
        restored = restore_response_body(response_body, result.restore_map)
        assert real_key in restored["choices"][0]["message"]["content"]

    def test_builtin_rules_not_empty(self):
        assert len(BUILTIN_RULES) > 0

    def test_mask_empty_messages(self):
        body = {"messages": []}
        result = mask_credentials(body)
        assert result.masked_body == {"messages": []}
        assert len(result.restore_map) == 0

    def test_mask_no_messages_key(self):
        body = {"model": "gpt-4"}
        result = mask_credentials(body)
        assert result.masked_body == {"model": "gpt-4"}
        assert len(result.restore_map) == 0

    def test_github_pat_masked(self):
        body = {"messages": [{"role": "user", "content": "token: ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef12"}]}
        result = mask_credentials(body)
        assert "ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdef12" not in str(result.masked_body)

    def test_bearer_token_masked(self):
        body = {"messages": [{"role": "user", "content": "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"}]}
        result = mask_credentials(body)
        assert "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" not in str(result.masked_body)
