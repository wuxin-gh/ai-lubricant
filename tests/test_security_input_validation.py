"""输入校验检测器测试"""

from security.detectors.input_validation import detect
from security.types import ScanRequest


def _req(messages: list[dict], **kwargs) -> ScanRequest:
    return ScanRequest(messages=messages, **kwargs)


class TestInputValidation:
    def test_empty_messages_detected(self):
        req = _req([])
        tags = detect(req)
        assert any(t.tag == "input:empty-messages" for t in tags)
        assert any(t.severity == "high" for t in tags)

    def test_valid_messages_no_tags(self):
        req = _req([
            {"role": "system", "content": "You are a helpful assistant"},
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi there!"},
        ])
        tags = detect(req)
        assert len(tags) == 0

    def test_invalid_role_detected(self):
        req = _req([{"role": "moderator", "content": "test"}])
        tags = detect(req)
        assert any(t.tag == "input:invalid-role" for t in tags)

    def test_message_too_large(self):
        large_content = "x" * 600000  # ~600KB
        req = _req([{"role": "user", "content": large_content}])
        tags = detect(req)
        assert any(t.tag == "input:message-too-large" for t in tags)

    def test_normal_size_no_warning(self):
        req = _req([{"role": "user", "content": "Hello world"}])
        tags = detect(req)
        size_tags = [t for t in tags if t.tag == "input:message-too-large"]
        assert len(size_tags) == 0

    def test_too_many_messages(self):
        messages = [{"role": "user", "content": f"msg {i}"} for i in range(600)]
        req = _req(messages)
        tags = detect(req)
        assert any(t.tag == "input:too-many-messages" for t in tags)

    def test_normal_message_count_no_warning(self):
        messages = [{"role": "user", "content": f"msg {i}"} for i in range(10)]
        req = _req(messages)
        tags = detect(req)
        count_tags = [t for t in tags if t.tag == "input:too-many-messages"]
        assert len(count_tags) == 0

    def test_content_parts_size_estimation(self):
        large_text = "y" * 600000
        req = _req([{
            "role": "user",
            "content": [{"type": "text", "text": large_text}]
        }])
        tags = detect(req)
        assert any(t.tag == "input:message-too-large" for t in tags)

    def test_empty_messages_short_circuits(self):
        """空消息只返回 empty-messages 标签，不检查其他"""
        req = _req([])
        tags = detect(req)
        assert len(tags) == 1
        assert tags[0].tag == "input:empty-messages"

    def test_multiple_issues_detected(self):
        """多个问题同时检测"""
        messages = [{"role": "bad_role", "content": "x" * 600000} for _ in range(600)]
        req = _req(messages)
        tags = detect(req)
        tag_names = {t.tag for t in tags}
        assert "input:invalid-role" in tag_names
        assert "input:too-many-messages" in tag_names
        assert "input:message-too-large" in tag_names
