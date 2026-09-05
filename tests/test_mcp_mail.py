"""Regression tests for the built-in mail MCP client and adapter."""
import asyncio
import base64
import os
import sys

import pytest

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from mcp_builtin.mail.client import normalize_mail, parse_mail_body
from mcp_runtime.builtin_plugins import mail_plugin
from mcp_runtime.plugin_loader import PluginContext, PluginRegistrar


MIME_SAMPLE = """From: sender@example.com
To: target@example.com
Subject: hello
MIME-Version: 1.0
Content-Type: multipart/alternative; boundary=mail-boundary

--mail-boundary
Content-Type: text/plain; charset=utf-8

Plain body
--mail-boundary
Content-Type: text/html; charset=utf-8

<b>HTML body</b>
--mail-boundary--
"""


def test_parse_mail_body_extracts_text_and_html():
    text, html = parse_mail_body(MIME_SAMPLE)
    assert text.strip() == "Plain body"
    assert html.strip() == "<b>HTML body</b>"


def test_normalize_mail_preserves_original_message_source():
    normalized = normalize_mail({"raw": MIME_SAMPLE, "address": "source@example.com", "subject": "hello"})
    assert normalized["raw"] == MIME_SAMPLE
    assert normalized["raw_content"] == MIME_SAMPLE
    assert normalized["text_content"].strip() == "Plain body"
    assert normalized["html_content"].strip() == "<b>HTML body</b>"


def test_normalize_mail_uses_direct_content_without_raw():
    normalized = normalize_mail({"content": "验证码：123456", "title": "登录验证码"})
    assert normalized["text_content"] == "验证码：123456"
    assert normalized["subject"] == "登录验证码"


def test_normalize_mail_received_address_from_to_header():
    normalized = normalize_mail({"raw": MIME_SAMPLE, "address": "login@example.com"})
    assert normalized["received_address"] == "target@example.com"


def test_normalize_mail_received_address_falls_back_to_upstream_address():
    normalized = normalize_mail({"content": "验证码", "address": "login@example.com"})
    assert normalized["received_address"] == "login@example.com"


def test_normalize_mail_repairs_mojibake_and_derives_text_from_html():
    normalized = normalize_mail({
        "subject": "ä¸­æ–‡ä¸»é¢˜",
        "html": "<div>ä¸­æ–‡æ­£æ–‡<br>123456</div>",
    })
    assert normalized["subject"] == "中文主题"
    assert "中文正文" in normalized["html_content"]
    assert "中文正文" in normalized["text_content"]
    assert "123456" in normalized["text_content"]


def test_normalize_mail_decodes_encoded_subject_and_gb18030_body():
    body = "中文正文".encode("gb18030")
    raw = (
        "Subject: =?UTF-8?B?5Lit5paH5Li76aKY?=\n"
        "Content-Type: text/plain; charset=gb18030\n"
        "Content-Transfer-Encoding: base64\n\n"
        + base64.b64encode(body).decode("ascii")
    )
    normalized = normalize_mail({"raw": raw})
    assert normalized["subject"] == "中文主题"
    assert normalized["text_content"].strip() == "中文正文"


def _forwarded_raw(content_type: str, body: str) -> str:
    encoded = base64.b64encode(body.encode("utf-8")).decode("ascii")
    return (
        "Received: from forwarder.example.com\n"
        "To: alias@example.com\n"
        "X-Forwarded-By: qq\n\n"
        "From: Cancri <auth@nexusvai.xyz>\n"
        "To: target@qq.com\n"
        "Subject: =?UTF-8?B?5Lit5paH5Li76aKY?=\n"
        f"Content-Type: {content_type}; charset=utf-8\n"
        "Content-Transfer-Encoding: base64\n\n"
        f"{encoded}"
    )


def test_normalize_mail_decodes_forwarded_serialized_text_message():
    raw = _forwarded_raw("text/plain", "你的登录验证码是：932092")

    normalized = normalize_mail({"raw": raw})

    assert normalized["text_content"].strip() == "你的登录验证码是：932092"
    assert "Content-Transfer-Encoding" not in normalized["text_content"]


def test_normalize_mail_decodes_forwarded_serialized_html_message():
    raw = _forwarded_raw("text/html", "<div>你的验证码<br>246810</div>")

    normalized = normalize_mail({"raw": raw})

    assert normalized["html_content"].strip() == "<div>你的验证码<br>246810</div>"
    assert "你的验证码" in normalized["text_content"]
    assert "246810" in normalized["text_content"]


def test_parse_mail_body_does_not_treat_header_words_as_forwarded_message():
    raw = "Content-Type: text/plain; charset=utf-8\n\nSubject: quoted example\nordinary body"

    text, html = parse_mail_body(raw)

    assert text == "Subject: quoted example\nordinary body"
    assert html == ""


def test_parse_mail_body_decodes_only_one_forwarded_level():
    nested = _forwarded_raw("text/plain", "deep body")
    raw = _forwarded_raw("text/plain", nested)

    text, html = parse_mail_body(raw)

    assert text == nested
    assert html == ""


def test_mail_plugin_registers_dynamic_address_schema():
    reg = PluginRegistrar("mail")
    mail_plugin.register(reg)
    assert [tool.name for tool in reg.tools] == ["mail_info", "mail_list"]
    assert [action.name for action in reg.actions] == ["query_messages"]
    schema = next(tool.params for tool in reg.tools if tool.name == "mail_list")
    assert schema["required"] == ["address"]
    assert "enum" not in schema["properties"]["address"]


def test_mail_info_projects_suffix_and_aliases_without_secrets(monkeypatch):
    async def fake_scope(_token):
        return "2", {2}

    monkeypatch.setattr(mail_plugin, "_resolve_account_scope", fake_scope)
    ctx = PluginContext(plugin_name="mail", resources={"upstream_accounts": [
        {
            "id": 1, "instance_key": "1", "enabled": True,
            "display_name": "other", "username": "other@example.com",
            "mail_suffix": "example.com", "password": "must-not-leak",
            "secret_key": "must-not-leak", "addresses": [],
        },
        {
            "id": 2, "instance_key": "2", "enabled": True,
            "display_name": "selected", "username": "selected@example.net",
            "mail_suffix": "mail.example.net", "password": "must-not-leak",
            "secret_key": "must-not-leak", "addresses": [
                {"address": "alias@example.net", "source_address": "source@example.net", "is_primary": True},
            ],
        },
    ]})

    result = asyncio.run(mail_plugin._mail_info({}, ctx))

    assert result == {"accounts": [{
        "config_id": 2,
        "display_name": "selected",
        "username": "selected@example.net",
        "mail_suffix": "mail.example.net",
        "addresses": [{
            "address": "alias@example.net",
            "source_address": "source@example.net",
            "is_primary": True,
        }],
    }]}
    assert "password" not in str(result)
    assert "secret_key" not in str(result)
    assert "must-not-leak" not in str(result)


def test_mail_plugin_maps_alias_to_source_and_keeps_received_address_from_to_header(monkeypatch):
    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    # 上游按映射后的源邮箱查询；normalize_mail 会从 To 头解出实际收件邮箱。
    async def fake_list_mail(self, session, **kwargs):
        assert kwargs["address"] == "source@qq.com"
        return [normalize_mail({"raw": (
            "From: sender@example.com\n"
            "To: box+auto=810344058=xx.xin@qq.com\n"
            "Subject: code\n\n"
            "1234"
        )})]

    monkeypatch.setattr(mail_plugin.MailClient, "list_mail", fake_list_mail)
    ctx = PluginContext(plugin_name="mail", resources={"upstream_accounts": [{
        "enabled": True,
        "username": "login@example.com",
        "password": "secret",
        "base_url": "https://mail.example.com",
        "secret_key": "key",
        "addresses": [{"address": "target@cc.com", "source_address": "source@qq.com"}],
    }]})
    ctx.http_session = lambda: FakeSession()  # type: ignore[method-assign]

    result = asyncio.run(mail_plugin._mail_list({"address": "target@cc.com"}, ctx))
    # received_address 来自邮件 To 头（实际收件邮箱），而非映射的源邮箱。
    assert result["messages"][0]["received_address"] == "box+auto=810344058=xx.xin@qq.com"
    assert result["messages"][0]["requested_address"] == "target@cc.com"


def test_mail_plugin_received_address_falls_back_to_query_address_without_to_header(monkeypatch):
    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    async def fake_list_mail(self, session, **kwargs):
        assert kwargs["address"] == "source@qq.com"
        return [normalize_mail({"subject": "code", "content": "1234"})]

    monkeypatch.setattr(mail_plugin.MailClient, "list_mail", fake_list_mail)
    ctx = PluginContext(plugin_name="mail", resources={"upstream_accounts": [{
        "enabled": True,
        "username": "login@example.com",
        "password": "secret",
        "base_url": "https://mail.example.com",
        "secret_key": "key",
        "addresses": [{"address": "target@cc.com", "source_address": "source@qq.com"}],
    }]})
    ctx.http_session = lambda: FakeSession()  # type: ignore[method-assign]

    result = asyncio.run(mail_plugin._mail_list({"address": "target@cc.com"}, ctx))
    # 邮件缺少 To 头时，received_address 回退到本次查询替换后的源邮箱。
    assert result["messages"][0]["received_address"] == "source@qq.com"


def test_mail_action_allows_empty_address_for_selected_config(monkeypatch):
    class FakeSession:
        async def __aenter__(self): return self
        async def __aexit__(self, *_args): return False

    async def fake_list_mail(self, session, **kwargs):
        assert kwargs["address"] is None
        return []

    monkeypatch.setattr(mail_plugin.MailClient, "list_mail", fake_list_mail)
    ctx = PluginContext(plugin_name="mail", resources={"upstream_accounts": [{
        "id": 7, "enabled": True, "username": "u", "password": "p",
        "base_url": "https://mail.example.com", "secret_key": "s", "addresses": [],
    }]})
    ctx.http_session = lambda: FakeSession()  # type: ignore[method-assign]
    result = asyncio.run(mail_plugin._query_messages_action({"config_id": 7, "address": ""}, ctx))
    assert result["messages"] == []
    assert result["received_address"] == ""


def test_mail_query_instance_key_isolates_accounts(monkeypatch):
    """外部 MCP token 绑定单实例：instance_key 过滤后只查得到本实例账户，
    杜绝一个 token 读到其它实例配置的邮箱。"""
    class FakeSession:
        async def __aenter__(self): return self
        async def __aexit__(self, *_args): return False

    called: dict = {}

    async def fake_list_mail(self, session, **kwargs):
        called["username"] = self.username
        return []

    monkeypatch.setattr(mail_plugin.MailClient, "list_mail", fake_list_mail)
    # 两个实例的账户混在同一 upstream_accounts 快照里（运行时全量投影）。
    ctx = PluginContext(plugin_name="mail", resources={"upstream_accounts": [
        {"id": 1, "instance_key": "1", "enabled": True, "username": "a@x.com",
         "password": "p", "base_url": "https://m.x.com", "secret_key": "", "addresses": []},
        {"id": 2, "instance_key": "2", "enabled": True, "username": "b@x.com",
         "password": "p", "base_url": "https://m.x.com", "secret_key": "", "addresses": []},
    ]})
    ctx.http_session = lambda: FakeSession()  # type: ignore[method-assign]

    # 绑定实例 2 的 token：只应命中 b@x.com。
    result = asyncio.run(mail_plugin._query_messages(
        {"address": ""}, ctx, require_address=False, instance_key="2"))
    assert called["username"] == "b@x.com"
    assert result["messages"] == []


def test_mail_query_instance_key_rejects_other_instance_address(monkeypatch):
    """带 instance_key 时，查询另一实例配置的别名地址应报「未找到」，而非跨实例命中。"""
    class FakeSession:
        async def __aenter__(self): return self
        async def __aexit__(self, *_args): return False

    async def fake_list_mail(self, session, **kwargs):  # 不应被调用
        raise AssertionError("cross-instance address must not reach upstream")

    monkeypatch.setattr(mail_plugin.MailClient, "list_mail", fake_list_mail)
    ctx = PluginContext(plugin_name="mail", resources={"upstream_accounts": [
        {"id": 1, "instance_key": "1", "enabled": True, "username": "a@x.com",
         "password": "p", "base_url": "https://m.x.com", "secret_key": "",
         "addresses": [{"address": "alias@x.com", "source_address": "real@x.com"}]},
        {"id": 2, "instance_key": "2", "enabled": True, "username": "b@x.com",
         "password": "p", "base_url": "https://m.x.com", "secret_key": "", "addresses": []},
    ]})
    ctx.http_session = lambda: FakeSession()  # type: ignore[method-assign]

    # 实例 2 的 token 查实例 1 的别名 → 该地址不在实例 2 → 报未找到（require_address 命中地址但无账户）。
    with pytest.raises(ValueError):
        asyncio.run(mail_plugin._query_messages(
            {"address": "alias@x.com"}, ctx, require_address=True, instance_key="2"))



def test_mail_query_selected_account_rejects_ungranted_account(monkeypatch):
    """principal selected 模式下，即使账户在全量快照里也不能跨子权限读取。"""
    async def fake_list_mail(self, session, **kwargs):
        raise AssertionError("ungranted account must not reach upstream")

    monkeypatch.setattr(mail_plugin.MailClient, "list_mail", fake_list_mail)
    ctx = PluginContext(plugin_name="mail", resources={"upstream_accounts": [
        {"id": 1, "instance_key": "1", "enabled": True, "username": "a@x.com",
         "password": "p", "base_url": "https://m.x.com", "secret_key": "",
         "addresses": [{"address": "a@x.com", "source_address": "a@x.com"}]},
        {"id": 2, "instance_key": "2", "enabled": True, "username": "b@x.com",
         "password": "p", "base_url": "https://m.x.com", "secret_key": "",
         "addresses": [{"address": "b@x.com", "source_address": "b@x.com"}]},
    ]})

    with pytest.raises(ValueError):
        asyncio.run(mail_plugin._query_messages(
            {"address": "a@x.com"}, ctx, require_address=True,
            allowed_account_ids={2},
        ))


def test_mail_query_selected_account_uses_granted_account(monkeypatch):
    called = {}

    class FakeSession:
        async def __aenter__(self): return self
        async def __aexit__(self, *_args): return False

    async def fake_list_mail(self, session, **kwargs):
        called["username"] = self.username
        return []

    monkeypatch.setattr(mail_plugin.MailClient, "list_mail", fake_list_mail)
    ctx = PluginContext(plugin_name="mail", resources={"upstream_accounts": [
        {"id": 1, "instance_key": "1", "enabled": True, "username": "a@x.com",
         "password": "p", "base_url": "https://m.x.com", "secret_key": "", "addresses": []},
        {"id": 2, "instance_key": "2", "enabled": True, "username": "b@x.com",
         "password": "p", "base_url": "https://m.x.com", "secret_key": "", "addresses": []},
    ]})
    ctx.http_session = lambda: FakeSession()  # type: ignore[method-assign]

    asyncio.run(mail_plugin._query_messages(
        {"address": ""}, ctx, require_address=False, allowed_account_ids={2},
    ))
    assert called["username"] == "b@x.com"

    class FakeSession:
        async def __aenter__(self): return self
        async def __aexit__(self, *_args): return False

    async def fake_list_mail(self, session, **kwargs):
        return messages

    monkeypatch.setattr(mail_plugin.MailClient, "list_mail", fake_list_mail)
    ctx = PluginContext(plugin_name="mail", resources={"upstream_accounts": [{
        "enabled": True,
        "username": "login@example.com",
        "password": "secret",
        "base_url": "https://mail.example.com",
        "secret_key": "key",
        "addresses": [{"address": "target@cc.com", "source_address": "source@qq.com"}],
    }]})
    ctx.http_session = lambda: FakeSession()  # type: ignore[method-assign]
    return ctx


def test_mail_list_projection_drops_raw_html_and_unknown_fields(monkeypatch):
    # normalize_mail 保留完整 MIME/HTML；投影后这些不得暴露给模型。
    normalized = normalize_mail({"raw": MIME_SAMPLE, "address": "source@example.com", "subject": "hello"})
    normalized["internal_secret"] = "must-not-leak"
    ctx = _projection_ctx(monkeypatch, [normalized])

    result = asyncio.run(mail_plugin._mail_list({"address": "target@cc.com"}, ctx))
    msg = result["messages"][0]

    assert "raw" not in msg
    assert "raw_content" not in msg
    assert "html_content" not in msg
    assert "html" not in msg
    assert "internal_secret" not in msg
    # 有用字段保留。
    assert msg["subject"] == "hello"
    assert msg["text_content"].strip() == "Plain body"
    assert msg["received_address"] == "target@example.com"
    assert result["returned_count"] == 1


def test_mail_list_projection_forwarded_message_returns_decoded_text(monkeypatch):
    raw = _forwarded_raw("text/plain", "你的登录验证码是：932092")
    normalized = normalize_mail({"raw": raw, "address": "source@example.com"})
    ctx = _projection_ctx(monkeypatch, [normalized])

    result = asyncio.run(mail_plugin._mail_list({"address": "target@cc.com"}, ctx))
    msg = result["messages"][0]

    assert msg["text_content"].strip() == "你的登录验证码是：932092"
    assert "raw" not in msg
    assert "raw_content" not in msg
    assert "html_content" not in msg


def test_mail_list_projection_truncates_long_body_with_marker(monkeypatch):
    big_body = "验证码" * 5000  # 远超单封预算
    normalized = normalize_mail({"content": big_body, "subject": "code", "address": "a@b.com"})
    ctx = _projection_ctx(monkeypatch, [normalized])

    result = asyncio.run(mail_plugin._mail_list({"address": "target@cc.com"}, ctx))
    msg = result["messages"][0]

    assert msg["content_truncated"] is True
    assert msg["content_length"] == len(big_body)
    assert len(msg["text_content"]) == mail_plugin.MAIL_TEXT_BUDGET
    assert result["truncated_count"] == 1


def test_mail_list_projection_html_only_email_returns_decoded_text(monkeypatch):
    normalized = normalize_mail({"html": "<div>你的验证码<br>246810</div>", "address": "a@b.com"})
    ctx = _projection_ctx(monkeypatch, [normalized])

    result = asyncio.run(mail_plugin._mail_list({"address": "target@cc.com"}, ctx))
    msg = result["messages"][0]

    assert "html_content" not in msg
    assert "你的验证码" in msg["text_content"]
    assert "246810" in msg["text_content"]


def test_mail_list_projection_reports_pagination(monkeypatch):
    # 上游返回满 limit 条 → has_more/next_offset 提示还能翻页。
    messages = [normalize_mail({"content": f"body{i}", "address": "a@b.com"}) for i in range(20)]
    ctx = _projection_ctx(monkeypatch, messages)

    result = asyncio.run(mail_plugin._mail_list({"address": "target@cc.com", "limit": 20, "offset": 0}, ctx))

    assert result["returned_count"] == 20
    assert result["has_more"] is True
    assert result["next_offset"] == 20


def test_mail_list_projection_attachment_metadata_only(monkeypatch):
    normalized = normalize_mail({"content": "see attachment", "address": "a@b.com"})
    normalized["attachments"] = [
        {"filename": "report.pdf", "content_type": "application/pdf", "size": 1024, "data": "BASE64BYTES"},
    ]
    ctx = _projection_ctx(monkeypatch, [normalized])

    result = asyncio.run(mail_plugin._mail_list({"address": "target@cc.com"}, ctx))
    attachments = result["messages"][0]["attachments"]

    assert attachments == [{"filename": "report.pdf", "content_type": "application/pdf", "size": 1024}]
    assert "data" not in attachments[0]


def test_mail_list_projection_total_body_budget_enforced(monkeypatch):
    # 多封大邮件合计超过总预算 → 后续邮件正文被清空并标记截断，但仍返回元数据。
    per = mail_plugin.MAIL_TEXT_BUDGET
    count = (mail_plugin.MAIL_TOTAL_TEXT_BUDGET // per) + 3
    messages = [
        normalize_mail({"content": "x" * (per * 2), "subject": f"s{i}", "address": "a@b.com"})
        for i in range(count)
    ]
    ctx = _projection_ctx(monkeypatch, messages)

    result = asyncio.run(mail_plugin._mail_list({"address": "target@cc.com"}, ctx))
    total_text = sum(len(m["text_content"]) for m in result["messages"])

    assert total_text <= mail_plugin.MAIL_TOTAL_TEXT_BUDGET
    assert result["body_budget_exhausted"] is True
    # 即使正文预算耗尽，主题等元数据仍在。
    assert all(m.get("subject") for m in result["messages"])


def test_registrar_allows_same_name_across_capability_kinds():
    reg = PluginRegistrar("test")

    async def handler(args, ctx): return args

    reg.tool("shared")(handler)
    reg.action("shared")(handler)
    reg.view("shared")(handler)
    with pytest.raises(Exception, match="duplicate action name"):
        reg.action("shared")(handler)



    ctx = PluginContext(plugin_name="mail", resources={"upstream_accounts": []})
    with pytest.raises(ValueError, match="未找到已启用的邮箱配置"):
        asyncio.run(mail_plugin._mail_list({"address": "missing@example.com"}, ctx))
