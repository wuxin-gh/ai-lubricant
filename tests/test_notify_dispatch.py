"""Outbound notify dispatch: per-kind request shape and channel fan-out.

Two things are locked down here.

**Signing.** Each provider signs differently, and a wrong signature fails at the
provider with an HTTP 200 — so a silent regression looks like a healthy channel
that never delivers. Every HMAC is recomputed independently rather than compared
against a golden string, so the test states the rule instead of the output.

**Fan-out scope.** ``dispatch_event`` must reach the recipient's personal
channels *and* the channels owned by the teams they belong to. The team arm was
missing originally: team channels could be created, listed and test-fired, yet no
real event ever reached them — invisible from the UI.

``build_request``/``render_text`` are pure, so they need no stubbing. The DB paths
run against a stubbed asyncpg pool; no Postgres, no outbound HTTP.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import urllib.parse
import uuid
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest

from monkeycode_compat import notify_dispatch
from monkeycode_compat.notify_dispatch import build_request, dispatch_event, render_text


# ── request shape / signing ───────────────────────────────────────────────────


def test_dingtalk_signs_into_the_query_string():
    """HMAC over ``"{timestamp}\\n{secret}"``, base64, urlencoded, in the query."""
    secret = "SECdeadbeef"
    url, body, headers = build_request(
        "dingtalk", "https://oapi.dingtalk.com/robot/send?access_token=abc", secret,
        event_type="task.ended", text="hello",
    )
    query = parse_qs(urlsplit(url).query)
    # The caller's own query params must survive the join.
    assert query["access_token"] == ["abc"]

    timestamp = query["timestamp"][0]
    expected = base64.b64encode(
        hmac.new(
            secret.encode(), f"{timestamp}\n{secret}".encode(), hashlib.sha256
        ).digest()
    ).decode()
    # ``sign`` is urlencoded in the URL; parse_qs has already decoded it.
    assert query["sign"] == [expected]
    assert body == {"msgtype": "text", "text": {"content": "hello"}}
    assert headers == {}


def test_dingtalk_without_a_secret_is_unsigned():
    url, _, _ = build_request(
        "dingtalk", "https://oapi.dingtalk.com/robot/send?access_token=abc", "",
        event_type="task.ended", text="hi",
    )
    assert "sign=" not in url and "timestamp=" not in url


def test_dingtalk_sign_is_urlencoded():
    """Base64 yields ``+``/``=``, which must not reach the URL raw."""
    url, _, _ = build_request(
        "dingtalk", "https://oapi.dingtalk.com/robot/send", "s" * 32,
        event_type="task.ended", text="hi",
    )
    raw_sign = urlsplit(url).query.split("sign=")[1]
    assert "+" not in raw_sign
    assert raw_sign == urllib.parse.quote_plus(urllib.parse.unquote_plus(raw_sign))


def test_feishu_signs_an_empty_message_keyed_by_timestamp_and_secret():
    """Feishu inverts DingTalk: the timestamp+secret is the *key*, body empty."""
    secret = "FSsecret"
    url, body, _ = build_request(
        "feishu", "https://open.feishu.cn/open-apis/bot/v2/hook/xyz", secret,
        event_type="task.ended", text="hello",
    )
    assert url.endswith("/hook/xyz"), "feishu must not sign into the query"
    expected = base64.b64encode(
        hmac.new(f"{body['timestamp']}\n{secret}".encode(), b"", hashlib.sha256).digest()
    ).decode()
    assert body["sign"] == expected
    assert body["msg_type"] == "text"
    assert body["content"] == {"text": "hello"}


def test_feishu_without_a_secret_omits_the_signature_fields():
    _, body, _ = build_request(
        "feishu", "https://open.feishu.cn/hook/x", "", event_type="task.ended", text="hi"
    )
    assert "sign" not in body and "timestamp" not in body


def test_wecom_is_never_signed():
    """The bot key already lives in the URL; WeCom has no body signature."""
    url, body, headers = build_request(
        "wecom", "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=k", "ignored",
        event_type="task.ended", text="hello",
    )
    assert url.endswith("?key=k")
    assert body == {"msgtype": "text", "text": {"content": "hello"}}
    assert headers == {}


def test_generic_webhook_carries_structured_event_not_chat_text():
    _, body, _ = build_request(
        "webhook", "https://example.test/hook", "s",
        event_type="task.ended", text="rendered", payload={"task_id": "t1"},
    )
    assert body["event_type"] == "task.ended"
    assert body["data"] == {"task_id": "t1"}
    assert body["text"] == "rendered"


def test_unknown_kind_falls_back_to_the_generic_webhook_shape():
    """A kind added to the enum but not here must degrade, not crash."""
    _, body, _ = build_request(
        "some_new_im", "https://example.test/hook", "", event_type="task.ended", text="t"
    )
    assert body["event_type"] == "task.ended"


def test_render_text_drops_empty_fields_and_clips():
    text = render_text("task.ended", {"任务": "修复登录", "结果": "成功", "原因": ""})
    assert "原因" not in text, "an empty value must not render a bare label"
    assert text.startswith("任务已结束")
    assert "任务：修复登录" in text

    clipped = render_text("task.ended", {"任务": "x" * 5000})
    assert len(clipped) <= notify_dispatch._MAX_TEXT


def test_render_text_falls_back_to_the_raw_event_type():
    assert render_text("some.unmapped.event", {}) == "some.unmapped.event"


# ── fan-out ───────────────────────────────────────────────────────────────────


class _FakeConn:
    def __init__(self, rows: list[dict]):
        self._rows = rows
        self.fetches: list[tuple[str, tuple[Any, ...]]] = []
        self.executes: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, query: str, *params: Any):
        self.fetches.append((query, params))
        return self._rows

    async def execute(self, query: str, *params: Any):
        self.executes.append((query, params))
        return "INSERT 1"


class _FakePool:
    def __init__(self, conn: _FakeConn):
        self._conn = conn

    def acquire(self):
        conn = self._conn

        class _Ctx:
            async def __aenter__(self_inner):
                return conn

            async def __aexit__(self_inner, *exc):
                return False

        return _Ctx()


def _row(channel_id: uuid.UUID, *, kind: str = "webhook", url: str = "https://x.test/h") -> dict:
    return {
        "id": channel_id,
        "kind": kind,
        "webhook_url": url,
        "secret": "",
        "headers": None,
        "subscription_id": uuid.uuid4(),
    }


def _install(monkeypatch, rows: list[dict]) -> tuple[_FakeConn, list[str]]:
    conn = _FakeConn(rows)
    monkeypatch.setattr("db.PostgresClient.pool", _FakePool(conn))
    sent: list[str] = []

    async def fake_post(url, body, headers, secret, sign_body):
        sent.append(url)

    monkeypatch.setattr(notify_dispatch, "_post", fake_post)
    return conn, sent


@pytest.mark.asyncio
async def test_dispatch_reaches_both_personal_and_team_channels(monkeypatch):
    personal, team = uuid.uuid4(), uuid.uuid4()
    conn, sent = _install(
        monkeypatch,
        [
            _row(personal, url="https://personal.test/h"),
            _row(team, url="https://team.test/h"),
        ],
    )

    count = await dispatch_event(
        str(uuid.uuid4()), "task.ended", fields={"任务": "t"}, event_ref_id="task-1"
    )
    assert count == 2
    assert sorted(sent) == ["https://personal.test/h", "https://team.test/h"]

    # One send-log row per channel, so a broken channel is diagnosable.
    logged = {params[2] for _, params in conn.executes}
    assert logged == {personal, team}


@pytest.mark.asyncio
async def test_selection_sql_covers_personal_rows_and_team_membership(monkeypatch):
    """Guard the predicate itself.

    Matching on ``owner_id`` alone would let a personal channel fire for a team
    whose id happened to equal a user id, and would miss team channels entirely.
    """
    conn, _ = _install(monkeypatch, [])
    user_id = str(uuid.uuid4())
    await dispatch_event(user_id, "task.ended", fields={"任务": "t"})

    query, params = conn.fetches[0]
    # The personal arm must be pinned to owner_type='user' (COALESCE covers
    # legacy NULL rows) *and* the owner id, so a team id can never match it.
    assert "COALESCE(c.owner_type, 'user') = 'user' AND c.owner_id = $1" in query
    assert "c.owner_type = 'team'" in query
    assert "mc_team_members" in query, "team channels must resolve via membership"
    assert "c.enabled" in query and "s.enabled" in query
    assert "event_types::jsonb @> $2::jsonb" in query

    assert str(params[0]) == user_id
    assert json.loads(params[1]) == ["task.ended"]
    assert params[2] == notify_dispatch._MAX_CHANNELS_PER_EVENT


@pytest.mark.asyncio
async def test_no_subscribed_channels_sends_nothing(monkeypatch):
    conn, sent = _install(monkeypatch, [])
    assert await dispatch_event(str(uuid.uuid4()), "task.ended") == 0
    assert sent == []
    assert conn.executes == [], "no attempt means no send log"


@pytest.mark.asyncio
async def test_a_failing_channel_is_logged_and_does_not_block_the_others(monkeypatch):
    """One dead webhook must not suppress the rest of the fan-out."""
    good, bad = uuid.uuid4(), uuid.uuid4()
    conn = _FakeConn([_row(bad, url="https://bad.test/h"), _row(good, url="https://good.test/h")])
    monkeypatch.setattr("db.PostgresClient.pool", _FakePool(conn))
    sent: list[str] = []

    async def flaky_post(url, body, headers, secret, sign_body):
        if "bad" in url:
            raise RuntimeError("provider rejected (errcode=310000): sign not match")
        sent.append(url)

    monkeypatch.setattr(notify_dispatch, "_post", flaky_post)

    assert await dispatch_event(str(uuid.uuid4()), "task.ended", fields={"任务": "t"}) == 2
    assert sent == ["https://good.test/h"]

    statuses = {params[2]: (params[5], params[6]) for _, params in conn.executes}
    assert statuses[good][0] == "success"
    assert statuses[bad][0] == "failed"
    assert "sign not match" in statuses[bad][1], "the provider's reason must be recorded"


@pytest.mark.asyncio
async def test_channel_without_a_url_is_recorded_as_failed(monkeypatch):
    cid = uuid.uuid4()
    conn, sent = _install(monkeypatch, [_row(cid, url="")])
    await dispatch_event(str(uuid.uuid4()), "task.ended")
    assert sent == []
    assert conn.executes[0][1][5] == "failed"


@pytest.mark.asyncio
async def test_malformed_recipient_id_is_ignored(monkeypatch):
    conn, _ = _install(monkeypatch, [])
    assert await dispatch_event("not-a-uuid", "task.ended") == 0
    assert conn.fetches == [], "a bad id must not reach the database"


@pytest.mark.asyncio
async def test_cold_pool_is_ignored(monkeypatch):
    monkeypatch.setattr("db.PostgresClient.pool", None)
    # Never raises: notification is a side effect of a lifecycle transition and
    # must not surface to the caller that triggered it.
    assert await dispatch_event(str(uuid.uuid4()), "task.ended") == 0


@pytest.mark.asyncio
async def test_database_failure_is_swallowed(monkeypatch):
    class _BoomConn(_FakeConn):
        async def fetch(self, query: str, *params: Any):
            raise RuntimeError("connection reset")

    monkeypatch.setattr("db.PostgresClient.pool", _FakePool(_BoomConn([])))
    assert await dispatch_event(str(uuid.uuid4()), "task.ended") == 0
