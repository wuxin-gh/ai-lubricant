"""Webhook signature verification and cross-platform event normalization."""
from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class NormalizedWebhookEvent:
    event_type: str
    delivery_id: str
    payload: dict[str, Any]
    commit_sha: str | None = None
    before_sha: str | None = None
    base_ref: str | None = None
    head_ref: str | None = None
    pr_number: str | None = None
    source_branch: str | None = None
    target_branch: str | None = None
    ignored_reason: str | None = None


def _header(headers: Mapping[str, str], name: str) -> str:
    return str(headers.get(name) or headers.get(name.lower()) or "").strip()


def verify_webhook_signature(
    platform: str, headers: Mapping[str, str], body: bytes, secret: str
) -> bool:
    """Verify a delivery without ever formatting the shared secret into an error."""
    if not secret:
        return False
    platform = (platform or "").lower()
    secret_bytes = secret.encode("utf-8")
    if platform == "github":
        supplied = _header(headers, "x-hub-signature-256")
        expected = "sha256=" + hmac.new(secret_bytes, body, hashlib.sha256).hexdigest()
        return bool(supplied) and hmac.compare_digest(supplied, expected)
    if platform == "gitlab":
        supplied = _header(headers, "x-gitlab-token")
        return bool(supplied) and hmac.compare_digest(supplied, secret)
    if platform == "gitea":
        supplied = _header(headers, "x-gitea-signature")
        expected = hmac.new(secret_bytes, body, hashlib.sha256).hexdigest()
        if supplied.lower().startswith("sha256="):
            supplied = supplied[7:]
        return bool(supplied) and hmac.compare_digest(supplied.lower(), expected)
    if platform == "gitee":
        # Gitee's webhook password is delivered as X-Gitee-Token. Some
        # installations use X-Gitee-Signature for the same configured value.
        supplied = _header(headers, "x-gitee-token") or _header(headers, "x-gitee-signature")
        return bool(supplied) and hmac.compare_digest(supplied, secret)
    return False


def parse_json_body(body: bytes) -> dict[str, Any]:
    parsed = json.loads(body.decode("utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError("webhook payload must be an object")
    return parsed


def _branch(value: Any) -> str | None:
    text = str(value or "").strip()
    for prefix in ("refs/heads/", "refs/tags/"):
        if text.startswith(prefix):
            text = text[len(prefix):]
            break
    return text or None


def _sha(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text or set(text) == {"0"}:
        return None
    return text


def _delivery_id(platform: str, headers: Mapping[str, str], body: bytes) -> str:
    names = {
        "github": ("x-github-delivery",),
        "gitlab": ("x-gitlab-event-uuid", "x-gitlab-webhook-uuid"),
        "gitea": ("x-gitea-delivery",),
        "gitee": ("x-gitee-delivery",),
    }.get(platform, ())
    for name in names:
        value = _header(headers, name)
        if value:
            return value[:255]
    return "sha256:" + hashlib.sha256(body).hexdigest()


def _event_header(platform: str, headers: Mapping[str, str], payload: dict[str, Any]) -> str:
    names = {
        "github": "x-github-event",
        "gitlab": "x-gitlab-event",
        "gitea": "x-gitea-event",
        "gitee": "x-gitee-event",
    }
    return (_header(headers, names.get(platform, "")) or str(
        payload.get("hook_name") or payload.get("event_name") or ""
    )).strip().lower()


def _pull_request_event(
    platform: str,
    delivery_id: str,
    payload: dict[str, Any],
) -> NormalizedWebhookEvent:
    if platform == "gitlab":
        obj = payload.get("object_attributes") or {}
        last = obj.get("last_commit") or {}
        return NormalizedWebhookEvent(
            event_type="pull_request",
            delivery_id=delivery_id,
            payload=payload,
            commit_sha=_sha(last.get("id")),
            base_ref=_sha(obj.get("target_branch")),
            head_ref=_sha(obj.get("source_branch")),
            pr_number=str(obj.get("iid") or obj.get("id") or "") or None,
            source_branch=_branch(obj.get("source_branch")),
            target_branch=_branch(obj.get("target_branch")),
            ignored_reason=None if str(obj.get("action") or "").lower() in {
                "open", "reopen", "update", "merge"
            } else "unsupported_action",
        )
    pr = payload.get("pull_request") or payload.get("merge_request") or {}
    head = pr.get("head") or {}
    base = pr.get("base") or {}
    action = str(payload.get("action") or payload.get("action_name") or "").lower()
    number = payload.get("number") or pr.get("number") or pr.get("id") or pr.get("iid")
    return NormalizedWebhookEvent(
        event_type="pull_request",
        delivery_id=delivery_id,
        payload=payload,
        commit_sha=_sha(head.get("sha") or head.get("id")),
        base_ref=_sha(base.get("sha") or base.get("id")),
        head_ref=_sha(head.get("sha") or head.get("id")),
        pr_number=str(number) if number is not None else None,
        source_branch=_branch(head.get("ref") or pr.get("source_branch")),
        target_branch=_branch(base.get("ref") or pr.get("target_branch")),
        ignored_reason=None if action in {
            "", "opened", "open", "reopened", "reopen", "synchronize", "synchronized", "update", "updated"
        } else "unsupported_action",
    )


def normalize_webhook_event(
    platform: str, headers: Mapping[str, str], body: bytes, payload: dict[str, Any]
) -> NormalizedWebhookEvent:
    platform = (platform or "").lower()
    delivery_id = _delivery_id(platform, headers, body)
    event = _event_header(platform, headers, payload)
    if "merge request" in event or "pull_request" in event or "pull request" in event:
        return _pull_request_event(platform, delivery_id, payload)
    if "push" in event or event == "push_hooks":
        after = _sha(payload.get("after") or (payload.get("head_commit") or {}).get("id"))
        before = _sha(payload.get("before"))
        deleted = bool(payload.get("deleted")) or after is None
        return NormalizedWebhookEvent(
            event_type="push",
            delivery_id=delivery_id,
            payload=payload,
            commit_sha=after,
            before_sha=before,
            head_ref=after,
            source_branch=_branch(payload.get("ref")),
            ignored_reason="deleted_ref" if deleted else None,
        )
    return NormalizedWebhookEvent(
        event_type="unknown",
        delivery_id=delivery_id,
        payload=payload,
        ignored_reason="unsupported_event",
    )
