"""Agent 附件签名临时 URL（presigned URL）。

取代「裸 attachment_id + 依赖 session cookie」的旧 URL 模型：URL 自带
``exp``（过期 unix 秒）与 ``sig``（HMAC-SHA256 签名），短 TTL（默认 2 小时），
与文件 30 天 TTL 解耦。拿到签名 URL 即可访问，无需登录态；枚举顺序 ID 无用，
每个 ID 必须独立签名。

密钥来自环境变量 ``AGENT_ATTACHMENT_SIGNING_KEY``（多实例必须一致）。缺失时
``ensure_signing_key()`` 在启动阶段生成随机值并写回 ``.env`` —— 不静默降级。

签名结构（域分离前缀防挪用）::

    message = "agent-attachment:{attachment_id}:{kind}:{exp}"
    sig = hmac_sha256(key, message).hexdigest()

``kind`` 为 ``content`` 或 ``thumbnail``，缩略图签名不能用于 content。
"""
from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import time
from urllib.parse import urlencode

from loguru import logger

_TOKEN_DOMAIN = "agent-attachment"
DEFAULT_ATTACHMENT_URL_TTL = 2 * 60 * 60  # 2 小时
_KINDS = ("content", "thumbnail")
_CONTENT_PATH = "/agent/attachments/{aid}/{kind}"
_ENV_NAME = "AGENT_ATTACHMENT_SIGNING_KEY"


def _signing_key() -> bytes:
    """读取 AGENT_ATTACHMENT_SIGNING_KEY。延迟到调用时读取，避免 import 期耦合。"""
    raw = os.getenv(_ENV_NAME)
    return raw.strip().encode("utf-8") if raw and raw.strip() else b""


def ensure_signing_key() -> str:
    """启动时确保签名密钥存在：缺失即生成随机值并持久化到 .env。

    不静默关闭——附件签名 URL 是必需能力，缺密钥会让所有图片预览退化成需登录态
    且无法签发临时链接。生成后写回 .env（沿用 node_server 对 NODE_CONTROL_TOKEN /
    NODE_CREDENTIAL_ENCRYPTION_KEY 的同款持久化路径），并注入当前进程环境，使本次
    运行立即可用。多实例部署下应显式配置同一密钥；此处的自动生成是单机/首启兜底。
    """
    existing = os.getenv(_ENV_NAME)
    if existing and existing.strip():
        return existing.strip()
    generated = secrets.token_urlsafe(48)
    os.environ[_ENV_NAME] = generated
    try:
        import dotenv_loader
        dotenv_loader.update_env_vars({_ENV_NAME: generated}, path=dotenv_loader.ENV_FILE)
        logger.info("[attachment] generated and persisted {} to .env", _ENV_NAME)
    except Exception as exc:  # noqa: BLE001 — 持久化失败不阻断启动，本进程仍可用内存值
        logger.warning(
            "[attachment] generated {} but failed to persist to .env ({}); "
            "multi-instance deployments must set it explicitly",
            _ENV_NAME, exc,
        )
    return generated


def _signature(attachment_id: int, kind: str, exp: int) -> str:
    key = _signing_key()
    if not key:
        return ""
    message = f"{_TOKEN_DOMAIN}:{int(attachment_id)}:{kind}:{int(exp)}".encode("utf-8")
    return hmac.new(key, message, hashlib.sha256).hexdigest()


def sign_attachment_url(attachment_id: int, kind: str, ttl_seconds: int = DEFAULT_ATTACHMENT_URL_TTL) -> str | None:
    """签发一个相对路径签名 URL：``/agent/attachments/{id}/{kind}?exp=...&sig=...``。

    密钥未配置或 kind 非法时返回 None（调用方应回退到登录态路径）。
    """
    if kind not in _KINDS or not isinstance(attachment_id, int) or attachment_id <= 0:
        return None
    if not isinstance(ttl_seconds, int) or ttl_seconds <= 0:
        ttl_seconds = DEFAULT_ATTACHMENT_URL_TTL
    exp = int(time.time()) + ttl_seconds
    sig = _signature(attachment_id, kind, exp)
    if not sig:
        return None
    query = urlencode({"exp": exp, "sig": sig})
    return f"{_CONTENT_PATH.format(aid=attachment_id, kind=kind)}?{query}"


def verify_attachment_token(attachment_id: int, kind: str, exp: int, sig: str | None) -> bool:
    """常时校验签名 + 过期。密钥空、kind 非法、sig 不匹配或已过期均返回 False。"""
    if kind not in _KINDS or not isinstance(attachment_id, int) or attachment_id <= 0:
        return False
    if not sig or not isinstance(sig, str):
        return False
    if not isinstance(exp, int):  # FastAPI query 已是 int，兜底
        try:
            exp = int(exp)
        except (TypeError, ValueError):
            return False
    if int(time.time()) >= exp:
        return False
    expected = _signature(attachment_id, kind, exp)
    if not expected:
        return False
    return hmac.compare_digest(expected, str(sig))
