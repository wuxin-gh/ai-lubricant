"""Team login-method authentication service.

Owns the network-facing protocol flow for the team login methods stored in
``TeamOIDCConfig``: OIDC (discovery + JWKS-verified ID Token) and OAuth2 code
flow for GitHub / Google (token exchange + userinfo). ``state`` is keyed by
method id so the callback resolves the exact configuration that started the
flow.
"""
from __future__ import annotations

import json
import secrets
import time
import uuid
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import aiohttp
import jwt
from loguru import logger

from .models import TeamMember, User, UserIdentity
from .models_team_admin import TeamOIDCConfig

_STATE_PREFIX = "mc:oidc:state:"
_STATE_TTL_SECONDS = 600
_CACHE_TTL_SECONDS = 3600
_HTTP_TIMEOUT_SECONDS = 10

# OAuth2 endpoints for the non-OIDC providers. OIDC reads its own endpoints from
# the issuer's discovery document; these are fixed per provider.
_OAUTH_ENDPOINTS = {
    "oauth_github": {
        "authorize": "https://github.com/login/oauth/authorize",
        "token": "https://github.com/login/oauth/access_token",
        "userinfo": "https://api.github.com/user",
        "default_scopes": "read:user user:email",
        "platform": "github",
    },
    "oauth_google": {
        "authorize": "https://accounts.google.com/o/oauth2/v2/auth",
        "token": "https://oauth2.googleapis.com/token",
        "userinfo": "https://www.googleapis.com/oauth2/v3/userinfo",
        "default_scopes": "openid email profile",
        "platform": "google",
    },
}


class OIDCError(ValueError):
    """A user-safe login-method flow error."""


@dataclass
class _CacheEntry:
    value: dict[str, Any]
    expires_at: float


class OidcService:
    def __init__(self) -> None:
        self._discovery: dict[str, _CacheEntry] = {}
        self._jwks: dict[str, _CacheEntry] = {}

    @staticmethod
    def _issuer(value: str | None) -> str:
        issuer = (value or "").strip().rstrip("/")
        if not issuer.startswith(("https://", "http://")):
            raise OIDCError("issuer 配置无效")
        return issuer

    async def _get_json(self, url: str, headers: dict[str, str] | None = None) -> dict[str, Any]:
        timeout = aiohttp.ClientTimeout(total=_HTTP_TIMEOUT_SECONDS)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers={"Accept": "application/json", **(headers or {})}) as response:
                    if response.status < 200 or response.status >= 300:
                        raise OIDCError(f"上游返回 HTTP {response.status}")
                    data = await response.json(content_type=None)
        except OIDCError:
            raise
        except (aiohttp.ClientError, TimeoutError) as exc:
            raise OIDCError("上游不可达") from exc
        if not isinstance(data, dict):
            raise OIDCError("上游响应格式无效")
        return data

    async def _post_form(self, url: str, form: dict[str, str]) -> dict[str, Any]:
        timeout = aiohttp.ClientTimeout(total=_HTTP_TIMEOUT_SECONDS)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, data=form, headers={"Accept": "application/json"}) as response:
                    data = await response.json(content_type=None)
                    if response.status < 200 or response.status >= 300:
                        raise OIDCError("授权码交换失败")
        except OIDCError:
            raise
        except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
            raise OIDCError("token endpoint 请求失败") from exc
        if not isinstance(data, dict):
            raise OIDCError("token endpoint 响应格式无效")
        return data

    async def discover(self, issuer: str) -> dict[str, Any]:
        issuer = self._issuer(issuer)
        cached = self._discovery.get(issuer)
        if cached and cached.expires_at > time.time():
            return cached.value
        value = await self._get_json(f"{issuer}/.well-known/openid-configuration")
        if not value.get("authorization_endpoint") or not value.get("token_endpoint"):
            raise OIDCError("OIDC 发现文档缺少必要端点")
        self._discovery[issuer] = _CacheEntry(value, time.time() + _CACHE_TTL_SECONDS)
        return value

    async def _get_jwks(self, jwks_uri: str) -> dict[str, Any]:
        cached = self._jwks.get(jwks_uri)
        if cached and cached.expires_at > time.time():
            return cached.value
        value = await self._get_json(jwks_uri)
        if not isinstance(value.get("keys"), list):
            raise OIDCError("OIDC JWKS 响应格式无效")
        self._jwks[jwks_uri] = _CacheEntry(value, time.time() + _CACHE_TTL_SECONDS)
        return value

    def _redis(self):
        from rd import JdbcClient

        if JdbcClient.redis is None:
            raise RuntimeError("redis client not initialized")
        return JdbcClient.redis

    async def issue_state(self, method_id: str) -> tuple[str, str]:
        state = secrets.token_urlsafe(32)
        nonce = secrets.token_urlsafe(24)
        payload = json.dumps({"method_id": str(method_id), "nonce": nonce}, separators=(",", ":"))
        await self._redis().set(f"{_STATE_PREFIX}{state}", payload, ex=_STATE_TTL_SECONDS)
        return state, nonce

    async def consume_state(self, state: str) -> dict[str, str] | None:
        if not state:
            return None
        redis = self._redis()
        key = f"{_STATE_PREFIX}{state}"
        raw = await redis.get(key)
        # Consume before doing any remote work: the callback is strictly one-shot.
        await redis.delete(key)
        if not raw:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            value = json.loads(raw)
        except (TypeError, ValueError):
            return None
        if not isinstance(value, dict) or not value.get("method_id") or not value.get("nonce"):
            return None
        return {"method_id": str(value["method_id"]), "nonce": str(value["nonce"])}

    async def build_authorize_url(
        self, cfg: TeamOIDCConfig, redirect_uri: str, state: str, nonce: str
    ) -> str:
        """Authorization-request URL for this method.

        OIDC reads ``authorization_endpoint`` from the issuer's discovery
        document and carries ``nonce``; the OAuth providers use their fixed
        endpoint and have no nonce (identity comes from userinfo, not a token).
        """
        method_type = cfg.type or "oidc"
        default_scopes = _OAUTH_ENDPOINTS.get(method_type, {}).get(
            "default_scopes", "openid email profile"
        )
        scopes = (cfg.scopes or default_scopes).split()
        query = {
            "response_type": "code",
            "client_id": cfg.client_id or "",
            "redirect_uri": redirect_uri,
            "scope": " ".join(dict.fromkeys(scopes)),
            "state": state,
        }
        if method_type == "oidc":
            discovery = await self.discover(cfg.issuer or "")
            query["nonce"] = nonce
            return f"{discovery['authorization_endpoint']}?{urlencode(query)}"
        endpoints = _OAUTH_ENDPOINTS.get(method_type)
        if endpoints is None:
            raise OIDCError(f"不支持的登录方式类型: {method_type}")
        return f"{endpoints['authorize']}?{urlencode(query)}"

    async def exchange_token(self, cfg: TeamOIDCConfig, code: str, redirect_uri: str) -> dict[str, Any]:
        method_type = cfg.type or "oidc"
        form = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": cfg.client_id or "",
            "client_secret": cfg.client_secret or "",
        }
        if method_type == "oidc":
            discovery = await self.discover(cfg.issuer or "")
            token_url = discovery["token_endpoint"]
        else:
            endpoints = _OAUTH_ENDPOINTS.get(method_type)
            if endpoints is None:
                raise OIDCError(f"不支持的登录方式类型: {method_type}")
            token_url = endpoints["token"]
        return await self._post_form(token_url, form)

    async def verify_claims(self, cfg: TeamOIDCConfig, token_resp: dict[str, Any], nonce: str) -> dict[str, Any]:
        method_type = cfg.type or "oidc"
        if method_type == "oidc":
            return await self._verify_oidc_id_token(cfg, token_resp.get("id_token") or "", nonce)
        endpoints = _OAUTH_ENDPOINTS.get(method_type)
        if endpoints is None:
            raise OIDCError(f"不支持的登录方式类型: {method_type}")
        access_token = token_resp.get("access_token")
        if not access_token:
            raise OIDCError("OAuth 响应缺少 access_token")
        userinfo = await self._get_json(
            endpoints["userinfo"], headers={"Authorization": f"Bearer {access_token}"}
        )
        return self._normalize_oauth_claims(userinfo, method_type)

    async def _verify_oidc_id_token(self, cfg: TeamOIDCConfig, id_token: str, nonce: str) -> dict[str, Any]:
        if not id_token:
            raise OIDCError("OIDC id_token 为空")
        issuer = self._issuer(cfg.issuer)
        discovery = await self.discover(issuer)
        jwks_uri = discovery.get("jwks_uri")
        if not jwks_uri:
            raise OIDCError("OIDC 发现文档缺少 jwks_uri")
        header = jwt.get_unverified_header(id_token)
        kid = header.get("kid")
        alg = header.get("alg")
        if alg not in {"RS256", "RS384", "RS512"} or not kid:
            raise OIDCError("OIDC id_token 签名算法无效")
        jwks = await self._get_jwks(str(jwks_uri))
        jwk = next((item for item in jwks["keys"] if item.get("kid") == kid), None)
        if jwk is None:
            self._jwks.pop(str(jwks_uri), None)
            jwks = await self._get_jwks(str(jwks_uri))
            jwk = next((item for item in jwks["keys"] if item.get("kid") == kid), None)
        if jwk is None:
            raise OIDCError("OIDC 找不到匹配的签名密钥")
        try:
            key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(jwk))
            claims = jwt.decode(
                id_token,
                key=key,
                algorithms=[alg],
                audience=cfg.client_id,
                issuer=issuer,
                options={"require": ["exp", "iat", "sub"]},
            )
        except jwt.PyJWTError as exc:
            raise OIDCError("OIDC id_token 验证失败") from exc
        if claims.get("nonce") != nonce:
            raise OIDCError("OIDC nonce 校验失败")
        return claims

    @staticmethod
    def _normalize_oauth_claims(userinfo: dict[str, Any], method_type: str) -> dict[str, Any]:
        if method_type == "oauth_github":
            return {
                "sub": str(userinfo.get("id") or ""),
                "email": str(userinfo.get("email") or "").lower() or None,
                "name": str(userinfo.get("name") or userinfo.get("login") or ""),
                "preferred_username": str(userinfo.get("login") or ""),
                "picture": str(userinfo.get("avatar_url") or ""),
            }
        if method_type == "oauth_google":
            return {
                "sub": str(userinfo.get("sub") or ""),
                "email": str(userinfo.get("email") or "").lower() or None,
                "name": str(userinfo.get("name") or ""),
                "preferred_username": str(userinfo.get("email") or userinfo.get("name") or ""),
                "picture": str(userinfo.get("picture") or ""),
            }
        raise OIDCError(f"不支持的登录方式类型: {method_type}")

    async def resolve_or_create_user(
        self, cfg: TeamOIDCConfig, claims: dict[str, Any], team_id: str
    ) -> User:
        method_type = cfg.type or "oidc"
        platform = "oidc" if method_type == "oidc" else _OAUTH_ENDPOINTS.get(method_type, {}).get("platform", method_type)
        sub = str(claims.get("sub") or "").strip()
        if not sub:
            raise OIDCError("身份缺少 sub")
        identity_id = f"{platform}|{sub}"
        identity = await UserIdentity.get_or_none(platform=platform, identity_id=identity_id)
        if identity is not None:
            user = await User.get_or_none(id=identity.user_id, is_deleted=False)
            if user is None or user.is_blocked or user.status != "active":
                raise OIDCError("账号已停用")
            member = await TeamMember.get_or_none(team_id=team_id, user_id=user.id)
            if member is None:
                raise OIDCError("身份未加入该团队")
            return user
        if not cfg.auto_create_member:
            raise OIDCError("identity_not_authorized")
        email = str(claims.get("email") or "").strip().lower() or None
        domain = (cfg.email_domain or "").strip().lower().lstrip("@")
        if domain and (not email or email.rsplit("@", 1)[-1] != domain):
            raise OIDCError("邮箱域不在允许范围")
        user = None
        if email:
            candidate = await User.filter(email=email, is_deleted=False).first()
            if candidate is not None:
                member = await TeamMember.get_or_none(team_id=team_id, user_id=candidate.id)
                if member is None:
                    raise OIDCError("邮箱对应账号未加入该团队")
                if candidate.is_blocked or candidate.status != "active":
                    raise OIDCError("账号已停用")
                user = candidate
        created_user = user is None
        if user is None:
            user = await User.create(
                id=uuid.uuid4(),
                name=str(claims.get("name") or claims.get("preferred_username") or email or sub)[:255],
                email=email,
                avatar_url=str(claims.get("picture") or "")[:512] or None,
                password=None,
                role="individual",
                status="active",
            )
        try:
            await UserIdentity.create(
                user_id=user.id,
                platform=platform,
                identity_id=identity_id,
                username=str(claims.get("preferred_username") or claims.get("name") or sub)[:255],
                email=email,
                avatar_url=user.avatar_url,
            )
            await TeamMember.get_or_create(
                team_id=team_id, user_id=user.id, defaults={"role": "user"}
            )
        except Exception:
            if created_user:
                await user.delete()
            raise
        logger.info("[monkeycode-compat] login method linked user={} team={} platform={}", user.id, team_id, platform)
        return user


oidc_service = OidcService()
