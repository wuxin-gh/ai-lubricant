"""Login-method protocol and identity mapping tests."""
from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from tortoise import Tortoise

from user_platform.models import Team, TeamMember, User, UserIdentity
from user_platform.models_team_admin import TeamOIDCConfig
from user_platform.oidc_service import OidcService, OIDCError


@pytest_asyncio.fixture
async def tortoise_db():
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={
            "user_platform": [
                "user_platform.models",
                "user_platform.models_team_admin",
            ]
        },
    )
    await Tortoise.generate_schemas()
    try:
        yield
    finally:
        await Tortoise.close_connections()


class FakeRedis:
    def __init__(self):
        self.values = {}

    async def set(self, key, value, ex=None):
        self.values[key] = value

    async def get(self, key):
        return self.values.get(key)

    async def delete(self, key):
        self.values.pop(key, None)


async def _team_config(**over):
    team = await Team.create(id=uuid.uuid4(), name="SSO Team")
    values = {
        "team_id": team.id,
        "type": "oidc",
        "issuer": "https://id.example.test",
        "client_id": "client-1",
        "client_secret": "secret-1",
        "scopes": "openid email profile",
        "enabled": True,
        "auto_create_member": True,
    }
    values.update(over)
    return team, await TeamOIDCConfig.create(id=uuid.uuid4(), **values)


def _fake_cfg(**over):
    values = {
        "type": "oidc",
        "issuer": "https://id.example.test",
        "client_id": "client-1",
        "client_secret": "secret-1",
        "scopes": None,
    }
    values.update(over)
    return type("Config", (), values)()


async def _async_value(value):
    return value


@pytest.mark.asyncio
async def test_state_is_single_use_and_carries_method_id(monkeypatch):
    """State keys the method that started the flow and is consumed exactly once."""
    service = OidcService()
    redis = FakeRedis()
    monkeypatch.setattr(service, "_redis", lambda: redis)
    state, nonce = await service.issue_state("method-1")
    assert state and nonce
    pending = await service.consume_state(state)
    assert pending == {"method_id": "method-1", "nonce": nonce}
    assert await service.consume_state(state) is None


@pytest.mark.asyncio
async def test_oidc_authorize_url_uses_discovery_and_nonce(monkeypatch):
    service = OidcService()
    monkeypatch.setattr(
        service,
        "discover",
        lambda issuer: _async_value(
            {
                "authorization_endpoint": "https://id.example.test/auth",
                "token_endpoint": "https://id.example.test/token",
            }
        ),
    )
    url = await service.build_authorize_url(
        _fake_cfg(scopes="openid email profile"), "https://app.test/callback", "state-1", "nonce-1"
    )
    assert url.startswith("https://id.example.test/auth?")
    assert "client_id=client-1" in url
    assert "state=state-1" in url
    assert "nonce=nonce-1" in url
    assert "redirect_uri=https%3A%2F%2Fapp.test%2Fcallback" in url


@pytest.mark.asyncio
async def test_oauth_authorize_url_uses_fixed_endpoint_without_nonce():
    """OAuth providers have no ID Token, so no nonce is sent."""
    service = OidcService()
    github = await service.build_authorize_url(
        _fake_cfg(type="oauth_github", issuer=None), "https://app.test/cb", "st", "nc"
    )
    assert github.startswith("https://github.com/login/oauth/authorize?")
    assert "scope=read%3Auser+user%3Aemail" in github
    assert "nonce" not in github
    google = await service.build_authorize_url(
        _fake_cfg(type="oauth_google", issuer=None), "https://app.test/cb", "st", "nc"
    )
    assert google.startswith("https://accounts.google.com/o/oauth2/v2/auth?")
    assert "nonce" not in google


@pytest.mark.asyncio
async def test_unsupported_method_type_is_rejected():
    with pytest.raises(OIDCError, match="不支持的登录方式类型"):
        await OidcService().build_authorize_url(
            _fake_cfg(type="oauth_wechat", issuer=None), "https://app.test/cb", "st", "nc"
        )


@pytest.mark.asyncio
async def test_oauth_claims_come_from_userinfo(monkeypatch):
    """OAuth identity is read from userinfo, keyed by the provider's own id field."""
    service = OidcService()
    monkeypatch.setattr(
        service,
        "_get_json",
        lambda url, headers=None: _async_value(
            {"id": 4242, "login": "octo", "name": "Octo Cat", "email": "Octo@Example.Test", "avatar_url": "https://a/b.png"}
        ),
    )
    claims = await service.verify_claims(
        _fake_cfg(type="oauth_github", issuer=None), {"access_token": "tok"}, "unused-nonce"
    )
    assert claims["sub"] == "4242"
    assert claims["email"] == "octo@example.test"
    assert claims["preferred_username"] == "octo"


@pytest.mark.asyncio
async def test_oauth_without_access_token_is_rejected():
    with pytest.raises(OIDCError, match="access_token"):
        await OidcService().verify_claims(
            _fake_cfg(type="oauth_github", issuer=None), {}, "nonce"
        )


@pytest.mark.asyncio
async def test_unknown_identity_rejected_when_auto_create_disabled(tortoise_db):
    team, cfg = await _team_config(auto_create_member=False)
    with pytest.raises(OIDCError, match="identity_not_authorized"):
        await OidcService().resolve_or_create_user(
            cfg, {"sub": "subject-1", "email": "person@example.test"}, str(team.id)
        )


@pytest.mark.asyncio
async def test_auto_create_links_user_and_membership(tortoise_db):
    team, cfg = await _team_config(auto_create_member=True)
    user = await OidcService().resolve_or_create_user(
        cfg,
        {"sub": "subject-1", "email": "person@example.test", "name": "Person"},
        str(team.id),
    )
    assert user.password is None
    assert await UserIdentity.filter(user_id=user.id, platform="oidc").exists()
    member = await TeamMember.get(team_id=team.id, user_id=user.id)
    assert member.role == "user"
    again = await OidcService().resolve_or_create_user(
        cfg, {"sub": "subject-1", "email": "person@example.test"}, str(team.id)
    )
    assert again.id == user.id


@pytest.mark.asyncio
async def test_identity_namespace_is_per_platform(tortoise_db):
    """Same ``sub`` from two providers must map to two distinct identities.

    Otherwise a GitHub user numbered 1 would inherit the account of an OIDC
    subject also called "1".
    """
    team, oidc_cfg = await _team_config(type="oidc", auto_create_member=True)
    github_cfg = await TeamOIDCConfig.create(
        id=uuid.uuid4(), team_id=team.id, type="oauth_github",
        client_id="gh", client_secret="s", enabled=True, auto_create_member=True,
    )
    service = OidcService()
    first = await service.resolve_or_create_user(
        oidc_cfg, {"sub": "1", "email": "a@example.test"}, str(team.id)
    )
    second = await service.resolve_or_create_user(
        github_cfg, {"sub": "1", "email": "b@example.test"}, str(team.id)
    )
    assert first.id != second.id
    assert await UserIdentity.filter(platform="oidc", identity_id="oidc|1").exists()
    assert await UserIdentity.filter(platform="github", identity_id="github|1").exists()


@pytest.mark.asyncio
async def test_existing_email_must_already_be_a_team_member(tortoise_db):
    """An IdP claiming someone's email cannot hijack an account outside the team."""
    team, cfg = await _team_config(auto_create_member=True)
    await User.create(
        id=uuid.uuid4(), name="Outsider", email="outsider@example.test",
        password="hash", role="individual", status="active",
    )
    with pytest.raises(OIDCError, match="未加入该团队"):
        await OidcService().resolve_or_create_user(
            cfg, {"sub": "subject-9", "email": "outsider@example.test"}, str(team.id)
        )


@pytest.mark.asyncio
async def test_email_domain_restriction_is_enforced(tortoise_db):
    team, cfg = await _team_config(auto_create_member=True, email_domain="corp.test")
    with pytest.raises(OIDCError, match="邮箱域"):
        await OidcService().resolve_or_create_user(
            cfg, {"sub": "subject-2", "email": "person@other.test"}, str(team.id)
        )
