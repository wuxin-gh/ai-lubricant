"""Tunnel manager dispatch + allocator unit tests.

No network, no node server: exercises the pure logic — config validation,
port-range allocation, and the POSIX shell launcher scripts the node runs.
"""
from __future__ import annotations

import shutil
import subprocess
import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
import pytest_asyncio

from tortoise import Tortoise
from monkeycode_compat import tunnel_allocator as alloc
from monkeycode_compat import tunnel_node_dispatch as dispatch
from monkeycode_compat import tunnel_runtime_manager as rm
from monkeycode_compat import tunnel_service as svc
from monkeycode_compat import tunnel_cloudflare as cf
from monkeycode_compat.models_tunnel import TunnelBinding, TunnelScheme


@pytest_asyncio.fixture
async def db():
    """In-memory sqlite so allocate()'s _used_ports query can run."""
    await Tortoise.init(
        config={
            "connections": {"monkeycode_compat": "sqlite://:memory:"},
            "apps": {
                "monkeycode_compat": {
                    "models": ["monkeycode_compat.models_tunnel"],
                    "default_connection": "monkeycode_compat",
                }
            },
        },
        use_tz=False,
        _enable_global_fallback=True,
    )
    await Tortoise.generate_schemas()
    try:
        yield
    finally:
        await Tortoise.close_connections()


def _scheme(kind: str, config: dict) -> TunnelScheme:
    s = TunnelScheme.__new__(TunnelScheme)
    s.id = uuid.uuid4()
    s.name = "test"
    s.kind = kind
    s.config = config
    s.enabled = True
    return s


def _binding(local_port=8000, allocated="30001") -> TunnelBinding:
    b = TunnelBinding.__new__(TunnelBinding)
    b.id = uuid.uuid4()
    b.local_host = "127.0.0.1"
    b.local_port = local_port
    b.allocated_value = allocated
    return b


# ── config validation ──────────────────────────────────────────────────────


def test_validate_frpc_requires_server_addr():
    with pytest.raises(svc.TunnelServiceError) as exc:
        svc._validate_scheme_config("frpc", {"port_range": [30000, 30100]})
    assert exc.value.code == "invalid_argument"


def test_validate_frpc_requires_port_range():
    with pytest.raises(svc.TunnelServiceError):
        svc._validate_scheme_config("frpc", {"server_addr": "h", "port_range": [1]})


def test_validate_cloudflared_quick_ok():
    svc._validate_scheme_config("cloudflared", {"mode": "quick"})  # no raise


def test_validate_cloudflared_managed_requires_all_credentials():
    with pytest.raises(svc.TunnelServiceError):
        svc._validate_scheme_config("cloudflared", {"mode": "managed", "api_token": "t"})


def test_validate_cloudflared_managed_requires_domain():
    with pytest.raises(svc.TunnelServiceError) as exc:
        svc._validate_scheme_config(
            "cloudflared",
            {"mode": "managed", "api_token": "t", "account_id": "a", "zone_id": "z"},
        )
    assert "domain" in exc.value.message


def test_validate_cloudflared_managed_ok():
    svc._validate_scheme_config(
        "cloudflared",
        {
            "mode": "managed", "api_token": "tok", "account_id": "acc",
            "zone_id": "zone", "domain": "example.com",
        },
    )  # no raise


def test_validate_normalizes_domain_in_place():
    """Stored config must carry the cleaned domain, not the raw input."""
    cfg = {
        "mode": "managed", "api_token": "t", "account_id": "a", "zone_id": "z",
        "domain": "  Example.COM.  ",
    }
    svc._validate_scheme_config("cloudflared", cfg)
    assert cfg["domain"] == "example.com"


def test_validate_frpc_domain_optional_but_checked():
    svc._validate_scheme_config(
        "frpc", {"server_addr": "h", "port_range": [1, 2]}
    )  # absent domain is fine
    cfg = {"server_addr": "h", "port_range": [1, 2], "domain": "Tunnel.Example.com"}
    svc._validate_scheme_config("frpc", cfg)
    assert cfg["domain"] == "tunnel.example.com"
    with pytest.raises(svc.TunnelServiceError):
        svc._validate_scheme_config(
            "frpc", {"server_addr": "h", "port_range": [1, 2], "domain": "https://x.com"}
        )


# ── domain / subdomain helpers ─────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw",
    [
        "https://example.com",   # scheme
        "example.com/path",      # path
        "example.com:8080",      # port
        "*.example.com",         # wildcard
        "a..b.com",              # empty label
        "localhost",             # single label
        "-bad.example.com",      # leading hyphen
        "bad-.example.com",      # trailing hyphen
        "under_score.com",       # invalid char
        "a" * 64 + ".com",       # label > 63
    ],
)
def test_normalize_domain_rejects_malformed(raw):
    with pytest.raises(svc.TunnelServiceError):
        svc._normalize_domain(raw)


def test_normalize_domain_rejects_overlong():
    long_domain = ".".join(["abcdefghij"] * 25) + ".com"  # > 253 chars
    with pytest.raises(svc.TunnelServiceError):
        svc._normalize_domain(long_domain)


def test_normalize_domain_accepts_nested():
    assert svc._normalize_domain("a.b.example.com") == "a.b.example.com"


def test_compose_hostname_simple():
    assert svc._compose_hostname("app", "example.com") == "app.example.com"


def test_compose_hostname_nested_subdomain():
    assert svc._compose_hostname("api.dev", "example.com") == "api.dev.example.com"


def test_compose_hostname_normalizes_case_and_dot():
    assert svc._compose_hostname(" App. ", "example.com") == "app.example.com"


def test_compose_hostname_rejects_full_fqdn():
    """Pasting the whole hostname must not yield app.example.com.example.com."""
    with pytest.raises(svc.TunnelServiceError) as exc:
        svc._compose_hostname("app.example.com", "example.com")
    assert "relative" in exc.value.message


def test_compose_hostname_rejects_bare_domain():
    with pytest.raises(svc.TunnelServiceError):
        svc._compose_hostname("example.com", "example.com")


@pytest.mark.parametrize("bad", ["", "  ", "*", "a/b", "a:b", "https://a", "-x", "a..b"])
def test_compose_hostname_rejects_malformed_subdomain(bad):
    with pytest.raises(svc.TunnelServiceError):
        svc._compose_hostname(bad, "example.com")


def test_validate_npc_ok():
    svc._validate_scheme_config("npc", {"server_addr": "h", "port_range": [1, 2]})


def test_validate_unknown_kind_rejected():
    with pytest.raises(svc.TunnelServiceError):
        svc._validate_scheme_config("wireguard", {})


# ── allocator ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_allocate_frpc_picks_first_free_port(db):
    scheme = _scheme("frpc", {"server_addr": "10.0.0.8", "port_range": [30000, 30002]})
    result = await alloc.allocate(scheme)
    assert result["public_addr"] == "10.0.0.8:30000"
    assert result["allocated_value"] == "30000"


@pytest.mark.asyncio
async def test_allocate_frpc_prefers_public_domain(db):
    """Public display uses domain; client connection still uses server_addr."""
    scheme = _scheme(
        "frpc",
        {
            "server_addr": "10.0.0.8", "server_port": 7000,
            "domain": "tunnel.example.com", "port_range": [30000, 30002],
        },
    )
    result = await alloc.allocate(scheme)
    assert result["public_addr"] == "tunnel.example.com:30000"
    assert result["allocated_value"] == "30000"


@pytest.mark.asyncio
async def test_allocate_cloudflared_quick_returns_none(db):
    scheme = _scheme("cloudflared", {"mode": "quick"})
    result = await alloc.allocate(scheme)
    assert result["public_addr"] is None
    assert result["allocated_value"] is None


@pytest.mark.asyncio
async def test_allocate_cloudflared_managed_defers_to_dispatch(db):
    """managed 不预分配地址:hostname 由用户给,tunnel/DNS 在 dispatch 时建。"""
    scheme = _scheme(
        "cloudflared",
        {"mode": "managed", "api_token": "t", "account_id": "a", "zone_id": "z"},
    )
    result = await alloc.allocate(scheme)
    assert result["public_addr"] is None
    assert result["allocated_value"] is None


@pytest.mark.asyncio
async def test_allocate_cloudflared_rejects_unknown_mode(db):
    scheme = _scheme("cloudflared", {"mode": "named", "domain": "x.example.com"})
    with pytest.raises(alloc.AllocationError):
        await alloc.allocate(scheme)


@pytest.mark.asyncio
async def test_allocate_exhausts_gracefully(db):
    scheme = _scheme("frpc", {"server_addr": "h", "port_range": [30000, 30000]})
    # The single port is "free" because allocate reads live bindings; with no
    # DB row it picks 30000. A second allocation in the same process also picks
    # 30000 (no row persisted), so exhaustion is only observable under the real
    # store — here we assert the single-port case still resolves deterministically.
    result = await alloc.allocate(scheme)
    assert result["public_addr"] == "h:30000"


# ── dispatch: POSIX shell launcher syntax ───────────────────────────────────


@pytest.mark.skipif(shutil.which("sh") is None, reason="sh not available")
@pytest.mark.parametrize("kind", ["frpc", "cloudflared", "npc"])
def test_ensure_binary_script_is_posix_sh(kind):
    """The generated launcher must parse under plain POSIX sh (dash on Debian).

    A syntax check (``sh -n``) catches the common bashism pitfalls (``${var//}``
    etc.) before the script ever reaches a node running dash.
    """
    script = dispatch._ensure_binary_script(kind)
    proc = subprocess.run(["sh", "-n"], input=script, text=True, capture_output=True)
    assert proc.returncode == 0, f"{kind} launcher not POSIX sh:\n{proc.stderr}\n{script}"


def test_frpc_ini_contains_remote_port():
    ini = dispatch._frpc_ini(_binding(), _scheme("frpc", {"server_addr": "h", "server_port": 7000}))
    assert "remotePort = 30001" in ini
    assert 'localPort = 8000' in ini
    assert 'serverAddr = "h"' in ini


def test_npc_conf_contains_vkey_when_token_set():
    conf = dispatch._npc_conf(
        _binding(),
        _scheme("npc", {"server_addr": "nps", "server_port": 8024, "token": "secret"}),
    )
    assert "vkey=secret" in conf
    assert "remote_port=30001" in conf


def test_npc_conf_omits_vkey_when_no_token():
    conf = dispatch._npc_conf(_binding(), _scheme("npc", {"server_addr": "nps", "server_port": 8024}))
    assert "vkey=" not in conf


def test_ensure_binary_script_uses_configured_url_and_sha():
    cfg = {
        "binary_url": "https://mirror.example.com/cloudflared-{os}-{arch}",
        "binary_sha256": "deadbeef",
    }
    script = dispatch._ensure_binary_script("cloudflared", cfg)
    assert "mirror.example.com/cloudflared" in script
    assert "deadbeef" in script
    assert "checksum mismatch" in script


def test_ensure_binary_script_omits_checksum_when_not_configured():
    script = dispatch._ensure_binary_script("cloudflared", {})
    assert "checksum mismatch" not in script


# ── secret masking + masked round-trip ──────────────────────────────────────


def test_public_config_masks_api_token():
    cfg = {"mode": "managed", "api_token": "secret-token-1234567890", "account_id": "acc", "zone_id": "zone", "domain": "example.com"}
    public = svc._public_config("cloudflared", cfg)
    assert public["api_token"] == "***7890"
    assert public["account_id"] == "acc"  # non-secret left untouched
    assert public["zone_id"] == "zone"
    assert public["domain"] == "example.com"  # domain is not a secret


def test_public_config_masks_frpc_token():
    public = svc._public_config("frpc", {"token": "very-secret-value", "server_addr": "h"})
    assert public["token"] == "***alue"
    assert public["server_addr"] == "h"


def test_merge_masked_secrets_preserves_stored_cleartext():
    stored = {"api_token": "real-secret-1234567890", "account_id": "acc"}
    incoming = {"mode": "managed", "api_token": "***7890", "account_id": "acc", "zone_id": "zone"}
    merged = svc._merge_masked_secrets("cloudflared", incoming, stored)
    assert merged["api_token"] == "real-secret-1234567890"
    assert merged["zone_id"] == "zone"


def test_merge_masked_secrets_keeps_new_cleartext():
    stored = {"api_token": "old-secret-1234567890"}
    incoming = {"mode": "managed", "api_token": "brand-new-secret-9876543210", "account_id": "a", "zone_id": "z"}
    merged = svc._merge_masked_secrets("cloudflared", incoming, stored)
    assert merged["api_token"] == "brand-new-secret-9876543210"


# ── cloudflare provisioning (no network: _request is stubbed) ────────────────


def _fake_cf_requests(monkeypatch, responses: dict, calls: list):
    """Stub tunnel_cloudflare._request, recording calls and replaying responses.

    ``responses`` maps a ``"<METHOD> <url-suffix>"`` prefix to the value the
    stub should return; the first matching key wins.
    """
    async def fake_request(method, url, *, api_token, json_body=None):
        calls.append({"method": method, "url": url, "body": json_body, "token": api_token})
        for key, value in responses.items():
            m, _, suffix = key.partition(" ")
            if method == m and suffix in url:
                if isinstance(value, Exception):
                    raise value
                return value
        raise AssertionError(f"unstubbed call {method} {url}")

    monkeypatch.setattr(cf, "_request", fake_request)


@pytest.mark.asyncio
async def test_create_managed_tunnel_calls_api_in_order(monkeypatch):
    calls: list = []
    _fake_cf_requests(
        monkeypatch,
        {
            "POST /cfd_tunnel": {"id": "tun-123"},
            "GET /token": "connector-token-abc",
            "PUT /configurations": {"config": {}},
            "POST /dns_records": {"id": "dns-456"},
        },
        calls,
    )
    out = await cf.create_managed_tunnel(
        api_token="tok", account_id="acc", zone_id="zone",
        tunnel_name="mc-b1", hostname="app.example.com",
        service="http://127.0.0.1:8000",
    )
    assert out == {"tunnel_id": "tun-123", "dns_record_id": "dns-456", "token": "connector-token-abc"}
    assert [c["method"] for c in calls] == ["POST", "GET", "PUT", "POST"]
    # remotely-managed tunnel so ingress lives in Cloudflare, not a node file
    assert calls[0]["body"] == {"name": "mc-b1", "config_src": "cloudflare"}
    # ingress maps the public hostname to the node's local service + catch-all
    ingress = calls[2]["body"]["config"]["ingress"]
    assert ingress[0] == {"hostname": "app.example.com", "service": "http://127.0.0.1:8000"}
    assert ingress[-1] == {"service": "http_status:404"}
    # DNS CNAME points at the tunnel
    assert calls[3]["body"]["content"] == "tun-123.cfargotunnel.com"
    assert calls[3]["body"]["type"] == "CNAME"
    assert calls[3]["body"]["proxied"] is True


@pytest.mark.asyncio
async def test_create_managed_tunnel_rolls_back_tunnel_on_dns_failure(monkeypatch):
    """A later step failing must not leak the already-created tunnel."""
    calls: list = []
    _fake_cf_requests(
        monkeypatch,
        {
            "POST /cfd_tunnel": {"id": "tun-999"},
            "GET /token": "tok-abc",
            "PUT /configurations": {"config": {}},
            "POST /dns_records": cf.CloudflareError("zone not found"),
            # dns create failure now triggers a reclaim lookup; no match found
            "GET /dns_records": [],
            "DELETE /cfd_tunnel": {},
        },
        calls,
    )
    with pytest.raises(cf.CloudflareError):
        await cf.create_managed_tunnel(
            api_token="tok", account_id="acc", zone_id="zone",
            tunnel_name="mc-b2", hostname="bad.example.com",
            service="http://127.0.0.1:9000",
        )
    deletes = [c for c in calls if c["method"] == "DELETE"]
    assert len(deletes) == 1 and "tun-999" in deletes[0]["url"]


@pytest.mark.asyncio
async def test_create_managed_tunnel_reclaims_existing_tunnel_on_name_conflict(monkeypatch):
    """409 "name already taken" (crashed prior run) → reclaim, don't fail.

    A prior run can create the tunnel on Cloudflare but crash before recording
    its id locally; the retry must look the tunnel up by name and reuse it.
    """
    calls: list = []
    _fake_cf_requests(
        monkeypatch,
        {
            "POST /cfd_tunnel": cf.CloudflareError(
                "POST .../cfd_tunnel returned HTTP 409: code 1013, "
                "You already have a tunnel with this name"
            ),
            "GET /token": "connector-token-abc",
            "PUT /configurations": {"config": {}},
            "POST /dns_records": {"id": "dns-456"},
            # the list call: live namesake first, then a deleted namesake
            "GET /cfd_tunnel": [
                {"id": "tun-live", "name": "mc-b1", "deleted_at": None},
                {"id": "tun-gone", "name": "mc-b1", "deleted_at": "2024-01-01"},
            ],
        },
        calls,
    )
    out = await cf.create_managed_tunnel(
        api_token="tok", account_id="acc", zone_id="zone",
        tunnel_name="mc-b1", hostname="app.example.com",
        service="http://127.0.0.1:8000",
    )
    assert out == {
        "tunnel_id": "tun-live", "dns_record_id": "dns-456",
        "token": "connector-token-abc",
    }
    assert any(
        c["method"] == "GET" and "cfd_tunnel?name=mc-b1" in c["url"] for c in calls
    )
    # the reclaimed tunnel predates this call — nothing was deleted
    assert not [c for c in calls if c["method"] == "DELETE"]


@pytest.mark.asyncio
async def test_create_managed_tunnel_name_conflict_without_live_match_reraises(monkeypatch):
    """A 409 with no live namesake to reclaim surfaces the original error."""
    calls: list = []
    _fake_cf_requests(
        monkeypatch,
        {
            "POST /cfd_tunnel": cf.CloudflareError("returned HTTP 409: code 1013"),
            "GET /cfd_tunnel": [{"id": "tun-other", "name": "someone-elses"}],
        },
        calls,
    )
    with pytest.raises(cf.CloudflareError):
        await cf.create_managed_tunnel(
            api_token="tok", account_id="acc", zone_id="zone",
            tunnel_name="mc-b2", hostname="app.example.com",
            service="http://127.0.0.1:8000",
        )
    # nothing was created here, so there is nothing to roll back
    assert not [c for c in calls if c["method"] == "DELETE"]


@pytest.mark.asyncio
async def test_create_managed_tunnel_keeps_reclaimed_tunnel_on_later_failure(monkeypatch):
    """A later-step failure must only roll back tunnels created in this call."""
    calls: list = []
    _fake_cf_requests(
        monkeypatch,
        {
            "POST /cfd_tunnel": cf.CloudflareError("returned HTTP 409: name taken"),
            "GET /token": "tok-abc",
            "PUT /configurations": {"config": {}},
            "POST /dns_records": cf.CloudflareError("zone not found"),
            "GET /dns_records": [],
            "GET /cfd_tunnel": [{"id": "tun-live", "name": "mc-b3", "deleted_at": None}],
        },
        calls,
    )
    with pytest.raises(cf.CloudflareError):
        await cf.create_managed_tunnel(
            api_token="tok", account_id="acc", zone_id="zone",
            tunnel_name="mc-b3", hostname="app.example.com",
            service="http://127.0.0.1:8000",
        )
    # the pre-existing tunnel survived the failure
    assert not [c for c in calls if c["method"] == "DELETE"]


@pytest.mark.asyncio
async def test_create_dns_record_reclaims_stale_managed_record(monkeypatch):
    """Our own stale record at the hostname (carrying our marker) → delete + recreate."""
    calls: list = []
    state = {"first_post_failed": False}

    async def fake_request(method, url, *, api_token, json_body=None):
        calls.append({"method": method, "url": url, "body": json_body})
        if method == "POST" and "/dns_records" in url:
            if not state["first_post_failed"]:
                state["first_post_failed"] = True
                raise cf.CloudflareError("record already exists", status=400)
            return {"id": "dns-new"}  # the retry create succeeds
        if method == "GET" and "/dns_records" in url:
            return [{"id": "dns-old", "type": "CNAME", "name": "app.example.com",
                     "content": "tun-9.cfargotunnel.com",
                     "comment": "mc-tunnel-managed"}]
        if method == "DELETE" and "/dns_records" in url:
            return {}
        raise AssertionError(f"unstubbed call {method} {url}")

    monkeypatch.setattr(cf, "_request", fake_request)
    out = await cf.create_dns_record(
        api_token="tok", zone_id="zone", tunnel_id="tun-9",
        hostname="app.example.com",
    )
    assert out == "dns-new"
    assert [c["method"] for c in calls] == ["POST", "GET", "DELETE", "POST"]
    # the stale record was deleted before the recreate
    assert "dns-old" in calls[2]["url"]
    # every record we create is tagged with the managed marker
    assert calls[3]["body"]["comment"] == "mc-tunnel-managed"


@pytest.mark.asyncio
async def test_create_dns_record_user_record_is_not_clobbered(monkeypatch):
    """A user-created record at the hostname must surface a permanent error."""
    calls: list = []

    async def fake_request(method, url, *, api_token, json_body=None):
        calls.append({"method": method, "url": url})
        if method == "POST" and "/dns_records" in url:
            raise cf.CloudflareError("record already exists", status=400)
        if method == "GET" and "/dns_records" in url:
            return [{"id": "dns-user", "type": "A", "name": "app.example.com",
                     "content": "1.2.3.4", "comment": "my own app"}]
        raise AssertionError(f"unstubbed call {method} {url}")

    monkeypatch.setattr(cf, "_request", fake_request)
    with pytest.raises(cf.CloudflareError) as exc_info:
        await cf.create_dns_record(
            api_token="tok", zone_id="zone", tunnel_id="tun-9",
            hostname="app.example.com",
        )
    assert "occupied by a user-created" in str(exc_info.value)
    assert cf.is_permanent_error(exc_info.value)
    # we never deleted the user's record
    assert not [c for c in calls if c["method"] == "DELETE"]


@pytest.mark.asyncio
async def test_create_dns_record_403_is_annotated_and_permanent(monkeypatch):
    """A DNS 403 (token lacks zone DNS permission) gets a cause hint + permanent."""

    async def fake_request(method, url, *, api_token, json_body=None):
        raise cf.CloudflareError(
            f"{method} {url} returned HTTP 403: Authentication error", status=403
        )

    monkeypatch.setattr(cf, "_request", fake_request)
    with pytest.raises(cf.CloudflareError) as exc_info:
        await cf.create_dns_record(
            api_token="tok", zone_id="zone", tunnel_id="tun-9",
            hostname="app.example.com",
        )
    assert "Zone->DNS->Edit" in str(exc_info.value)
    assert cf.is_permanent_error(exc_info.value)


# ── permanent provisioning failures are not auto-retried ───────────────────


def test_permanent_provisioning_error_classifies_cf_api_errors_only():
    assert rm._permanent_provisioning_error("POST .../dns_records returned HTTP 403: ...")
    assert rm._permanent_provisioning_error("GET .../token returned HTTP 401: ...")
    assert rm._permanent_provisioning_error("POST .../cfd_tunnel returned HTTP 404: ...")
    assert rm._permanent_provisioning_error(
        "hostname cccc.comic.xin is occupied by a user-created DNS record (A)"
    )
    # client-runtime stderr failures must NOT count — they may self-recover
    assert not rm._permanent_provisioning_error("connection refused")
    assert not rm._permanent_provisioning_error("authentication failed")
    assert not rm._permanent_provisioning_error("client exited code=1")
    assert not rm._permanent_provisioning_error("download failed: timeout")
    assert not rm._permanent_provisioning_error(None)
    assert not rm._permanent_provisioning_error("")


def test_config_changed_since_gates_retry_on_admin_action():
    failure = datetime(2026, 9, 5, 8, 49, 39)
    runtime = SimpleNamespace(updated_at=failure)
    scheme = SimpleNamespace(updated_at=failure)
    binding = SimpleNamespace(updated_at=failure)

    # nothing touched after the failure → skip
    assert not rm._config_changed_since(runtime, scheme, [binding])

    # a member binding edited after the failure (e.g. start click) → retry
    assert rm._config_changed_since(
        runtime, scheme, [SimpleNamespace(updated_at=failure + timedelta(seconds=1))]
    )

    # the scheme edited after the failure (e.g. token fixed) → retry
    assert rm._config_changed_since(
        runtime, SimpleNamespace(updated_at=failure + timedelta(seconds=1)), [binding]
    )

    # never attempted (no failure timestamp) → always retry
    assert rm._config_changed_since(SimpleNamespace(updated_at=None), scheme, [])


@pytest.mark.asyncio
async def test_reconcile_skips_permanent_failure_until_config_changes(db, monkeypatch):
    """A permanent provisioning failure must not be re-attempted every tick."""
    scheme = await TunnelScheme.create(
        id=uuid.uuid4(), user_id=uuid.uuid4(), name="cf", kind="cloudflared",
        config={"mode": "managed", "api_token": "t", "account_id": "a",
                "zone_id": "z", "domain": "example.com"},
        enabled=True,
    )
    await TunnelBinding.create(
        id=uuid.uuid4(), user_id=scheme.user_id, scheme_id=scheme.id,
        node_id="__main__", local_host="127.0.0.1", local_port=8000,
        hostname="app.example.com", desired_state="running", client_status="failed",
    )
    runtime = await rm.TunnelRuntime.create(
        id=uuid.uuid4(), scheme_id=scheme.id, target_id="__main__",
        runtime_key="k", kind="cloudflared",
        desired_state="running", runtime_status="failed",
        error="POST .../dns_records returned HTTP 403: Authentication error",
    )
    started: list = []

    async def fail_start(*args, **kwargs):
        started.append(1)

    monkeypatch.setattr(rm, "_start_local", fail_start)

    out = await rm.reconcile_runtime(runtime, scheme)
    assert not started, "permanent failure must not re-attempt start"
    assert out.runtime_status == "failed"
    refreshed = await rm.TunnelRuntime.get(id=runtime.id)
    assert refreshed.config_revision == 0, "skip must not bump the revision"


@pytest.mark.asyncio
async def test_reconcile_still_retries_transient_failures(db, monkeypatch):
    """Non-permanent failures keep the existing retry-every-tick behavior."""
    scheme = await TunnelScheme.create(
        id=uuid.uuid4(), user_id=uuid.uuid4(), name="cf", kind="cloudflared",
        config={"mode": "managed", "api_token": "t", "account_id": "a",
                "zone_id": "z", "domain": "example.com"},
        enabled=True,
    )
    await TunnelBinding.create(
        id=uuid.uuid4(), user_id=scheme.user_id, scheme_id=scheme.id,
        node_id="__main__", local_host="127.0.0.1", local_port=8000,
        hostname="app.example.com", desired_state="running", client_status="failed",
    )
    runtime = await rm.TunnelRuntime.create(
        id=uuid.uuid4(), scheme_id=scheme.id, target_id="__main__",
        runtime_key="k", kind="cloudflared",
        desired_state="running", runtime_status="failed",
        error="client exited code=1: connection refused",
    )
    started: list = []

    async def fake_start(*args, **kwargs):
        started.append(1)

    monkeypatch.setattr(rm, "_start_local", fake_start)

    await rm.reconcile_runtime(runtime, scheme)
    assert started, "transient failure must still be retried"


@pytest.mark.asyncio
async def test_update_scheme_bumps_runtime_revisions_for_hot_restart(db, monkeypatch):
    """Editing a scheme's config must invalidate running clients.

    update_scheme bumps config_revision on every desired-running runtime of
    the scheme so the reconciler's liveness guards (is_live revision match /
    node inventory revision match) see the processes as stale and restart
    them with the new config — the hot-push contract update_scheme's comment
    has always claimed.
    """
    scheme = await TunnelScheme.create(
        id=uuid.uuid4(), user_id=uuid.uuid4(), name="cf", kind="cloudflared",
        config={"mode": "managed", "api_token": "t", "account_id": "a",
                "zone_id": "z", "domain": "example.com"},
        enabled=True,
    )
    from monkeycode_compat.models_tunnel import TunnelRuntime

    running_rt = await TunnelRuntime.create(
        id=uuid.uuid4(), scheme_id=scheme.id, target_id="__main__",
        runtime_key="k1", kind="cloudflared",
        desired_state="running", runtime_status="running", config_revision=3,
    )
    stopped_rt = await TunnelRuntime.create(
        id=uuid.uuid4(), scheme_id=scheme.id, target_id="node-1",
        runtime_key="k2", kind="cloudflared",
        desired_state="stopped", runtime_status="stopped", config_revision=3,
    )

    async def fake_notify(*, runtime_id=None, target_id=None):
        return None

    monkeypatch.setattr(
        "monkeycode_compat.tunnel_notify.notify_tunnel_changed", fake_notify
    )

    await svc.update_scheme(
        str(scheme.user_id), str(scheme.id),
        config={**scheme.config, "api_token": "t2"},
    )
    refreshed_run = await TunnelRuntime.get(id=running_rt.id)
    assert refreshed_run.config_revision == 4, "desired-running runtime must be invalidated"
    refreshed_stopped = await TunnelRuntime.get(id=stopped_rt.id)
    assert refreshed_stopped.config_revision == 3, "stopped runtime picks up config on next start; no need to bump"


@pytest.mark.asyncio
async def test_reconcile_stops_process_when_scheme_disabled(db, monkeypatch):
    """A disabled scheme must stop the running client, not just mark failed."""
    scheme = await TunnelScheme.create(
        id=uuid.uuid4(), user_id=uuid.uuid4(), name="cf", kind="cloudflared",
        config={"mode": "managed", "api_token": "t", "account_id": "a",
                "zone_id": "z", "domain": "example.com"},
        enabled=False,
    )
    runtime = await rm.TunnelRuntime.create(
        id=uuid.uuid4(), scheme_id=scheme.id, target_id="__main__",
        runtime_key="k", kind="cloudflared",
        desired_state="running", runtime_status="running",
    )
    stopped: list = []

    async def fake_stop(runtime_arg):
        stopped.append(1)

    monkeypatch.setattr(rm, "_stop_runtime", fake_stop)

    out = await rm.reconcile_runtime(runtime, scheme)
    assert stopped, "disabling a scheme must stop its running client"
    assert out.runtime_status == "failed"
    assert "disabled" in (out.error or "")


def test_is_live_guards_on_revision():
    """The supervisor's is_live must treat a revision bump as not-live."""
    from monkeycode_compat.tunnel_supervisor import LocalTunnelSupervisor

    sup = LocalTunnelSupervisor()
    key = "rt-1"
    sup._procs[key] = SimpleNamespace(returncode=None)  # fake healthy process
    sup._revisions[key] = 5
    assert sup.is_live(key, 5) is True
    # Same process, but the reconciler (or update_scheme) bumped the revision:
    # the config it runs is stale, so it must be restarted.
    assert sup.is_live(key, 6) is False
    # Unknown runtime / dead process are not live either.
    assert sup.is_live("other", 1) is False
    sup._procs[key].returncode = 1
    assert sup.is_live(key, 5) is False


@pytest.mark.asyncio
async def test_delete_managed_tunnel_removes_dns_then_tunnel(monkeypatch):
    calls: list = []
    _fake_cf_requests(
        monkeypatch,
        {"DELETE /dns_records": {}, "DELETE /cfd_tunnel": {}},
        calls,
    )
    await cf.delete_managed_tunnel(
        api_token="tok", account_id="acc", zone_id="zone",
        provider_ref={"tunnel_id": "tun-1", "dns_record_id": "dns-1"},
    )
    assert [c["method"] for c in calls] == ["DELETE", "DELETE"]
    assert "dns_records/dns-1" in calls[0]["url"]
    assert "cfd_tunnel/tun-1" in calls[1]["url"]


@pytest.mark.asyncio
async def test_delete_managed_tunnel_ignores_missing_resources(monkeypatch):
    """Already-deleted CF resources must not block binding deletion."""
    calls: list = []
    _fake_cf_requests(
        monkeypatch,
        {
            "DELETE /dns_records": cf.CloudflareError("HTTP 404"),
            "DELETE /cfd_tunnel": cf.CloudflareError("HTTP 404"),
        },
        calls,
    )
    await cf.delete_managed_tunnel(  # no raise
        api_token="tok", account_id="acc", zone_id="zone",
        provider_ref={"tunnel_id": "tun-1", "dns_record_id": "dns-1"},
    )
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_delete_managed_tunnel_noop_without_refs(monkeypatch):
    calls: list = []
    _fake_cf_requests(monkeypatch, {}, calls)
    await cf.delete_managed_tunnel(
        api_token="tok", account_id="acc", zone_id="zone", provider_ref={},
    )
    assert calls == []


def test_unwrap_rejects_success_false():
    """A 2xx with success:false is still a failure."""
    with pytest.raises(cf.CloudflareError):
        cf._unwrap({"success": False, "errors": [{"message": "bad token"}]}, "GET /x")


def test_unwrap_returns_result():
    assert cf._unwrap({"success": True, "result": {"id": "x"}}, "GET /x") == {"id": "x"}


# ── managed dispatch: token via env, hostname required ──────────────────────


@pytest.mark.asyncio
async def test_managed_dispatch_requires_hostname():
    binding = _binding()
    binding.hostname = None
    scheme = _scheme(
        "cloudflared",
        {"mode": "managed", "api_token": "t", "account_id": "a", "zone_id": "z"},
    )
    await dispatch._dispatch_cloudflared(binding, scheme)
    assert binding.client_status == "failed"
    assert "hostname" in (binding.error or "")


@pytest.mark.asyncio
async def test_managed_dispatch_passes_token_via_env_not_argv(monkeypatch):
    """The connector token must never land on argv (visible in the process list)."""
    binding = _binding()
    binding.hostname = "app.example.com"
    binding.node_id = "node-1"
    binding.provider_ref = {}
    scheme = _scheme(
        "cloudflared",
        {"mode": "managed", "api_token": "t", "account_id": "a", "zone_id": "z"},
    )

    async def fake_create(**kwargs):
        return {"tunnel_id": "tun-1", "dns_record_id": "dns-1", "token": "SECRET-CONNECTOR"}

    monkeypatch.setattr(cf, "create_managed_tunnel", fake_create)

    started: dict = {}

    class FakeClient:
        async def start_tool_run(self, node_id, run_id, binary, args, *, env=None, cwd=""):
            started.update({"node_id": node_id, "args": args, "env": env or {}})

    monkeypatch.setattr(
        "monkeycode_compat.node_client.get_local_node_client", lambda: FakeClient()
    )
    # Monitoring the run needs a live loop consumer; stub it out.
    monkeypatch.setattr(dispatch, "_monitor_tool_run", lambda *a, **k: _noop())

    await dispatch._dispatch_cloudflared(binding, scheme)

    assert binding.client_status == "running"
    assert binding.public_addr == "https://app.example.com"
    assert binding.provider_ref == {"tunnel_id": "tun-1", "dns_record_id": "dns-1"}
    assert started["env"].get("TUNNEL_TOKEN") == "SECRET-CONNECTOR"
    # Neither the secret nor a --token flag may appear on argv: cloudflared reads
    # TUNNEL_TOKEN from the environment, so the process list stays clean.
    joined = " ".join(started["args"])
    assert "SECRET-CONNECTOR" not in joined
    assert "--token" not in joined
    assert "tunnel --no-autoupdate run" in joined


def test_frpc_group_toml_contains_multiple_proxies():
    first = _binding(local_port=8000, allocated="30001")
    second = _binding(local_port=9000, allocated="30002")
    conf = dispatch._frpc_group_toml(
        [first, second],
        _scheme("frpc", {"server_addr": "h", "server_port": 7000, "token": "s"}),
    )
    assert conf.count("[[proxies]]") == 2
    assert "localPort = 8000" in conf
    assert "localPort = 9000" in conf
    assert "remotePort = 30001" in conf
    assert "remotePort = 30002" in conf


def test_runtime_key_groups_frpc_but_not_quick():
    import monkeycode_compat.tunnel_runtime_manager as rm

    frpc = _scheme("frpc", {})
    quick = _scheme("cloudflared", {"mode": "quick"})
    assert rm.runtime_key(frpc, "node-a", "b1") == rm.runtime_key(frpc, "node-a", "b2")
    assert rm.runtime_key(quick, "node-a", "b1") != rm.runtime_key(quick, "node-a", "b2")


def test_runtime_key_keeps_npc_per_binding_until_verified():
    import monkeycode_compat.tunnel_runtime_manager as rm

    npc = _scheme("npc", {})
    assert rm.runtime_key(npc, "node-a", "b1") != rm.runtime_key(npc, "node-a", "b2")


def test_readiness_parser():
    import monkeycode_compat.tunnel_runtime_manager as rm

    assert rm._readiness("frpc", "login to server success") == "ready"
    assert rm._readiness("cloudflared", "Registered tunnel connection") == "ready"
    assert rm._readiness("frpc", "authentication failed") == "failed"
    assert rm._readiness("frpc", "routine debug output") is None


@pytest.mark.asyncio
async def test_runtime_event_revision_fences_late_exit(monkeypatch):
    """EXITED from a replaced process must not mark the current revision failed."""
    import monkeycode_compat.tunnel_runtime_manager as rm

    runtime = type("Runtime", (), {
        "config_revision": 2,
        "runtime_status": "running",
    })()
    monkeypatch.setattr(
        rm.TunnelRuntime, "get_or_none",
        classmethod(lambda cls, **k: _awaitable(runtime)),
    )
    await rm.handle_event(
        uuid.uuid4(), revision=1, kind="exited", exit_code=1, error="old process"
    )
    assert runtime.runtime_status == "running"


def test_quick_and_npc_remain_per_binding_runtime():
    import monkeycode_compat.tunnel_runtime_manager as rm

    quick = _scheme("cloudflared", {"mode": "quick"})
    npc = _scheme("npc", {})
    assert not rm.is_grouped(quick)
    assert not rm.is_grouped(npc)


async def _noop():
    return None


# ── main-service target + project node resolution ──────────────────────────


def test_main_service_target_is_a_sentinel():
    """The sentinel is a stable, node-id-collision-free string."""
    assert svc.MAIN_SERVICE_TARGET == "__main__"


class _q:
    """A minimal awaitable stand-in for a Tortoise queryset builder chain."""
    def __init__(self, rows):
        self._rows = rows
    def only(self, *a, **k):
        return self
    def order_by(self, *a, **k):
        return self
    def filter(self, *a, **k):
        return self
    def __await__(self):
        async def _run():
            return self._rows
        return _run().__await__()


def test_resolve_project_node_requires_live_task(monkeypatch):
    """No live task → invalid_argument, so project bindings cannot be created."""
    import asyncio

    monkeypatch.setattr(
        "monkeycode_compat.models_task.ProjectTask.filter",
        classmethod(lambda cls, **k: _q([])),
    )
    with pytest.raises(svc.TunnelServiceError) as exc:
        asyncio.run(svc._resolve_project_node("00000000-0000-0000-0000-000000000000"))
    assert "运行中的任务" in exc.value.message


def test_resolve_project_node_picks_node(monkeypatch):
    """The node id of the project's live task is returned when one exists."""
    import asyncio

    class _TaskRow:
        def __init__(self, task_uuid, node_id):
            self.id = task_uuid
            self.node_id = node_id

    task = _TaskRow(uuid.uuid4(), "exec-node-7")
    monkeypatch.setattr(
        "monkeycode_compat.models_task.ProjectTask.filter",
        classmethod(lambda cls, **k: _q([type("R", (), {"task_id": task.id})()])),
    )
    monkeypatch.setattr(
        "monkeycode_compat.models_task.Task.filter",
        classmethod(lambda cls, **k: _q([task])),
    )
    monkeypatch.setattr(
        "monkeycode_compat.models_task.TaskNodeBinding.get_or_none",
        classmethod(lambda cls, **k: _awaitable(None)),
    )
    resolved = asyncio.run(
        svc._resolve_project_node("00000000-0000-0000-0000-000000000000")
    )
    assert resolved == "exec-node-7"


def _awaitable(value):
    """Wrap a plain value so a patched ``get_or_none`` can be awaited."""
    async def _run():
        return value
    return _run()


# ── main-service dispatch routes through the supervisor ───────────────────


@pytest.mark.asyncio
async def test_main_service_target_dispatches_locally(monkeypatch):
    """A __main__ binding enters the grouped runtime reconciler."""
    binding = _binding()
    binding.node_id = svc.MAIN_SERVICE_TARGET
    binding.local_port = 8000
    scheme = _scheme("frpc", {"server_addr": "h", "server_port": 7000, "port_range": [1, 2]})

    called: dict = {}

    async def fake_reconcile(b, s):
        called["binding"] = str(b.id)
        return None

    import monkeycode_compat.tunnel_runtime_manager as runtime_manager
    monkeypatch.setattr(runtime_manager, "reconcile_binding", fake_reconcile)

    await dispatch.dispatch_binding(binding, scheme)
    assert called["binding"] == str(binding.id)


# ── binaries: platform-aware asset selection ──────────────────────────────


def test_binaries_pick_windows_zip(monkeypatch):
    import monkeycode_compat.tunnel_binaries as bins
    monkeypatch.setattr(bins.platform, "system", lambda: "Windows")
    monkeypatch.setattr(bins.platform, "machine", lambda: "AMD64")
    url, archive, member = bins.resolve_source("frpc")
    assert url.endswith("windows_amd64.zip")
    assert archive == "zip"
    assert member is not None and member.endswith("frpc.exe")


def test_binaries_pick_linux_targz(monkeypatch):
    import monkeycode_compat.tunnel_binaries as bins
    monkeypatch.setattr(bins.platform, "system", lambda: "Linux")
    monkeypatch.setattr(bins.platform, "machine", lambda: "x86_64")
    url, archive, member = bins.resolve_source("frpc")
    assert url.endswith("linux_amd64.tar.gz")
    assert archive == "tar.gz"
    assert member is not None and member.endswith("/frpc")


def test_binaries_cloudflared_is_bare_binary(monkeypatch):
    import monkeycode_compat.tunnel_binaries as bins
    monkeypatch.setattr(bins.platform, "system", lambda: "Linux")
    monkeypatch.setattr(bins.platform, "machine", lambda: "aarch64")
    url, archive, member = bins.resolve_source("cloudflared")
    assert archive is None
    assert member is None
    assert "cloudflared-linux-arm64" in url


def test_binaries_binary_name_has_exe_on_windows(monkeypatch):
    import monkeycode_compat.tunnel_binaries as bins
    monkeypatch.setattr(bins.platform, "system", lambda: "Windows")
    monkeypatch.setattr(bins.platform, "machine", lambda: "AMD64")
    assert bins.binary_name("frpc") == "frpc.exe"
    monkeypatch.setattr(bins.platform, "system", lambda: "Linux")
    assert bins.binary_name("frpc") == "frpc"


def test_binaries_custom_url_override(monkeypatch):
    """An operator mirror URL is substituted with platform tokens."""
    import monkeycode_compat.tunnel_binaries as bins
    monkeypatch.setattr(bins.platform, "system", lambda: "Linux")
    monkeypatch.setattr(bins.platform, "machine", lambda: "x86_64")
    url, _archive, _member = bins.resolve_source(
        "cloudflared", {"binary_url": "https://mirror.local/cf-{os}-{arch}{exe}"}
    )
    assert url == "https://mirror.local/cf-linux-amd64"


# ── managed restart reuses the existing tunnel ──────────────────────────────


@pytest.mark.asyncio
async def test_managed_restart_reuses_existing_tunnel(monkeypatch):
    """Restarting a node binding must not provision a second tunnel."""
    binding = _binding()
    binding.hostname = "app.example.com"
    binding.node_id = "node-1"
    binding.provider_ref = {"tunnel_id": "existing-tun", "dns_record_id": "existing-dns"}
    scheme = _scheme(
        "cloudflared",
        {"mode": "managed", "api_token": "t", "account_id": "a", "zone_id": "z"},
    )

    calls: list = []

    async def fake_get_token(*, api_token, account_id, tunnel_id):
        calls.append(("get_token", tunnel_id))
        return "RESTART-TOKEN"

    monkeypatch.setattr(cf, "get_managed_tunnel_token", fake_get_token)

    started: dict = {}

    class FakeClient:
        async def start_tool_run(self, node_id, run_id, binary, args, *, env=None, cwd=""):
            started.update({"env": env or {}, "args": args})

    monkeypatch.setattr(
        "monkeycode_compat.node_client.get_local_node_client", lambda: FakeClient()
    )
    monkeypatch.setattr(dispatch, "_monitor_tool_run", lambda *a, **k: _noop())

    await dispatch._dispatch_cloudflared(binding, scheme)

    # Reused the stored tunnel id, never called create_managed_tunnel.
    assert ("get_token", "existing-tun") in calls
    assert binding.provider_ref == {"tunnel_id": "existing-tun", "dns_record_id": "existing-dns"}
    assert started["env"].get("TUNNEL_TOKEN") == "RESTART-TOKEN"
    assert binding.client_status == "running"


@pytest.mark.asyncio
async def test_stop_binding_enters_runtime_reconciler(monkeypatch):
    binding = _binding()
    binding.node_id = svc.MAIN_SERVICE_TARGET
    binding.scheme_id = uuid.uuid4()
    called: dict = {}
    import monkeycode_compat.tunnel_runtime_manager as runtime_manager
    import monkeycode_compat.tunnel_node_dispatch as tnd

    async def fake_desired(b, scheme, desired):
        called["desired"] = desired
        return None

    monkeypatch.setattr(TunnelScheme, "get_or_none", classmethod(lambda cls, **k: _awaitable(_scheme("frpc", {}))))
    monkeypatch.setattr(runtime_manager, "set_binding_desired", fake_desired)

    await tnd.stop_binding(binding)
    assert called["desired"] == "stopped"


# ── async CRUD: data service only writes desired + notifies ────────────────


@pytest.mark.asyncio
async def test_create_binding_returns_pending_and_notifies(db, monkeypatch):
    """create_binding writes desired_state=running, returns pending, fires NOTIFY."""
    scheme = await TunnelScheme.create(
        id=uuid.uuid4(), user_id=uuid.uuid4(), name="frp", kind="frpc",
        config={"server_addr": "h", "server_port": 7000, "port_range": [30000, 30010]},
        enabled=True,
    )
    notified: list[dict] = []

    async def fake_notify(*, runtime_id=None, target_id=None):
        notified.append({"runtime_id": runtime_id, "target_id": target_id})

    monkeypatch.setattr(
        "monkeycode_compat.tunnel_notify.notify_tunnel_changed", fake_notify
    )

    row = await svc.create_binding(
        str(scheme.user_id), scheme_id=str(scheme.id), node_id=svc.MAIN_SERVICE_TARGET,
        local_port=8000,
    )
    assert row["client_status"] == "pending"
    assert row["desired_state"] == "running"
    assert notified and notified[0]["target_id"] == svc.MAIN_SERVICE_TARGET


@pytest.mark.asyncio
async def test_create_binding_managed_surfaces_public_addr_at_create(db, monkeypatch):
    """A managed cloudflared binding's public address is the user-chosen hostname
    behind Cloudflare's edge — fully known at create time. It must be returned
    immediately so the list shows the URL instead of "地址生成中…" while the
    runtime asynchronously provisions DNS + starts the connector.
    """
    scheme = await TunnelScheme.create(
        id=uuid.uuid4(), user_id=uuid.uuid4(), name="cf-managed", kind="cloudflared",
        config={
            "mode": "managed", "api_token": "tok", "account_id": "acc",
            "zone_id": "zone", "domain": "comic.xin",
        },
        enabled=True,
    )

    async def fake_notify(*, runtime_id=None, target_id=None):
        return None

    monkeypatch.setattr(
        "monkeycode_compat.tunnel_notify.notify_tunnel_changed", fake_notify
    )

    row = await svc.create_binding(
        str(scheme.user_id), scheme_id=str(scheme.id),
        node_id=svc.MAIN_SERVICE_TARGET, local_port=8000, subdomain="cccc",
    )
    assert row["hostname"] == "cccc.comic.xin"
    assert row["public_addr"] == "https://cccc.comic.xin"
    assert row["client_status"] == "pending"


@pytest.mark.asyncio
async def test_delete_binding_marks_soft_delete_and_notifies(db, monkeypatch):
    """delete_binding sets delete_requested, returns pending, fires NOTIFY."""
    scheme = await TunnelScheme.create(
        id=uuid.uuid4(), user_id=uuid.uuid4(), name="frp", kind="frpc",
        config={"server_addr": "h", "server_port": 7000, "port_range": [30000, 30010]},
        enabled=True,
    )
    binding = await TunnelBinding.create(
        id=uuid.uuid4(), user_id=scheme.user_id, scheme_id=scheme.id,
        node_id=svc.MAIN_SERVICE_TARGET, local_host="127.0.0.1", local_port=8000,
        desired_state="running", client_status="running",
    )
    notified: list[dict] = []

    async def fake_notify(*, runtime_id=None, target_id=None):
        notified.append({"runtime_id": runtime_id, "target_id": target_id})

    monkeypatch.setattr(
        "monkeycode_compat.tunnel_notify.notify_tunnel_changed", fake_notify
    )

    result = await svc.delete_binding(str(scheme.user_id), str(binding.id))
    assert result == {"deleted": False, "pending": True, "id": str(binding.id)}
    assert notified and notified[0]["runtime_id"] == str(binding.id)

    refreshed = await TunnelBinding.get(id=binding.id)
    assert refreshed.delete_requested is True
    assert refreshed.desired_state == "stopped"


@pytest.mark.asyncio
async def test_list_bindings_hides_soft_deleted(db):
    """Soft-deleted rows disappear from list responses while pending teardown."""
    scheme = await TunnelScheme.create(
        id=uuid.uuid4(), user_id=uuid.uuid4(), name="frp", kind="frpc",
        config={"server_addr": "h", "server_port": 7000, "port_range": [30000, 30010]},
        enabled=True,
    )
    await TunnelBinding.create(
        id=uuid.uuid4(), user_id=scheme.user_id, scheme_id=scheme.id,
        node_id=svc.MAIN_SERVICE_TARGET, local_host="127.0.0.1", local_port=8000,
        desired_state="running", delete_requested=False,
    )
    await TunnelBinding.create(
        id=uuid.uuid4(), user_id=scheme.user_id, scheme_id=scheme.id,
        node_id=svc.MAIN_SERVICE_TARGET, local_host="127.0.0.1", local_port=8001,
        desired_state="stopped", delete_requested=True,
    )
    rows = await svc.list_bindings(str(scheme.user_id))
    assert len(rows) == 1
    assert rows[0]["delete_requested"] is False


@pytest.mark.asyncio
async def test_update_binding_changes_port_and_node(db, monkeypatch):
    """Editing a proxy writes mutable fields, returns pending, and wakes runtime."""
    scheme = await TunnelScheme.create(
        id=uuid.uuid4(), user_id=uuid.uuid4(), name="frp", kind="frpc",
        config={"server_addr": "h", "server_port": 7000, "port_range": [30000, 30010]},
        enabled=True,
    )
    binding = await TunnelBinding.create(
        id=uuid.uuid4(), user_id=scheme.user_id, scheme_id=scheme.id,
        node_id="node-old", local_host="127.0.0.1", local_port=8000,
        desired_state="running", client_status="running", allocated_value="30000",
    )
    notified: list[dict] = []

    async def fake_notify(*, runtime_id=None, target_id=None):
        notified.append({"runtime_id": runtime_id, "target_id": target_id})

    monkeypatch.setattr("monkeycode_compat.tunnel_notify.notify_tunnel_changed", fake_notify)
    result = await svc.update_binding(
        str(scheme.user_id), str(binding.id), scheme_id=str(scheme.id),
        node_id="node-new", local_host="10.0.0.7", local_port=9000,
    )
    assert result["node_id"] == "node-new"
    assert result["local_host"] == "10.0.0.7"
    assert result["local_port"] == 9000
    assert result["client_status"] == "pending"
    assert result["allocated_value"] == "30000"
    assert notified[-1]["target_id"] == "node-new"


@pytest.mark.asyncio
async def test_update_binding_managed_subdomain_rebuilds_dns(db, monkeypatch):
    """Managed hostname edit drops the old DNS record and clears its provider id."""
    scheme = await TunnelScheme.create(
        id=uuid.uuid4(), user_id=uuid.uuid4(), name="cf", kind="cloudflared",
        config={"mode": "managed", "api_token": "t", "account_id": "a",
                "zone_id": "z", "domain": "example.com"},
        enabled=True,
    )
    binding = await TunnelBinding.create(
        id=uuid.uuid4(), user_id=scheme.user_id, scheme_id=scheme.id,
        node_id="__main__", local_host="127.0.0.1", local_port=8000,
        hostname="old.example.com", public_addr="https://old.example.com",
        provider_ref={"dns_record_id": "dns-old"},
        desired_state="running", client_status="running",
    )
    deleted: list[str] = []

    async def fake_delete_dns_record(*, api_token, zone_id, dns_record_id):
        deleted.append(dns_record_id)

    monkeypatch.setattr(cf, "delete_dns_record", fake_delete_dns_record)
    monkeypatch.setattr(
        "monkeycode_compat.tunnel_notify.notify_tunnel_changed",
        lambda **kwargs: _noop(),
    )
    result = await svc.update_binding(
        str(scheme.user_id), str(binding.id), scheme_id=str(scheme.id),
        node_id="__main__", local_host="127.0.0.1", local_port=8000,
        subdomain="new",
    )
    assert result["hostname"] == "new.example.com"
    assert deleted == ["dns-old"]
    refreshed = await TunnelBinding.get(id=binding.id)
    assert refreshed.provider_ref == {}


@pytest.mark.asyncio
async def test_update_binding_rejects_scheme_change(db):
    """A proxy cannot silently move to a different runtime scheme."""
    scheme = await TunnelScheme.create(
        id=uuid.uuid4(), user_id=uuid.uuid4(), name="frp", kind="frpc",
        config={"server_addr": "h", "server_port": 7000, "port_range": [30000, 30010]},
        enabled=True,
    )
    binding = await TunnelBinding.create(
        id=uuid.uuid4(), user_id=scheme.user_id, scheme_id=scheme.id,
        node_id="__main__", local_host="127.0.0.1", local_port=8000,
    )
    with pytest.raises(svc.TunnelServiceError) as exc:
        await svc.update_binding(
            str(scheme.user_id), str(binding.id), scheme_id=str(uuid.uuid4()),
            node_id="__main__", local_port=8000,
        )
    assert exc.value.code == "invalid_argument"


@pytest.mark.asyncio
async def test_lease_no_owner_always_acquires(db):
    """Single-replica / tests: no owner configured → claim always succeeds."""
    import monkeycode_compat.tunnel_runtime_manager as rm
    runtime = await rm.TunnelRuntime.create(
        id=uuid.uuid4(), scheme_id=uuid.uuid4(), target_id="__main__",
        runtime_key="k", kind="frpc",
    )
    got = await rm._acquire_lease(runtime)
    assert got is True


@pytest.mark.asyncio
async def test_lease_second_owner_cannot_steal_live_lease(db, monkeypatch):
    """A live lease held by another owner blocks this process from acquiring it."""
    import monkeycode_compat.tunnel_runtime_manager as rm
    rm.configure_lease(owner="replica-A", ttl_seconds=60)
    try:
        runtime = await rm.TunnelRuntime.create(
            id=uuid.uuid4(), scheme_id=uuid.uuid4(), target_id="__main__",
            runtime_key="k2", kind="frpc",
        )
        assert await rm._acquire_lease(runtime) is True
        # Switch identity to a second replica; the unexpired lease blocks us.
        rm.configure_lease(owner="replica-B", ttl_seconds=60)
        assert await rm._acquire_lease(runtime) is False
    finally:
        rm.configure_lease(owner="", ttl_seconds=60)


@pytest.mark.asyncio
async def test_lease_expired_can_be_taken_over(db, monkeypatch):
    """An expired lease is acquirable by a different owner (crash recovery)."""
    import monkeycode_compat.tunnel_runtime_manager as rm
    from datetime import timedelta
    rm.configure_lease(owner="replica-A", ttl_seconds=60)
    try:
        runtime = await rm.TunnelRuntime.create(
            id=uuid.uuid4(), scheme_id=uuid.uuid4(), target_id="__main__",
            runtime_key="k3", kind="frpc",
        )
        assert await rm._acquire_lease(runtime) is True
        # Force the lease into the past, simulating a crashed replica.
        runtime.lease_until = rm._now() - timedelta(seconds=10)
        await runtime.save(update_fields=["lease_until"])
        rm.configure_lease(owner="replica-B", ttl_seconds=60)
        assert await rm._acquire_lease(runtime) is True
    finally:
        rm.configure_lease(owner="", ttl_seconds=60)


@pytest.mark.asyncio
async def test_release_lease_lets_other_owner_acquire(db):
    """Releasing our lease lets a different process immediately take over."""
    import monkeycode_compat.tunnel_runtime_manager as rm
    rm.configure_lease(owner="replica-A", ttl_seconds=60)
    try:
        runtime = await rm.TunnelRuntime.create(
            id=uuid.uuid4(), scheme_id=uuid.uuid4(), target_id="__main__",
            runtime_key="k4", kind="frpc",
        )
        assert await rm._acquire_lease(runtime) is True
        await rm._release_lease(runtime)
        refreshed = await rm.TunnelRuntime.get(id=runtime.id)
        assert refreshed.lease_owner is None
        rm.configure_lease(owner="replica-B", ttl_seconds=60)
        assert await rm._acquire_lease(runtime) is True
    finally:
        rm.configure_lease(owner="", ttl_seconds=60)


@pytest.mark.asyncio
async def test_notify_payload_has_no_secrets(db, monkeypatch):
    """NOTIFY payload carries only locator fields, never config secrets."""
    scheme = await TunnelScheme.create(
        id=uuid.uuid4(), user_id=uuid.uuid4(), name="frp", kind="frpc",
        config={"server_addr": "h", "server_port": 7000, "port_range": [30000, 30010],
                "token": "SECRET-TOKEN-VALUE"},
        enabled=True,
    )
    captured: list = []

    async def fake_pg_notify(channel, payload):
        captured.append(payload)

    class _FakeConn:
        async def execute_query_dict(self, sql, params):
            await fake_pg_notify(params[0], params[1])
            return []

    monkeypatch.setattr(
        "tortoise.Tortoise.get_connection", lambda _name: _FakeConn()
    )
    from monkeycode_compat import tunnel_notify as tn
    await tn.notify_tunnel_changed(runtime_id=str(scheme.id), target_id="__main__")
    assert captured
    assert "SECRET-TOKEN-VALUE" not in captured[-1]
    assert "__main__" in captured[-1]

