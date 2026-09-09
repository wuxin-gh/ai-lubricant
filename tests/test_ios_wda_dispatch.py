"""Contract tests for the iOS WDA job-request builder.

The Go node speaks the flat ``NodeIosSigningMaterial`` (a mode enum + per-mode
fields) and numeric ``IosWdaJobAction``; the platform side carries lowercase
strings ("prepare"/"asc"/…) and base64-encoded bytes. ``_build_wda_job_request``
is the single place that translation happens, and it is pure (no node I/O), so
these tests pin the three signing kinds + four actions without a live node.

They also guard the latent bugs this builder replaces: the old code passed
``action="prepare"`` (protobuf rejects lowercase labels → ValueError), wrote
``req.signing_asc`` / ``req.signing_p12`` (those sub-messages don't exist on the
flat material → AttributeError), and put ``wda_bundle_id`` / ``xctest_config_name``
on the job request (those fields live on the artifact).
"""
from __future__ import annotations

import base64

import pytest

from node_server import agentcompose_v2_pb2 as pb
from node_server.service import _build_wda_job_request, _fill_wda_signing


def _asc_profile() -> dict:
    return {
        "kind": "asc",
        "secret_data": {
            "p8_key": "-----BEGIN PRIVATE KEY-----\nZm9v\n-----END PRIVATE KEY-----",
            "key_id": "K1234ABCD",
            "issuer_id": "issuer-uuid",
            "team_id": "TEAM1234",
        },
    }


def _p12_profile() -> dict:
    return {
        "kind": "p12",
        "secret_data": {
            "p12_base64": base64.b64encode(b"P12-BYTES").decode(),
            "p12_password": "hunter2",
            "mobileprovision_base64": base64.b64encode(b"MP-BYTES").decode(),
        },
    }


def _presigned_profile() -> dict:
    return {"kind": "presigned", "secret_data": {}}


def _artifact() -> dict:
    return {
        "sha256": "a" * 64,
        "download_url": "https://github.com/o/r/d/device-control-1-ios.ipa",
        "size_bytes": 1024,
        "version": "1.2.3",
    }


# ── action string → enum ──────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "label,enum",
    [
        ("prepare", pb.IosWdaJobAction.IOS_WDA_JOB_ACTION_PREPARE),
        ("renew", pb.IosWdaJobAction.IOS_WDA_JOB_ACTION_RENEW),
        ("reinstall", pb.IosWdaJobAction.IOS_WDA_JOB_ACTION_REINSTALL),
        ("install_signed", pb.IosWdaJobAction.IOS_WDA_JOB_ACTION_INSTALL_SIGNED),
        # uppercase / whitespace tolerated
        (" PREPARE ", pb.IosWdaJobAction.IOS_WDA_JOB_ACTION_PREPARE),
    ],
)
def test_action_label_maps_to_enum(label: str, enum: int) -> None:
    req = _build_wda_job_request(job_id="j", udid="u", device_id="d", action=label)
    assert req.action == enum


def test_unknown_action_raises_value_error() -> None:
    with pytest.raises(ValueError, match="unknown wda action"):
        _build_wda_job_request(job_id="j", udid="u", device_id="d", action="bogus")


# ── signing kind → flat material ─────────────────────────────────────────────


def test_asc_maps_to_app_store_connect_mode() -> None:
    req = _build_wda_job_request(
        job_id="j", udid="u", device_id="d", action="prepare", signing_profile=_asc_profile()
    )
    s = req.signing
    assert s.mode == pb.IosSigningMode.IOS_SIGNING_MODE_APP_STORE_CONNECT
    assert s.asc_key_id == "K1234ABCD"
    assert s.asc_issuer_id == "issuer-uuid"
    # p8 key travels as bytes (the proto field is bytes asc_private_key).
    assert s.asc_private_key == _asc_profile()["secret_data"]["p8_key"].encode("utf-8")
    # team_id is stored for reference; the node derives the team from the key, so
    # the material carries no team field and team_id is intentionally dropped.
    assert s.provisioning_profile == b""
    assert s.certificate_p12 == b""


def test_p12_maps_to_manual_p12_mode_and_decodes_base64() -> None:
    req = _build_wda_job_request(
        job_id="j", udid="u", device_id="d", action="prepare", signing_profile=_p12_profile()
    )
    s = req.signing
    assert s.mode == pb.IosSigningMode.IOS_SIGNING_MODE_MANUAL_P12
    # base64 is decoded to raw bytes (the node writes these 0600 to the work dir).
    assert s.certificate_p12 == b"P12-BYTES"
    assert s.provisioning_profile == b"MP-BYTES"
    assert s.p12_password == "hunter2"


def test_p12_rejects_non_base64() -> None:
    bad = {"kind": "p12", "secret_data": {"p12_base64": "@@@", "p12_password": "", "mobileprovision_base64": ""}}
    with pytest.raises(ValueError, match="p12_base64"):
        _build_wda_job_request(job_id="j", udid="u", device_id="d", action="prepare", signing_profile=bad)


def test_p12_rejects_empty_base64() -> None:
    bad = {
        "kind": "p12",
        "secret_data": {"p12_base64": "", "p12_password": "x", "mobileprovision_base64": ""},
    }
    with pytest.raises(ValueError, match="p12_base64"):
        _build_wda_job_request(job_id="j", udid="u", device_id="d", action="prepare", signing_profile=bad)


def test_presigned_maps_to_presigned_mode() -> None:
    req = _build_wda_job_request(
        job_id="j", udid="u", device_id="d", action="prepare", signing_profile=_presigned_profile()
    )
    s = req.signing
    assert s.mode == pb.IosSigningMode.IOS_SIGNING_MODE_PRESIGNED
    # No credentials travel for the presigned path; the node skips signing.
    assert s.asc_private_key == b""
    assert s.certificate_p12 == b""
    assert s.provisioning_profile == b""


def test_unknown_signing_kind_raises() -> None:
    with pytest.raises(ValueError, match="unknown signing kind"):
        _build_wda_job_request(
            job_id="j", udid="u", device_id="d", action="prepare",
            signing_profile={"kind": "bogus", "secret_data": {}},
        )


def test_asc_missing_p8_raises() -> None:
    with pytest.raises(ValueError, match="p8_key"):
        _build_wda_job_request(
            job_id="j", udid="u", device_id="d", action="prepare",
            signing_profile={"kind": "asc", "secret_data": {"key_id": "k", "issuer_id": "i", "team_id": "t"}},
        )


# ── artifact + bundle id placement ────────────────────────────────────────────


def test_artifact_url_and_bundle_id_land_on_artifact() -> None:
    req = _build_wda_job_request(
        job_id="j", udid="u", device_id="d", action="prepare",
        artifact=_artifact(), signing_profile=_asc_profile(),
        wda_bundle_id="com.facebook.WebDriverAgentRunner.xctrunner",
        xctest_config_name="WebDriverAgentRunner.xctest",
    )
    # bundle id / xctest config name are fields on the ARTIFACT, not the job
    # request (the old code put them on the request, which has no such fields).
    assert req.artifact.url == "https://github.com/o/r/d/device-control-1-ios.ipa"
    assert req.artifact.sha256 == "a" * 64
    assert req.artifact.version == "1.2.3"
    assert req.artifact.size_bytes == 1024
    assert req.artifact.target_bundle_id == "com.facebook.WebDriverAgentRunner.xctrunner"
    assert req.artifact.xctest_config_name == "WebDriverAgentRunner.xctest"


def test_bundle_id_keeps_artifact_non_nil_without_market_dict() -> None:
    """Even with no artifact dict, setting target_bundle_id makes art != nil so
    the node doesn't short-circuit on "artifact_missing" before a clear error.
    The node then fails with a precise "artifact url is empty" instead."""
    req = _build_wda_job_request(
        job_id="j", udid="u", device_id="d", action="renew",
        wda_bundle_id="com.facebook.WebDriverAgentRunner.xctrunner",
    )
    assert req.artifact.target_bundle_id == "com.facebook.WebDriverAgentRunner.xctrunner"
    assert req.artifact.url == ""  # no market dict → fetch will fail cleanly


def test_presigned_job_with_artifact_is_install_signed_compatible() -> None:
    """The Go PrepareSigning short-circuits on EITHER action=INSTALL_SIGNED OR
    mode=PRESIGNED, so presigned-via-prepare (mode=PRESIGNED) is the supported
    free-Apple-ID path under the market redesign."""
    req = _build_wda_job_request(
        job_id="j", udid="u", device_id="d", action="prepare",
        artifact=_artifact(), signing_profile=_presigned_profile(),
        wda_bundle_id="com.facebook.WebDriverAgentRunner.xctrunner",
    )
    assert req.action == pb.IosWdaJobAction.IOS_WDA_JOB_ACTION_PREPARE
    assert req.signing.mode == pb.IosSigningMode.IOS_SIGNING_MODE_PRESIGNED
    assert req.artifact.url  # market artifact is downloaded as-is, not re-signed


# ── _fill_wda_signing mutates in place ────────────────────────────────────────


def test_fill_wda_signing_is_in_place() -> None:
    material = pb.NodeIosSigningMaterial()
    _fill_wda_signing(material, _asc_profile())
    assert material.mode == pb.IosSigningMode.IOS_SIGNING_MODE_APP_STORE_CONNECT
    assert material.asc_key_id == "K1234ABCD"
