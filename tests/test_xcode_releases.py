"""xcode_releases.resolve_from_data 的纯函数解析测试（不联网）。

fixture 模拟 xcodereleases data.json 的条目形态（version.number/build/beta、
links.download.url、requires 最低 macOS），覆盖：

* latest = 最新非 beta 且带可用 .xip 直链的条目（数据按最新在前排布）；
* 精确版本匹配（含 "Xcode " 前缀剥离）；
* beta 只能被显式版本号选中，latest 默认跳过（allow_beta 可放开）；
* 最低 macOS 兼容过滤：latest 跳过不兼容条目，显式版本则报错；
* 没有可用 .xip 直链的条目（如指向需登录页面的链接）一律被跳过；
* 全部不可用时明确报错。
"""
from __future__ import annotations

import pytest

from user_platform.xcode_releases import XcodeReleasesError, resolve_from_data


def _entry(number, *, build="A", beta=False, requires="15.4", url=None):
    return {
        "version": {"number": number, "build": build, "beta": beta},
        "requires": requires,
        "links": {
            "download": {
                "url": url
                or f"https://download.developer.apple.com/Xcode_{number}.xip",
                "size": 12_000_000_000,
            }
        },
    }


_FIXTURE = [
    _entry("16.2", beta=True, requires="15.5"),
    _entry("16.1", requires="15.5"),
    _entry("16.0", requires="15.4"),
    _entry("15.4", requires="14.6", url="https://developer.apple.com/services-account/signin"),
]


def test_latest_picks_newest_stable_with_xip():
    got = resolve_from_data(_FIXTURE, "latest")
    assert got["target_version"] == "16.1"  # 16.2 是 beta，被 latest 跳过
    assert got["download_url"].endswith("Xcode_16.1.xip")
    assert got["download_size_bytes"] == 12_000_000_000
    assert got["requires_macos"] == "15.5"
    assert got["beta"] is False
    assert got["stale"] is False


def test_latest_allow_beta_takes_newest_overall():
    got = resolve_from_data(_FIXTURE, "latest", allow_beta=True)
    assert got["target_version"] == "16.2"
    assert got["beta"] is True


def test_exact_version_match_strips_xcode_prefix():
    got = resolve_from_data(_FIXTURE, "Xcode 16.0")
    assert got["target_version"] == "16.0"


def test_exact_version_can_pick_beta():
    got = resolve_from_data(_FIXTURE, "16.2")
    assert got["target_version"] == "16.2"
    assert got["beta"] is True


def test_exact_version_unknown_errors():
    with pytest.raises(XcodeReleasesError) as exc_info:
        resolve_from_data(_FIXTURE, "14.2")
    assert "14.2" in str(exc_info.value)


def test_latest_skips_incompatible_macos():
    # 节点 macOS 15.4：16.1/16.2（requires 15.5）被跳过，落到 16.0。
    got = resolve_from_data(_FIXTURE, "latest", macos_version="15.4")
    assert got["target_version"] == "16.0"


def test_exact_version_incompatible_macos_errors():
    with pytest.raises(XcodeReleasesError) as exc_info:
        resolve_from_data(_FIXTURE, "16.1", macos_version="15.4")
    assert "15.5" in str(exc_info.value) and "15.4" in str(exc_info.value)


def test_latest_all_incompatible_errors():
    with pytest.raises(XcodeReleasesError):
        resolve_from_data(_FIXTURE, "latest", macos_version="14.2")


def test_entries_without_direct_xip_are_skipped():
    # 15.4 的下载链接指向需登录页面，不算可用；latest 仍能落到 16.0。
    got = resolve_from_data(_FIXTURE, "latest", macos_version="15.4")
    assert got["target_version"] == "16.0"
    # 显式选它则明确报「找不到」。
    with pytest.raises(XcodeReleasesError):
        resolve_from_data(_FIXTURE, "15.4")


def test_no_usable_entries_at_all_errors():
    with pytest.raises(XcodeReleasesError):
        resolve_from_data([_entry("16.0", url="https://example.com/not-xip")], "latest")
    with pytest.raises(XcodeReleasesError):
        resolve_from_data([], "latest")


def test_stale_flag_passthrough():
    got = resolve_from_data(_FIXTURE, "latest", stale=True)
    assert got["stale"] is True
