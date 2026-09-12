"""Xcode 安装任务一律直连下载（不经节点代理）。

2026-09-10 实测根因：12GB .xip 过出口代理被截断/改写，`xip -x` 报
「archive is damaged」——而 .xip 最终 302 到 Apple 官方 CDN，直连即可（且
更快）。本文件钉住这个口径：

* 服务端不再把节点绑定的代理（proxy_config_id）折进 Xcode 任务帧——显式
  传了也忽略；
* 任务帧显式带 proxy_mode="direct"，覆盖节点端可能残留的持久化代理快照
  （节点侧 spec.mode 非空即不回退 r.proxy()，见 xcodejob.go 的合并规则）；
* 目录（xcodereleases data.json）也直连拉取。
"""
from __future__ import annotations

import inspect

import pytest

from user_platform import nodes_service


@pytest.mark.asyncio
async def test_start_xcode_install_job_never_uses_proxy(monkeypatch):
    """无论节点绑定什么代理、调用方传没传 proxy_config_id，Xcode 任务帧都直连。"""

    captured: dict = {}

    class _FakeClient:
        async def start_host_tool_job(self, node_id, job_id, **kwargs):
            captured.update(kwargs)
            return {"status": "accepted"}

    async def fake_get_node(self_or_slf, node_id):
        # 节点绑定了一个网络代理——旧逻辑会把它折进任务帧。
        return {
            "node_id": node_id,
            "status": "approved",
            "proxy_config_id": "px-123",
            "capabilities": {"os": "darwin", "arch": "arm64"},
        }

    async def fake_resolve(desired, *, proxy_fields=None, **kwargs):
        # 记录目录拉取用的代理字段；返回一个最小可用 target。
        captured["catalog_proxy_fields"] = proxy_fields
        return {
            "target_version": "16.2",
            "download_url": "https://download.developer.apple.com/Xcode_16.2.xip",
            "download_size_bytes": 12_000_000_000,
            "requires_macos": "15.5",
            "beta": False,
            "stale": False,
        }

    monkeypatch.setattr(nodes_service, "get_local_node_client", lambda: _FakeClient())
    monkeypatch.setattr(nodes_service.NodesService, "get_node_if_exists", fake_get_node)

    import user_platform.xcode_releases as xr

    monkeypatch.setattr(xr, "resolve", fake_resolve)

    svc = nodes_service.NodesService()
    # 显式传一个 proxy_config_id —— 旧口径会 resolve 它并折进帧；现在必须被忽略。
    result = await svc.start_xcode_install_job(
        "n-mac", proxy_config_id="px-123"
    )

    # 任务帧显式 direct：节点侧 spec.mode 非空 → 不回退持久化代理快照。
    assert captured["proxy_mode"] == "direct"
    assert captured.get("proxy_url", "") == ""
    assert captured.get("proxy_url_prefix", "") == ""
    assert captured["tool"] == "xcode"
    assert captured["download_url"].endswith(".xip")
    assert result["job_id"].startswith("htj-")


def test_xcode_job_function_has_no_proxy_resolution_import():
    """源码级回归：start_xcode_install_job 体内不再引用 resolve_proxy。

    之前的实现从 node_upgrade_targets import resolve_proxy 并把节点绑定代理
    折进任务帧。用源码检查钉死：函数体内出现 resolve_proxy 即视为回退。
    """
    src = inspect.getsource(nodes_service.NodesService.start_xcode_install_job)
    assert "resolve_proxy" not in src, (
        "Xcode 安装任务回到了经代理下载的口径——12GB .xip 过代理会损坏，"
        "如确需代理请显式说明并把该断言改掉"
    )
