"""Tests for the cross-platform one-click node installer renderers.

These are pure string-contract tests: they make sure repeated installs converge
on one fixed launcher/task/container and that startup entries never embed the
node secret. The actual node binary owns persisted config + the host lock.
"""
from __future__ import annotations

import re

from user_platform import nodes_service
from user_platform.nodes_service import render_install_bat, render_install_script
from node_server import scripts


def test_ios_host_installer_uses_node_ios_binary():
    bootstrap = {**BOOTSTRAP, "role": "ios_host"}
    assets = {
        ("linux", "amd64"): {
            "version": "20260813-1430",
            "url": "https://github.com/o/r/d/node-ios-linux-amd64",
            "sha256": "a" * 64,
        },
    }
    body = render_install_script(
        bootstrap,
        public_base_url="https://model.example.test",
        agent_image="",
        assets=assets,
        ios_host_assets=assets,
    )
    assert 'binary="$bindir/node-ios"' in body
    assert "https://github.com/o/r/d/node-ios-linux-amd64" in body
    assert 'binary="$bindir/node-execution"' not in body


def test_ios_host_selects_ios_release_asset():
    from server.node_release_catalog import select_upgrade_assets

    latest = {
        "version": "20260813-1430",
        "assets": [
            {"role": "execution", "platform": "linux", "arch": "amd64", "version": "20260812-1200"},
            {"role": "ios_host", "platform": "linux", "arch": "amd64", "version": "20260813-1430"},
        ],
    }
    selected = select_upgrade_assets(latest, node_role="ios_host", os_name="linux", arch="amd64")
    assert selected["node"]["role"] == "ios_host"


BOOTSTRAP = {
    "node_id": "node-11111111-2222-3333-4444-555555555555",
    "secret": "JBSWY3DPEHPK3PXP",
    "server_url": "https://nodes.example.test",
    "role": "execution",
    "startup_method": "standalone",
}


def test_versioned_release_directory_is_used_for_public_installer_binary(tmp_path, monkeypatch):
    release = tmp_path / "20260813-1138"
    release.mkdir()
    binary = release / "node-execution-windows-amd64.exe"
    binary.write_bytes(b"node")
    monkeypatch.setattr(nodes_service, "_default_node_bin_candidates", lambda: [tmp_path])

    assert nodes_service.resolve_node_bin_dir() == release
    assert nodes_service.resolve_node_binary(binary.name) == binary


def test_windows_installer_uses_one_fixed_entry_and_migrates_legacy_tasks():
    body = render_install_bat(BOOTSTRAP, public_base_url="https://model.example.test")
    assert 'set "TASK=agent-compose-node"' in body
    assert 'set "RUNNER=%ROOT%\\start-node.cmd"' in body
    assert "agent-compose-node-node-" in body  # legacy task migration matcher
    assert 'run-node-*.bat' in body
    assert "Agent Compose Node.lnk" in body
    assert "--install --install-only --yes" in body
    assert "schtasks.exe /Create /F /SC ONLOGON" in body
    assert "StartsWith($root" in body  # process cleanup is path-scoped


def test_windows_runner_and_task_do_not_embed_secret():
    body = render_install_bat(BOOTSTRAP, public_base_url="https://model.example.test")
    # The install command necessarily receives the one-time secret. Restrict the
    # assertion to generated runner/task/shortcut statements.
    runner_lines = [
        line
        for line in body.splitlines()
        if line.startswith('>"%RUNNER%"')
        or line.startswith('>>"%RUNNER%"')
        or "schtasks.exe /Create" in line
        or "CreateShortcut" in line
    ]
    rendered_entries = "\n".join(runner_lines)
    assert BOOTSTRAP["secret"] not in rendered_entries
    assert "NODE_SECRET" not in rendered_entries
    assert "--node-id" not in rendered_entries


def test_windows_management_installer_selects_management_binary():
    bootstrap = {**BOOTSTRAP, "role": "management"}
    body = render_install_bat(
        bootstrap,
        public_base_url="https://model.example.test",
        assets={
            ("windows", "amd64"): {
                "version": "20260813-1430",
                "url": "https://github.com/e/d/agent-compose-node-management-windows-amd64.exe",
                "sha256": "a" * 64,
            },
        },
    )
    # The binary is named after the role; the download URL comes from version.json.
    assert "agent-compose-node-management.exe" in body
    assert "agent-compose-node-management-windows-amd64.exe" in body
    assert "node-execution-windows-amd64.exe" not in body


def test_windows_installer_picks_arch_and_errors_without_a_release():
    """安装脚本在下载时自检架构；没有该架构的版本就报错，不回退服务端镜像。"""
    with_amd64 = render_install_bat(
        BOOTSTRAP,
        public_base_url="https://model.example.test",
        assets={
            ("windows", "amd64"): {
                "version": "20260813-1430",
                "url": "https://github.com/e/d/node-execution-windows-amd64.exe",
                "sha256": "f" * 64,
            },
        },
    )
    # 架构自检 + 两个架构各自的 URL 槽位
    assert 'if /I "%PROCESSOR_ARCHITECTURE%"=="AMD64"' in with_amd64
    assert 'if /I "%PROCESSOR_ARCHITECTURE%"=="ARM64"' in with_amd64
    assert 'set "URL_AMD64=https://github.com/e/d/node-execution-windows-amd64.exe"' in with_amd64
    # arm64 没有资产 → 槽位为空，脚本运行时报错
    assert 'set "URL_ARM64="' in with_amd64
    assert "no published node release covers Windows" in with_amd64
    # sha256 校验（与升级链路同口径）
    assert 'set "SHA_AMD64=' + "f" * 64 + '"' in with_amd64
    assert "certutil.exe -hashfile" in with_amd64
    # 不再走服务端二进制镜像
    assert "/api/v1/public/nodes/binaries/" not in with_amd64

    # 空 catalog：脚本仍生成，但两个架构都会报"无对应版本"
    empty = render_install_bat(BOOTSTRAP, public_base_url="https://model.example.test")
    assert 'set "URL_AMD64="' in empty
    assert 'set "URL_ARM64="' in empty
    assert "no published node release covers Windows" in empty


def test_windows_installer_downloads_runtime_before_register():
    """install.bat 在下载节点程序后、注册前下载 agent-compose runtime 到 Go 的 stateDir，
    这样节点首次注册就带 runtime_version，避免「批准需要 runtime / runtime 需要批准后才下发」的死锁。"""
    body = render_install_bat(
        BOOTSTRAP,
        public_base_url="https://model.example.test",
        assets={
            ("windows", "amd64"): {
                "version": "20260813-1430",
                "url": "https://github.com/e/d/node-execution-windows-amd64.exe",
                "sha256": "f" * 64,
            },
        },
        runtime_assets={
            ("windows", "amd64"): {
                "url": "https://gitee.com/e/r/raw/main/node-releases/v1/files/node-runtime.tar.gz",
                "sha256": "r" * 64,
            },
        },
    )
    # runtime 是平台无关的通用包：一个 URL/SHA 槽位覆盖所有 Windows 架构。
    assert 'set "RT_URL=https://gitee.com/e/r/raw/main/node-releases/v1/files/node-runtime.tar.gz"' in body
    assert 'set "RT_SHA=' + "r" * 64 + '"' in body
    assert 'RT_URL_AMD64' not in body
    assert 'RT_URL_ARM64' not in body
    assert "no published agent-compose runtime archive" in body
    # 解压到 Go 的 stateDir (%APPDATA%\agent-compose\node) 下的 runtime/，对上 RuntimeDir()
    assert "set \"RT_DIR=%STATE_DIR%\\runtime\"" in body
    assert "set \"STATE_DIR=%APPDATA%\\agent-compose\\node\"" in body
    assert "tar.exe -xzf" in body
    # 校验 runtime/dist/cli.js 存在（pack-release.sh 产出的 archive 顶层就是 runtime/）
    assert "runtime\\dist\\cli.js" in body
    # tar.exe 必须存在——没有就报明确错误，而不是静默失败
    assert "where tar.exe" in body


def test_unix_standalone_downloads_from_release_and_errors_without_platform():
    body = render_install_script(
        BOOTSTRAP,
        public_base_url="https://model.example.test",
        agent_image="example/node:latest",
        execution_assets={
            ("linux", "amd64"): {
                "version": "20260813-1430",
                "url": "https://github.com/e/d/node-execution-linux-amd64",
                "sha256": "e" * 64,
            },
            ("darwin", "arm64"): {
                "version": "20260801-1000",
                "url": "https://github.com/e/d/node-execution-darwin-arm64",
                "sha256": "9" * 64,
            },
        },
    )
    # 各平台 URL 内嵌 + 运行时自检 os/arch 选一条
    assert "URL_LINUX_AMD64='https://github.com/e/d/node-execution-linux-amd64'" in body
    assert "URL_DARWIN_ARM64='https://github.com/e/d/node-execution-darwin-arm64'" in body
    assert "URL_LINUX_ARM64=''" in body  # 缺该平台
    assert 'case "$os/$arch" in' in body
    assert 'linux/amd64)   url="$URL_LINUX_AMD64"' in body
    assert "当前没有支持 $os/$arch 的节点发行版本" in body
    # sha256 校验
    assert "sha256sum" in body
    # 不再走服务端二进制镜像
    assert "/api/v1/public/nodes/binaries/" not in body


def test_unix_standalone_persists_config_and_has_fixed_launcher():
    body = render_install_script(
        BOOTSTRAP,
        public_base_url="https://model.example.test",
        agent_image="example/node:latest",
    )
    assert "--install --install-only --yes" in body
    # 交互式安装目录提示：默认 ~/.agent-compose，回车=用默认，tty 才提示；
    # AGENT_COMPOSE_NODE_HOME env 绕过提示（自动化）。docker 形态不走这段。
    assert 'default_root="$HOME/.agent-compose"' in body
    assert "安装目录（回车用默认" in body
    assert 'read -r _answer </dev/tty' in body
    assert "${AGENT_COMPOSE_NODE_HOME" in body    # work_root 落盘进持久化 config，重启不漂回 ~/.cache
    assert '--work-root "$work_root"' in body
    assert 'start-node.sh' in body
    assert "agent-compose-node.service" in body
    assert "@reboot" in body
    # launcher 导出 AGENT_COMPOSE_NODE_STATE_DIR（state 目录不走持久化 config，
    # Go 侧读环境变量，所以 launcher 必须导出），占位符 sed 注入实际路径。
    assert 'export AGENT_COMPOSE_NODE_STATE_DIR="__STATE_DIR__"' in body
    assert 's#__STATE_DIR__#$state_dir#g' in body
    # install 步骤必须带同一个 STATE_DIR，否则凭据写到默认目录、launcher 读不到，
    # 节点启动即报「node is not installed on this machine」退出。
    assert 'AGENT_COMPOSE_NODE_STATE_DIR="$state_dir" "$binary" --install --install-only --yes' in body
    # launcher 自愈：先 cd 到安装根再 exec（安装器替换/终端清理掉的 cwd 会让 bash
    # 报 getcwd: cannot access parent directories），并显式打印 state/config 落点。
    assert 'cd "$(dirname "$0")"' in body
    assert 'config=$AGENT_COMPOSE_NODE_STATE_DIR/config.json' in body
    assert 'config missing: $AGENT_COMPOSE_NODE_STATE_DIR/config.json' in body
    assert 'exec "./bin/node-execution"' in body
    # nohup 启动也固定从安装根起跑（后台任务继承的 cwd 可能已消失）。
    assert '( cd "$root" && nohup ./start-node.sh >> ./node.log 2>&1 </dev/null & )' in body
    # 安装目录选择把 bin/state/work 都收到一个根下（默认 ~/.agent-compose）。
    assert 'state_dir="${AGENT_COMPOSE_NODE_STATE_DIR:-$root/state}"' in body
    assert 'work_root="${AGENT_COMPOSE_NODE_WORK_ROOT:-$root/work}"' in body
    # 末尾打印实际落点
    assert "agent-compose node installed to: $root" in body


def test_one_click_docker_uses_fixed_container_name():
    bootstrap = {**BOOTSTRAP, "startup_method": "docker"}
    exec_assets = {
        ("linux", "amd64"): {"version": "v1", "url": "https://github.com/e/d/node-execution-linux-amd64", "sha256": "a" * 64},
        ("linux", "arm64"): {"version": "v1", "url": "https://github.com/e/d/node-execution-linux-arm64", "sha256": "b" * 64},
    }
    mgmt_assets = {
        ("linux", "amd64"): {"version": "v1", "url": "https://github.com/e/d/agent-compose-node-management-linux-amd64", "sha256": "c" * 64},
        ("linux", "arm64"): {"version": "v1", "url": "https://github.com/e/d/agent-compose-node-management-linux-arm64", "sha256": "d" * 64},
    }
    body = render_install_script(
        bootstrap,
        public_base_url="https://model.example.test",
        agent_image="example/node:latest",
        execution_assets=exec_assets,
        management_assets=mgmt_assets,
        runtime_assets={
            ("linux", "amd64"): {
                "version": "20260828-1118",
                "url": "https://github.com/e/d/node-runtime.tar.gz",
                "sha256": "e" * 64,
            },
        },
    )
    # Host-wide uniqueness: role does not appear in the container name.
    assert 'CONTAINER="agent-compose-node"' in body
    assert 'agent-compose-${NODE_ROLE}-node' not in body
    assert "--restart always" in body
    # The one-click docker script BUILDS the image locally from the two role
    # binaries plus the Dockerfile, and reuses it for the child execution nodes.
    assert "docker build" in body
    # Both role binaries come from version.json GitHub direct links (per arch).
    assert "https://github.com/e/d/node-execution-linux-amd64" in body
    assert "https://github.com/e/d/agent-compose-node-management-linux-arm64" in body
    assert "/api/v1/public/nodes/docker/Dockerfile" in body
    assert "/api/v1/public/nodes/docker/entrypoint.sh" in body
    # Management nodes forward the image tag to children so the locally-built image is reused.
    assert "AGENT_COMPOSE_AGENT_IMAGE=" in body
    # Missing-platform error path exists (empty URL → clear error, no mirror fallback).
    assert "当前没有支持 linux/" in body
    # 镜像 bake runtime：docker 形态首次 build 下载通用 runtime 归档并 docker build，
    # Dockerfile 把它解压进镜像 state 目录，装齐编辑器 CLI。镜像存在即跳过。
    assert "RT_URL='https://github.com/e/d/node-runtime.tar.gz'" in body
    assert "RT_SHA='" + "e" * 64 + "'" in body
    assert 'download "$RT_URL" "$build_dir/node-runtime.tar.gz"' in body
    assert 'sha256sum "$build_dir/node-runtime.tar.gz"' in body
    assert "bakes runtime + editor CLIs" in body
    # 第二次（镜像存在）跳过下载与 build。
    assert "already present; skipping build" in body


def test_server_templates_use_fixed_persisted_entries():
    standalone = scripts.render(
        "standalone",
        role="execution",
        server_url="https://nodes.example.test",
        script_url="https://nodes.example.test/install.sh",
    )
    assert "--install --install-only" in standalone
    assert "start-node.sh" in standalone

    systemd = scripts.render(
        "systemd",
        role="execution",
        server_url="https://nodes.example.test",
        script_url="https://nodes.example.test/install.sh",
    )
    assert "--install --install-only --yes" in systemd
    unit = systemd.split("cat > /etc/systemd/system/", 1)[1]
    assert "AGENT_COMPOSE_NODE_SECRET" not in unit
    assert "ExecStart=$binary" in unit

    docker = scripts.render(
        "docker",
        role="execution",
        server_url="https://nodes.example.test",
        script_url="https://nodes.example.test/install.sh",
        agent_image="example/node:latest",
    )
    assert 'CONTAINER_NAME="${AGENT_COMPOSE_NODE_CONTAINER:-agent-compose-node}"' in docker
    assert "AGENT_COMPOSE_NODE_REPLACE" in docker
    assert "docker build" in docker
    assert "/api/v1/public/nodes/binaries/node-execution-linux-" in docker
    assert "/api/v1/public/nodes/binaries/agent-compose-node-management-linux-" in docker
    assert "/api/v1/public/nodes/docker/Dockerfile" in docker
    assert '-e AGENT_COMPOSE_AGENT_IMAGE="$IMAGE"' in docker

    compose = scripts.render(
        "docker-compose",
        role="management",
        server_url="https://nodes.example.test",
        script_url="https://nodes.example.test/install.sh",
        agent_image="example/node:latest",
    )
    assert "container_name: agent-compose-node" in compose
    assert "$HOME/.agent-compose/compose" in compose
    assert "agent-compose-management-node" not in compose
    assert "docker build" in compose
    assert "AGENT_COMPOSE_AGENT_IMAGE=$IMAGE" in compose
    assert "/api/v1/public/nodes/docker/entrypoint.sh" in compose


def test_rendered_windows_has_no_per_node_task_assignment():
    body = render_install_bat(BOOTSTRAP, public_base_url="https://model.example.test")
    assignments = re.findall(r'^set "TASK=(.+)"$', body, flags=re.MULTILINE)
    assert assignments == ["agent-compose-node"]


def test_public_install_routes_pull_release_and_embed_github_urls(monkeypatch):
    """install.sh / install.bat 路由渲染前读 node_release_catalog，把 GitHub 直链烧进脚本。"""
    from fastapi import FastAPI
    from starlette.testclient import TestClient

    import node_release_catalog
    from user_platform import routes_node_bootstrap

    router = routes_node_bootstrap.router

    release = {
        "version": "20260813-1430",
        "assets": [
            {
                "role": "execution", "platform": "linux", "arch": "amd64",
                "version": "20260813-1430",
                "download_url": "https://github.com/o/r/d/node-execution-linux-amd64",
                "digest": "sha256:" + "1" * 64, "size_bytes": 1024,
            },
            {
                "role": "management", "platform": "windows", "arch": "amd64",
                "version": "20260813-1430",
                "download_url": "https://github.com/o/r/d/agent-compose-node-management-windows-amd64.exe",
                "digest": "sha256:" + "2" * 64, "size_bytes": 1024,
            },
        ],
    }
    async def _bootstrap(node_id):
        return {"node_id": node_id, "secret": "JBSWY3DPEHPK3PXP",
                "server_url": "https://nodes.example.test", "role": "execution",
                "startup_method": "standalone", "proxy_config_id": "proxy-test"}

    async def _proxy_fields(_proxy_config_id):
        return {"proxy_mode": "url_prefix", "proxy_url_prefix": "https://mirror.example.test"}

    monkeypatch.setattr(node_release_catalog, "get_latest_release", lambda: _async(release))
    monkeypatch.setattr(routes_node_bootstrap, "get_node_bootstrap", _bootstrap)
    monkeypatch.setattr(routes_node_bootstrap.nodes_service, "resolved_proxy_fields_for", _proxy_fields)

    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)

    sh = client.get("/api/v1/public/nodes/node-x/install.sh")
    assert sh.status_code == 200
    assert "https://github.com/o/r/d/node-execution-linux-amd64" in sh.text
    assert "/api/v1/public/nodes/binaries/" not in sh.text
    # 节点绑定的代理必须烘进脚本：直连 GitHub 在多数网络会无限挂起下载。
    assert "PROXY_MODE='url_prefix'" in sh.text
    assert "PROXY_URL_PREFIX='https://mirror.example.test'" in sh.text
    assert 'url="${PROXY_URL_PREFIX%/}/${url#/}"' in sh.text

    async def _mgmt_bootstrap(node_id):
        return {**await _bootstrap(node_id), "role": "management"}

    monkeypatch.setattr(routes_node_bootstrap, "get_node_bootstrap", _mgmt_bootstrap)
    bat = client.get("/api/v1/public/nodes/node-x/install.bat")
    assert bat.status_code == 200
    assert "https://github.com/o/r/d/agent-compose-node-management-windows-amd64.exe" in bat.text
    assert "/api/v1/public/nodes/binaries/" not in bat.text


def _async(value):
    import asyncio

    async def _coro():
        return value

    return _coro()
