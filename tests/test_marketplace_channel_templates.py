from channel_template_service import apply_channel_template
from user_platform.marketplace import config as marketplace_config
from user_platform.marketplace.channel_template_export import build_manifest, resolve_requested_providers
from user_platform.marketplace.validator import validate_manifest


def _channel_manifest() -> dict:
    return {
        "schema": "ai-lubricant.channel-template/v1",
        "id": "openai-compatible",
        "name": "openai-compatible",
        "display_name": "OpenAI 兼容",
        "version": "1.0.0",
        "summary": "标准 OpenAI 兼容渠道",
        "kind": "channel_template",
        "resource": {
            "type": "channel_template",
            "channel": {
                "name": "OpenAI 兼容",
                "base_url": "https://api.openai.com/v1",
                "billing_mode": "token",
                "chat_protocols": [
                    {"enabled": True, "protocol": "openai", "path": "/v1/chat/completions"}
                ],
            },
            "freeze_policy": {"enabled": True, "rules": []},
        },
    }


def test_channels_is_default_marketplace_module():
    assert "channels" in marketplace_config._DEFAULT_MODULES


def test_node_versions_is_default_marketplace_module():
    assert "node-versions" in marketplace_config._DEFAULT_MODULES


def test_channel_template_manifest_is_accepted():
    assert validate_manifest("channels", _channel_manifest()) == []


def test_channel_template_manifest_accepts_legacy_schema():
    """2026-08 品牌改名前发布的市场文件仍写 model-api.*，校验必须双认。"""
    manifest = _channel_manifest()
    manifest["schema"] = "model-api.channel-template/v1"
    assert validate_manifest("channels", manifest) == []


def test_mcp_manifest_still_validates_after_marketplace_extensions():
    manifest = {
        "id": "example.remote-mcp",
        "name": "remote-mcp",
        "display_name": "Remote MCP",
        "summary": "远程 MCP 服务",
        "version": "1.0.0",
        "kind": "mcp",
        "resource": {"type": "remote_mcp", "url": "https://mcp.example.com/sse", "transport": "sse"},
    }
    assert validate_manifest("mcp", manifest) == []


def test_channel_template_rejects_secret_and_local_header_id():
    manifest = _channel_manifest()
    manifest["resource"]["channel"]["api_key"] = "secret"
    assert any("禁止携带" in error for error in validate_manifest("channels", manifest))

    manifest = _channel_manifest()
    manifest["resource"]["channel"]["chat_protocols"][0]["header_template"] = "local-id"
    assert any("禁止携带" in error for error in validate_manifest("channels", manifest))


def test_channel_template_rejects_unknown_icon_key():
    manifest = _channel_manifest()
    manifest["icon"] = "https://example.com/icon.svg"
    assert validate_manifest("channels", manifest) == []

    manifest["icon"] = "http://example.com/icon.svg"
    assert any("icon 只能是" in error for error in validate_manifest("channels", manifest))

    manifest["icon"] = "data:image/png;base64,iVBOR"
    assert any("icon 只能是" in error for error in validate_manifest("channels", manifest))

    manifest["icon"] = "brain"
    assert validate_manifest("channels", manifest) == []


def test_build_manifest_strips_data_url_icon():
    from user_platform.marketplace.channel_template_export import build_manifest
    cfg = {
        "remark": "测试渠道",
        "base_url": "https://api.example.com/v1",
        "billing_mode": "token",
        "website_url": "https://example.com/console",
        "icon": "data:image/png;base64,iVBOR",
        "chat_protocols": [{"protocol": "openai", "path": "/v1/chat/completions"}],
    }
    manifest = build_manifest("test", cfg, {"freeze_policy": {"enabled": True, "rules": []}})
    assert manifest["icon"] == ""
    assert manifest["resource"]["channel"]["website_url"] == "https://example.com/console"
    assert validate_manifest("channels", manifest) == []


def test_build_manifest_uses_configured_summary_or_blank():
    base = {
        "remark": "测试渠道",
        "base_url": "https://api.example.com/v1",
        "billing_mode": "token",
        "chat_protocols": [{"protocol": "openai", "path": "/v1/chat/completions"}],
    }
    manifest = build_manifest("test", {**base, "description": "  聚合网关渠道  "}, {})
    assert manifest["summary"] == "聚合网关渠道"

    manifest = build_manifest("test", {**base, "summary": "  兼容 summary 字段  "}, {})
    assert manifest["summary"] == "兼容 summary 字段"

    manifest = build_manifest("test", base, {})
    assert manifest["summary"] == ""
    assert validate_manifest("channels", manifest) == []


def test_channel_summary_is_optional_but_mcp_summary_remains_required():
    channel = _channel_manifest()
    channel.pop("summary")
    assert validate_manifest("channels", channel) == []

    mcp = {
        "id": "example.remote-mcp",
        "name": "remote-mcp",
        "display_name": "Remote MCP",
        "version": "1.0.0",
        "kind": "mcp",
        "resource": {"type": "remote_mcp", "url": "https://mcp.example.com/sse", "transport": "sse"},
    }
    assert any("summary 必填" in error for error in validate_manifest("mcp", mcp))


def test_channel_template_validates_website_url():
    manifest = _channel_manifest()
    manifest["resource"]["channel"]["website_url"] = "https://example.com/console"
    assert validate_manifest("channels", manifest) == []
    manifest["resource"]["channel"]["website_url"] = "http://example.com"
    assert any("website_url 必须是 HTTPS" in error for error in validate_manifest("channels", manifest))



def test_channel_template_requires_enabled_protocol_and_freeze_rules():
    manifest = _channel_manifest()
    manifest["resource"]["channel"]["chat_protocols"][0]["enabled"] = False
    assert any("至少启用一条" in error for error in validate_manifest("channels", manifest))

    manifest = _channel_manifest()
    manifest["resource"]["freeze_policy"].pop("rules")
    assert any("freeze_policy.rules" in error for error in validate_manifest("channels", manifest))


def test_channel_template_requires_base_url_and_protocol_paths():
    """模板必须带 base_url 与每条协议行 path：缺了就退化成只有名字的假模板。"""
    manifest = _channel_manifest()
    manifest["resource"]["channel"]["base_url"] = ""
    errors = validate_manifest("channels", manifest)
    assert any("base_url 必填" in error for error in errors)

    manifest = _channel_manifest()
    manifest["resource"]["channel"]["base_url"] = "http://insecure.example.com"
    assert any("base_url 必须是 HTTPS" in error for error in validate_manifest("channels", manifest))

    manifest = _channel_manifest()
    manifest["resource"]["channel"]["chat_protocols"][0]["path"] = ""
    assert any("path 必填" in error for error in validate_manifest("channels", manifest))


def test_apply_channel_template_only_updates_base_configuration():
    existing = {
        "base_url": "https://old.example.com",
        "remark": "现有渠道名称",
        "local_only_setting": "keep-me",
        "accounts": [{"username": "must-never-be-read"}],
        "channel_config_revision": 4,
    }
    applied, freeze_policy = apply_channel_template(existing, _channel_manifest())
    assert applied["base_url"] == "https://api.openai.com/v1"
    assert applied["local_only_setting"] == "keep-me"
    # 服务本身不读取/转换账号；真正持久化则走 base-config 窄写，不触 account 表。
    assert applied["accounts"] == existing["accounts"]
    assert applied["channel_template_id"] == "openai-compatible"
    assert applied["channel_config_revision"] == 5
    assert freeze_policy == {"enabled": True, "rules": []}


def test_apply_channel_template_mirrors_icon_and_website_url():
    manifest = _channel_manifest()
    manifest["icon"] = "brain"
    manifest["resource"]["channel"]["website_url"] = "https://example.com/console"
    applied, _ = apply_channel_template({}, manifest)
    assert applied["icon"] == "brain"
    assert applied["website_url"] == "https://example.com/console"

    # 顶层 icon 缺省时不应清空 provider 已有 icon。
    manifest2 = _channel_manifest()
    manifest2["resource"]["channel"].pop("website_url", None)
    applied2, _ = apply_channel_template({"icon": "bot"}, manifest2)
    assert applied2["icon"] == "bot"
    assert "website_url" not in applied2


def test_apply_channel_template_rejects_secret_before_merge():
    manifest = _channel_manifest()
    manifest["resource"]["channel"]["token"] = "secret"
    try:
        apply_channel_template({}, manifest)
    except ValueError as exc:
        assert "禁止携带" in str(exc)
    else:
        raise AssertionError("secret-bearing template must be rejected")


def test_resolve_requested_providers_exports_all_when_unspecified():
    """「从当前平台导入」缺省=导出全部（脚本与旧调用方兼容）。"""
    available = {"openai", "anthropic", "gemini"}
    selected, unknown = resolve_requested_providers(None, available)
    assert selected == ["anthropic", "gemini", "openai"]
    assert unknown == []
    selected, unknown = resolve_requested_providers([], available)
    assert selected == ["anthropic", "gemini", "openai"]
    assert unknown == []
    # 非列表入参也走「全部」分支，避免脏请求体误清空。
    selected, unknown = resolve_requested_providers("openai", available)
    assert selected == ["anthropic", "gemini", "openai"]


def test_resolve_requested_providers_filters_to_selection_and_flags_unknown():
    """勾选时只导出所选，并把不存在的渠道挑出来交给路由报 400。"""
    available = {"openai", "anthropic", "gemini"}
    selected, unknown = resolve_requested_providers(["anthropic", "openai"], available)
    assert selected == ["anthropic", "openai"]
    assert unknown == []

    # 去重 + 保留未知项顺序，交路由报「渠道不存在」。
    selected, unknown = resolve_requested_providers(["openai", "openai", "nope"], available)
    assert selected == ["openai"]
    assert unknown == ["nope"]

    # 空白条目被忽略。
    selected, unknown = resolve_requested_providers(["  ", "anthropic", ""], available)
    assert selected == ["anthropic"]
    assert unknown == []


def test_channel_import_job_tracks_explicit_progress_and_single_active_job():
    from user_platform.marketplace import channel_import_jobs as jobs

    # 隔离模块级内存表，避免测试间或开发进程遗留状态。
    jobs._jobs.clear()
    jobs._tasks.clear()
    jobs._active_job_id = None

    job_id, conflict = jobs.create_job(["openai", "anthropic"], overwrite=False)
    assert job_id and conflict is None
    second, active = jobs.create_job(["gemini"], overwrite=True)
    assert second is None and active == job_id

    jobs.set_phase(job_id, "importing", current_provider="openai")
    jobs.set_item(job_id, "openai", "written", template_id="local.openai")
    jobs.set_item(job_id, "anthropic", "skipped", template_id="local.anthropic")
    state = jobs.public_state(jobs.get_job(job_id))
    assert state["completed"] == 2
    assert state["written"] == ["local.openai"]
    assert state["skipped"] == ["anthropic"]

    jobs.mark_done(job_id)
    next_id, conflict = jobs.create_job(["gemini"], overwrite=True)
    assert next_id and conflict is None

    jobs._jobs.clear()
    jobs._tasks.clear()
    jobs._active_job_id = None


def test_channel_import_job_records_per_provider_failure():
    from user_platform.marketplace import channel_import_jobs as jobs

    jobs._jobs.clear()
    jobs._tasks.clear()
    jobs._active_job_id = None
    job_id, _ = jobs.create_job(["broken"], overwrite=False)
    jobs.set_item(job_id, "broken", "failed", error="base_url 必填")
    state = jobs.public_state(jobs.get_job(job_id))
    assert state["completed"] == 1
    assert state["failed"] == [{"provider": "broken", "errors": ["base_url 必填"]}]
    jobs.mark_failed(job_id, "没有可写入的模板")
    assert jobs.get_job(job_id)["status"] == "failed"

    jobs._jobs.clear()
    jobs._tasks.clear()
    jobs._active_job_id = None


def _patch_channel_import_env(monkeypatch):
    """搭一个「store 可写、GitHub/raw 全程不可达也不该被碰」的导入环境。

    新架构下导入 job 只写 PG 编辑真相源；GitHub 镜像由 publisher 异步消费 outbox，
    不在导入链路上。任何对 GitHub client 或 raw refresh 的触碰都会在这里炸出来。
    返回 (已写 store 条目, 已入队 job)。
    """
    import asyncio

    import db
    import limit_policy_store
    from user_platform.marketplace import channel_catalog
    from user_platform.marketplace import channel_import_jobs as jobs
    from user_platform.marketplace import routes

    jobs._jobs.clear()
    jobs._tasks.clear()
    jobs._active_job_id = None

    cfg = {
        "remark": "Agnes",
        "base_url": "https://api.agnes.example.com/v1",
        "billing_mode": "token",
        "chat_protocols": [{"protocol": "openai", "path": "/v1/chat/completions"}],
    }

    class ForbiddenClient:
        def __getattr__(self, name):
            raise AssertionError(f"import job must not touch GitHub client: {name}")

    monkeypatch.setattr(routes, "_client", lambda: ForbiddenClient())

    async def fake_base_config(_cls, _name):
        return dict(cfg)

    async def fake_policy(_name, refresh=True):
        return {"freeze_policy": {"enabled": True, "rules": []}}

    async def must_not_refresh(*_args, **_kwargs):
        raise AssertionError("raw refresh must not run during import")

    async def must_not_apply(*_args, **_kwargs):
        raise AssertionError("apply_authoritative_manifests is obsolete in store mode")

    # 内存版 marketplace_store：记录 upsert_item 与 enqueue 行为，覆盖导入路径所需。
    import marketplace_store as store
    stored: dict = {"items": {}, "jobs": [], "populated": True}

    async def fake_upsert_item(module, manifest):
        stored["items"][(module, str(manifest.get("id")))] = manifest
        stored["jobs"].append((module, str(manifest.get("id")), "upsert"))
        store.notify_publisher()
        return {"module": module, "item_id": str(manifest.get("id")), "manifest": manifest, "summary": {}}

    async def fake_list_summaries(module):
        return []

    async def fake_is_populated():
        return stored["populated"]

    monkeypatch.setattr(store, "upsert_item", fake_upsert_item)
    monkeypatch.setattr(store, "list_summaries", fake_list_summaries)
    monkeypatch.setattr(store, "is_populated", fake_is_populated)
    monkeypatch.setattr(db.PostgresClient, "get_provider_base_config", classmethod(fake_base_config))
    monkeypatch.setattr(limit_policy_store, "get_effective_provider_policy", fake_policy)
    monkeypatch.setattr(channel_catalog, "apply_authoritative_manifests", must_not_apply)
    monkeypatch.setattr(channel_catalog, "refresh", must_not_refresh)
    return stored


def test_channel_import_job_writes_store_without_github_or_raw_refresh(monkeypatch):
    """导入 job 只写 PG 真相源；GitHub 镜像归 publisher，raw 全程不可达也不受影响。"""
    import asyncio

    from user_platform.marketplace import channel_import_jobs as jobs
    from user_platform.marketplace import routes

    stored = _patch_channel_import_env(monkeypatch)
    job_id, conflict = jobs.create_job(["agnes"], overwrite=True)
    assert job_id and conflict is None
    asyncio.run(routes._run_channel_import_job(job_id, ["agnes"], True))

    job = jobs.get_job(job_id)
    assert job["status"] == "done"
    assert job["warning"] == ""
    assert job["written"] == ["local.agnes"]
    assert ("channels", "local.agnes") in stored["items"]
    assert ("channels", "local.agnes", "upsert") in stored["jobs"]

    jobs._jobs.clear()
    jobs._tasks.clear()
    jobs._active_job_id = None


def test_channel_import_job_surfaces_store_failure_as_item_failure(monkeypatch):
    """store 写入失败要落成该渠道的 failed 项，不能把整任务炸成 failed。"""
    import asyncio

    import marketplace_store as store
    from user_platform.marketplace import channel_import_jobs as jobs
    from user_platform.marketplace import routes

    _patch_channel_import_env(monkeypatch)

    async def failing_upsert(module, manifest):
        raise RuntimeError("pg down")

    monkeypatch.setattr(store, "upsert_item", failing_upsert)
    job_id, conflict = jobs.create_job(["agnes"], overwrite=True)
    assert job_id and conflict is None
    asyncio.run(routes._run_channel_import_job(job_id, ["agnes"], True))

    job = jobs.get_job(job_id)
    assert job["status"] == "done"
    assert job["failed"] and job["failed"][0]["provider"] == "agnes"
    assert "pg down" in job["failed"][0]["errors"][0]

    jobs._jobs.clear()
    jobs._tasks.clear()
    jobs._active_job_id = None


def test_explain_git_write_error_classifies_token_and_404_cases():
    """token 缺写权限 / 404 / 其余 三类错误各自给出可执行提示，且不含 token。"""
    from user_platform.marketplace.routes import _explain_git_write_error, _explain_write_failures

    tokenless = (
        "PUT https://api.github.com/repos/o/r/contents/modules/channels/index.json "
        'returned HTTP 403: {"message":"Resource not accessible by personal access token",'
        '"documentation_url":"https://docs.github.com/rest/repos/contents",'
        '"status":"403"}'
    )
    assert "Contents: Read and write" in _explain_git_write_error(tokenless)

    not_found = "GET modules/missing.json returned HTTP 404"
    assert "404" in _explain_git_write_error(not_found)

    other = "PUT ... returned HTTP 500: server error"
    assert _explain_git_write_error(other) == other

    # _explain_write_failures 汇总多条 per-item 错误为一条人话，并保留 token 权限判定。
    msg = _explain_write_failures([
        {"provider": "openai", "errors": [tokenless]},
        {"provider": "anthropic", "errors": ["some other failure"]},
    ])
    assert "没有写入任何渠道模板" in msg
    assert "Contents: Read and write" in msg
    # 错误信息里绝不含 token（即便万一上游回显也不泄密）。
    assert "github_pat_" not in msg and "ghp_" not in msg

    # 空失败列表也要稳。
    assert "未知原因" in _explain_write_failures([])


def test_export_current_channel_never_packages_accounts_or_url_credentials():
    cfg = {
        "remark": "测试渠道",
        "base_url": "https://user:pass@api.example.com/v1?token=secret",
        "billing_mode": "token",
        "accounts": [{"username": "u", "api_key": "secret"}],
        "chat_protocols": [{"protocol": "openai", "path": "/v1/chat/completions", "header_template": "local"}],
    }
    manifest = build_manifest("test", cfg, {"freeze_policy": {"enabled": True, "rules": []}})
    assert manifest["resource"]["channel"]["base_url"] == "https://api.example.com/v1"
    assert "accounts" not in str(manifest).lower()
    assert "api_key" not in str(manifest).lower()
    assert "header_template" not in str(manifest).lower()
    assert validate_manifest("channels", manifest) == []


def _node_version_manifest() -> dict:
    return {
        "schema": "ai-lubricant.node-version/v1",
        "id": "node-suite-1.2.3",
        "kind": "node_program_version",
        "name": "node-suite",
        "display_name": "节点程序套件 1.2.3",
        "version": "1.2.3",
        "version_notes": "首个多平台发行",
        "status": "published",
        "test_version": False,
        "assets": [
            {
                "filename": "node-execution-linux-amd64",
                "component": "node", "role": "execution", "platform": "linux", "arch": "amd64", "format": "executable",
                "download_url": "https://github.com/example/releases/download/node-suite-1.2.3/node-execution-linux-amd64",
                "digest": "sha256:" + "a" * 64, "size_bytes": 1024, "asset_id": 1,
            },
            {
                "filename": "agent-compose-runtime-linux-amd64.tar.gz",
                "component": "agent-compose", "role": "runtime", "platform": "linux", "arch": "amd64", "format": "tar.gz",
                "download_url": "https://github.com/example/releases/download/node-suite-1.2.3/agent-compose-runtime-linux-amd64.tar.gz",
                "digest": "sha256:" + "b" * 64, "size_bytes": 2048, "asset_id": 2,
            },
        ],
    }


def test_node_version_manifest_is_accepted():
    assert validate_manifest("node-versions", _node_version_manifest()) == []


def test_node_version_rejects_editor_cli_and_channel_fields():
    from user_platform.marketplace.validator import _NODE_COMPONENTS
    assert "editor-cli" not in _NODE_COMPONENTS


def test_node_version_requires_status_and_test_flag():
    manifest = _node_version_manifest()
    manifest["status"] = "beta"
    assert any("status" in error for error in validate_manifest("node-versions", manifest))

    manifest = _node_version_manifest()
    manifest["test_version"] = "yes"
    assert any("test_version" in error for error in validate_manifest("node-versions", manifest))


def test_node_version_rejects_unrecognized_asset_filename():
    manifest = _node_version_manifest()
    manifest["assets"][0]["filename"] = "random-binary.bin"
    assert any("命名规范" in error for error in validate_manifest("node-versions", manifest))


def test_node_version_rejects_windows_asset_without_exe():
    manifest = _node_version_manifest()
    manifest["assets"][0].update({
        "filename": "node-execution-windows-amd64", "platform": "windows",
    })
    assert any("命名规范" in error for error in validate_manifest("node-versions", manifest))


def test_node_version_rejects_duplicate_assets():
    manifest = _node_version_manifest()
    manifest["assets"].append(dict(manifest["assets"][0]))
    assert any("重复" in error for error in validate_manifest("node-versions", manifest))


def test_node_version_rejects_download_url_with_token_query():
    manifest = _node_version_manifest()
    manifest["assets"][0]["download_url"] += "?token=secret"
    assert any("token" in error for error in validate_manifest("node-versions", manifest))


def test_node_version_rejects_non_https_download_url():
    manifest = _node_version_manifest()
    manifest["assets"][0]["download_url"] = "http://insecure.example.com/node-execution-linux-amd64"
    assert any("HTTPS" in error for error in validate_manifest("node-versions", manifest))


def test_node_version_rejects_secret_fields():
    manifest = _node_version_manifest()
    manifest["token"] = "secret"
    assert any("禁止携带" in error for error in validate_manifest("node-versions", manifest))


def test_node_release_version_json_is_accepted_and_rejects_secrets():
    from user_platform.marketplace.validator import validate_node_release

    manifest = {
        "schema": "ai-lubricant.node-release/v1",
        "version": "1.2.3",
        "version_notes": "统一节点发行版本",
        "release_tag": "node-suite-1.2.3",
        "assets": _node_version_manifest()["assets"],
    }
    assert validate_node_release(manifest) == []

    manifest["token"] = "secret"
    assert any("禁止携带" in error for error in validate_node_release(manifest))


def test_node_release_accepts_legacy_schema():
    from user_platform.marketplace.validator import validate_node_release

    manifest = {
        "schema": "model-api.node-release/v1",
        "version": "1.2.3",
        "version_notes": "统一节点发行版本",
        "release_tag": "node-suite-1.2.3",
        "assets": _node_version_manifest()["assets"],
    }
    assert validate_node_release(manifest) == []


def test_node_version_accepts_date_suffix_format_and_rejects_garbage():
    """打包脚本默认用 YYYYMMDD-HHMM 版本号，validator 必须接受；同时兼容历史 semver。"""
    from user_platform.marketplace.validator import _is_node_version

    # 日期后缀格式（打包脚本默认产物）
    assert _is_node_version("20260813-1430")
    assert _is_node_version("20260813-1430-2")  # 同分钟多次打的序号
    # 历史 semver 仍接受
    assert _is_node_version("1.2.3")
    assert _is_node_version("0.6.0")
    # 非法格式
    assert not _is_node_version("20260813")          # 缺后缀
    assert not _is_node_version("2026-08-13")        # 不是 YYYYMMDD
    assert not _is_node_version("latest")
    assert not _is_node_version("")


def test_node_release_accepts_date_suffix_version():
    """version.json 用日期后缀版本号也要过 validate_node_release。"""
    from user_platform.marketplace.validator import validate_node_release

    manifest = {
        "schema": "ai-lubricant.node-release/v1",
        "version": "20260813-1430",
        "version_notes": "节点发行版 20260813-1430",
        "release_tag": "node-suite-20260813-1430",
        "assets": _node_version_manifest()["assets"],
    }
    assert validate_node_release(manifest) == []



def test_select_upgrade_assets_returns_runtime_and_role_specific_node_asset():
    from node_release_catalog import select_upgrade_assets

    assets = _node_version_manifest()["assets"]
    assets.append({
        "filename": "agent-compose-node-management-linux-amd64",
        "component": "node", "role": "management", "platform": "linux", "arch": "amd64", "format": "executable",
        "download_url": "https://github.com/example/releases/download/node-suite-1.2.3/agent-compose-node-management-linux-amd64",
        "digest": "sha256:" + "c" * 64, "size_bytes": 1024, "asset_id": 3,
    })
    latest = {"version": "1.2.3", "version_notes": "notes", "release_tag": "node-suite-1.2.3", "assets": assets}

    selected = select_upgrade_assets(latest, node_role="execution", os_name="linux", arch="amd64")
    assert selected["version"] == "1.2.3"
    assert selected["runtime"]["role"] == "runtime"
    assert selected["node"]["role"] == "execution"

    selected = select_upgrade_assets(latest, node_role="management", os_name="linux", arch="amd64")
    assert selected["runtime"]["role"] == "runtime"
    assert selected["node"]["role"] == "management"

    selected = select_upgrade_assets(latest, node_role="execution", os_name="darwin", arch="arm64")
    assert selected["runtime"] is None
    assert selected["node"] is None


def test_select_upgrade_assets_uses_per_asset_version_when_present():
    """version.json 按平台各自取最新合并后，runtime 与 node 可能来自不同版本。"""
    from node_release_catalog import select_upgrade_assets

    assets = _node_version_manifest()["assets"]
    # 节点程序停留在旧版本，runtime 有更新的版本
    assets[0]["version"] = "20260801-1000"
    assets[1]["version"] = "20260813-1430"
    latest = {"version": "20260813-1430", "version_notes": "n", "release_tag": "t", "assets": assets}

    selected = select_upgrade_assets(latest, node_role="execution", os_name="linux", arch="amd64")
    assert selected["node"]["version"] == "20260801-1000"
    assert selected["runtime"]["version"] == "20260813-1430"


def test_coverage_projects_each_platform_with_its_own_version():
    import node_release_catalog

    assets = _node_version_manifest()["assets"]
    assets[0]["version"] = "20260801-1000"
    latest = {"version": "20260813-1430", "assets": assets}

    rows = node_release_catalog.coverage(latest)
    by_role = {(row["role"], row["platform"], row["arch"]): row for row in rows}
    assert by_role[("execution", "linux", "amd64")]["version"] == "20260801-1000"
    # 缺 per-asset version 的旧快照回退到顶层 version
    assert by_role[("runtime", "linux", "amd64")]["version"] == "20260813-1430"
    assert by_role[("execution", "linux", "amd64")]["download_url"].startswith("https://")


def test_node_release_accepts_per_asset_version_and_rejects_garbage():
    from user_platform.marketplace.validator import validate_node_release

    manifest = {
        "schema": "ai-lubricant.node-release/v1",
        "version": "20260813-1430",
        "version_notes": "合并多版本",
        "release_tag": "node-suite-20260813-1430",
        "assets": _node_version_manifest()["assets"],
    }
    manifest["assets"][0]["version"] = "20260801-1000"
    assert validate_node_release(manifest) == []

    manifest["assets"][0]["version"] = "latest"
    assert any("version" in error for error in validate_node_release(manifest))


def test_render_node_release_merges_latest_per_platform():
    """3 个版本各覆盖部分平台时，version.json 要含每个平台各自的最新版本。"""
    from user_platform.marketplace.render import render_node_release

    def asset(filename, role, platform, arch, component="node", fmt="executable", digest="a"):
        return {
            "filename": filename, "component": component, "role": role,
            "platform": platform, "arch": arch, "format": fmt,
            "download_url": f"https://github.com/e/releases/download/x/{filename}",
            "digest": "sha256:" + digest * 64, "size_bytes": 1024,
        }

    manifests = [
        # 旧版本：只有 darwin/arm64 执行节点（后续版本没再发这个平台）
        {
            "status": "published", "test_version": False, "version": "20260801-1000",
            "version_notes": "旧版", "release_tag": "node-suite-20260801-1000",
            "assets": [asset("node-execution-darwin-arm64", "execution", "darwin", "arm64")],
        },
        # 中间版本：linux/amd64 执行节点
        {
            "status": "published", "test_version": False, "version": "20260810-1000",
            "version_notes": "中版", "release_tag": "node-suite-20260810-1000",
            "assets": [asset("node-execution-linux-amd64", "execution", "linux", "amd64", digest="b")],
        },
        # 最新版本：linux/amd64 执行节点 + runtime，覆盖中间版本的 linux/amd64
        {
            "status": "published", "test_version": False, "version": "20260813-1430",
            "version_notes": "新版", "release_tag": "node-suite-20260813-1430",
            "assets": [
                asset("node-execution-linux-amd64", "execution", "linux", "amd64", digest="c"),
                asset("agent-compose-runtime-linux-amd64.tar.gz", "runtime", "linux", "amd64",
                      component="agent-compose", fmt="tar.gz", digest="d"),
            ],
        },
        # 测试版本必须被排除
        {
            "status": "published", "test_version": True, "version": "20260814-0900",
            "version_notes": "测试版", "release_tag": "node-suite-20260814-0900",
            "assets": [asset("node-execution-linux-arm64", "execution", "linux", "arm64", digest="e")],
        },
        # 草稿也必须被排除（管理端可见，但不触发线上升级）
        {
            "status": "draft", "test_version": False, "version": "20260815-0000",
            "version_notes": "草稿", "release_tag": "node-suite-20260815-0000",
            "assets": [asset("node-execution-linux-amd64", "execution", "linux", "amd64", digest="f")],
        },
    ]

    payload, errors = render_node_release(manifests)
    assert errors == []

    by_key = {(a["role"], a["platform"], a["arch"]): a for a in payload["assets"]}
    # linux/amd64 执行节点取最新版本
    assert by_key[("execution", "linux", "amd64")]["version"] == "20260813-1430"
    # darwin/arm64 只在旧版本里有，必须保留（否则该平台永远无法升级）
    assert by_key[("execution", "darwin", "arm64")]["version"] == "20260801-1000"
    # runtime 来自最新版本
    assert by_key[("runtime", "linux", "amd64")]["version"] == "20260813-1430"
    # 测试版本的 linux/arm64 不得进入 version.json
    assert ("execution", "linux", "arm64") not in by_key
    # 顶层 version 仍是全局最新（非测试）
    assert payload["version"] == "20260813-1430"


def test_identify_node_asset_covers_all_kinds():
    from user_platform.marketplace.validator import identify_node_asset
    assert identify_node_asset("node-execution-linux-amd64")["role"] == "execution"
    assert identify_node_asset("agent-compose-node-management-windows-arm64.exe")["role"] == "management"
    assert identify_node_asset("agent-compose-runtime-darwin-arm64.tar.gz")["role"] == "runtime"
    assert identify_node_asset("node-ios-linux-amd64")["role"] == "ios_host"
    assert identify_node_asset("node-ios-windows-amd64.exe")["role"] == "ios_host"
    assert identify_node_asset("node-execution-windows-amd64") == {}  # 缺 .exe
    assert identify_node_asset("whatever.zip") == {}
