"""CLI 逆向内置渠道下架迁移回归。

copilot/codebuddy/atomcode/eaichat/qoder 已从产品里删除为「代码渠道」形态。
启动时 lifespan 的迁移分支把这些旧行的 builtin_type 改写成 "code"、code 留空，
渠道进入代码渠道编辑器形态——用户贴上 spec 后保存即恢复。
"""
import asyncio

import main


def _cfg(builtin_type: str | None, **extra) -> dict:
    cfg = {"enabled": True, "type": "builtin", "accounts": []}
    if builtin_type:
        cfg["builtin_type"] = builtin_type
    cfg.update(extra)
    return cfg


class _FakeStore:
    def __init__(self):
        self.writes: list[tuple[str, dict]] = []

    async def write_provider_async(self, name, cfg):
        self.writes.append((name, dict(cfg)))


def _run_migration(provider_configs: dict) -> tuple[dict, list[tuple[str, dict]]]:
    """执行真实迁移 helper，不连接 PG/Redis。"""
    fake = _FakeStore()
    original = main.config.CONFIG_STORE
    main.config.CONFIG_STORE = fake
    try:
        asyncio.run(main._migrate_deprecated_builtin_channels(provider_configs))
    finally:
        main.config.CONFIG_STORE = original
    return provider_configs, fake.writes


def test_migration_rewrites_deprecated_builtin_by_name():
    """靠渠道名认领的旧行（copilot/atomcode/eaichat，无 builtin_type）被改成 code。"""
    configs = {
        "copilot": _cfg(None, base_url="https://api.githubcopilot.com"),
        "atomcode": _cfg(None),
        "eaichat": _cfg(None, base_url="https://eaichat.ctyun.cn"),
    }
    out, writes = _run_migration(configs)
    for name in ("copilot", "atomcode", "eaichat"):
        assert out[name]["builtin_type"] == "code", name
        assert out[name]["code"] == "", name
    assert {name for name, _ in writes} == {"copilot", "atomcode", "eaichat"}


def test_migration_rewrites_deprecated_builtin_by_type():
    """靠 builtin_type 认领的派生行（如 codebuddy 模板创建的多账号渠道）也被改成 code。"""
    configs = {
        "my-cb-1": _cfg("codebuddy"),
        "my-cb-2": _cfg("codebuddy"),
    }
    out, _ = _run_migration(configs)
    for name in ("my-cb-1", "my-cb-2"):
        assert out[name]["builtin_type"] == "code", name
        assert out[name]["code"] == "", name


def test_migration_preserves_other_fields():
    """迁移不改 base_url/chat_path/rate_limit 等字段——用户贴 spec 后仍能用旧配置。"""
    configs = {"copilot": _cfg(None, base_url="https://x", chat_path="/chat", rate_limit={"rpm_per_account": 30})}
    out, _ = _run_migration(configs)
    assert out["copilot"]["base_url"] == "https://x"
    assert out["copilot"]["chat_path"] == "/chat"
    assert out["copilot"]["rate_limit"] == {"rpm_per_account": 30}


def test_migration_skips_non_deprecated():
    """cloudflare / edgeone-ai / 自定义 code 渠道不被迁移碰。"""
    configs = {
        "cloudflare": _cfg("cloudflare"),
        "edgeone-ai": _cfg("edgeone-ai"),
        "my-code": _cfg("code", code="class X:\n    @staticmethod\n    async def init_auth(p): return True\n"),
        "custom-1": {"type": "custom"},
    }
    out, writes = _run_migration(configs)
    assert out["cloudflare"]["builtin_type"] == "cloudflare"
    assert out["edgeone-ai"]["builtin_type"] == "edgeone-ai"
    assert out["my-code"]["builtin_type"] == "code"
    assert out["my-code"]["code"] != ""  # 已有 spec 不被清空
    assert out["custom-1"] == {"type": "custom"}
    assert writes == []


def test_migration_is_idempotent():
    """迁移过的行 builtin_type 已变 code，再跑一次不再触发。"""
    configs = {"copilot": _cfg(None)}
    out, first_writes = _run_migration(configs)
    assert out["copilot"]["builtin_type"] == "code"
    out2, second_writes = _run_migration(out)
    assert out2["copilot"]["builtin_type"] == "code"
    assert out2["copilot"]["code"] == ""
    assert first_writes and second_writes == []
