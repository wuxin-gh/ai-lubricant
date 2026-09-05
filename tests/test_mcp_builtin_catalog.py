"""内置服务「单一声明源」契约测试。

服务名单原先手写两遍：mcp_runtime/registry.py 的 adapter 表 + db.py 的 seed 行。
两处漂移的后果是隐性的——registry 注册了但 DB 没有对应行时，startup 的
configured_only 分支会静默跳过该内置服务（见 mcp_runtime/startup.py），
表现为「服务突然不见了」而不是报错。这里把 catalog 与两个消费方钉在一起。
"""
import os
from pathlib import Path
import sys

_proj = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _proj not in sys.path:
    sys.path.insert(0, _proj)

from mcp_builtin.catalog import (
    BUILTIN_SERVICE_SPECS,
    INTERNAL_BUILTIN_SERVICE_SPECS,
    PERSISTED_BUILTIN_SERVICE_SPECS,
)


def test_catalog_has_no_dependency_on_adapters():
    """catalog 必须是纯声明模块：db.py 导入它做 seed，不能被驱动依赖污染。

    catalog 若在模块顶层 import adapter，则 CDP 的可选浏览器依赖
    (simple_websocket_server / bottle / bs4) 缺失时数据库初始化就会炸。
    """
    import mcp_builtin.catalog as catalog

    assert not hasattr(catalog, "cdp_bridge_plugin")
    source = Path(catalog.__file__).read_text(encoding="utf-8")
    assert "import_module" not in source
    assert "builtin_plugins" not in source.split("BUILTIN_SERVICE_SPECS")[0]


def test_persisted_and_internal_partition_is_total():
    assert set(PERSISTED_BUILTIN_SERVICE_SPECS) | set(INTERNAL_BUILTIN_SERVICE_SPECS) == set(
        BUILTIN_SERVICE_SPECS
    )
    assert not set(PERSISTED_BUILTIN_SERVICE_SPECS) & set(INTERNAL_BUILTIN_SERVICE_SPECS)


def test_registry_adapters_match_catalog():
    """registry 的两张 adapter 表必须与 catalog 逐名对齐（含顺序）。"""
    import mcp_runtime.registry as registry_mod

    assert list(registry_mod._BUILTIN_ADAPTERS) == [
        spec.name for spec in PERSISTED_BUILTIN_SERVICE_SPECS
    ]
    assert list(registry_mod._INTERNAL_BUILTIN_ADAPTERS) == [
        spec.name for spec in INTERNAL_BUILTIN_SERVICE_SPECS
    ]


def test_registry_resolves_declared_adapter_module():
    """每个 spec 的 adapter_module 必须真解析到暴露 register() 的模块。"""
    import mcp_runtime.registry as registry_mod

    for spec in BUILTIN_SERVICE_SPECS:
        adapter = registry_mod._builtin_adapter(spec.name)
        assert adapter is not None, spec.name
        assert adapter.__name__ == spec.adapter_module
        assert callable(getattr(adapter, "register", None)), spec.name


def test_internal_builtins_are_not_persisted():
    """内部会话服务不落 mcp_services，故不需要 seed 元数据。"""
    for spec in INTERNAL_BUILTIN_SERVICE_SPECS:
        assert spec.persisted is False
        import mcp_runtime.registry as registry_mod

        assert registry_mod.MCPRegistry.is_internal_builtin(spec.name) is True


def test_persisted_specs_carry_seed_metadata():
    """落库的内置服务必须带齐 seed 列，否则 DB 行会写出空 display_name。"""
    for spec in PERSISTED_BUILTIN_SERVICE_SPECS:
        assert spec.display_name, spec.name
        assert spec.description, spec.name
        assert spec.category, spec.name
        assert spec.version, spec.name
        assert spec.author, spec.name
