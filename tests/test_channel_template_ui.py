from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHANNELS = ROOT / "user-frontend/src/pages/manager/platform/Channels.tsx"
SELECTOR = ROOT / "user-frontend/src/pages/manager/platform/channel-catalog/ChannelCatalogSelector.tsx"
ICON = ROOT / "user-frontend/src/pages/manager/platform/channel-catalog/ChannelIcon.tsx"
API = ROOT / "user-frontend/src/@admin-port/api/providers.ts"


def test_channel_page_has_one_unified_add_entry():
    source = CHANNELS.read_text(encoding="utf-8")

    assert "<ChannelCatalogSelector" in source
    assert "添加渠道" in source
    assert "从模板添加" not in source
    assert "QuickAddModal" not in source
    assert "ChannelTemplatePicker" not in source
    assert "addDropdownOpen" not in source


def test_channel_selector_only_reads_local_catalog_api():
    source = SELECTOR.read_text(encoding="utf-8")

    assert "getChannelCatalog" in source
    assert "refreshChannelCatalog" in source
    assert "marketplaceRaw" not in source
    assert "raw.githubusercontent.com" not in source
    assert "模板" not in source


def test_channel_selector_uses_configured_and_default_icons_in_large_dialog():
    selector = SELECTOR.read_text(encoding="utf-8")
    icons = ICON.read_text(encoding="utf-8")

    assert "channelIconKey" in selector
    assert "item.icon" in selector
    assert "channelType(item)" in selector
    assert 'return "自定义"' in selector
    assert 'return "内置"' in selector
    assert 'return "其他"' in selector
    assert "继续填写详情" not in selector
    assert "max-w-6xl" in selector
    assert "h-[90vh]" in selector
    assert "BUILTIN_ICONS" in icons
    assert 'return "server"' in icons



def test_provider_api_exposes_local_catalog_endpoints():
    source = API.read_text(encoding="utf-8")

    assert '"/admin/channel-catalog"' in source
    assert '"/admin/channel-catalog/refresh"' in source
