from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MARKETPLACE = ROOT / "user-frontend/src/pages/manager/platform/ModelMetadata.tsx"
ROUTING = ROOT / "user-frontend/src/pages/manager/platform/ModelRouting.tsx"


def test_model_marketplace_uses_tabs_without_three_column_overview():
    source = MARKETPLACE.read_text(encoding="utf-8")

    # 三栏总览必须已移除
    assert 'title="真实模型"' not in source
    assert 'title="运行链路"' not in source
    assert 'xl:grid-cols-3' not in source
    assert 'customGroups' not in source

    # tab 切分：模型广场 / 未配置模型 / 自定义模型
    assert "type MarketplaceTab = 'marketplace' | 'unconfigured' | 'custom'" in source
    assert "setTab('marketplace')" in source
    assert "setTab('unconfigured')" in source
    assert "setTab('custom')" in source
    assert "<ModelRoutingContent createSignal={createCustomSignal} />" in source
    assert "tab === 'marketplace' && !model._hasMeta" in source
    assert "tab === 'unconfigured' && model._hasMeta" in source

    # 未配置页不再走状态二次筛选
    assert "未配置页没有状态二次筛选" in source


def test_model_routing_extracts_scheme_editor_and_filters_real_rows():
    source = ROUTING.read_text(encoding="utf-8")

    assert "export function ModelRoutingContent({ createSignal }" in source
    assert "<RouteSchemeEditor" in source
    assert "modelOptions={modelOptions}" in source
    assert "filter((m) => m.type !== 'model_group')" in source


def test_model_global_config_uses_chinese_structured_forms():
    source = MARKETPLACE.read_text(encoding="utf-8")

    assert 'rules JSON' not in source
    assert 'thinking_defaults"><Textarea' not in source
    assert 'reasoning_defaults"><Textarea' not in source
    assert 'Token 估算规则' in source
    assert '模型匹配规则' in source
    assert '按字符估算' in source
    assert '精确分词（tiktoken）' in source
    assert '系统默认行为' in source
    assert '默认推理强度' in source
    assert '保存模型全局配置' in source
