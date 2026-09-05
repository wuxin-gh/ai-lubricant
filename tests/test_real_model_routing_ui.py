"""真实模型渠道策略前端入口：模型广场「真实模型」tab 卡片上的渠道策略弹框。

后端真相源与写入口早已存在（model_groups kind='real' 行的 provider_whitelist /
provider_blacklist / schemes / active_scheme，窄写端点 PUT /admin/model-metadata/
{id}/routing）；这些断言锁住前端把它们接上：API 封装、卡片入口、方案编辑视图，
以及降级语义的口径（仅 is_backup 方案进降级链、顶层两列是激活方案投影）。
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
API = ROOT / "user-frontend/src/@admin-port/api/modelMetadata.ts"
DIALOG = ROOT / "user-frontend/src/pages/manager/platform/RealModelRouting.tsx"
MARKETPLACE = ROOT / "user-frontend/src/pages/manager/platform/ModelMetadata.tsx"
ROUTING = ROOT / "user-frontend/src/pages/manager/platform/ModelRouting.tsx"


def test_api_exposes_real_model_routing_narrow_write():
    source = API.read_text(encoding="utf-8")

    # 窄写端点与后端路由一致（只改渠道策略列，不触碰元数据）
    assert "export async function updateRealModelRouting" in source
    assert "/routing" in source
    assert "request.put<RealModelRoutingResponse>" in source

    # 方案结构与后端 _normalize_schemes 同构
    assert "export interface RealModelScheme" in source
    for field in ("id", "name", "models", "provider_whitelist", "provider_blacklist", "is_backup"):
        assert field in source

    # 列表接口回传的 real 行渠道策略列要在类型里可读，否则弹框拿不到回填数据
    metadata_entry = source[source.index("export interface ModelMetadataEntry"):source.index("export interface RuntimeModelEntry")]
    assert "provider_whitelist?" in metadata_entry
    assert "provider_blacklist?" in metadata_entry
    assert "schemes?" in metadata_entry
    assert "active_scheme?" in metadata_entry


def test_marketplace_card_has_routing_entry_next_to_edit():
    source = MARKETPLACE.read_text(encoding="utf-8")

    # 入口就在「编辑」按钮旁的同一操作区
    actions = source[source.index("const renderActions = useCallback"):source.index("const renderMetadataForm")]
    assert "openMetadataModal('edit', model)" in actions
    assert "openRoutingModal(model)" in actions
    # 按钮文案：「选路」是生造词，且与 API Key 的「选择策略」（selection_strategy，
    # 按算法挑候选）不是同一件事——这里配的是允许哪些渠道承接，故用「渠道策略」。
    assert ">渠道策略</Button>" in actions
    assert ">选路</Button>" not in actions

    # 弹框接上窄写；保存后重新拉取列表
    assert "updateRealModelRouting" in source
    assert "<RealModelRoutingDialog" in source
    assert "const saveRouting = useCallback" in source

    # 目标行按 model_id 从最新数据取，避免拿旧快照覆盖 schemes
    assert "const routingTarget = useMemo<RealModelRoutingTarget | null>" in source
    assert "unified.find((item) => item.model_id === routingModelId)" in source

    # 行上的渠道策略列（顶层两列 + 备用数）
    assert "function routingSummary" in source
    assert "function RoutingTags" in source


def test_routing_dialog_matches_backend_scheme_semantics():
    source = DIALOG.read_text(encoding="utf-8")

    # 三视图：当前方案 / 方案列表 / 单套编辑（编辑走独立缓冲，保存才写回 draft）
    assert "type SchemeView = 'current' | 'manage' | 'edit'" in source
    assert "function CurrentSchemeView" in source
    assert "function ManageSchemesView" in source
    assert "function EditSchemeView" in source

    # 方案身份是 id，不是 name
    assert "function newSchemeId" in source
    assert "scheme.id === draft.activeScheme" in source

    # 旧行（无 schemes）用顶层两列合成默认方案，与后端 DEFAULT_SCHEME_NAME 对齐
    assert "const DEFAULT_SCHEME_NAME = '默认'" in source
    assert "legacy-0" in source
    assert "legacy-${index}" in source

    # 真实模型的候选模型恒为自身：方案 models 补 model_id，否则后端校验拒绝
    assert "models: [target.model_id]" in source
    assert "models: stringList(scheme.models).length ? stringList(scheme.models) : [modelId]" in source

    # 仅显式勾选 is_backup 的非激活方案进降级链
    assert "is_backup" in source
    assert "item.is_backup === true && item.id !== activeId" in source

    # 编辑态未收口时禁止落库（与自定义模型弹框同口径）
    assert "请先「保存」或「退出」当前正在编辑的方案" in source


def test_routing_dialog_does_not_repeat_carrier_info_from_card():
    """卡片上已有「模型在哪些渠道」的标签；弹框里再列命中渠道是重复信息，已砍掉。"""
    source = DIALOG.read_text(encoding="utf-8")

    # 弹框顶部的「承载渠道 N」徽章与卡片 providerTags 重复，已删除
    assert "承载渠道" not in source
    # 方案列表里的「N 个命中渠道」与卡片重复，已删除
    assert "个命中渠道" not in source
    # channelsCarrying 仍保留：tagOptionsFor 靠它列出该模型承载渠道的标签
    assert "function channelsCarrying" in source


def test_shared_field_widgets_exported_from_model_routing():
    source = ROUTING.read_text(encoding="utf-8")

    # 渠道策略弹框复用自定义模型页的字段/多选控件，避免两处渠道标签选择行为漂移
    assert "export function Field(" in source
    assert "export function FieldLabel(" in source
    assert "export function MultiSelect(" in source
    assert "export interface Option" in source
