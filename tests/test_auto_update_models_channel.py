"""回归测试：auto_update_models 一律以渠道配置为准。

历史 bug：内置渠道（直接继承 BaseProvider）忽略渠道配置里的
auto_update_models，永远走类属性 True，导致管理端关闭定时更新后仍周期性拉取上游。
修复后 BaseProvider.auto_update_models 变为读取所注入 Channel 的 property。
"""
from channel import Channel
from providers.edgeone_ai import EdgeOneAIProvider


def _make_builtin_provider() -> EdgeOneAIProvider:
    return EdgeOneAIProvider("u", "p")


def test_builtin_provider_respects_channel_disable():
    """渠道配置 auto_update_models=False -> provider 报告 False。"""
    p = _make_builtin_provider()
    p.attach_channel(Channel("edgeone-ai", {"auto_update_models": False}))
    assert p.auto_update_models is False


def test_builtin_provider_respects_channel_enable():
    p = _make_builtin_provider()
    p.attach_channel(Channel("edgeone-ai", {"auto_update_models": True}))
    assert p.auto_update_models is True


def test_builtin_provider_defaults_true_without_channel():
    """未注入渠道时回退到类默认（内置渠道默认拉取）。"""
    p = _make_builtin_provider()
    p._channel = None
    assert p.auto_update_models is True
