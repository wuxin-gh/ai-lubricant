"""模型提供商模块

CLI 逆向渠道（copilot/codebuddy/atomcode/eaichat/qoder）已下架为「代码渠道」形态：
产品只发框架能力，spec 源码由使用者自行粘贴进管理端「源码」Tab 与分发。
保留的内置渠道仅剩走官方公开 API 的 Cloudflare / EdgeOne。
"""
from providers.base import BaseProvider
from providers.custom import CustomProvider
from providers.edgeone_ai import EdgeOneAIProvider

__all__ = ["BaseProvider", "CustomProvider", "EdgeOneAIProvider"]
