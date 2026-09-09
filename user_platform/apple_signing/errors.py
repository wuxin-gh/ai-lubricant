"""引擎的预期失败（用户可读）。

移植自 iPASide（MIT License, github.com/pwnapplehat/iPASide）ipaside_engine/errors.py，
并为服务端移植新增 :class:`AppleSessionExpired`——路由据此把配置标记为
「需重新登录」并发通知。

区分 EngineError 与普通异常是刻意的：不是 EngineError 的是 bug 而非业务情形，
保留 traceback 便于诊断。
"""

from __future__ import annotations


class EngineError(Exception):
    """一个用户能理解并采取行动的失败。"""


class AnisetteError(EngineError):
    """设备 provisioning 无法建立（库下载失败 / 状态损坏）。"""


class GsaError(EngineError):
    """Apple GrandSlam 认证失败（密码错 / 2FA 码错 / Apple 返回错误码）。"""


class DeveloperServicesError(EngineError):
    """developerservices2 返回非 0 结果码。"""


class SigningError(EngineError):
    """签名材料生成本地失败（密钥解析 / p12 组装）。"""


class AppleSessionExpired(EngineError):
    """缓存的 Apple ID 会话已失效——用户必须重新登录。

    由物化层（routes_ios._materialize_apple_id_profile）在捕获
    GsaError / DeveloperServicesError 时转换抛出；session token 过期是
    免登录续签流程里唯一用户无法自助修复的失败。
    """
