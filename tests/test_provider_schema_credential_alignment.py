"""校验 provider 的 account_schema 声明的凭据字段与 __init__ 实际读取的 kwargs 一致。

背景：schema 只是「告诉前端表单显示哪些输入框」的元数据；前端只会把 schema 声明的
字段名写回 DB 账号行。若 schema 声明字段 A、但 __init__/运行时读的是字段 B，就会出现
「前端改了不生效」甚至「界面根本无法录入真凭据」。历史上有渠道因继承 base 的
password-only schema、却在 __init__ 读 cookies/token 之类的字段而完全无法在界面配置。

这里锁定几个非 password 凭据渠道的 schema 字段集合，防止未来改 __init__ 字段名却漏改
schema 再次错配。
"""
from providers.cloudflare import CloudflareProvider
from providers.edgeone_ai import EdgeOneAIProvider


def _schema_field_keys(provider_cls) -> set[str]:
    return {f["key"] for f in provider_cls.account_schema()["fields"]}


def test_cloudflare_schema_declares_account_id():
    # CloudflareProvider 除 API Token 外还需要 Account ID，schema 必须声明它。
    assert "account_id" in _schema_field_keys(CloudflareProvider)


def test_edgeone_schema_declares_name_and_model_name():
    # EdgeOneAIProvider 用二级域名 name + 对外模型名 model_name 定位上游。
    assert {"name", "model_name"} <= _schema_field_keys(EdgeOneAIProvider)
