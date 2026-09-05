"""EdgeOne AI provider."""

from providers.custom import CustomProvider


class EdgeOneAIProvider(CustomProvider):
    PROVIDER_NAME = "edgeone-ai"
    SUPPORTS_MULTI_MESSAGES = True

    @classmethod
    def account_schema(cls) -> dict:
        schema = super().account_schema()
        schema.update({
            "provider_name": cls.PROVIDER_NAME,
            "display_name": "EdgeOne AI",
            "add_guidance": "填写 EdgeOne 二级域名和实际模型名；该渠道通常不需要 API Key。",
            "metadata_badges": ["name", "model_name"],
        })
        schema["fields"] = [
            {
                "key": "name",
                "label": "二级域名 name",
                "type": "text",
                "required": True,
                "placeholder": "edgeone.dev 前的二级域名",
                "section": "渠道字段",
            },
            {
                "key": "model_name",
                "label": "模型名 model_name",
                "type": "text",
                "required": True,
                "placeholder": "deepseek-v4-flash",
                "section": "渠道字段",
            },
            {
                "key": "password",
                "label": "密码 / API Key（可空）",
                "type": "password",
                "secret": True,
                "section": "基础信息",
            },
        ]
        return schema

    def __init__(self, username: str, password: str = "", proxy: str = None, **kwargs):
        edgeone_name = (kwargs.get("name") or username or "").strip().strip(".")
        model_name = (kwargs.get("model_name") or "").strip()
        if edgeone_name:
            kwargs["base_url"] = f"https://{edgeone_name}.edgeone.dev"
        super().__init__(username, password or "", proxy, **kwargs)
        self.PROVIDER_NAME = "edgeone-ai"
        self.channel_provider_name = kwargs.get("provider_name") or "edgeone-ai"
        self.edgeone_name = edgeone_name
        self.model_name = model_name
        self.api_key = ""

    @property
    def base_url(self):
        if self.edgeone_name:
            return f"https://{self.edgeone_name}.edgeone.dev"
        return super().base_url

    def is_init(self) -> bool:
        return bool(self.edgeone_name and self.model_name)

    async def init_auth(self, is_check: bool = False) -> bool:
        return self.is_init()

    async def check_auth(self) -> bool:
        return self.is_init()

    async def fetch_upstream_model_list(self) -> list[dict]:
        return []

    async def health_check(self) -> bool:
        return self.is_init()

    async def check_message(self, model_id: str, messages: list[dict] | None = None) -> bool:
        return True

    def supports_system_model(self, model_id: str) -> bool:
        return model_id == self.model_name

    def _headers(self) -> dict:
        return {"Content-Type": "application/json"}

    def _upstream_model_id(self, model_id: str) -> str:
        from rate_limiter import ModelClientPool
        return ModelClientPool.resolve_upstream_id(self.channel_provider_name, self.model_name) or model_id
