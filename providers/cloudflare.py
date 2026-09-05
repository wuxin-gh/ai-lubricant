"""Cloudflare Workers AI provider."""
import json
import time

from loguru import logger

from providers.base import format_aiohttp_error
from providers.custom import CustomProvider


class CloudflareProvider(CustomProvider):
    PROVIDER_NAME = "cloudflare"
    SUPPORTS_MULTI_MESSAGES = True

    @classmethod
    def account_schema(cls) -> dict:
        schema = super().account_schema()
        schema.update({
            "provider_name": cls.PROVIDER_NAME,
            "display_name": "Cloudflare Workers AI",
            "add_guidance": "在 Cloudflare Dashboard 获取 Account ID，并创建包含 Workers AI 权限的 API Token。API Token 填写到 API Key / 密码。",
            "metadata_badges": ["account_id"],
        })
        schema["fields"] = [
            {
                "key": "password",
                "label": "API Key",
                "type": "password",
                "secret": True,
                "placeholder": "Cloudflare API Token",
                "section": "基础信息",
            },
            {
                "key": "account_id",
                "label": "Account ID",
                "type": "text",
                "required": True,
                "placeholder": "Cloudflare Account ID",
                "help_text": "用于拼接 Workers AI API 地址；API Key 仍填写到上面的密码字段。",
                "section": "渠道字段",
            },
        ]
        return schema

    def __init__(self, username: str, password: str, proxy: str = None, **kwargs):
        account_id = (kwargs.get("account_id") or username or "").strip()
        super().__init__(username, password, proxy, **kwargs)
        self.PROVIDER_NAME = "cloudflare"
        self.account_id = account_id

    @property
    def base_url(self):
        """Cloudflare Workers AI 地址由 account_id 拼接；不依赖是否挂载 Channel。

        基类 base_url 属性只在挂了 cloudflare Channel 时才拼 account-scoped URL；
        直接构造（如手动测试/单测）时会回退空串。这里覆写为始终由 account_id 拼。
        """
        if self.account_id:
            return f"https://api.cloudflare.com/client/v4/accounts/{self.account_id}/ai/v1"
        return self._channel_get("base_url", "")

    @property
    def models_path(self):
        if self.account_id:
            return f"https://api.cloudflare.com/client/v4/accounts/{self.account_id}/ai/models/search"
        return self._channel_get("models_path", "/v1/models")

    def _chat_url(self, kwargs: dict | None = None) -> str:
        """account-scoped base_url 已含 /ai/v1，直接接 /chat/completions。

        不走基类（基类会再拼 /v1/chat/completions，导致 /ai/v1/v1/...）。
        挂了 Channel 且配了自定义 chat_path 时优先用它。
        """
        channel = getattr(self, "_channel", None)
        if channel is not None:
            active = channel.active_chat_protocol(kwargs) if hasattr(channel, "active_chat_protocol") else None
            if active and active.get("path"):
                return self._url(active["path"])
            if channel.chat_path:
                return self._url(channel.chat_path)
        if self.account_id:
            return f"{self.base_url}/chat/completions"
        return super()._chat_url(kwargs)

    def is_init(self) -> bool:
        return bool(self.account_id and self.api_key)

    async def fetch_upstream_model_list(self) -> list[dict]:
        if not self.is_init():
            self._last_fetch_models_error = "account_id 或 api_key 未配置"
            return []
        models_url = self._url(self.models_path)
        try:
            async with self._make_session() as session:
                async with session.get(models_url, headers=self._headers(), proxy=self.proxy) as response:
                    text = await response.text()
                    if response.status != 200:
                        self._last_fetch_models_error = f"GET {models_url} → HTTP {response.status}"
                        logger.warning(f"[Cloudflare] fetch models failed: username={self.username} {self._last_fetch_models_error}")
                        return []
                    try:
                        data = json.loads(text)
                    except (json.JSONDecodeError, ValueError) as e:
                        self._last_fetch_models_error = f"GET {models_url} 返回非 JSON: {type(e).__name__}; body_len={len(text)}"
                        logger.warning(f"[Cloudflare] fetch models invalid JSON: username={self.username} {self._last_fetch_models_error}")
                        return []
        except Exception as e:
            self._last_fetch_models_error = format_aiohttp_error(e, models_url)
            logger.warning(f"[Cloudflare] fetch models exception: username={self.username} {self._last_fetch_models_error}")
            return []

        raw_models = self._extract_raw_models(data)
        models = self._normalize_models(raw_models)
        if not models:
            self._last_fetch_models_error = f"GET {models_url} 未返回可识别模型列表"
            logger.warning(f"[Cloudflare] fetch models empty: username={self.username} {self._last_fetch_models_error}")
            return []
        # 只对标记为 require_workers_paid 的模型探测当前账号是否真有权限（付费账号能调则保留，
        # 免费账号被 403 则剔除）。免费模型上游搜索结果本身就不带该标记，直接保留，不探测。
        models = await self._filter_models_by_account_access(models)
        self._last_fetch_models_error = ""
        return models

    async def _filter_models_by_account_access(self, models: list[dict]) -> list[dict]:
        """剔除标记为付费、但当前账号实测无权访问的模型。

        Cloudflare 对 require_workers_paid=true 的模型按账号 Workers 套餐放行：付费账号可调，
        免费账号返回 403 code 5035。这里只探测带该标记的模型（通常很少），用一个 max_tokens=1
        的极小请求确认权限；非 403-5035 的失败（网络/超时/限流）不剔除，保留模型以免误删。
        """
        kept: list[dict] = []
        to_probe: list[dict] = []
        for model in models:
            if model.get("require_workers_paid"):
                to_probe.append(model)
            else:
                kept.append(model)
        if not to_probe:
            return models
        async with self._make_session() as session:
            for model in to_probe:
                upstream_id = model["id"]
                allowed, reason = await self._probe_model_access(session, upstream_id)
                if allowed:
                    kept.append(model)
                else:
                    logger.info(
                        f"[Cloudflare] 账号 {self.username} 无权访问 {upstream_id}，已从模型列表剔除: {reason}"
                    )
        return kept

    async def _probe_model_access(self, session, upstream_model_id: str) -> tuple[bool, str]:
        """用一个 max_tokens=1 的 chat 请求探测当前账号对该模型是否有权限。

        - 2xx：有权限
        - 403 且错误码 5035（Workers Free plan 不可用）：无权限
        - 其它（4xx 非 5035 / 5xx / 网络/超时）：判为"不确定"，保留模型以免误删
        """
        url = f"{self.base_url}/chat/completions"
        body = {
            "model": upstream_model_id,
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 1,
        }
        try:
            async with session.post(url, headers=self._headers(), json=body, proxy=self.proxy) as response:
                if 200 <= response.status < 300:
                    return True, ""
                if response.status == 403:
                    text = await response.text()
                    if '"code":5035' in text or "not available on the Workers Free plan" in text:
                        return False, "Workers Free plan 不可用 (403 / code 5035)"
                    return True, f"403 但非套餐门禁，保留: {text[:200]}"
                return True, f"HTTP {response.status}，非套餐门禁，保留"
        except Exception as e:
            return True, f"探测异常，保留: {e}"


    @staticmethod
    def _extract_raw_models(data) -> list:
        if isinstance(data, list):
            return data
        if not isinstance(data, dict):
            return []
        for key in ("result", "data", "models", "items"):
            value = data.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                nested = CloudflareProvider._extract_raw_models(value)
                if nested:
                    return nested
        return []

    def _normalize_models(self, raw_models: list) -> list[dict]:
        models = []
        now = int(time.time())
        for item in raw_models:
            if isinstance(item, str):
                model_id = item
                model = {"id": model_id, "name": model_id}
            elif isinstance(item, dict):
                model_id = self._model_id_from_item(item)
                if not model_id:
                    continue
                model = dict(item)
                if item.get("id") and item.get("id") != model_id:
                    model["cloudflare_id"] = item.get("id")
                model["id"] = model_id
                model["name"] = model_id
                # 标记 require_workers_paid：这类模型能否用取决于账号 Workers 套餐，
                # 由 fetch_upstream_model_list 的探测决定是否保留，normalize 阶段只打标不剔除。
                if self._is_paid_model(item):
                    model["require_workers_paid"] = True
            else:
                continue
            model.setdefault("owned_by", "cloudflare")
            model.setdefault("created", now)
            model.setdefault("object", "model")
            if self.channel_remark:
                model["channel_remark"] = self.channel_remark
            models.append(model)
        return models


    @staticmethod
    def _is_paid_model(item: dict) -> bool:
        """Cloudflare models/search 的 properties 里带 require_workers_paid=true 即付费专享。"""
        for prop in item.get("properties") or []:
            if not isinstance(prop, dict):
                continue
            if prop.get("property_id") == "require_workers_paid" and str(prop.get("value")).lower() == "true":
                return True
        return False


    @staticmethod
    def _model_id_from_item(item: dict) -> str:
        for key in ("name", "model", "id"):
            value = str(item.get(key) or "").strip()
            if value.startswith("@cf/"):
                return value
        return str(item.get("name") or item.get("model") or item.get("id") or "").strip()

    def _upstream_model_id(self, model_id: str) -> str:
        try:
            from rate_limiter import ModelClientPool
            return ModelClientPool.resolve_upstream_id("cloudflare", model_id)
        except Exception:
            return model_id
