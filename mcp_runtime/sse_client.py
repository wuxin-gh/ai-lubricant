"""Minimal MCP-over-SSE client for remote ``kind=sse`` services."""
from __future__ import annotations

import json
from urllib.parse import urljoin

import aiohttp


class SSEClientError(RuntimeError):
    pass


class SSEClient:
    def __init__(self, url: str, headers: dict[str, str] | None = None) -> None:
        self.url = (url or "").strip()
        self.headers = {str(k): str(v) for k, v in (headers or {}).items() if v is not None}

    async def _open(self, session: aiohttp.ClientSession):
        if not self.url:
            raise SSEClientError("MCP SSE URL is empty")
        response = await session.get(
            self.url,
            headers={**self.headers, "Accept": "text/event-stream"},
        )
        if response.status >= 400:
            text = await response.text()
            await response.release()
            raise SSEClientError(f"MCP SSE endpoint returned HTTP {response.status}: {text[:300]}")
        return response

    @staticmethod
    async def _events(response):
        event = "message"
        data: list[str] = []
        async for raw in response.content:
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            if line:
                if line.startswith("event:"):
                    event = line[6:].strip() or "message"
                elif line.startswith("data:"):
                    data.append(line[5:].lstrip())
                continue
            if data:
                yield event, "\n".join(data)
            event = "message"
            data = []

    async def _rpc(self, method: str, params: dict, *, timeout: float = 60) -> dict:
        timeout_cfg = aiohttp.ClientTimeout(total=timeout, sock_connect=15, sock_read=None)
        async with aiohttp.ClientSession(timeout=timeout_cfg) as session:
            stream = await self._open(session)
            endpoint = None
            events = self._events(stream)
            async for event_name, data in events:
                if event_name != "endpoint":
                    continue
                endpoint = urljoin(str(stream.url), data.strip())
                break
            if not endpoint:
                stream.close()
                raise SSEClientError("MCP SSE stream did not provide an endpoint")

            async def post(payload: dict) -> None:
                async with session.post(
                    endpoint,
                    headers={**self.headers, "Content-Type": "application/json"},
                    json=payload,
                ) as response:
                    if response.status >= 400:
                        text = await response.text()
                        raise SSEClientError(f"MCP message endpoint returned HTTP {response.status}: {text[:300]}")

            request_id = 1
            await post({
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "ai-lubricant", "version": "0.1.0"},
                },
            })
            result = None
            async for _event_name, data in events:
                try:
                    payload = json.loads(data)
                except json.JSONDecodeError:
                    continue
                if payload.get("id") == request_id:
                    result = payload
                    break
            if not isinstance(result, dict):
                raise SSEClientError("MCP initialize returned no JSON-RPC response")
            if result.get("error"):
                raise SSEClientError(str(result["error"].get("message") or result["error"]))

            await post({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}})
            request_id += 1
            await post({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
            async for _event_name, data in events:
                try:
                    payload = json.loads(data)
                except json.JSONDecodeError:
                    continue
                if payload.get("id") != request_id:
                    continue
                if payload.get("error"):
                    raise SSEClientError(str(payload["error"].get("message") or payload["error"]))
                value = payload.get("result")
                return value if isinstance(value, dict) else {}
            raise SSEClientError(f"MCP {method} returned no JSON-RPC response")

    async def list_tools(self) -> list[dict]:
        result = await self._rpc("tools/list", {})
        tools = result.get("tools")
        return tools if isinstance(tools, list) else []

    async def call_tool(self, name: str, arguments: dict) -> list[dict]:
        result = await self._rpc("tools/call", {"name": name, "arguments": arguments or {}})
        content = result.get("content")
        if isinstance(content, list):
            return content
        return [{"type": "text", "text": json.dumps(result, ensure_ascii=False)}]
