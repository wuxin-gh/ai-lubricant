"""Read request-log payloads from ClickHouse without affecting request traffic."""
from __future__ import annotations

from typing import Any

from clickhouse_config import get_settings
from integrations.clickhouse import ClickHousePayloadClient

PAYLOAD_FIELDS = (
    "request_body",
    "request_headers",
    "router_request_body",
    "router_request_headers",
    "router_response_body",
    "response_body",
    "response_headers",
)


async def fetch_payloads(attempt_keys: list[str]) -> tuple[dict[str, dict[str, Any]], str]:
    settings = get_settings()
    if not settings.enabled:
        return {}, "disabled"
    client = ClickHousePayloadClient(
        addr=settings.addr,
        database=settings.database,
        username=settings.username,
        password=settings.password,
    )
    try:
        await client.connect()
        payloads = await client.fetch_request_payloads(attempt_keys)
        return payloads, "available"
    except Exception:
        return {}, "unavailable"
    finally:
        await client.close()


def hydrate(row: dict, payload: dict[str, Any] | None, status: str) -> dict:
    for field in PAYLOAD_FIELDS:
        row[field] = (payload or {}).get(field, {})
    row["payload_status"] = status if payload is None else "available"
    return row
