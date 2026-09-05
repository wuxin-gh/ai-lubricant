"""ClickHouse 请求 payload 双写的运行期配置（缓存一次，供热路径复用）。

ClickHouse 是与 postgres/redis 并列的主链路数据源，配置统一由
``bootstrap_config.get_clickhouse_config()`` 解析（环境变量优先→.env→默认）。
本模块把解析结果缓存成一个只读对象，避免逐请求热路径
(``request_log_writer._fanout_payload``) 重复解析 .env。
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ClickHouseSettings:
    enabled: bool
    addr: str
    database: str
    username: str
    password: str
    ttl_days: int
    max_payload_bytes: int


_cached: ClickHouseSettings | None = None


def _load() -> ClickHouseSettings:
    from bootstrap_config import get_clickhouse_config

    cfg = get_clickhouse_config()
    return ClickHouseSettings(
        enabled=cfg["enabled"],
        addr=cfg["addr"],
        database=cfg["database"],
        username=cfg["username"],
        password=cfg["password"],
        ttl_days=cfg["ttl_days"],
        max_payload_bytes=cfg["max_payload_bytes"],
    )


def get_settings() -> ClickHouseSettings:
    """返回缓存的 ClickHouse 数据源配置（首次调用解析并缓存）。"""
    global _cached
    if _cached is None:
        _cached = _load()
    return _cached


def reload() -> ClickHouseSettings:
    """强制重新解析配置（测试用）。"""
    global _cached
    _cached = _load()
    return _cached
