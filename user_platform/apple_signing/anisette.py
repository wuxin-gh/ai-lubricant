"""Anisette 提供者（移植自 iPASide anisette.py，加进程内锁防并发重复 provisioning）。

Anisette 头是 Apple GrandSlam（GSA）服务器要求非 Apple 机器提供的设备 provisioning
数据，纯 Python ``anisette`` 包用 Apple 自家便携库在进程内生成。库（约 2MB，arm64，
与宿主 CPU 无关）从公共主机拉一次，和 provisioning 状态缓存成单文件——机器每次
都以**稳定、已 provision 的设备**出现；反复重 provision 才是触发 Apple 反滥用的
原因。库是 Apple 自家二进制，故下载而非再分发。无 Apple ID 信息进入 anisette 数据。

Windows 特有坑（都踩过、live 验证过，见函数 docstring）：
区域显示名 / 本地化时区名会让受信设备 2FA 静默 HTTP 500。
"""

from __future__ import annotations

import io
import locale
import re
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from . import paths
from .errors import AnisetteError


#: 便携库的下载源，按序尝试。
_LIBS_URLS: tuple[str, ...] = (
    "https://anisette.dl.mikealmel.ooo/libs?arch=arm64-v8a",
)

#: 响应不是这些格式之一 = 错误页/截断下载，不是库文件——在这里拦掉，
#: 免得报出晦涩的 "not a gzip/tar file"。
_ARCHIVE_MAGIC = (b"PK", b"\x1f\x8b", b"BZh", b"\xfd7zXZ")

#: Apple 风格 locale 形如 ``zh_CN`` / ``en_US``。``anisette`` 包用
#: ``locale.getlocale()``，Windows 上是**显示名**（Chinese (Simplified)_China）——
#: ASCII、latin-1 编码没问题，但 Apple 受信设备 2FA 端点对它回 HTTP 500、永不推码
#: （iPASide issue #5，live 复现）。
_APPLE_LOCALE = re.compile(r"^[A-Za-z]{2,3}([_-][A-Za-z0-9]+)+$")

#: Windows getlocale() 显示名 → Apple 客户端发的标识符。
_WINDOWS_LOCALE_TO_APPLE: dict[str, str] = {
    "chinese (simplified)_china": "zh_CN",
    "chinese (simplified)_singapore": "zh_SG",
    "chinese (traditional)_taiwan": "zh_TW",
    "chinese (traditional)_hong kong s.a.r.": "zh_HK",
    "chinese (traditional)_hong kong sar": "zh_HK",
    "chinese (traditional)_macao s.a.r.": "zh_MO",
    "chinese_china": "zh_CN",
    "chinese_taiwan": "zh_TW",
    "english_united states": "en_US",
    "english_united kingdom": "en_GB",
    "english_india": "en_IN",
    "english_australia": "en_AU",
    "english_canada": "en_CA",
    "japanese_japan": "ja_JP",
    "korean_korea": "ko_KR",
    "german_germany": "de_DE",
    "french_france": "fr_FR",
    "french_canada": "fr_CA",
    "spanish_spain": "es_ES",
    "spanish_mexico": "es_MX",
    "portuguese_brazil": "pt_BR",
    "portuguese_portugal": "pt_PT",
    "russian_russia": "ru_RU",
    "italian_italy": "it_IT",
    "dutch_netherlands": "nl_NL",
    "polish_poland": "pl_PL",
    "turkish_turkey": "tr_TR",
    "thai_thailand": "th_TH",
    "vietnamese_vietnam": "vi_VN",
    "arabic_saudi arabia": "ar_SA",
    "hindi_india": "hi_IN",
}

# 服务器上多个请求可能并发首次触发 provisioning——串行化，避免同时
# 下载/重复 provision 触发 Apple 反滥用。
_provider_lock = threading.Lock()


def _download_libs() -> io.BytesIO:
    """拉取 Apple provisioning 库：每个源最多 3 次重试。

    拉不到时抛 :class:`AnisetteError`（而非归档解析器的错），服务端能据此
    说出「源站挂了 / 网络拦截」。
    """
    attempts: list[str] = []
    for url in _LIBS_URLS:
        for _ in range(3):
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                data = response.content
                if not any(data.startswith(magic) for magic in _ARCHIVE_MAGIC):
                    raise ValueError(
                        f"response was not an archive (starts {data[:12]!r})"
                    )
                return io.BytesIO(data)
            except Exception as exc:  # noqa: BLE001 — 记录每个失败源
                attempts.append(f"{url}: {exc}")
    raise AnisetteError(
        "无法下载 Apple 设备 provisioning 库（anisette 源不可达或被网络拦截）。\n"
        "Tried:\n  " + "\n  ".join(attempts)
    )


def _load_provider() -> Any:
    """返回就绪、已 provision 的 Anisette 提供者，状态持久化到机器级缓存。

    坏缓存直接丢弃重建（而非抛错重放）：首次 provision 中断 / 存了坏下载，
    以后每次启动都会抛同一个归档错误，用户无从自救。
    """
    from anisette import Anisette

    state = paths.anisette_state_file()

    def _fresh() -> Any:
        return Anisette.init(_download_libs())

    if state.exists():
        try:
            provider = Anisette.load(str(state))
        except AnisetteError:
            raise
        except Exception:  # noqa: BLE001 — 载入失败即缓存不可用
            state.unlink(missing_ok=True)
            provider = _fresh()
    else:
        provider = _fresh()

    if not provider.is_provisioned:
        provider.provision()

    provider.save_all(str(state))
    return provider


def _gmt_offset_label(offset: timedelta) -> str:
    """按 Apple 客户端无缩写时的格式输出 UTC 偏移。"""
    total = int(offset.total_seconds())
    sign = "+" if total >= 0 else "-"
    total = abs(total)
    hours, rem = divmod(total, 3600)
    minutes = rem // 60
    return f"GMT{sign}{hours:02d}:{minutes:02d}"


def ascii_timezone(now: datetime | None = None) -> str:
    """可安全放进 HTTP 头的时区标签。

    ``anisette`` 包用 ``str(tzinfo)``，Windows 上是**本地化显示名**
    （中国标准时间）。HTTP 头是 latin-1（urllib3 这么编码），CJK 名会当场
    UnicodeEncodeError。Apple 自家客户端发缩写（PDT/CST）或 GMT±HH:MM——
    都是 ASCII，我们就发这个。
    """
    when = now if now is not None else datetime.now().astimezone()
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)

    name = when.tzname() or ""
    if name.isascii() and name.strip():
        return name.strip()

    offset = when.utcoffset()
    if offset is None:
        return "GMT"
    return _gmt_offset_label(offset)


def ascii_locale(preferred: str | None = None) -> str:
    """Apple GrandSlam 端点接受的 locale 标签。

    必须既 latin-1 安全又是 Apple 认识的标识符。Windows 显示名是 ASCII 不够——
    ``Chinese (Simplified)_China`` 编码没问题，``/auth/verify/trusteddevice``
    照样 HTTP 500 不推码（issue #5）。映射 Windows 名 → Apple 风格标签
    （zh_CN），已是 zh_CN / en_US 形的直接用，兜底 en_US。
    """
    candidates: list[str] = []
    if preferred:
        candidates.append(preferred)
    # Windows 常在 getdefaultlocale() 暴露 POSIX 标签（zh_CN），即使 getlocale()
    # 返回的是 anisette 包拷进头的显示名形态。
    try:
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            default = locale.getdefaultlocale()[0]
    except Exception:  # noqa: BLE001 — 各平台 locale 栈差异大
        default = None
    if default:
        candidates.append(default)
    try:
        current = locale.getlocale()[0]
    except Exception:  # noqa: BLE001
        current = None
    if current:
        candidates.append(current)

    for candidate in candidates:
        mapped = _apple_locale_tag(candidate)
        if mapped:
            return mapped
    return "en_US"


def _apple_locale_tag(value: str) -> str | None:
    """把 value 映射成 Apple 风格 locale 标签，映射不了返回 None。"""
    if not value or not value.isascii():
        return None
    cleaned = value.strip().replace("-", "_")
    if _APPLE_LOCALE.fullmatch(cleaned):
        # 轻量归一化大小写：zh_cn → zh_CN（region 两位时）。
        parts = cleaned.split("_")
        if len(parts) >= 2 and len(parts[1]) == 2:
            parts[1] = parts[1].upper()
        parts[0] = parts[0].lower()
        return "_".join(parts)
    return _WINDOWS_LOCALE_TO_APPLE.get(value.strip().lower())


def _wire_safe_headers(raw: dict[str, Any]) -> dict[str, Any]:
    """重写必须既 latin-1 安全又被 Apple 接受的 anisette 字段。

    X-Apple-I-TimeZone / X-Apple-Locale 来自 OS 的本地化/Windows 显示串；其余
    anisette 值本来就是 ASCII（base64、UUID、ISO 时间戳）。在所有调用方必经的
    这里重写，覆盖 GrandSlam 2FA、developer services 等一切把 anisette 放进
    请求头的路径。
    """
    headers = dict(raw)
    headers["X-Apple-I-TimeZone"] = ascii_timezone()
    existing_locale = headers.get("X-Apple-Locale")
    headers["X-Apple-Locale"] = ascii_locale(
        existing_locale if isinstance(existing_locale, str) else None
    )
    return headers


def get_headers() -> dict[str, Any]:
    """返回一套新鲜 anisette 头（GSA 请求用）。"""
    with _provider_lock:
        provider = _load_provider()
        return _wire_safe_headers(dict(provider.get_data()))
