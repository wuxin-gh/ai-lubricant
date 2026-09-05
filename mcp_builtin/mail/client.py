"""Client for the upstream mail API used by the built-in mail MCP service."""
from __future__ import annotations

import base64
import email
import hashlib
import hmac
import json
import re
import time
from email import policy
from email.header import decode_header, make_header
from email.utils import getaddresses
from html import unescape
from typing import Any

import aiohttp


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)


def _decode_jwt_payload(token: str) -> dict[str, Any]:
    """Decode JWT payload only for the upstream token fields; signature is not trusted here."""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        value = json.loads(base64.urlsafe_b64decode(payload).decode("utf-8"))
    except Exception as exc:
        raise MailClientError("上游登录令牌格式无效") from exc
    if not isinstance(value, dict):
        raise MailClientError("上游登录令牌载荷无效")
    return value


def _encode_hs256_jwt(payload: dict[str, Any], secret: str) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    def encoded(value: dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode("utf-8")).rstrip(b"=").decode("ascii")
    head = encoded(header)
    body = encoded(payload)
    signature = base64.urlsafe_b64encode(
        hmac.new(secret.encode("utf-8"), f"{head}.{body}".encode("ascii"), hashlib.sha256).digest()
    ).rstrip(b"=").decode("ascii")
    return f"{head}.{body}.{signature}"


class MailClientError(RuntimeError):
    """An upstream mail API request could not be completed."""


class MailClient:
    """Authenticated, read-only client for one configured upstream mailbox."""

    def __init__(self, username: str, password: str, base_url: str, secret_key: str) -> None:
        self.username = (username or "").strip()
        self.password = hashlib.sha256((password or "").encode("utf-8")).hexdigest()
        self.base_url = (base_url or "").strip().rstrip("/")
        self.secret_key = secret_key or "GZFS_SECRET_KEY_2024"
        self.user_token: str | None = None
        self.access_token: str | None = None

    @staticmethod
    def _jwt_valid(token: str | None) -> bool:
        if not token:
            return False
        try:
            payload = _decode_jwt_payload(token)
            return float(payload.get("exp") or 0) > time.time() + 60
        except MailClientError:
            return False

    @property
    def authorization(self) -> str:
        if not self._jwt_valid(self.user_token):
            raise MailClientError("上游登录令牌无效")
        try:
            payload = _decode_jwt_payload(self.user_token or "")
            return _encode_hs256_jwt(
                {
                    "address": payload.get("user_email"),
                    "address_id": str(payload.get("user_id") or ""),
                },
                self.secret_key,
            )
        except MailClientError:
            raise
        except Exception as exc:
            raise MailClientError("无法构造上游授权令牌") from exc

    async def _request(self, session: aiohttp.ClientSession, method: str, path: str, **kwargs) -> dict:
        if not self.base_url:
            raise MailClientError("未配置上游邮件服务地址")
        try:
            async with session.request(method, f"{self.base_url}{path}", **kwargs) as response:
                raw = await response.read()
                # JSON 按 RFC 8259 默认为 UTF-8。部分上游错误声明 latin-1，若先
                # response.text()/response.json() 会在 MIME 解析前就把中文破坏成乱码。
                text = ""
                for charset in ("utf-8-sig", response.charset, "gb18030", "latin-1"):
                    if not charset:
                        continue
                    try:
                        text = raw.decode(charset)
                        break
                    except (LookupError, UnicodeDecodeError):
                        continue
                if not text:
                    text = raw.decode("utf-8", errors="replace")
                if response.status >= 400:
                    raise MailClientError(f"上游邮件服务返回 HTTP {response.status}: {text[:300]}")
                try:
                    payload = json.loads(text)
                except Exception as exc:
                    raise MailClientError("上游邮件服务返回了非 JSON 响应") from exc
        except aiohttp.ClientError as exc:
            raise MailClientError(f"无法连接上游邮件服务: {exc}") from exc
        if not isinstance(payload, dict):
            raise MailClientError("上游邮件服务返回格式无效")
        return payload

    async def _login(self, session: aiohttp.ClientSession) -> None:
        if not self.username:
            raise MailClientError("未配置上游登录邮箱")
        if not self._jwt_valid(self.user_token):
            data = await self._request(
                session,
                "POST",
                "/user_api/login",
                json={"email": self.username, "password": self.password},
                headers={"User-Agent": USER_AGENT},
            )
            token = data.get("jwt")
            if not isinstance(token, str) or not token:
                raise MailClientError("上游登录响应缺少 jwt")
            self.user_token = token

        if not self._jwt_valid(self.access_token):
            data = await self._request(
                session,
                "GET",
                "/user_api/settings",
                headers={
                    "Authorization": self.authorization,
                    "X-User-Token": self.user_token,
                    "X-Admin-Auth": "",
                    "X-Custom-Auth": "",
                },
            )
            token = data.get("access_token")
            if not isinstance(token, str) or not token:
                raise MailClientError("上游设置响应缺少 access_token")
            self.access_token = token

    async def list_mail(
        self,
        session: aiohttp.ClientSession,
        *,
        limit: int = 20,
        offset: int = 0,
        address: str | None = None,
        keyword: str | None = None,
    ) -> list[dict[str, Any]]:
        await self._login(session)
        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "_ts": int(time.time() * 1000),
        }
        if address:
            params["address"] = address
        if keyword:
            params["keyword"] = keyword
        payload = await self._request(
            session,
            "GET",
            "/admin/mails",
            params=params,
            headers={
                "Authorization": self.authorization,
                "X-User-Token": self.user_token,
                "X-User-Access-Token": self.access_token,
                "X-Admin-Auth": "",
                "X-Custom-Auth": "",
                "User-Agent": USER_AGENT,
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
            },
        )
        results = payload.get("results")
        if not isinstance(results, list):
            raise MailClientError("上游邮件列表响应缺少 results")
        return [normalize_mail(item) for item in results if isinstance(item, dict)]


def _part_content(part) -> str:
    """Decode one MIME part using its declared charset with practical fallbacks."""
    try:
        payload = part.get_payload(decode=True)
        if isinstance(payload, bytes):
            declared = part.get_content_charset()
            for charset in (declared, "utf-8", "gb18030", "latin-1"):
                if not charset:
                    continue
                try:
                    return payload.decode(charset)
                except (LookupError, UnicodeDecodeError):
                    continue
            return payload.decode("utf-8", errors="replace")
        value = part.get_content()
        return value if isinstance(value, str) else ""
    except Exception:
        return ""


_UNICODE_ESCAPE_RE = re.compile(r"\\u([0-9a-fA-F]{4})")


def _decode_unicode_escapes(value: str) -> str:
    """Decode literal ``\\uXXXX`` escapes upstream left un-decoded inside content/html.

    某些上游把已 JSON 转义的字符串又当纯文本回传，导致 content/html 里出现字面量
    ``\\u9a8c\\u8bc1``（一个反斜杠 + uXXXX）而非中文。这里按码点还原，并合并可能出现的
    UTF-16 代理对（如 emoji 的 ``\\ud83d\\ude00``）。
    """
    if not value or "\\u" not in value:
        return value
    decoded = _UNICODE_ESCAPE_RE.sub(lambda m: chr(int(m.group(1), 16)), value)
    if any(0xD800 <= ord(ch) <= 0xDFFF for ch in decoded):
        try:
            decoded = decoded.encode("utf-16", "surrogatepass").decode("utf-16")
        except UnicodeError:
            return value
    return decoded


def _repair_mojibake(value: str) -> str:
    """Repair text whose bytes were decoded with the wrong charset, without touching normal Unicode.

    Two upstream faults are handled:
    - UTF-8 bytes mis-decoded as cp1252/latin-1 (西文 mojibake：Ã©, Â …)。
    - 双字节中文（GBK/gb2312/gb18030）字节被当 latin-1 解码，整串退化成 <=0xFF 的裸字节
      （中文 -> \\xd6\\xd0… = ÖÐ…），上游直接返回这种 content/html 时 JSON 里就是字节而非中文。
    """
    if not value:
        return value
    # 1) UTF-8-as-cp1252/latin-1：靠典型 mojibake 标记触发，避免误伤正常文本。
    if any(marker in value for marker in ("Ã", "Â", "ä", "å", "æ", "ç", "é", "–", "‡")):
        for source_encoding in ("cp1252", "latin-1"):
            try:
                repaired = value.encode(source_encoding).decode("utf-8")
                if repaired.count("�") <= value.count("�"):
                    return repaired
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue
    # 2) 双字节中文被当 latin-1 解码：整串都是 <=0xFF 的字节且含高位字节。正确解码的中文码点
    #    会 >0xFF，因此这里天然不会碰到已正常的 Unicode。先按 latin-1 还原字节，utf-8 解不出再
    #    试 gb18030；仅当结果确实包含 CJK 才采纳，避免把正常西文 latin-1 文本改坏。
    if any(ord(ch) >= 0x80 for ch in value) and all(ord(ch) <= 0xFF for ch in value):
        raw = value.encode("latin-1")
        for charset in ("utf-8", "gb18030"):
            try:
                repaired = raw.decode(charset)
            except (LookupError, UnicodeDecodeError):
                continue
            if any("一" <= ch <= "鿿" for ch in repaired):
                return repaired
    return value


def _html_to_text(value: str) -> str:
    if not value:
        return ""
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", value)
    text = re.sub(r"(?i)<br\s*/?>|</p>|</div>|</tr>|</li>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    return unescape(text).strip()


def _decode_header_value(value: Any) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(str(value))))
    except Exception:
        return str(value)


def parse_mail_recipients(raw: str) -> list[str]:
    """Return recipient addresses extracted from MIME To/Cc/Bcc headers."""
    if not raw:
        return []
    try:
        message = email.message_from_string(raw, policy=policy.default)
    except Exception:
        return []
    values = []
    for header in ("To", "Cc", "Bcc"):
        values.extend(message.get_all(header, []))
    addresses = []
    for _name, addr in getaddresses(values):
        addr = (addr or "").strip()
        if addr:
            addresses.append(addr)
    return addresses


_FORWARDED_HEADER_START_RE = re.compile(r"(?im)^From:[ \t]+")


def _extract_forwarded_bodies(text: str, html: str, *, is_multipart: bool) -> tuple[str, str]:
    """Decode one serialized message that a forwarding service placed in a text body."""
    if not text or html or is_multipart:
        return text, html

    for match in _FORWARDED_HEADER_START_RE.finditer(text):
        candidate = text[match.start():]
        header_end = re.search(r"\r?\n\r?\n", candidate)
        if not header_end:
            continue
        try:
            forwarded = email.message_from_string(candidate, policy=policy.default)
        except Exception:
            continue
        if not all(forwarded.get(name) for name in (
            "From", "To", "Subject", "Content-Type", "Content-Transfer-Encoding"
        )):
            continue
        if forwarded.is_multipart() or forwarded.get_content_type() not in ("text/plain", "text/html"):
            continue

        forwarded_text, forwarded_html = parse_mail_body(candidate, _decode_forwarded=False)
        decoded = forwarded_html if forwarded.get_content_type() == "text/html" else forwarded_text
        if decoded and decoded.strip() and decoded != text:
            return forwarded_text, forwarded_html
    return text, html


def parse_mail_body(raw: str, *, _decode_forwarded: bool = True) -> tuple[str, str]:
    """Return decoded text and HTML bodies without executing either one."""
    if not raw:
        return "", ""
    try:
        message = email.message_from_string(raw, policy=policy.default)
    except Exception:
        return raw, ""
    text = ""
    html = ""
    for part in message.walk():
        if part.get_content_disposition() == "attachment":
            continue
        content_type = part.get_content_type()
        if content_type == "text/plain" and not text:
            text = _part_content(part)
        elif content_type == "text/html" and not html:
            html = _part_content(part)
    if not text and not html:
        text = raw
    if _decode_forwarded:
        text, html = _extract_forwarded_bodies(text, html, is_multipart=message.is_multipart())
    return text, html


def normalize_mail(item: dict[str, Any]) -> dict[str, Any]:
    """Expose decoded bodies together with the original message source."""
    raw = item.get("raw")
    raw_text = raw if isinstance(raw, str) else ""
    text_content, html_content = parse_mail_body(raw_text)
    parsed = None
    if raw_text:
        try:
            parsed = email.message_from_string(raw_text, policy=policy.default)
        except Exception:
            parsed = None

    result = dict(item)
    result["raw_content"] = raw_text or str(item.get("content") or item.get("body") or item.get("text") or item.get("html") or "")
    # 部分上游会直接返回已解码 content/body，而 raw 为空；不能误报“无文本内容”。
    if not text_content:
        direct = item.get("text_content") or item.get("text") or item.get("content") or item.get("body")
        text_content = direct if isinstance(direct, str) else ""
    if not html_content:
        direct_html = item.get("html_content") or item.get("html")
        html_content = direct_html if isinstance(direct_html, str) else ""
    text_content = _repair_mojibake(_decode_unicode_escapes(text_content))
    html_content = _repair_mojibake(_decode_unicode_escapes(html_content))
    if not text_content and html_content:
        text_content = _html_to_text(html_content)

    subject = item.get("subject") or item.get("title")
    if not subject and parsed is not None:
        subject = parsed.get("Subject")
    result["subject"] = _repair_mojibake(_decode_header_value(subject))
    result["text_content"] = text_content
    result["html_content"] = html_content
    # received_address: 以邮件 To 头为准，缺失时回退到上游返回的 address/source 字段。
    recipients = parse_mail_recipients(raw_text)
    fallback_address = str(item.get("address") or item.get("received_address") or item.get("source") or "")
    if "@" not in fallback_address:
        fallback_address = ""
    result["received_address"] = (recipients[0] if recipients else "") or fallback_address
    return result
