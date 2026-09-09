"""社区运营配置（技术交流群 + 社区通知），存 DB 主配置 blob 的 ``community`` key。

与市场仓库完全无关：不进 GitHub manifest、不吃 ``_require_market_writable`` 门禁，
所以**不挂在 ``marketplace`` key 下**（source_config 的 _normalize/update/public_view
都是显式白名单，嵌套字段会被静默丢弃），用独立 key + 自己的形状归一。

数据形态（归一后）::

    {
      "groups": [                          # ≤12 个群
        {"id": "g1", "type": "wechat", "label": "微信群①",
         "qr_image": "data:image/webp;base64,..."},
      ],
      "notice": {
        "enabled": True,
        "entries": [                       # ≤12 条，文本/图片混排
          {"id": "n1", "kind": "text",  "text": "欢迎..."},
          {"id": "n2", "kind": "image", "image": "data:image/webp;base64,..."},
        ],
      },
    }

图片只收 ``data:image/`` 前缀的 data URL（管理端上传时已做 2MB/512px/WebP 压缩，
见前端 loadIconFile 同款管线），存 DB 不入仓库——与 channel icon 在 DB 的口径一致。
图片只收 data URL（http(s) 的不受此限制）。
公开读接口只回这份视图，不掺任何仓库/内部字段。
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any

MAX_GROUPS = 12
MAX_NOTICE_ENTRIES = 12
MAX_TEXT_CHARS = 2000
MAX_LABEL_CHARS = 64

# 群类型白名单：前端下拉与其保持一致；不在名单里的类型兜底为 other。
GROUP_TYPES = ("wechat", "feishu", "dingtalk", "qq", "other")

_CONFIG_KEY = "community"


def _data_url(value: Any) -> str:
    """图片字段：只收 data:image/ 前缀的 data URL，其余（含 http 链接、空）原样透传
    ——历史值里若有 http URL 也能显示；非字符串一律清空。"""
    text = str(value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if text.startswith("data:") and not lowered.startswith("data:image/"):
        return ""
    return text


def _clean_text(value: Any, limit: int) -> str:
    text = str(value or "").strip()
    return text[:limit]


def _normalize_group(raw: Any, index: int) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    group_type = str(raw.get("type") or "").strip()
    if group_type not in GROUP_TYPES:
        group_type = "other"
    qr_image = _data_url(raw.get("qr_image"))
    # 没有二维码的群没有展示价值：整条丢弃，避免用户侧渲染出半张空卡。
    if not qr_image:
        return {}
    return {
        "id": _clean_text(raw.get("id"), 64) or f"g{index + 1}",
        "type": group_type,
        "label": _clean_text(raw.get("label"), MAX_LABEL_CHARS),
        "qr_image": qr_image,
    }


def _normalize_entry(raw: Any, index: int) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    kind = "image" if str(raw.get("kind") or "").strip() == "image" else "text"
    if kind == "image":
        image = _data_url(raw.get("image"))
        if not image:
            return {}
        return {"id": _clean_text(raw.get("id"), 64) or f"n{index + 1}", "kind": kind, "image": image}
    text = _clean_text(raw.get("text"), MAX_TEXT_CHARS)
    if not text:
        return {}
    return {"id": _clean_text(raw.get("id"), 64) or f"n{index + 1}", "kind": kind, "text": text}


def _normalize(data: dict[str, Any] | None) -> dict[str, Any]:
    raw = data if isinstance(data, dict) else {}
    groups_raw = raw.get("groups")
    groups = [
        group
        for group in (_normalize_group(item, i) for i, item in enumerate(groups_raw if isinstance(groups_raw, list) else []))
        if group
    ][:MAX_GROUPS]
    notice_raw = raw.get("notice")
    notice: dict[str, Any] = {"enabled": False, "entries": []}
    if isinstance(notice_raw, dict):
        entries_raw = notice_raw.get("entries")
        entries = [
            entry
            for entry in (_normalize_entry(item, i) for i, item in enumerate(entries_raw if isinstance(entries_raw, list) else []))
            if entry
        ][:MAX_NOTICE_ENTRIES]
        # 开关开着但一条内容都没有 = 没东西可展示，直接视为关闭。
        notice = {"enabled": bool(notice_raw.get("enabled")) and bool(entries), "entries": entries}
    return {"groups": groups, "notice": notice}


async def _read_config_blob_async_impl() -> dict[str, Any]:
    try:
        from config import CONFIG_STORE

        if hasattr(CONFIG_STORE, "read_main_async"):
            data = await CONFIG_STORE.read_main_async()
        else:
            data = CONFIG_STORE.read_main()
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


async def get_community_config_async() -> dict[str, Any]:
    """DB 里保存的社区运营配置（未配置时返回空骨架）。"""
    blob = await _read_config_blob_async_impl()
    return _normalize(blob.get(_CONFIG_KEY))


async def update_community_config(patch: dict[str, Any]) -> dict[str, Any]:
    """整段替换保存：``groups`` / ``notice`` 各自 if-in-patch 全量替换，未传的不动。"""
    current = await get_community_config_async()
    merged = deepcopy(current)
    if "groups" in patch:
        merged["groups"] = _normalize({"groups": patch.get("groups")})["groups"]
    if "notice" in patch:
        merged["notice"] = _normalize({"notice": patch.get("notice")})["notice"]
    elif isinstance(patch.get("notice_enabled"), bool):
        # 便捷字段：只翻开关不动内容（管理端若只想改开关可用）。
        merged["notice"]["enabled"] = patch["notice_enabled"] and bool(merged["notice"]["entries"])

    blob = await _read_config_blob_async_impl()
    blob[_CONFIG_KEY] = merged

    from config import CONFIG_STORE

    if hasattr(CONFIG_STORE, "write_main_async"):
        await CONFIG_STORE.write_main_async(blob)
    else:
        CONFIG_STORE.write_main(blob)
    return merged


def public_view(config: dict[str, Any]) -> dict[str, Any]:
    """用户侧/管理端共用的公开视图：只有 groups + notice，无任何内部字段。"""
    return deepcopy({"groups": config.get("groups") or [], "notice": config.get("notice") or {"enabled": False, "entries": []}})
