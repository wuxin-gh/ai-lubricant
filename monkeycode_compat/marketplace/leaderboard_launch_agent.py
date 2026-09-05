"""用专用 agent 补齐候选池里 MCP 条目的启动方式。

外部榜单只给「仓库主页」，而我们的 mcp 模块需要可连接的启动方式：stdio 要
``command``/``args``/``env``，remote 要 ``url``/``transport``。这一步让 agent 读仓库
的 README 与包清单，把启动方式提取出来。

三条硬约束（与产品口径一致，别在别处放松）：

1. **产出永远是草稿。** 只写 ``launch_spec``/``launch_spec_status``，从不碰
   ``status``。发布始终是管理员的显式点击。
2. **必须配置专用 agent。** 未配置 ``leaderboard_launch_agent_id`` 时直接拒绝，
   而不是随便找一个 agent 顶上——通用 agent 会把「看起来像命令的字符串」当答案，
   填错的启动方式比缺失更糟（用户装了连不上，还以为是平台的问题）。
3. **场景专用提示词。** 提取启动方式是个很窄的判断（stdio 还是 remote、命令怎么
   拼、哪些 env 必填、要不要先编译），所以这里用固定的场景约束，不吃 agent 的
   默认行为。
"""
from __future__ import annotations

import json
import re
from typing import Any

from loguru import logger

from .source_config import get_source_config_async


class LaunchAgentUnavailable(RuntimeError):
    """未配置专用 agent，或该 agent 不可用。"""


_ALLOWED_TRANSPORTS = ("sse", "streamable-http")

_SYSTEM_PROMPT = """\
你的任务：读一个 GitHub 仓库，产出「资源信息」结构化结果，供上架前的草稿填充。

必产出（所有仓库都要）：
- description：一句到一段的中文介绍，说明这个仓库是什么、能做什么。上游描述可能是
  英文、太长或空白，你要重写成清晰简洁的中文。
- categories：子分类建议数组（3-6 个），每个是一个具体用途/场景。
- tags：标签建议数组（搜索用的自由词，如 llm / agent / rag）。

当仓库分类包含 MCP（下方会告诉你分类）时，额外判断启动方式：
- stdio：本地进程。需要 command（可执行文件）、args（参数数组）、env（必填环境变量名）。
- remote：远程服务。需要 url（HTTPS）、transport（sse 或 streamable-http）。
非 MCP 的仓库，kind 填 "none"，不要编造启动方式。

工作方法：
1. 读仓库的 README、package.json / pyproject.toml / Dockerfile 等清单文件。
2. 介绍与场景基于 README 实际内容，不要照搬营销话术，也不要空泛。
3. MCP 启动方式只报告你在仓库里**实际读到**的内容；读不到就说读不到。

硬性要求：
- 不要猜启动方式。没有明确依据时 confidence 填 "low" 并在 notes 说明缺什么。
- 不要把示例里的占位符（YOUR_API_KEY、/path/to/xxx）当成真实值。
- 仓库若只是 awesome 列表或文档集合，kind 填 "none"，description 照样要写清楚它是个目录。

最后一条消息只输出一个 JSON 对象，不要任何额外文字、不要代码块围栏：
{
  "description": "",
  "categories": [],
  "tags": [],
  "kind": "stdio" | "remote" | "none",
  "command": "",
  "args": [],
  "env": [],
  "url": "",
  "transport": "",
  "confidence": "high" | "medium" | "low",
  "notes": ""
}
"""


def _build_prompt(item: dict, modules: list[str]) -> str:
    is_mcp = "mcp" in modules
    module_line = "、".join(modules) if modules else "（未分类，仅浏览）"
    scope = (
        "本仓库分类包含 MCP，请一并判断启动方式。"
        if is_mcp
        else "本仓库分类不含 MCP，kind 填 none，只产出介绍/场景/关键词。"
    )
    return (
        f"仓库：{item.get('repo_full_name')}\n"
        f"地址：{item.get('repo_url')}\n"
        f"当前分类：{module_line}\n"
        f"上游描述：{item.get('description') or '(无)'}\n"
        f"语言：{item.get('language') or '(未标注)'}\n"
        f"当前标签：{', '.join(item.get('tags') or []) or '(无)'}\n"
        f"当前子分类：{', '.join(item.get('categories') or []) or '(无)'}\n\n"
        f"{scope}\n"
        "请按系统提示词的要求，识别这个仓库的信息并输出 JSON。"
    )


def _extract_json(outputs: list[dict[str, Any]]) -> dict | None:
    """从 agent 输出里取最后一个 JSON 对象。

    模型偶尔会在 JSON 外包一层说明或代码块围栏，所以从后往前找第一个能解析的
    候选，而不是硬要求整条消息就是 JSON。
    """
    for item in reversed(outputs or []):
        text = ""
        if isinstance(item, dict):
            text = str(item.get("content") or item.get("text") or "")
        if not text.strip():
            continue
        fenced = re.findall(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
        candidates = fenced + re.findall(r"(\{[\s\S]*\})", text)
        for candidate in candidates:
            try:
                parsed = json.loads(candidate)
            except (TypeError, ValueError):
                continue
            if isinstance(parsed, dict) and "kind" in parsed:
                return parsed
    return None


def normalize_spec(raw: dict) -> dict:
    """把模型输出收敛成可存的形状，非法值一律丢弃而不是硬塞。"""
    kind = str(raw.get("kind") or "").strip().lower()
    if kind not in ("stdio", "remote", "none"):
        kind = "none"
    spec: dict[str, Any] = {
        "kind": kind,
        "confidence": str(raw.get("confidence") or "low").strip().lower(),
        "notes": str(raw.get("notes") or "").strip()[:2000],
        # 仓库信息（所有分类都识别）：中文介绍 / 子分类 / 标签。
        "description": str(raw.get("description") or "").strip()[:2000],
    }
    categories = raw.get("categories")
    if isinstance(categories, list):
        spec["categories"] = [str(u).strip() for u in categories if str(u).strip()][:20]
    else:
        spec["categories"] = []
    tags = raw.get("tags")
    if isinstance(tags, list):
        spec["tags"] = [str(t).strip() for t in tags if str(t).strip()][:20]
    else:
        spec["tags"] = []
    if kind == "stdio":
        spec["command"] = str(raw.get("command") or "").strip()
        args = raw.get("args")
        spec["args"] = [str(a) for a in args if str(a).strip()] if isinstance(args, list) else []
        env = raw.get("env")
        spec["env"] = [str(e).strip() for e in env if str(e).strip()] if isinstance(env, list) else []
    elif kind == "remote":
        url = str(raw.get("url") or "").strip()
        spec["url"] = url if url.startswith("https://") else ""
        transport = str(raw.get("transport") or "").strip().lower()
        spec["transport"] = transport if transport in _ALLOWED_TRANSPORTS else ""
    return spec


def _is_usable(spec: dict) -> bool:
    """够不够格算「补上了」。不够格仍存草稿，但状态标 failed 供人工接手。"""
    if spec.get("kind") == "stdio":
        return bool(spec.get("command"))
    if spec.get("kind") == "remote":
        return bool(spec.get("url") and spec.get("transport"))
    return False


async def resolve_agent_id() -> int:
    source = await get_source_config_async()
    agent_id = int(source.get("leaderboard_launch_agent_id") or 0)
    if agent_id <= 0:
        raise LaunchAgentUnavailable(
            "未配置补全启动方式的专用 agent，请先在市场管理的外部榜单配置里指定"
        )
    from db import PostgresClient

    if not PostgresClient.pool:
        raise LaunchAgentUnavailable("数据库不可用")
    row = await PostgresClient.get_agent(agent_id)
    if not row:
        raise LaunchAgentUnavailable(f"配置的 agent 不存在：{agent_id}")
    if row.get("enabled") is False:
        raise LaunchAgentUnavailable(f"配置的 agent 已禁用：{agent_id}")
    return agent_id


async def resolve_agent_id(requested: int | None = None) -> int:
    """解析本次识别用的 agent。调用方显式指定（批量弹框选了 agent）时优先；
    未指定才回落市场配置里的专用 agent。两条路都校验存在与启用。"""
    if requested is not None and requested > 0:
        agent_id = int(requested)
    else:
        source = await get_source_config_async()
        agent_id = int(source.get("leaderboard_launch_agent_id") or 0)
        if agent_id <= 0:
            raise LaunchAgentUnavailable(
                "未配置专用 agent，请先在市场管理的外部榜单配置里指定，或在批量识别时选择"
            )
    from db import PostgresClient

    if not PostgresClient.pool:
        raise LaunchAgentUnavailable("数据库不可用")
    row = await PostgresClient.get_agent(agent_id)
    if not row:
        raise LaunchAgentUnavailable(f"配置的 agent 不存在：{agent_id}")
    if row.get("enabled") is False:
        raise LaunchAgentUnavailable(f"配置的 agent 已禁用：{agent_id}")
    return agent_id


async def recognize_item(item_id: int, agent_id: int | None = None, model: str = "") -> dict:
    """识别一条仓库信息（通用入口）：所有分类都产出描述/子分类/标签，
    分类含 MCP 时顺带识别启动方式。产出只写草稿字段，条目发布状态不变。

    ``agent_id``/``model`` 来自批量弹框的下拉选择：agent 未传则用配置的专用
    agent；model 未传用 agent 自己绑定的模型（``GenericAgent.model_override``
    复用同一把网关 key 只换模型，同定时任务口径）。

    写回口径：
    - 描述直接写列；子分类/标签与现有值**合并去重**（agent 是建议者不是覆盖者）。
    - 启动方式仍走 ``set_launch_spec``（pending/filled/failed 状态机不变），
      仅在分类含 mcp 时执行——非 mcp 条目不碰 launch_spec*，避免误标徽标。
    """
    import marketplace_leaderboard_store as store

    item = await store.get_item(item_id)
    if item is None:
        raise ValueError("条目不存在")
    modules = item.get("target_modules")
    if not isinstance(modules, list):
        modules = [item.get("target_module")] if item.get("target_module") else []
    is_mcp = "mcp" in modules

    resolved_agent_id = await resolve_agent_id(agent_id)
    if is_mcp:
        await store.set_launch_spec(item_id, spec=item.get("launch_spec") or {}, status="pending")

    from agent.agent_main import GenericAgent

    try:
        agent = GenericAgent(agent_id=resolved_agent_id, model_override=model or "")
        outputs = await agent.run_task(
            _build_prompt(item, modules), system_prompt=_SYSTEM_PROMPT, max_turns=12
        )
    except Exception as exc:  # noqa: BLE001 — 失败要落状态，不能只抛
        logger.warning("[leaderboard] recognize agent failed for {}: {}", item_id, exc)
        if is_mcp:
            await store.set_launch_spec(item_id, spec={}, status="failed", error=str(exc)[:500])
        return {"id": item_id, "ok": False, "error": str(exc)}

    raw = _extract_json(outputs)
    if raw is None:
        if is_mcp:
            await store.set_launch_spec(
                item_id, spec={}, status="failed", error="agent 未返回可解析的 JSON"
            )
        return {"id": item_id, "ok": False, "error": "agent 未返回可解析的 JSON"}

    spec = normalize_spec(raw)
    usable = _is_usable(spec)

    # 仓库信息写回。agent 给了非空值才写，保持「没识别出什么就别动」；
    # 子分类/标签与现有值合并去重——agent 是建议者，不抹掉管理员已填的。
    curation: dict[str, Any] = {}
    if spec.get("description"):
        curation["description"] = spec["description"]
    merged_categories = list(dict.fromkeys(
        [str(c) for c in (item.get("categories") or [])]
        + [str(c) for c in (spec.get("categories") or [])]
    ))
    if merged_categories:
        curation["categories"] = merged_categories
    merged_tags = list(dict.fromkeys(
        [str(t) for t in (item.get("tags") or [])]
        + [str(t) for t in (spec.get("tags") or [])]
    ))
    if merged_tags:
        curation["tags"] = merged_tags
    row = None
    if curation:
        row = await store.update_curation(item_id, curation)

    if is_mcp:
        row = await store.set_launch_spec(
            item_id,
            spec=spec,
            status="filled" if usable else "failed",
            error="" if usable else "未能提取出可用的启动方式，请人工补充",
        ) or row
        # 自动验证（默认关，source_config.leaderboard_auto_verify 开启时）：识别填完 launch_spec
        # 顺带跑一次 verify_launch_spec 写 verified/failed。默认关因为握手可能慢/抖，不该挡识别主流程。
        if usable:
            try:
                from .source_config import get_source_config_async
                source = await get_source_config_async()
            except Exception:  # noqa: BLE001 — 配置读不了就不自动验证，不挡识别
                source = {}
            if source.get("leaderboard_auto_verify"):
                from . import leaderboard_verify
                result = await leaderboard_verify.verify_launch_spec(row or {"launch_spec": spec})
                if result["status"]:
                    row = await store.set_launch_spec(
                        item_id, spec=spec,
                        status=result["status"],
                        error=("verify: " + result["error"]) if result["error"] else "",
                    ) or row
    else:
        # 非 mcp：ok 以「有没有产出仓库信息」计。
        usable = bool(spec.get("description") or spec.get("categories") or spec.get("tags"))

    # 注意：这里刻意不改 status/不发布。识别只是把草稿填得更完整，
    # 是否上线仍由管理员在核对后决定。
    return {
        "id": item_id,
        "ok": usable,
        "description": spec.get("description") or "",
        "launch_spec": spec if is_mcp else None,
        "needs_review": True,
        "item": row,
    }


async def fill_launch_spec(item_id: int) -> dict:
    """兼容入口：识别仓库信息（含 MCP 启动方式）。见 ``recognize_item``。"""
    return await recognize_item(item_id)
