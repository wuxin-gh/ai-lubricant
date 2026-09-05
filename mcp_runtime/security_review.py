"""AI security review for MCP plugin versions.

每个版本必须人工触发本检查，检查通过（security_status='passed'）后才允许 activate。
检查本身走 agent_llm_configs 的默认 LLM 配置，不走主系统 provider 调度。
"""
from __future__ import annotations

import json
from typing import Any

from loguru import logger

import mcp_plugin_store


SECURITY_PROMPT = """你是代码安全审查员。请审查下面这段即将在 MCP Runtime 进程内执行的 Python 插件代码。

背景：
- 代码在服务端 Runtime 单进程内执行，不做子进程隔离。
- 插件应只通过 register(reg) 注册工具，工具函数签名 async def fn(args, ctx)。
- 允许正常的数据处理、调用 ctx.http_session() 做必要 HTTP 请求、记录日志。
- 需要重点识别：命令执行、任意文件读写、凭证/环境变量读取、网络外传、eval/exec/动态 import、无限循环/资源耗尽、绕过注册契约、隐藏后门、敏感数据泄漏。

输出必须是严格 JSON，不要 Markdown，不要解释性前后缀。结构：
{
  "verdict": "pass" | "fail",
  "risk_level": "low" | "medium" | "high",
  "summary": "一句话结论",
  "issues": [
    {"severity":"low|medium|high", "line": 1, "type":"问题类型", "detail":"问题说明", "suggestion":"修复建议"}
  ]
}

判定规则：
- 存在高危命令执行、任意文件写入、读取系统凭据/环境变量并外传、动态执行/加载不可信代码、明显后门时 verdict 必须 fail。
- 只有在代码符合插件契约且没有明显安全风险时 verdict 才能 pass。

待审查代码：
```python
{code}
```
"""


def _extract_json(text: str) -> dict[str, Any]:
    text = (text or "").strip()
    if not text:
        raise ValueError("empty model response")
    try:
        return json.loads(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            return json.loads(text[start:end + 1])
        raise


async def _resolve_security_llm() -> tuple[dict, str]:
    from db import PostgresClient

    cfg = await PostgresClient.get_default_agent_llm_config()
    if not cfg:
        raise RuntimeError("未配置默认 Agent LLM，无法执行 MCP 插件安全检查")
    models = await PostgresClient.list_agent_llm_models(cfg.get("id"), enabled_only=True)
    if not models:
        raise RuntimeError("默认 Agent LLM 配置下没有启用模型，无法执行 MCP 插件安全检查")
    model = models[0].get("model") or models[0].get("name") or models[0].get("model_id")
    if not model:
        raise RuntimeError("默认 Agent LLM 模型配置缺少模型名")
    return cfg, str(model)


async def run_security_check(version_id: int) -> dict:
    version = await mcp_plugin_store.get_version(version_id)
    if not version:
        raise ValueError(f"MCP plugin version {version_id} not found")

    code = version.get("code") or ""
    if not code.strip():
        report = {
            "verdict": "fail",
            "risk_level": "high",
            "summary": "版本没有插件代码，不能发布。",
            "issues": [{
                "severity": "high",
                "line": 1,
                "type": "empty_code",
                "detail": "plugin_versions.code 为空。",
                "suggestion": "写入符合 register(reg) 契约的插件代码后重新检查。",
            }],
        }
        updated = await mcp_plugin_store.update_security_result(version_id, status="failed", report=report, model="")
        return updated or {"id": version_id, "security_status": "failed", "security_report": report}

    cfg, model = await _resolve_security_llm()
    from agent.llm_bridge import LLMBridge

    bridge = LLMBridge(cfg, model, session_id=f"mcp-security-{version_id}")
    messages = [
        {"role": "system", "content": "你只输出严格 JSON。"},
        {"role": "user", "content": SECURITY_PROMPT.format(code=code)},
    ]
    try:
        resp = await bridge.chat(messages)
        report = _extract_json(resp.content)
        verdict = str(report.get("verdict") or "").lower()
        status = "passed" if verdict == "pass" else "failed"
    except Exception as e:
        logger.error(f"[mcp-security] check failed version={version_id}: {e}")
        report = {
            "verdict": "error",
            "risk_level": "high",
            "summary": f"AI 安全检查执行失败: {e}",
            "issues": [{
                "severity": "high",
                "line": None,
                "type": "review_error",
                "detail": str(e),
                "suggestion": "检查 LLM 配置/网络后重试安全检查。",
            }],
        }
        status = "error"

    updated = await mcp_plugin_store.update_security_result(version_id, status=status, report=report, model=model)
    return updated or {"id": version_id, "security_status": status, "security_report": report}
