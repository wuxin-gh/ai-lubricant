"""脚本型定时任务的执行体与无人值守授权。

定时任务是无人值守场景：``code_run`` 的交互式审批
（:class:`agent.code_run_approval.CodeRunApprovalBatch`）在这里必然拿不到人，
``_code_run_approval is None`` 会让每次调用直接 ``denied``。本模块提供一个鸭子类型
等价的审批对象，把「人在场点确认」换成「人预先授权过这段脚本的哈希」：

- 授权时刻从「每次执行」前移到「创建/启用任务」时，由人调 approve-script 落
  ``approved_hash``；
- 执行时刻只做哈希比对，脚本正文一改（无论 AI 改还是人改）哈希即失配，任务拒跑。

执行体本身**不重写**：仍走 ``ToolRegistry.execute("code_run", ...)``，复用它的
subprocess/超时/输出截断/审计（``agent/tools.py`` 的 ``_code_run``）。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

# 与 CodeRunApprovalBatch._canonical 完全同形态（同键、同默认值、同 sort_keys/separators）。
# 刻意共用：两条审批路径若各自算哈希，日后改了一边就会静默漂移——同一段脚本在交互态
# 和定时态得出不同哈希，授权就会莫名失效。
_DEFAULT_TIMEOUT = 60


def canonical_script(code: str, script_type: str = "python", timeout: int | None = None) -> str:
    """脚本的规范化 JSON 形态，哈希与审批比对的唯一口径。"""
    return json.dumps(
        {
            "type": str(script_type or "python"),
            "code": str(code or ""),
            "timeout": int(timeout or _DEFAULT_TIMEOUT),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def script_hash(code: str, script_type: str = "python", timeout: int | None = None) -> str:
    """sha256(canonical_script)，落库到 approved_hash / pending_script_hash。"""
    return hashlib.sha256(canonical_script(code, script_type, timeout).encode("utf-8")).hexdigest()


def row_script_hash(row: dict) -> str:
    """按任务行当前的 script_code/script_type/script_timeout 算哈希。"""
    return script_hash(
        row.get("script_code") or "",
        row.get("script_type") or "python",
        row.get("script_timeout") or _DEFAULT_TIMEOUT,
    )


def is_script_approved(row: dict) -> bool:
    """approved_hash 存在且与当前脚本正文一致。

    这是哈希锁的判定点：脚本被改动过（AI 自愈写入、人工直接改库、update 端点改字段）
    都会让哈希失配，从而拒跑，逼一次显式的人工授权。
    """
    approved = str(row.get("approved_hash") or "")
    if not approved:
        return False
    return approved == row_script_hash(row)


class ScheduledScriptApproval:
    """无人值守审批：哈希匹配预先授权的 ``approved_hash`` 才放行。

    鸭子类型对齐 :class:`CodeRunApprovalBatch`——``_code_run`` 只需要
    ``decision_for(args) -> (outcome, confirmation_id)``，
    ``_schedule_code_run_audit`` 另外读 ``conversation_id`` / ``caller``。
    满足这三个成员，``_code_run`` 与它的审计一行都不用改。
    """

    def __init__(
        self,
        *,
        approved_hash: str | None,
        job_id: int,
        agent_id: int | None = None,
        caller: str | None = None,
    ) -> None:
        self.approved_hash = str(approved_hash or "")
        self.job_id = int(job_id)
        self.agent_id = agent_id
        self.caller = caller
        # 审计表里能按 conversation_id 反查某个定时任务的全部代码执行记录。
        self.conversation_id = f"scheduled-task:{self.job_id}"

    def decision_for(self, args: dict[str, Any]) -> tuple[str, str | None]:
        """哈希匹配 → allow；否则 fail closed。

        未授权（approved_hash 为空）与被改动（哈希失配）走同一条 deny 分支：对执行侧
        来说两者等价——都不是人授权过的那段代码。
        """
        if not self.approved_hash:
            return "deny", None
        actual = script_hash(
            args.get("code") or "",
            args.get("type") or "python",
            args.get("timeout"),
        )
        if actual != self.approved_hash:
            return "deny", None
        return "allow", f"scheduled-task:{self.job_id}:{actual[:12]}"


async def run_scheduled_script(row: dict, *, agent_id: int | None = None) -> dict:
    """执行任务行里的脚本，返回 ``code_run`` 的原始结果 dict。

    结果形态即 ``_code_run`` 的返回：``status`` / ``stdout`` / ``stderr`` /
    ``exit_code`` / ``duration_ms``；未授权时是 ``status='denied'``,
    ``code='approval_required'``。工作目录是 ``ToolContext.temp_dir()``，与交互态
    ``code_run`` 同一沙箱边界。
    """
    from agent.agent_main import GenericAgent
    from agent.tools import ToolContext, ToolRegistry

    # 复用 GenericAgent 的配置加载：脚本的沙箱边界（allowed_roots/denied_patterns/
    # temp_dir）必须与该 Agent 交互态 code_run 完全一致，不能另起一套默认值。
    agent = GenericAgent(agent_id=agent_id) if agent_id else GenericAgent()
    config = await agent._ensure_config()

    registry = ToolRegistry(ToolContext(config), agent_id=agent_id)
    registry.set_code_run_approval(
        ScheduledScriptApproval(
            approved_hash=row.get("approved_hash"),
            job_id=int(row.get("id") or 0),
            agent_id=agent_id,
        )
    )
    return await registry.execute(
        "code_run",
        {
            "code": row.get("script_code") or "",
            "type": row.get("script_type") or "python",
            "timeout": int(row.get("script_timeout") or _DEFAULT_TIMEOUT),
        },
    )


__all__ = [
    "ScheduledScriptApproval",
    "canonical_script",
    "is_script_approved",
    "row_script_hash",
    "run_scheduled_script",
    "script_hash",
]
