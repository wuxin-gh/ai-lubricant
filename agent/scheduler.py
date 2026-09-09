"""APScheduler-backed scheduler for GenericAgent 定时任务。

支持两种调度表达方式：
1. cron 表达式（标准 5 位：分 时 日 月 周）
2. repeat 类型：daily / weekday / weekly / monthly / once / every_Nh / every_Nm / every_Nd

APScheduler 负责：触发器解析、到期检测、下次运行时间计算、job 执行调度。
PostgreSQL 表 ``agent_scheduled_tasks`` 保留作为任务元数据和执行结果的存储。
"""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, JobExecutionEvent
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from agent.context_manager import attach_tool_result, _collect_attachment_media
from db import PostgresClient

# repeat 类型 → APScheduler 触发器
REPEAT_TYPES = {"daily", "weekday", "weekly", "monthly", "once", "hourly"}


def _is_repeat_type(expr: str) -> bool:
    """判断表达式是 repeat 类型还是 cron 表达式。"""
    return expr.strip().lower() in REPEAT_TYPES or _is_every_n(expr)


def _is_every_n(expr: str) -> bool:
    e = expr.strip().lower()
    return (
        e.startswith("every_")
        and (e.endswith("h") or e.endswith("m") or e.endswith("d"))
        and e[6:-1].isdigit()
    )


def build_trigger(expr: str):
    """将调度表达式转换为 APScheduler 触发器。

    支持：
    - 标准 cron（5 位）：'0 9 * * 1-5'
    - repeat 关键字：daily/weekday/weekly/monthly/hourly/once
    - every_N：every_30m / every_2h / every_1d
    """
    e = expr.strip().lower()

    if e == "daily":
        return CronTrigger(hour=0, minute=0)
    if e == "weekday":
        return CronTrigger(day_of_week="mon-fri", hour=0, minute=0)
    if e == "weekly":
        return CronTrigger(day_of_week="mon", hour=0, minute=0)
    if e == "monthly":
        return CronTrigger(day=1, hour=0, minute=0)
    if e == "hourly":
        return CronTrigger(minute=0)
    if e == "once":
        # 立即触发一次（5 秒后）
        from datetime import datetime, timedelta
        return DateTrigger(run_date=datetime.now() + timedelta(seconds=5))

    if e.startswith("every_"):
        suffix = e[6:]
        unit = suffix[-1]
        try:
            value = int(suffix[:-1])
        except ValueError:
            raise ValueError(f"Invalid every_N expression: {expr}")
        if unit == "h":
            return IntervalTrigger(hours=value)
        elif unit == "m":
            return IntervalTrigger(minutes=value)
        elif unit == "d":
            return IntervalTrigger(days=value)
        raise ValueError(f"Invalid every_N unit '{unit}': {expr}")

    # 默认当作 cron 表达式
    return CronTrigger.from_crontab(expr)


def _validate_expression(expr: str) -> bool:
    """验证调度表达式是否合法。"""
    try:
        build_trigger(expr)
        return True
    except Exception:
        return False


def _background_block(row: dict) -> str:
    """把「定时的背景」组装成注入块。

    prompt 模式（前缀给 task_prompt）与自愈模式（前缀给 heal prompt）共用同一块，
    保证 AI 两条路拿到的调度上下文口径一致：它是被定时器唤醒的、多久跑一次、上次
    退出码、连续失败几次、以及人/AI 写下的业务背景。
    """
    name = row.get("name") or ""
    job_id = row.get("id")
    lines = ["[Scheduled Context]", f"task: {name} (job_id={job_id})"]
    if row.get("cron_expression"):
        lines.append(f"schedule: {row['cron_expression']}")
    last_run = row.get("last_run_at")
    if last_run is not None:
        exit_code = row.get("last_exit_code")
        tail = f" → exit_code={exit_code}" if exit_code is not None else ""
        lines.append(f"last_run: {last_run}{tail}")
    failures = row.get("consecutive_failures") or 0
    if failures:
        lines.append(f"consecutive_failures: {failures}")
    background = (row.get("background") or "").strip()
    if background:
        lines.append(f"background: {background}")
    return "\n".join(lines)


def _heal_history(row: dict) -> list[dict]:
    """把 heal_history 列（JSONB，可能是 str/list/None）读成 list[dict]。"""
    raw = row.get("heal_history")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            raw = []
    return raw if isinstance(raw, list) else []


async def _start_run(job_id: int, *, task_kind: str, triggered_by: str) -> int | None:
    """开一条执行记录行（status='running'），返回 run_id；DB 不可用时 None。"""
    pool = getattr(PostgresClient, "pool", None)
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO agent_scheduled_task_runs(job_id, task_kind, triggered_by, status) "
                "VALUES($1, $2, $3, 'running') RETURNING id",
                job_id, task_kind, triggered_by,
            )
        return int(row["id"]) if row else None
    except Exception as e:
        logger.warning(f"定时任务 _start_run 写入失败: job_id={job_id} error={e}")
        return None


# _finish_run 只接受这些列；其余 kwargs 忽略（防误传）。
_RUN_FINISH_FIELDS = (
    "status", "exit_code", "stdout", "stderr", "result_text",
    "conversation_id", "duration_ms", "error",
)


async def _finish_run(run_id: int | None, *, status: str, **fields) -> None:
    """收尾一条执行记录：写 status + 任意提供的列。run_id 为 None 时 no-op。

    可被多次调用并合并（幂等）：_scheduled_run 中途写 conversation_id，外层执行器
    结尾再写 status/result_text/duration。大文本写入时截断，防撑爆（与 _heal_script 的
    [:4000] 截断同口径，stdout/stderr 放宽到 16KB 以便排查脚本输出）。
    """
    if run_id is None:
        return
    pool = getattr(PostgresClient, "pool", None)
    if not pool:
        return
    updates: dict[str, Any] = {"status": status}
    for k, v in fields.items():
        if k in _RUN_FINISH_FIELDS and v is not None:
            updates[k] = v
    if updates.get("stdout") is not None:
        updates["stdout"] = str(updates["stdout"])[:16000]
    if updates.get("stderr") is not None:
        updates["stderr"] = str(updates["stderr"])[:16000]
    if updates.get("result_text") is not None:
        updates["result_text"] = str(updates["result_text"])[:2000]
    if updates.get("error") is not None:
        updates["error"] = str(updates["error"])[:2000]
    set_parts = [f"{k}=${i+2}" for i, k in enumerate(updates)]
    values = list(updates.values())
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                f"UPDATE agent_scheduled_task_runs SET {', '.join(set_parts)} WHERE id=$1",
                run_id, *values,
            )
    except Exception as e:
        logger.warning(f"定时任务 _finish_run 更新失败: run_id={run_id} status={status} error={e}")


async def _finalize_job(
    job_id: int, cron_expression: str, result_text: str,
    *, last_exit_code: int | None = None, last_stderr: str | None = None,
    consecutive_failures: int | None = None, heal_history: list | None = None,
) -> None:
    """统一收尾：写 last_result / next_run 及脚本模式的健康度列。"""
    pool = getattr(PostgresClient, "pool", None)
    if not pool:
        return
    next_run = _compute_next_run(cron_expression)
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE agent_scheduled_tasks SET last_run_at=now(), last_result=$2, "
            "next_run_at=$3, last_exit_code=$4, last_stderr=$5, "
            "consecutive_failures=COALESCE($6, consecutive_failures), "
            "heal_history=COALESCE($7::jsonb, heal_history) WHERE id=$1",
            job_id, result_text, next_run, last_exit_code,
            (last_stderr or "")[:4000] if last_stderr is not None else None,
            consecutive_failures,
            json.dumps(heal_history, ensure_ascii=False, default=str) if heal_history is not None else None,
        )


async def _execute_job(job_id: int, *, triggered_by: str = "scheduler") -> None:
    """APScheduler 执行入口：按 task_kind 分流（prompt / script），收尾统一。

    ``triggered_by`` 仅用于落执行记录（scheduler=到点、manual=立即运行）；APScheduler
    注册时只传 job_id，走默认 "scheduler"。
    """
    pool = getattr(PostgresClient, "pool", None)
    if not pool:
        logger.error(f"定时任务 {job_id} 执行失败: 数据库连接池不可用")
        return

    # 读全行：脚本模式要 script_*/approved_hash/on_error 等，背景块要 last_*/failures。
    async with pool.acquire() as conn:
        record = await conn.fetchrow(
            "SELECT * FROM agent_scheduled_tasks WHERE id=$1 AND enabled=true", job_id,
        )
    if not record:
        logger.warning(f"定时任务 {job_id} 不存在或已禁用，跳过执行")
        return

    row = dict(record)
    task_kind = (row.get("task_kind") or "prompt").lower()
    if task_kind == "script":
        await _execute_script_job(row, triggered_by=triggered_by)
    else:
        await _execute_prompt_job(row, triggered_by=triggered_by)
    logger.info(f"定时任务完成: id={job_id}")


def _scheduled_scene(row: dict):
    """本任务的定时场景（无人值守 + 任务身份）。"""
    from agent import scene_context

    return scene_context.normalize(scheduled={
        "job_id": row.get("id"),
        "name": row.get("name"),
        "cron": row.get("cron_expression"),
    })


async def _scheduled_run(
    row: dict, prompt: str, *, max_turns: int,
    run_id: int | None = None, triggered_by: str = "scheduler",
) -> list:
    """按定时任务的场景/模型跑一轮 Agent。prompt 任务与自愈诊断共用。

    - 场景段（scene）说明「现场没有人」：ask_user 问不到人、结论要自己落地。
    - 模型走 resolve_scheduled_llm：任务级 api_key_id/model > Agent 的 scheduled_* >
      主 Agent，让无人值守跑批能用比交互态更便宜/更稳的模型。
    - system_prompt = Agent 人设 + 场景段。人设此前在定时态整段丢失（调度侧没传，
      run_task 也不会自己去读），这里显式拼好；两者必须一起传，只传场景段会把人设顶掉。
    - 对话持久化（run_id 提供时）：镜像 chat 路径的渐进落库——建 kind="scheduled" 会话
      + user/assistant 占位，on_event 累积 content/reasoning/tool_calls/media/usage，
      1.5s 节流写回，结尾 done。ClickHouse 不可用则整段跳过（对话丢失但任务照跑），
      run 行 conversation_id 留空，详情页显示「对话未记录」。
    """
    from agent.agent_main import GenericAgent

    agent_id = row.get("agent_id")
    scene = _scheduled_scene(row)
    agent = (
        GenericAgent(agent_id=agent_id, scene=scene)
        if agent_id else GenericAgent(scene=scene)
    )
    # 先加载配置：下面要读 Agent 人设拼场景段，也让 resolve_scheduled_llm 读到 scheduled_*。
    await agent._ensure_config()
    llm = None
    if agent_id:
        # 无 agent_id 的历史任务没有 Agent 行可读，保持旧行为（默认桥）。
        llm = await agent.resolve_scheduled_llm(
            api_key_id=row.get("api_key_id"),
            model=row.get("model") or "",
        )
    # 场景段与 Agent 人设一起进 system：run_task 只在 system_prompt 为空时才回落到
    # Agent 人设，所以这里要显式把两者拼好再传（否则场景段会顶掉人设）。
    from agent import scene_context

    system_prompt = scene_context.append_prompt(agent._agent_system_prompt, scene)
    effective_model = getattr(llm, "model", "") or ""

    # ── 对话持久化（best-effort；CH 不可用即降级为不落库）──
    conversation_id: str | None = None
    assistant_msg_id: int | None = None
    collected_tool_calls: list[dict] = []
    collected_tool_results: list = []
    collected_media: list[dict] = []
    collected_usage: dict | None = None
    collected_reasoning = ""
    collected_content = ""
    _last_persist_ts = 0.0  # 节流：正文/思考高频到达时最多每 1.5s 落一次中间态。

    from agent import conversation_store

    owner = row.get("user_id")
    owner_str = str(owner) if owner is not None else None
    try:
        conv = await conversation_store.create_conversation(
            title=f"定时任务 {row.get('name')} #{row.get('id')}",
            system_prompt=system_prompt,
            model=effective_model,
            agent_id=agent_id,
            kind="scheduled",
            chat_settings=scene_context.persist(scene),
            user_id=owner_str,
        )
        conversation_id = conv["id"]
        # user 轮是 get_messages_page_by_user_turns 的分页锚点，必须先建。
        await conversation_store.add_message(conversation_id, "user", content=prompt)
        assistant_msg = await conversation_store.add_message(
            conversation_id, "assistant", "", status="streaming",
        )
        assistant_msg_id = assistant_msg["id"]
    except Exception as e:
        # 覆盖 ClickHouseUnavailable 及一切 CH 故障：对话记不下来不能拦住任务执行。
        conversation_id = None
        assistant_msg_id = None
        logger.warning(f"定时任务对话持久化不可用，本轮不记录对话: job={row.get('id')} error={e}")

    async def _persist_progress() -> None:
        """把已累积的内容渐进写回 assistant 消息（status 仍留 streaming）。

        与 chat 路径（api.py _stream_conversation_turn）同口径：审批挂起 / 进程重启 /
        连接中断时，已产生的内容仍在库里，而不是一条空壳。
        """
        nonlocal _last_persist_ts
        if assistant_msg_id is None:
            return
        try:
            await conversation_store.update_message(
                assistant_msg_id,
                content=collected_content,
                tool_calls=collected_tool_calls or None,
                tool_results=collected_tool_results or None,
                media=collected_media or None,
                status="streaming",
                reasoning=collected_reasoning,
            )
            _last_persist_ts = time.monotonic()
        except Exception:
            pass  # 中途落库失败不能中断主流程

    on_event = None
    if assistant_msg_id is not None:

        async def on_event(event: dict) -> None:  # noqa: F811 — 单一定义
            nonlocal collected_usage, collected_reasoning, collected_content, _last_persist_ts
            etype = event.get("type")
            if etype == "tool_call":
                collected_tool_calls.append({
                    # id 是 assistant.tool_calls 与 tool_results 的相关键；历史回放
                    # 靠它还原调用形状，缺了整条调用只能丢弃。
                    "id": event.get("id"),
                    "name": event.get("name"),
                    "args": event.get("args"),
                    "status": "running",
                })
                # 工具调用是天然检查点：立即落库。
                await _persist_progress()
            elif etype == "reasoning":
                collected_reasoning += str(event.get("text") or "")
                if time.monotonic() - _last_persist_ts > 1.5:
                    await _persist_progress()
            elif etype == "content":
                collected_content += str(event.get("text") or "")
                if time.monotonic() - _last_persist_ts > 1.5:
                    await _persist_progress()
            elif etype == "tool_result":
                # 按 call_id 配对（agent_loop 的 index 是「本轮第几个」，跨轮重置，
                # 按 index 配会把第二轮 result 错配到第一轮 call，永久转圈）。
                attach_tool_result(
                    collected_tool_calls, collected_tool_results,
                    call_id=event.get("id"),
                    index=event.get("index"),
                    result=event.get("data"),
                )
                await _collect_attachment_media(event, collected_media, owner_str, conversation_id)
                await _persist_progress()
            elif etype == "question":
                # ask_user 在无人值守场景必然没人答，但问题本身要留在历史里。
                data = event.get("data") if isinstance(event.get("data"), dict) else {}
                q = str(data.get("question") or event.get("message") or "")
                if q:
                    collected_content = f"{collected_content}\n\n{q}".strip("\n")
                await _collect_attachment_media(event, collected_media, owner_str, conversation_id)
                await _persist_progress()
            elif etype == "done" and isinstance(event.get("usage"), dict):
                collected_usage = event["usage"]

    try:
        result = await agent.run_task(
            prompt, system_prompt=system_prompt, max_turns=max_turns, llm=llm,
            on_event=on_event,
        )
    except asyncio.CancelledError:
        if assistant_msg_id is not None:
            try:
                await conversation_store.update_message(
                    assistant_msg_id, status="error", error="已中止",
                    reasoning=collected_reasoning,
                )
            except Exception:
                pass
        raise
    except Exception as e:
        if assistant_msg_id is not None:
            try:
                await conversation_store.update_message(
                    assistant_msg_id, status="error", error=str(e)[:2000],
                    reasoning=collected_reasoning,
                )
            except Exception:
                pass
        raise

    # 正常结束：最终态写回（accumulated content 为准——最后一项 data 只是结束标记）。
    if assistant_msg_id is not None:
        try:
            await conversation_store.update_message(
                assistant_msg_id,
                content=collected_content,
                tool_calls=collected_tool_calls or None,
                tool_results=collected_tool_results or None,
                media=collected_media or None,
                status="done",
                model=effective_model,
                usage=collected_usage,
                reasoning=collected_reasoning,
            )
        except Exception:
            pass  # 收尾落库失败不能掩盖执行结果

    # run 行先记 conversation_id（status/duration 由外层执行器结尾再写）。
    if run_id is not None and conversation_id:
        await _finish_run(run_id, status="running", conversation_id=conversation_id)

    return result


def _prompt_run_status(result: list) -> str:
    """从 agent_runner_loop 的结束标记派生执行记录的 status。

    结束标记集见 agent_loop.py 的 yield：CURRENT_TASK_DONE（正常）/ ERROR /
    MAX_TURNS_EXCEEDED / EXITED。EXITED 是主动退出（goal 模式等）标 aborted；
    MAX_TURNS_EXCEEDED 意味着没做完，标 failed。
    """
    last = result[-1] if result else None
    if isinstance(last, dict):
        r = str(last.get("result") or "")
        if r == "ERROR":
            return "failed"
        if r == "MAX_TURNS_EXCEEDED":
            return "failed"
        if r == "EXITED":
            return "aborted"
    return "completed"


async def _execute_prompt_job(row: dict, *, triggered_by: str = "scheduler") -> None:
    """prompt 模式：背景块 + task_prompt 交给 GenericAgent 跑（历史行为 + 背景前缀）。"""
    job_id = int(row["id"])
    run_id = await _start_run(job_id, task_kind="prompt", triggered_by=triggered_by)
    t0 = time.monotonic()
    prompt = f"{_background_block(row)}\n\n{row.get('task_prompt') or ''}".strip()
    result_text = ""
    try:
        result = await _scheduled_run(row, prompt, max_turns=40, run_id=run_id)
        if result:
            result_text = json.dumps(result[-1], ensure_ascii=False, default=str)[:2000]
        else:
            result_text = "status: completed\nsummary: no output"
        await _finish_run(
            run_id, status=_prompt_run_status(result), result_text=result_text,
            duration_ms=int((time.monotonic() - t0) * 1000),
        )
    except asyncio.CancelledError:
        result_text = "status: aborted\nsummary: task cancelled"
        await _finish_run(
            run_id, status="aborted", result_text=result_text, error="task cancelled",
            duration_ms=int((time.monotonic() - t0) * 1000),
        )
        raise
    except Exception as e:
        result_text = f"status: failed\nsummary: {type(e).__name__}: {e}"
        logger.error(f"定时任务执行失败: id={job_id} error={e}")
        await _finish_run(
            run_id, status="failed", result_text=result_text, error=str(e)[:2000],
            duration_ms=int((time.monotonic() - t0) * 1000),
        )
    await _finalize_job(job_id, row.get("cron_expression") or "", result_text)


async def _execute_script_job(row: dict, *, _heal_attempted: bool = False, triggered_by: str = "scheduler") -> None:
    """script 模式：哈希锁校验 → 执行 → 非零退出码按 on_error 触发自愈。"""
    from agent.scheduled_script import is_script_approved, run_scheduled_script

    job_id = int(row["id"])
    cron = row.get("cron_expression") or ""
    agent_id = row.get("agent_id")
    failures = int(row.get("consecutive_failures") or 0)

    run_id = await _start_run(job_id, task_kind="script", triggered_by=triggered_by)
    t0 = time.monotonic()

    # 哈希锁：未授权或脚本被改动过（AI/人）→ 拒跑并 disable，逼一次显式人工授权。
    if not is_script_approved(row):
        pool = getattr(PostgresClient, "pool", None)
        if pool:
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE agent_scheduled_tasks SET last_run_at=now(), "
                    "last_result=$2, enabled=false WHERE id=$1",
                    job_id, "status: blocked\nsummary: script not approved (approve-script required)",
                )
        await _finish_run(
            run_id, status="blocked",
            result_text="status: blocked\nsummary: script not approved (approve-script required)",
            duration_ms=int((time.monotonic() - t0) * 1000),
        )
        logger.warning(f"定时任务 {job_id} 脚本未授权/哈希失配，已禁用等待人工授权")
        return

    try:
        run_result = await run_scheduled_script(row, agent_id=agent_id)
    except asyncio.CancelledError:
        await _finish_run(
            run_id, status="aborted", error="task cancelled",
            duration_ms=int((time.monotonic() - t0) * 1000),
        )
        raise
    except Exception as e:
        logger.error(f"定时任务脚本执行异常: id={job_id} error={e}")
        result_text = f"status: failed\nsummary: {type(e).__name__}: {e}"
        await _finish_run(
            run_id, status="failed", result_text=result_text, error=str(e)[:2000],
            duration_ms=int((time.monotonic() - t0) * 1000),
        )
        await _finalize_job(
            job_id, cron, result_text,
            last_exit_code=None, consecutive_failures=failures + 1,
        )
        return

    exit_code = run_result.get("exit_code")
    stdout = run_result.get("stdout") or ""
    stderr = run_result.get("stderr") or ""

    # 非零退出码（含超时/denied：exit_code 为 None 也当失败）即触发失败分支。
    if exit_code == 0:
        result_text = f"status: completed\nexit_code: 0\n{stdout[:1800]}".strip()
        await _finish_run(
            run_id, status="completed", exit_code=0,
            stdout=stdout, stderr="", result_text=result_text[:2000],
            duration_ms=run_result.get("duration_ms") or int((time.monotonic() - t0) * 1000),
        )
        await _finalize_job(
            job_id, cron, result_text[:2000],
            last_exit_code=0, last_stderr="", consecutive_failures=0,
        )
        return

    result_text = (
        f"status: failed\nexit_code: {exit_code}\n"
        f"stderr: {stderr[:1600]}"
    ).strip()
    on_error = (row.get("on_error") or "diagnose").lower()

    if _heal_attempted or on_error == "none":
        # 二次失败不再自愈；on_error=none 记录即止。
        await _finish_run(
            run_id, status="failed", exit_code=exit_code,
            stdout=stdout, stderr=stderr, result_text=result_text[:2000],
            duration_ms=run_result.get("duration_ms") or int((time.monotonic() - t0) * 1000),
        )
        await _finalize_job(
            job_id, cron, result_text[:2000],
            last_exit_code=exit_code, last_stderr=stderr,
            consecutive_failures=failures + 1,
        )
        return

    # 即将进入自愈：先把本轮脚本失败落到 run 行（自愈诊断会另起一行 triggered_by=heal）。
    await _finish_run(
        run_id, status="failed", exit_code=exit_code,
        stdout=stdout, stderr=stderr, result_text=result_text[:2000],
        duration_ms=run_result.get("duration_ms") or int((time.monotonic() - t0) * 1000),
    )
    await _heal_script(row, run_result, base_failures=failures)


async def _heal_script(row: dict, run_result: dict, *, base_failures: int) -> None:
    """脚本报错后拉 AI 进来诊断（可选改脚本 + 重跑一次）。

    一次调度最多一轮自愈 + 最多一次重跑：AI 通过 capability_call
    ``scheduler.propose_script_fix`` 回传修复，是否即时生效由任务的
    ``allow_ai_script_fix`` 决定（见 _scheduler_dispatch）。这里不做循环，避免 AI
    反复改反复跑烧 token。
    """
    job_id = int(row["id"])
    cron = row.get("cron_expression") or ""
    on_error = (row.get("on_error") or "diagnose").lower()
    exit_code = run_result.get("exit_code")
    stdout = (run_result.get("stdout") or "")[:4000]
    stderr = (run_result.get("stderr") or "")[:4000]

    # 自愈诊断单独记一条执行记录（triggered_by=heal），与脚本失败那条分开，
    # 这样列表里能清楚看到「这次跑挂了 → 紧接着 AI 自愈了一轮」。
    heal_run_id = await _start_run(job_id, task_kind="prompt", triggered_by="heal")
    heal_t0 = time.monotonic()

    history = _heal_history(row)
    diagnosis = ""
    try:
        if on_error == "diagnose_fix_retry":
            fix_instruction = (
                "你的职责是诊断根因，并在确有把握时通过 "
                'capability_call(name="scheduler.propose_script_fix", '
                'args={"job_id": %d, "script_code": "<修好的完整脚本>", '
                '"reason": "<改动说明>"}) 提交修复。' % job_id
            )
        else:
            fix_instruction = "你的职责是诊断根因（仅诊断，不修改脚本）。"
        heal_prompt = (
            f"{_background_block(row)}\n\n"
            "[Scheduled Script Failed]\n"
            f"这是一个定时脚本任务，本轮执行失败。{fix_instruction}\n"
            f"script_type: {row.get('script_type') or 'python'}\n"
            f"exit_code: {exit_code}\n\n"
            f"--- script_code ---\n{row.get('script_code') or ''}\n\n"
            f"--- stdout ---\n{stdout}\n\n"
            f"--- stderr ---\n{stderr}\n"
        )
        # 自愈诊断同样是无人值守：与 prompt 任务共用一条路，拿到同样的场景段、
        # 同样的定时模型和同样的 Agent 人设。run_id 让本轮诊断对话也落库可回放。
        result = await _scheduled_run(
            row, heal_prompt, max_turns=20, run_id=heal_run_id, triggered_by="heal",
        )
        if result:
            diagnosis = json.dumps(result[-1], ensure_ascii=False, default=str)[:1500]
    except asyncio.CancelledError:
        await _finish_run(
            heal_run_id, status="aborted", error="task cancelled",
            duration_ms=int((time.monotonic() - heal_t0) * 1000),
        )
        raise
    except Exception as e:
        diagnosis = f"heal failed: {type(e).__name__}: {e}"
        logger.error(f"定时任务自愈异常: id={job_id} error={e}")
        await _finish_run(
            heal_run_id, status="failed", result_text=diagnosis[:2000], error=str(e)[:2000],
            duration_ms=int((time.monotonic() - heal_t0) * 1000),
        )

    # 重新读行：propose_script_fix 可能已把新脚本写入 script_code + approved_hash。
    pool = getattr(PostgresClient, "pool", None)
    fresh = row
    if pool:
        async with pool.acquire() as conn:
            record = await conn.fetchrow("SELECT * FROM agent_scheduled_tasks WHERE id=$1", job_id)
        if record:
            fresh = dict(record)

    applied = (fresh.get("approved_hash") or "") != (row.get("approved_hash") or "") or \
              (fresh.get("script_code") or "") != (row.get("script_code") or "")
    from datetime import datetime, timezone
    history.append({
        "at": datetime.now(timezone.utc).isoformat(),
        "exit_code": exit_code,
        "diagnosis": diagnosis[:800],
        "applied": bool(applied and fresh.get("enabled")),
    })

    # 自动重跑：仅当 diagnose_fix_retry + 修复已即时生效（allow_ai_script_fix=true 路径，
    # script_code 已换且仍 enabled）。否则记录诊断，等下一周期或人工授权。
    if on_error == "diagnose_fix_retry" and applied and fresh.get("enabled") and is_script_approved(fresh):
        # 自愈成功 + 即将重跑：本轮自愈诊断记 completed，重跑会另开一条 run 行。
        await _finish_run(
            heal_run_id, status="completed", result_text=diagnosis[:2000],
            duration_ms=int((time.monotonic() - heal_t0) * 1000),
        )
        fresh["consecutive_failures"] = base_failures + 1
        fresh["heal_history"] = history
        await _execute_script_job(fresh, _heal_attempted=True, triggered_by=triggered_by)
        return

    heal_status = "completed" if diagnosis and not diagnosis.startswith("heal failed") else "failed"
    await _finish_run(
        heal_run_id, status=heal_status, result_text=diagnosis[:2000],
        duration_ms=int((time.monotonic() - heal_t0) * 1000),
    )
    await _finalize_job(
        job_id, cron,
        f"status: failed\nexit_code: {exit_code}\ndiagnosis: {diagnosis[:1200]}"[:2000],
        last_exit_code=exit_code, last_stderr=stderr,
        consecutive_failures=base_failures + 1, heal_history=history,
    )


def _compute_next_run(expr: str):
    """根据调度表达式计算下次运行时间。"""
    try:
        trigger = build_trigger(expr)
        now = datetime.now(timezone.utc)
        if isinstance(trigger, DateTrigger):
            return None  # 一次性任务无下次
        next_fire = trigger.get_next_fire_time(None, now)
        return next_fire.replace(tzinfo=None) if next_fire else None
    except Exception:
        return None


def _job_listener(event: JobExecutionEvent) -> None:
    if event.exception:
        logger.error(f"定时任务异常: job_id={event.job_id} error={event.exception}")
    else:
        logger.info(f"定时任务执行成功: job_id={event.job_id}")


class AgentScheduler:
    """APScheduler 封装层。

    - APScheduler 负责调度（cron / repeat / interval 触发器解析、到期检测、执行）
    - PostgreSQL 表 ``agent_scheduled_tasks`` 负责元数据持久化
    - 启动时从 DB 重新加载所有 enabled 任务到 APScheduler
    """

    def __init__(self) -> None:
        self._scheduler = AsyncIOScheduler(job_defaults={"coalesce": True, "max_instances": 1})
        self._scheduler.add_listener(_job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    def start(self) -> None:
        """启动 APScheduler 并从 DB 重新加载任务。"""
        if self._scheduler.running:
            return
        self._scheduler.start()
        # 修复：使用 get_running_loop 而非已弃用的 get_event_loop
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._reload_jobs_from_db())
        except RuntimeError:
            # 没有运行中的事件循环，延迟加载
            asyncio.ensure_future(self._reload_jobs_from_db())
        # 执行记录保留清理：启动清一次历史超额行，之后每天一次（每任务最近 200 条）。
        try:
            asyncio.get_running_loop().create_task(self._cleanup_old_runs())
            self._scheduler.add_job(
                self._cleanup_old_runs,
                trigger=IntervalTrigger(days=1),
                id="scheduled-runs-cleanup",
                replace_existing=True,
            )
        except RuntimeError:
            asyncio.ensure_future(self._cleanup_old_runs())
        logger.info("AgentScheduler 启动")

    def shutdown(self, wait: bool = True) -> None:
        if self._scheduler.running:
            self._scheduler.shutdown(wait=wait)
            logger.info("AgentScheduler 已停止")

    async def _reload_jobs_from_db(self) -> None:
        pool = getattr(PostgresClient, "pool", None)
        if not pool:
            return
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT id, cron_expression FROM agent_scheduled_tasks WHERE enabled=true"
                )
            for row in rows:
                try:
                    trigger = build_trigger(row["cron_expression"])
                    self._scheduler.add_job(
                        _execute_job, trigger=trigger,
                        id=str(row["id"]),
                        kwargs={"job_id": int(row["id"])},
                        replace_existing=True,
                    )
                except Exception as e:
                    logger.error(f"加载定时任务 {row['id']} 失败: {e}")
            logger.info(f"从 DB 加载了 {len(rows)} 个定时任务")
        except Exception as e:
            logger.error(f"加载定时任务失败: {e}")

    # ------------------------------------------------------------------
    # 任务管理
    # ------------------------------------------------------------------

    async def add_job(
        self,
        name: str,
        cron_expression: str,
        task_prompt: str | None = None,
        skill_id: int | None = None,
        enabled: bool = True,
        agent_id: int | None = None,
        user_id: str | None = None,
        *,
        task_kind: str = "prompt",
        script_code: str | None = None,
        script_type: str = "python",
        script_timeout: int = 300,
        background: str | None = None,
        on_error: str = "diagnose",
        allow_ai_script_fix: bool = False,
        approved_hash: str | None = None,
        api_key_id: int | None = None,
        model: str | None = None,
    ) -> int:
        """创建定时任务。cron_expression 支持 cron / repeat 关键字 / every_N。返回 job_id。

        ``task_kind='script'`` 落 script_code 但**不写 approved_hash**（除调用方显式传入，
        即 REST 的人工授权路径）：脚本必须经人 approve-script 才跑得起来，AI 不能自我授权。
        """
        pool = getattr(PostgresClient, "pool", None)
        if not pool:
            return 0

        # 校验表达式
        if not _validate_expression(cron_expression):
            logger.error(f"无效的调度表达式: {cron_expression}")
            return 0

        kind = (task_kind or "prompt").lower()
        if kind not in {"prompt", "script"}:
            logger.error(f"无效的 task_kind: {task_kind}")
            return 0
        if kind == "prompt" and not (task_prompt or "").strip():
            logger.error("task_kind=prompt 需要 task_prompt")
            return 0
        if kind == "script" and not (script_code or "").strip():
            logger.error("task_kind=script 需要 script_code")
            return 0

        # 归属兜底：AI 经 capability_call 建任务时没有 C 端 caller，只有 agent_id。
        # 不补 user_id 的话落库是 NULL，而用户态列表会把 user_id IS NULL 的行过滤掉
        # （视为平台/管理员行），结果「AI 建的任务用户自己看不到」。任务归属跟着
        # Agent 归属走：查 agents.user_id 补上。平台 Agent（owner NULL）仍为 NULL。
        if user_id is None and agent_id is not None:
            try:
                async with pool.acquire() as conn:
                    owner_row = await conn.fetchrow("SELECT user_id FROM agents WHERE id=$1", agent_id)
                if owner_row and owner_row["user_id"]:
                    user_id = str(owner_row["user_id"])
            except Exception as e:
                logger.warning(f"add_job 解析 Agent {agent_id} 归属失败: {e}")

        # 写入 DB
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """INSERT INTO agent_scheduled_tasks(name, cron_expression, task_prompt, skill_id, enabled, agent_id, user_id,
                   task_kind, script_code, script_type, script_timeout, background, on_error, allow_ai_script_fix, approved_hash,
                   api_key_id, model)
                   VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) RETURNING id""",
                name, cron_expression, task_prompt, skill_id, enabled, agent_id, user_id,
                kind, script_code, script_type, int(script_timeout or 300),
                background, (on_error or "diagnose").lower(), bool(allow_ai_script_fix), approved_hash,
                api_key_id, (model or "").strip() or None,
            )
        job_id = int(row["id"]) if row else 0
        if not job_id:
            return 0

        # 注册到 APScheduler
        if enabled:
            try:
                trigger = build_trigger(cron_expression)
                self._scheduler.add_job(
                    _execute_job, trigger=trigger,
                    id=str(job_id),
                    kwargs={"job_id": job_id},
                )
            except Exception as e:
                logger.error(f"注册 APScheduler job 失败: id={job_id} error={e}")
                async with pool.acquire() as conn:
                    await conn.execute("DELETE FROM agent_scheduled_tasks WHERE id=$1", job_id)
                return 0

        return job_id

    async def cancel_job(self, job_id: int) -> bool:
        """禁用任务（DB 标记 + 从 APScheduler 移除）。"""
        pool = getattr(PostgresClient, "pool", None)
        if not pool:
            return False
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE agent_scheduled_tasks SET enabled=false WHERE id=$1 AND enabled=true RETURNING id",
                job_id,
            )
        if row:
            try:
                self._scheduler.remove_job(str(job_id))
            except Exception:
                pass
            return True
        return False

    async def get_jobs(self, enabled: bool | None = None, user_id: str | None = None) -> list[dict]:
        pool = getattr(PostgresClient, "pool", None)
        if not pool:
            return []
        # script_code / pending_script_code 体积大，不进列表；只给哈希/背景/健康度，
        # 明细走单条 SELECT *（REST get_scheduled_task）。has_pending_fix 由 pending 哈希派生。
        fields = (
            "id, name, cron_expression, task_prompt, skill_id, enabled, "
            "last_run_at, next_run_at, last_result, created_at, user_id, agent_id, "
            "task_kind, script_type, script_timeout, background, on_error, "
            "allow_ai_script_fix, approved_hash, pending_script_hash, "
            "last_exit_code, consecutive_failures, api_key_id, model"
        )
        clauses: list[str] = []
        params: list[Any] = []
        if enabled is not None:
            clauses.append("enabled=$1")
            params.append(enabled)
        if user_id is not None:
            clauses.append("user_id=$%d" % (len(params) + 1))
            params.append(user_id)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"SELECT {fields} FROM agent_scheduled_tasks {where} ORDER BY created_at DESC"
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        result = []
        for row in rows:
            job = dict(row)
            job["task_kind"] = job.get("task_kind") or "prompt"
            job["has_pending_fix"] = bool(job.get("pending_script_hash"))
            for key in ("last_run_at", "next_run_at", "created_at"):
                if job.get(key) is not None:
                    job[key] = str(job[key])
            result.append(job)
        return result

    async def get_enabled_jobs(self) -> list[dict]:
        return await self.get_jobs(enabled=True)

    async def job_owned_by(self, job_id: int, user_id: str) -> bool:
        """定时任务归属校验：user_id 匹配（管理员路径不走此校验）。"""
        pool = getattr(PostgresClient, "pool", None)
        if not pool:
            return False
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT user_id FROM agent_scheduled_tasks WHERE id=$1", job_id,
            )
        if not row:
            return False
        owner = row["user_id"]
        return owner is None or str(owner) == str(user_id)

    async def run_now(self, job_id: int, user_id: str | None = None) -> bool:
        """立即触发执行一个任务。user_id 非 None 时校验归属。"""
        pool = getattr(PostgresClient, "pool", None)
        if not pool:
            return False
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id FROM agent_scheduled_tasks WHERE id=$1 AND enabled=true", job_id,
            )
        if not row:
            return False
        if user_id is not None and not await self.job_owned_by(job_id, user_id):
            return False
        asyncio.create_task(_execute_job(job_id, triggered_by="manual"))
        return True

    async def get_task(self, job_id: int) -> dict | None:
        """单条任务全字段（含 script_code / pending_script_code）——列表故意不带这些大字段。"""
        pool = getattr(PostgresClient, "pool", None)
        if not pool:
            return None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM agent_scheduled_tasks WHERE id=$1", job_id)
        if not row:
            return None
        job = dict(row)
        job["task_kind"] = job.get("task_kind") or "prompt"
        job["has_pending_fix"] = bool(job.get("pending_script_hash"))
        for key in ("last_run_at", "next_run_at", "created_at"):
            if job.get(key) is not None:
                job[key] = str(job[key])
        heal = job.get("heal_history")
        if isinstance(heal, str):
            try:
                job["heal_history"] = json.loads(heal)
            except (TypeError, ValueError):
                job["heal_history"] = []
        return job

    # ------------------------------------------------------------------
    # 执行记录（agent_scheduled_task_runs）
    # ------------------------------------------------------------------

    async def list_runs(self, job_id: int, *, limit: int = 50, cursor: int | None = None) -> list[dict]:
        """某任务的执行记录列表，按 run_at 倒序。cursor = 上一页最早的 id（取更老的）。"""
        pool = getattr(PostgresClient, "pool", None)
        if not pool:
            return []
        page = max(1, min(int(limit), 100))
        params: list[Any] = [job_id, page]
        cursor_clause = ""
        if cursor is not None:
            params.append(int(cursor))
            cursor_clause = f" AND id < ${len(params)}"
        sql = (
            "SELECT id, run_at, task_kind, triggered_by, status, exit_code, "
            "duration_ms, conversation_id, "
            "CASE WHEN result_text IS NULL THEN NULL ELSE substring(result_text, 1, 200) END AS result_snippet "
            "FROM agent_scheduled_task_runs WHERE job_id=$1" + cursor_clause +
            f" ORDER BY run_at DESC LIMIT $2"
        )
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        result = []
        for row in rows:
            d = dict(row)
            if d.get("run_at") is not None:
                d["run_at"] = str(d["run_at"])
            result.append(d)
        return result

    async def get_run(self, run_id: int) -> dict | None:
        """单条执行记录全字段（含 stdout/stderr/conversation_id）。归属由调用方先校验。"""
        pool = getattr(PostgresClient, "pool", None)
        if not pool:
            return None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM agent_scheduled_task_runs WHERE id=$1", run_id)
        if not row:
            return None
        d = dict(row)
        for k in ("run_at", "created_at"):
            if d.get(k) is not None:
                d[k] = str(d[k])
        return d

    async def _cleanup_old_runs(self) -> None:
        """每任务保留最近 200 条执行记录，删更老的。启动跑一次 + 每日一次。

        不在 _finish_run 里顺手清（每次执行都跑窗口函数太贵），日级足够。
        """
        pool = getattr(PostgresClient, "pool", None)
        if not pool:
            return
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM agent_scheduled_task_runs WHERE id IN ("
                    " SELECT id FROM ("
                    "  SELECT id, row_number() OVER (PARTITION BY job_id ORDER BY run_at DESC) AS rn"
                    "  FROM agent_scheduled_task_runs"
                    " ) t WHERE rn > 200)"
                )
        except Exception as e:
            logger.warning(f"清理定时任务执行记录失败: {e}")

    async def approve_script(self, job_id: int) -> bool:
        """人工授权脚本：把当前 script_code（或 pending_script_code）提升为授权态。

        - 有 pending_script_code → 提升为 script_code、清 pending；
        - 否则对现有 script_code 直接授权。
        随后写 approved_hash（哈希锁真相源）并重新 enable + 注册回 APScheduler。
        这是**唯一**能写 approved_hash 的人工入口，AI 无从触达。
        """
        from agent.scheduled_script import script_hash

        pool = getattr(PostgresClient, "pool", None)
        if not pool:
            return False
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM agent_scheduled_tasks WHERE id=$1", job_id)
            if not row or (row["task_kind"] or "prompt") != "script":
                return False
            r = dict(row)
            code = r.get("pending_script_code") or r.get("script_code") or ""
            if not code.strip():
                return False
            new_hash = script_hash(code, r.get("script_type") or "python", r.get("script_timeout") or 300)
            await conn.execute(
                "UPDATE agent_scheduled_tasks SET script_code=$2, approved_hash=$3, "
                "pending_script_code=NULL, pending_script_hash=NULL, enabled=true WHERE id=$1",
                job_id, code, new_hash,
            )
        # 重新注册到 APScheduler（授权后任务应能被调度）。
        try:
            trigger = build_trigger(r.get("cron_expression") or "")
            self._scheduler.add_job(
                _execute_job, trigger=trigger, id=str(job_id),
                kwargs={"job_id": job_id}, replace_existing=True,
            )
        except Exception as e:
            logger.error(f"approve_script 重新注册 job 失败: id={job_id} error={e}")
        return True

    async def propose_script_fix(
        self, job_id: int, script_code: str, reason: str = "",
    ) -> dict:
        """AI 自愈回传的脚本修复入口（经 capability_call）。

        授权决定在**创建任务时**由人拍板（allow_ai_script_fix）：
        - true → 新脚本直接写入 script_code + approved_hash，即时生效，可自动重跑；
        - false → 只落 pending_script_code/hash 并 disable，等人 approve-script。
        AI 走这条路都无法给「未开 allow_ai_script_fix 的任务」自我授权。
        """
        from agent.scheduled_script import script_hash

        pool = getattr(PostgresClient, "pool", None)
        if not pool:
            return {"status": "error", "msg": "数据库不可用"}
        code = (script_code or "").strip()
        if not code:
            return {"status": "error", "msg": "script_code 不能为空"}
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM agent_scheduled_tasks WHERE id=$1", job_id)
            if not row or (row["task_kind"] or "prompt") != "script":
                return {"status": "error", "msg": f"脚本任务 {job_id} 不存在"}
            r = dict(row)
            new_hash = script_hash(code, r.get("script_type") or "python", r.get("script_timeout") or 300)
            if r.get("allow_ai_script_fix"):
                await conn.execute(
                    "UPDATE agent_scheduled_tasks SET script_code=$2, approved_hash=$3, "
                    "pending_script_code=NULL, pending_script_hash=NULL WHERE id=$1",
                    job_id, code, new_hash,
                )
                logger.info(f"定时任务 {job_id} AI 修复已即时生效 (allow_ai_script_fix=true) reason={reason}")
                return {"status": "ok", "applied": True}
            await conn.execute(
                "UPDATE agent_scheduled_tasks SET pending_script_code=$2, "
                "pending_script_hash=$3, enabled=false WHERE id=$1",
                job_id, code, new_hash,
            )
            try:
                self._scheduler.remove_job(str(job_id))
            except Exception:
                pass
        logger.info(f"定时任务 {job_id} AI 修复待人工授权 (approve-script) reason={reason}")
        return {"status": "ok", "applied": False, "msg": "修复已提交，待人工 approve-script 授权"}
