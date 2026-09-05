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
from datetime import datetime, timezone
from typing import Any

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, JobExecutionEvent
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

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


async def _execute_job(job_id: int) -> None:
    """APScheduler 执行入口：按 task_kind 分流（prompt / script），收尾统一。"""
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
        await _execute_script_job(row)
    else:
        await _execute_prompt_job(row)
    logger.info(f"定时任务完成: id={job_id}")


def _scheduled_scene(row: dict):
    """本任务的定时场景（无人值守 + 任务身份）。"""
    from agent import scene_context

    return scene_context.normalize(scheduled={
        "job_id": row.get("id"),
        "name": row.get("name"),
        "cron": row.get("cron_expression"),
    })


async def _scheduled_run(row: dict, prompt: str, *, max_turns: int) -> list:
    """按定时任务的场景/模型跑一轮 Agent。prompt 任务与自愈诊断共用。

    - 场景段（scene）说明「现场没有人」：ask_user 问不到人、结论要自己落地。
    - 模型走 resolve_scheduled_llm：任务级 api_key_id/model > Agent 的 scheduled_* >
      主 Agent，让无人值守跑批能用比交互态更便宜/更稳的模型。
    - system_prompt = Agent 人设 + 场景段。人设此前在定时态整段丢失（调度侧没传，
      run_task 也不会自己去读），这里显式拼好；两者必须一起传，只传场景段会把人设顶掉。
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
    return await agent.run_task(
        prompt, system_prompt=system_prompt, max_turns=max_turns, llm=llm,
    )


async def _execute_prompt_job(row: dict) -> None:
    """prompt 模式：背景块 + task_prompt 交给 GenericAgent 跑（历史行为 + 背景前缀）。"""
    job_id = int(row["id"])
    prompt = f"{_background_block(row)}\n\n{row.get('task_prompt') or ''}".strip()
    result_text = ""
    try:
        result = await _scheduled_run(row, prompt, max_turns=40)
        if result:
            result_text = json.dumps(result[-1], ensure_ascii=False, default=str)[:2000]
        else:
            result_text = "status: completed\nsummary: no output"
    except asyncio.CancelledError:
        result_text = "status: aborted\nsummary: task cancelled"
        raise
    except Exception as e:
        result_text = f"status: failed\nsummary: {type(e).__name__}: {e}"
        logger.error(f"定时任务执行失败: id={job_id} error={e}")
    await _finalize_job(job_id, row.get("cron_expression") or "", result_text)


async def _execute_script_job(row: dict, *, _heal_attempted: bool = False) -> None:
    """script 模式：哈希锁校验 → 执行 → 非零退出码按 on_error 触发自愈。"""
    from agent.scheduled_script import is_script_approved, run_scheduled_script

    job_id = int(row["id"])
    cron = row.get("cron_expression") or ""
    agent_id = row.get("agent_id")
    failures = int(row.get("consecutive_failures") or 0)

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
        logger.warning(f"定时任务 {job_id} 脚本未授权/哈希失配，已禁用等待人工授权")
        return

    try:
        run_result = await run_scheduled_script(row, agent_id=agent_id)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error(f"定时任务脚本执行异常: id={job_id} error={e}")
        await _finalize_job(
            job_id, cron, f"status: failed\nsummary: {type(e).__name__}: {e}",
            last_exit_code=None, consecutive_failures=failures + 1,
        )
        return

    exit_code = run_result.get("exit_code")
    stdout = run_result.get("stdout") or ""
    stderr = run_result.get("stderr") or ""

    # 非零退出码（含超时/denied：exit_code 为 None 也当失败）即触发失败分支。
    if exit_code == 0:
        result_text = f"status: completed\nexit_code: 0\n{stdout[:1800]}".strip()
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
        await _finalize_job(
            job_id, cron, result_text[:2000],
            last_exit_code=exit_code, last_stderr=stderr,
            consecutive_failures=failures + 1,
        )
        return

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
        # 同样的定时模型和同样的 Agent 人设。
        result = await _scheduled_run(row, heal_prompt, max_turns=20)
        if result:
            diagnosis = json.dumps(result[-1], ensure_ascii=False, default=str)[:1500]
    except asyncio.CancelledError:
        raise
    except Exception as e:
        diagnosis = f"heal failed: {type(e).__name__}: {e}"
        logger.error(f"定时任务自愈异常: id={job_id} error={e}")

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
        fresh["consecutive_failures"] = base_failures + 1
        fresh["heal_history"] = history
        await _execute_script_job(fresh, _heal_attempted=True)
        return

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
        asyncio.create_task(_execute_job(job_id))
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
