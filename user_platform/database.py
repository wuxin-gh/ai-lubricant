"""Isolated Tortoise ORM bootstrap for the compatibility layer.

This connection is fully separate from the main asyncpg ``PostgresClient`` pool
and must only host compatibility tables (``mc_*``). It never runs against the
main service tables and never runs Aerich against the main database.
"""
from __future__ import annotations

from loguru import logger

from .config import settings

# Compatibility models live in their own namespace to avoid clashing with the
# main service tables. Only additive ``mc_*`` tables are generated here.
MODEL_MODULES = [
    "user_platform.models",
    "user_platform.models_task",
    "user_platform.models_project",
    "user_platform.models_git",
    "user_platform.models_skill",
    "user_platform.models_notify",
    "user_platform.models_team_admin",
    "user_platform.models_resources",
    "user_platform.models_webhook",
    "user_platform.models_webhook_event",
    "user_platform.models_review",
    "user_platform.models_tunnel",
    "user_platform.models_environment",
    # The node ledger (mc_ac_nodes / mc_ac_node_sessions) is owned by the
    # separate node control service (``node_server.store``) and is deliberately
    # NOT registered here: the data process must not create or migrate it.
]
APP_LABEL = "user_platform"

_TORTOISE_CONFIG = {
    "connections": {"user_platform": settings.database_url},
    "apps": {
        APP_LABEL: {
            "models": MODEL_MODULES,
            "default_connection": "user_platform",
        }
    },
}


async def init_database() -> None:
    """Initialize the isolated Tortoise connection and create ``mc_*`` tables."""
    from tortoise import Tortoise

    config = dict(_TORTOISE_CONFIG)
    config["connections"] = {"user_platform": settings.database_url}
    # _enable_global_fallback: FastAPI runs lifespan (where init() runs) in a
    # background task, but request handlers run in a different task with no
    # ContextVar link. The global fallback lets the compat layers' Tortoise
    # queries find the connection across that task boundary.
    # use_tz=False：历史上 mc_* 表的 datetime 列建为朴素 TIMESTAMP（当年 Tortoise
    # use_tz 默认 False）。新版 Tortoise use_tz 默认 True 会生成带时区 datetime，写进
    # 朴素列时 asyncpg 报 "can't subtract offset-naive and offset-aware"。显式关掉，
    # 让 ORM 生成朴素 UTC datetime 与既有列匹配；读出的朴素值各 service 的 _unix()
    # 已按 UTC 兜底，语义一致。
    await Tortoise.init(config=config, use_tz=False, _enable_global_fallback=True)
    # Drop the retired Agent↔Skill binding table before schema creation. Its
    # model is intentionally no longer registered, so the table would otherwise
    # survive forever on upgraded installations. The Skill catalog stays.
    await _drop_retired_agent_skill_storage()
    # Tortoise's generate_schemas never ALTERs existing tables, so columns added
    # to a model after the table was first created are missing. generate_schemas
    # would then abort the whole script on "column ... does not exist" when it
    # tries to build an index over such a column on a pre-existing table.
    # Reconcile additive columns FIRST, then create schemas.
    await _ensure_additive_columns()
    # Only creates tables that do not yet exist; safe to call repeatedly.
    await Tortoise.generate_schemas(safe=True)
    # Must run AFTER generate_schemas too: on a fresh database the table did not
    # exist during additive reconciliation, so only now can the partial unique
    # index be created. Existing DBs are idempotent via IF NOT EXISTS.
    await _ensure_task_event_message_index()
    await _ensure_task_event_seq_sequence()
    await _migrate_system_env_identity()
    await _migrate_project_issue_workflow()
    await _migrate_task_bootstrap()
    await _migrate_tunnel_runtime()
    logger.debug("[user-platform] tortoise schemas ready")


async def _drop_retired_agent_skill_storage() -> None:
    """Drop the retired Agent↔Skill binding table and its data.

    GenericAgent resource delivery goes through AgentSop and never installs
    Skills, so ``mc_agent_skill_bindings`` is dead storage. The Skill catalog
    itself (``mc_agent_skills`` / ``_versions`` / ``_repos``) stays: it powers
    the resource center's Skill management. Runs after Tortoise.init (needs the
    connection) but before generate_schemas so the table is never recreated.
    The guard query keeps fresh databases from paying a DROP on every boot;
    existing installs are cleaned exactly once. Idempotent via IF EXISTS.
    """
    from tortoise import Tortoise

    conn = Tortoise.get_connection("user_platform")
    rows = await conn.execute_query_dict(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_name = 'mc_agent_skill_bindings'"
    )
    if not rows:
        return
    await conn.execute_script("DROP TABLE IF EXISTS mc_agent_skill_bindings CASCADE;")
    logger.info("[user-platform] dropped retired table mc_agent_skill_bindings")


async def _ensure_additive_columns() -> None:
    """Reconcile every compat table with its model, adding any missing column.

    Tortoise ``generate_schemas(safe=True)`` only creates missing *tables*; it
    never ALTERs an existing table to append columns added to the model after
    the table was first created. That mismatch surfaces as ``OperationalError:
    column "..." does not exist`` the first time we query the new field
    (``last_active_at``, ``updated_at`` etc.).

    This walks every model registered in the compat app, compares its declared
    DB columns against ``information_schema.columns`` for that table, and runs a
    plain ``ALTER TABLE ... ADD COLUMN`` for each absent column. It is:

    * Generic — no hardcoded column list, so newly added model fields are
      picked up automatically on the next startup.
    * Additive only — never drops, renames, or retypes an existing column.
    * Safe on populated tables — every added column is made NULLable regardless
      of the model's ``null`` flag, because ``ADD COLUMN ... NOT NULL`` without a
      default fails when rows already exist. Tortoise still supplies values on
      insert; reads simply stop 500-ing. Idempotent across restarts.
    """
    from tortoise import Tortoise

    conn = Tortoise.get_connection("user_platform")
    # Tortoise.apps is an ``Apps`` object (not a plain dict); iterate via
    # ``.items()`` and pick our compat app's model map.
    app_models: dict = {}
    for label, model_map in Tortoise.apps.items():
        if label == APP_LABEL:
            app_models = model_map
            break
    for model in app_models.values():
        meta = model._meta
        table = meta.db_table
        # Skip if the table itself does not exist yet (generate_schemas will
        # have created it; a missing table here means a transient race — leave
        # it to the next startup rather than ALTERing a non-existent table).
        table_rows = await conn.execute_query_dict(
            "SELECT column_name FROM information_schema.columns WHERE table_name = $1",
            [table],
        )
        if not table_rows:
            continue
        existing = {row["column_name"] for row in table_rows}
        # fields_db_projection maps model field name -> actual DB column name;
        # this excludes reverse relations and other non-column pseudo-fields.
        for field_name, column in meta.fields_db_projection.items():
            if column in existing:
                continue
            field = meta.fields_map[field_name]
            sql_type = getattr(field, "SQL_TYPE", None)
            if not sql_type:
                # Unknown/relational field with no direct column type — skip.
                continue
            await conn.execute_script(
                f'ALTER TABLE "{table}" ADD COLUMN "{column}" {sql_type} NULL;'
            )
            logger.info(
                "[user-platform] added missing column {}.{} ({})",
                table,
                column,
                sql_type,
            )


async def _ensure_task_event_message_index() -> None:
    """Add the (task_id, client_message_id) partial unique index for idempotent sends.

    Column reconciliation runs first, so ``client_message_id`` exists by the time
    this runs. The partial ``WHERE client_message_id IS NOT NULL`` keeps legacy
    rows (which never carried an id) outside the constraint, so backfilling the
    index onto a populated table cannot fail on duplicates. Idempotent via
    ``IF NOT EXISTS``.
    """
    from tortoise import Tortoise

    conn = Tortoise.get_connection("user_platform")
    rows = await conn.execute_query_dict(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'mc_task_events'"
    )
    if not rows:
        return  # table not created yet; generate_schemas will build it with the model's indexes
    columns = {row["column_name"] for row in rows}
    if "client_message_id" not in columns:
        return
    await conn.execute_script(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_mc_task_events_client_message_id "
        "ON mc_task_events (task_id, client_message_id) "
        "WHERE client_message_id IS NOT NULL;"
    )


async def _migrate_system_env_identity() -> None:
    """Widen the system-env snapshot's unique key to include ``provider``.

    The node reports MCP entries keyed (provider, name) on purpose — the same
    server name in two editors' configs is two capabilities, because each
    runtime loads its own definition — but the table's old constraint was
    UNIQUE (node_id, kind, name). An operator with e.g. a mis-nested
    ``mcpServers`` key in two editors' configs therefore failed refresh with a
    duplicate-key 500. Swap the stale 3-column constraint for the model's
    4-column one. No cleanup pass is needed: rows unique on the narrower key
    are trivially unique on the wider one. Idempotent by matching constraint
    *definitions*, not names — Tortoise generates a different hash per field
    set, and fresh databases already get the 4-column one from the model.
    """
    from tortoise import Tortoise

    conn = Tortoise.get_connection("user_platform")
    table = "mc_node_system_env_entries"
    exists = await conn.execute_query_dict(
        "SELECT 1 FROM information_schema.tables WHERE table_name = $1", [table]
    )
    if not exists:
        return  # table not created yet; generate_schemas will build it with the model's key
    constraints = await conn.execute_query_dict(
        "SELECT conname AS name, pg_get_constraintdef(oid) AS def "
        "FROM pg_constraint "
        f"WHERE conrelid = to_regclass('{table}') AND contype = 'u'"
    )

    def columns(defn: str) -> set[str]:
        inner = defn[defn.find("(") + 1 : defn.rfind(")")]
        return {c.strip().strip('"') for c in inner.split(",")}

    for row in constraints:
        if columns(row["def"]) == {"node_id", "kind", "name"}:
            await conn.execute_script(f'ALTER TABLE {table} DROP CONSTRAINT "{row["name"]}";')
            logger.info(
                "[user-platform] dropped stale system-env unique constraint {}",
                row["name"],
            )
    if not any(
        columns(row["def"]) == {"node_id", "kind", "provider", "name"}
        for row in constraints
    ):
        await conn.execute_script(
            f"ALTER TABLE {table} ADD CONSTRAINT uid_mc_node_sys_env_provider "
            "UNIQUE (node_id, kind, provider, name);"
        )
        logger.info("[user-platform] system-env snapshot key widened to include provider")


async def _ensure_task_event_seq_sequence() -> None:
    """Create the shared ``mc_task_events_seq`` and seat it past the current max.

    ``seq`` was previously allocated as ``SELECT MAX(seq)+1`` per task by both
    the gateway (``_next_event_seq``) and node_server (``task_event_log`` /
    ``task_stage``). Two writers doing ``MAX+1`` on separate connections is a
    race: both read the same max and both insert that value +1, producing
    duplicate seqs. The ``(seq, id)`` cursor in ``list_task_events`` then
    repeats or skips rows at a page boundary that lands inside a tie.

    A single PG SEQUENCE makes ``nextval`` atomic across every writer, so seq
    is globally unique and monotonic (per-task values are not contiguous once
    other tasks consume seqs, but the ``seq < before`` cursor only needs
    monotonic + unique, which holds). Created here on the gateway because
    node_server deliberately does not create domain tables (see
    ``node_server/shared_store.py``); the SEQUENCE object lives in the shared
    DB, so any connection can ``nextval`` it. ``setval`` past the current max
    keeps existing rows' seqs below the next allocation, so old and new rows
    never collide. Idempotent via ``IF NOT EXISTS``.
    """
    from tortoise import Tortoise

    conn = Tortoise.get_connection("user_platform")
    rows = await conn.execute_query_dict(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'mc_task_events'"
    )
    if not rows:
        return  # table not created yet; generate_schemas will build it
    if "seq" not in {row["column_name"] for row in rows}:
        return
    await conn.execute_script("CREATE SEQUENCE IF NOT EXISTS mc_task_events_seq;")
    # Seat past the current max so the first nextval is strictly greater than
    # every existing seq. is_called=false (setval(..., ..., true) is the
    # default) means the next nextval returns exactly the set value — so use
    # max+1 semantics by seating at GREATEST(max, 1) and letting nextval start
    # from the value after it.
    await conn.execute_script(
        "SELECT setval('mc_task_events_seq', "
        "GREATEST((SELECT COALESCE(MAX(seq), 0) FROM mc_task_events), 1));"
    )


async def _migrate_project_issue_workflow() -> None:
    """Migrate legacy issue rows to the requirement/bug state machine.

    The table used to store ``open | completed | closed`` and had no type. New
    rows always start ``unassigned``; existing rows are requirements because no
    discriminator existed when they were created. The statements are idempotent
    and run after additive columns are present.
    """
    from tortoise import Tortoise

    conn = Tortoise.get_connection("user_platform")
    rows = await conn.execute_query_dict(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'mc_project_issues'"
    )
    columns = {row["column_name"] for row in rows}
    required = {"type", "status", "pending_items"}
    if not required.issubset(columns):
        return
    # Additive reconciliation creates missing columns nullable, regardless of
    # model defaults, so materialize defaults for existing rows explicitly.
    await conn.execute_script(
        """
        UPDATE mc_project_issues
           SET type = 'requirement'
         WHERE type IS NULL OR BTRIM(type) = '';
        UPDATE mc_project_issues
           SET pending_items = '[]'::jsonb
         WHERE pending_items IS NULL;
        UPDATE mc_project_issues SET status = 'unassigned' WHERE status = 'open';
        """
    )
    logger.debug("[user-platform] project issue workflow migration ready")


async def _migrate_task_bootstrap() -> None:
    """Normalize additive Task bootstrap fields and enforce thread uniqueness.

    ``_ensure_additive_columns`` intentionally adds fields nullable on populated
    tables. The gateway treats NULL booleans as false during rollout; this step
    materializes the defaults and adds the partial unique index that makes a
    provider-native thread belong to exactly one canonical Task.
    """
    from tortoise import Tortoise

    conn = Tortoise.get_connection("user_platform")
    rows = await conn.execute_query_dict(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'mc_tasks'"
    )
    columns = {row["column_name"] for row in rows}
    required = {"provider_thread_id", "first_request_seen", "bootstrap_consumed"}
    if not required.issubset(columns):
        return
    await conn.execute_script(
        """
        UPDATE mc_tasks SET first_request_seen = false WHERE first_request_seen IS NULL;
        UPDATE mc_tasks SET bootstrap_consumed = false WHERE bootstrap_consumed IS NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS uq_mc_tasks_provider_thread
            ON mc_tasks(provider_thread_id)
            WHERE provider_thread_id IS NOT NULL;
        """
    )
    logger.debug("[user-platform] task bootstrap migration ready")


async def _migrate_tunnel_runtime() -> None:
    """Backfill desired state and runtime rows for pre-runtime tunnel bindings."""
    from tortoise import Tortoise

    conn = Tortoise.get_connection("user_platform")
    tables = await conn.execute_query_dict(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_name IN ('mc_tunnel_bindings','mc_tunnel_runtimes')"
    )
    if {row["table_name"] for row in tables} != {
        "mc_tunnel_bindings", "mc_tunnel_runtimes"
    }:
        return
    # Failed/stopped legacy rows remain stopped to avoid a boot-time retry storm;
    # pending/running rows preserve intent and are reconciled after startup.
    await conn.execute_script(
        """
        UPDATE mc_tunnel_bindings
           SET desired_state = CASE
               WHEN client_status IN ('pending','running') THEN 'running'
               ELSE 'stopped'
           END
         WHERE desired_state IS NULL OR BTRIM(desired_state) = '';
        UPDATE mc_tunnel_bindings
           SET delete_requested = false
         WHERE delete_requested IS NULL;
        """
    )
    logger.debug("[user-platform] tunnel runtime migration ready")


async def close_database() -> None:
    from tortoise import Tortoise

    await Tortoise.close_connections()
