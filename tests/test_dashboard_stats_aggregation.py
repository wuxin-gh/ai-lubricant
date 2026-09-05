import pytest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_source(rel_path: str) -> str:
    # 业务模块已收进 server/（见 refactor(structure)），根级路径作为兼容回退。
    path = ROOT / "server" / rel_path
    if not path.exists():
        path = ROOT / rel_path
    return path.read_text(encoding="utf-8")


def test_hourly_log_stats_table_exists_in_schema():
    db = load_source("db.py")
    assert "CREATE TABLE IF NOT EXISTS hourly_log_stats" in db
    assert "PRIMARY KEY (hour, provider_name, model, actual_model)" in db


def test_hourly_dashboard_stats_table_exists_in_schema():
    db = load_source("db.py")
    assert "CREATE TABLE IF NOT EXISTS hourly_dashboard_stats" in db
    assert "PRIMARY KEY (hour, provider_name, account_username, api_key_name, model, actual_model)" in db
    assert "idx_hourly_dashboard_stats_api_key" in db


def test_hourly_stats_tables_migrate_actual_model_column():
    db = load_source("db.py")
    # 旧库升级：加 actual_model 列并把主键扩展到新口径，幂等可重复执行。
    assert "ALTER TABLE hourly_log_stats ADD COLUMN IF NOT EXISTS actual_model" in db
    assert "ALTER TABLE hourly_dashboard_stats ADD COLUMN IF NOT EXISTS actual_model" in db
    assert "ALTER TABLE hourly_log_stats DROP CONSTRAINT IF EXISTS hourly_log_stats_pkey" in db
    assert "ALTER TABLE hourly_dashboard_stats DROP CONSTRAINT IF EXISTS hourly_dashboard_stats_pkey" in db


def test_aggregate_hourly_logs_uses_on_conflict_do_nothing():
    db = load_source("db.py")
    body = db[db.index("async def aggregate_hourly_logs"):db.index("    @classmethod\n    async def _normalize_legacy_usage_tokens")]
    assert "ON CONFLICT (hour, provider_name, model, actual_model) DO NOTHING" in body
    # 运营看板统计所有已有终态的渠道尝试，不再用 token 是否大于 0 判断请求是否有效。
    assert "parent_log_id" not in body
    assert "status IS NOT NULL AND status <> 'requesting'" in body


def test_aggregate_hourly_logs_refreshes_dashboard_stats_table():
    db = load_source("db.py")
    body = db[db.index("async def aggregate_hourly_logs"):db.index("    @classmethod\n    async def _normalize_legacy_usage_tokens")]
    assert "INSERT INTO hourly_dashboard_stats" in body
    assert "account_username" in body
    assert "api_key_name" in body
    assert "coalesce(model, 'unknown') AS model" in body
    # actual_model = 真实路由到的模型广场模型 ID；历史行为空时回退到 model。
    assert "coalesce(nullif(actual_model, ''), model, 'unknown') AS actual_model" in body
    assert "GROUP BY date_trunc('hour', created_at), coalesce(provider_name, 'unknown'), coalesce(account_username, ''), coalesce(api_key_name, ''), coalesce(model, 'unknown'), coalesce(nullif(actual_model, ''), model, 'unknown')" in body
    assert "ON CONFLICT (hour, provider_name, account_username, api_key_name, model, actual_model) DO UPDATE SET" in body
    assert "status NOT IN ('ok','success','200')" in body
    # 错误判定需排除空字符串 error，避免成功请求（error='' ）被误计为失败导致失败率 100%。
    assert "(error IS NOT NULL AND error <> '')" in body
    # 先删后插：避免旧主键口径行残留导致同一小时被重复统计
    assert "DELETE FROM hourly_log_stats WHERE hour >= $1::timestamptz AND hour < $2::timestamptz" in body
    assert "DELETE FROM hourly_dashboard_stats WHERE hour >= $1::timestamptz AND hour < $2::timestamptz" in body


def test_init_never_truncates_or_backfills_hourly_stats():
    """启动绝不 TRUNCATE/重建聚合表：聚合表是长期数据，request_logs 只在保留期内
    可重算，清空重建会把超出保留期的历史一起抹掉（曾致看板只剩最近几天）。
    口径变更用 script/rebuild_hourly_stats.py 显式重建。"""
    db = load_source("db.py")
    assert "TRUNCATE hourly_dashboard_stats" not in db
    assert "TRUNCATE hourly_log_stats" not in db
    assert "backfill_hourly_dashboard_stats" not in db


def test_dashboard_stats_uses_hourly_dashboard_stats():
    db = load_source("db.py")
    body = db[db.index("async def dashboard_stats"):db.index("    @classmethod\n    async def billing_usage")]
    assert "hourly_dashboard_stats" in body
    assert "hourly_log_stats" not in body


def test_dashboard_stats_live_range_does_not_overlap_hourly_aggregate():
    db = load_source("db.py")
    body = db[db.index("async def dashboard_stats"):db.index("    @classmethod\n    async def billing_usage")]
    # hourly_dashboard_stats 已覆盖 [aggregate_start, aggregate_end)，且 aggregate_end <= current_hour。
    # 实时 request_logs 只能从 current_hour 开始，否则会和预聚合段 UNION ALL 重叠导致 token 翻倍。
    assert "live_start = current_hour" in body
    assert "add_raw_range(aggregate_start, prefix_end" not in body
    assert "add_raw_range(aggregate_end, end_dt" not in body


def test_dashboard_stats_raw_queries_use_main_table_only():
    db = load_source("db.py")
    body = db[db.index("async def dashboard_stats"):db.index("    @classmethod\n    async def billing_usage")]
    raw_range = body[body.index("def add_raw_range"):body.index("            # 实时段：")]
    fallback = body[body.index("if not parts:"):body.index("            combined_sql")]
    raw_source = body[body.index("raw_source ="):body.index("def add_raw_range")]
    assert "FROM request_logs" in raw_source
    # 归档表已下线，看板只查主表 request_logs。
    assert "request_logs_archive" not in raw_source
    for query in (raw_range, fallback):
        assert "FROM ({raw_source}) src" in query
        assert "status IS NOT NULL AND status <> 'requesting'" in query
        # 看板统计所有已有终态的渠道尝试，不能误改成仅统计顶层请求。
        assert "parent_log_id IS NULL" not in query


def test_dashboard_stats_returns_token_component_series():
    db = load_source("db.py")
    body = db[db.index("async def dashboard_stats"):db.index("    @classmethod\n    async def billing_usage")]
    assert "model_input_distribution" in body
    assert "model_output_distribution" in body
    assert "model_cache_read_distribution" in body
    assert "model_cache_write_distribution" in body
    assert "model_failure_distribution" in body


def test_dashboard_stats_zero_fills_missing_series_buckets():
    db = load_source("db.py")
    body = db[db.index("async def dashboard_stats"):db.index("    @classmethod\n    async def billing_usage")]
    assert "start_bucket = align_bucket(start_dt)" in body
    assert "end_bucket = align_bucket(end_dt)" in body
    assert "names = sorted({r[\"name\"] for r in raw_rows})" in body
    assert "filled.append(values.get((bucket, name)" in body
    assert "\"value\": value_type(0)" in body


def test_dashboard_stats_does_not_fallback_to_raw_for_api_key_filter():
    db = load_source("db.py")
    body = db[db.index("async def dashboard_stats"):db.index("    @classmethod\n    async def billing_usage")]
    assert "if account_filter or api_key_filter" not in body
    assert "api_key_name =" in body
    assert "account_username =" in body


def test_hourly_stats_schema_contains_reasoning_columns():
    db = load_source("db.py")
    assert "reasoning_tokens BIGINT NOT NULL DEFAULT 0" in db
    assert "reasoning_requests INTEGER NOT NULL DEFAULT 0" in db
    assert "ALTER TABLE hourly_log_stats ADD COLUMN IF NOT EXISTS reasoning_tokens" in db
    assert "ALTER TABLE hourly_dashboard_stats ADD COLUMN IF NOT EXISTS reasoning_requests" in db


def test_dashboard_stats_filters_to_finalized_channel_attempts():
    db = load_source("db.py")
    aggregate_body = db[db.index("async def aggregate_hourly_logs"):db.index("    @classmethod\n    async def _normalize_legacy_usage_tokens")]
    dashboard_body = db[db.index("async def dashboard_stats"):db.index("    @classmethod\n    async def billing_usage")]
    finalized_filter = "status IS NOT NULL AND status <> 'requesting'"
    assert aggregate_body.count(finalized_filter) >= 2
    assert dashboard_body.count(finalized_filter) >= 2
    assert "total_tokens > 0" not in aggregate_body
    assert "total_tokens > 0" not in dashboard_body


def test_dashboard_stats_duration_uses_successful_requests_only():
    db = load_source("db.py")
    aggregate_body = db[db.index("async def aggregate_hourly_logs"):db.index("    @classmethod\n    async def _normalize_legacy_usage_tokens")]
    dashboard_body = db[db.index("async def dashboard_stats"):db.index("    @classmethod\n    async def billing_usage")]
    success_duration = "CASE WHEN success=true AND coalesce(error, '') = '' AND status IN ('ok','success','200') THEN duration_ms ELSE 0 END"
    success_average = "coalesce(sum(total_duration_ms),0)::float / (sum(requests) - sum(errors))"
    assert aggregate_body.count(success_duration) >= 2
    assert dashboard_body.count(success_duration) >= 2
    assert dashboard_body.count(success_average) >= 5


def test_dashboard_stats_returns_reasoning_summary_and_rankings():
    db = load_source("db.py")
    body = db[db.index("async def dashboard_stats"):db.index("    @classmethod\n    async def billing_usage")]
    assert "reasoning_tokens" in body
    assert "reasoning_requests" in body
    assert "reasoning_request_rate" in body
    for key in (
        "model_usage_top",
        "provider_usage_top",
        "model_requests_top",
        "provider_requests_top",
        "provider_success_rate_top",
        "model_speed_top",
        "provider_speed_top",
        "model_cache_hit_top",
        "provider_cache_hit_top",
        # 账号维度占比榜：按 account_username 聚合，过滤空账号；provider 参数可限定到单渠道。
        "account_usage_top",
        "account_requests_top",
    ):
        assert key in body
    assert "cache_hit_rate" in body
    assert "success_rate" in body
    assert "avg_duration_ms" in body


def test_dashboard_stats_rankings_use_success_requests_and_actual_model():
    db = load_source("db.py")
    body = db[db.index("async def dashboard_stats"):db.index("    @classmethod\n    async def billing_usage")]
    # 请求次数榜/响应最快榜展示成功请求数（requests - errors），并按其排序/过滤。
    assert "sum(requests) - sum(errors)" in body
    assert "AS success_requests" in body
    # 失败/成功率的错误判定排除空字符串 error。
    assert "(error IS NOT NULL AND error <> '')" in body
    # 主统计模型维度使用 actual_model（真实路由到的模型广场模型 ID），
    # 而非 model（客户端请求名/自定义别名）。
    assert 'rank_select("actual_model")' in body
    assert "actual_model AS name" in body
    # 模型筛选按真实模型 ID 过滤：聚合段直接列过滤，实时/回退段带空值回退。
    assert "actual_model = \" + arg(model_filter)" in body
    assert "coalesce(nullif(actual_model, ''), model) = \" + arg(model_filter)" in body
    # 自定义模型占用榜（次要）：按请求别名 model 聚合，且只统计别名流量。
    assert "custom_model_usage_top" in body
    assert "model <> actual_model" in body


def test_config_get_log_retention_days_default_30():
    config = load_source("config.py")
    assert "get_log_retention_days" in config
    assert "30" in config


def test_config_max_entries_default_disabled():
    """数据库删除阈值默认 0=不按条数删：按条数裁剪会删掉尚未聚合的原始日志，
    聚合表是这些行的唯一长期形态，删了就永久缺数（旧默认 200 的教训）。"""
    config = load_source("config.py")
    assert '.get("max_entries", 0)' in config


def test_cleanup_uses_log_retention_days():
    db = load_source("db.py")
    assert "get_log_retention_days" in db
    assert "cleanup_request_logs" in db or "delete_old_logs" in db


def test_admin_data_retention_endpoint_exists():
    admin = load_source("admin.py")
    assert "/config/main/data-retention" in admin


def test_admin_html_has_log_days_field():
    path = ROOT / "static/admin.html"
    if not path.exists():
        pytest.skip("legacy static/admin.html is not present in the Vite admin frontend")
    html = path.read_text(encoding="utf-8")
    assert (
        "drLogDays" in html
        or "data_retention" in html
        or "日志保留天数" in html
    )
