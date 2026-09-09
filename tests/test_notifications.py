from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ADMIN_HTML = ROOT / "static" / "admin.html"


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def function_body(source: str, name: str) -> str:
    marker = f"function {name}("
    assert marker in source, f"Function {name} declaration not found"
    start = source.index(marker)
    brace = source.index("{", start)
    depth = 0
    quote = None
    escaped = False
    template_expr_depth = 0
    for index in range(brace, len(source)):
        char = source[index]
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif quote == "`" and char == "$" and source[index + 1:index + 2] == "{":
                template_expr_depth += 1
            elif quote == "`" and char == "}" and template_expr_depth:
                template_expr_depth -= 1
            elif char == quote and not template_expr_depth:
                quote = None
            continue
        if char in "'\"`":
            quote = char
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[brace + 1:index]
    raise AssertionError(f"Function {name} body not found")


def test_toast_uses_stacked_card_container_without_overlapping_fixed_children():
    source = read_text(ADMIN_HTML)
    body = function_body(source, "toast")

    assert "function toast(msg, type='success')" in source
    assert "#toastContainer{position:fixed" in source
    assert "display:flex;flex-direction:column;gap:" in source
    assert ".toast{position:relative" in source
    assert ".toast{position:fixed;top:18px" not in source
    assert "while (container.children.length >= 5)" in body
    assert "toast-close" in body
    assert "toast-title" in body
    assert "toast-msg" in body


def test_notification_center_is_registered_in_user_menu():
    source = read_text(ADMIN_HTML)

    assert "'notifications'" in source
    assert 'id="userMenu"' in source
    assert "项目文档" in source
    assert "通知中心" in source
    assert "notificationBadge" in source
    assert "notificationMenuBadge" in source
    assert "navTo('notifications');closeUserMenu()" in source
    assert "'notifications':['通知中心','运行错误、告警与处理状态']" in source
    assert "case 'notifications': renderNotificationsShell(); break;" in source
    assert 'data-page="notifications"' not in source


def test_notification_center_frontend_functions_exist_and_link_request_logs():
    source = read_text(ADMIN_HTML)

    for name in (
        "loadNotificationSummary",
        "renderNotificationsShell",
        "renderNotificationsPage",
        "renderNotificationSeverityTag",
        "showNotificationDetail",
        "markNotificationRead",
        "markAllNotificationsRead",
    ):
        assert f"function {name}(" in source or f"async function {name}(" in source

    detail_body = function_body(source, "showNotificationDetail")
    assert "/admin/notifications/${id}" in detail_body
    assert "showRequestLogDetail(${n.request_log_id})" in detail_body
    assert "renderLogJson(metadata)" in detail_body


def test_notification_backend_schema_and_helpers_are_present():
    db_source = read_text(ROOT / "db.py")

    assert "CREATE TABLE IF NOT EXISTS notifications" in db_source
    for field in (
        "severity TEXT NOT NULL DEFAULT 'info'",
        "kind TEXT NOT NULL",
        "status TEXT NOT NULL DEFAULT 'unread'",
        "occurrence_count INTEGER NOT NULL DEFAULT 1",
        "request_log_id BIGINT REFERENCES request_logs(id) ON DELETE SET NULL",
        "metadata JSONB NOT NULL DEFAULT '{}'::jsonb",
    ):
        assert field in db_source
    for index in (
        "idx_notifications_created_at",
        "idx_notifications_status",
        "idx_notifications_severity",
        "idx_notifications_dedupe_key",
        "idx_notifications_request_log_id",
    ):
        assert index in db_source
    for method in (
        "async def upsert_notification",
        "async def query_notifications",
        "async def notification_summary",
        "async def get_notification_detail",
        "async def mark_notification_read",
        "async def mark_all_notifications_read",
        # 删除/批量删除：平台域 + 用户域（owner-scoped）两套对称口径。
        "async def delete_notification",
        "async def delete_notifications_by_ids",
        "async def delete_notifications_by_filter",
        "async def delete_notification_for_owner",
        "async def delete_notifications_by_ids_for_owner",
        "async def delete_notifications_by_filter_for_owner",
    ):
        assert method in db_source


def test_notification_admin_routes_are_exposed_before_dynamic_detail_route():
    admin_source = read_text(ROOT / "admin.py")

    assert '@router.get("/notifications")' in admin_source
    assert '@router.get("/notifications/summary")' in admin_source
    assert '@router.get("/notifications/{notification_id}")' in admin_source
    assert '@router.put("/notifications/{notification_id}/read")' in admin_source
    assert '@router.put("/notifications/read-all")' in admin_source
    detail_at = admin_source.index('@router.get("/notifications/{notification_id}")')
    assert admin_source.index('@router.get("/notifications/summary")') < detail_at
    # Every static sub-resource must be registered *before* the dynamic detail
    # route, and must actually carry its decorator. A GET that loses its
    # decorator (or lands after this one) is matched by /{notification_id}
    # instead, so "channels" is parsed as an int and the call 422s.
    for path in (
        "/notifications/channels",
        "/notifications/events",
        "/notifications/event-types",
        "/notifications/subscription-rules",
    ):
        decorator = f'@router.get("{path}")'
        assert decorator in admin_source, f"missing GET decorator for {path}"
        assert admin_source.index(decorator) < detail_at, f"{path} shadowed by detail route"
    assert "await _require_admin(token)" in admin_source


def test_notification_delete_routes_are_registered_and_not_shadowed():
    """删除 / 批量删除 / 清空已读三条路由必须存在，且静态 POST 声明在动态详情路由之前。"""
    admin_source = read_text(ROOT / "admin.py")

    assert '@router.delete("/notifications/{notification_id}")' in admin_source
    detail_at = admin_source.index('@router.get("/notifications/{notification_id}")')
    for path in ("/notifications/batch-delete", "/notifications/clear-read"):
        decorator = f'@router.post("{path}")'
        assert decorator in admin_source, f"missing POST decorator for {path}"
        assert admin_source.index(decorator) < detail_at, f"{path} shadowed by detail route"

    user_source = read_text(ROOT / "user_platform" / "routes_user_notifications.py")
    assert '@router.delete("/{notification_id}")' in user_source
    user_detail_at = user_source.index('@router.get("/{notification_id}")')
    for path in ("/batch-delete", "/clear-read"):
        decorator = f'@router.post("{path}")'
        assert decorator in user_source, f"missing POST decorator for {path}"
        assert user_source.index(decorator) < user_detail_at, f"{path} shadowed by detail route"


def test_runtime_notification_helper_is_available_without_default_error_policy():
    main_source = read_text(ROOT / "main.py")

    assert "async def _log_notification" in main_source
    assert "def _schedule_notification" in main_source
    assert "PostgresClient.upsert_notification(data)" in main_source
    assert "写入通知失败" in main_source
    assert "kind\": \"no_available_account\"" not in main_source
    assert "kind\": \"retry_exhausted\"" not in main_source
    assert "source\": \"main._finalize_channel_attempt_log\"" not in main_source
    assert "parent_request_id" not in main_source
    assert "retry_path" not in main_source
