from pathlib import Path


ADMIN_HTML = Path(__file__).resolve().parents[1] / "static" / "admin.html"


def admin_html() -> str:
    return ADMIN_HTML.read_text(encoding="utf-8")


def function_body(source: str, name: str) -> str:
    marker = f"function {name}("
    assert marker in source, f"Function {name} declaration not found"
    start = source.index(marker)
    params_start = source.index("(", start)
    params_depth = 0
    for index in range(params_start, len(source)):
        char = source[index]
        if char == "(":
            params_depth += 1
        elif char == ")":
            params_depth -= 1
            if params_depth == 0:
                brace = source.index("{", index)
                break
    else:
        raise AssertionError(f"Function {name} parameters not closed")
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
            elif quote == "`" and source[index + 1:index + 2] == "{":
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


def test_shared_time_range_component_helpers_exist():
    html = admin_html()

    assert "function timeRangeFilterHtml(" in html
    assert "function initTimeRangeFilter(" in html
    assert "function applyTimeRangeShortcut(" in html
    assert "function readTimeRangeFilter(" in html
    assert "function setTimeRangeError(" in html


def test_time_range_popover_trigger_panel_shortcuts_and_error_slot_are_rendered():
    html = admin_html()
    body = function_body(html, "timeRangeFilterHtml")

    assert "time-range-filter" in body
    assert "time-range-trigger" in body
    assert "toggleTimeRangePanel" in body
    assert "time-range-panel" in body
    assert "time-range-panel hidden" in body
    assert "time-range-summary" in body
    assert "time-shortcuts" in body
    assert "data-range=\"1h\"" in body
    assert "近1小时" in body
    assert "data-range=\"2h\"" in body
    assert "近2小时" in body
    assert "data-range=\"3h\"" in body
    assert "近3小时" in body
    assert "data-range=\"6h\"" in body
    assert "近6小时" in body
    assert "data-range=\"12h\"" in body
    assert "近12小时" in body
    assert "data-range=\"24h\"" in body
    assert "近24小时" in body
    assert "data-range=\"today\"" in body
    assert "今天" in body
    assert "data-range=\"yesterday\"" in body
    assert "昨天" in body
    assert "data-range=\"default\"" in body
    assert "恢复默认" in body
    assert "confirmTimeRange" in body
    assert "cancelTimeRange" in body
    assert "time-range-error" in body
    assert "<button type=\"button\" class=\"btn primary\" onclick=\"${escAttr(queryHandler)}\">查询</button>" not in body


def test_time_range_popover_helpers_exist():
    html = admin_html()

    assert "function toggleTimeRangePanel(" in html
    assert "function openTimeRangePanel(" in html
    assert "function closeTimeRangePanel(" in html
    assert "function confirmTimeRange(" in html
    assert "function cancelTimeRange(" in html
    assert "function updateTimeRangeSummary(" in html


def test_shortcuts_only_update_draft_range_until_confirmed():
    html = admin_html()
    shortcut_body = function_body(html, "applyTimeRangeShortcut")
    confirm_body = function_body(html, "confirmTimeRange")

    assert "window._timeRangeDrafts[prefix] = range" in shortcut_body
    assert "setTimeRangeInputs(prefix, range)" in shortcut_body
    assert "window._rlTimeRange = range" not in shortcut_body
    assert "window._ddTimeRange = range" not in shortcut_body
    assert "window._rlTimeRange = range" in confirm_body
    assert "window._ddTimeRange = range" in confirm_body


def test_request_logs_and_dashboard_use_shared_time_range_component():
    html = admin_html()
    request_filter_body = function_body(html, "requestLogFilterHtml")
    dashboard_body = function_body(html, "renderDataDashboardPage")

    assert "timeRangeFilterHtml({prefix:'rl'" in request_filter_body
    assert "timeRangeFilterHtml({prefix:'dd'" in dashboard_body
    assert "initTimeRangeFilter('rl'" in html
    assert "initTimeRangeFilter('dd'" in html


def test_time_range_validation_blocks_invalid_requests():
    html = admin_html()
    read_body = function_body(html, "readTimeRangeFilter")
    request_page_body = function_body(html, "renderRequestLogsPage")
    dashboard_body = function_body(html, "renderDataDashboardPage")

    assert "开始时间不能晚于结束时间" in read_body
    assert "请选择有效时间" in read_body
    assert "return null" in read_body
    assert "const range = readTimeRangeFilter('rl'" in request_page_body
    assert "if (!range) return" in request_page_body
    assert "let range = readTimeRangeFilter('dd'" in dashboard_body
    assert "if (!range) return" in dashboard_body


def test_shortcuts_write_values_through_flatpickr_when_available():
    html = admin_html()
    body = function_body(html, "setTimeRangeInputs")

    assert "startEl._flatpickr.setDate" in body
    assert "endEl._flatpickr.setDate" in body


def css_block(source: str, selector: str) -> str:
    start = source.index(selector)
    end = source.index("}", start) + 1
    return source[start:end]


def test_time_range_layout_stays_inline_and_popover_overlays_content():
    html = admin_html()
    filter_css = css_block(html, ".time-range-filter")
    trigger_css = css_block(html, ".time-range-trigger")

    assert "margin-bottom:0" in filter_css
    assert "align-items:flex-end" not in filter_css
    assert "flex:0 0 340px" in filter_css
    assert "font-family:var(--font)" in trigger_css
    assert "font-family:var(--fontM)" not in trigger_css
    assert "z-index:200" in css_block(html, ".time-range-panel")
    assert ".toolbar-panel{position:relative;z-index:20" in html
    assert ".card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radiusL);margin-bottom:18px;overflow:visible" in html


def test_time_range_component_does_not_force_full_toolbar_width():
    html = admin_html()
    filter_css = css_block(html, ".time-range-filter")
    trigger_css = css_block(html, ".time-range-trigger")

    assert "width:100%" not in filter_css
    assert "max-width:480px" not in trigger_css


def test_time_range_uses_form_group_label_and_query_button_at_toolbar_end():
    html = admin_html()
    body = function_body(html, "timeRangeFilterHtml")
    request_filter_body = function_body(html, "requestLogFilterHtml")
    dashboard_body = function_body(html, "renderDataDashboardPage")

    assert "<div class=\"form-group time-range-filter\"" in body
    assert "<label>时间范围</label>" in body
    assert "<span class=\"label\">时间范围</span>" not in body
    assert "<button type=\"button\" class=\"btn primary toolbar-query-btn\"" in request_filter_body
    assert "<button type=\"button\" class=\"btn primary toolbar-query-btn\"" in dashboard_body
    assert "queryHandler" not in body


def test_time_range_popover_typography_inherits_admin_form_styles():
    html = admin_html()

    assert ".time-range-panel{font-family:var(--font);font-size:13px" in html
    assert ".time-range-panel .btn{font-family:inherit" in html
    assert ".time-range-trigger{width:100%;justify-content:space-between;text-align:left" in html
    assert ".time-range-trigger .label" not in html
    assert ".toolbar-query-btn{align-self:flex-end;margin-left:auto" in html


def test_backend_query_parameter_names_stay_unchanged():
    html = admin_html()
    request_page_body = function_body(html, "renderRequestLogsPage")
    dashboard_body = function_body(html, "renderDataDashboardPage")

    assert "start_time:String(start)" in request_page_body
    assert "end_time:String(end)" in request_page_body
    assert "start:String(start)" in dashboard_body
    assert "end:String(end)" in dashboard_body
