from pathlib import Path


ADMIN_HTML = Path(__file__).resolve().parents[1] / "static" / "admin.html"


def admin_html() -> str:
    return ADMIN_HTML.read_text(encoding="utf-8")


def function_body(source: str, name: str) -> str:
    marker = f"function {name}("
    assert marker in source, f"Function {name} declaration not found"
    start = source.index(marker)
    # Scan past the parameter list to find the real opening brace.
    # Parameters may contain default object literals like `policy={enabled:false}`.
    paren_open = source.index("(", start)
    paren_depth = 0
    q = None
    esc = False
    for i in range(paren_open, len(source)):
        c = source[i]
        if q:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == q:
                q = None
            continue
        if c in "'\"`":
            q = c
        elif c == "(":
            paren_depth += 1
        elif c == ")":
            paren_depth -= 1
            if paren_depth == 0:
                break
    params_end = i
    brace = source.index("{", params_end)
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


def test_channel_detail_layout_renders_three_top_cards_in_one_row():
    html = admin_html()

    assert ".channel-detail-grid" in html
    body = function_body(html, "renderChannelLayout")
    expected_layout = """return `<div class=\"channel-detail-grid\">
    ${renderChannelConfig(name, p, viewType, ctx)}
    ${renderLimitPolicyCard(name, ctx.limitPolicy)}
    ${renderModelConfig(name, p, viewType, ctx)}
  </div>`;"""
    assert expected_layout in body
    assert "admin-split-layout" not in body


def test_limit_policy_page_card_is_summary_only_and_opens_editor_modal():
    html = admin_html()
    body = function_body(html, "renderLimitPolicyCard")

    assert "showLimitPolicy(" in body
    assert "保存策略" not in body
    assert "id=\"lpEnabled\"" not in body
    assert "policy-editor-grid" not in body
    for label in ["策略状态", "账号 RPM", "账号 TPM", "模型 TPM", "账号并发", "冻结策略"]:
        assert label in body


def test_limit_policy_editor_uses_chinese_condition_labels():
    html = admin_html()
    editor_body = function_body(html, "renderLimitPolicyEditor")
    render_rows_body = function_body(html, "renderFreezeRows")

    assert "响应头" in html
    assert "状态码" in html
    assert "异常" in html
    assert "冻结方式/时间" in editor_body
    assert "headers" not in editor_body
    assert "status_code" not in editor_body
    assert "freeze-name" not in editor_body


def test_limit_policy_editor_adds_default_429_rule():
    html = admin_html()
    add_row_body = function_body(html, "addFreezeRuleRow")

    assert "condition:'status_code'" in add_row_body
    assert "operator:'=='" in add_row_body
    assert "value:'429'" in add_row_body
    assert "freeze_mode:'fixed_duration'" in add_row_body
    assert "freeze_seconds:60" in add_row_body
    assert "condition:'exception'" in add_row_body


def test_limit_policy_editor_persists_visible_fields_and_refreshes_summary():
    html = admin_html()
    card_body = function_body(html, "renderLimitPolicyCard")
    save_body = function_body(html, "saveLimitPolicyCard")
    modal_body = function_body(html, "showLimitPolicy")

    assert "/limit-policy" in save_body
    assert "freeze_policy: collectFreezePolicy()" in save_body
    assert "saveLimitPolicyCard(name, true)" in modal_body
    assert "showLimitPolicy(${escAttr(JSON.stringify(name))})" in card_body
    assert "freeze-name" not in save_body


def test_collect_freeze_policy_filter_allows_exception():
    html = admin_html()
    body = function_body(html, "collectFreezePolicy")

    # The filter must pass exception through without requiring key
    assert "exception" in body
    assert "rule.condition === 'exception'" in body or "rule.condition==='exception'" in body
