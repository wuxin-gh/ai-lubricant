from pathlib import Path


ADMIN_HTML = Path(__file__).resolve().parents[1] / "static" / "admin.html"


def admin_html() -> str:
    return ADMIN_HTML.read_text(encoding="utf-8")


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


def test_admin_custom_channel_media_generation_paths_and_test_endpoints():
    source = admin_html()
    create_body = function_body(source, "renderCustomCreatePage")
    edit_body = function_body(source, "showCustomConfig")
    read_body = function_body(source, "readCustomChannelForm")
    test_body = function_body(source, "runMediaTestModel")

    assert "ccImagePath" in create_body and "ccVideoPath" in create_body
    assert "ccSupportsImage" not in create_body and "ccSupportsVideo" not in create_body
    assert "ccImagePath" in edit_body and "ccVideoPath" in edit_body
    assert "supports_image_generation" not in read_body
    assert "supports_video_generation" not in read_body
    assert "/v1/images/generations" in test_body
    assert "/v1/videos/generations" in test_body


def test_request_log_ui_layout_and_theme_regressions():
    source = admin_html()
    render_body = function_body(source, "renderRequestLogsPage")
    detail_body = function_body(source, "showRequestLogDetail")
    channels_body = function_body(source, "renderChannelCards")
    path_body = function_body(source, "renderAttemptsPath")

    assert 'sidebar-tools-row' in source
    assert 'user-menu' in source
    assert 'user-trigger' in source
    assert 'toolbar-panel{position:relative;z-index:20;display:flex;align-items:flex-end' in source
    assert 'body.theme-light .log-block{background:#fff;color:var(--textH)}' in source
    assert 'class="log-block"' in source
    assert 'token-main-row' in render_body
    assert 'token-cache-row' in render_body
    assert 'card-title ${titleCls}' in channels_body
    assert "titleCls = status === 'ok' ? 'ok' : 'err'" in channels_body
    assert "disabledHint = status === 'disabled'" in channels_body
    assert "status === 'error' ? ''" in channels_body
    assert "pathHtml" in detail_body and "attemptsSorted" in detail_body
    assert "showRequestLogDetail" in path_body
    assert "${esc(provider)} / ${esc(account)} / ${esc(model)}" in path_body


def test_request_logs_table_hides_request_id_columns_and_parent_request_text():
    body = function_body(admin_html(), "renderRequestLogsPage")

    assert "requestIdCell" not in body
    assert "父请求" not in body
    assert "请求ID" not in body
    assert "请求 ID" not in body


def test_request_log_detail_shows_request_id_but_not_parent_request_metadata():
    body = function_body(admin_html(), "showRequestLogDetail")

    assert "请求ID" in body or "请求 ID" in body
    assert "parent_request_id" not in body
    assert "父请求" not in body


def test_request_log_model_formatter_prefers_explicit_requested_and_actual_fields():
    formatter_body = function_body(admin_html(), "formatRequestLogModel")

    assert "const requested = r.requested_model || r.model || '-'" in formatter_body
    assert "const actual = r.actual_model || r.model || ''" in formatter_body
    assert "actual && actual !== requested" in formatter_body
    assert '实际 ${esc(actual)}' in formatter_body
    assert "table-stack" in formatter_body


def test_request_log_table_renders_model_formatter_result_in_model_cell():
    body = function_body(admin_html(), "renderRequestLogsPage")

    assert '<th style="padding:7px 8px;width:145px">模型</th>' in body
    assert '<th style="padding:7px 8px">实际模型</th>' not in body
    assert '<td style="padding:7px 8px;font-family:var(--fontM)">${formatRequestLogModel(r)}</td>' in body
    assert 'colspan="11"' in body
    assert "mkModelCell" not in body


def test_request_log_detail_uses_cache_read_wording():
    body = function_body(admin_html(), "showRequestLogDetail")

    assert "缓存读" in body
    assert "缓存读取" not in body


def test_request_log_detail_renders_model_formatter_result_in_model_row():
    body = function_body(admin_html(), "showRequestLogDetail")

    assert '<div class="kv-row"><span class="key">模型</span><span class="val" style="color:var(--blue)">${formatRequestLogModel(r)}</span></div>' in body
    assert '上游返回模型' in body
    assert 'extractUpstreamReturnedModel(resp, r.upstream_returned_model' in body


def test_account_list_shows_freeze_and_cooldown_reasons():
    body = function_body(admin_html(), "renderAccountList")

    assert '冻结原因：' in body
    assert '冻结剩余' in body
    assert 'a.cooldown_reason' in body


def test_cached_token_label_uses_cache_read_wording():
    body = function_body(admin_html(), "renderRequestLogsPage")

    assert "缓存读" in body
    assert ">读 " not in body

def test_request_logs_table_has_client_column_and_stream_safe_time_cell():
    body = function_body(admin_html(), "renderRequestLogsPage")

    assert '<th style="padding:7px 8px;text-align:center;width:74px">客户端</th>' in body
    assert "clientTypeLabel(r.client_type || 'unknown')" in body
    assert "if (!r.stream || !r.success)" in body
    assert "if (!r.first_token_ms)" in body
    assert "${dur}</span><span style=\"color:var(--text2);font-size:11px;margin-left:4px\">/ ${ttft}" in body


def test_request_logs_table_error_column_has_fixed_wrapping_width():
    body = function_body(admin_html(), "renderRequestLogsPage")

    assert "table-layout:fixed" in body
    assert '<th style="padding:7px 8px;width:196px">异常内容</th>' in body
    assert 'width:196px;max-width:196px' in body
    assert 'width:180px;max-width:180px' in body
    assert 'title="${escAttr(value)}"' in body
    assert '-webkit-line-clamp:2' in body
    assert 'word-break:break-word' in body


def test_stream_attempt_log_keeps_provider_usage_chunk_when_summary_has_no_usage():
    source = (Path(__file__).resolve().parents[1] / "main.py").read_text(encoding="utf-8")
    retry_body = source[source.index("async def _chat_with_retry"):source.index("@app.post(\"/v1/responses\")", source.index("async def _chat_with_retry"))]

    assert 'if attempt_stream_summary is not None and attempt_usage_body is None:' in retry_body
    assert 'summary_usage_body = _stream_summary_to_openai_response(model, attempt_stream_summary)' not in retry_body
    assert 'finalize_usage_body = attempt_usage_body or (_stream_summary_to_openai_response(model, attempt_stream_summary) if attempt_stream_summary is not None else None)' in retry_body
    assert 'usage=_usage(\n                    finalize_usage_body,' in retry_body


def test_request_log_backend_query_exposes_requested_and_actual_model_aliases():
    source = (Path(__file__).resolve().parents[1] / "db.py").read_text(encoding="utf-8")
    query_body = source[source.index("async def query_request_logs"):source.index("    @classmethod", source.index("async def query_request_logs") + 1)]

    assert "model," in query_body
    assert "model AS requested_model" in query_body
    assert "coalesce(actual_model, model) AS actual_model" in query_body
    assert "retry_path" not in query_body
    assert "jsonb_array_length" not in query_body


def test_request_log_detail_derives_requested_and_actual_model_fields():
    source = (Path(__file__).resolve().parents[1] / "db.py").read_text(encoding="utf-8")
    detail_body = source[source.index("async def get_request_log_detail"):source.index("    @classmethod\n    async def dashboard_stats", source.index("async def get_request_log_detail"))]

    assert 'data["requested_model"] = data.get("model")' in detail_body
    assert 'data.get("actual_model") or data.get("model")' in detail_body
    assert 'attempts[-1].get("actual_model")' in detail_body
    assert "attempts" in detail_body
    assert "parent_log_id = $1" in detail_body
    assert "retry_path" not in detail_body


def test_request_logs_table_renders_request_id_for_all_rows():
    body = function_body(admin_html(), "renderRequestLogsPage")

    assert "r.request_id" in body
    assert "r.parent_log_id" in body
    assert "parent_request_id" not in body
    assert "retry_path" not in body


def test_request_log_detail_renders_path_from_attempts_sorted_by_created_at():
    body = function_body(admin_html(), "showRequestLogDetail")

    assert "attempts" in body
    assert "parent_log_id" in body
    assert "parent_request_id" not in body
    assert "retry_path" not in body
    assert "renderRetryPathPanel" not in body
    assert "renderAttemptsPath" in body


def test_request_log_detail_renders_router_request_headers_section():
    body = function_body(admin_html(), "showRequestLogDetail")

    assert "routerReqHeaders" in body
    assert "r.router_request_headers" in body
    assert "发送给渠道的 Headers" in body


def test_render_attempts_path_uses_id_and_request_id_fields():
    body = function_body(admin_html(), "renderAttemptsPath")

    assert "item.id" in body
    assert "requestId" in body
    assert "item.provider_name" in body
    assert "item.account_username" in body
    assert "item.first_token_ms" in body
    assert "item.prompt_tokens" in body
    assert "item.completion_tokens" in body
    assert "retry_path" not in body
    assert "parent_request_id" not in body


def test_backend_rejects_duplicate_or_blank_normalized_model_group_names():
    source = (Path(__file__).resolve().parents[1] / "admin.py").read_text(encoding="utf-8")
    validation_body = source[source.index("async def _validate_model_groups"):source.index("@router.get(\"/model-routing\")", source.index("async def _validate_model_groups"))]

    assert "normalized_group_names" in validation_body
    assert "模型组名称不能为空" in validation_body
    assert "模型组名称重复" in validation_body


def test_admin_streaming_first_token_uses_content_helper_and_minimum_one_ms():
    source = (Path(__file__).resolve().parents[1] / "admin.py").read_text(encoding="utf-8")

    assert "def _has_first_token_content(chunk):" in source
    assert "if first_token_ms is None and _has_first_token_content(chunk):" in source
    assert "first_token_ms = max(1, int((time.time() - start_time) * 1000))" in source
    assert "if first_token_ms is None and chunk:" not in source


def test_main_streaming_ttft_uses_content_helper_and_minimum_one_ms():
    source = (Path(__file__).resolve().parents[1] / "main.py").read_text(encoding="utf-8")

    assert "def _has_first_token_content(chunk):" in source
    assert "if stream and not ttft_recorded and _has_first_token_content(chunk):" in source
    assert "if stream and not ttft_recorded and chunk:" not in source
    assert "ttft_ms = _duration_ms(attempt_start)" in source


def test_has_first_token_content_ignores_empty_done_and_usage_only_chunks():
    import json

    source = (Path(__file__).resolve().parents[1] / "admin.py").read_text(encoding="utf-8")
    helper_source = source[source.index("def _has_first_token_content(chunk):"):source.index("def _read_json", source.index("def _has_first_token_content(chunk):"))]
    namespace = {"json": json}
    exec(helper_source, namespace)
    has_content = namespace["_has_first_token_content"]

    assert not has_content("")
    assert not has_content("data: [DONE]\n\n")
    assert not has_content({"usage": {"total_tokens": 1}, "choices": []})
    assert not has_content({"choices": [{"delta": {}}]})
    assert has_content('data: {"choices":[{"delta":{"content":"hi"}}]}\n\n')
    assert has_content({"choices": [{"delta": {"content": "hi"}}]})
    assert has_content({"choices": [{"message": {"content": "hi"}}]})
    assert has_content({"choices": [{"delta": {"reasoning_content": "thinking"}}]})
    assert has_content({"choices": [{"delta": {"thinking_delta": "thinking"}}]})
    assert has_content({"choices": [{"delta": {"tool_calls": [{"index": 0}]}}]})
    assert has_content('event: content_block_start\ndata: {"type":"content_block_start","content_block":{"type":"tool_use","id":"toolu_1","name":"x","input":{}}}\n\n')
    assert has_content('event: response.output_item.added\ndata: {"type":"response.output_item.added","item":{"type":"function_call","call_id":"call_1","name":"x"}}\n\n')
