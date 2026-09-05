"""工具调用解析单测（意图驱动：只认 tool_function 协议词的两种边界）。

解析器认两种边界：``  tool_function ... ``` `` 围栏（推荐形态）与
``<tool_function>...</tool_function>`` 尖括号变体（同一协议词的语法变体，弱模型按
训练惯性会吐这种）。判据是「有没有 tool_function 这个专属协议词」，不是「边界符号
长什么样」——模型写出协议词就已经明确表达了调用意图。

边界外的裸 JSON、普通 json 代码块、XML function 标签、Hermes 标签、arg_key/arg_value
形态一律不当工具调用——那些是正文，不是意图信号。
"""
import json
from tool_utils import (
    parse_tool_calls_from_content,
    generate_tools_prompt,
    find_tool_call_start,
    extract_complete_tool_calls,
    hold_partial_tool_marker_index,
    _strip_all_orphan_closing_tags,
    _content_can_contain_tool_calls,
    _loads_tool_json,
    _TOOL_FENCE_OPEN,
    _TOOL_FENCE_CLOSE,
    _TOOL_XML_OPEN,
    _TOOL_XML_CLOSE,
    render_tool_call_json,
)


def _names(tcs):
    return [t["function"]["name"] for t in tcs]


def _fence(payload: str) -> str:
    """包一个 tool_function 围栏块。"""
    return _TOOL_FENCE_OPEN + "\n" + payload + "\n" + _TOOL_FENCE_CLOSE


def _xml(payload: str) -> str:
    """包一个 tool_function 尖括号变体块。"""
    return _TOOL_XML_OPEN + ">" + payload + _TOOL_XML_CLOSE


# ---- 围栏内正向解析 ----

def test_tool_function_fence_parsed():
    """tool_function 围栏内的 JSON 被解析成 tool_call，工具块从正文剥干净。"""
    content = "好的我来查。\n" + _fence('{"name": "get_weather", "arguments": {"city": "北京"}}')
    cleaned, tcs = parse_tool_calls_from_content(content)
    assert _names(tcs) == ["get_weather"]
    assert json.loads(tcs[0]["function"]["arguments"]) == {"city": "北京"}
    assert "get_weather" not in cleaned  # 工具块不泄漏
    assert "tool_function" not in cleaned
    assert "好的我来查" in cleaned  # 前置文本保留


def test_tool_function_fence_nested_arguments():
    """arguments 嵌套对象/数组也要正确解析。"""
    content = _fence('{"name": "tool", "arguments": {"opts": {"a": 1}, "items": [1, 2, 3]}}')
    _, tcs = parse_tool_calls_from_content(content)
    assert _names(tcs) == ["tool"]
    assert json.loads(tcs[0]["function"]["arguments"]) == {"opts": {"a": 1}, "items": [1, 2, 3]}


def test_multiple_tool_function_fences():
    """多个围栏块各自解析成独立 tool_call。"""
    content = _fence('{"name": "a", "arguments": {"x": 1}}') + "\n" + \
              _fence('{"name": "b", "arguments": {"y": 2}}')
    _, tcs = parse_tool_calls_from_content(content)
    assert _names(tcs) == ["a", "b"]


def test_malformed_json_in_fence_still_parsed():
    """围栏内缺引号的畸形 JSON 也要解析（json_repair 容错）。"""
    content = _fence('{name: "get_weather", arguments: {city: "北京"}}')
    _, tcs = parse_tool_calls_from_content(content)
    assert _names(tcs) == ["get_weather"]
    assert json.loads(tcs[0]["function"]["arguments"]) == {"city": "北京"}


def test_fence_without_name_field_not_a_tool_call():
    """围栏内 JSON 没有 name 字段不算工具调用。"""
    content = _fence('{"arguments": {"x": 1}}')
    _, tcs = parse_tool_calls_from_content(content)
    assert tcs == []


# ---- 围栏外反向断言：意图驱动核心，正文形态不当工具调用 ----

def test_bare_json_outside_fence_not_a_tool_call():
    """围栏外的裸 JSON（哪怕带 name 键）不当工具调用——正文里的 JSON 是给用户看的。"""
    content = '我先调工具 {"name": "file_read", "arguments": {"path": "main.py"}} 完成'
    _, tcs = parse_tool_calls_from_content(content)
    assert tcs == [], "围栏外的裸 JSON 被误判成工具调用"


def test_plain_json_code_block_not_a_tool_call():
    """普通 json 代码块（标注不是 tool_function）不当工具调用。"""
    content = '配置是 ```json\n{"name": "config", "timeout": 30}\n``` 这样'
    _, tcs = parse_tool_calls_from_content(content)
    assert tcs == [], "普通 json 代码块被误判成工具调用"


def test_xml_function_tags_parsed_as_tool_call():
    """XML ``<function=名称><parameter=键>值</parameter></function>`` 是协议词形态，要解析。

    改判理由：``<function=`` 是专属协议词，正文几乎不出现，模型写出它就是在调工具。
    弱模型按训练惯性只吐这种，不解析就等于工具调用被当正文漏给客户端、任务卡死。
    """
    content = " <function=get_weather><parameter=city>北京</parameter></function> 尾"
    cleaned, tcs = parse_tool_calls_from_content(content)
    assert _names(tcs) == ["get_weather"]
    assert json.loads(tcs[0]["function"]["arguments"]) == {"city": "北京"}
    assert "<function=" not in cleaned  # 块从正文剥干净


def test_hermes_tags_parsed_as_tool_call():
    """Hermes ``<tool_call>{...}</tool_call>`` 是协议词形态，要解析（内含 JSON）。"""
    hermes_open = "<" + "tool_call>"
    hermes_close = "</" + "tool_call>"
    content = "讨论 " + hermes_open + '{"name": "get_weather", "arguments": {"city": "北京"}}' + hermes_close + " 完"
    cleaned, tcs = parse_tool_calls_from_content(content)
    assert _names(tcs) == ["get_weather"]
    assert json.loads(tcs[0]["function"]["arguments"]) == {"city": "北京"}
    assert "tool_call>" not in cleaned


def test_hermes_with_arg_key_pairs_parsed():
    """Hermes 块内是「工具名 + arg_key/arg_value 参数对」（glm5.2 训练惯性形态），要解析。"""
    hermes_open = "<" + "tool_call>"
    hermes_close = "</" + "tool_call>"
    inner = "Read\n<arg_key>path</arg_key><arg_value>main.py</arg_value>"
    content = hermes_open + inner + hermes_close
    _, tcs = parse_tool_calls_from_content(content)
    assert _names(tcs) == ["Read"]
    assert json.loads(tcs[0]["function"]["arguments"]) == {"path": "main.py"}


def test_bare_json_still_not_a_tool_call():
    """裸 JSON（无任何协议词边界）仍然不解析——正文里贴 JSON 配置极常见，是真正的误抓来源。"""
    content = '我先调工具 {"name": "file_read", "arguments": {"path": "main.py"}} 完成'
    _, tcs = parse_tool_calls_from_content(content)
    assert tcs == [], "裸 JSON 被误判成工具调用"


# ---- 快速短路 / 孤儿闭合标签清理 ----

def test_plain_text_short_circuits_unchanged():
    """无 tool_function 标记的纯正文：整条解析短路，原文（去首尾空白）返回、零 tool_call。

    非流式路径对每个请求的 full_content 都调本函数，绝大多数是无工具对话，
    这条短路省掉的是最常见路径上的全部正则/平衡抽取成本。
    """
    content = "  普通对话正文，没有任何 tool_function 围栏  "
    cleaned, tcs = parse_tool_calls_from_content(content)
    assert tcs == []
    assert cleaned == "普通对话正文，没有任何 tool_function 围栏"


def test_content_can_contain_tool_calls_predicate():
    """快速判定：只有 tool_function 子串才算可能含工具调用。"""
    assert _content_can_contain_tool_calls(_fence('{"name":"x"}'))
    assert not _content_can_contain_tool_calls('{"name": "x"}')  # 裸 JSON 不算
    assert not _content_can_contain_tool_calls("```json\n{}\n```")  # json 围栏不算
    assert not _content_can_contain_tool_calls("</function> 片段")
    assert not _content_can_contain_tool_calls("普通正文没有特征")


def test_strip_orphan_closing_tags_keeps_matched_open():
    """有对应开标签的 </function> 不被误删；孤儿闭合标签全删。

    工具围栏闭合后模型可能多吐 </invoke> / </args> 等孤儿标签，必须清掉。
    """
    text = "<function=get_weather><parameter=city>x</parameter></function></invoke></args>"
    cleaned = _strip_all_orphan_closing_tags(text)
    assert "</function>" in cleaned
    assert "</parameter>" in cleaned
    assert "</invoke>" not in cleaned
    assert "</args>" not in cleaned


def test_strip_orphan_closing_tags_linear_complexity():
    """多闭合标签场景只做两遍线性扫描。"""
    text = "正文" + "</orphan>" * 500 + "<open>x</open>"
    cleaned = _strip_all_orphan_closing_tags(text)
    assert "</orphan>" not in cleaned
    assert "<open>x</open>" in cleaned


# ---- json_repair 容错解析 ----

def test_loads_tool_json_repairs_malformed():
    """LLM 常吐的畸形 JSON：缺引号 / 尾随逗号 / 被流截断，都要还原成对象。"""
    assert _loads_tool_json('{name: "x", arguments: {city: "BJ"}}') == {
        "name": "x", "arguments": {"city": "BJ"}}
    assert _loads_tool_json('{"name": "x", "arguments": {"a": 1,}}') == {
        "name": "x", "arguments": {"a": 1}}
    assert _loads_tool_json('{"name": "x", "arguments": {"city": "BJ"') == {
        "name": "x", "arguments": {"city": "BJ"}}


# ---- 流式切块：tool_function 围栏 ----

def test_fence_is_a_streaming_start_marker():
    """流式：find_tool_call_start 认 tool_function 围栏起点，否则整块泄漏成文本。"""
    content = "前置。" + _fence('{"name": "a", "arguments": {}}')
    assert find_tool_call_start(content) == len("前置。")
    prefix, blocks, rest = extract_complete_tool_calls(content)
    assert prefix == "前置。"
    assert len(blocks) == 1
    assert rest == ""
    _, calls = parse_tool_calls_from_content(blocks[0])
    assert _names(calls) == ["a"]


def test_incomplete_fence_held_in_buffer():
    """未闭合的 tool_function 围栏留 buffer 等后续 delta 补全，不当正文提前下发。"""
    content = "前置。" + _TOOL_FENCE_OPEN + '\n{"name": "file_read", "arguments": {"path": "main'
    prefix, blocks, rest = extract_complete_tool_calls(content)
    assert prefix == "前置。"
    assert blocks == []
    assert rest.startswith(_TOOL_FENCE_OPEN)


def test_fence_partial_prefix_held_back():
    """半截围栏开标记（反引号 / tool_func…）必须 hold 住，不能当正文下发。

    流式切块时 tool_function 开标记可能被切成多个 delta，尾部那截是标记前缀而非正文，
    必须留 buffer 等后续 delta 补全，否则前缀先泄漏成正文、后续补全的围栏再切不出来。
    """
    assert hold_partial_tool_marker_index("正文") == len("正文")
    # tool_function 开标记的各前缀长度都 hold
    assert hold_partial_tool_marker_index("正文`") == len("正文")
    assert hold_partial_tool_marker_index("正文``") == len("正文")
    assert hold_partial_tool_marker_index("正文```t") == len("正文")
    assert hold_partial_tool_marker_index("正文```tool") == len("正文")
    assert hold_partial_tool_marker_index("正文```tool_functio") == len("正文")  # 差一字符的半截前缀也 hold
    # 已含完整开标记 -> find_tool_call_start 接管，不再 hold（hold_start == len）
    full_open = "正文" + _TOOL_FENCE_OPEN
    assert hold_partial_tool_marker_index(full_open) == len(full_open)


def test_plain_brace_in_prose_not_held():
    """正文里的花括号 / json 前缀不被误 hold（围栏才是唯一标记）。"""
    assert hold_partial_tool_marker_index("正文{abc") == len("正文{abc")
    assert hold_partial_tool_marker_index('正文{"name"') == len('正文{"name"')
    assert hold_partial_tool_marker_index("正文```json") == len("正文```json")


# ---- 提示词：tool_function 围栏 + 具体示例 + 禁叙述 ----

def test_prompt_states_fence_boundaries():
    tools = [{"type": "function", "function": {
        "name": "Read", "description": "读取文件", "parameters": {"type": "object"}}}]
    prompt = generate_tools_prompt(tools, format_type="tool_function")
    assert _TOOL_FENCE_OPEN in prompt
    assert _TOOL_FENCE_CLOSE in prompt
    assert "开始边界" in prompt or "调用边界" in prompt
    assert "换行与缩进可有可无" in prompt


def test_prompt_uses_concrete_example_tool_name():
    """示例用第一个真实工具名，不用占位符。"""
    tools = [{"type": "function", "function": {
        "name": "file_read", "description": "读取文件",
        "parameters": {"type": "object", "properties": {"path": {"type": "string"}}}}}]
    prompt = generate_tools_prompt(tools)
    assert "file_read" in prompt
    assert "上面列出的工具名" not in prompt


def test_prompt_forbids_narrating_plan():
    """提示词禁止「只叙述意图不调用」并把首字符钉死在围栏边界。"""
    tools = [{"type": "function", "function": {
        "name": "Read", "description": "读取文件", "parameters": {"type": "object"}}}]
    prompt = generate_tools_prompt(tools)
    assert "不要用文字描述你打算执行的操作" in prompt
    assert "第一个字符" in prompt
    assert "意图描述" in prompt


def test_prompt_declares_outside_fence_not_tool_call():
    """提示词必须明确：围栏外的形态不当工具调用（意图驱动核心）。"""
    tools = [{"type": "function", "function": {
        "name": "Read", "description": "读取文件", "parameters": {"type": "object"}}}]
    prompt = generate_tools_prompt(tools)
    assert "围栏外" in prompt or "不得有任何工具调用形态" in prompt


def test_prompt_names_forbidden_native_formats():
    """弱模型（glm5.2 等）会按训练惯性吐 Hermes/arg_key/XML/json 围栏等形态。

    提示词必须逐条点名这些「错误形态」并声明无效——负面示例比抽象规则对弱模型
    更有约束力，这是掰过来 glm5.2 这类不听 ``tool_function`` 指令的模型的关键。
    锁住这条：删掉反例清单会回到「只说围栏外不解析」的弱约束，弱模型照样吐老格式。
    """
    tools = [{"type": "function", "function": {
        "name": "Read", "description": "读取文件", "parameters": {"type": "object"}}}]
    prompt = generate_tools_prompt(tools)
    # Hermes tool_call 标签被点名禁用
    assert "tool_call>" in prompt
    # arg_key/arg_value 参数标签被点名禁用
    assert "arg_key" in prompt and "arg_value" in prompt
    # XML function 标签被点名禁用
    assert "function=" in prompt or "<function>" in prompt
    # 普通 ```json 围栏被点名禁用（标注必须逐字是 tool_function）
    assert "```json" in prompt
    # 明确声明这些形态「无效」/「不解析」
    assert "无效" in prompt or "不解析" in prompt


def test_prompt_format_type_aliases_unified():
    """xml/json/hermes 三 alias 都路由到同一围栏产出（向后兼容）。"""
    tools = [{"type": "function", "function": {
        "name": "Read", "description": "读取文件", "parameters": {"type": "object"}}}]
    for ft in ("xml", "json", "hermes", "tool_function"):
        prompt = generate_tools_prompt(tools, format_type=ft)
        assert _TOOL_FENCE_OPEN in prompt, f"format_type={ft} 未产出 tool_function 围栏"


def test_render_tool_call_json_produces_fence():
    """render_tool_call_json 产出 tool_function 围栏（与解析器同形态，历史轮回填自洽）。"""
    tc = {"id": "c1", "type": "function",
          "function": {"name": "get_weather", "arguments": '{"city": "北京"}'}}
    rendered = render_tool_call_json(tc)
    assert rendered.startswith(_TOOL_FENCE_OPEN)
    assert rendered.endswith(_TOOL_FENCE_CLOSE)
    # 渲染产物能被解析器认回
    _, parsed = parse_tool_calls_from_content(rendered)
    assert _names(parsed) == ["get_weather"]
    assert json.loads(parsed[0]["function"]["arguments"]) == {"city": "北京"}


# ---- tool_function 尖括号变体：同一协议词的语法变体，必须同样解析 ----

def test_xml_variant_parsed():
    """``<tool_function>{...}</tool_function>`` 被解析成 tool_call，块从正文剥干净。

    锁这条的理由：弱模型（glm5.2 等）按训练惯性把边界写成 XML 标签而非围栏，但它
    写出了 tool_function 这个专属协议词——调用意图已经明确表达。不认它的代价是真实的：
    模型想调用 -> 被当正文下发 -> 工具没执行、任务卡死，用户只看到一段像工具调用的文本。
    """
    content = "好的。" + _xml('{"name": "Read", "arguments": {"path": "main.py"}}')
    cleaned, tcs = parse_tool_calls_from_content(content)
    assert _names(tcs) == ["Read"]
    assert json.loads(tcs[0]["function"]["arguments"]) == {"path": "main.py"}
    assert "tool_function" not in cleaned  # 块不泄漏
    assert cleaned == "好的。"  # 前置文本保留


def test_xml_variant_with_attributes_parsed():
    """开标签带多余属性（``<tool_function name="x">``）也要解析——模型常自作主张加属性。"""
    content = '<tool_function name="Grep">{"name": "Grep", "arguments": {}}</tool_function>'
    _, tcs = parse_tool_calls_from_content(content)
    assert _names(tcs) == ["Grep"]


def test_xml_variant_is_a_streaming_start_marker():
    """流式：尖括号变体也是切块起点，否则整块泄漏成文本。"""
    content = "前置。" + _xml('{"name": "a", "arguments": {}}') + "尾巴"
    assert find_tool_call_start(content) == len("前置。")
    prefix, blocks, rest = extract_complete_tool_calls(content)
    assert prefix == "前置。"
    assert len(blocks) == 1
    assert rest == "尾巴"
    _, calls = parse_tool_calls_from_content(blocks[0])
    assert _names(calls) == ["a"]


def test_incomplete_xml_variant_held_in_buffer():
    """未闭合的尖括号变体留 buffer 等后续 delta，不当正文提前下发。"""
    content = "前置。" + _TOOL_XML_OPEN + '>{"name": "file_read", "arguments": {"path": "main'
    prefix, blocks, rest = extract_complete_tool_calls(content)
    assert prefix == "前置。"
    assert blocks == []
    assert rest.startswith(_TOOL_XML_OPEN)


def test_xml_variant_partial_prefix_held_back():
    """半截尖括号开标记（``<`` / ``<tool_functio``）必须 hold 住，不能当正文下发。"""
    assert hold_partial_tool_marker_index("正文<") == len("正文")
    assert hold_partial_tool_marker_index("正文<t") == len("正文")
    assert hold_partial_tool_marker_index("正文<tool_functio") == len("正文")


def test_both_boundaries_in_one_content():
    """围栏与尖括号变体混在同一段里，两个调用都要解析出来（去重不误杀不同调用）。"""
    content = _fence('{"name": "a", "arguments": {"x": 1}}') + "\n中间\n" + \
              _xml('{"name": "b", "arguments": {"y": 2}}')
    cleaned, tcs = parse_tool_calls_from_content(content)
    assert _names(tcs) == ["a", "b"]
    assert "tool_function" not in cleaned
    assert "中间" in cleaned


def test_content_can_contain_tool_calls_accepts_both_boundaries():
    """快速判定按协议词 tool_function 走，两种边界都算「可能含工具调用」。"""
    assert _content_can_contain_tool_calls(_fence('{"name":"x"}'))
    assert _content_can_contain_tool_calls(_xml('{"name":"x"}'))
    assert not _content_can_contain_tool_calls('{"name": "x"}')  # 裸 JSON 不算
    assert not _content_can_contain_tool_calls("普通正文没有特征")


def test_prompt_recommends_fence_as_primary_form():
    """提示词把围栏定为推荐形态（尖括号变体是容错兜底，不主动教）。"""
    tools = [{"type": "function", "function": {
        "name": "Read", "description": "读取文件", "parameters": {"type": "object"}}}]
    prompt = generate_tools_prompt(tools)
    assert _TOOL_FENCE_OPEN in prompt
    assert "唯一推荐" in prompt or "始终用" in prompt


def test_prompt_forbids_stating_intent_without_calling():
    """提示词必须专门治「表达了调用意图却没真调用」——本轮实测最常见的失败模式。

    锁这条：模型说「我将读取该文件」然后结束回复，工具不执行、任务卡死。抽象规则
    （「不要只叙述」）对弱模型不够，必须有独立段落 + 具体反例句式点名这种失败。
    """
    tools = [{"type": "function", "function": {
        "name": "Read", "description": "读取文件", "parameters": {"type": "object"}}}]
    prompt = generate_tools_prompt(tools)
    # 独立段落点名这个失败模式
    assert "想调用就必须真的发出调用" in prompt
    # 具体反例句式（模型最常吐的那几种叙述）
    assert "我将读取" in prompt or "接下来我会" in prompt or "让我先看" in prompt
    # 明确「想调用 = 立刻调用，没有中间状态」
    assert "立刻发出调用" in prompt or "没有中间状态" in prompt
    # 禁止把调用推迟到下一轮
    assert "下一轮" in prompt
