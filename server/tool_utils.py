"""工具相关工具方法"""
import json
import re
import uuid
from loguru import logger

# 容错 JSON 还原：LLM 吐的工具调用 JSON 常缺引号 / 尾随逗号 / 被流截断 / 单引号，
# json_repair 能把这些还原成合法对象（参考 tooluser/tools4all 等库的统一做法）。
# optional import：环境没装 json_repair 时回退 json.loads，保持旧解析能力不退化。
try:  # pragma: no cover - 装了就走这条
    from json_repair import repair_json as _repair_json
except Exception:  # noqa: BLE001 - 任何导入失败都静默回退
    _repair_json = None


def _loads_tool_json(text: str):
    """解析工具调用 JSON：优先 json_repair（容错），缺失则 json.loads。

    json_repair 把 ``{name: "x", arguments: {city: "北京"}}`` （缺引号）、
    ``{"a": 1,}`` （尾随逗号）、被流截断的半截 JSON 都还原成合法对象；
    json.loads 只认严格合法 JSON，任何瑕疵都整段丢。
    """
    if _repair_json is not None:
        try:
            return _repair_json(text, return_objects=True)
        except Exception:  # noqa: BLE001 - 容错库偶发抛错时仍回退严格解析
            pass
    return json.loads(text)


# 工具调用的统一边界：三反引号围栏 + 专属标注 tool_function。
# 选围栏形态（而非 XML 标签）是为了抗上游注入的超长中文 system prompt 噪音——
# 围栏边界在噪音里更显眼、更稳定（eaichat 当初弃 Hermes 标签选围栏的理由）。
# 标注用 tool_function 而非泛 json：正文里 json 代码块常见（配置示例、代码片段），
# 会与工具调用混淆；tool_function 正文几乎不出现，看到它就是模型明确表示
# 「这里要调用工具」——意图驱动，不再凭「长得像」误抓正文 JSON。
_TOOL_FENCE_OPEN = "```tool_function"
_TOOL_FENCE_CLOSE = "```"
_TOOL_FENCE_RE = re.compile(
    re.escape(_TOOL_FENCE_OPEN) + r"\s*([\s\S]*?)\s*" + re.escape(_TOOL_FENCE_CLOSE)
)

# 尖括号变体：``<tool_function>{...}</tool_function>``。这不是"又开一种格式"，而是
# **同一个协议词 tool_function 的语法变体**——模型已经明确表达了「这里要调用工具」的
# 意图（tool_function 正文几乎不出现），只是按训练惯性把边界写成了 XML 标签而非围栏。
# ``[^>]*`` 容忍 ``<tool_function name="x">`` 这类多余属性；``\s*`` 容忍闭合标签内空格。
_TOOL_XML_OPEN = "<tool_function"
_TOOL_XML_CLOSE = "</tool_function>"
_TOOL_XML_RE = re.compile(
    r"<tool_function\b[^>]*>\s*([\s\S]*?)\s*</tool_function\s*>", re.IGNORECASE
)

# ---- 训练惯性形态：Hermes 标签 / arg_key 参数对 / XML function 标签 ----
# 这三种同样是**协议词**，不是"长得像"：``tool_call`` / ``arg_key`` / ``function=`` 在
# 正常正文里几乎不会出现，模型写出它们就是在明确表达「这里要调用工具」。弱模型
# （glm5.2 等）按训练惯性只会吐这些，提示词压不住——不解析的代价是模型明明在调工具，
# 却被当正文漏给客户端、工具不执行、任务卡死（本轮实测到的就是这个）。
#
# 与之相对，**裸 JSON 与泛 ```json 围栏仍然不解析**：正文里贴 JSON 配置 / 代码示例极常见，
# 那才是真正的误抓来源。判据始终是「有没有专属协议词」，不是「结构像不像工具调用」。
_HERMES_OPEN = "<" + "tool_call>"
_HERMES_CLOSE = "</" + "tool_call>"
_HERMES_BLOCK_RE = re.compile(
    re.escape(_HERMES_OPEN) + r"\s*([\s\S]*?)\s*" + re.escape(_HERMES_CLOSE), re.IGNORECASE
)
# arg_key/arg_value 成对参数（glm5.2 形态：块内首行是工具名，随后若干参数对）
_ARG_PAIR_RE = re.compile(
    r"<arg_key>\s*([\s\S]*?)\s*</arg_key>\s*<arg_value>\s*([\s\S]*?)\s*</arg_value>",
    re.IGNORECASE,
)
# XML function 标签：``<function=名称><parameter=键>值</parameter></function>``
_XML_FUNC_OPEN = "<" + "function="
_XML_FUNC_CLOSE = "</" + "function>"
_XML_FUNC_BLOCK_RE = re.compile(
    r"<function=\s*([\w.\-]+)\s*>\s*([\s\S]*?)\s*</function\s*>", re.IGNORECASE
)
_XML_PARAM_RE = re.compile(
    r"<parameter=\s*([\w.\-]+)\s*>\s*([\s\S]*?)\s*</parameter\s*>", re.IGNORECASE
)

# 所有边界的开标记全集：流式切块 / 短路判定 / holdback 都按这个集合走，
# 加新形态只改这里 + 对应的 close 映射。
_TOOL_OPEN_MARKERS = (
    _TOOL_FENCE_OPEN, _TOOL_XML_OPEN, _HERMES_OPEN, _XML_FUNC_OPEN,
)
_TOOL_CLOSE_BY_OPEN = {
    _TOOL_FENCE_OPEN: _TOOL_FENCE_CLOSE,
    _TOOL_XML_OPEN: _TOOL_XML_CLOSE,
    _HERMES_OPEN: _HERMES_CLOSE,
    _XML_FUNC_OPEN: _XML_FUNC_CLOSE,
}
# 快速短路用的协议词全集：正文里一个都没有 -> 一定没有工具调用，整条解析跳过。
_TOOL_PROTOCOL_WORDS = ("tool_function", "tool_call>", "arg_key>", "<function=")

# 提示词里「反面教材」用的错误形态字面量。集中一处，日后加新反例只改这里。
_HERMES_OPEN_LITERAL = _HERMES_OPEN
_HERMES_CLOSE_LITERAL = _HERMES_CLOSE
_ARG_KEY_LITERAL = "<" + "arg_key>"
_ARG_VALUE_LITERAL = "<" + "arg_value>"
_XML_FUNC_LITERAL = "<" + "function=名称>...</function>"
_JSON_FENCE_LITERAL = "```json"


def generate_tools_prompt(tools: list[dict], format_type: str = "tool_function") -> str:
    """根据 OpenAI tools 格式生成工具描述 prompt。

    统一教模型用 ```tool_function 围栏吐工具调用——意图驱动：模型必须用正文里
    几乎不出现的专属围栏边界明确表示「这里要调用工具」，解析器只认这个边界内的
    内容；边界外的裸 JSON / 普通 ```json 代码块 / XML / Hermes 标签一律不当工具调用，
    避免把正文里贴的 JSON 配置示例误判成工具调用执行。

    format_type 参数保留以兼容既有调用与渠道配置（xml/json/hermes），语义已收敛为
    同一围栏、同一 JSON——三 alias 都路由到同一个统一生成器。
    """
    if not tools:
        return ""
    return _generate_tools_prompt(tools)


def _generate_tools_prompt(tools: list[dict]) -> str:
    """工具调用转换器提示词。

    定位是「转换器」：先判断是否需要工具 → 选工具 → 造参数 → 吐合法调用，不是单纯
    教模型用围栏。结构沿用转换器十二章节：判断 / 选择 / 参数 / 区分 / 格式 / 严格 JSON /
    防伪造 / 处理结果 / 多工具 / 最少必要 / 不可用与不足 / 最终决策。

    围栏是唯一边界（意图驱动：解析器只认 tool_function 协议词的两种边界——围栏与尖括号
    变体）。反例里**不出现 tool_function 这个词**，防模型照抄反例触发解析；反例用别的
    标签名（tool_call / function= / arg_key / json 围栏）。用第一个真实工具造具体示例，
    弱模型对具体示例遵从远好于占位符。
    """
    tool_lines = []
    first_name = ""
    first_sample_args: dict = {}
    for tool in tools:
        if tool.get("type") != "function":
            continue
        func = tool.get("function", {})
        name = func.get("name", "")
        desc = func.get("description", "")
        params = func.get("parameters", {}) or {}
        props = params.get("properties") or {}
        required = params.get("required") or []
        param_parts = []
        for pname, pinfo in props.items():
            ptype = pinfo.get("type", "any")
            pdesc = (pinfo.get("description") or "").strip()
            req = "必填" if pname in required else "可选"
            param_parts.append(f"  - {pname}（{ptype}，{req}）：{pdesc}")
        params_block = "\n".join(param_parts) if param_parts else "  （无参数）"
        tool_lines.append(f"- **{name}**：{desc}\n  参数：\n{params_block}")
        if not first_name and name:
            first_sample_args = {k: "value" for k in list(props)[:2]} if props else {}
            first_name = name
    tool_list = "\n".join(tool_lines)
    # 用第一个真实工具造具体示例，不用占位符——弱模型对具体示例遵从远好于占位符。
    example_obj = json.dumps(
        {"name": first_name or "tool_name", "arguments": first_sample_args},
        ensure_ascii=False,
    )
    example = f"{_TOOL_FENCE_OPEN}\n{example_obj}\n{_TOOL_FENCE_CLOSE}"

    return f"""# 工具调用转换器

你是一个**工具调用转换器**：把用户请求转成对可用工具的调用。你的价值在于**执行**，不在于说明。

## 最高规则（先读这条）
只要任务能用下面的工具推进，就**立即发出工具调用**——本次回复的第一个字符就是调用围栏的起始，调用之前**一个字都不要写**。
- 不要写"好的 / 我来帮你 / 我先看一下 / 让我读取 / 我需要调用 X / 接下来我会…"这类话。
- 不要复述任务、不要解释你的计划、不要意图描述、不要把判断过程写成文字。判断只在心里做，输出只有两种：**要么是工具调用围栏，要么是最终答案**，没有第三种。
- 不要用文字描述你打算执行的操作；"说要调用"不等于"调用了"。只要你写出了"我将去 xxx"却没发出围栏，本次回复就是失败的——直接改成发出调用。

**想调用就必须真的发出调用。** 想调用 = 立刻发出调用，没有中间状态。把调用推迟到"下一轮"等于没调。

## 何时调用 vs 何时直接回答
- **需要调用**：读取 / 搜索 / 查询信息、执行命令、增删改文件、调用外部能力、答案依赖工具返回的数据。**拿不准时，倾向调用**——漏调让任务卡住，比多调一次代价大。
- **直接回答**（不调用）：纯知识问答、翻译 / 解释 / 总结、仅凭已有上下文就能完整回答的问题。这种情况直接给最终答案，不要生造调用。

## 可用工具
{tool_list}

只能用上面列出的工具，工具名逐字一致，不虚构工具。选能直接完成任务的那个。

## 调用格式（唯一合法形态）
{example}

- 起止 ``{_TOOL_FENCE_OPEN}`` … ``{_TOOL_FENCE_CLOSE}`` 是精确的调用边界，是"这里要调用工具"的唯一信号；**围栏外**的一切都被当普通文本，不执行。
- 围栏内是 JSON 对象，只含两个键：`name`（从上面逐字复制的工具名）与 `arguments`（参数对象，无参数用 `{{}}`）。
- 换行与缩进可有可无，不影响解析。

## 参数
- 参数名 / 类型必须符合工具定义，必填项必须给，不加未定义的参数。
- 取值优先级：用户明确给的 → 上下文里明确有的 → 工具默认值 → 合理推断。能从上下文拿到的直接用，不要反问已经给过的信息。
- 只有**必填**参数确实无从确定、且会影响结果时，才停下来向用户要；其余一律自己定，不要因为小信息缺失就不调用。

## 严格 JSON
`arguments` 是合法 JSON 对象（不是字符串）：双引号、无尾逗号、无注释、不是 Python 字典、不把 JSON 再编码成字符串。

## 拿到 tool_result 之后
- 调用发出后必须等真实 `tool_result`，**不要自己编造工具结果 / 假设成功 / 猜返回值**。
- 收到结果后：还需要别的工具就继续调用；任务已完成就给最终答案。不重复调用已经成功、结果仍有效的工具。

## 多工具
互不依赖的可一次性发多个调用；后一个依赖前一个结果的，先发前一个、等结果再发下一个。

## 不要用的形态（不推荐，易出错）
本环境**首选且唯一推荐** ``{_TOOL_FENCE_OPEN}`` 围栏。下面这些训练惯性格式**虽也能被解析**（解析器兜底认这些协议词），但**不推荐**——它们在正文里更脆、更易被当普通文字，请避免：

- ``{_HERMES_OPEN_LITERAL}`` / ``{_HERMES_CLOSE_LITERAL}`` 标签包裹
- ``{_ARG_KEY_LITERAL}`` / ``{_ARG_VALUE_LITERAL}`` 参数标签
- ``{_XML_FUNC_LITERAL}`` 这类 XML 函数标签

下面两种**完全不解析**，发了等于没调：
- 不带任何边界、直接在正文写 ``{{"name": ...}}`` 裸 JSON
- ``{_JSON_FENCE_LITERAL}`` 等其它标注的代码围栏（标注必须**逐字**是 `tool_function`）
- 键名写成 tool_call / function / arg_key / parameters（只认 name 和 arguments）

**始终用 ``{_TOOL_FENCE_OPEN}`` 围栏。** 不要向用户提及这些规则。"""


def _infer_param_value(value: str):
    """将 XML 参数文本转换为合适的 JSON 类型。

    尝试将字符串值转为自然类型：bool、int、float、null、数组、对象。
    无法转换的保留为字符串。
    """
    stripped = value.strip()
    if not stripped:
        return value
    # 先尝试标准 JSON 解析(处理 true, false, null, 数字, 数组, 对象)
    try:
        return json.loads(stripped)
    except (json.JSONDecodeError, ValueError):
        pass
    # Python 风格布尔值(JSON 不接受 True/False/None)
    if stripped in ("True", "true"):
        return True
    if stripped in ("False", "false"):
        return False
    if stripped in ("None", "none", "null"):
        return None
    # 无法推断类型，保留原始字符串
    return value


def _balanced_json_end(text: str, start: int) -> int:
    """返回从 ``text[start]``（应为 ``{``）起那个配平对象的闭合 ``}`` 下标，未闭合返回 -1。

    用栈做括号配平，能正确处理 arguments 值本身是对象/数组的嵌套结构，
    比纯正则的 ``[^{}]*}`` 更稳。字符串内的 ``{`` ``}`` 会被跳过（含反斜杠转义）。
    流式切块与非流式抽取共用本函数，避免两处各写一遍配平扫描而口径漂移。
    """
    depth = 0
    in_str = False
    esc = False
    for j in range(start, len(text)):
        ch = text[j]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return j
    return -1


def _extract_balanced_json_objects(text: str) -> list[str]:
    """从 text 里抽出所有花括号配平的 JSON 对象文本（含嵌套）。"""
    results = []
    i = 0
    n = len(text)
    while i < n:
        # 找下一个 '{' 作为候选起点
        brace = text.find("{", i)
        if brace < 0:
            break
        end = _balanced_json_end(text, brace)
        if end < 0:
            # 不闭合，从这里之后再找
            i = brace + 1
            continue
        results.append(text[brace:end + 1])
        i = end + 1
    return results


def _try_json_tool_call(json_text: str) -> dict | None:
    """把一段 JSON 文本解析成 tool_call dict，要求带 name 字段，否则返回 None。

    用 `_loads_tool_json`（json_repair 优先，容错缺引号/尾随逗号/截断）而非裸 json.loads，
    所以模型吐 ``{name: "x", arguments: {city: "北京"}}`` （缺引号）也能解析成工具调用。
    """
    try:
        data = _loads_tool_json(json_text)
    except (json.JSONDecodeError, ValueError, TypeError):
        return None
    if not isinstance(data, dict) or not data.get("name"):
        return None
    args = data.get("arguments")
    if isinstance(args, dict):
        args_str = json.dumps(args, ensure_ascii=False)
    elif isinstance(args, str):
        # 模型可能把 arguments 写成 JSON 字符串，直接用
        try:
            json.loads(args)
            args_str = args
        except (json.JSONDecodeError, ValueError):
            args_str = json.dumps({"_raw": args}, ensure_ascii=False)
    else:
        args_str = json.dumps({}, ensure_ascii=False)
    return {
        "id": f"call_{uuid.uuid4().hex[:24]}",
        "type": "function",
        "function": {"name": data["name"], "arguments": args_str},
    }


def _add_tool_call(tool_calls: list, seen: set, tc: dict | None) -> None:
    """去重后追加 tool_call（同 name+arguments 只计一次）。"""
    if not tc:
        return
    key = (tc["function"]["name"], tc["function"]["arguments"])
    if key in seen:
        return
    seen.add(key)
    tool_calls.append(tc)


def _tool_call_from_name_args(name: str, args: dict) -> dict | None:
    """用工具名 + 参数 dict 直接组一个 OpenAI tool_call。"""
    if not name:
        return None
    return {
        "id": f"call_{uuid.uuid4().hex[:24]}",
        "type": "function",
        "function": {"name": name, "arguments": json.dumps(args, ensure_ascii=False)},
    }


def _parse_arg_pair_block(block: str) -> dict | None:
    """解析 arg_key/arg_value 形态块：块内首个非空行是工具名，随后是参数对。

    glm5.2 等模型的训练惯性形态：
        <tool_call>Read
        <arg_key>path</arg_key><arg_value>main.py</arg_value>
        </tool_call>
    也兼容参数对里嵌 JSON 值（走 _infer_param_value 推断类型）。
    """
    pairs = _ARG_PAIR_RE.findall(block)
    if not pairs:
        return None
    # 工具名 = 第一个 arg_key 之前的那段文本里的首个非空行
    head = block[:_ARG_PAIR_RE.search(block).start()]
    name = ""
    for line in head.splitlines():
        stripped = line.strip()
        if stripped:
            name = stripped
            break
    if not name:
        return None
    args = {k.strip(): _infer_param_value(v) for k, v in pairs}
    return _tool_call_from_name_args(name, args)


def _parse_hermes_blocks(content: str, tool_calls: list, seen: set) -> None:
    """解析 Hermes ``<tool_call>`` 块：块内既可能是 JSON，也可能是 arg_key 参数对。"""
    for m in _HERMES_BLOCK_RE.finditer(content):
        inner = m.group(1)
        # 形态 A：块内是 JSON 对象（带 name / arguments）
        matched = False
        for obj_text in _extract_balanced_json_objects(inner):
            tc = _try_json_tool_call(obj_text)
            if tc:
                _add_tool_call(tool_calls, seen, tc)
                matched = True
        if matched:
            continue
        # 形态 B：块内是「工具名 + arg_key/arg_value 参数对」
        _add_tool_call(tool_calls, seen, _parse_arg_pair_block(inner))


def _parse_xml_function_blocks(content: str, tool_calls: list, seen: set) -> None:
    """解析 ``<function=名称><parameter=键>值</parameter></function>`` 形态。"""
    for m in _XML_FUNC_BLOCK_RE.finditer(content):
        name = (m.group(1) or "").strip()
        inner = m.group(2) or ""
        args = {k.strip(): _infer_param_value(v)
                for k, v in _XML_PARAM_RE.findall(inner)}
        _add_tool_call(tool_calls, seen, _tool_call_from_name_args(name, args))


def _parse_json_tool_calls(content: str, tool_calls: list) -> None:
    """从 content 里抽出所有协议词边界内的工具调用，追加进 tool_calls。

    认四种边界（都以专属**协议词**为判据，不是"结构长得像"）：
    1. ``  tool_function ... ``` `` 围栏（推荐形态，提示词只教这个）
    2. ``<tool_function>...</tool_function>`` 尖括号变体
    3. ``<tool_call>...</tool_call>`` Hermes 块（内含 JSON 或 arg_key 参数对）
    4. ``<function=名称><parameter=键>值</parameter></function>`` XML 形态

    后三种是弱模型（glm5.2 等）的训练惯性形态，提示词压不住；不解析的代价是模型
    明明在调工具却被当正文漏给客户端、工具不执行、任务卡死。

    **裸 JSON 与泛 ```json 围栏仍然不解析**——正文里贴 JSON 配置/代码示例极常见，
    那才是真正的误抓来源。用集合去重 (name, arguments) 避免同一调用计两次。
    """
    seen: set = set()
    for regex in (_TOOL_FENCE_RE, _TOOL_XML_RE):
        for m in regex.finditer(content):
            for obj_text in _extract_balanced_json_objects(m.group(1)):
                _add_tool_call(tool_calls, seen, _try_json_tool_call(obj_text))
    _parse_hermes_blocks(content, tool_calls, seen)
    _parse_xml_function_blocks(content, tool_calls, seen)


def _content_can_contain_tool_calls(content: str) -> bool:
    """快速判定 content 是否可能含工具调用形态，不可能则整条解析直接短路。

    判据是**协议词**：``tool_function`` / ``tool_call>`` / ``arg_key>`` / ``<function=``。
    正文里一个都没有就一定没有工具调用，整条解析短路、原文返回。普通对话正文在此
    几次子串扫描即可返回，不再跑 JSON 平衡抽取与多形态正则。
    """
    return any(word in content for word in _TOOL_PROTOCOL_WORDS)


def parse_tool_calls_from_content(content: str) -> tuple[str, list[dict]]:
    """从模型输出中解析工具调用（意图驱动：认协议词边界，不认「长得像」）。

    认四种协议词边界（见 _parse_json_tool_calls）：``  tool_function`` 围栏（推荐）、
    ``<tool_function>`` 尖括号变体、``<tool_call>`` Hermes 块、``<function=>`` XML 形态。
    裸 JSON / ```json 代码块**不当工具调用**——正文里贴这些就是正文，不执行。

    返回:
        tuple[str, list[dict]]: (清理后的内容, 工具调用列表)
    """
    tool_calls: list[dict] = []

    # 快速短路：没有任何协议词时，整条解析跳过全部正则/平衡抽取，原文返回。非流式
    # 路径对每个请求的 full_content 都调本函数，绝大多数请求是无工具的普通对话，
    # 这条短路省掉的是最常见路径上的全部解析成本。
    if not _content_can_contain_tool_calls(content):
        return content.strip(), tool_calls

    _parse_json_tool_calls(content, tool_calls)

    if tool_calls:
        logger.debug(f"[parse_tool_calls] total={len(tool_calls)}, "
                     f"names={[tc['function']['name'] for tc in tool_calls]}")
        for tc in tool_calls:
            logger.debug(f"[parse_tool_calls.detail] name={tc['function']['name']}, "
                         f"arguments={tc['function']['arguments'][:200]}")

    # 清理：剥掉四种协议词边界段，边界外的正文（含裸 JSON / ```json 等）原样保留
    # ——那些是给用户看的正文，不是工具调用残留。这一步保证客户端拿不到任何
    # 工具调用形态的残留文本（用户报的「调用还是被发给客户端」就是漏了这步的清理）。
    cleaned = re.sub(
        re.escape(_TOOL_FENCE_OPEN) + r"[\s\S]*?" + re.escape(_TOOL_FENCE_CLOSE),
        '',
        content,
    )
    cleaned = _TOOL_XML_RE.sub('', cleaned)
    cleaned = _HERMES_BLOCK_RE.sub('', cleaned)
    cleaned = _XML_FUNC_BLOCK_RE.sub('', cleaned)
    return cleaned.strip(), tool_calls


def find_tool_call_start(content: str) -> int:
    """返回最靠前的 tool_function 边界起始位置，未找到返回 -1。

    意图驱动：认两种边界——``  tool_function `` 围栏开标记与 ``<tool_function`` 尖括号
    开标签（同一协议词的语法变体）。其它形态（<function= XML / Hermes <tool_call> /
    裸 JSON）不是工具调用，不在这里探测。多种边界并存时取最靠前的命中点。
    """
    starts = [i for i in (content.find(m) for m in _TOOL_OPEN_MARKERS) if i >= 0]
    return min(starts) if starts else -1


_ORPHAN_CLOSE_RE = re.compile(r'\s*</([\w:.-]+)>')


def _strip_orphan_closing_tags(text: str) -> str:
    """吃掉开头那些"没有对应开标签"的闭合标签。

    模型在工具围栏闭合后可能多吐一截残缺收尾（如 ``</tool_function>`` / ``</args>`` /
    ``</invoke>``）——这些标签从没被打开过，纯属格式噪音。不吞掉的话会随 buffer 流到
    出口，被当普通文本发给客户端，客户端就看到工具调用后面挂一串尖括号垃圾。

    判定标准是"孤儿"而非白名单：只有当 ``<name`` 在文本里根本没出现过时才吞，
    所以 ``</function>`` / ``</parameter>`` 这种有开标签的正常结构不会被误删。
    """
    while True:
        match = _ORPHAN_CLOSE_RE.match(text)
        if not match:
            return text
        name = match.group(1)
        if re.search(r'<' + re.escape(name) + r'[\s>=]', text):
            return text
        text = text[match.end():]


def _strip_all_orphan_closing_tags(text: str) -> str:
    """删除全文里所有"没有对应开标签"的闭合标签。

    与 ``_strip_orphan_closing_tags`` 同口径，但扫描整个字符串而非仅开头，
    供 ``parse_tool_calls_from_content`` 出口做最终清理：工具围栏正则删完后，
    尾部残留的孤儿闭合标签也要一并清掉，否则会当正文下发给客户端。

    实现：先一遍扫出文本里出现过的所有"开标签名"集合（``<name`` 后跟空白/``>``/``=``），
    再一遍扫所有闭合标签 ``</name>``，名字不在开标签集合里的就是孤儿，整段删掉。
    两遍线性扫描，避免旧实现对每个闭合标签都 ``re.search`` 全文的 O(n²)。
    """
    open_names = {m.group(1) for m in re.finditer(r'<([A-Za-z_:][\w:.\-]*)[\s>=]', text)}
    result = []
    pos = 0
    for match in _ORPHAN_CLOSE_RE.finditer(text):
        if match.group(1) in open_names:
            result.append(text[pos:match.end()])
        else:
            result.append(text[pos:match.start()])
        pos = match.end()
    result.append(text[pos:])
    return ''.join(result)


def extract_complete_tool_calls(content: str) -> tuple[str, list[str], str]:
    """从缓冲文本中切出完整的 tool_function 块，返回 (前置文本, 完整块列表, 剩余未完成文本)。

    意图驱动：切两种边界的块——``  tool_function ... ``` `` 围栏与
    ``<tool_function>...</tool_function>`` 尖括号变体。每种边界有各自的闭合标记，
    切到当前块时按其开标记对应的 close 找闭合；闭合了整块切出交 parse 判断是否工具调用；
    未闭合留 buffer 等后续 delta 补全。围栏外的裸 JSON / XML <function> / Hermes
    标签不是工具调用，不在这里切块，原样当下发文本。
    """
    start = find_tool_call_start(content)
    if start < 0:
        return content, [], ""

    prefix = content[:start]
    rest = content[start:]
    blocks = []

    while rest:
        start = find_tool_call_start(rest)
        if start < 0:
            return prefix, blocks, rest
        if start > 0:
            prefix += rest[:start]
            rest = rest[start:]

        # 判定当前块是哪种边界 -> 取对应的开标记长度与闭合标记。find_tool_call_start
        # 找的必是 _TOOL_OPEN_MARKERS 之一，按前缀匹配回溯到那个开标记。
        open_marker = next((m for m in _TOOL_OPEN_MARKERS if rest.startswith(m)), None)
        if open_marker is None:
            # 理论上不会到这里（前缀匹配必命中其一）；防御性跳一字避免死循环
            prefix += rest[0]
            rest = rest[1:]
            continue
        close_marker = _TOOL_CLOSE_BY_OPEN[open_marker]
        # 在开标记之后找闭合标记；找不到说明块未闭合，留 buffer 等后续 delta。
        close_idx = rest.find(close_marker, len(open_marker))
        if close_idx < 0:
            return prefix, blocks, rest
        end = close_idx + len(close_marker)

        block = rest[:end]
        blocks.append(block)
        rest = rest[end:]
        # 闭合后紧跟的孤儿闭合标签属于模型格式噪音，不能当正文下发
        rest = _strip_orphan_closing_tags(rest)

    return prefix, blocks, ""


def check_tool_call_start(content: str) -> bool:
    """检测内容是否包含工具调用开始标记（任一 tool_function 边界开标记）。"""
    return find_tool_call_start(content) >= 0


# 流式 holdback 用的工具开标记全集：模型把开标记逐字符吐出来时，半截标记
# （围栏：`` ` `` → `` `` ` `` → `` ```t `` …；尖括号：`` < `` → `` <t `` → `` <tool_functio `` …）
# 绝不能当正文下发，否则客户端会看到半截标记垃圾、且后续补全的边界再也切不出来。
# 两种开标记的前缀链都要 hold。顺序无关，取最靠前的命中点。代价：正文末尾恰好是
# `` ` `` 或 `` < `` 时会被多 hold 一两字符，等下个 delta 到就放出——只是延迟，不丢字符。
_PARTIAL_TOOL_MARKERS = (_TOOL_FENCE_OPEN, _TOOL_XML_OPEN)


def hold_partial_tool_marker_index(text: str) -> int:
    """返回 text 中「应该开始 hold 住不下发」的下标；无半截标记则返回 len(text)。

    流式出口在还没切出完整工具块时调用：```tool_function 开标记可能被切成多个 delta
    （`` ` `` → `` `` ` `` → `` ```t `` → …），此时尾部那截是标记前缀而非正文，必须留在
    buffer 里等后续 delta 补全。单一真相源放这里，避免 base.py 各自维护一份标记表——
    漏一个标记就等于该形态的工具调用在流式路径整体泄漏成文本。
    """
    hold_start = len(text)
    for marker in _PARTIAL_TOOL_MARKERS:
        for size in range(1, min(len(marker), len(text)) + 1):
            # 完整开标记交给 find_tool_call_start 接管，这里只 hold「半截前缀」；
            # 否则已含完整围栏开标记的文本会被误 hold，切不出块。
            if size == len(marker):
                continue
            if text.endswith(marker[:size]):
                hold_start = min(hold_start, len(text) - size)
    return hold_start


def check_tool_call_end(content: str) -> bool:
    """检测内容是否包含完整的工具调用结束标记（任一协议词边界闭合）。"""
    for open_marker in _TOOL_OPEN_MARKERS:
        open_idx = content.find(open_marker)
        if open_idx < 0:
            continue
        close_marker = _TOOL_CLOSE_BY_OPEN[open_marker]
        if content.find(close_marker, open_idx + len(open_marker)) >= 0:
            return True
    return False


def build_tool_result_message(tool_call_id: str, name: str, result: str) -> dict:
    """构建工具结果消息(OpenAI 格式)。"""
    return {
        "role": "tool",
        "tool_call_id": tool_call_id,
        "name": name,
        "content": result
    }


def build_tool_call_message(tool_calls: list[dict], content: str = "") -> dict:
    """构建工具调用消息(OpenAI 格式)。"""
    return {
        "role": "assistant",
        "content": content,
        "tool_calls": tool_calls
    }


def _normalize_arguments(arguments_raw) -> dict:
    """把 tool_call.function.arguments 归一成 dict（JSON 字符串 / dict / 其它）。

    render_tool_call_* 三处共用，统一参数归一口径，避免三份近似拷贝。
    """
    if isinstance(arguments_raw, str):
        try:
            return json.loads(arguments_raw) if arguments_raw.strip() else {}
        except (json.JSONDecodeError, ValueError):
            return {"_raw": arguments_raw}
    if isinstance(arguments_raw, dict):
        return arguments_raw
    return {}


def render_tool_call_xml(tool_call: dict) -> str:
    """把单个 OpenAI tool_call 反向序列化为 ```tool_function 围栏。

    与统一提示词 / parse_tool_calls_from_content 同形态：提示词教模型吐
    ```tool_function 围栏、解析器只认该围栏、历史轮也用同形态回填，保证多轮工具
    对话上下文自洽（tools-as-prompt 渠道把历史 assistant tool_calls 拍回 content）。
    """
    return _render_tool_call_fence(tool_call)


def render_tool_result_xml(tool_call_id: str, name: str, result: str) -> str:
    """把单个工具结果渲染成文本块，供回传给 tools-as-prompt 渠道。

    模型上一轮调用工具后，结果以 ``<tool_result>`` 文本回填，让对话历史对不支持
    OpenAI function calling 的上游自洽。结果不进围栏，无需统一形态。
    """
    name_attr = f' name="{name}"' if name else ""
    id_attr = f' id="{tool_call_id}"' if tool_call_id else ""
    return f"<tool_result{name_attr}{id_attr}>{result}</tool_result>"


def render_tool_call_json(tool_call: dict) -> str:
    """把单个 OpenAI tool_call 反向序列化为 ```tool_function 围栏。

    与 _generate_tools_prompt / parse_tool_calls_from_content 同形态：提示词教
    模型吐 ```tool_function 围栏、解析器吃该围栏、历史轮也用同形态回填，保证多轮
    工具对话上下文自洽（eaichat 上游不认 OpenAI function calling，必须把历史
    tool_calls 拍回 content）。与 render_tool_call_xml / render_tool_call_hermes
    产同一形态——三者已统一。
    """
    return _render_tool_call_fence(tool_call)


def render_tool_result_json(tool_call_id: str, name: str, result: str) -> str:
    """把单个工具结果渲染成文本块 (json 形态)。

    与 render_tool_result_xml 同语义, 只是围栏/标签风格与 json 形态对齐。
    """
    name_attr = f' name="{name}"' if name else ''
    id_attr = f' id="{tool_call_id}"' if tool_call_id else ''
    return f'<tool_result{name_attr}{id_attr}>{result}</tool_result>'


def render_tool_call_hermes(tool_call: dict) -> str:
    """把 OpenAI tool_call 反向序列化为 ```tool_function 围栏。

    与 _generate_tools_prompt / parse_tool_calls_from_content 同形态，
    供 tools-as-prompt 渠道把历史 assistant tool_calls 拍回 content。与
    render_tool_call_xml / render_tool_call_json 产同一形态——三者已统一。
    """
    return _render_tool_call_fence(tool_call)


def render_tool_result_hermes(tool_call_id: str, name: str, result: str) -> str:
    """把工具结果渲染成 Hermes / tooluser 风格文本块。"""
    name_line = f"<name>{name}</name>\n" if name else ""
    return (
        "<tool_result>\n"
        f"<id>{tool_call_id}</id>\n"
        f"{name_line}"
        "<result>\n"
        f"{result}\n"
        "</result>\n"
        "</tool_result>"
    )


def _render_tool_call_fence(tool_call: dict) -> str:
    """把单个 OpenAI tool_call 反向序列化为 ```tool_function 围栏（三 render 共用）。"""
    func = (tool_call or {}).get("function") or {}
    arguments = _normalize_arguments(func.get("arguments"))
    payload = json.dumps(
        {"name": func.get("name") or "", "arguments": arguments},
        ensure_ascii=False,
    )
    return f"{_TOOL_FENCE_OPEN}\n{payload}\n{_TOOL_FENCE_CLOSE}"
