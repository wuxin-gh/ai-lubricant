import json
import re
from pathlib import Path
from typing import Callable

from loguru import logger


# ── Tokenizer 估算规则 ────────────────────────────────────────────
# 规则自上而下用 re.search 匹配模型 ID，首个命中生效；自定义规则优先于内置规则。
#
# type:
#   chars        字符估算。chars_per_token 管非 CJK 字符，cjk_chars_per_token 管 CJK。
#   tiktoken     OpenAI BPE，encoding 指定编码器。失败降级为 chars。
#   huggingface  真实词表，repo 指定 HF 仓库。词表由管理端预热落盘，运行时只读本地文件；
#                缺词表时降级到 encoding（若配了）再到 chars，绝不在请求链路联网。
#
# calibration: 计数结果乘的系数。闭源模型（claude/gemini 等）拿不到真词表，只能用近似
#              分词器加系数贴合上游真实口径。实测这是最有效的一档修正。
# structural:  是否为 messages 列表叠加每条固定结构开销（OpenAI 口径每条 +3、整体 +3）。
#              默认关闭：实测叠加校准系数后它对准确度几乎无增益。
_BUILTIN_TOKENIZER_RULES = [
    # 顺序敏感：legacy 必须在通用 OpenAI 规则之前，否则 gpt-4-turbo 会被 o200k 抢走。
    {
        "name": "OpenAI legacy",
        "enabled": True,
        "pattern": r"(?i)gpt-(?:3\.5|4(?![o.]))",
        "type": "tiktoken",
        "encoding": "cl100k_base",
    },
    {
        "name": "OpenAI",
        "enabled": True,
        "pattern": r"(?i)(?:gpt-|chatgpt|\bo[134]-)",
        "type": "tiktoken",
        "encoding": "o200k_base",
        # 0.87 由 script/test_data 下 8 条已知真实 prompt_tokens 的 gpt-5.5 请求体标定：
        # 未校准时中位比值 1.152（o200k 平铺对结构化请求偏高），乘 0.87 后中位 ≈ 1.00。
        "calibration": 0.87,
    },
    # 闭源：无公开词表，用 o200k 近似 + 系数贴合上游真实口径。
    {
        "name": "Claude",
        "enabled": True,
        "pattern": r"(?i)(?:claude|anthropic|opus|sonnet|haiku)",
        "type": "tiktoken",
        "encoding": "o200k_base",
        # Claude 口径随上游/中转/缓存差异波动较大，不写死系数；后台试算后按真实日志手动填 calibration。
        "calibration": 1.0,
    },
    {
        "name": "Gemini",
        "enabled": True,
        "pattern": r"(?i)gemini",
        "type": "tiktoken",
        "encoding": "o200k_base",
        "calibration": 1.0,
    },
    # 开源：有公开词表，预热后走真实分词器；未预热时降级到 o200k 近似。
    {
        "name": "GLM",
        "enabled": True,
        "pattern": r"(?i)(?:glm|chatglm|z-ai)",
        "type": "huggingface",
        "repo": "zai-org/GLM-5.2",
        "encoding": "o200k_base",
    },
    {
        "name": "Qwen",
        "enabled": True,
        "pattern": r"(?i)(?:qwen|tongyi)",
        "type": "huggingface",
        "repo": "Qwen/Qwen3.8-27B",
        "encoding": "o200k_base",
    },
    {
        "name": "DeepSeek",
        "enabled": True,
        "pattern": r"(?i)deepseek",
        "type": "huggingface",
        "repo": "deepseek-ai/DeepSeek-V4-Pro",
        "encoding": "o200k_base",
    },
    {
        "name": "MiniMax",
        "enabled": True,
        "pattern": r"(?i)minimax",
        "type": "huggingface",
        "repo": "MiniMaxAI/MiniMax-M3",
        "encoding": "o200k_base",
    },
    # 其余国产/兼容模型：暂无核实过的公开词表，统一用 o200k 近似，好过裸字符估算。
    {
        "name": "OpenAI-compatible",
        "enabled": True,
        "pattern": r"(?i)(?:kimi|moonshot|hunyuan|hy\d|longcat|doubao|yi-|ernie|baichuan|spark|step-)",
        "type": "tiktoken",
        "encoding": "o200k_base",
    },
]
# 兜底：未命中任何规则的未知模型。按实测真实请求体的字符/token 比取值——
# 旧的 chars_per_token=2 对拉丁文本高估约 2 倍，是「预测入」虚高的主因之一。
_DEFAULT_TOKENIZER_RULE = {"type": "chars", "chars_per_token": 3.5, "cjk_chars_per_token": 1.5}
_tokenizer_rules_getter: Callable[[], list[dict]] | None = None

# CJK 及全角标点：BPE 下每字符约占 1 个 token，远高于拉丁文本，必须分开计权。
_CJK_RE = re.compile(
    r"[　-〿぀-ヿ㐀-䶿一-鿿"
    r"豈-﫿＀-￯\U00020000-\U0002ebef]"
)

# 预热后的 HF 词表落盘位置。运行时只读，不联网。
# 仅用于迁移期：本地已有词表先导入 PG，之后本地文件不再被读取。
from project_paths import data_dir as _data_dir

HF_TOKENIZER_DIR = _data_dir() / "tokenizers"
_HF_TOKENIZER_CACHE: dict[str, object] = {}
_HF_TOKENIZER_CACHE_MAX = 8
_hf_warned: set[str] = set()

# PG 词表在进程内的快照。启动时由 load_hf_tokenizers_from_pg() 预载，热路径只读这两个 dict。
# _HF_VOCAB_TEXT: repo → tokenizer.json 原文；_HF_VOCAB_META: repo → {etag, bytes, downloaded_at, updated_at}
_HF_VOCAB_TEXT: dict[str, str] = {}
_HF_VOCAB_META: dict[str, dict] = {}


def set_tokenizer_rules_getter(getter: Callable[[], list[dict]] | None) -> None:
    global _tokenizer_rules_getter
    _tokenizer_rules_getter = getter


def tokenizer_rules() -> list[dict]:
    custom_rules = []
    if _tokenizer_rules_getter is not None:
        try:
            rules = _tokenizer_rules_getter()
        except Exception as e:
            logger.warning(f"tokenizer 规则读取失败: error={e}")
            rules = []
        custom_rules = rules if isinstance(rules, list) else []
    return [*custom_rules, *_BUILTIN_TOKENIZER_RULES]


def resolve_tokenizer_rule(model: str, rules: list[dict] | None = None) -> dict:
    """按模型名解析生效规则。

    rules 显式传入时用它替代「配置里的自定义规则」（内置规则仍作兜底），语义与生产一致。
    管理端试算靠这个参数跑候选规则，避免改全局 getter——那样会污染正在服务的真实请求。
    """
    model = model or ""
    for rule in (tokenizer_rules() if rules is None else [*rules, *_BUILTIN_TOKENIZER_RULES]):
        if not isinstance(rule, dict) or rule.get("enabled") is False:
            continue
        pattern = str(rule.get("pattern") or "")
        if not pattern:
            continue
        try:
            if re.search(pattern, model):
                return rule
        except re.error as e:
            logger.warning(f"tokenizer 规则正则无效: name={rule.get('name')}, pattern={pattern}, error={e}")
            continue
    return dict(_DEFAULT_TOKENIZER_RULE)


def _rule_float(rule: dict, key: str, default: float) -> float:
    try:
        value = float(rule.get(key) or default)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def estimate_chars_tokens(text: str, rule: dict) -> int:
    """字符估算。CJK 与非 CJK 分开计权——两者的字符/token 比差一个数量级。

    未配 cjk_chars_per_token 时退化为单一比值，保持与历史行为一致。
    """
    chars_per_token = _rule_float(rule, "chars_per_token", 2)
    cjk_raw = rule.get("cjk_chars_per_token")
    if cjk_raw is None:
        return max(1, int((len(text) + chars_per_token - 1) // chars_per_token))
    cjk_per_token = _rule_float(rule, "cjk_chars_per_token", chars_per_token)
    cjk_chars = sum(1 for char in text if _CJK_RE.match(char))
    other_chars = len(text) - cjk_chars
    tokens = cjk_chars / cjk_per_token + other_chars / chars_per_token
    return max(1, int(tokens + 0.999))


def estimate_tiktoken_tokens(text: str, rule: dict) -> int | None:
    try:
        import tiktoken
    except ImportError:
        return None
    encoding_name = str(rule.get("encoding") or "cl100k_base")
    try:
        encoding = tiktoken.get_encoding(encoding_name)
    except Exception as e:
        logger.warning(f"tiktoken encoding 不可用: encoding={encoding_name}, error={e}")
        return None
    return len(encoding.encode(text))


def hf_tokenizer_path(repo: str) -> Path:
    """预热词表的落盘路径。repo 里的 / 会造成子目录，保持与 HF 仓库结构一致。"""
    return HF_TOKENIZER_DIR / repo / "tokenizer.json"


def _parse_hf_tokenizer(repo: str, text: str):
    """从 tokenizer.json 文本解析 Tokenizer。失败返回 None，不抛——热路径不能崩。"""
    try:
        from tokenizers import Tokenizer
    except ImportError:
        if "__pkg__" not in _hf_warned:
            _hf_warned.add("__pkg__")
            logger.warning("tokenizers 包未安装，huggingface 规则降级估算")
        return None
    try:
        return Tokenizer.from_str(text)
    except Exception as e:
        logger.warning(f"HF 词表加载失败: repo={repo}, error={e}")
        return None


def _load_hf_tokenizer(repo: str):
    """加载预热过的词表。同步、只读进程内内存，永不联网、永不查 PG。

    词表 JSON 可达 20MB，解析昂贵，因此进程内缓存；但有界，避免规则配多吃满内存。
    查找顺序：缓存 → PG 预载的内存文本 → 本地文件（迁移期回退）。
    """
    if repo in _HF_TOKENIZER_CACHE:
        return _HF_TOKENIZER_CACHE[repo]
    text = _HF_VOCAB_TEXT.get(repo)
    if text is None:
        # PG 中无此 repo：迁移期可能本地文件还在，兜底一次。
        path = hf_tokenizer_path(repo)
        if not path.is_file():
            return None
        try:
            text = path.read_text(encoding="utf-8")
        except Exception as e:
            logger.warning(f"HF 词表本地文件读取失败: repo={repo}, error={e}")
            return None
    tokenizer = _parse_hf_tokenizer(repo, text)
    if tokenizer is None:
        return None
    if len(_HF_TOKENIZER_CACHE) >= _HF_TOKENIZER_CACHE_MAX:
        _HF_TOKENIZER_CACHE.pop(next(iter(_HF_TOKENIZER_CACHE)), None)
    _HF_TOKENIZER_CACHE[repo] = tokenizer
    return tokenizer


def load_hf_tokenizer(repo: str):
    """公开入口：加载词表，成功返回 tokenizer，否则 None。供管理端预热后自检使用。"""
    return _load_hf_tokenizer(repo)


def invalidate_hf_tokenizer_cache(repo: str | None = None) -> None:
    """清掉进程内词表缓存。管理端重新预热后必须调用，否则仍用旧词表。

    同时清掉告警记录，让降级告警在重新预热失败后还能再报一次。
    """
    if repo:
        _HF_TOKENIZER_CACHE.pop(repo, None)
        _hf_warned.discard(repo)
    else:
        _HF_TOKENIZER_CACHE.clear()
        _hf_warned.clear()


async def load_hf_tokenizers_from_pg() -> int:
    """启动预载：把 PG 里所有词表文本拉进进程内存。

    PG 未就绪时直接返回 0（测试环境无 DB 也能跑）。解析失败只告警，不抛——词表只是
    估算降级，绝不能拖死启动。解析这里做一次，热路径就只读缓存了。
    调用多次是幂等的（每次重新从 PG 拉取）。
    """
    from db import PostgresClient
    if not PostgresClient.pool:
        return len(_HF_VOCAB_TEXT) or 0
    try:
        rows = await PostgresClient.get_tokenizer_vocabs()
    except Exception as e:
        logger.warning(f"[tokenizer-vocab] 启动预载读 PG 失败，估算将降级: {type(e).__name__}: {e}")
        return len(_HF_VOCAB_TEXT) or 0
    _HF_VOCAB_TEXT.clear()
    _HF_VOCAB_META.clear()
    for row in rows:
        repo = row.get("repo")
        content = row.get("content")
        if not repo or not content:
            continue
        _HF_VOCAB_TEXT[repo] = content
        _HF_VOCAB_META[repo] = {
            "etag": row.get("etag"),
            "bytes": row.get("bytes") or 0,
            "mirror": row.get("mirror"),
            "downloaded_at": row.get("downloaded_at"),
            "updated_at": row.get("updated_at"),
        }
    if _HF_VOCAB_TEXT:
        logger.info(f"[tokenizer-vocab] 已从 PG 预载 {len(_HF_VOCAB_TEXT)} 个词表")
    return len(_HF_VOCAB_TEXT)


async def load_hf_tokenizer_from_pg(repo: str) -> bool:
    """单条重载：预热后自检 / 收到广播后刷新本实例内存。

    只刷新文本与 meta，缓存里的旧 Tokenizer 对象由调用方先 invalidate 清掉。
    """
    from db import PostgresClient
    if not PostgresClient.pool:
        return False
    try:
        row = await PostgresClient.get_tokenizer_vocab(repo)
    except Exception as e:
        logger.warning(f"[tokenizer-vocab] 单条重载失败: repo={repo}, error={e}")
        return False
    if row is None:
        # PG 里也没了（被删？），清掉内存残留，避免用旧文本。
        _HF_VOCAB_TEXT.pop(repo, None)
        _HF_VOCAB_META.pop(repo, None)
        return False
    _HF_VOCAB_TEXT[repo] = row["content"]
    _HF_VOCAB_META[repo] = {
        "etag": row.get("etag"),
        "bytes": row.get("bytes") or 0,
        "mirror": row.get("mirror"),
        "downloaded_at": row.get("downloaded_at"),
        "updated_at": row.get("updated_at"),
    }
    return True


async def migrate_local_vocabs_to_pg() -> int:
    """一次性迁移：把本地磁盘已有词表导入 PG（ON CONFLICT DO NOTHING）。

    在 load_hf_tokenizers_from_pg() 之前调用。PG 已有同名 repo 时不覆盖，
    多实例同时迁移也安全。迁移后本地文件保留作回退，不主动删。
    """
    from db import PostgresClient
    if not PostgresClient.pool:
        return 0
    import json as _json
    # 内置 HF repo 集合：只迁这些，避免乱写无关文件进 PG。
    repos = []
    for rule in _BUILTIN_TOKENIZER_RULES:
        if rule.get("enabled") is False or str(rule.get("type") or "") != "huggingface":
            continue
        repo = str(rule.get("repo") or "").strip()
        if repo and repo not in repos:
            repos.append(repo)
    migrated = 0
    for repo in repos:
        path = hf_tokenizer_path(repo)
        if not path.is_file():
            continue
        try:
            content = path.read_text(encoding="utf-8")
            _json.loads(content)  # 校验，写坏的文件不进 PG
        except Exception as e:
            logger.warning(f"[tokenizer-vocab] 迁移跳过坏文件: repo={repo}, error={e}")
            continue
        meta_path = path.parent / "meta.json"
        etag = size = mirror = None
        if meta_path.is_file():
            try:
                meta = _json.loads(meta_path.read_text(encoding="utf-8"))
                etag = meta.get("etag")
                size = meta.get("bytes")
                mirror = meta.get("mirror")
            except Exception:
                pass
        try:
            await PostgresClient.insert_tokenizer_vocab_if_absent(
                repo, content, etag=etag, size=int(size or len(content)), mirror=mirror
            )
            migrated += 1
        except Exception as e:
            logger.warning(f"[tokenizer-vocab] 迁移写入失败: repo={repo}, error={e}")
    if migrated:
        logger.info(f"[tokenizer-vocab] 从本地磁盘迁移 {migrated} 个词表到 PG")
    return migrated


def estimate_hf_tokens(text: str, rule: dict) -> int | None:
    repo = str(rule.get("repo") or "").strip()
    if not repo:
        return None
    tokenizer = _load_hf_tokenizer(repo)
    if tokenizer is None:
        # 每个 repo 只告警一次，否则每请求一条日志会淹掉日志系统。
        if repo not in _hf_warned:
            _hf_warned.add(repo)
            logger.warning(f"HF 词表未预热，降级估算: repo={repo}")
        return None
    try:
        return len(tokenizer.encode(text, add_special_tokens=False).ids)
    except Exception as e:
        logger.warning(f"HF 分词失败: repo={repo}, error={e}")
        return None


def request_part_to_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _apply_calibration(tokens: int, rule: dict) -> int:
    """按规则系数校准。闭源模型只能用近似分词器 + 系数贴合上游真实口径。"""
    factor = _rule_float(rule, "calibration", 1.0)
    if factor == 1.0:
        return max(1, tokens)
    return max(1, int(tokens * factor + 0.5))


def estimate_text_tokens_by_rule(text: str, rule: dict) -> int:
    """按规则统计一段文本的 token。

    降级链：huggingface → tiktoken（若配了 encoding）→ chars。任一环节不可用都不抛异常，
    因为本函数在 token 预占与上下文校验的热路径上，估算失败绝不能影响请求本身。
    """
    if not text:
        return 0
    rule_type = str(rule.get("type") or "chars").strip().lower()
    if rule_type == "huggingface":
        tokens = estimate_hf_tokens(text, rule)
        if tokens is not None:
            return _apply_calibration(tokens, rule)
        if rule.get("encoding"):
            tokens = estimate_tiktoken_tokens(text, rule)
            if tokens is not None:
                return _apply_calibration(tokens, rule)
    elif rule_type == "tiktoken":
        tokens = estimate_tiktoken_tokens(text, rule)
        if tokens is not None:
            return _apply_calibration(tokens, rule)
    return _apply_calibration(estimate_chars_tokens(text, rule), rule)


def estimate_request_part_tokens(model: str, value, rules: list[dict] | None = None) -> int:
    text = request_part_to_text(value)
    if not text:
        return 0
    rule = resolve_tokenizer_rule(model, rules)
    tokens = estimate_text_tokens_by_rule(text, rule)
    # 结构开销：OpenAI 口径下每条 message 有固定包装开销（role 标记等），
    # 长对话里这部分不可忽略。只对 messages 列表生效，默认关闭。
    if rule.get("structural") and isinstance(value, list):
        tokens += 3 * len(value) + 3
    return max(1, tokens)


def estimate_tokens_from_text(text: str, model: str = "") -> int:
    return estimate_request_part_tokens(model, text)


def request_input_parts(body: dict | None, endpoint: str = "chat") -> list[tuple[str, object]]:
    if not isinstance(body, dict):
        return []
    if endpoint == "responses":
        keys = ("instructions", "input", "tools", "tool_choice", "text", "response_format")
    elif endpoint == "anthropic":
        keys = ("system", "messages", "tools", "tool_choice", "thinking", "metadata", "stop_sequences")
    elif endpoint == "upstream":
        keys = (
            "system", "messages", "input", "instructions", "tools", "functions",
            "tool_choice", "function_call", "thinking", "text", "response_format", "prediction",
        )
    else:
        keys = (
            "system", "messages", "tools", "functions", "tool_choice",
            "function_call", "response_format", "prediction",
        )
    return [(key, body.get(key)) for key in keys]


def estimate_input_token_parts(
    model: str, body: dict | None, endpoint: str = "chat", rules: list[dict] | None = None
) -> dict[str, int]:
    return {
        name: estimate_request_part_tokens(model, value, rules)
        for name, value in request_input_parts(body, endpoint)
        if value is not None
    }


def estimate_input_tokens(
    model: str, body: dict | None, endpoint: str = "chat", rules: list[dict] | None = None
) -> int:
    return sum(estimate_input_token_parts(model, body, endpoint, rules).values())


def _requested_output_tokens(body: dict | None) -> int:
    """从请求体里取客户端显式声明的输出上限（max_tokens/max_completion_tokens）。

    纯函数：只读 body，不依赖模型元数据；返回 0 表示客户端未显式声明。
    """
    if not isinstance(body, dict):
        return 0
    for key in ("max_tokens", "max_completion_tokens"):
        value = body.get(key)
        if value is not None:
            try:
                return max(0, int(value))
            except (TypeError, ValueError):
                return 0
    return 0


def estimate_request_tokens(
    model: str, body: dict | None, endpoint: str = "chat", rules: list[dict] | None = None
) -> dict[str, int]:
    """请求 token 纯测算（公共计算层）。

    返回 ``{part_tokens, input_tokens, output_tokens, total_tokens}``，不读模型窗口上限、
    不抛异常。入口校验、上游校验、选路预估都复用本函数，保证口径一致。

    - input_tokens：请求各输入部分（messages/system/tools 等）token 之和。
    - output_tokens：客户端显式声明的输出上限（max_tokens/max_completion_tokens）；
      未声明则 0。
    - total_tokens：input + output，入口/上游校验用这个跟 max_context_tokens 比。
    """
    part_tokens = estimate_input_token_parts(model, body, endpoint, rules)
    input_tokens = sum(part_tokens.values())
    output_tokens = _requested_output_tokens(body)
    return {
        "part_tokens": part_tokens,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
    }


def content_to_text(content) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                parts.append(str(item.get("text") or item.get("content") or item.get("input") or ""))
        return "\n".join(parts)
    return json.dumps(content, ensure_ascii=False)


def estimate_usage(request_body: dict | None, response_body: dict | None, model: str = "") -> dict:
    prompt_text = ""
    if isinstance(request_body, dict):
        prompt_text += content_to_text(request_body.get("system"))
        for message in request_body.get("messages") or []:
            if isinstance(message, dict):
                prompt_text += "\n" + content_to_text(message.get("content"))
                if message.get("tool_calls"):
                    prompt_text += "\n" + json.dumps(message.get("tool_calls"), ensure_ascii=False)
        if request_body.get("tools"):
            prompt_text += "\n" + json.dumps(request_body.get("tools"), ensure_ascii=False)

    completion_text = ""
    if isinstance(response_body, dict):
        choices = response_body.get("choices") or []
        if choices:
            message = (choices[0] or {}).get("message") or {}
            completion_text += content_to_text(message.get("content"))
            completion_text += content_to_text(message.get("reasoning_content"))
            if message.get("tool_calls"):
                completion_text += "\n" + json.dumps(message.get("tool_calls"), ensure_ascii=False)
        for block in response_body.get("content") or []:
            if isinstance(block, dict):
                completion_text += content_to_text(block.get("text") or block.get("content") or block.get("input"))
        for event in response_body.get("events") or []:
            for data in _stream_payloads(event):
                delta = data.get("delta") or {}
                completion_text += content_to_text(delta.get("text") or delta.get("partial_json"))
                if data.get("content_block"):
                    completion_text += content_to_text(data.get("content_block"))
                if data.get("message"):
                    completion_text += content_to_text((data.get("message") or {}).get("content"))

    prompt = estimate_tokens_from_text(prompt_text, model)
    completion = estimate_tokens_from_text(completion_text, model)
    return {"prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": prompt + completion, "cached_tokens": 0, "cache_creation_tokens": 0, "reasoning_tokens": 0}


def _stream_payloads(chunk) -> list[dict]:
    if isinstance(chunk, dict):
        return [chunk]
    if not isinstance(chunk, str):
        return []
    try:
        from message_utils import iter_sse_payloads
    except Exception:
        return []
    return [payload for payload in iter_sse_payloads(chunk) if isinstance(payload, dict)]


def _usage_value_with_key(usage: dict, *keys: str) -> tuple[int, str | None]:
    """同 usage_value，但额外返回命中的 key（便于判断 token 来源口径）。"""
    for key in keys:
        value = usage
        for part in key.split("."):
            if not isinstance(value, dict):
                value = None
                break
            value = value.get(part)
        if value is not None:
            try:
                return int(value), key
            except (TypeError, ValueError):
                return 0, key
    return 0, None


def _usage_sum_value_with_key(usage: dict, *keys: str) -> tuple[int, str | None]:
    """同 usage_sum_value，但额外返回命中的 key。"""
    for key in keys:
        value = usage
        for part in key.split("."):
            if not isinstance(value, dict):
                value = None
                break
            value = value.get(part)
        if value is None:
            continue
        if isinstance(value, dict):
            total = 0
            stack = list(value.values())
            while stack:
                item = stack.pop()
                if isinstance(item, dict):
                    stack.extend(item.values())
                    continue
                try:
                    total += int(item)
                except (TypeError, ValueError):
                    continue
            if total:
                return total, key
            continue
        try:
            scalar = int(value)
        except (TypeError, ValueError):
            scalar = 0
        if scalar:
            return scalar, key
    return 0, None


def usage_value(usage: dict, *keys: str) -> int:
    value, _ = _usage_value_with_key(usage, *keys)
    return value


def usage_sum_value(usage: dict, *keys: str) -> int:
    """Return the first non-zero scalar value, or sum numeric leaves of a dict value.

    Some upstreams expose cache write as a scalar (cache_creation_tokens), while
    Anthropic-style payloads may expose cache_creation as a breakdown object, e.g.
    {ephemeral_5m_input_tokens, ephemeral_1h_input_tokens}.
    """
    for key in keys:
        value = usage
        for part in key.split("."):
            if not isinstance(value, dict):
                value = None
                break
            value = value.get(part)
        if value is None:
            continue
        if isinstance(value, dict):
            total = 0
            stack = list(value.values())
            while stack:
                item = stack.pop()
                if isinstance(item, dict):
                    stack.extend(item.values())
                    continue
                try:
                    total += int(item)
                except (TypeError, ValueError):
                    continue
            if total:
                return total
            continue
        try:
            scalar = int(value)
        except (TypeError, ValueError):
            scalar = 0
        if scalar:
            return scalar
    return 0


def _input_field_is_total_input(input_key: str | None, cached_key: str | None, cache_creation_key: str | None) -> bool:
    """判断命中的 input 字段是否已经是“总输入”。

    口径：total_tokens = 总输入 + 输出。

    - OpenAI chat: prompt_tokens 本身就是总输入，prompt_tokens_details.* 只是明细。
    - OpenAI Responses: input_tokens 本身是总输入，input_tokens_details.* 只是明细。
    - Anthropic: input_tokens 不含 cache_read/cache_creation；顶层
      cache_read_input_tokens/cache_creation_input_tokens 是并列输入块，需要加回。
    """
    if input_key in ("prompt_tokens", "promptTokens"):
        return True
    if input_key in ("input_tokens", "inputTokens"):
        # input_tokens_details.* 表示 input_tokens 已经是总输入（OpenAI Responses 风格）。
        return any(
            key is not None and key.startswith("input_tokens_details.")
            for key in (cached_key, cache_creation_key)
        )
    return False


def normalize_usage(usage: dict | None) -> dict:
    usage = usage or {}
    native_usage = usage.get("nativeUsage") if isinstance(usage.get("nativeUsage"), dict) else {}
    source = {**native_usage, **usage}
    # raw 分量 + 命中 key（用于判断上游是否已经提供“总输入”）
    input_val, input_key = _usage_value_with_key(
        source, "prompt_tokens", "input_tokens", "promptTokens", "inputTokens"
    )
    completion = usage_value(
        source, "completion_tokens", "output_tokens", "completionTokens", "outputTokens"
    )
    cached, cached_key = _usage_value_with_key(
        source,
        "cached_tokens",
        "cache_read_input_tokens",
        "prompt_tokens_details.cached_tokens",
        "input_tokens_details.cached_tokens",
    )
    cache_creation, cache_creation_key = _usage_sum_value_with_key(
        source,
        "cache_creation_tokens",
        "cache_creation_input_tokens",
        "cache_write_tokens",
        "prompt_cache_miss_tokens",
        "cache_creation",
        "prompt_tokens_details.cache_creation_input_tokens",
        "prompt_tokens_details.cache_creation_tokens",
        "prompt_tokens_details.cache_write_tokens",
        "prompt_tokens_details.prompt_cache_miss_tokens",
        "prompt_tokens_details.cache_creation",
        "input_tokens_details.cache_creation_tokens",
        "input_tokens_details.cache_creation_input_tokens",
        "input_tokens_details.cache_write_tokens",
        "input_tokens_details.prompt_cache_miss_tokens",
        "input_tokens_details.cache_creation",
    )
    reasoning = usage_value(
        source,
        "reasoning_tokens",
        "completion_tokens_details.reasoning_tokens",
        "output_tokens_details.reasoning_tokens",
    )

    # 缓存 = 缓存读 + 缓存写（两者都属于输入 token）。
    cache_total = cached + cache_creation

    if _input_field_is_total_input(input_key, cached_key, cache_creation_key):
        # 上游声称给了总输入（OpenAI 风格），缓存本应是这份输入里的明细子集。
        # 但部分渠道把缓存从 input 里扣掉了：当 输入 <= 缓存 时，说明缓存并未计入
        # 该 input，需要把缓存加回，令 输入 = 缓存 + 输入。缓存为 0 时不触发。
        if cache_total > 0 and input_val <= cache_total:
            prompt = input_val + cache_total
        else:
            prompt = input_val
    else:
        # 上游只有输入分量（典型 Anthropic）：常规输入 + 缓存读 + 缓存写 = 总输入。
        prompt = input_val + cache_total

    # total 恒等于 总输入 + 输出，不再采信上游自报 total（上游 total 可能漏缓存或口径不一致）。
    total = prompt + completion
    return {
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": total,
        "cached_tokens": cached,
        "cache_creation_tokens": cache_creation,
        "reasoning_tokens": reasoning,
    }


def merge_usage(current: dict | None, incoming: dict | None) -> dict:
    """合并两个 usage 字典，仅用 incoming 中的非零值更新 current。

    语义是「incoming 覆盖 current」，供**累计真实上游 usage** 用（每帧全量递增的
    Kimi/vLLM 等，后到的帧就是更新的真值）。不要用它合并估算值——估算的非零小值会
    盖掉上游的真实大值，见 `fill_usage_with_estimate`。
    """
    if not isinstance(incoming, dict):
        return dict(current or {})
    merged = dict(current or {})
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge_usage(merged[key], value)
        elif value is not None and value != 0:
            merged[key] = value
    return merged


def fill_usage_with_estimate(real_usage: dict | None, estimated_usage: dict | None) -> dict:
    """真实 usage 优先，仅用估算值补真实值缺失（为 0）的分量，返回归一化口径。

    用于上游 usage 残缺时兜底：典型是 eaichat 这类只给 ``{prompt:N, completion:0}``
    甚至完全不发 usage 帧的渠道。

    不能用 `merge_usage(real, estimated)` 替代，有两个原因：
    1. 方向相反——`merge_usage` 让 incoming 覆盖 current，估算出的 ``prompt_tokens=1``
       会盖掉上游真实的 ``prompt_tokens=12345``，total 随之崩成个位数；
    2. 键名跨协议不同——上游可能给 Anthropic 风格 ``input_tokens``，估算给 OpenAI 风格
       ``prompt_tokens``，裸字典合并后两个键并存，`normalize_usage` 优先取
       ``prompt_tokens``（估算值）仍然选错。

    故先把双方各自 `normalize_usage` 到统一口径再按分量取值，绕开键名歧义。
    total 由 `normalize_usage` 按 总输入 + 输出 重算，不单独拼。
    """
    real = normalize_usage(real_usage)
    estimated = normalize_usage(estimated_usage)
    filled = {
        key: (real[key] or estimated[key])
        for key in ("prompt_tokens", "completion_tokens", "cached_tokens",
                    "cache_creation_tokens", "reasoning_tokens")
    }
    # prompt_tokens 此时已是「总输入」（含缓存明细），再过一遍 normalize 时不能让缓存
    # 被二次加回，故显式声明成 OpenAI 风格明细：prompt_tokens_details 的存在会让
    # _input_field_is_total_input 认定 prompt_tokens 就是总输入。
    return normalize_usage({
        "prompt_tokens": filled["prompt_tokens"],
        "completion_tokens": filled["completion_tokens"],
        "prompt_tokens_details": {
            "cached_tokens": filled["cached_tokens"],
            "cache_creation_tokens": filled["cache_creation_tokens"],
        },
        "completion_tokens_details": {"reasoning_tokens": filled["reasoning_tokens"]},
    })
