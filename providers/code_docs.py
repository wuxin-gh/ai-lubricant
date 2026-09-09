"""代码渠道「使用说明」与最小样例的数据源。

前端弹框不再把说明硬编码成 JSX——那份说明改一次要改两处、还没法给可粘贴的代码。
说明正文统一从仓库里的真实文件读：``docs/providers/code-channel.md``（文档与前端同源，
改一处即处处生效）。

样例只保留「最小可跑」这一条，且不读任何文件：它就是建渠道时回填编辑器的 EchoChannel
目录预设。具体上游的完整实现（认证、签名、非标协议那些）不随产品分发——产品只发框架
能力，spec 源料由使用者自行持有，见 ``.gitignore`` 里 ``specs/`` 一节。

admin 端点 ``GET /admin/providers/code-channel/docs`` 直接返回本模块的结果。
文件缺失（例如裁剪过的镜像没带 docs/）不报错，只是该项内容为空 + 记一条 warning。
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from loguru import logger

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DOC_FILE = _REPO_ROOT / "docs" / "providers" / "code-channel.md"

# 样例清单：id / 标题 / 一句话适用场景 / 用到的钩子 / 源文件。
# minimal 那条不读文件——它就是目录预设回填进编辑器的 EchoChannel，真相源在
# channel_catalog._code_entry()，这里引用同一个函数，避免第三处副本。
_SAMPLES: tuple[dict[str, Any], ...] = (
    {
        "id": "minimal",
        "title": "最小可跑：EchoChannel",
        "summary": "原样回吐用户最后一条消息。只写 init_auth / fetch_models / stream_chat，用来验证「贴代码→出渠道」这条链路是通的。",
        "hooks": ["init_auth", "fetch_models", "stream_chat"],
        "file": "",
    },
)


def _read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.warning("[code-docs] 文件不存在，返回空内容: {}", path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[code-docs] 读取失败 {}: {}", path, exc)
    return ""


def _minimal_sample_code() -> str:
    """最小样例的源码来自渠道目录预设（与创建渠道时回填编辑器的那份同源）。"""
    try:
        from user_platform.marketplace.channel_catalog import _code_entry

        return str((_code_entry().get("preset") or {}).get("code") or "")
    except Exception as exc:  # noqa: BLE001
        logger.warning("[code-docs] 取目录预设样例失败: {}", exc)
        return ""


def get_code_channel_docs() -> dict[str, Any]:
    """返回 {"doc": <markdown 正文>, "samples": [{id,title,summary,hooks,code}]}。"""
    samples = [
        {
            "id": meta["id"],
            "title": meta["title"],
            "summary": meta["summary"],
            "hooks": list(meta["hooks"]),
            "code": _minimal_sample_code(),
        }
        for meta in _SAMPLES
    ]
    return {"doc": _read(_DOC_FILE), "samples": samples}
