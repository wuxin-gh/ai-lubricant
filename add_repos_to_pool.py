"""批量添加 GitHub 仓库到候选池。"""
import asyncio
import os
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
import aiohttp

load_dotenv()

# 要添加的仓库列表
REPOS = [
    "modelscope/agentscope",
    "thinker-ai/agent-matrix",  # 这个可能不存在，脚本会跳过
]


async def fetch_github_repo(session: aiohttp.ClientSession, full_name: str) -> dict:
    """直接调用 GitHub API 获取仓库元数据。"""
    url = f"https://api.github.com/repos/{full_name}"
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "ai-lubricant"}

    token = os.getenv("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"token {token}"

    async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
        if resp.status >= 400:
            raise RuntimeError(f"HTTP {resp.status}")
        return await resp.json()


def build_item(full_name: str, meta: dict) -> dict:
    """从 GitHub 元数据组装候选池条目（简化版）。"""
    owner, _, short = full_name.partition("/")

    topics = meta.get("topics") or []
    description = meta.get("description") or ""
    updated_at_str = meta.get("updated_at")

    # 转换日期
    upstream_updated_at = None
    if updated_at_str:
        try:
            upstream_updated_at = datetime.fromisoformat(updated_at_str.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            pass

    # 简单分类：从 topics 里找关键词
    modules = []
    category_text = " ".join(topics).lower()
    if "mcp" in category_text or "model-context-protocol" in category_text:
        modules.append("mcp")
    if "skill" in category_text or "claude" in category_text:
        modules.append("skill")
    if "plugin" in category_text:
        modules.append("plugin")
    if "prompt" in category_text:
        modules.append("prompt")

    # 如果没识别出任何模块，标记为不可安装
    installable = len(modules) > 0

    # 版本号从更新时间生成
    version = upstream_updated_at.strftime("%Y.%m.%d") if upstream_updated_at else "latest"

    # external_data
    external_data = {
        "source": "manual",
        "board": "manual",
        "repo_full_name": full_name,
        "repo_url": meta.get("html_url") or f"https://github.com/{full_name}",
        "description": description,
        "upstream_category": " ".join(topics)[:200],  # 截断到 200 字符
        "language": meta.get("language") or "",
        "topics": topics,
        "use_cases": [],
        "stars": meta.get("stargazers_count") or 0,
        "forks": meta.get("forks_count") or 0,
        "upstream_rank": None,
        "upstream_updated_at": updated_at_str,
        "version": version,
        "raw": meta,
    }

    # install_spec 默认值
    install_spec = {}
    if "skill" in modules:
        install_spec["skill"] = {"install_method": "github_clone", "path": "", "ref": "main"}
    if "plugin" in modules:
        install_spec["plugin"] = {
            "download_url": f"https://github.com/{full_name}/archive/refs/heads/main.zip",
            "provider": "claude",
        }
    if "prompt" in modules:
        install_spec["prompt"] = {"content": ""}

    return {
        "source": "manual",
        "board": "manual",
        "repo_full_name": full_name,
        "repo_url": external_data["repo_url"],
        "description": description,
        "stars": external_data["stars"],
        "forks": external_data["forks"],
        "language": external_data["language"],
        "topics": topics,
        "upstream_category": external_data["upstream_category"][:200],
        "use_cases": [],
        "upstream_rank": None,
        "upstream_updated_at": upstream_updated_at,
        "sort_order": 9999,
        "external_data": external_data,
        "target_modules": modules,
        "installable": installable,
        "name": short,
        "display_name": short,
        "publisher": owner,
        "version": version,
        "categories": [],
        "tags": topics,
        "install_spec": install_spec,
        "status": "draft",
    }


async def main():
    # 直接用 asyncpg 连接
    import asyncpg

    # 数据库连接信息
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = int(os.getenv("POSTGRES_PORT", 5432))
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("POSTGRES_PASSWORD", "")
    db_name = os.getenv("POSTGRES_DATABASE") or os.getenv("POSTGRES_DB", "postgres")

    # 创建连接池
    pool = await asyncpg.create_pool(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name,
        min_size=1,
        max_size=3,
    )

    print(f"\n准备添加 {len(REPOS)} 个仓库到候选池...\n")

    async with aiohttp.ClientSession() as session:
        for repo_full_name in REPOS:
            print(f"处理 {repo_full_name} ...")

            # 检查是否已存在
            async with pool.acquire() as conn:
                existing = await conn.fetchrow(
                    "SELECT id FROM marketplace_leaderboard_items WHERE lower(repo_full_name)=lower($1) LIMIT 1",
                    repo_full_name,
                )
            if existing:
                print(f"  已存在（id={existing['id']}，跳过）")
                continue

            # 拉 GitHub 元数据
            try:
                meta = await fetch_github_repo(session, repo_full_name)
            except Exception as exc:
                print(f"  获取元数据失败: {exc}")
                continue

            # 组装条目
            item = build_item(repo_full_name, meta)

            # 写入
            try:
                async with pool.acquire() as conn:
                    item_id = await conn.fetchval(
                        """
                        INSERT INTO marketplace_leaderboard_items (
                            source, board, repo_full_name, repo_url, description, stars, forks,
                            language, topics, upstream_category, use_cases, upstream_rank,
                            upstream_updated_at, sort_order, external_data,
                            target_modules, installable, name, display_name, publisher, version,
                            categories, tags, install_spec, status, last_synced_at
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                            $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, now()
                        )
                        RETURNING id
                        """,
                        item["source"],
                        item["board"],
                        item["repo_full_name"],
                        item["repo_url"],
                        item["description"],
                        item["stars"],
                        item["forks"],
                        item["language"],
                        json.dumps(item["topics"]),
                        item["upstream_category"],
                        json.dumps(item["use_cases"]),
                        item["upstream_rank"],
                        item["upstream_updated_at"],
                        item["sort_order"],
                        json.dumps(item["external_data"]),
                        json.dumps(item["target_modules"]),
                        item["installable"],
                        item["name"],
                        item["display_name"],
                        item["publisher"],
                        item["version"],
                        json.dumps(item["categories"]),
                        json.dumps(item["tags"]),
                        json.dumps(item["install_spec"]),
                        item["status"],
                    )
                print(f"  已添加（id={item_id}）")
            except Exception as exc:
                print(f"  写入失败: {exc}")

    await pool.close()
    print("\n完成！")


if __name__ == "__main__":
    asyncio.run(main())
