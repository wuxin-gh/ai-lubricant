#!/usr/bin/env python
"""清理旧同步路径的垃圾数据：marketplace_store 里的 agency-agents*/agentscope 条目。

新代码统一写候选池表 marketplace_leaderboard_items，旧表里的 prompts/skills 模块
数据不再被读取。运行后重新同步即可。
"""
import asyncio
import os
import asyncpg
from pathlib import Path


def load_env():
    """从 .env 加载环境变量"""
    env_file = Path(__file__).parent / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and value:
            os.environ.setdefault(key, value)


async def cleanup():
    load_env()

    # 从环境变量读取数据库连接信息
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        # 尝试从 POSTGRES_* 环境变量组装
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "")
        database = os.getenv("POSTGRES_DATABASE", "postgres")
        db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    print(f"连接数据库: {host}:{port}/{database}")
    conn = await asyncpg.connect(db_url)
    try:
        # 先检查表是否存在
        tables = await conn.fetch("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
            AND tablename IN ('marketplace_store', 'marketplace_leaderboard_items')
            ORDER BY tablename
        """)
        print(f"找到的表: {[r['tablename'] for r in tables]}")

        if not any(r['tablename'] == 'marketplace_store' for r in tables):
            print("\nmarketplace_store 表不存在，无需清理（可能已经全部迁移到新表）")

            # 检查候选池当前有什么数据
            if any(r['tablename'] == 'marketplace_leaderboard_items' for r in tables):
                stats = await conn.fetch("""
                    SELECT source, board, COUNT(*) as cnt
                    FROM marketplace_leaderboard_items
                    GROUP BY source, board
                    ORDER BY source, board
                """)
                if stats:
                    print("\n当前候选池统计:")
                    total = 0
                    for r in stats:
                        cnt = r['cnt']
                        total += cnt
                        print(f"  {r['source']:20} {r['board']:20} {cnt:5} 条")
                    print(f"  合计 {total} 条")
                else:
                    print("\n候选池为空，可以开始同步")

            return

        # 清理前先看有多少条
        rows = await conn.fetch("""
            SELECT module, publisher, COUNT(*) as cnt
            FROM marketplace_store
            WHERE (module = 'prompts' AND publisher LIKE 'agency-agents%')
               OR (module = 'skills' AND publisher = 'agentscope')
            GROUP BY module, publisher
            ORDER BY module, publisher
        """)
        if not rows:
            print("没有需要清理的数据")
            return

        print("=== 待清理数据 ===")
        total = 0
        for r in rows:
            cnt = r["cnt"]
            total += cnt
            print(f"{r['module']:12} {r['publisher']:20} {cnt:5} 条")
        print(f"合计 {total} 条\n")

        # 删除
        result = await conn.execute("""
            DELETE FROM marketplace_store
            WHERE (module = 'prompts' AND publisher LIKE 'agency-agents%')
               OR (module = 'skills' AND publisher = 'agentscope')
        """)
        deleted = int(result.split()[-1])
        print(f"已删除 {deleted} 条旧数据")
        print("\n下一步：重启服务 + 重新同步 agency-agents-zh 和 agentscope")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(cleanup())

