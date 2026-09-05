"""执行 PostgreSQL 初始化 SQL。"""
import asyncio
from pathlib import Path
import sys as _sys

# 自身在 server/ 目录，注入 server/ 路径让 `from db import` 可用。
_sys.path.insert(0, str(Path(__file__).resolve().parent))

import asyncpg
from loguru import logger

from db import get_db_config


async def main():
    from project_paths import sql_dir

    sql_file = sql_dir() / "init.sql"
    sql = sql_file.read_text(encoding="utf-8")
    # Connect to default 'postgres' DB to create 'ai-lubricant' database first
    config = get_db_config()
    config['database'] = "postgres"
    conn = await asyncpg.connect(**config)
    try:
        await conn.execute("CREATE DATABASE \"ai-lubricant\" ENCODING 'UTF8'")
    except asyncpg.exceptions.DuplicateDatabaseError:
        logger.info("Database 'ai-lubricant' already exists")
    finally:
        await conn.close()

    # Now connect to the newly created 'ai-lubricant' database
    conn = await asyncpg.connect(**get_db_config())
    try:
        await conn.execute(sql)
        logger.info(f"数据库初始化完成: {sql_file}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
