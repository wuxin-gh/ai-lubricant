"""扩展 upstream_category 字段长度到 200。"""
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

async def main():
    pool = await asyncpg.create_pool(
        host=os.getenv('POSTGRES_HOST'),
        port=int(os.getenv('POSTGRES_PORT')),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        database=os.getenv('POSTGRES_DATABASE'),
    )
    async with pool.acquire() as conn:
        await conn.execute('ALTER TABLE marketplace_leaderboard_items ALTER COLUMN upstream_category TYPE VARCHAR(200)')
        print('✓ upstream_category 已扩展到 VARCHAR(200)')
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
