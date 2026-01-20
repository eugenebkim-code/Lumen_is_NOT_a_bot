import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

_pool: asyncpg.Pool | None = None


async def init_db():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            min_size=1,
            max_size=5,
        )


async def close_db():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def fetchrow(query: str, *args):
    async with _pool.acquire() as conn:
        return await conn.fetchrow(query, *args)


async def fetch(query: str, *args):
    async with _pool.acquire() as conn:
        return await conn.fetch(query, *args)


async def execute(query: str, *args):
    async with _pool.acquire() as conn:
        return await conn.execute(query, *args)