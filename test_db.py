import asyncio
from db import init_db, fetchrow, close_db


async def main():
    await init_db()

    row = await fetchrow("SELECT current_database();")
    print("DB:", row["current_database"])

    await close_db()


asyncio.run(main())