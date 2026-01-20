import asyncio
from db import init_db, fetchrow, close_db

from db_presence import upsert_presence, get_presence

await upsert_presence(123, "STATE_DIALOG", "dlg_test", 999)
row = await get_presence(123)
print(dict(row))


async def main():
    await init_db()

    row = await fetchrow("SELECT current_database();")
    print("DB:", row["current_database"])

    await close_db()


asyncio.run(main())