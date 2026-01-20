from db import fetchrow, execute


async def get_user(user_id: int):
    return await fetchrow(
        "SELECT * FROM users WHERE user_id = $1",
        user_id
    )


async def upsert_user(user_id: int, username: str | None):
    await execute(
        """
        INSERT INTO users (user_id, username)
        VALUES ($1, $2)
        ON CONFLICT (user_id)
        DO UPDATE SET username = EXCLUDED.username
        """,
        user_id,
        username
    )