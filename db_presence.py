from db import fetchrow, execute


async def get_presence(user_id: int):
    return await fetchrow(
        "SELECT * FROM presence WHERE user_id = $1",
        user_id
    )


async def upsert_presence(
    user_id: int,
    state: str,
    current_dialog_id: str | None,
    main_message_id: int | None,
):
    await execute(
        """
        INSERT INTO presence (user_id, state, current_dialog_id, main_message_id, updated_at)
        VALUES ($1, $2, $3, $4, now())
        ON CONFLICT (user_id)
        DO UPDATE SET
            state = EXCLUDED.state,
            current_dialog_id = EXCLUDED.current_dialog_id,
            main_message_id = EXCLUDED.main_message_id,
            updated_at = now()
        """,
        user_id,
        state,
        current_dialog_id,
        main_message_id,
    )