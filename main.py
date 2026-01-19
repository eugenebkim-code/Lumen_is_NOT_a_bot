import os
import logging
from datetime import datetime, timezone

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

from google.oauth2 import service_account
from googleapiclient.discovery import build

# =========================
# LOGGING
# =========================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
import json

GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")

if not BOT_TOKEN or not SPREADSHEET_ID or not GOOGLE_SERVICE_ACCOUNT_JSON:
    log.error(
        "ENV CHECK | BOT_TOKEN=%s | SPREADSHEET_ID=%s | GOOGLE_JSON=%s",
        bool(BOT_TOKEN),
        bool(SPREADSHEET_ID),
        bool(GOOGLE_SERVICE_ACCOUNT_JSON),
    )
    raise RuntimeError("ENV vars missing")

service_account_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)

creds = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"],
)


# =========================
# LOGGING
# =========================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# =========================
# GOOGLE SHEETS
# =========================
sheets = build("sheets", "v4", credentials=creds)

# =========================
# STATES
# =========================
STATE_ONBOARDING = "ONBOARDING"
STATE_DIALOGS = "DIALOGS"
STATE_RECOMMENDATION = "RECOMMENDATION"
STATE_EMPTY = "EMPTY"


# =========================
# USER STATE (TEMP)
# =========================
def get_state(context: ContextTypes.DEFAULT_TYPE) -> str:
    return context.user_data.get("state", STATE_ONBOARDING)


def set_state(context: ContextTypes.DEFAULT_TYPE, state: str):
    context.user_data["state"] = state


def get_main_message_id(context: ContextTypes.DEFAULT_TYPE):
    return context.user_data.get("main_message_id")


def set_main_message_id(context: ContextTypes.DEFAULT_TYPE, message_id: int):
    context.user_data["main_message_id"] = message_id


# =========================
# DATA ACCESS (MINIMAL)
# =========================
# =========================
# DATA ACCESS
# =========================
def user_exists(user_id: int) -> bool:
    rows = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="users!A2:A",
    ).execute().get("values", [])
    return any(int(r[0]) == user_id for r in rows if r)


def get_user_dialogs(user_id: int):
    rows = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="dialogs!A2:E",
    ).execute().get("values", [])

    dialogs = []
    for r in rows:
        if len(r) < 5:
            continue
        d_id, u1, u2, created_at, status = r
        try:
            if int(u1) == user_id or int(u2) == user_id:
                dialogs.append({
                    "dialog_id": d_id,
                    "status": status,
                })
        except Exception:
            continue

    return dialogs


# =========================
# RENDERERS
# =========================
def render_dialogs(user_id: int):
    dialogs = get_user_dialogs(user_id)

    lines = []
    buttons = []

    for i in range(3):
        if i < len(dialogs):
            status = dialogs[i]["status"]
            lines.append(f"{i+1}. {status}")
            buttons.append(
                InlineKeyboardButton(
                    f"Диалог {i+1}",
                    callback_data=f"dialog:{dialogs[i]['dialog_id']}"
                )
            )
        else:
            lines.append(f"{i+1}. —")
            buttons.append(
                InlineKeyboardButton(
                    f"Диалог {i+1}",
                    callback_data="dialog:empty"
                )
            )

    text = "Диалоги\n\n" + "\n".join(lines)

    kb = InlineKeyboardMarkup([
        buttons,
        [InlineKeyboardButton("Профиль", callback_data="profile:view")]
    ])

    return text, kb


def render_recommendation(user_id: int):
    text = (
        "Рекомендация\n\n"
        "Имя\n"
        "Возраст\n"
        "Город\n\n"
        "О себе"
    )

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Начать диалог", callback_data="rec:start"),
            InlineKeyboardButton("Пропустить", callback_data="rec:skip"),
        ]
    ])

    return text, kb


def render_empty():
    text = "На сегодня предложений больше нет"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("К диалогам", callback_data="go:dialogs")]
    ])
    return text, kb


# =========================
# SCREEN ROUTER
# =========================
async def show_screen(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    keyboard: InlineKeyboardMarkup,
):
    msg_id = get_main_message_id(context)

    if msg_id:
        try:
            await update.effective_chat.edit_message_text(
                message_id=msg_id,
                text=text,
                reply_markup=keyboard,
            )
            return
        except Exception:
            pass

    sent = await update.effective_chat.send_message(
        text=text,
        reply_markup=keyboard,
    )
    set_main_message_id(context, sent.message_id)


# =========================
# HANDLERS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()

    uid = update.effective_user.id

    if not user_exists(uid):
        set_state(context, STATE_ONBOARDING)
        text = "Онбординг"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Начать", callback_data="onboarding:start")]
        ])
        await show_screen(update, context, text, kb)
        return

    set_state(context, STATE_DIALOGS)
    text, kb = render_dialogs(uid)
    await show_screen(update, context, text, kb)


async def callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    uid = q.from_user.id
    data = q.data
    state = get_state(context)

    if data == "go:dialogs":
        set_state(context, STATE_DIALOGS)
        text, kb = render_dialogs(uid)
        await show_screen(update, context, text, kb)
        return

    if data.startswith("dialog:"):
        dialog_id = data.split(":")[1]

        if dialog_id == "empty":
            return

        # Пока только экран-заглушка диалога
        text = "Диалог"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Назад", callback_data="go:dialogs")]
        ])
        await show_screen(update, context, text, kb)
        return

    if data == "profile:view":
        text = "Профиль"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Назад", callback_data="go:dialogs")]
        ])
        await show_screen(update, context, text, kb)
        return


# =========================
# MAIN
# =========================
def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(callback))

    log.info("LUMEN CORE STARTED")
    app.run_polling()


if __name__ == "__main__":
    main()
