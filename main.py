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
import base64
import json

GOOGLE_SERVICE_ACCOUNT_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64")

if not BOT_TOKEN or not SPREADSHEET_ID or not GOOGLE_SERVICE_ACCOUNT_B64:
    log.error(
        "ENV CHECK | BOT_TOKEN=%s | SPREADSHEET_ID=%s | GOOGLE_B64=%s",
        bool(BOT_TOKEN),
        bool(SPREADSHEET_ID),
        bool(GOOGLE_SERVICE_ACCOUNT_B64),
    )
    raise RuntimeError("ENV vars missing")

service_account_json = base64.b64decode(GOOGLE_SERVICE_ACCOUNT_B64).decode("utf-8")
service_account_info = json.loads(service_account_json)

creds = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"],
)


# =========================
# GOOGLE SHEETS
# =========================
sheets = build("sheets", "v4", credentials=creds)

# =========================
# STATES
# =========================
STATE_ONBOARDING_NAME = "ONBOARDING_NAME"
STATE_ONBOARDING_AGE = "ONBOARDING_AGE"
STATE_ONBOARDING_CITY = "ONBOARDING_CITY"
STATE_ONBOARDING_ABOUT = "ONBOARDING_ABOUT"
STATE_ONBOARDING_PHOTO_MAIN = "ONBOARDING_PHOTO_MAIN"
STATE_ONBOARDING_PHOTO_EXTRA = "ONBOARDING_PHOTO_EXTRA"

STATE_DIALOGS = "DIALOGS"
STATE_RECOMMENDATION = "RECOMMENDATION"
STATE_EMPTY = "EMPTY"


# =========================
# USER STATE (TEMP)
# =========================
def get_state(context: ContextTypes.DEFAULT_TYPE) -> str:
    return context.user_data.get("state", STATE_ONBOARDING_NAME)


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
    log.info("SEND NEW MAIN MESSAGE")
    if msg_id:
        try:
            await update.effective_chat.edit_message_text(
                message_id=msg_id,
                text=text,
                reply_markup=keyboard,
            )
            return
        except Exception:
            context.user_data.pop("main_message_id", None)

    sent = await update.effective_chat.send_message(
        text=text,
        reply_markup=keyboard,
    )
    set_main_message_id(context, sent.message_id)


# =========================
# HANDLERS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log.info("START | user=%s", update.effective_user.id)
    main_msg_id = context.user_data.get("main_message_id")
    context.user_data.clear()
    if main_msg_id:
        context.user_data["main_message_id"] = main_msg_id

    uid = update.effective_user.id

    if not user_exists(uid):
        set_state(context, STATE_ONBOARDING_NAME)
        context.user_data["profile"] = {
            "photos": []
        }
        text = "Как тебя зовут?"
        kb = InlineKeyboardMarkup([
            
        ])
        await show_screen(update, context, text, kb)
        return

    set_state(context, STATE_DIALOGS)
    text, kb = render_dialogs(uid)
    await show_screen(update, context, text, kb)

# =========================
# CALLBACK
# =========================


async def callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    uid = q.from_user.id
    data = q.data
    state = get_state(context)
    
    if data == "onboarding:start":
        set_state(context, STATE_DIALOGS)
        text, kb = render_dialogs(uid)
        await show_screen(update, context, text, kb)
        return

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
    
    if data == "onboarding:finish":
        set_state(context, STATE_DIALOGS)

        # ВАЖНО: тут позже будет запись в users sheet
        log.info("ONBOARDING DONE | profile=%s", context.user_data.get("profile"))

        text, kb = render_dialogs(uid)
        await show_screen(update, context, text, kb)
    return
    
# =========================
# ONBOARDING
# =========================

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context)
    text = update.message.text.strip()

    profile = context.user_data.get("profile", {})

    if state == STATE_ONBOARDING_NAME:
        profile["name"] = text
        set_state(context, STATE_ONBOARDING_AGE)
        context.user_data["profile"] = profile
        await show_screen(update, context, "Сколько тебе лет?", InlineKeyboardMarkup([]))
        return

    if state == STATE_ONBOARDING_AGE:
        if not text.isdigit() or not (18 <= int(text) <= 99):
            await update.message.reply_text("Возраст числом, от 18 до 99")
            return
        profile["age"] = int(text)
        set_state(context, STATE_ONBOARDING_CITY)
        context.user_data["profile"] = profile
        await show_screen(update, context, "Из какого ты города?", InlineKeyboardMarkup([]))
        return

    if state == STATE_ONBOARDING_CITY:
        profile["city"] = text
        set_state(context, STATE_ONBOARDING_ABOUT)
        context.user_data["profile"] = profile
        await show_screen(update, context, "Пару слов о себе", InlineKeyboardMarkup([]))
        return

    if state == STATE_ONBOARDING_ABOUT:
        profile["about"] = text
        set_state(context, STATE_ONBOARDING_PHOTO_MAIN)
        context.user_data["profile"] = profile
        await show_screen(
            update,
            context,
            "Загрузи главное фото\n(без него нельзя продолжить)",
            InlineKeyboardMarkup([])
        )
        return

# =========================
# PHOTO
# =========================

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context)
    profile = context.user_data.get("profile", {})

    photos = profile.get("photos", [])

    if state == STATE_ONBOARDING_PHOTO_MAIN:
        file_id = update.message.photo[-1].file_id
        profile["photo_main"] = file_id
        photos.append(file_id)
        profile["photos"] = photos

        set_state(context, STATE_ONBOARDING_PHOTO_EXTRA)
        context.user_data["profile"] = profile

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Готово", callback_data="onboarding:finish")]
        ])

        await show_screen(
            update,
            context,
            "Можно добавить еще до 2 фото\nили нажми «Готово»",
            kb
        )
        return

    if state == STATE_ONBOARDING_PHOTO_EXTRA:
        if len(photos) >= 3:
            await update.message.reply_text("Можно максимум 3 фото")
            return

        file_id = update.message.photo[-1].file_id
        photos.append(file_id)
        profile["photos"] = photos
        context.user_data["profile"] = profile
        return

# =========================
# MAIN
# =========================
def main():
    app = Application.builder().token(BOT_TOKEN).build()

    from telegram.ext import MessageHandler, filters

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))

    log.info("LUMEN CORE STARTED")
    app.run_polling()


if __name__ == "__main__":
    main()
