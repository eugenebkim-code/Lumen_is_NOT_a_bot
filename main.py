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
STATE_ONBOARDING_GENDER = "ONBOARDING_GENDER"
STATE_ONBOARDING_LOOKING_GENDER = "ONBOARDING_LOOKING_GENDER"
STATE_ONBOARDING_INTERESTS = "ONBOARDING_INTERESTS"
STATE_ONBOARDING_LOOKING_AGE_MIN = "STATE_ONBOARDING_LOOKING_AGE_MIN"
STATE_ONBOARDING_LOOKING_AGE_MAX = "STATE_ONBOARDING_LOOKING_AGE_MAX"
STATE_RECOMMENDATION = "RECOMMENDATION"
STATE_DIALOG = "DIALOG"
STATE_DIALOGS = "DIALOGS"
STATE_EMPTY = "EMPTY"

INTERESTS = [
    "Путешествия", "Музыка", "Кино", "Спорт",
    "Игры", "Книги", "IT", "Бизнес",
    "Еда", "Искусство", "Саморазвитие", "Прогулки",
]

# =========================
# META HELPERS
# =========================
PRESENCE_ACTIVE_SEC = 60  # считаем что юзер "сейчас в экране", если обновлялся в последние 60 сек
STATE_IDLE = "IDLE"
NOTIFY_COOLDOWN_SEC = 60
ACTIVE_WINDOW_SEC = 20

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def iso_to_dt(s: str | None):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def get_dialog_meta(dialog_id: str) -> dict:
    rows = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="dialog_meta!A2:E",
    ).execute().get("values", [])

    for r in rows:
        if not r or r[0] != dialog_id:
            continue
        # добиваем длину до 5
        r = r + [""] * (5 - len(r))
        return {
            "dialog_id": r[0],
            "u1_last_open_at": r[1],
            "u2_last_open_at": r[2],
            "u1_last_notify_at": r[3],
            "u2_last_notify_at": r[4],
        }

    return {
        "dialog_id": dialog_id,
        "u1_last_open_at": "",
        "u2_last_open_at": "",
        "u1_last_notify_at": "",
        "u2_last_notify_at": "",
    }

def upsert_dialog_meta(meta: dict):
    # simple strategy: read all, find row index, update; if not found append
    rows = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="dialog_meta!A2:A",
    ).execute().get("values", [])

    target_row = None
    for idx, r in enumerate(rows, start=2):
        if r and r[0] == meta["dialog_id"]:
            target_row = idx
            break

    values = [[
        meta.get("dialog_id", ""),
        meta.get("u1_last_open_at", ""),
        meta.get("u2_last_open_at", ""),
        meta.get("u1_last_notify_at", ""),
        meta.get("u2_last_notify_at", ""),
    ]]

    if target_row:
        sheets.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"dialog_meta!A{target_row}:E{target_row}",
            valueInputOption="RAW",
            body={"values": values},
        ).execute()
    else:
        sheets.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range="dialog_meta!A2",
            valueInputOption="RAW",
            body={"values": values},
        ).execute()

def get_presence(user_id: int) -> dict:
    rows = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="presence!A2:E",
    ).execute().get("values", [])

    for r in rows:
        if not r or not r[0]:
            continue
        try:
            uid = int(r[0])
        except Exception:
            continue
        if uid != user_id:
            continue

        r = r + [""] * (5 - len(r))
        return {
            "user_id": uid,
            "state": r[1] or "",
            "current_dialog_id": r[2] or "",
            "main_message_id": r[3] or "",
            "updated_at": r[4] or "",
        }

    return {
        "user_id": user_id,
        "state": "",
        "current_dialog_id": "",
        "main_message_id": "",
        "updated_at": "",
    }


def upsert_presence(p: dict):
    rows = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="presence!A2:A",
    ).execute().get("values", [])

    target_row = None
    for idx, r in enumerate(rows, start=2):
        if r and r[0] and int(r[0]) == int(p["user_id"]):
            target_row = idx
            break

    values = [[
        str(p.get("user_id", "")),
        p.get("state", ""),
        p.get("current_dialog_id", ""),
        str(p.get("main_message_id", "")) if p.get("main_message_id", "") != "" else "",
        p.get("updated_at", ""),
    ]]

    if target_row:
        sheets.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"presence!A{target_row}:E{target_row}",
            valueInputOption="RAW",
            body={"values": values},
        ).execute()
    else:
        sheets.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range="presence!A2",
            valueInputOption="RAW",
            body={"values": values},
        ).execute()


def set_presence(user_id: int, state: str, current_dialog_id: str = "", main_message_id: int | None = None):
    p = get_presence(user_id)
    p["state"] = state
    p["current_dialog_id"] = current_dialog_id or ""
    if main_message_id is not None:
        p["main_message_id"] = str(main_message_id)
    p["updated_at"] = utc_now_iso()
    upsert_presence(p)

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

def save_user(profile: dict, user: Update.effective_user):
    now = datetime.now(timezone.utc).isoformat()

    row = [
        user.id,
        now,
        user.username or "",
        profile.get("name", ""),
        profile.get("age", ""),
        profile.get("city", ""),
        profile.get("gender", ""),
        profile.get("about", ""),
        True,
        profile.get("looking_for_gender", ""),
        profile.get("looking_for_age_min", ""),
        profile.get("looking_for_age_max", ""),
        profile.get("photo_main", ""),
        ", ".join(profile.get("interests", [])),
    ]

    sheets.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="users!A2",
        valueInputOption="RAW",
        body={"values": [row]},
    ).execute()


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

def render_recommendation_card(user: dict):
    text = (
        f"{user['name']}, {user['age']}\n"
        f"{user['city']}\n\n"
        f"{user['about']}"
    )

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "💬 Начать диалог",
                callback_data=f"rec:start:{user['user_id']}"
            ),
            InlineKeyboardButton(
                "➡️ Пропустить",
                callback_data="rec:skip"
            ),
        ]
    ])

    return text, kb

def render_empty():
    text = "На сегодня предложений больше нет"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("К диалогам", callback_data="go:dialogs")]
    ])
    return text, kb

def render_interests_keyboard(context: ContextTypes.DEFAULT_TYPE):
    selected = set(context.user_data.get("profile", {}).get("interests", []))

    buttons = []
    row = []

    for interest in INTERESTS:
        prefix = "✅ " if interest in selected else ""
        row.append(
            InlineKeyboardButton(
                prefix + interest,
                callback_data=f"interest:{interest}"
            )
        )
        if len(row) == 2:
            buttons.append(row)
            row = []

    if row:
        buttons.append(row)

    buttons.append([
        InlineKeyboardButton("Готово", callback_data="interests:done")
    ])

    return InlineKeyboardMarkup(buttons)

# =========================
# SCREEN ROUTER
# =========================

async def show_recommendation(update, context, user: dict):
    # удаляем старое главное сообщение
    msg_id = context.user_data.pop("main_message_id", None)
    if msg_id:
        try:
            await update.effective_chat.delete_message(msg_id)
        except Exception:
            pass

    text = (
        f"{user['name']}, {user['age']}\n"
        f"{user['city']}\n\n"
        f"{user['about']}"
    )

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "💬 Начать диалог",
                callback_data=f"rec:start:{user['user_id']}"
            ),
            InlineKeyboardButton(
                "➡️ Пропустить",
                callback_data="rec:skip"
            ),
        ]
    ])

    sent = await update.effective_chat.send_photo(
        photo=user["photo_main"],
        caption=text,
        reply_markup=kb,
    )

    context.user_data["main_message_id"] = sent.message_id

    set_presence(
        user_id=update.effective_user.id,
        state=get_state(context),
        current_dialog_id=context.user_data.get("current_dialog_id", ""),
        main_message_id=sent.message_id
    )

async def show_screen(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    keyboard: InlineKeyboardMarkup,
):
    if get_state(context) == STATE_DIALOG:
        log.warning("show_screen called inside STATE_DIALOG - forbidden")
        return  # ⬅️ КРИТИЧНО: сразу выходим

    msg_id = context.user_data.pop("main_message_id", None)

    if msg_id:
        try:
            await update.effective_chat.delete_message(msg_id)
        except Exception:
            pass

    sent = await update.effective_chat.send_message(
        text=text,
        reply_markup=keyboard,
    )

    set_main_message_id(context, sent.message_id)

    set_presence(
        user_id=update.effective_user.id,
        state=get_state(context),
        current_dialog_id=context.user_data.get("current_dialog_id", ""),
        main_message_id=sent.message_id,
    )

async def render_dialog_screen(
    update: Update | None,
    context: ContextTypes.DEFAULT_TYPE | None,
    dialog_id: str,
    user_id: int,
):
    presence = get_presence(user_id)
    old_mid = presence.get("main_message_id")

    bot = None
    chat_id = user_id

    # удаляем старый экран
    if old_mid:
        try:
            if update:
                # Chat API
                await update.effective_chat.delete_message(int(old_mid))
            else:
                # Bot API
                await context.application.bot.delete_message(
                    chat_id=user_id,
                    message_id=int(old_mid),
                )
        except Exception:
            pass

    # рендерим диалог
    text, kb = render_dialog(dialog_id, user_id)

    if update:
        # Chat API
        sent = await update.effective_chat.send_message(
            text=text,
            reply_markup=kb,
        )
    else:
        # Bot API
        sent = await context.application.bot.send_message(
            chat_id=user_id,
            text=text,
            reply_markup=kb,
        )

    set_presence(
        user_id=user_id,
        state=STATE_DIALOG,
        current_dialog_id=dialog_id,
        main_message_id=sent.message_id,
    )

    if context:
        context.user_data["current_dialog_id"] = dialog_id
        context.user_data["main_message_id"] = sent.message_id
        set_state(context, STATE_DIALOG)


def load_user_profile(user_id: int) -> dict | None:
    rows = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="users!A2:N",
    ).execute().get("values", [])

    for r in rows:
        if not r:
            continue

        try:
            uid = int(r[0])
        except Exception:
            continue

        if uid != user_id:
            continue

        return {
            "user_id": uid,
            "created_at": r[1],
            "username": r[2],
            "name": r[3],
            "age": int(r[4]),
            "city": r[5],
            "gender": r[6],
            "about": r[7],
            "onboarding_completed": str(r[8]).upper() == "TRUE",
            "looking_for_gender": r[9],
            "looking_for_age_min": int(r[10]),
            "looking_for_age_max": int(r[11]),
            "photo_main": r[12],
            "interests": [i.strip() for i in r[13].split(",") if i.strip()],
            "photos": [r[12]] if r[12] else [],
        }

    return None

def get_user_name(user_id: int) -> str:
    rows = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="users!A2:D",
    ).execute().get("values", [])

    for r in rows:
        if not r:
            continue
        try:
            if int(r[0]) == user_id:
                return r[3] or "пользователем"
        except Exception:
            continue

    return "пользователем"


# =========================
# HANDLERS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log.info("START | user=%s", update.effective_user.id)

    # сохраняем main_message_id при рестарте
    main_msg_id = context.user_data.get("main_message_id")
    context.user_data.clear()
    if main_msg_id:
        context.user_data["main_message_id"] = main_msg_id

    uid = update.effective_user.id

    # === 1. ПЫТАЕМСЯ ЗАГРУЗИТЬ ПРОФИЛЬ ===
    profile = load_user_profile(uid)

    # === 2. ЕСЛИ ПРОФИЛЯ НЕТ → ОНБОРДИНГ ===
    if not profile:
        set_state(context, STATE_ONBOARDING_NAME)
        context.user_data["profile"] = {
            "photos": []
        }

        await show_screen(
            update,
            context,
            "Как тебя зовут?",
            InlineKeyboardMarkup([])
        )
        return

    # === 3. ПРОФИЛЬ ЕСТЬ ===
    context.user_data["profile"] = profile

    rec = find_recommendation(uid, profile)

    if not rec:
        text, kb = render_empty()
        await show_screen(update, context, text, kb)
        return

    set_state(context, STATE_RECOMMENDATION)
    await show_recommendation(update, context, rec)

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

        # presence приводим в нейтральное состояние
        set_presence(
            user_id=uid,
            state=STATE_IDLE,
            current_dialog_id="",
            main_message_id=context.user_data.get("main_message_id"),
        )
        return

    if data.startswith("dialog:"):
        dialog_id = data.split(":")[1]
        if dialog_id == "empty":
            return

        # 1. фиксируем open_at (ТОЛЬКО meta)
        u1, u2 = get_dialog_users(dialog_id)
        meta = get_dialog_meta(dialog_id)
        now = utc_now_iso()

        if uid == u1:
            meta["u1_last_open_at"] = now
        elif uid == u2:
            meta["u2_last_open_at"] = now

        upsert_dialog_meta(meta)

        # 2. единственный вход в экран диалога
        await render_dialog_screen(update, context, dialog_id, uid)
        return

    if data.startswith("gender:"):
        profile = context.user_data["profile"]
        profile["gender"] = data.split(":")[1]
        context.user_data["profile"] = profile

        set_state(context, STATE_ONBOARDING_ABOUT)
        await show_screen(update, context, "Пару слов о себе", InlineKeyboardMarkup([]))
        return

    if data == "profile:view":
        text = "Профиль"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Назад", callback_data="go:dialogs")]
        ])
        await show_screen(update, context, text, kb)
        return
    
    if data.startswith("looking:"):
        profile = context.user_data["profile"]
        profile["looking_for_gender"] = data.split(":")[1]
        context.user_data["profile"] = profile

        set_state(context, STATE_ONBOARDING_LOOKING_AGE_MIN)
        await show_screen(
            update,
            context,
            "Минимальный возраст (числом)",
            InlineKeyboardMarkup([])
        )
        return

    if data.startswith("interest:"):
        interest = data.split(":", 1)[1]
        profile = context.user_data["profile"]
        interests = set(profile.get("interests", []))

        if interest in interests:
            interests.remove(interest)
        else:
            if len(interests) >= 6:
                await q.answer("Можно выбрать максимум 6", show_alert=True)
                return
            interests.add(interest)

        profile["interests"] = list(interests)
        context.user_data["profile"] = profile

        await q.edit_message_reply_markup(
            reply_markup=render_interests_keyboard(context)
        )
        return

    if data == "interests:done":
        profile = context.user_data["profile"]

        profile["onboarding_completed"] = True
        context.user_data["profile"] = profile

        save_user(profile, q.from_user)

        set_state(context, STATE_DIALOGS)

        await show_screen(
            update,
            context,
            "Профиль готов ✅\n\n"
            "Ты можешь посмотреть рекомендации на сегодня\n"
            "или вернуться к диалогам.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🔍 Рекомендации", callback_data="go:recommendations")],
                [InlineKeyboardButton("💬 Диалоги", callback_data="go:dialogs")]
            ])
        )
        return

    if data == "onboarding:finish":
        set_state(context, STATE_ONBOARDING_LOOKING_GENDER)

        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Мужчин", callback_data="looking:male"),
                InlineKeyboardButton("Женщин", callback_data="looking:female"),
            ],
            [InlineKeyboardButton("Всех", callback_data="looking:any")]
        ])

        await show_screen(update, context, "Кого ты ищешь?", kb)
        return
    
    if data == "go:recommendations":
        profile = context.user_data["profile"]
        rec = find_recommendation(uid, profile)

        if not rec:
            text, kb = render_empty()
            await show_screen(update, context, text, kb)
            context.user_data["current_dialog_id"] = ""
            set_presence(uid, STATE_IDLE, "", context.user_data.get("main_message_id"))
            return

        set_state(context, STATE_RECOMMENDATION)
        await show_recommendation(update, context, rec)
        return
    
    # =========================
    # RECOMMENDATIONS ACTIONS
    # =========================
    if data == "rec:skip":
        profile = context.user_data.get("profile") or load_user_profile(uid)
        context.user_data["profile"] = profile

        rec = find_recommendation(uid, profile)

        if not rec:
            text, kb = render_empty()
            await show_screen(update, context, text, kb)
            return

        set_state(context, STATE_RECOMMENDATION)
        await show_recommendation(update, context, rec)
        return

    if data.startswith("rec:start:"):
        other_id = int(data.split(":")[2])

        dialog_id = create_dialog(uid, other_id)

        # фиксируем meta open_at (чтобы active-window работал корректно)
        u1, u2 = get_dialog_users(dialog_id)
        meta = get_dialog_meta(dialog_id)
        now = utc_now_iso()
        if uid == u1:
            meta["u1_last_open_at"] = now
        elif uid == u2:
            meta["u2_last_open_at"] = now
        upsert_dialog_meta(meta)

        await render_dialog_screen(update, context, dialog_id, uid)
        return
    
# =========================
# ONBOARDING
# =========================

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context)
    text = update.message.text.strip()

    profile = context.user_data.get("profile", {})

    if state == STATE_DIALOG:
        dialog_id = context.user_data.get("current_dialog_id")
        if not dialog_id:
            return

        from_user = update.effective_user.id

        save_dialog_message(dialog_id, from_user, text)
        await notify_new_dialog(
            context.application,
            context,
            dialog_id,
            from_user,
        )

        await render_dialog_screen(
            update=update,
            context=context,
            dialog_id=dialog_id,
            user_id=from_user,
        )
        return

    if state == STATE_ONBOARDING_LOOKING_AGE_MIN:
        if not text.isdigit() or not (18 <= int(text) <= 99):
            await update.message.reply_text("Возраст числом, от 18 до 99")
            return

        profile["looking_for_age_min"] = int(text)
        context.user_data["profile"] = profile

        set_state(context, STATE_ONBOARDING_LOOKING_AGE_MAX)
        await show_screen(
            update,
            context,
            "Максимальный возраст (числом)",
            InlineKeyboardMarkup([])
        )
        return

    if state == STATE_ONBOARDING_LOOKING_AGE_MAX:
        if not text.isdigit() or not (18 <= int(text) <= 99):
            await update.message.reply_text("Возраст числом, от 18 до 99")
            return

        if int(text) < profile.get("looking_for_age_min", 18):
            await update.message.reply_text("Максимальный возраст не может быть меньше минимального")
            return

        profile["looking_for_age_max"] = int(text)
        context.user_data["profile"] = profile

        set_state(context, STATE_ONBOARDING_INTERESTS)
        await show_screen(
            update,
            context,
            "Выбери интересы (до 6)",
            render_interests_keyboard(context)
        )
        return

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
        set_state(context, STATE_ONBOARDING_GENDER)
        context.user_data["profile"] = profile

        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Мужчина", callback_data="gender:male"),
                InlineKeyboardButton("Женщина", callback_data="gender:female"),
            ],
            [InlineKeyboardButton("Не указывать", callback_data="gender:other")]
        ])

        await show_screen(update, context, "Укажи свой пол", kb)
        return

    if state == STATE_ONBOARDING_GENDER:
        await update.message.reply_text("Выбери вариант кнопкой")
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
# RECOMMENDATIONS
# =========================

def get_all_users():
    rows = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="users!A2:N",
    ).execute().get("values", [])

    users = []

    for r in rows:
        # ❗ защита от пустых и кривых строк
        if not r or not r[0]:
            continue

        try:
            user_id = int(r[0])
            age = int(r[4])
            age_min = int(r[10])
            age_max = int(r[11])
        except Exception:
            continue

        users.append({
            "user_id": user_id,
            "username": r[2],
            "name": r[3],
            "age": age,
            "city": r[5],
            "gender": r[6],
            "about": r[7],
            "onboarding_completed": str(r[8]).upper() == "TRUE",
            "looking_for_gender": r[9],
            "looking_for_age_min": age_min,
            "looking_for_age_max": age_max,
            "photo_main": r[12],
            "interests": [i.strip() for i in r[13].split(",") if i.strip()],
        })

    return users

def find_recommendation(current_user_id: int, profile: dict):
    users = get_all_users()

    for u in users:
        if not u["onboarding_completed"]:
            continue
        if u["user_id"] == current_user_id:
            continue

        # возраст
        if not (profile["looking_for_age_min"] <= u["age"] <= profile["looking_for_age_max"]):
            continue

        # пол
        lf = profile["looking_for_gender"]
        if lf != "any" and u["gender"] != lf:
            continue

        return u  # первый подходящий

    return None

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
# DIALOGS
# =========================

def create_dialog(user_1: int, user_2: int) -> str:
    dialog_id = f"{user_1}_{user_2}_{int(datetime.now().timestamp())}"
    now = datetime.now(timezone.utc).isoformat()

    sheets.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="dialogs!A2",
        valueInputOption="RAW",
        body={"values": [[dialog_id, user_1, user_2, now, "active"]]},
    ).execute()

    return dialog_id

def get_dialog_users(dialog_id: str):
    rows = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="dialogs!A2:E",
    ).execute().get("values", [])

    for r in rows:
        if not r or r[0] != dialog_id:
            continue
        return int(r[1]), int(r[2])

    return None, None

def save_dialog_message(dialog_id: str, from_user: int, text: str):
    now = datetime.now(timezone.utc).isoformat()

    sheets.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="dialog_messages!A2",
        valueInputOption="RAW",
        body={"values": [[dialog_id, from_user, text, now]]},
    ).execute()

def render_dialog(dialog_id: str, current_user: int):
    u1, u2 = get_dialog_users(dialog_id)
    other_id = u2 if u1 == current_user else u1
    other_name = get_user_name(other_id)

    rows = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="dialog_messages!A2:D",
    ).execute().get("values", [])

    msgs = [r for r in rows if r and r[0] == dialog_id][-10:]

    lines = []
    for _, from_user, msg_text, _ in msgs:
        prefix = "Ты:" if int(from_user) == current_user else f"{other_name}:"
        lines.append(f"{prefix} {msg_text}")

    if not lines:
        lines.append("Напиши первое сообщение 👇")

    text = f"Диалог с {other_name}\n\n" + "\n".join(lines)

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Назад", callback_data="go:dialogs")]
    ])

    return text, kb

async def notify_new_dialog(
    app,
    context: ContextTypes.DEFAULT_TYPE,
    dialog_id: str,
    from_user: int,
):
    u1, u2 = get_dialog_users(dialog_id)
    if not u1 or not u2:
        return

    target = u2 if u1 == from_user else u1
    now_dt = datetime.now(timezone.utc)

    meta = get_dialog_meta(dialog_id)

    if target == u1:
        last_open = iso_to_dt(meta.get("u1_last_open_at"))
        last_notify = iso_to_dt(meta.get("u1_last_notify_at"))
        notify_field = "u1_last_notify_at"
    else:
        last_open = iso_to_dt(meta.get("u2_last_open_at"))
        last_notify = iso_to_dt(meta.get("u2_last_notify_at"))
        notify_field = "u2_last_notify_at"

    # === active window ===
    if last_open and (now_dt - last_open).total_seconds() <= ACTIVE_WINDOW_SEC:
        return

    # === notify cooldown ===
    if last_notify and (now_dt - last_notify).total_seconds() <= NOTIFY_COOLDOWN_SEC:
        return

    presence = get_presence(target)
    presence_state = presence.get("state")
    presence_dialog = presence.get("current_dialog_id")
    presence_updated = iso_to_dt(presence.get("updated_at"))

    is_presence_fresh = (
        presence_updated is not None
        and (now_dt - presence_updated).total_seconds() <= PRESENCE_ACTIVE_SEC
    )

    # === пользователь уже в этом диалоге → тихо обновляем экран ===
    if (
        presence_state == STATE_DIALOG
        and presence_dialog == dialog_id
        and is_presence_fresh
    ):
        await render_dialog_screen(
            update=None,
            context=context,
            dialog_id=dialog_id,
            user_id=target,
        )
        return

    # === обычное уведомление ===
    await app.bot.send_message(
        chat_id=target,
        text="У тебя новое сообщение ✨",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Открыть", callback_data=f"dialog:{dialog_id}")]
        ])
    )

    meta[notify_field] = now_dt.isoformat()
    upsert_dialog_meta(meta)

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
