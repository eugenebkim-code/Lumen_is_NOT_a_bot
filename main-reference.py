import os
import logging
from datetime import datetime, timezone, timedelta
from uuid import uuid4

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

from google.oauth2 import service_account
from googleapiclient.discovery import build


# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")

if not BOT_TOKEN or not SPREADSHEET_ID or not GOOGLE_SERVICE_ACCOUNT_FILE:
    raise RuntimeError("ENV vars missing")


# =========================
# LOGGING
# =========================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# =========================
# GOOGLE SHEETS
# =========================
creds = service_account.Credentials.from_service_account_file(
    GOOGLE_SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/spreadsheets"],
)
sheets = build("sheets", "v4", credentials=creds)


def now():
    return datetime.now(timezone.utc).isoformat()

# =========================
# INTERESTS
# =========================
INTERESTS = [
    "Музыка", "Путешествия", "Кино", "Книги", "Спорт",
    "Фитнес", "IT", "Игры", "Аниме", "Кофе",
    "Кулинария", "Прогулки", "Природа", "Фото",
    "Искусство", "Психология", "Саморазвитие",
    "Бизнес", "Авто", "Животные"
]

# =========================
# PROFILE EDIT LIMIT
# =========================
def can_edit_profile(context: ContextTypes.DEFAULT_TYPE) -> bool:
    last = context.user_data.get("last_profile_edit_at")
    if not last:
        return True
    return datetime.now(timezone.utc) - last >= timedelta(hours=24)


# =========================
# USERS
# =========================
def get_user(user_id: int):
    rows = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="users!A2:Z",
    ).execute().get("values", [])

    for r in reversed(rows):
        if not r:
            continue
        try:
            if int(r[0]) != user_id:
                continue
        except Exception:
            continue

        return {
            "user_id": int(r[0]),
            "name": r[3],
            "age": int(r[4]),
            "city": r[5],
            "gender": r[6],
            "about": r[7],
            "looking_for_gender": r[9],
            "looking_for_age_min": int(r[10]),
            "looking_for_age_max": int(r[11]),
            "photo_file_id": r[12] if len(r) > 12 else "",
            "interests": r[13].split(",") if len(r) > 13 and r[13] else [],
            "onboarding_completed": r[8] == "TRUE",
        }
    return None


def get_user_row(user_id: int):
    rows = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="users!A2:Z",
    ).execute().get("values", [])

    for idx, r in enumerate(rows, start=2):
        if r and int(r[0]) == user_id:
            return idx
    return None


def update_user_field(user_id: int, col_letter: str, value):
    row = get_user_row(user_id)
    if not row:
        return
    sheets.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"users!{col_letter}{row}",
        valueInputOption="RAW",
        body={"values": [[value]]},
    ).execute()


def create_user(data: dict):
    sheets.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="users!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [[
            data["user_id"],
            now(),
            data.get("username", ""),
            data["name"],
            data["age"],
            data["city"],
            data["gender"],
            data["about"],
            "TRUE",
            data["looking_for_gender"],
            data["looking_for_age_min"],
            data["looking_for_age_max"],
            data.get("photo_file_id", ""),
            ",".join(data.get("interests", [])),
        ]]},
    ).execute()


def get_all_users():
    rows = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="users!A2:Z",
    ).execute().get("values", [])

    users = []
    for r in rows:
        if len(r) < 12 or r[8] != "TRUE":
            continue
        try:
            users.append({
                "user_id": int(r[0]),
                "name": r[3],
                "age": int(r[4]),
                "gender": r[6],
                "looking_for_gender": r[9],
                "looking_for_age_min": int(r[10]),
                "looking_for_age_max": int(r[11]),
            })
        except Exception:
            continue
    return users

def get_recommendation(user_id: int):
    print("RECOMMENDATION CALLED FOR", user_id)
    me = get_user(user_id)
    if not me:
        return None

    my_interests = set(me.get("interests", []))

    candidates = []

    rows = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="users!A2:Z",
    ).execute().get("values", [])

    for r in rows:
        try:
            uid = int(r[0])
        except Exception:
            continue

        if uid == user_id:
            continue

        gender = r[6]
        age = int(r[4])

        if me["looking_for_gender"] != "any" and gender != me["looking_for_gender"]:
            continue

        if not (me["looking_for_age_min"] <= age <= me["looking_for_age_max"]):
            continue

        interests = set(
            r[13].split(",") if len(r) > 13 and r[13] else []
        )

        score = len(my_interests & interests)

        candidates.append({
            "user_id": uid,
            "name": r[3],
            "age": age,
            "city": r[5],
            "about": r[7],
            "photo_file_id": r[12] if len(r) > 12 else "",
            "interests": interests,
            "score": score,
        })

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates[0]

async def show_recommendation(q, uid):
    rec = get_recommendation(uid)

    if not rec:
        await q.message.reply_text("Пока нет подходящих рекомендаций")
        return

    text = (
        "🔥 Рекомендация\n\n"
        f"Имя: {rec['name']}\n"
        f"Возраст: {rec['age']}\n"
        f"Город: {rec['city']}\n\n"
        f"О себе:\n{rec['about']}"
    )

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("❤️", callback_data=f"rec:like:{rec['user_id']}"),
            InlineKeyboardButton("⏭", callback_data=f"rec:skip:{rec['user_id']}"),
        ]
    ])

    if rec.get("photo_file_id"):
        await q.message.reply_photo(
            photo=rec["photo_file_id"],
            caption=text,
            reply_markup=kb
        )
    else:
        await q.message.reply_text(
            text,
            reply_markup=kb
        )

# =========================
# DIALOGS
# =========================
def get_all_dialogs():
    return sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="dialogs!A2:E",
    ).execute().get("values", [])


def get_user_dialogs(user_id: int):
    return [d for d in get_all_dialogs() if str(user_id) in (d[1], d[2])]


def get_open_dialogs(user_id: int):
    return [d for d in get_user_dialogs(user_id) if d[4] == "OPEN"]


def dialog_exists(u1: int, u2: int):
    for d in get_all_dialogs():
        if {str(u1), str(u2)} == {d[1], d[2]}:
            return True
    return False


def create_dialog(user_id: int):
    if len(get_open_dialogs(user_id)) >= 3:
        return None

    me = get_user(user_id)
    candidates = []

    for u in get_all_users():
        if u["user_id"] == user_id:
            continue
        if dialog_exists(user_id, u["user_id"]):
            continue
        if len(get_open_dialogs(u["user_id"])) >= 3:
            continue
        if me["looking_for_gender"] != "any" and u["gender"] != me["looking_for_gender"]:
            continue
        if not (me["looking_for_age_min"] <= u["age"] <= me["looking_for_age_max"]):
            continue
        candidates.append(u)

    if not candidates:
        return None

    partner = candidates[0]
    dialog_id = str(uuid4())

    sheets.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="dialogs!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [[dialog_id, user_id, partner["user_id"], now(), "OPEN"]]},
    ).execute()

    return dialog_id

def create_dialog_between(u1: int, u2: int):
    if dialog_exists(u1, u2):
        return None

    if len(get_open_dialogs(u1)) >= 3 or len(get_open_dialogs(u2)) >= 3:
        return None

    dialog_id = str(uuid4())

    sheets.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="dialogs!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [[dialog_id, u1, u2, now(), "OPEN"]]},
    ).execute()

    return dialog_id

def close_dialog(dialog_id: str):
    rows = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="dialogs!A2:E",
    ).execute().get("values", [])

    for i, r in enumerate(rows, start=2):
        if r and r[0] == dialog_id:
            sheets.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"dialogs!E{i}",
                valueInputOption="RAW",
                body={"values": [["CLOSED"]]},
            ).execute()
            return


def add_message(dialog_id: str, sender_id: int, text: str):
    sheets.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="messages!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [[dialog_id, sender_id, text, now()]]},
    ).execute()

def has_mutual_like(user_id: int, target_user_id: int) -> bool:
    rows = sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="preferences!A2:D",
    ).execute().get("values", [])

    liked_by_target = False
    liked_by_me = False

    for r in rows:
        if len(r) < 3:
            continue

        try:
            u_from = int(r[0])
            u_to = int(r[1])
            action = r[2]
        except Exception:
            continue

        if u_from == user_id and u_to == target_user_id and action == "like":
            liked_by_me = True

        if u_from == target_user_id and u_to == user_id and action == "like":
            liked_by_target = True

        if liked_by_me and liked_by_target:
            return True

    return False

# =========================
# PREFERENCES
# =========================
def save_preference(user_id: int, target_user_id: int, action: str):
    sheets.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="preferences!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [[
            user_id,
            target_user_id,
            action,
            now()
        ]]},
    ).execute()

# =========================
# UI
# =========================
def dialogs_keyboard(user_id: int, active_dialog_id):
    dialogs = get_user_dialogs(user_id)[:3]
    buttons = []

    for i in range(3):
        if i < len(dialogs):
            prefix = "▶️ " if dialogs[i][0] == active_dialog_id else "💬 "
            label = f"{prefix}Диалог {i+1}"
        else:
            label = f"➕ Диалог {i+1}"
        buttons.append(
            InlineKeyboardButton(label, callback_data=f"dialog_slot:{i}")
        )

    rows = [buttons]

    if active_dialog_id:
        rows.append([
            InlineKeyboardButton("⬅️ К диалогам", callback_data="dialogs_home"),
            InlineKeyboardButton("❌ Закрыть диалог", callback_data=f"dialog_close:{active_dialog_id}")
        ])
        rows.append([
            InlineKeyboardButton("👥 Анкета собеседника", callback_data="dialog:partner_profile")
        ])

    rows.append([
        InlineKeyboardButton("👤 Моя анкета", callback_data="profile:view")
    ])

    return InlineKeyboardMarkup(rows)

def interests_keyboard(selected: set):
    rows = []
    for i in range(0, len(INTERESTS), 2):
        row = []
        for interest in INTERESTS[i:i+2]:
            prefix = "✅ " if interest in selected else ""
            row.append(
                InlineKeyboardButton(
                    prefix + interest,
                    callback_data=f"interest:{interest}"
                )
            )
        rows.append(row)

    rows.append([
        InlineKeyboardButton("Готово", callback_data="interests:done")
    ])

    return InlineKeyboardMarkup(rows)

def recommendation_keyboard(target_user_id: int):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Начать диалог", callback_data=f"rec:start:{target_user_id}"),
            InlineKeyboardButton("Пропустить", callback_data=f"rec:skip:{target_user_id}")
        ]
    ])

def render_profile(user: dict):
    gender_map = {"m": "Мужчина", "f": "Женщина", "any": "Не важно"}

    text = (
        "👤 Твоя анкета\n\n"
        f"Имя: {user['name']}\n"
        f"Возраст: {user['age']}\n"
        f"Город: {user['city']}\n"
        f"Пол: {gender_map.get(user['gender'], user['gender'])}\n\n"
        "Ищу:\n"
        f"{gender_map.get(user['looking_for_gender'], user['looking_for_gender'])}\n"
        f"Возраст: {user['looking_for_age_min']}–{user['looking_for_age_max']}\n\n"
        f"Интересы:\n{', '.join(user.get('interests', [])) or '—'}\n\n"
        f"О себе:\n{user['about']}\n\n"
        f"Фото: {'✅' if user.get('photo_file_id') else '❌'}"
    )

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Редактировать", callback_data="profile:edit")],
        [InlineKeyboardButton("📷 Добавить / сменить фото", callback_data="profile:photo")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="dialogs_home")],
    ])

    return text, kb


# =========================
# HANDLERS
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    user = get_user(update.effective_user.id)

    if user:
        await update.message.reply_text(
            "Диалоги",
            reply_markup=dialogs_keyboard(update.effective_user.id, None)
        )
        return

    context.user_data["step"] = "name"
    await update.message.reply_text("Как тебя зовут?")


async def message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    uid = update.effective_user.id
    text = update.message.text.strip() if update.message.text else ""
    user = get_user(uid)

    edit_field = context.user_data.get("profile_edit_field")
    if edit_field and user:
        col_map = {
            "name": "D",
            "age": "E",
            "city": "F",
            "about": "H",
            "looking_for_age_min": "K",
            "looking_for_age_max": "L",
        }

        if edit_field in ("looking_for_age_min", "looking_for_age_max"):
            try:
                value = int(text)
            except ValueError:
                await update.message.reply_text("Введите число")
                return

            other = (
                user["looking_for_age_max"]
                if edit_field == "looking_for_age_min"
                else user["looking_for_age_min"]
            )

            if edit_field == "looking_for_age_min" and value > other:
                await update.message.reply_text("Минимальный возраст не может быть больше максимального")
                return

            if edit_field == "looking_for_age_max" and value < other:
                await update.message.reply_text("Максимальный возраст не может быть меньше минимального")
                return

            text = value

        update_user_field(uid, col_map[edit_field], text)
        context.user_data["last_profile_edit_at"] = datetime.now(timezone.utc)
        context.user_data.pop("profile_edit_field", None)
        context.user_data.pop("profile_edit_active", None)

        user = get_user(uid)
        t, kb = render_profile(user)
        await update.message.reply_text("Обновлено\n\n" + t, reply_markup=kb)
        return

    if context.user_data.get("step") == "profile_photo" and update.message.photo and user:
        file_id = update.message.photo[-1].file_id
        update_user_field(uid, "M", file_id)
        context.user_data["last_profile_edit_at"] = datetime.now(timezone.utc)
        context.user_data.pop("step", None)
        context.user_data.pop("profile_edit_active", None)

        user = get_user(uid)
        t, kb = render_profile(user)
        await update.message.reply_text("Фото обновлено\n\n" + t, reply_markup=kb)
        return

    if user:
        active = context.user_data.get("active_dialog_id")
        if not active:
            await update.message.reply_text(
                "Выбери диалог",
                reply_markup=dialogs_keyboard(uid, None)
            )
            return
        add_message(active, uid, text)
        return

    step = context.user_data.get("step")

    if step == "name":
        context.user_data["name"] = text
        context.user_data["step"] = "age"
        await update.message.reply_text("Сколько тебе лет?")
        return

    if step == "age":
        context.user_data["age"] = int(text)
        context.user_data["step"] = "city"
        await update.message.reply_text("Город?")
        return

    if step == "city":
        context.user_data["city"] = text
        context.user_data["step"] = "gender"
        await update.message.reply_text(
            "Твой пол?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Мужчина", callback_data="gender:m")],
                [InlineKeyboardButton("Женщина", callback_data="gender:f")],
            ])
        )
        return

    if step == "looking_age_min":
        try:
            context.user_data["looking_for_age_min"] = int(text)
        except ValueError:
            await update.message.reply_text("Введите число")
            return

        context.user_data["step"] = "looking_age_max"
        await update.message.reply_text("Максимальный возраст?")
        return

    if step == "looking_age_max":
        try:
            context.user_data["looking_for_age_max"] = int(text)
        except ValueError:
            await update.message.reply_text("Введите число")
            return

        if context.user_data["looking_for_age_max"] < context.user_data["looking_for_age_min"]:
            await update.message.reply_text("Максимальный возраст не может быть меньше минимального")
            return

        context.user_data["step"] = "about"
        await update.message.reply_text("Расскажи о себе")
        return

    if step == "about":
        context.user_data["about"] = text
        context.user_data["step"] = "interests"
        context.user_data["interests"] = set()

        await update.message.reply_text(
            "Выбери до 6 интересов:",
            reply_markup=interests_keyboard(context.user_data["interests"])
        )
        return

    if step == "photo" and update.message.photo:
        context.user_data["photo_file_id"] = update.message.photo[-1].file_id
        create_user(context.user_data | {
            "user_id": uid,
            "username": update.effective_user.username or ""
        })
        context.user_data.clear()

        await update.message.reply_text("Анкета готова")

        rec = get_recommendation(uid)
        if rec:
            text = (
                "🔥 Рекомендация\n\n"
                f"Имя: {rec['name']}\n"
                f"Возраст: {rec['age']}\n"
                f"Город: {rec['city']}\n\n"
                f"О себе:\n{rec['about']}"
            )

            kb = recommendation_keyboard(rec["user_id"])

            if rec.get("photo_file_id"):
                await update.message.reply_photo(
                    photo=rec["photo_file_id"],
                    caption=text,
                    reply_markup=kb
                )
            else:
                await update.message.reply_text(
                    text,
                    reply_markup=kb
                )
        else:
            await update.message.reply_text("Пока нет подходящих рекомендаций")
        return


async def callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    data = q.data

    if data.startswith("rec:"):
        _, action, target_id = data.split(":")
        target_id = int(target_id)

    if action == "like":
        save_preference(uid, target_id, "like")

        if has_mutual_like(uid, target_id):
            dialog_id = create_dialog_between(uid, target_id)

            if dialog_id:
                await q.message.reply_text(
                    "🔥 Это взаимно!\nДиалог открыт.",
                    reply_markup=dialogs_keyboard(uid, dialog_id)
                )
            else:
                await q.message.reply_text("🔥 Это взаимно!")
        else:
            await q.message.reply_text("❤️ Лайк сохранён")

    else:
        save_preference(uid, target_id, "skip")
        await q.message.reply_text("⏭ Пропущено")

    if data.startswith("interest:"):
        interest = data.split(":", 1)[1]
        selected = context.user_data.get("interests", set())

        if interest in selected:
            selected.remove(interest)
        else:
            if len(selected) >= 6:
                await q.message.reply_text("Можно выбрать не более 6 интересов")
                return
            selected.add(interest)

        context.user_data["interests"] = selected

        await q.message.edit_reply_markup(
            reply_markup=interests_keyboard(selected)
        )
        return

    if data == "interests:done":
        selected = context.user_data.get("interests", set())
        if not selected:
            await q.message.reply_text("Выбери хотя бы один интерес")
            return

        context.user_data["step"] = "photo"
        await q.message.reply_text(
            "Загрузи фото или нажми Пропустить",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Пропустить", callback_data="skip_photo")]
            ])
        )
        return


    if data == "profile:view":
        user = get_user(uid)
        t, kb = render_profile(user)

        if user.get("photo_file_id"):
            await q.message.reply_photo(
                photo=user["photo_file_id"],
                caption=t,
                reply_markup=kb
            )
        else:
            await q.message.reply_text(
                t,
                reply_markup=kb
            )
        return


    if data == "dialog:partner_profile":
        active_dialog_id = context.user_data.get("active_dialog_id")
        if not active_dialog_id:
            await q.message.reply_text("Диалог не выбран")
            return

        dialogs = get_user_dialogs(uid)
        dialog = next((d for d in dialogs if d[0] == active_dialog_id), None)
        if not dialog:
            await q.message.reply_text("Диалог не найден")
            return

        partner_id = int(dialog[2]) if int(dialog[1]) == uid else int(dialog[1])
        partner = get_user(partner_id)
        if not partner:
            await q.message.reply_text("Анкета собеседника недоступна")
            return

        text = (
            "👥 Анкета собеседника\n\n"
            f"Имя: {partner['name']}\n"
            f"Возраст: {partner['age']}\n"
            f"Город: {partner['city']}\n\n"
            f"О себе:\n{partner['about']}"
        )

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Назад к диалогу", callback_data="dialogs_home")]
        ])

        if partner.get("photo_file_id"):
            await q.message.reply_photo(
                photo=partner["photo_file_id"],
                caption=text,
                reply_markup=kb
            )
        else:
            await q.message.reply_text(
                text,
                reply_markup=kb
            )
        return


    if data == "profile:edit":
        if not can_edit_profile(context):
            await q.message.reply_text("Анкету можно редактировать только один раз в сутки.")
            return

        context.user_data["profile_edit_active"] = True

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Имя", callback_data="profile:edit:name")],
            [InlineKeyboardButton("Возраст", callback_data="profile:edit:age")],
            [InlineKeyboardButton("Город", callback_data="profile:edit:city")],
            [InlineKeyboardButton("О себе", callback_data="profile:edit:about")],
            [InlineKeyboardButton("Кого ищу", callback_data="profile:edit:looking_for_gender")],
            [InlineKeyboardButton("Мин. возраст", callback_data="profile:edit:looking_for_age_min")],
            [InlineKeyboardButton("Макс. возраст", callback_data="profile:edit:looking_for_age_max")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="dialogs_home")],
        ])
        await q.message.reply_text("Что редактируем?", reply_markup=kb)
        return


    if data.startswith("profile:edit:"):
        field = data.split(":")[2]

        if field == "looking_for_gender":
            context.user_data["profile_edit_field"] = "looking_for_gender"
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Мужчину", callback_data="looking_edit:m")],
                [InlineKeyboardButton("Женщину", callback_data="looking_edit:f")],
                [InlineKeyboardButton("Не важно", callback_data="looking_edit:any")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="profile:view")],
            ])
            await q.message.reply_text("Кого ты ищешь?", reply_markup=kb)
            return

        context.user_data["profile_edit_field"] = field
        await q.message.reply_text("Введи новое значение")
        return


    if data == "profile:photo":
        context.user_data["step"] = "profile_photo"
        await q.message.reply_text("Пришли новое фото")
        return


    if data.startswith("gender:"):
        context.user_data["gender"] = data.split(":")[1]
        context.user_data["step"] = "looking_for_gender"
        await q.message.reply_text(
            "Кого ты ищешь?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Мужчину", callback_data="looking:m")],
                [InlineKeyboardButton("Женщину", callback_data="looking:f")],
                [InlineKeyboardButton("Не важно", callback_data="looking:any")],
            ])
        )
        return


    if data.startswith("looking:"):
        context.user_data["looking_for_gender"] = data.split(":")[1]
        context.user_data["step"] = "looking_age_min"
        await q.message.reply_text("Минимальный возраст?")
        return


    if data == "skip_photo":
        create_user(context.user_data | {"user_id": uid, "username": q.from_user.username or ""})
        context.user_data.clear()
        await q.message.reply_text("Анкета готова")
        return


    if data.startswith("looking_edit:"):
        value = data.split(":")[1]
        update_user_field(uid, "J", value)

        context.user_data["last_profile_edit_at"] = datetime.now(timezone.utc)
        context.user_data.pop("profile_edit_active", None)
        context.user_data.pop("profile_edit_field", None)

        user = get_user(uid)
        t, kb = render_profile(user)
        await q.message.reply_text("Обновлено\n\n" + t, reply_markup=kb)
        return


    if data.startswith("dialog_slot:"):
        slot = int(data.split(":")[1])
        dialogs = get_user_dialogs(uid)
        dialog_id = dialogs[slot][0] if slot < len(dialogs) else create_dialog(uid)

        if not dialog_id:
            await q.message.reply_text("Нет подходящих собеседников")
            return

        context.user_data["active_dialog_id"] = dialog_id
        await q.message.reply_text(
            f"Диалог {slot+1}",
            reply_markup=dialogs_keyboard(uid, dialog_id)
        )
        return


    if data == "dialogs_home":
        context.user_data.pop("active_dialog_id", None)
        await q.message.reply_text(
            "Диалоги",
            reply_markup=dialogs_keyboard(uid, None)
        )
        return


    if data.startswith("dialog_close:"):
        close_dialog(data.split(":")[1])
        context.user_data.pop("active_dialog_id", None)
        await q.message.reply_text(
            "Диалог закрыт",
            reply_markup=dialogs_keyboard(uid, None)
        )
        return

# =========================
# MAIN
# =========================
def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(callback))
    app.add_handler(MessageHandler(filters.TEXT | filters.PHOTO, message))

    log.info("LUMEN STARTED")
    app.run_polling()


if __name__ == "__main__":
    main()
