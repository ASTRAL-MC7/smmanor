"""
Nakrutka (SMM) reseller bot — Kometa SMM API (kometa-smm.uz/api/v2)
To'lov: Telegram Stars -> ichki balans (so'm). Render web service (webhook).
"""
import asyncio
import hashlib
import html
import json
import logging
import os
import re
import threading
import time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

import aiohttp
from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, LabeledPrice, Message,
                           PreCheckoutQuery)
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

# ===================== SOZLAMALAR =====================
BOT_TOKEN = os.environ["BOT_TOKEN"]
API_KEY = os.environ.get("SMM_API_KEY", "")
PROVIDER = "kometa"  # buyurtmalar qaysi panelga tegishli
API_URL = os.environ.get("SMM_API_URL", "https://kometa-smm.uz/api/v2")

ADMIN_ID = 5523761749
ADMIN_USERNAME = "neindev"

STAR_RATE = 160        # 1 ⭐ = 160 so'm (100 ⭐ = 16 000 so'm)
MARKUP_PER_1000 = 5000  # har 1000 taga +5000 so'm
MIN_STARS = 1   # TEST uchun 1. Ishga tushirishda 75 qiling.

WEBHOOK_BASE = os.environ.get("WEBHOOK_URL") or os.environ.get("RENDER_EXTERNAL_URL")
WEBHOOK_PATH = "/webhook"
WEBHOOK_SECRET = hashlib.sha256(BOT_TOKEN.encode()).hexdigest()[:48]
PORT = int(os.environ.get("PORT", 10000))

DATABASE_URL = os.environ.get("DATABASE_URL")  # bo'lsa Postgres, bo'lmasa SQLite
DB_PATH = os.environ.get("DB_PATH", "bot.db")

SERVICES_TTL = 600     # xizmatlar keshini 10 daqiqada yangilash
POLL_INTERVAL = 120    # buyurtma holatlarini 2 daqiqada tekshirish
PAGE_SIZE = 8

FINAL_STATUSES = {"Completed", "Partial", "Canceled", "Failed", "Unknown"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("bot")


# ===================== YORDAMCHI =====================
def fmt(n) -> str:
    return f"{int(n):,}".replace(",", " ")


def esc(s) -> str:
    return html.escape(str(s or ""))


_TAG_RE = re.compile(r"<[^>]+>")


def clean(s) -> str:
    """API matnidagi HTML teglarini (<tg-emoji ...>👤</tg-emoji> va h.k.) olib tashlaydi."""
    return html.unescape(_TAG_RE.sub("", str(s or ""))).strip()


def now() -> int:
    return int(time.time())


def norm_status(s: str) -> str:
    s = str(s or "").strip()
    low = s.lower()
    if any(w in low for w in ("cancel", "bekor", "отмен", "refund", "qaytar")):
        return "Canceled"
    if any(w in low for w in ("partial", "qisman", "частич")):
        return "Partial"
    if any(w in low for w in ("complet", "bajarildi", "tugad", "выполнен", "success")):
        return "Completed"
    if any(w in low for w in ("progress", "bajarilmoqda", "jarayon", "выполня")):
        return "In progress"
    if "process" in low:
        return "Processing"
    if any(w in low for w in ("pending", "kutil", "ожида")):
        return "Pending"
    return s or "Pending"


STATUS_UZ = {
    "Pending": "⏳ Kutilmoqda",
    "Processing": "⚙️ Jarayonda",
    "In progress": "🚀 Bajarilmoqda",
    "Completed": "✅ Bajarildi",
    "Partial": "⚠️ Qisman bajarildi",
    "Canceled": "❌ Bekor qilindi",
    "Failed": "❌ Xatolik",
    "Unknown": "❓ Noma'lum (admin tekshiradi)",
}


def status_uz(s: str) -> str:
    return STATUS_UZ.get(s, esc(s))


# ===================== BAZA =====================
class DB:
    def __init__(self):
        self.pg = bool(DATABASE_URL)
        self.lock = threading.Lock()
        self._connect()
        self._init()

    def _connect(self):
        if self.pg:
            import psycopg2
            self.conn = psycopg2.connect(DATABASE_URL)
            self.conn.autocommit = True
        else:
            import sqlite3
            self.conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
            self.conn.execute("PRAGMA journal_mode=WAL")

    def _q(self, sql: str) -> str:
        return sql.replace("?", "%s") if self.pg else sql

    def _run(self, sql, params=(), fetch=None):
        with self.lock:
            for attempt in (1, 2):
                try:
                    cur = self.conn.cursor()
                    cur.execute(self._q(sql), params)
                    if fetch == "one":
                        return cur.fetchone()
                    if fetch == "all":
                        return cur.fetchall()
                    return cur.rowcount
                except Exception as e:
                    if self.pg and attempt == 1 and e.__class__.__name__ in ("OperationalError", "InterfaceError"):
                        log.warning("DB qayta ulanmoqda: %s", e)
                        self._connect()
                        continue
                    raise

    def exec(self, sql, params=()):
        return self._run(sql, params)

    def one(self, sql, params=()):
        return self._run(sql, params, "one")

    def all(self, sql, params=()):
        return self._run(sql, params, "all")

    def _init(self):
        serial = "BIGSERIAL PRIMARY KEY" if self.pg else "INTEGER PRIMARY KEY AUTOINCREMENT"
        self.exec("""CREATE TABLE IF NOT EXISTS users(
            id BIGINT PRIMARY KEY, username TEXT, first_name TEXT,
            balance BIGINT NOT NULL DEFAULT 0, created_at BIGINT)""")
        self.exec(f"""CREATE TABLE IF NOT EXISTS orders(
            id {serial}, user_id BIGINT, api_order BIGINT, service_id BIGINT,
            service_name TEXT, link TEXT, quantity BIGINT, charge BIGINT,
            status TEXT, start_count TEXT, remains TEXT,
            refunded BIGINT NOT NULL DEFAULT 0, created_at BIGINT)""")
        self.exec("""CREATE TABLE IF NOT EXISTS payments(
            charge_id TEXT PRIMARY KEY, user_id BIGINT, stars BIGINT,
            amount BIGINT, created_at BIGINT)""")
        self.exec("CREATE TABLE IF NOT EXISTS hidden_kometa(service_id BIGINT PRIMARY KEY)")
        try:
            self.exec("ALTER TABLE orders ADD COLUMN provider TEXT")
        except Exception:
            pass  # ustun allaqachon bor

    # --- users ---
    def ensure_user(self, uid, username, first_name):
        self.exec("""INSERT INTO users(id, username, first_name, balance, created_at)
                     VALUES(?,?,?,0,?) ON CONFLICT(id) DO UPDATE SET
                     username=excluded.username, first_name=excluded.first_name""",
                  (uid, username, first_name, now()))

    def balance(self, uid) -> int:
        r = self.one("SELECT balance FROM users WHERE id=?", (uid,))
        return int(r[0]) if r else 0

    def add_balance(self, uid, amount):
        self.exec("""INSERT INTO users(id, balance, created_at) VALUES(?,?,?)
                     ON CONFLICT(id) DO UPDATE SET balance = users.balance + excluded.balance""",
                  (uid, amount, now()))

    def deduct(self, uid, amount) -> bool:
        return self.exec("UPDATE users SET balance = balance - ? WHERE id=? AND balance >= ?",
                         (amount, uid, amount)) == 1

    def user_count(self) -> int:
        return int(self.one("SELECT COUNT(*) FROM users")[0])

    def all_user_ids(self):
        return [r[0] for r in self.all("SELECT id FROM users")]

    # --- payments ---
    def add_payment(self, charge_id, uid, stars, amount) -> bool:
        return self.exec("""INSERT INTO payments(charge_id, user_id, stars, amount, created_at)
                            VALUES(?,?,?,?,?) ON CONFLICT(charge_id) DO NOTHING""",
                         (charge_id, uid, stars, amount, now())) == 1

    # --- orders ---
    ORDER_COLS = ("id, user_id, api_order, service_id, service_name, link, quantity, charge, "
                  "status, start_count, remains, refunded, created_at, provider")

    def _order_dict(self, r):
        if not r:
            return None
        keys = [k.strip() for k in self.ORDER_COLS.split(",")]
        return dict(zip(keys, r))

    def create_order(self, uid, sid, name, link, qty, charge) -> int:
        r = self.one("""INSERT INTO orders(user_id, service_id, service_name, link, quantity,
                        charge, status, refunded, created_at, provider)
                        VALUES(?,?,?,?,?,?,'Pending',0,?,?) RETURNING id""",
                     (uid, sid, name, link, qty, charge, now(), PROVIDER))
        return int(r[0])

    def update_order(self, oid, **fields):
        if not fields:
            return
        sets = ", ".join(f"{k}=?" for k in fields)
        self.exec(f"UPDATE orders SET {sets} WHERE id=?", (*fields.values(), oid))

    def get_order(self, oid):
        return self._order_dict(self.one(f"SELECT {self.ORDER_COLS} FROM orders WHERE id=?", (oid,)))

    def user_orders(self, uid, limit, offset):
        rows = self.all(f"SELECT {self.ORDER_COLS} FROM orders WHERE user_id=? ORDER BY id DESC LIMIT ? OFFSET ?",
                        (uid, limit, offset))
        return [self._order_dict(r) for r in rows]

    def user_order_count(self, uid) -> int:
        return int(self.one("SELECT COUNT(*) FROM orders WHERE user_id=?", (uid,))[0])

    def last_orders(self, limit=20):
        rows = self.all(f"SELECT {self.ORDER_COLS} FROM orders ORDER BY id DESC LIMIT ?", (limit,))
        return [self._order_dict(r) for r in rows]

    def active_orders(self, limit=100):
        rows = self.all(f"""SELECT {self.ORDER_COLS} FROM orders
                            WHERE api_order IS NOT NULL AND provider=? AND status NOT IN
                            ('Completed','Partial','Canceled','Failed','Unknown')
                            ORDER BY id ASC LIMIT ?""", (PROVIDER, limit))
        return [self._order_dict(r) for r in rows]

    def add_refund(self, oid, target_total) -> int:
        """Buyurtma bo'yicha jami qaytariladigan summani target_total ga yetkazadi. Qaytarilgan farqni beradi."""
        o = self.get_order(oid)
        if not o:
            return 0
        diff = int(target_total) - int(o["refunded"] or 0)
        if diff <= 0:
            return 0
        if self.exec("UPDATE orders SET refunded=? WHERE id=? AND refunded=?",
                     (int(target_total), oid, int(o["refunded"] or 0))) == 1:
            self.add_balance(o["user_id"], diff)
            return diff
        return 0

    # --- hidden ---
    def hidden_ids(self) -> set:
        return {int(r[0]) for r in self.all("SELECT service_id FROM hidden_kometa")}

    def toggle_hidden(self, sid) -> bool:
        if self.one("SELECT 1 FROM hidden_kometa WHERE service_id=?", (sid,)):
            self.exec("DELETE FROM hidden_kometa WHERE service_id=?", (sid,))
            return False
        self.exec("INSERT INTO hidden_kometa(service_id) VALUES(?) ON CONFLICT DO NOTHING", (sid,))
        return True


db: DB = None  # startup'da yaratiladi


# ===================== API =====================
class ApiError(Exception):
    """API aniq xato qaytardi (buyurtma yaratilmagan)."""


class ApiNetError(Exception):
    """Tarmoq/timeout/noto'g'ri javob — natija noma'lum."""


http: aiohttp.ClientSession = None


async def api(action: str, **params):
    data = {"key": API_KEY, "action": action}
    data.update({k: str(v) for k, v in params.items() if v is not None and v != ""})
    try:
        async with http.post(API_URL, data=data, timeout=aiohttp.ClientTimeout(total=40)) as r:
            text = await r.text()
    except Exception as e:
        raise ApiNetError(str(e) or e.__class__.__name__)
    try:
        res = json.loads(text)
    except Exception:
        raise ApiNetError(f"JSON emas: {text[:200]}")
    if isinstance(res, dict) and res.get("error"):
        raise ApiError(str(res["error"]))
    return res


# ===================== XIZMATLAR KESHI =====================
PLATFORM_NAMES = {
    "telegram": "✈️ Telegram",
    "instagram": "📸 Instagram",
    "tiktok": "🎵 TikTok",
    "youtube": "▶️ YouTube",
    "boshqa": "📦 Boshqa",
}
PLATFORM_ORDER = ["telegram", "instagram", "tiktok", "youtube", "boshqa"]

SERVICES: dict = {}     # id -> service dict
CATEGORIES: dict = {}   # key -> {"name", "platform", "services": [ids]}
_services_loaded_at = 0
_services_lock = asyncio.Lock()


def detect_platform(text: str) -> str:
    t = (text or "").lower()
    if "telegram" in t or "телеграм" in t:
        return "telegram"
    if "instagram" in t or "insta" in t or "инстаграм" in t:
        return "instagram"
    if "tiktok" in t or "tik tok" in t or "тикток" in t:
        return "tiktok"
    if "youtube" in t or "ютуб" in t:
        return "youtube"
    return "boshqa"


def cat_key(name: str) -> str:
    return hashlib.md5(name.encode()).hexdigest()[:10]


async def load_services(force=False):
    global _services_loaded_at
    if not force and SERVICES and now() - _services_loaded_at < SERVICES_TTL:
        return
    async with _services_lock:
        if not force and SERVICES and now() - _services_loaded_at < SERVICES_TTL:
            return
        try:
            res = await api("services")
        except (ApiError, ApiNetError) as e:
            log.error("Xizmatlarni yuklab bo'lmadi: %s", e)
            return
        if not isinstance(res, list):
            log.error("Xizmatlar ro'yxati noto'g'ri: %s", str(res)[:200])
            return
        services, cats = {}, {}
        for s in res:
            try:
                sid = int(s["service"])
            except Exception:
                continue
            s["service"] = sid
            s["name"] = clean(s.get("name")) or f"Xizmat {sid}"
            s["category"] = clean(s.get("category")) or "Boshqa"
            if s.get("description"):
                s["description"] = clean(s["description"])
            cname = s["category"]
            plat = detect_platform(cname)
            if plat == "boshqa":
                plat = detect_platform(s.get("name", ""))
            k = cat_key(cname)
            cats.setdefault(k, {"name": cname, "platform": plat, "services": []})["services"].append(sid)
            services[sid] = s
        SERVICES.clear()
        SERVICES.update(services)
        CATEGORIES.clear()
        CATEGORIES.update(cats)
        _services_loaded_at = now()
        log.info("Xizmatlar yuklandi: %d ta, %d kategoriya", len(services), len(cats))


def visible_services(ids, hidden):
    return [sid for sid in ids if sid in SERVICES and sid not in hidden]


def svc_type(s) -> str:
    return str(s.get("type") or "Default").lower()


def is_package(s) -> bool:
    return "package" in svc_type(s) and "comment" not in svc_type(s)


def needs_lines(s) -> bool:
    t = svc_type(s)
    return "comment" in t or "custom list" in t or "usernames" in t


def has_dripfeed(s) -> bool:
    return bool(s.get("dripfeed")) and not needs_lines(s) and not is_package(s)


def rate_with_markup(s) -> Decimal:
    return Decimal(str(s.get("rate") or "0")) + MARKUP_PER_1000


def calc_price(s, total_qty: int) -> int:
    rate = rate_with_markup(s)
    if is_package(s):
        return int(rate.to_integral_value(rounding=ROUND_CEILING))
    return int((rate * total_qty / 1000).to_integral_value(rounding=ROUND_CEILING))


# ===================== KLAVIATURALAR =====================
def kb(rows) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=rows)


def btn(text, data=None, url=None) -> InlineKeyboardButton:
    if url:
        return InlineKeyboardButton(text=text, url=url)
    return InlineKeyboardButton(text=text, callback_data=data)


def main_kb():
    return kb([
        [btn("🛍 Xizmatlar", "services")],
        [btn("💰 Balans to'ldirish", "topup"), btn("👤 Hisobim", "account")],
        [btn("📦 Buyurtmalarim", "myorders:0")],
        [btn("☎️ Admin", url=f"https://t.me/{ADMIN_USERNAME}")],
    ])


def back_main():
    return [btn("🏠 Bosh menyu", "menu")]


def cancel_kb():
    return kb([[btn("❌ Bekor qilish", "cancel_flow")]])


def pager(prefix: str, page: int, total: int):
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    row = []
    if page > 0:
        row.append(btn("⬅️", f"{prefix}:{page - 1}"))
    if pages > 1:
        row.append(btn(f"{page + 1}/{pages}", "noop"))
    if page < pages - 1:
        row.append(btn("➡️", f"{prefix}:{page + 1}"))
    return row


async def safe_edit(cb: CallbackQuery, text: str, markup=None):
    try:
        await cb.message.edit_text(text, reply_markup=markup, disable_web_page_preview=True)
    except Exception as e:
        if "message is not modified" in str(e):
            return
        await cb.message.answer(text, reply_markup=markup, disable_web_page_preview=True)


# ===================== FSM =====================
class OrderSG(StatesGroup):
    link = State()
    quantity = State()
    comments = State()
    drip_choice = State()
    runs = State()
    interval = State()
    confirm = State()


class TopupSG(StatesGroup):
    amount = State()


router = Router()


def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID


async def notify_admin(bot: Bot, text: str):
    try:
        await bot.send_message(ADMIN_ID, text, disable_web_page_preview=True)
    except Exception as e:
        log.error("Adminga yuborib bo'lmadi: %s", e)


def welcome_text(uid: int) -> str:
    return (f"👋 <b>Assalomu alaykum!</b>\n\n"
            f"Telegram, Instagram, TikTok va boshqa tarmoqlar uchun nakrutka xizmatlari.\n\n"
            f"💳 Balansingiz: <b>{fmt(db.balance(uid))} so'm</b>")


# ===================== ADMIN BUYRUQLARI =====================
@router.message(Command("orders"))
async def cmd_orders(m: Message):
    if not is_admin(m.from_user.id):
        return
    rows = db.last_orders(20)
    if not rows:
        return await m.answer("Buyurtmalar yo'q.")
    lines = ["📋 <b>Oxirgi 20 ta buyurtma:</b>\n"]
    for o in rows:
        lines.append(f"#{o['id']} (API: {o['api_order'] or '—'}) · 👤 <code>{o['user_id']}</code>\n"
                     f"   {esc((o['service_name'] or '')[:45])} · {fmt(o['quantity'])} ta · "
                     f"{fmt(o['charge'])} so'm · {status_uz(o['status'])}")
    await m.answer("\n".join(lines), disable_web_page_preview=True)


@router.message(Command("check"))
async def cmd_check(m: Message, command: CommandObject, bot: Bot):
    """/check <bot buyurtma #> — bazadagi holat + panelning xom javobi; holatni qo'llaydi."""
    if not is_admin(m.from_user.id):
        return
    arg = (command.args or "").strip().lstrip("#")
    if not arg.isdigit():
        return await m.answer("Foydalanish: <code>/check &lt;bot buyurtma raqami&gt;</code>\nMasalan: <code>/check 3</code>")
    o = db.get_order(int(arg))
    if not o:
        return await m.answer("Bunday buyurtma bazada yo'q.")
    text = (f"🧾 Bot #{o['id']} · Panel ID: <code>{o['api_order'] or '—'}</code>\n"
            f"👤 <code>{o['user_id']}</code> · {fmt(o['charge'])} so'm\n"
            f"📊 Bazadagi holat: <code>{esc(o['status'])}</code> · qaytarilgan: {fmt(o['refunded'] or 0)}")
    if not o["api_order"]:
        text += "\n\n❗️ Panel ID yo'q — buyurtma API'ga yetib bormagan yoki javob kelmagan."
        return await m.answer(text)
    if o.get("provider") != PROVIDER:
        text += "\n\nℹ️ Bu eski (Anor) buyurtmasi — Kometa'da tekshirib bo'lmaydi."
        return await m.answer(text)
    try:
        st = await api("status", order=o["api_order"])
        text += f"\n\n📡 Panel javobi:\n<code>{esc(json.dumps(st, ensure_ascii=False)[:1500])}</code>"
        if isinstance(st, dict) and st.get("status"):
            await apply_status(bot, o, st)
            o = db.get_order(o["id"])
            text += (f"\n\n✅ Yangilandi: <code>{esc(o['status'])}</code> · "
                     f"qaytarilgan: {fmt(o['refunded'] or 0)} so'm")
    except Exception as e:
        text += f"\n\n❗️ API xato: <code>{esc(e)}</code>"
    await m.answer(text)


@router.message(Command("usercount"))
async def cmd_usercount(m: Message):
    if not is_admin(m.from_user.id):
        return
    text = f"👥 Foydalanuvchilar soni: <b>{fmt(db.user_count())}</b>"
    try:
        b = await api("balance")
        text += f"\n💼 API balans: <b>{esc(b.get('balance'))} {esc(b.get('currency', 'UZS'))}</b>"
    except Exception as e:
        text += f"\n💼 API balans: olinmadi ({esc(e)})"
    await m.answer(text)


def products_kb(page: int):
    hidden = db.hidden_ids()
    ids = sorted(SERVICES)
    chunk = ids[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]
    rows = []
    for sid in chunk:
        s = SERVICES[sid]
        mark = "🚫" if sid in hidden else "✅"
        rows.append([btn(f"{mark} {sid} · {str(s.get('name', ''))[:38]}", f"ptog:{sid}:{page}")])
    nav = pager("ppage", page, len(ids))
    if nav:
        rows.append(nav)
    return kb(rows), len(ids), len(hidden)


@router.message(Command("products"))
async def cmd_products(m: Message):
    if not is_admin(m.from_user.id):
        return
    await load_services(force=True)
    if not SERVICES:
        return await m.answer("Xizmatlarni API'dan olib bo'lmadi.")
    markup, total, hid = products_kb(0)
    await m.answer(f"🛠 <b>Mahsulotlar</b> — jami {total}, yashirin {hid}.\n"
                   f"✅ ko'rinadi · 🚫 yashirin. Bosib almashtiring.", reply_markup=markup)


@router.callback_query(F.data.startswith("ppage:"))
async def cb_products_page(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await cb.answer()
    page = int(cb.data.split(":")[1])
    markup, total, hid = products_kb(page)
    await cb.answer()
    await safe_edit(cb, f"🛠 <b>Mahsulotlar</b> — jami {total}, yashirin {hid}.\n"
                        f"✅ ko'rinadi · 🚫 yashirin. Bosib almashtiring.", markup)


@router.callback_query(F.data.startswith("ptog:"))
async def cb_products_toggle(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await cb.answer()
    _, sid, page = cb.data.split(":")
    hidden_now = db.toggle_hidden(int(sid))
    await cb.answer("🚫 Yashirildi" if hidden_now else "✅ Ko'rinadi")
    markup, total, hid = products_kb(int(page))
    await safe_edit(cb, f"🛠 <b>Mahsulotlar</b> — jami {total}, yashirin {hid}.\n"
                        f"✅ ko'rinadi · 🚫 yashirin. Bosib almashtiring.", markup)


@router.message(Command("add"))
async def cmd_add(m: Message, command: CommandObject, bot: Bot):
    if not is_admin(m.from_user.id):
        return
    parts = (command.args or "").split()
    if len(parts) != 2:
        return await m.answer("Foydalanish: <code>/add &lt;user_id&gt; &lt;summa&gt;</code>\nMasalan: <code>/add 123456789 10000</code>")
    try:
        uid, amount = int(parts[0]), int(parts[1].replace("_", ""))
    except ValueError:
        return await m.answer("❗️ user_id va summa butun son bo'lishi kerak.")
    db.add_balance(uid, amount)
    await m.answer(f"✅ <code>{uid}</code> balansiga {fmt(amount)} so'm qo'shildi.\n"
                   f"Yangi balans: <b>{fmt(db.balance(uid))} so'm</b>")
    if amount > 0:
        try:
            await bot.send_message(uid, f"💰 Balansingizga <b>{fmt(amount)} so'm</b> qo'shildi.\n"
                                        f"Balans: <b>{fmt(db.balance(uid))} so'm</b>")
        except Exception:
            pass


async def run_broadcast(bot: Bot, src: Message, text: str | None):
    ids = db.all_user_ids()
    ok = fail = 0
    for uid in ids:
        try:
            if text is not None:
                await bot.send_message(uid, text, disable_web_page_preview=True)
            else:
                await bot.copy_message(uid, src.chat.id, src.message_id)
            ok += 1
        except Exception as e:
            fail += 1
            if "retry after" in str(e).lower():
                await asyncio.sleep(5)
        await asyncio.sleep(0.05)
    await notify_admin(bot, f"📣 Xabar yuborildi.\n✅ {ok} ta · ❌ {fail} ta")


@router.message(Command("broadcast"))
async def cmd_broadcast(m: Message, command: CommandObject, bot: Bot):
    if not is_admin(m.from_user.id):
        return
    if m.reply_to_message:
        asyncio.create_task(run_broadcast(bot, m.reply_to_message, None))
    elif command.args:
        asyncio.create_task(run_broadcast(bot, m, command.args))
    else:
        return await m.answer("Foydalanish:\n• <code>/broadcast matn</code>\n"
                              "• yoki istalgan xabarga (rasm, video...) reply qilib <code>/broadcast</code>")
    await m.answer(f"📣 Yuborish boshlandi ({fmt(db.user_count())} ta foydalanuvchi)...")


# ===================== FOYDALANUVCHI =====================
@router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    db.ensure_user(m.from_user.id, m.from_user.username, m.from_user.first_name)
    await m.answer(welcome_text(m.from_user.id), reply_markup=main_kb())


@router.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery):
    await cb.answer()


@router.callback_query(F.data == "menu")
async def cb_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    db.ensure_user(cb.from_user.id, cb.from_user.username, cb.from_user.first_name)
    await cb.answer()
    await safe_edit(cb, welcome_text(cb.from_user.id), main_kb())


@router.callback_query(F.data == "cancel_flow")
async def cb_cancel_flow(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.answer("Bekor qilindi")
    await safe_edit(cb, welcome_text(cb.from_user.id), main_kb())


@router.callback_query(F.data == "account")
async def cb_account(cb: CallbackQuery):
    uid = cb.from_user.id
    db.ensure_user(uid, cb.from_user.username, cb.from_user.first_name)
    await cb.answer()
    await safe_edit(cb,
                    f"👤 <b>Hisobim</b>\n\n🆔 ID: <code>{uid}</code>\n"
                    f"💳 Balans: <b>{fmt(db.balance(uid))} so'm</b>\n"
                    f"📦 Buyurtmalar: {fmt(db.user_order_count(uid))} ta",
                    kb([[btn("💰 Balans to'ldirish", "topup")], back_main()]))


# ---------- Balans to'ldirish (Stars) ----------
@router.callback_query(F.data == "topup")
async def cb_topup(cb: CallbackQuery, state: FSMContext):
    await state.set_state(TopupSG.amount)
    await cb.answer()
    await safe_edit(cb,
                    f"💰 <b>Balans to'ldirish</b>\n\n"
                    f"⭐ Kurs: <b>100 ⭐ = {fmt(100 * STAR_RATE)} so'm</b>\n"
                    f"📉 Minimal: <b>{MIN_STARS} ⭐</b> ({fmt(MIN_STARS * STAR_RATE)} so'm)\n\n"
                    f"Nechta ⭐ bilan to'ldirasiz? Sonini yozing:",
                    cancel_kb())


@router.message(TopupSG.amount)
async def msg_topup_amount(m: Message, state: FSMContext, bot: Bot):
    txt = (m.text or "").strip().replace(" ", "")
    if not txt.isdigit():
        return await m.answer("❗️ Faqat son yozing. Masalan: <code>100</code>", reply_markup=cancel_kb())
    stars = int(txt)
    if stars < MIN_STARS:
        return await m.answer(f"❗️ Minimal miqdor {MIN_STARS} ⭐.", reply_markup=cancel_kb())
    if stars > 100000:
        return await m.answer("❗️ Maksimal miqdor 100 000 ⭐.", reply_markup=cancel_kb())
    await state.clear()
    amount = stars * STAR_RATE
    await bot.send_invoice(
        chat_id=m.chat.id,
        title="Balans to'ldirish",
        description=f"{fmt(stars)} ⭐ = {fmt(amount)} so'm balansga qo'shiladi",
        payload=f"topup:{m.from_user.id}:{stars}",
        currency="XTR",
        prices=[LabeledPrice(label=f"{stars} ⭐", amount=stars)],
    )


@router.pre_checkout_query()
async def pre_checkout(q: PreCheckoutQuery):
    try:
        kind, uid, stars = q.invoice_payload.split(":")
        valid = (kind == "topup" and int(uid) == q.from_user.id and int(stars) == q.total_amount
                 and int(stars) >= MIN_STARS and q.currency == "XTR")
    except Exception:
        valid = False
    if valid:
        await q.answer(ok=True)
    else:
        await q.answer(ok=False, error_message="To'lov ma'lumotlari noto'g'ri. Qaytadan urinib ko'ring.")


@router.message(F.successful_payment)
async def on_paid(m: Message, bot: Bot):
    p = m.successful_payment
    uid = m.from_user.id
    stars = int(p.total_amount)
    amount = stars * STAR_RATE
    db.ensure_user(uid, m.from_user.username, m.from_user.first_name)
    if db.add_payment(p.telegram_payment_charge_id, uid, stars, amount):
        db.add_balance(uid, amount)
        await m.answer(f"✅ To'lov qabul qilindi!\n\n⭐ {fmt(stars)} → <b>{fmt(amount)} so'm</b>\n"
                       f"💳 Balans: <b>{fmt(db.balance(uid))} so'm</b>",
                       reply_markup=kb([[btn("🛍 Xizmatlar", "services")], back_main()]))
        uname = f"@{m.from_user.username}" if m.from_user.username else "—"
        await notify_admin(bot, f"💰 Yangi to'lov: {fmt(stars)} ⭐ ({fmt(amount)} so'm)\n"
                                f"👤 <code>{uid}</code> {esc(uname)}")


# ---------- Xizmatlar ----------
@router.callback_query(F.data == "services")
async def cb_services(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.answer()
    await load_services()
    if not SERVICES:
        return await safe_edit(cb, "⚠️ Xizmatlar vaqtincha mavjud emas. Keyinroq urinib ko'ring.",
                               kb([back_main()]))
    hidden = db.hidden_ids()
    present = set()
    for c in CATEGORIES.values():
        if visible_services(c["services"], hidden):
            present.add(c["platform"])
    rows, row = [], []
    for p in PLATFORM_ORDER:
        if p in present:
            row.append(btn(PLATFORM_NAMES[p], f"plat:{p}:0"))
            if len(row) == 2:
                rows.append(row)
                row = []
    if row:
        rows.append(row)
    rows.append(back_main())
    await safe_edit(cb, "🛍 <b>Tarmoqni tanlang:</b>", kb(rows))


@router.callback_query(F.data.startswith("plat:"))
async def cb_platform(cb: CallbackQuery):
    _, plat, page = cb.data.split(":")
    page = int(page)
    await cb.answer()
    await load_services()
    hidden = db.hidden_ids()
    cats = [(k, c) for k, c in CATEGORIES.items()
            if c["platform"] == plat and visible_services(c["services"], hidden)]
    if not cats:
        return await safe_edit(cb, "Bu bo'limda xizmat yo'q.", kb([[btn("⬅️ Orqaga", "services")]]))
    chunk = cats[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]
    rows = [[btn(c["name"][:60], f"cat:{k}:0")] for k, c in chunk]
    nav = pager(f"plat:{plat}", page, len(cats))
    if nav:
        rows.append(nav)
    rows.append([btn("⬅️ Orqaga", "services")])
    await safe_edit(cb, f"{PLATFORM_NAMES.get(plat, plat)} — <b>bo'limni tanlang:</b>", kb(rows))


@router.callback_query(F.data.startswith("cat:"))
async def cb_category(cb: CallbackQuery):
    _, key, page = cb.data.split(":")
    page = int(page)
    await cb.answer()
    await load_services()
    c = CATEGORIES.get(key)
    if not c:
        return await safe_edit(cb, "⚠️ Bo'lim yangilandi, qaytadan tanlang.", kb([[btn("🛍 Xizmatlar", "services")]]))
    ids = visible_services(c["services"], db.hidden_ids())
    chunk = ids[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]
    rows = []
    for sid in chunk:
        s = SERVICES[sid]
        price = int(rate_with_markup(s).to_integral_value(rounding=ROUND_CEILING))
        label = f"{str(s.get('name', ''))[:40]} — {fmt(price)}"
        rows.append([btn(label, f"svc:{sid}")])
    nav = pager(f"cat:{key}", page, len(ids))
    if nav:
        rows.append(nav)
    rows.append([btn("⬅️ Orqaga", f"plat:{c['platform']}:0")])
    await safe_edit(cb, f"📂 <b>{esc(c['name'])}</b>\n\nNarxlar 1000 ta uchun (so'm):", kb(rows))


def service_card(s) -> str:
    price = int(rate_with_markup(s).to_integral_value(rounding=ROUND_CEILING))
    lines = [f"🔹 <b>{esc(s.get('name'))}</b>", f"🆔 ID: {s['service']}"]
    if is_package(s):
        lines.append(f"💰 Narx: <b>{fmt(price)} so'm</b> (paket)")
    else:
        lines.append(f"💰 Narx: <b>{fmt(price)} so'm</b> / 1000 ta")
        lines.append(f"📉 Min: {fmt(s.get('min', 0))} · 📈 Max: {fmt(s.get('max', 0))}")
    lines.append(f"♻️ Qayta tiklash: {'bor' if s.get('refill') else 'yoʻq'}")
    if s.get("description"):
        lines.append(f"\nℹ️ {esc(str(s['description'])[:700])}")
    return "\n".join(lines)


@router.callback_query(F.data.startswith("svc:"))
async def cb_service(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    sid = int(cb.data.split(":")[1])
    await cb.answer()
    await load_services()
    s = SERVICES.get(sid)
    if not s or sid in db.hidden_ids():
        return await safe_edit(cb, "⚠️ Bu xizmat hozir mavjud emas.", kb([[btn("🛍 Xizmatlar", "services")]]))
    back = cat_key(str(s.get("category") or "Boshqa").strip())
    await safe_edit(cb, service_card(s), kb([
        [btn("🛒 Buyurtma berish", f"order:{sid}")],
        [btn("⬅️ Orqaga", f"cat:{back}:0")],
    ]))


@router.callback_query(F.data.startswith("order:"))
async def cb_order_start(cb: CallbackQuery, state: FSMContext):
    sid = int(cb.data.split(":")[1])
    s = SERVICES.get(sid)
    await cb.answer()
    if not s or sid in db.hidden_ids():
        return await safe_edit(cb, "⚠️ Bu xizmat hozir mavjud emas.", kb([[btn("🛍 Xizmatlar", "services")]]))
    await state.clear()
    await state.set_state(OrderSG.link)
    await state.update_data(sid=sid)
    await safe_edit(cb, f"🔹 <b>{esc(s.get('name'))}</b>\n\n🔗 Havola yoki username yuboring:", cancel_kb())


@router.message(OrderSG.link)
async def msg_link(m: Message, state: FSMContext):
    data = await state.get_data()
    s = SERVICES.get(data.get("sid"))
    if not s:
        await state.clear()
        return await m.answer("⚠️ Xizmat topilmadi, qaytadan tanlang.", reply_markup=main_kb())
    link = normalize_link(m.text, s)
    if not link:
        return await m.answer("❗️ To'g'ri havola yuboring. Masalan:\n"
                              "<code>https://t.me/kanal/123</code> yoki <code>@kanal</code>",
                              reply_markup=cancel_kb())
    await state.update_data(link=link)
    if needs_lines(s):
        await state.set_state(OrderSG.comments)
        return await m.answer(f"💬 Izohlarni yuboring — <b>har birini yangi qatordan</b>.\n"
                              f"Soni: {fmt(s.get('min', 1))} – {fmt(s.get('max', 0))} ta",
                              reply_markup=cancel_kb())
    if is_package(s):
        await state.update_data(qty=1, total=1)
        return await show_confirm(m, state)
    await state.set_state(OrderSG.quantity)
    await m.answer(f"🔢 Miqdorni kiriting:\n📉 Min: {fmt(s.get('min', 0))} · 📈 Max: {fmt(s.get('max', 0))}",
                   reply_markup=cancel_kb())


_LINK_RE = re.compile(r"^https?://[^\s/]+\.[^\s]+$", re.I)


def normalize_link(text, s) -> str | None:
    """Kometa faqat to'liq https:// havola qabul qiladi — username va qisqa havolalarni to'g'irlaydi."""
    link = (text or "").strip()
    if not link or len(link) > 480 or any(c.isspace() for c in link):
        return None
    if link.startswith("@") and len(link) > 1:
        user = link[1:]
        plat = detect_platform(f"{s.get('category', '')} {s.get('name', '')}")
        if plat == "instagram":
            link = f"https://instagram.com/{user}"
        elif plat == "tiktok":
            link = f"https://www.tiktok.com/@{user}"
        elif plat == "youtube":
            link = f"https://www.youtube.com/@{user}"
        else:
            link = f"https://t.me/{user}"
    elif link.lower().startswith("http://"):
        link = "https://" + link[7:]
    elif not link.lower().startswith("https://"):
        link = "https://" + link
    return link if _LINK_RE.match(link) else None


def _int(text):
    t = (text or "").strip().replace(" ", "").replace("_", "")
    return int(t) if t.isdigit() else None


@router.message(OrderSG.quantity)
async def msg_quantity(m: Message, state: FSMContext):
    data = await state.get_data()
    s = SERVICES.get(data.get("sid"))
    if not s:
        await state.clear()
        return await m.answer("⚠️ Xizmat topilmadi, qaytadan tanlang.", reply_markup=main_kb())
    qty = _int(m.text)
    mn, mx = int(s.get("min") or 1), int(s.get("max") or 10 ** 9)
    if qty is None or not (mn <= qty <= mx):
        return await m.answer(f"❗️ Miqdor {fmt(mn)} dan {fmt(mx)} gacha bo'lishi kerak.", reply_markup=cancel_kb())
    await state.update_data(qty=qty, total=qty, runs=None, interval=None)
    if has_dripfeed(s):
        await state.set_state(OrderSG.drip_choice)
        return await m.answer("💧 Drip-feed (bo'lib-bo'lib yuborish) kerakmi?",
                              reply_markup=kb([[btn("Yo'q", "drip:no"), btn("Ha", "drip:yes")],
                                               [btn("❌ Bekor qilish", "cancel_flow")]]))
    await show_confirm(m, state)


@router.message(OrderSG.comments)
async def msg_comments(m: Message, state: FSMContext):
    data = await state.get_data()
    s = SERVICES.get(data.get("sid"))
    if not s:
        await state.clear()
        return await m.answer("⚠️ Xizmat topilmadi, qaytadan tanlang.", reply_markup=main_kb())
    lines = [ln.strip() for ln in (m.text or "").split("\n") if ln.strip()]
    mn, mx = int(s.get("min") or 1), int(s.get("max") or 10 ** 9)
    if not (mn <= len(lines) <= mx):
        return await m.answer(f"❗️ Qatorlar soni {fmt(mn)} dan {fmt(mx)} gacha bo'lishi kerak "
                              f"(siz {len(lines)} ta yubordingiz).", reply_markup=cancel_kb())
    await state.update_data(comments="\n".join(lines), qty=len(lines), total=len(lines))
    await show_confirm(m, state)


@router.callback_query(OrderSG.drip_choice, F.data.startswith("drip:"))
async def cb_drip(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    if cb.data == "drip:no":
        return await show_confirm(cb.message, state, edit_cb=cb)
    await state.set_state(OrderSG.runs)
    await safe_edit(cb, "🔁 Necha marta yuborilsin? (runs, masalan 5)", cancel_kb())


@router.message(OrderSG.runs)
async def msg_runs(m: Message, state: FSMContext):
    runs = _int(m.text)
    if runs is None or not (2 <= runs <= 1000):
        return await m.answer("❗️ 2 dan 1000 gacha son kiriting.", reply_markup=cancel_kb())
    data = await state.get_data()
    await state.update_data(runs=runs, total=data["qty"] * runs)
    await state.set_state(OrderSG.interval)
    await m.answer("⏱ Har yuborish orasidagi vaqt (daqiqada, masalan 30):", reply_markup=cancel_kb())


@router.message(OrderSG.interval)
async def msg_interval(m: Message, state: FSMContext):
    interval = _int(m.text)
    if interval is None or not (1 <= interval <= 10000):
        return await m.answer("❗️ 1 dan 10000 gacha daqiqa kiriting.", reply_markup=cancel_kb())
    await state.update_data(interval=interval)
    await show_confirm(m, state)


async def show_confirm(m: Message, state: FSMContext, edit_cb: CallbackQuery = None):
    data = await state.get_data()
    s = SERVICES.get(data.get("sid"))
    if not s:
        await state.clear()
        return await m.answer("⚠️ Xizmat topilmadi, qaytadan tanlang.", reply_markup=main_kb())
    uid = edit_cb.from_user.id if edit_cb else m.from_user.id
    price = calc_price(s, data["total"])
    await state.update_data(price=price)
    await state.set_state(OrderSG.confirm)
    bal = db.balance(uid)
    lines = [f"🧾 <b>Buyurtmani tasdiqlang</b>\n",
             f"🔹 {esc(s.get('name'))}",
             f"🔗 {esc(data['link'])}"]
    if not is_package(s):
        lines.append(f"🔢 Miqdor: {fmt(data['qty'])}")
    if data.get("runs"):
        lines.append(f"💧 Drip-feed: {data['runs']} marta, {data['interval']} daqiqa oralig'ida "
                     f"(jami {fmt(data['total'])})")
    lines += [f"\n💰 Narx: <b>{fmt(price)} so'm</b>", f"💳 Balans: {fmt(bal)} so'm"]
    if bal < price:
        lines.append(f"\n❗️ Balans yetarli emas. Yana <b>{fmt(price - bal)} so'm</b> kerak.")
        markup = kb([[btn("💰 Balans to'ldirish", "topup")], [btn("❌ Bekor qilish", "cancel_flow")]])
    else:
        markup = kb([[btn("✅ Tasdiqlash", "confirm_order")], [btn("❌ Bekor qilish", "cancel_flow")]])
    text = "\n".join(lines)
    if edit_cb:
        await safe_edit(edit_cb, text, markup)
    else:
        await m.answer(text, reply_markup=markup, disable_web_page_preview=True)


@router.callback_query(OrderSG.confirm, F.data == "confirm_order")
async def cb_confirm(cb: CallbackQuery, state: FSMContext, bot: Bot):
    data = await state.get_data()
    await state.clear()
    uid = cb.from_user.id
    s = SERVICES.get(data.get("sid"))
    if not s or s["service"] in db.hidden_ids():
        await cb.answer()
        return await safe_edit(cb, "⚠️ Bu xizmat hozir mavjud emas.", kb([back_main()]))

    price = calc_price(s, data["total"])
    if price != data.get("price"):
        await cb.answer("Narx yangilandi", show_alert=True)
        await state.update_data(**data)
        return await show_confirm(cb.message, state, edit_cb=cb)

    if not db.deduct(uid, price):
        await cb.answer("Balans yetarli emas", show_alert=True)
        return await safe_edit(cb, f"❗️ Balans yetarli emas.\n💳 Balans: {fmt(db.balance(uid))} so'm\n"
                                   f"💰 Kerak: {fmt(price)} so'm",
                               kb([[btn("💰 Balans to'ldirish", "topup")], back_main()]))
    await cb.answer("⏳ Yuborilmoqda...")

    oid = db.create_order(uid, s["service"], str(s.get("name", "")), data["link"], data["total"], price)
    params = {"service": s["service"], "link": data["link"]}
    if data.get("comments"):
        params["comments"] = data["comments"]
    elif not is_package(s):
        params["quantity"] = data["qty"]
    if data.get("runs"):
        params["runs"] = data["runs"]
        params["interval"] = data["interval"]

    uname = f"@{cb.from_user.username}" if cb.from_user.username else "—"
    try:
        res = await api("add", **params)
        api_order = res.get("order") if isinstance(res, dict) else None
        if not api_order:
            raise ApiNetError(f"order ID yo'q: {str(res)[:200]}")
    except ApiError as e:
        db.update_order(oid, status="Failed")
        db.add_refund(oid, price)
        await notify_admin(bot, f"⚠️ <b>API xatosi</b> (buyurtma #{oid})\n👤 <code>{uid}</code> {esc(uname)}\n"
                                f"🔹 {esc(s.get('name'))}\n❗️ {esc(e)}\n"
                                f"💳 {fmt(price)} so'm foydalanuvchi balansiga qaytarildi.")
        return await safe_edit(cb, f"❌ Buyurtma qabul qilinmadi. Pul balansingizga qaytarildi.\n"
                                   f"Muammo bo'lsa admin bilan bog'laning: @{ADMIN_USERNAME}",
                               kb([[btn("☎️ Admin", url=f"https://t.me/{ADMIN_USERNAME}")], back_main()]))
    except ApiNetError as e:
        db.update_order(oid, status="Unknown")
        await notify_admin(bot, f"❓ <b>Javob noma'lum</b> (buyurtma #{oid}) — kometa-smm panelda tekshiring!\n"
                                f"👤 <code>{uid}</code> {esc(uname)}\n🔹 {esc(s.get('name'))}\n"
                                f"🔗 {esc(data['link'])} · {fmt(data['total'])} ta · {fmt(price)} so'm\n❗️ {esc(e)}")
        return await safe_edit(cb, f"⚠️ Buyurtma holati aniqlanmadi. Admin tekshirib chiqadi.\n"
                                   f"Savollar bo'lsa: @{ADMIN_USERNAME}\n🧾 Buyurtma: #{oid}",
                               kb([[btn("☎️ Admin", url=f"https://t.me/{ADMIN_USERNAME}")], back_main()]))

    db.update_order(oid, api_order=int(api_order))
    try:
        cost = Decimal(str(res.get("charge"))) if res.get("charge") is not None else None
        if cost is not None and cost > price:
            await notify_admin(bot, f"⚠️ Zarar: buyurtma #{oid} — panel {cost} so'm yechdi, "
                                    f"foydalanuvchi {fmt(price)} so'm to'ladi. Narxlarni tekshiring.")
    except Exception:
        pass
    await safe_edit(cb, f"✅ <b>Buyurtma qabul qilindi!</b>\n\n🧾 Buyurtma: #{oid}\n"
                        f"🔹 {esc(s.get('name'))}\n💰 {fmt(price)} so'm yechildi\n"
                        f"💳 Balans: {fmt(db.balance(uid))} so'm",
                    kb([[btn("📦 Buyurtmalarim", "myorders:0")], back_main()]))


# ---------- Buyurtmalarim ----------
@router.callback_query(F.data.startswith("myorders:"))
async def cb_my_orders(cb: CallbackQuery):
    page = int(cb.data.split(":")[1])
    uid = cb.from_user.id
    await cb.answer()
    total = db.user_order_count(uid)
    if not total:
        return await safe_edit(cb, "📦 Sizda hali buyurtmalar yo'q.",
                               kb([[btn("🛍 Xizmatlar", "services")], back_main()]))
    rows = []
    for o in db.user_orders(uid, PAGE_SIZE, page * PAGE_SIZE):
        icon = STATUS_UZ.get(o["status"], "• x").split(" ")[0]
        rows.append([btn(f"{icon} #{o['id']} · {(o['service_name'] or '')[:32]}", f"ord:{o['id']}")])
    nav = pager("myorders", page, total)
    if nav:
        rows.append(nav)
    rows.append(back_main())
    await safe_edit(cb, f"📦 <b>Buyurtmalarim</b> ({fmt(total)} ta):", kb(rows))


def order_text(o) -> str:
    lines = [f"🧾 <b>Buyurtma #{o['id']}</b>\n",
             f"🔹 {esc(o['service_name'])}",
             f"🔗 {esc(o['link'])}",
             f"🔢 Miqdor: {fmt(o['quantity'])}",
             f"💰 Narx: {fmt(o['charge'])} so'm",
             f"📊 Holat: {status_uz(o['status'])}"]
    if o.get("start_count") not in (None, ""):
        lines.append(f"▶️ Boshlang'ich: {esc(o['start_count'])}")
    if o.get("remains") not in (None, ""):
        lines.append(f"⏳ Qolgan: {esc(o['remains'])}")
    if int(o.get("refunded") or 0) > 0:
        lines.append(f"↩️ Qaytarildi: {fmt(o['refunded'])} so'm")
    if o.get("api_order") and o.get("provider") != PROVIDER:
        lines.append(f"\nℹ️ Eski buyurtma — holati yangilanmaydi. Savol bo'lsa: @{ADMIN_USERNAME}")
    return "\n".join(lines)


def is_live(o) -> bool:
    """Faqat Kometa'da yaratilgan buyurtmalar API orqali tekshiriladi."""
    return bool(o and o.get("api_order") and o.get("provider") == PROVIDER)


def order_kb(o):
    rows = []
    s = SERVICES.get(int(o["service_id"] or 0), {})
    if is_live(o) and o["status"] not in FINAL_STATUSES:
        rows.append([btn("🔄 Yangilash", f"ord:{o['id']}")])
        if s.get("cancel"):
            rows.append([btn("🚫 Bekor qilish", f"ocancel:{o['id']}")])
    if is_live(o) and o["status"] == "Completed" and s.get("refill"):
        rows.append([btn("♻️ Qayta tiklash", f"orefill:{o['id']}")])
    rows.append([btn("⬅️ Orqaga", "myorders:0"), btn("🏠 Menyu", "menu")])
    return kb(rows)


async def apply_status(bot: Bot, o: dict, st: dict, notify=True):
    """API holatini bazaga yozadi, kerak bo'lsa pulni qaytaradi va xabar beradi."""
    new = norm_status(st.get("status"))
    old = o["status"]
    fields = {"status": new}
    if "start_count" in st:
        fields["start_count"] = str(st.get("start_count"))
    if "remains" in st:
        fields["remains"] = str(st.get("remains"))
    db.update_order(o["id"], **fields)

    refunded = 0
    charge = int(o["charge"] or 0)
    if new == "Canceled":
        refunded = db.add_refund(o["id"], charge)
    elif new == "Partial":
        try:
            remains = max(0, int(Decimal(str(st.get("remains") or 0))))
            qty = int(o["quantity"] or 0)
            if qty > 0:
                part = int((Decimal(charge) * min(remains, qty) / qty).to_integral_value(rounding=ROUND_FLOOR))
                refunded = db.add_refund(o["id"], part)
        except Exception as e:
            log.error("Partial refund xatosi #%s: %s", o["id"], e)

    if notify and new != old and new in ("Completed", "Partial", "Canceled"):
        msg = f"🧾 Buyurtma #{o['id']}: {status_uz(new)}\n🔹 {esc(o['service_name'])}"
        if refunded:
            msg += f"\n↩️ {fmt(refunded)} so'm balansingizga qaytarildi."
        try:
            await bot.send_message(o["user_id"], msg, disable_web_page_preview=True)
        except Exception:
            pass


@router.callback_query(F.data.startswith("ord:"))
async def cb_order_view(cb: CallbackQuery, bot: Bot):
    oid = int(cb.data.split(":")[1])
    o = db.get_order(oid)
    if not o or o["user_id"] != cb.from_user.id:
        return await cb.answer("Topilmadi", show_alert=True)
    await cb.answer()
    if is_live(o) and o["status"] not in FINAL_STATUSES:
        try:
            st = await api("status", order=o["api_order"])
            if isinstance(st, dict) and st.get("status"):
                await apply_status(bot, o, st, notify=False)
                o = db.get_order(oid)
        except (ApiError, ApiNetError) as e:
            log.warning("Status olinmadi #%s: %s", oid, e)
    await safe_edit(cb, order_text(o), order_kb(o))


@router.callback_query(F.data.startswith("orefill:"))
async def cb_order_refill(cb: CallbackQuery):
    oid = int(cb.data.split(":")[1])
    o = db.get_order(oid)
    if not o or o["user_id"] != cb.from_user.id or not is_live(o):
        return await cb.answer("Topilmadi", show_alert=True)
    try:
        await api("refill", order=o["api_order"])
        await cb.answer("♻️ Qayta tiklash so'rovi yuborildi", show_alert=True)
    except ApiError as e:
        await cb.answer(f"❗️ {str(e)[:150]}", show_alert=True)
    except ApiNetError:
        await cb.answer("❗️ Server javob bermadi, keyinroq urinib ko'ring", show_alert=True)


@router.callback_query(F.data.startswith("ocancel:"))
async def cb_order_cancel(cb: CallbackQuery):
    oid = int(cb.data.split(":")[1])
    o = db.get_order(oid)
    if not o or o["user_id"] != cb.from_user.id or not is_live(o):
        return await cb.answer("Topilmadi", show_alert=True)
    try:
        await api("cancel", orders=o["api_order"])
        await cb.answer("🚫 Bekor qilish so'rovi yuborildi. Bekor qilinsa pul balansga qaytadi.", show_alert=True)
    except ApiError as e:
        await cb.answer(f"❗️ {str(e)[:150]}", show_alert=True)
    except ApiNetError:
        await cb.answer("❗️ Server javob bermadi, keyinroq urinib ko'ring", show_alert=True)


# Holatdan tashqari matn — menyuni ko'rsatish
@router.message(F.text & ~F.text.startswith("/"))
async def fallback(m: Message, state: FSMContext):
    if await state.get_state():
        return
    db.ensure_user(m.from_user.id, m.from_user.username, m.from_user.first_name)
    await m.answer(welcome_text(m.from_user.id), reply_markup=main_kb())


# ===================== FON VAZIFALARI =====================
async def status_poller(bot: Bot):
    await asyncio.sleep(15)
    while True:
        try:
            orders = db.active_orders(100)
            if not orders:
                continue
            by_api = {str(o["api_order"]): o for o in orders}
            res = await api("status", orders=",".join(by_api))
            if isinstance(res, dict):
                if len(by_api) == 1 and "status" in res:
                    res = {next(iter(by_api)): res}
                for aid, st in res.items():
                    o = by_api.get(str(aid))
                    if o and isinstance(st, dict) and st.get("status") and not st.get("error"):
                        await apply_status(bot, o, st)
        except (ApiError, ApiNetError) as e:
            log.warning("Poller API xatosi: %s", e)
        except Exception:
            log.exception("Poller xatosi")
        await asyncio.sleep(POLL_INTERVAL)


async def services_refresher():
    while True:
        try:
            await load_services(force=True)
        except Exception:
            log.exception("Xizmatlarni yangilashda xato")
        await asyncio.sleep(SERVICES_TTL)


_bg_tasks = []


async def on_startup(bot: Bot, dispatcher: Dispatcher):
    global db, http
    db = DB()
    http = aiohttp.ClientSession()
    if not API_KEY:
        log.error("SMM_API_KEY o'rnatilmagan!")
    _bg_tasks.append(asyncio.create_task(services_refresher()))
    _bg_tasks.append(asyncio.create_task(status_poller(bot)))
    if WEBHOOK_BASE:
        url = WEBHOOK_BASE.rstrip("/") + WEBHOOK_PATH
        await bot.set_webhook(url, secret_token=WEBHOOK_SECRET,
                              allowed_updates=dispatcher.resolve_used_update_types())
        log.info("Webhook o'rnatildi: %s", url)


async def on_shutdown(bot: Bot):
    for t in _bg_tasks:
        t.cancel()
    if http:
        await http.close()


def main():
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    if not WEBHOOK_BASE:
        # Lokal sinov: polling
        async def _poll():
            await bot.delete_webhook(drop_pending_updates=False)
            await dp.start_polling(bot)
        asyncio.run(_poll())
        return

    app = web.Application()
    app.router.add_get("/", lambda r: web.Response(text="OK"))
    SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=WEBHOOK_SECRET).register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)
    web.run_app(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
