# -*- coding: utf-8 -*-
import asyncio
import io
import logging
import math
import os
import shutil
import urllib.parse
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import aiohttp
import aiosqlite
import segno
from aiogram import BaseMiddleware, Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramConflictError, TelegramRetryAfter
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BufferedInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Template
from starlette.middleware.sessions import SessionMiddleware

# ==========================================
# MASTER CONFIGURATION & STORAGE PATHS
# ==========================================
logging.basicConfig(level=logging.INFO)

MASTER_USER = os.getenv("MASTER_USER", "admin")
MASTER_PASS = os.getenv("MASTER_PASS", "admin@123")
SESSION_SECRET = os.getenv("SESSION_SECRET", "super_master_session_key_9988_xyz")

DATA_DIR = os.getenv("DATA_DIR", "/app/data" if os.path.exists("/app/data") else ".")
MASTER_DB_NAME = os.path.join(DATA_DIR, "master_database.db")
TENANTS_DIR = os.path.join(DATA_DIR, "tenants")
STATIC_DIR = os.path.join(os.getcwd(), "static")
UPLOAD_DIR = os.path.join(STATIC_DIR, "uploads")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(TENANTS_DIR, exist_ok=True)
os.makedirs(UPLOAD_DIR, exist_ok=True)

MASTER_DB_LOCK = asyncio.Lock()
TENANT_DB_LOCKS = defaultdict(asyncio.Lock)

# ==========================================
# MASTER DATABASE LAYER
# ==========================================
async def get_master_db():
    db = await aiosqlite.connect(MASTER_DB_NAME, timeout=30.0)
    await db.execute("PRAGMA journal_mode = WAL;")
    await db.execute("PRAGMA synchronous = NORMAL;")
    return db

async def init_master_db():
    async with MASTER_DB_LOCK:
        db = await get_master_db()
        try:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS clients (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_name TEXT NOT NULL,
                    telegram TEXT,
                    phone TEXT,
                    slug TEXT UNIQUE NOT NULL,
                    make_date TEXT NOT NULL,
                    expiry_date TEXT NOT NULL,
                    plan_days INTEGER NOT NULL,
                    price REAL DEFAULT 0,
                    status TEXT DEFAULT 'ACTIVE',
                    admin_username TEXT NOT NULL,
                    admin_password TEXT NOT NULL,
                    bot_token TEXT DEFAULT '',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_clients_slug ON clients(slug);")
            await db.commit()
        finally:
            await db.close()

# ==========================================
# TENANT DATABASE LAYER (ISOLATED PER SLUG)
# ==========================================
def get_tenant_db_path(slug: str) -> str:
    return os.path.join(TENANTS_DIR, f"{slug}.db")

async def get_tenant_db(slug: str):
    db = await aiosqlite.connect(get_tenant_db_path(slug), timeout=30.0)
    await db.execute("PRAGMA journal_mode = WAL;")
    await db.execute("PRAGMA synchronous = NORMAL;")
    await db.execute("PRAGMA busy_timeout = 30000;")
    await db.execute("PRAGMA cache_size = -64000;")
    return db

async def init_tenant_db(slug: str, initial_token: str = ""):
    async with TENANT_DB_LOCKS[slug]:
        db = await get_tenant_db(slug)
        try:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    full_name TEXT,
                    username TEXT,
                    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    premium_status TEXT DEFAULT 'Free',
                    is_banned INTEGER DEFAULT 0
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    plan_name TEXT,
                    amount REAL,
                    status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS plans (
                    plan_id TEXT PRIMARY KEY,
                    name TEXT,
                    amount REAL,
                    validity TEXT,
                    access_link TEXT DEFAULT ''
                )
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_joined ON users(joined_at DESC);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_payments_user_id ON payments(user_id);")

            defaults = {
                "bot_token": initial_token,
                "admin_chat_id": "",
                "maintenance": "off",
                "upi_id": "YOUR_UPI",
                "payee_name": "YOUR_UPI_NAME",
                "welcome_photo": "https://kommodo.ai/i/vMd2KH7PZC8bgMH9mGWm",
                "welcome_text": (
                    "🎉 Welcome to VIP Access Bot!\n\n✨ Get exclusive access to premium content\n💰 Affordable plans starting at just ₹99\n✨ Cotent quality aisi ki dekhi nahi hogi \n✨ Only Premium Content\n✨ Daily New Uploads\n✨ Cp, Rp, Indian, Foreign, Dark everything\n✨ 10000+ Cp videos\n✨  25000+ Rp videos \n✨  M0m S0n 5k Videos\n\n✨ TRY OUR ANY PLAN FOR CHECKING THE QUALITY ✨"
                ),
                "plans_text": (
                    "💎 ᴘʀᴇᴍɪᴜᴍ ᴘʟᴀɴs\n\n━━━━━━━━━━━━━━━━━\n🔹 😚ᴀᴅɪᴛʏ ᴍɪsʀʏ ᴀʟʟ 🥵\n   💰 ᴘʀɪᴄᴇ: ₹99\n   ⏳ ᴠᴀʟɪᴅɪᴛʏ: 30 Days\n\n🔹 🌽ᴄʜ!ᴅ ᴄᴏʀɴ 🌽\n   💰 ᴘʀɪᴄᴇ: ₹49\n   ⏳ ᴠᴀʟɪᴅɪᴛʏ: 30 Days\n🔹 💦ɪɴsᴛᴀɢʀᴀᴍ ʟᴇᴀᴋᴇᴅ 🍑\n   💰 ᴘʀɪᴄᴇ: ₹59\n   ⏳ ᴠᴀʟɪᴅɪᴛʏ: 30 Days\n🔹 💋ʙʀᴏᴛʜᴇʀ ᴀɴᴅ sɪsᴛᴇʀ 💦\n   💰 ᴘʀɪᴄᴇ: ₹69\n   ⏳ ᴠᴀʟɪᴅɪᴛʏ: 30 Days\n\n🔹 🫦ᴍᴏᴍ ᴀɴᴅ sᴏɴ 🥵\n   💰 ᴘʀɪᴄᴇ: ₹69\n   ⏳ ᴠᴀʟɪᴅɪᴛʏ: 30 Days\n🔹 ✂️ ʟᴇsʙɪᴀɴs ✂️\n   💰 ᴘʀɪᴄᴇ: ₹79\n   ⏳ ᴠᴀʟɪᴅɪᴛʏ: 30 Days\n\n🔹 🤤ɪɴᴅɪᴀɴ ᴡᴇʙsᴇʀɪᴇs 💦\n   💰 ᴘʀɪᴄᴇ: ₹99\n   ⏳ ᴠᴀʟɪᴅɪᴛʏ: 30 Days\n\n🔹 ᴀʟʟ ᴛʏᴘᴇ 🌽🌽\n   💰 ᴘʀɪᴄᴇ: ₹129\n   ⏳ ᴠᴀʟɪᴅɪᴛʏ: Life time\n\n━━━━━━━━━━━━━━━━━\n👇 sᴇʟᴇᴄᴛ ʏᴏᴜʀ ᴘʟᴀɴ ʙᴇʟᴏᴡ"
                ),
                "demo_video": "https://www.image2url.com/r2/default/videos/1788689350793-635df470-449c-4fd5-8ed3-96f503e8b88b.mp4",
            }
            for k, v in defaults.items():
                await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v))

            default_plans = [
                ("plan_1", "😚ᴀᴅɪᴛʏ ᴍɪsʀʏ ᴀʟʟ 🥵", 99.0, "30 Days", ""),
                ("plan_2", "🌽ᴄʜ!ᴅ ᴄᴏʀɴ 🌽", 49.0, "30 Days", ""),
                ("plan_3", "💦ɪɴsᴛᴀɢʀᴀᴍ ʟᴇᴀᴋᴇᴅ 🍑", 59.0, "30 Days", ""),
                ("plan_4", "💋ʙʀᴏᴛʜᴇʀ ᴀɴᴅ sɪsᴛᴇʀ 💦", 69.0, "30 Days", ""),
                ("plan_5", "🫦ᴍᴏᴍ ᴀɴᴅ sᴏɴ 🥵", 69.0, "30 Days", ""),
                ("plan_6", "✂️ ʟᴇsʙɪᴀɴs ✂️", 79.0, "30 Days", ""),
                ("plan_7", "🤤ɪɴᴅɪᴀɴ ᴡᴇʙsᴇʀɪᴇs 💦", 99.0, "30 Days", ""),
                ("plan_8", "ᴀʟʟ ᴛʏᴘᴇ 🌽🌽", 129.0, "Lifetime", ""),
            ]
            for p in default_plans:
                await db.execute(
                    "INSERT OR IGNORE INTO plans (plan_id, name, amount, validity, access_link) VALUES (?, ?, ?, ?, ?)",
                    p,
                )
            await db.commit()
        finally:
            await db.close()

async def get_tenant_setting(slug: str, key: str) -> str:
    db = await get_tenant_db(slug)
    try:
        async with db.execute("SELECT value FROM settings WHERE key = ?", (key,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else ""
    finally:
        await db.close()

async def update_tenant_setting(slug: str, key: str, value: str):
    async with TENANT_DB_LOCKS[slug]:
        db = await get_tenant_db(slug)
        try:
            await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
            await db.commit()
        finally:
            await db.close()

async def get_tenant_admin_ids(slug: str) -> list[int]:
    chat_id_str = await get_tenant_setting(slug, "admin_chat_id")
    ids = []
    if chat_id_str:
        for x in chat_id_str.split(","):
            x = x.strip()
            if x.lstrip("-").isdigit():
                ids.append(int(x))
    return ids

async def get_tenant_demo_video_list(slug: str) -> list[str]:
    raw = await get_tenant_setting(slug, "demo_video")
    if not raw:
        return []
    return [line.strip() for line in raw.replace(",", "\n").splitlines() if line.strip()]

async def get_tenant_all_plans(slug: str):
    db = await get_tenant_db(slug)
    try:
        async with db.execute("SELECT plan_id, name, amount, validity, COALESCE(access_link, '') FROM plans ORDER BY plan_id ASC") as cur:
            return await cur.fetchall()
    finally:
        await db.close()

async def get_tenant_plan(slug: str, plan_id: str):
    db = await get_tenant_db(slug)
    try:
        async with db.execute("SELECT plan_id, name, amount, validity, COALESCE(access_link, '') FROM plans WHERE plan_id = ?", (plan_id,)) as cur:
            return await cur.fetchone()
    finally:
        await db.close()

async def get_tenant_plan_by_name(slug: str, name: str):
    db = await get_tenant_db(slug)
    try:
        async with db.execute("SELECT plan_id, name, amount, validity, COALESCE(access_link, '') FROM plans WHERE name = ?", (name,)) as cur:
            return await cur.fetchone()
    finally:
        await db.close()

async def add_tenant_user(slug: str, user: types.User):
    async with TENANT_DB_LOCKS[slug]:
        db = await get_tenant_db(slug)
        try:
            await db.execute("""
                INSERT INTO users (user_id, full_name, username)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET full_name = excluded.full_name, username = excluded.username
            """, (user.id, user.full_name, user.username or "N/A"))
            await db.commit()
        finally:
            await db.close()

async def get_tenant_user(slug: str, user_id: int):
    db = await get_tenant_db(slug)
    try:
        async with db.execute("SELECT user_id, full_name, username, joined_at, premium_status, is_banned FROM users WHERE user_id = ?", (user_id,)) as cur:
            return await cur.fetchone()
    finally:
        await db.close()

async def get_tenant_paginated_users(slug: str, limit: int = 50, offset: int = 0, search: str = ""):
    db = await get_tenant_db(slug)
    try:
        search_query = f"%{search.strip().lstrip('@')}%"
        if search.strip():
            async with db.execute(
                "SELECT COUNT(*) FROM users WHERE CAST(user_id AS TEXT) LIKE ? OR username LIKE ? OR full_name LIKE ?",
                (search_query, search_query, search_query)
            ) as cur:
                total_count = (await cur.fetchone())[0]

            async with db.execute(
                "SELECT user_id, full_name, username, joined_at, premium_status, is_banned FROM users WHERE CAST(user_id AS TEXT) LIKE ? OR username LIKE ? OR full_name LIKE ? ORDER BY joined_at DESC LIMIT ? OFFSET ?",
                (search_query, search_query, search_query, limit, offset),
            ) as cur:
                rows = await cur.fetchall()
        else:
            async with db.execute("SELECT COUNT(*) FROM users") as cur:
                total_count = (await cur.fetchone())[0]

            async with db.execute(
                "SELECT user_id, full_name, username, joined_at, premium_status, is_banned FROM users ORDER BY joined_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ) as cur:
                rows = await cur.fetchall()

        return rows, total_count
    finally:
        await db.close()

async def get_tenant_dashboard_metrics(slug: str):
    db = await get_tenant_db(slug)
    try:
        async with db.execute("SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM payments WHERE status='approved'") as cur:
            row = await cur.fetchone()
            paid_orders = row[0] if row else 0
            revenue = row[1] if row else 0.0

        async with db.execute("SELECT COUNT(*) FROM users") as cur:
            total_users = (await cur.fetchone())[0]

        async with db.execute("""
            SELECT p.id, p.user_id, COALESCE(u.username, 'N/A'), p.plan_name, p.amount, p.status, p.created_at
            FROM payments p
            LEFT JOIN users u ON p.user_id = u.user_id
            ORDER BY p.id DESC 
            LIMIT 50
        """) as cur:
            all_orders = await cur.fetchall()

        return {
            "paid_orders": paid_orders,
            "revenue": f"{revenue:,.2f}",
            "total_users": total_users,
            "recent_orders": all_orders[:20],
            "all_orders": all_orders,
        }
    finally:
        await db.close()

async def get_tenant_user_payment_stats(slug: str, user_id: int):
    db = await get_tenant_db(slug)
    try:
        async with db.execute("""
            SELECT 
                COUNT(CASE WHEN status = 'approved' THEN 1 END),
                COUNT(CASE WHEN status = 'pending' THEN 1 END),
                COUNT(*)
            FROM payments WHERE user_id = ?
        """, (user_id,)) as cur:
            row = await cur.fetchone()
            return {"approved": row[0] or 0, "pending": row[1] or 0, "total": row[2] or 0}
    finally:
        await db.close()

# ==========================================
# UPI QR GENERATION & DETECTION
# ==========================================
def _generate_qr_sync(upi_url: str) -> io.BytesIO:
    qr = segno.make(upi_url, error="m")
    buffer = io.BytesIO()
    qr.save(buffer, kind="png", scale=8, border=2)
    buffer.seek(0)
    return buffer

async def generate_tenant_upi_qr(slug: str, plan_name: str, amount: float) -> io.BytesIO:
    upi_id = await get_tenant_setting(slug, "upi_id")
    payee_name = await get_tenant_setting(slug, "payee_name")
    upi_params = {
        "pa": upi_id,
        "pn": payee_name,
        "am": f"{amount:.2f}",
        "cu": "INR",
        "tn": f"Payment for {plan_name}",
    }
    upi_url = "upi://pay?" + urllib.parse.urlencode(upi_params)
    return await asyncio.to_thread(_generate_qr_sync, upi_url)

async def detect_payee_name_from_upi(upi_id: str) -> str:
    upi_id = upi_id.strip()
    if "@" not in upi_id:
        return ""
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    try:
        url = f"https://upier.vercel.app/api/check?vpa={urllib.parse.quote(upi_id)}"
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2.5), headers=headers) as s:
            async with s.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    name = data.get("name") or data.get("payeeAccountName")
                    if name and name.strip():
                        return name.strip()
    except Exception:
        pass
    return ""

# ==========================================
# KEYBOARDS & UI BUILDERS
# ==========================================
def get_home_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="🎬 View Demo", callback_data="btn_view_demo:0")
    builder.button(text="⭐ My Premium", callback_data="btn_my_premium")
    builder.button(text="👤 My Profile", callback_data="btn_my_profile")
    builder.adjust(1)
    return builder.as_markup()

def get_demo_keyboard(current_idx: int, total_videos: int):
    builder = InlineKeyboardBuilder()
    nav_row = []
    if current_idx > 0:
        nav_row.append(types.InlineKeyboardButton(text="◀️ Previous", callback_data=f"btn_view_demo:{current_idx - 1}"))
    if total_videos > 1:
        nav_row.append(types.InlineKeyboardButton(text=f"[{current_idx + 1}/{total_videos}]", callback_data="noop"))
    if current_idx < total_videos - 1:
        nav_row.append(types.InlineKeyboardButton(text="Next ▶️", callback_data=f"btn_view_demo:{current_idx + 1}"))
    if nav_row:
        builder.row(*nav_row)
    builder.row(
        types.InlineKeyboardButton(text="💎 Get Premium", callback_data="btn_get_premium"),
        types.InlineKeyboardButton(text="🏠 Home", callback_data="btn_home"),
    )
    return builder.as_markup()

async def get_plans_keyboard(slug: str):
    builder = InlineKeyboardBuilder()
    plans = await get_tenant_all_plans(slug)
    for pid, name, price, _, _ in plans:
        builder.button(text=f"🔥 {name} (Rs.{int(price)})", callback_data=f"buy_plan:{pid}")
    builder.button(text="🏠 Home", callback_data="btn_home")
    builder.adjust(*(1 for _ in range(len(plans) + 1)))
    return builder.as_markup()

def get_upi_card_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="📥 CHECK PAYMENT", callback_data="check_payment")
    builder.button(text="🔙 BACK TO PLANS", callback_data="btn_get_premium")
    builder.adjust(1)
    return builder.as_markup()

# ==========================================
# MULTI-TENANT BOT ENGINE
# ==========================================
class PaymentStates(StatesGroup):
    waiting_for_screenshot = State()

class MultiBotEngine:
    def __init__(self):
        self.active_bots: dict[str, Bot] = {}
        self.active_dispatchers: dict[str, Dispatcher] = {}
        self.running_tasks: dict[str, asyncio.Task] = {}
        self.active_sessions: dict[str, AiohttpSession] = {}
        self.user_messages: dict[str, dict[int, list[int]]] = defaultdict(lambda: defaultdict(list))

    def track(self, slug: str, chat_id: int, message_id: int):
        if message_id not in self.user_messages[slug][chat_id]:
            self.user_messages[slug][chat_id].append(message_id)

    async def delete_old_messages(self, slug: str, chat_id: int, exclude_ids: list[int] | None = None):
        bot = self.active_bots.get(slug)
        if not bot:
            return
        exclude = set(exclude_ids or [])
        all_ids = [mid for mid in self.user_messages[slug].get(chat_id, []) if mid not in exclude]
        self.user_messages[slug][chat_id] = [mid for mid in self.user_messages[slug].get(chat_id, []) if mid in exclude]
        if not all_ids:
            return

        for i in range(0, len(all_ids), 100):
            chunk = all_ids[i : i + 100]
            try:
                await bot.delete_messages(chat_id=chat_id, message_ids=chunk)
            except TelegramBadRequest:
                for mid in chunk:
                    try:
                        await bot.delete_message(chat_id=chat_id, message_id=mid)
                    except Exception:
                        pass
            except Exception:
                pass

    async def notify_payment_approved(self, slug: str, user_id: int, plan_name: str):
        bot = self.active_bots.get(slug)
        if not bot:
            return
        plan_info = await get_tenant_plan_by_name(slug, plan_name)
        access_link = plan_info[4] if plan_info and len(plan_info) > 4 else ""

        builder = InlineKeyboardBuilder()
        if access_link and access_link.strip().startswith(("http://", "https://", "t.me/")):
            link_url = access_link.strip()
            if link_url.startswith("t.me/"):
                link_url = "https://" + link_url
            builder.button(text="🔗 Join VIP Channel / Access Link", url=link_url)
        builder.button(text="🏠 Home", callback_data="btn_home")
        builder.adjust(1)

        caption = f"🎉 <b>Payment Approved!</b>\n\nYour subscription for <b>{plan_name}</b> is now active!\n"
        if access_link:
            caption += "\n👉 Click the button below to claim your access:"

        try:
            await bot.send_message(chat_id=user_id, text=caption, reply_markup=builder.as_markup())
        except Exception as e:
            logging.warning(f"[{slug}] Could not deliver approval message to {user_id}: {e}")

    async def send_welcome_flow(self, slug: str, chat_id: int):
        bot = self.active_bots.get(slug)
        if not bot:
            return
        photo_url = await get_tenant_setting(slug, "welcome_photo")
        caption = await get_tenant_setting(slug, "welcome_text")

        sent_photo = False
        if photo_url and photo_url.startswith(("http://", "https://", "AgAC")):
            try:
                async with asyncio.timeout(2.5):
                    m1 = await bot.send_photo(chat_id=chat_id, photo=photo_url, caption=caption, reply_markup=get_home_keyboard())
                    self.track(slug, chat_id, m1.message_id)
                    sent_photo = True
            except Exception as e:
                logging.warning(f"[{slug}] Welcome fallback: {e}")

        if not sent_photo:
            m1 = await bot.send_message(chat_id=chat_id, text=caption, reply_markup=get_home_keyboard())
            self.track(slug, chat_id, m1.message_id)

        plans_txt = await get_tenant_setting(slug, "plans_text")
        m2 = await bot.send_message(chat_id=chat_id, text=plans_txt, reply_markup=await get_plans_keyboard(slug))
        self.track(slug, chat_id, m2.message_id)

    async def start_tenant_bot(self, slug: str, token: str):
        await self.stop_tenant_bot(slug)
        token = token.strip()
        if not token or token == "YOUR_BOT_TOKEN_HERE" or ":" not in token:
            return

        session = AiohttpSession(timeout=20.0)
        bot = Bot(token=token, session=session, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        dp = Dispatcher(storage=MemoryStorage())

        try:
            await bot.delete_webhook(drop_pending_updates=True)
            await bot.session.close()
            session = AiohttpSession(timeout=20.0)
            bot.session = session
        except Exception as e:
            logging.warning(f"[{slug}] Initial reset warning: {e}")

        self._attach_handlers(dp, slug)

        async def runner():
            while True:
                try:
                    logging.info(f"[{slug}] Polling loop started...")
                    await dp.start_polling(bot, drop_pending_updates=True, allowed_updates=["message", "callback_query"])
                    break
                except TelegramConflictError:
                    await asyncio.sleep(5)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logging.error(f"[{slug}] Polling loop error: {e}. Retrying in 4s...")
                    await asyncio.sleep(4)

        self.active_bots[slug] = bot
        self.active_dispatchers[slug] = dp
        self.active_sessions[slug] = session
        self.running_tasks[slug] = asyncio.create_task(runner())

    async def stop_tenant_bot(self, slug: str):
        if slug in self.running_tasks:
            task = self.running_tasks.pop(slug)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        if slug in self.active_dispatchers:
            dp = self.active_dispatchers.pop(slug)
            try:
                await dp.stop_polling()
            except Exception:
                pass

        if slug in self.active_bots:
            bot = self.active_bots.pop(slug)
            session = self.active_sessions.pop(slug, None)
            try:
                if bot.session:
                    await bot.session.close()
            except Exception:
                pass
        logging.info(f"[{slug}] Bot stopped cleanly.")

    def _attach_handlers(self, dp: Dispatcher, slug: str):
        engine = self

        class TenantTrackerMiddleware(BaseMiddleware):
            async def __call__(self, handler, event: types.TelegramObject, data: dict):
                if isinstance(event, types.Message):
                    engine.track(slug, event.chat.id, event.message_id)
                return await handler(event, data)

        class TenantSecurityMiddleware(BaseMiddleware):
            async def __call__(self, handler, event: types.TelegramObject, data: dict):
                user = data.get("event_from_user")
                admin_ids = await get_tenant_admin_ids(slug)
                if not user or user.id in admin_ids:
                    return await handler(event, data)
                user_record = await get_tenant_user(slug, user.id)
                if user_record and user_record[5] == 1:
                    if isinstance(event, types.Message):
                        await event.answer("You are banned from using this bot.")
                    return
                if await get_tenant_setting(slug, "maintenance") == "on":
                    if isinstance(event, types.Message):
                        await event.answer("Bot is under maintenance. Please try again later.")
                    return
                return await handler(event, data)

        dp.message.outer_middleware(TenantTrackerMiddleware())
        dp.message.outer_middleware(TenantSecurityMiddleware())
        dp.callback_query.outer_middleware(TenantSecurityMiddleware())

        @dp.message(CommandStart())
        async def handle_start(message: types.Message):
            await add_tenant_user(slug, message.from_user)
            await engine.send_welcome_flow(slug, message.chat.id)

        @dp.callback_query(F.data == "btn_home")
        async def nav_home(callback: types.CallbackQuery, state: FSMContext):
            await state.clear()
            await callback.answer()
            await engine.delete_old_messages(slug, callback.message.chat.id)
            try:
                await callback.message.delete()
            except Exception:
                pass
            await engine.send_welcome_flow(slug, callback.message.chat.id)

        @dp.callback_query(F.data == "btn_get_premium")
        async def nav_plans(callback: types.CallbackQuery, state: FSMContext):
            await state.clear()
            await callback.answer()
            try:
                await callback.message.delete()
            except Exception:
                pass
            bot = engine.active_bots.get(slug)
            if not bot:
                return
            text = await get_tenant_setting(slug, "plans_text")
            msg = await bot.send_message(
                chat_id=callback.message.chat.id,
                text=text,
                reply_markup=await get_plans_keyboard(slug),
            )
            engine.track(slug, callback.message.chat.id, msg.message_id)

        @dp.callback_query(F.data == "noop")
        async def handle_noop(callback: types.CallbackQuery):
            await callback.answer()

        @dp.callback_query(F.data.startswith("btn_view_demo"))
        async def nav_demo(callback: types.CallbackQuery):
            await callback.answer()
            try:
                await callback.message.delete()
            except Exception:
                pass
            bot = engine.active_bots.get(slug)
            if not bot:
                return

            parts = callback.data.split(":")
            idx = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
            videos = await get_tenant_demo_video_list(slug)
            if not videos:
                msg = await bot.send_message(
                    chat_id=callback.message.chat.id,
                    text="📺 No demo videos available right now.",
                    reply_markup=get_demo_keyboard(0, 0),
                )
                engine.track(slug, callback.message.chat.id, msg.message_id)
                return

            idx = max(0, min(idx, len(videos) - 1))
            video_url = videos[idx]

            try:
                async with asyncio.timeout(3.5):
                    msg = await bot.send_video(
                        chat_id=callback.message.chat.id,
                        video=video_url,
                        caption=f"📺 <b>Demo Video</b> ({idx + 1}/{len(videos)})",
                        reply_markup=get_demo_keyboard(idx, len(videos)),
                    )
                    engine.track(slug, callback.message.chat.id, msg.message_id)
            except Exception:
                msg = await bot.send_message(
                    chat_id=callback.message.chat.id,
                    text=f"📺 <b>Demo Video</b> ({idx + 1}/{len(videos)})\n\n🔗 {video_url}",
                    reply_markup=get_demo_keyboard(idx, len(videos)),
                )
                engine.track(slug, callback.message.chat.id, msg.message_id)

        @dp.callback_query(F.data == "btn_my_premium")
        async def nav_my_premium(callback: types.CallbackQuery):
            await callback.answer()
            try:
                await callback.message.delete()
            except Exception:
                pass
            bot = engine.active_bots.get(slug)
            if not bot:
                return
            user = await get_tenant_user(slug, callback.from_user.id)
            plan = user[4] if user else "Free"

            builder = InlineKeyboardBuilder()
            if plan != "Free":
                p_info = await get_tenant_plan_by_name(slug, plan)
                if p_info and p_info[4]:
                    link = p_info[4].strip()
                    if link.startswith("t.me/"):
                        link = "https://" + link
                    builder.button(text="🔗 Open Premium Channel / Link", url=link)
            builder.button(text="💎 Upgrade", callback_data="btn_get_premium")
            builder.button(text="🏠 Home", callback_data="btn_home")
            builder.adjust(1)

            text = f"⭐ My Premium Membership\n\nPlan: <b>{plan}</b>\nStatus: <b>{'Active' if plan != 'Free' else 'Free Tier'}</b>"
            msg = await bot.send_message(chat_id=callback.message.chat.id, text=text, reply_markup=builder.as_markup())
            engine.track(slug, callback.message.chat.id, msg.message_id)

        @dp.callback_query(F.data == "btn_my_profile")
        async def nav_my_profile(callback: types.CallbackQuery):
            await callback.answer()
            try:
                await callback.message.delete()
            except Exception:
                pass
            bot = engine.active_bots.get(slug)
            if not bot:
                return
            user = await get_tenant_user(slug, callback.from_user.id)
            if not user:
                return
            uid, full_name, username, joined_at, plan, _ = user
            payments = await get_tenant_user_payment_stats(slug, uid)
            text = (
                f"👤 MY PROFILE\n"
                f"Name: {full_name}\n"
                f"Username: @{username}\n"
                f"ID: {uid}\n"
                f"Joined: {joined_at}\n"
                f"Plan: {plan}\n"
                f"Payments: Approved: {payments['approved']} | Pending: {payments['pending']}"
            )
            builder = InlineKeyboardBuilder()
            builder.button(text="💎 Get Premium", callback_data="btn_get_premium")
            builder.button(text="🏠 Home", callback_data="btn_home")
            builder.adjust(2)
            msg = await bot.send_message(chat_id=callback.message.chat.id, text=text, reply_markup=builder.as_markup())
            engine.track(slug, callback.message.chat.id, msg.message_id)

        @dp.callback_query(F.data.startswith("buy_plan:"))
        async def process_plan_selection(callback: types.CallbackQuery, state: FSMContext):
            await callback.answer()
            try:
                await callback.message.delete()
            except Exception:
                pass
            bot = engine.active_bots.get(slug)
            if not bot:
                return

            pid = callback.data.split(":")[1]
            plan = await get_tenant_plan(slug, pid)
            if not plan:
                return
            _, plan_name, amount, validity, _ = plan
            qr_buf = await generate_tenant_upi_qr(slug, plan_name, amount)
            photo_file = BufferedInputFile(qr_buf.getvalue(), filename="qr.png")
            upi_id = await get_tenant_setting(slug, "upi_id")
            payee = await get_tenant_setting(slug, "payee_name")
            formatted_price = f"₹{amount:.2f}"

            caption = (
                "📲 UPI PAYMENT\n\n"
                "━━━━━━━━━━━━━━━━━\n"
                f"📦 PLAN: {plan_name}\n"
                f"💰 AMOUNT: {formatted_price}\n"
                f"⏳ VALITY: {validity}\n"
                "━━━━━━━━━━━━━━━━━\n\n"
                f"👤 NAME: {payee}\n"
                f"📱 UPI ID: <code>{upi_id}</code>\n\n"
                "📋 STEPS:\n"
                "1️⃣ SCAN THE QR CODE ABOVE\n"
                f"2️⃣ {formatted_price} AMOUNT AND NOTE WILL BE BILLED AUTOMATICALLY\n"
                "3️⃣ ENTER YOUR PIN AND COMPLETE PAYMENT\n"
                "4️⃣ TAKE A SCREENSHOT AND CLICK CHECK PAYMENT ✅"
            )

            await state.update_data(current_plan=plan_name, current_amount=amount)
            msg = await bot.send_photo(
                chat_id=callback.message.chat.id,
                photo=photo_file,
                caption=caption,
                parse_mode="HTML",
                reply_markup=get_upi_card_keyboard(),
            )
            engine.track(slug, callback.message.chat.id, msg.message_id)

        @dp.callback_query(F.data == "check_payment")
        async def handle_check_payment(callback: types.CallbackQuery, state: FSMContext):
            await callback.answer()
            try:
                await callback.message.delete()
            except Exception:
                pass
            bot = engine.active_bots.get(slug)
            if not bot:
                return
            await state.set_state(PaymentStates.waiting_for_screenshot)

            text = (
                "📸 SEND PAYMENT SCREENSHOT\n\n"
                "✅ SEND THE SCREENSHOT HERE AFTER COMPLITING UPI PAYMENT.\n\n"
                "⚠️ ONLINE AN IMAGE OR SCREENSHOT IS ACCEPTED\n\n"
                "Type /cancel to abort."
            )
            msg = await bot.send_message(chat_id=callback.message.chat.id, text=text)
            engine.track(slug, callback.message.chat.id, msg.message_id)

        @dp.message(PaymentStates.waiting_for_screenshot, F.photo)
        async def process_payment_proof(message: types.Message, state: FSMContext):
            bot = engine.active_bots.get(slug)
            if not bot:
                return
            data = await state.get_data()
            plan_name = data.get("current_plan", "Unknown Plan")
            amount = data.get("current_amount", 0)

            async with TENANT_DB_LOCKS[slug]:
                db = await get_tenant_db(slug)
                try:
                    cur = await db.execute("INSERT INTO payments (user_id, plan_name, amount) VALUES (?, ?, ?)", (message.from_user.id, plan_name, amount))
                    pid = cur.lastrowid
                    await db.commit()
                finally:
                    await db.close()

            await state.clear()
            msg = await message.answer("✅ Screenshot Received! Verification is in progress.", reply_markup=get_home_keyboard())
            engine.track(slug, message.chat.id, msg.message_id)

            builder = InlineKeyboardBuilder()
            builder.button(text="✅ Approve", callback_data=f"adm_pay:{pid}:approved")
            builder.button(text="❌ Reject", callback_data=f"adm_pay:{pid}:rejected")
            builder.adjust(2)

            caption = (
                f"🔔 <b>New Payment Screenshot Received</b>\n\n"
                f"<b>Order ID:</b> #{pid}\n"
                f"<b>User ID:</b> <code>{message.from_user.id}</code>\n"
                f"<b>Username:</b> @{message.from_user.username or 'N/A'}\n"
                f"<b>Plan:</b> {plan_name}\n"
                f"<b>Amount:</b> Rs.{amount}"
            )

            photo_file_id = message.photo[-1].file_id
            admin_ids = await get_tenant_admin_ids(slug)
            for admin_id in admin_ids:
                try:
                    await bot.send_photo(chat_id=admin_id, photo=photo_file_id, caption=caption, reply_markup=builder.as_markup())
                except Exception as e:
                    logging.error(f"[{slug}] Failed to send proof to admin {admin_id}: {e}")
                    try:
                        await bot.send_message(chat_id=admin_id, text=f"{caption}\n\n⚠️ <i>(Screenshot could not be loaded directly)</i>", reply_markup=builder.as_markup())
                    except Exception:
                        pass

        @dp.message(PaymentStates.waiting_for_screenshot)
        async def invalid_proof(message: types.Message, state: FSMContext):
            if message.text == "/cancel":
                await state.clear()
                await engine.send_welcome_flow(slug, message.chat.id)
                return
            msg = await message.answer("⚠️ ONLINE AN IMAGE OR SCREENSHOT IS ACCEPTED\n\nPlease upload your payment screenshot image.")
            engine.track(slug, message.chat.id, message.message_id)

        @dp.callback_query(F.data.startswith("adm_pay:"))
        async def handle_admin_pay_approval(callback: types.CallbackQuery):
            bot = engine.active_bots.get(slug)
            if not bot:
                return
            admin_ids = await get_tenant_admin_ids(slug)
            if callback.from_user.id in admin_ids:
                _, pid, act = callback.data.split(":")
                async with TENANT_DB_LOCKS[slug]:
                    db = await get_tenant_db(slug)
                    try:
                        async with db.execute("SELECT user_id, plan_name FROM payments WHERE id=?", (int(pid),)) as cur:
                            p = await cur.fetchone()
                        if not p:
                            await callback.answer("Not found.")
                            return
                        t_uid, pl = p
                        if act == "approved":
                            await db.execute("UPDATE payments SET status='approved' WHERE id=?", (int(pid),))
                            await db.execute("UPDATE users SET premium_status=? WHERE user_id=?", (pl, t_uid))
                            await db.commit()
                            await engine.notify_payment_approved(slug, t_uid, pl)
                            await callback.message.edit_caption(caption=callback.message.caption + "\n\nSTATUS: APPROVED")
                        else:
                            await db.execute("UPDATE payments SET status='rejected' WHERE id=?", (int(pid),))
                            await db.commit()
                            try:
                                await bot.send_message(t_uid, "Payment verification failed.")
                            except Exception:
                                pass
                            await callback.message.edit_caption(caption=callback.message.caption + "\n\nSTATUS: REJECTED")
                    finally:
                        await db.close()
                await callback.answer("Status updated.")

bot_engine = MultiBotEngine()

# ==========================================
# FASTAPI APPLICATION LIFECYCLE
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_master_db()
    
    # Auto-spawn bots for all active clients
    db = await get_master_db()
    try:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT slug, bot_token, expiry_date, status FROM clients") as cur:
            clients = await cur.fetchall()
        today = datetime.now().date()
        for c in clients:
            exp_date = datetime.strptime(c["expiry_date"], "%Y-%m-%d").date()
            if c["status"] == "ACTIVE" and exp_date >= today:
                await init_tenant_db(c["slug"], c["bot_token"])
                if c["bot_token"]:
                    await bot_engine.start_tenant_bot(c["slug"], c["bot_token"])
    finally:
        await db.close()

    yield

    for slug in list(bot_engine.running_tasks.keys()):
        await bot_engine.stop_tenant_bot(slug)

app = FastAPI(title="Master SaaS Multi-Tenant Bot Engine", lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# ==========================================
# MASTER CONSOLE TEMPLATES
# ==========================================
MASTER_LOGIN_PAGE = """<!DOCTYPE html>
<html>
<head>
    <title>Master Access &mdash; Reseller Cloud</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-[#070b14] text-slate-100 flex items-center justify-center min-h-screen p-4">
    <div class="bg-[#0f172a] border border-blue-600/30 p-8 rounded-2xl w-full max-w-sm shadow-[0_0_30px_rgba(37,99,235,0.2)]">
        <h2 class="text-xl font-bold mb-1 text-white">⚡ MASTER CONSOLE</h2>
        <p class="text-xs text-blue-400 mb-6 font-mono">Multi-Tenant Bot Control &amp; Provisioning</p>
        {% if error %}<div class="bg-rose-500/20 text-rose-300 text-xs p-3 rounded-lg mb-4">{{ error }}</div>{% endif %}
        <form method="POST" action="/master/login" class="space-y-4">
            <div>
                <label class="text-xs text-slate-400 block mb-1">Master User</label>
                <input type="text" name="username" required class="w-full bg-[#070b14] border border-slate-700 rounded-lg p-2.5 text-sm text-white focus:outline-none focus:border-blue-500">
            </div>
            <div>
                <label class="text-xs text-slate-400 block mb-1">Master Password</label>
                <input type="password" name="password" required class="w-full bg-[#070b14] border border-slate-700 rounded-lg p-2.5 text-sm text-white focus:outline-none focus:border-blue-500">
            </div>
            <button type="submit" class="w-full bg-blue-600 hover:bg-blue-500 font-bold text-sm py-2.5 rounded-lg text-white transition shadow-lg shadow-blue-600/30">Authenticate</button>
        </form>
    </div>
</body>
</html>"""

MASTER_DASHBOARD_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>All Clients &mdash; Master Panel</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>body { font-family: 'Plus Jakarta Sans', sans-serif; background-color: #060913; }</style>
</head>
<body class="text-slate-100 flex min-h-screen">
    <aside class="w-60 bg-[#0d1322] border-r border-slate-800/80 p-5 flex flex-col justify-between hidden md:flex">
        <div class="space-y-6">
            <div class="flex items-center gap-2.5">
                <div class="w-7 h-7 rounded-lg bg-blue-600 flex items-center justify-center font-bold text-sm">🐺</div>
                <div>
                    <h3 class="font-bold text-sm text-white tracking-wide">MASTER PANEL</h3>
                    <span class="text-[10px] text-emerald-400 flex items-center gap-1 font-mono">● ONLINE PROVISIONER</span>
                </div>
            </div>
            <nav class="space-y-1.5 text-xs font-semibold">
                <a href="/master/clients" class="flex items-center gap-2 px-3 py-2.5 rounded-xl bg-blue-600/20 text-blue-400 border border-blue-500/30">👥 All Clients</a>
                <a href="/master/client/new" class="flex items-center gap-2 px-3 py-2.5 rounded-xl text-slate-400 hover:bg-slate-800/50 hover:text-white transition">➕ New Panel</a>
            </nav>
        </div>
        <div>
            <a href="/master/logout" class="block text-center text-xs text-rose-400 hover:bg-rose-500/10 py-2 rounded-lg font-mono">Log Out</a>
        </div>
    </aside>

    <main class="flex-1 p-6 md:p-8 overflow-y-auto">
        <div class="flex items-center justify-between mb-6">
            <h1 class="text-xl font-bold text-white tracking-tight">All Clients</h1>
            <a href="/master/client/new" class="bg-blue-600 hover:bg-blue-500 px-4 py-2 rounded-xl text-xs font-bold transition shadow-lg shadow-blue-600/20">+ New Client</a>
        </div>

        <div class="bg-[#0d1322] border border-slate-800/80 rounded-2xl overflow-hidden shadow-xl">
            <div class="overflow-x-auto">
                <table class="w-full text-left text-xs font-mono">
                    <thead class="bg-[#090d18] text-slate-400 uppercase text-[10px] border-b border-slate-800">
                        <tr>
                            <th class="py-3 px-4">Client</th>
                            <th class="py-3 px-4">Make Date</th>
                            <th class="py-3 px-4">Expiry</th>
                            <th class="py-3 px-4">Days Left</th>
                            <th class="py-3 px-4">Price</th>
                            <th class="py-3 px-4">Status</th>
                            <th class="py-3 px-4">Admin URL</th>
                            <th class="py-3 px-4 text-right">Actions</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-800/60 text-slate-300">
                        {% for c in clients %}
                        <tr class="hover:bg-slate-800/30 transition">
                            <td class="py-3.5 px-4">
                                <span class="font-bold text-white block">{{ c.client_name }}</span>
                                <span class="text-[10px] text-blue-400">@{{ c.telegram or 'No Telegram' }}</span>
                            </td>
                            <td class="py-3.5 px-4 text-slate-400">{{ c.make_date }}</td>
                            <td class="py-3.5 px-4 text-slate-400">{{ c.expiry_date }}</td>
                            <td class="py-3.5 px-4 font-bold {{ 'text-emerald-400' if c.days_left > 5 else ('text-amber-400' if c.days_left > 0 else 'text-rose-400') }}">
                                {{ c.days_left }} days left
                            </td>
                            <td class="py-3.5 px-4 font-semibold text-white">₹{{ c.price }}</td>
                            <td class="py-3.5 px-4">
                                <span class="px-2 py-0.5 rounded text-[10px] uppercase font-bold {{ 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30' if c.status == 'ACTIVE' and c.days_left > 0 else 'bg-rose-500/20 text-rose-400 border border-rose-500/30' }}">
                                    {{ 'ACTIVE' if c.status == 'ACTIVE' and c.days_left > 0 else 'EXPIRED' }}
                                </span>
                            </td>
                            <td class="py-3.5 px-4">
                                <a href="/c/{{ c.slug }}/admin" target="_blank" class="text-cyan-400 hover:underline flex items-center gap-1">
                                    /c/{{ c.slug }}/admin ↗
                                </a>
                            </td>
                            <td class="py-3.5 px-4 text-right">
                                <form method="POST" action="/master/client/delete" onsubmit="return confirm('Delete client completely? Isolated database will be removed.');">
                                    <input type="hidden" name="client_id" value="{{ c.id }}">
                                    <button type="submit" class="bg-rose-500/10 hover:bg-rose-600 text-rose-400 hover:text-white px-2.5 py-1 rounded-lg text-[10px] transition">
                                        Delete
                                    </button>
                                </form>
                            </td>
                        </tr>
                        {% else %}
                        <tr><td colspan="8" class="text-center py-10 text-slate-500 font-sans">No client panels deployed yet. Click "+ New Client" above.</td></tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>
    </main>
</body>
</html>"""

MASTER_NEW_CLIENT_PAGE = """<!DOCTYPE html>
<html>
<head>
    <title>Create New Panel &mdash; Master Console</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-[#060913] text-slate-100 flex min-h-screen p-6 md:p-12 items-center justify-center">
    <div class="bg-[#0d1322] border border-slate-800 rounded-2xl max-w-xl w-full p-8 shadow-2xl space-y-6">
        <div class="flex items-center justify-between border-b border-slate-800 pb-4">
            <div>
                <h2 class="text-lg font-bold text-white">Create New Client Panel</h2>
                <p class="text-xs text-blue-400 font-mono">Provisions isolated database &amp; bot runner</p>
            </div>
            <a href="/master/clients" class="text-xs text-slate-400 hover:text-white">&larr; Back</a>
        </div>

        <form method="POST" action="/master/client/new" class="space-y-4 text-xs font-mono">
            <div class="grid grid-cols-2 gap-4">
                <div>
                    <label class="text-slate-400 block mb-1">CLIENT NAME *</label>
                    <input type="text" name="client_name" required placeholder="e.g. Rahul VIP" class="w-full bg-[#060913] border border-slate-700 rounded-lg p-2.5 text-white">
                </div>
                <div>
                    <label class="text-slate-400 block mb-1">TELEGRAM USERNAME</label>
                    <input type="text" name="telegram" placeholder="@client_username" class="w-full bg-[#060913] border border-slate-700 rounded-lg p-2.5 text-white">
                </div>
            </div>

            <div class="grid grid-cols-2 gap-4">
                <div>
                    <label class="text-slate-400 block mb-1">FOLDER / URL SLUG *</label>
                    <input type="text" name="slug" required placeholder="rahul-deals" class="w-full bg-[#060913] border border-slate-700 rounded-lg p-2.5 text-white">
                </div>
                <div>
                    <label class="text-slate-400 block mb-1">PLAN DURATION (DAYS)</label>
                    <input type="number" name="plan_days" value="30" required class="w-full bg-[#060913] border border-slate-700 rounded-lg p-2.5 text-white">
                </div>
            </div>

            <div class="grid grid-cols-2 gap-4">
                <div>
                    <label class="text-slate-400 block mb-1">SELL PRICE (₹)</label>
                    <input type="number" name="price" value="199" required class="w-full bg-[#060913] border border-slate-700 rounded-lg p-2.5 text-white">
                </div>
                <div>
                    <label class="text-slate-400 block mb-1">TELEGRAM BOT TOKEN (OPTIONAL)</label>
                    <input type="text" name="bot_token" placeholder="123456:ABC-DEF..." class="w-full bg-[#060913] border border-slate-700 rounded-lg p-2.5 text-white">
                </div>
            </div>

            <div class="border-t border-slate-800 pt-4 grid grid-cols-2 gap-4">
                <div>
                    <label class="text-slate-400 block mb-1">ADMIN USERNAME</label>
                    <input type="text" name="admin_username" value="admin" required class="w-full bg-[#060913] border border-slate-700 rounded-lg p-2.5 text-white">
                </div>
                <div>
                    <label class="text-slate-400 block mb-1">ADMIN PASSWORD</label>
                    <input type="text" name="admin_password" placeholder="Passcode (min 6 chars)" required class="w-full bg-[#060913] border border-slate-700 rounded-lg p-2.5 text-white">
                </div>
            </div>

            <button type="submit" class="w-full bg-blue-600 hover:bg-blue-500 font-bold py-3 rounded-xl text-white uppercase tracking-wider text-xs transition">
                Create &amp; Provision Panel
            </button>
        </form>
    </div>
</body>
</html>"""

# ==========================================
# MASTER CONTROLLERS & ENDPOINTS
# ==========================================
def verify_master_auth(request: Request):
    if not request.session.get("is_master_authenticated"):
        raise HTTPException(status_code=status.HTTP_303_SEE_OTHER, headers={"Location": "/master/login"})

@app.get("/master/login", response_class=HTMLResponse)
async def master_login_view(error: str | None = None):
    return HTMLResponse(Template(MASTER_LOGIN_PAGE).render(error=error))

@app.post("/master/login")
async def master_login_post(request: Request, username: str = Form(...), password: str = Form(...)):
    if username == MASTER_USER and password == MASTER_PASS:
        request.session["is_master_authenticated"] = True
        return RedirectResponse(url="/master/clients", status_code=status.HTTP_303_SEE_OTHER)
    return HTMLResponse(Template(MASTER_LOGIN_PAGE).render(error="Incorrect credentials."), status_code=401)

@app.get("/master/logout")
async def master_logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/master/login", status_code=status.HTTP_303_SEE_OTHER)

@app.get("/master/clients", response_class=HTMLResponse)
async def master_clients_dashboard(request: Request):
    verify_master_auth(request)
    today = datetime.now().date()
    db = await get_master_db()
    try:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM clients ORDER BY id DESC") as cur:
            rows = await cur.fetchall()
        
        clients = []
        for r in rows:
            c = dict(r)
            exp_date = datetime.strptime(c["expiry_date"], "%Y-%m-%d").date()
            c["days_left"] = max(0, (exp_date - today).days)
            clients.append(c)
    finally:
        await db.close()

    return HTMLResponse(Template(MASTER_DASHBOARD_PAGE).render(clients=clients))

@app.get("/master/client/new", response_class=HTMLResponse)
async def master_new_client_form(request: Request):
    verify_master_auth(request)
    return HTMLResponse(MASTER_NEW_CLIENT_PAGE)

@app.post("/master/client/new")
async def master_create_client(
    request: Request,
    client_name: str = Form(...),
    telegram: str = Form(""),
    slug: str = Form(...),
    plan_days: int = Form(30),
    price: float = Form(0.0),
    bot_token: str = Form(""),
    admin_username: str = Form(...),
    admin_password: str = Form(...),
):
    verify_master_auth(request)
    clean_slug = slug.strip().lower().replace(" ", "-")
    today = datetime.now().date()
    expiry = (today + timedelta(days=plan_days)).strftime("%Y-%m-%d")

    async with MASTER_DB_LOCK:
        db = await get_master_db()
        try:
            await db.execute("""
                INSERT INTO clients (client_name, telegram, slug, make_date, expiry_date, plan_days, price, admin_username, admin_password, bot_token)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (client_name, telegram, clean_slug, today.strftime("%Y-%m-%d"), expiry, plan_days, price, admin_username, admin_password, bot_token.strip()))
            await db.commit()
        except aiosqlite.IntegrityError:
            raise HTTPException(status_code=400, detail="Slug already in use. Please select another.")
        finally:
            await db.close()

    await init_tenant_db(clean_slug, bot_token.strip())
    if bot_token.strip():
        await bot_engine.start_tenant_bot(clean_slug, bot_token.strip())

    return RedirectResponse(url="/master/clients", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/master/client/delete")
async def master_delete_client(request: Request, client_id: int = Form(...)):
    verify_master_auth(request)
    db = await get_master_db()
    slug = None
    try:
        async with db.execute("SELECT slug FROM clients WHERE id = ?", (client_id,)) as cur:
            row = await cur.fetchone()
            if row:
                slug = row[0]
        if slug:
            await bot_engine.stop_tenant_bot(slug)
            await db.execute("DELETE FROM clients WHERE id = ?", (client_id,))
            await db.commit()
            db_path = get_tenant_db_path(slug)
            if os.path.exists(db_path):
                os.remove(db_path)
    finally:
        await db.close()

    return RedirectResponse(url="/master/clients", status_code=status.HTTP_303_SEE_OTHER)

# ==========================================
# DEDICATED CLIENT ADMIN ROUTING
# ==========================================
async def get_tenant_meta(slug: str):
    db = await get_master_db()
    try:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM clients WHERE slug = ?", (slug,)) as cur:
            return await cur.fetchone()
    finally:
        await db.close()

@app.get("/c/{slug}/login", response_class=HTMLResponse)
async def tenant_login_page(slug: str, error: str | None = None):
    client = await get_tenant_meta(slug)
    if not client:
        raise HTTPException(status_code=404, detail="Store panel does not exist.")

    # Inlined Cyberpunk Login matching your original design
    return HTMLResponse(f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Admin Login &mdash; {client['client_name']}</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@600;700;800;900&family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body {{ font-family: 'Plus Jakarta Sans', sans-serif; background-color: #030108; }}
        .font-tech {{ font-family: 'Orbitron', monospace; }}
        .exact-login-card {{
            background: linear-gradient(180deg, rgba(16, 12, 34, 0.94) 0%, rgba(10, 8, 22, 0.96) 100%);
            border: 1px solid rgba(168, 85, 247, 0.5);
            box-shadow: 0 0 28px rgba(168, 85, 247, 0.35), 0 0 70px rgba(168, 85, 247, 0.15);
            border-radius: 26px;
        }}
        .custom-input {{
            background-color: #080613;
            border: 1px solid rgba(147, 51, 234, 0.25);
            transition: all 0.2s ease;
        }}
        .custom-input:focus {{
            outline: none;
            border-color: #38bdf8;
            box-shadow: 0 0 12px rgba(56, 189, 248, 0.3);
        }}
    </style>
</head>
<body class="text-slate-100 min-h-screen flex items-center justify-center p-4 relative overflow-hidden">
    <div class="w-full max-w-[370px] relative z-10">
        <div class="exact-login-card p-8 space-y-6">
            <div class="space-y-1">
                <div class="flex items-center gap-2.5">
                    <svg class="w-6 h-6 text-fuchsia-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/>
                        <path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/>
                        <path d="M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0"/>
                        <path d="M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5"/>
                    </svg>
                    <h1 class="text-xl font-bold tracking-tight bg-clip-text text-transparent bg-gradient-to-r from-purple-300 via-fuchsia-300 to-cyan-300">
                        {client['client_name']}
                    </h1>
                </div>
                <div class="font-tech text-[10px] tracking-[0.25em] text-cyan-400/90 font-bold uppercase pl-8">
                    ADMIN PANEL
                </div>
            </div>

            {'<div class="p-3 rounded-xl bg-rose-500/10 border border-rose-500/30 text-rose-400 text-xs font-mono">' + error + '</div>' if error else ''}

            <form method="POST" action="/c/{slug}/login" class="space-y-4 pt-1">
                <div>
                    <label class="block text-xs font-medium text-slate-300 mb-2">Username</label>
                    <input type="text" name="username" required autofocus class="custom-input w-full h-11 rounded-xl px-4 text-sm text-white">
                </div>
                <div>
                    <label class="block text-xs font-medium text-slate-300 mb-2">Password</label>
                    <input type="password" name="password" required class="custom-input w-full h-11 rounded-xl px-4 text-sm text-white">
                </div>
                <button type="submit" class="w-full h-11 mt-3 bg-gradient-to-r from-purple-500 via-fuchsia-500 to-cyan-400 hover:opacity-95 text-white font-semibold rounded-xl text-sm transition shadow-lg shadow-purple-600/30">
                    Sign in &rarr;
                </button>
            </form>
        </div>
    </div>
</body>
</html>""")

@app.post("/c/{slug}/login")
async def tenant_login_post(slug: str, request: Request, username: str = Form(...), password: str = Form(...)):
    client = await get_tenant_meta(slug)
    if not client:
        raise HTTPException(status_code=404, detail="Store panel not found.")

    if username == client["admin_username"] and password == client["admin_password"]:
        request.session[f"tenant_auth_{slug}"] = True
        return RedirectResponse(url=f"/c/{slug}/admin", status_code=status.HTTP_303_SEE_OTHER)

    return RedirectResponse(url=f"/c/{slug}/login?error=Invalid+credentials", status_code=status.HTTP_303_SEE_OTHER)

@app.get("/c/{slug}/logout")
async def tenant_logout(slug: str, request: Request):
    request.session.pop(f"tenant_auth_{slug}", None)
    return RedirectResponse(url=f"/c/{slug}/login", status_code=status.HTTP_303_SEE_OTHER)

# ==========================================
# CLIENT DASHBOARD & API CONTROLLERS
# ==========================================
@app.get("/c/{slug}/admin", response_class=HTMLResponse)
async def tenant_admin_panel(
    slug: str,
    request: Request,
    message: str | None = None,
    page: int = 1,
    user_search: str = "",
):
    if not request.session.get(f"tenant_auth_{slug}"):
        return RedirectResponse(url=f"/c/{slug}/login", status_code=status.HTTP_303_SEE_OTHER)

    client = await get_tenant_meta(slug)
    if not client:
        raise HTTPException(status_code=404, detail="Store not found.")

    exp_date = datetime.strptime(client["expiry_date"], "%Y-%m-%d").date()
    if exp_date < datetime.now().date():
        return HTMLResponse(f"""
            <div style="background:#070b14;color:#fff;font-family:sans-serif;height:100vh;display:flex;flex-direction:column;justify-content:center;align-items:center;">
                <h1 style="color:#f43f5e;font-size:24px;margin-bottom:8px;">Panel Subscription Expired</h1>
                <p style="color:#94a3b8;">Your plan expired on {client['expiry_date']}. Please contact @{client['telegram']} to renew access.</p>
            </div>
        """)

    metrics = await get_tenant_dashboard_metrics(slug)

    limit = 50
    page = max(1, page)
    offset = (page - 1) * limit
    users_list, users_total = await get_tenant_paginated_users(slug, limit=limit, offset=offset, search=user_search)
    total_pages = max(1, math.ceil(users_total / limit))

    context = {
        "message": message,
        "admin_username": client["admin_username"],
        "is_online": slug in bot_engine.active_bots,
        "paid_orders": metrics.get("paid_orders", 0),
        "revenue": metrics.get("revenue", "0.00"),
        "total_users": metrics.get("total_users", 0),
        "recent_orders": metrics.get("recent_orders", []),
        "all_orders": metrics.get("all_orders", []),
        "users_list": users_list or [],
        "users_total": users_total,
        "current_page": page,
        "total_pages": total_pages,
        "user_search": user_search,
        "bot_token": await get_tenant_setting(slug, "bot_token"),
        "admin_chat_id": await get_tenant_setting(slug, "admin_chat_id"),
        "upi_id": await get_tenant_setting(slug, "upi_id"),
        "payee_name": await get_tenant_setting(slug, "payee_name"),
        "maintenance": await get_tenant_setting(slug, "maintenance"),
        "welcome_text": await get_tenant_setting(slug, "welcome_text"),
        "plans_text": await get_tenant_setting(slug, "plans_text"),
        "welcome_photo": await get_tenant_setting(slug, "welcome_photo"),
        "demo_video": await get_tenant_setting(slug, "demo_video"),
        "plans": await get_tenant_all_plans(slug),
    }

    # Dynamically scope the exact HTML dashboard forms to this specific tenant slug
    scoped_html = (
        DASHBOARD_PAGE
        .replace('action="/admin/', f'action="/c/{slug}/admin/')
        .replace("action='/admin/", f"action='/c/{slug}/admin/")
        .replace("fetch('/admin/", f"fetch('/c/{slug}/admin/")
        .replace('href="/logout"', f'href="/c/{slug}/logout"')
        .replace('action="/logout"', f'action="/c/{slug}/logout"')
        .replace('href="/?', f'href="/c/{slug}/admin?')
        .replace('action="/"', f'action="/c/{slug}/admin"')
    )
    tmpl = Template(scoped_html)
    rendered = await asyncio.to_thread(tmpl.render, **context)
    return HTMLResponse(content=rendered)

@app.post("/c/{slug}/admin/save")
async def tenant_save_general_settings(
    slug: str,
    request: Request,
    bot_token: str = Form(...),
    welcome_text: str = Form(...),
    plans_text: str = Form(...),
    welcome_photo: str = Form(...),
    demo_video: str = Form(...),
):
    if not request.session.get(f"tenant_auth_{slug}"):
        return RedirectResponse(url=f"/c/{slug}/login", status_code=status.HTTP_303_SEE_OTHER)

    cleaned_token = bot_token.strip()
    await update_tenant_setting(slug, "bot_token", cleaned_token)
    await update_tenant_setting(slug, "welcome_text", welcome_text.strip())
    await update_tenant_setting(slug, "plans_text", plans_text.strip())
    await update_tenant_setting(slug, "welcome_photo", welcome_photo.strip())
    await update_tenant_setting(slug, "demo_video", demo_video.strip())

    async with MASTER_DB_LOCK:
        db = await get_master_db()
        try:
            await db.execute("UPDATE clients SET bot_token = ? WHERE slug = ?", (cleaned_token, slug))
            await db.commit()
        finally:
            await db.close()

    if cleaned_token:
        await bot_engine.start_tenant_bot(slug, cleaned_token)

    return RedirectResponse(url=f"/c/{slug}/admin?message=Settings+Saved+Successfully&tab=tab-bot", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/settings/upi")
async def tenant_save_upi(
    slug: str,
    request: Request,
    upi_id: str = Form(...),
    payee_name: str = Form(...),
    admin_chat_id: str = Form(""),
):
    if not request.session.get(f"tenant_auth_{slug}"):
        return RedirectResponse(url=f"/c/{slug}/login", status_code=status.HTTP_303_SEE_OTHER)

    cleaned_upi = upi_id.strip()
    cleaned_name = payee_name.strip()
    if not cleaned_name:
        detected = await detect_payee_name_from_upi(cleaned_upi)
        cleaned_name = detected if detected else "Merchant"

    await update_tenant_setting(slug, "upi_id", cleaned_upi)
    await update_tenant_setting(slug, "payee_name", cleaned_name)
    await update_tenant_setting(slug, "admin_chat_id", admin_chat_id.strip())

    return RedirectResponse(url=f"/c/{slug}/admin?message=UPI+Saved+Successfully&tab=tab-settings", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/password/update")
async def tenant_update_password(
    slug: str,
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
):
    if not request.session.get(f"tenant_auth_{slug}"):
        return RedirectResponse(url=f"/c/{slug}/login", status_code=status.HTTP_303_SEE_OTHER)

    client = await get_tenant_meta(slug)
    if current_password != client["admin_password"]:
        return RedirectResponse(url=f"/c/{slug}/admin?message=Error:+Current+password+incorrect&tab=tab-settings", status_code=status.HTTP_303_SEE_OTHER)

    async with MASTER_DB_LOCK:
        db = await get_master_db()
        try:
            await db.execute("UPDATE clients SET admin_password = ? WHERE slug = ?", (new_password.strip(), slug))
            await db.commit()
        finally:
            await db.close()

    return RedirectResponse(url=f"/c/{slug}/admin?message=Password+updated+successfully&tab=tab-settings", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/broadcast/send")
async def tenant_broadcast_send(
    slug: str,
    request: Request,
    broadcast_message: str = Form(...),
    broadcast_photo: str = Form(""),
):
    if not request.session.get(f"tenant_auth_{slug}"):
        return RedirectResponse(url=f"/c/{slug}/login", status_code=status.HTTP_303_SEE_OTHER)

    bot = bot_engine.active_bots.get(slug)
    if not bot:
        return RedirectResponse(url=f"/c/{slug}/admin?message=Bot+is+offline.+Add+token+first&tab=tab-broadcast", status_code=status.HTTP_303_SEE_OTHER)

    db = await get_tenant_db(slug)
    try:
        async with db.execute("SELECT user_id FROM users WHERE is_banned=0") as cur:
            users = await cur.fetchall()
    finally:
        await db.close()

    sent = 0
    cleaned_photo = broadcast_photo.strip()
    for (uid,) in users:
        try:
            if cleaned_photo:
                await bot.send_photo(chat_id=uid, photo=cleaned_photo, caption=broadcast_message)
            else:
                await bot.send_message(chat_id=uid, text=broadcast_message)
            sent += 1
            await asyncio.sleep(0.04)
        except Exception:
            pass

    return RedirectResponse(url=f"/c/{slug}/admin?message=Broadcast+delivered+to+{sent}+users!&tab=tab-broadcast", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/orders/status")
async def tenant_order_status_update(
    slug: str,
    request: Request,
    order_id: int = Form(...),
    action: str = Form(...),
):
    if not request.session.get(f"tenant_auth_{slug}"):
        return RedirectResponse(url=f"/c/{slug}/login", status_code=status.HTTP_303_SEE_OTHER)

    async with TENANT_DB_LOCKS[slug]:
        db = await get_tenant_db(slug)
        try:
            async with db.execute("SELECT user_id, plan_name FROM payments WHERE id = ?", (int(order_id),)) as cur:
                row = await cur.fetchone()
            if not row:
                return RedirectResponse(url=f"/c/{slug}/admin?tab=tab-orders", status_code=status.HTTP_303_SEE_OTHER)

            user_id, plan_name = row
            if action == "approved":
                await db.execute("UPDATE payments SET status = 'approved' WHERE id = ?", (int(order_id),))
                await db.execute("UPDATE users SET premium_status = ? WHERE user_id = ?", (plan_name, user_id))
                await db.commit()
                await bot_engine.notify_payment_approved(slug, user_id, plan_name)
            else:
                await db.execute("UPDATE payments SET status = 'rejected' WHERE id = ?", (int(order_id),))
                await db.commit()
                bot = bot_engine.active_bots.get(slug)
                if bot:
                    try:
                        await bot.send_message(user_id, "Payment verification failed. Your order has been rejected.")
                    except Exception:
                        pass
        finally:
            await db.close()

    return RedirectResponse(url=f"/c/{slug}/admin?tab=tab-orders", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/plans/add")
async def tenant_add_plan_endpoint(
    slug: str,
    request: Request,
    plan_id: str = Form(...),
    name: str = Form(...),
    amount: float = Form(...),
    validity: str = Form(...),
    access_link: str = Form(""),
):
    if not request.session.get(f"tenant_auth_{slug}"):
        return RedirectResponse(url=f"/c/{slug}/login", status_code=status.HTTP_303_SEE_OTHER)

    async with TENANT_DB_LOCKS[slug]:
        db = await get_tenant_db(slug)
        try:
            await db.execute(
                "INSERT INTO plans (plan_id, name, amount, validity, access_link) VALUES (?, ?, ?, ?, ?)",
                (plan_id.strip(), name.strip(), amount, validity.strip(), access_link.strip())
            )
            await db.commit()
        finally:
            await db.close()

    return RedirectResponse(url=f"/c/{slug}/admin?message=Plan+Added&tab=tab-plans", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/plans/update")
async def tenant_update_plan_endpoint(
    slug: str,
    request: Request,
    plan_id: str = Form(...),
    name: str = Form(...),
    amount: float = Form(...),
    validity: str = Form(...),
    access_link: str = Form(""),
):
    if not request.session.get(f"tenant_auth_{slug}"):
        return RedirectResponse(url=f"/c/{slug}/login", status_code=status.HTTP_303_SEE_OTHER)

    async with TENANT_DB_LOCKS[slug]:
        db = await get_tenant_db(slug)
        try:
            await db.execute(
                "UPDATE plans SET name = ?, amount = ?, validity = ?, access_link = ? WHERE plan_id = ?",
                (name.strip(), amount, validity.strip(), access_link.strip(), plan_id.strip())
            )
            await db.commit()
        finally:
            await db.close()

    return RedirectResponse(url=f"/c/{slug}/admin?message=Plan+Updated&tab=tab-plans", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/plans/delete")
async def tenant_delete_plan_endpoint(slug: str, request: Request, plan_id: str = Form(...)):
    if not request.session.get(f"tenant_auth_{slug}"):
        return RedirectResponse(url=f"/c/{slug}/login", status_code=status.HTTP_303_SEE_OTHER)

    async with TENANT_DB_LOCKS[slug]:
        db = await get_tenant_db(slug)
        try:
            await db.execute("DELETE FROM plans WHERE plan_id = ?", (plan_id.strip(),))
            await db.commit()
        finally:
            await db.close()

    return RedirectResponse(url=f"/c/{slug}/admin?message=Plan+Deleted&tab=tab-plans", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/users/ban")
async def tenant_ban_user(slug: str, request: Request, user_id: int = Form(...), status: int = Form(...)):
    if not request.session.get(f"tenant_auth_{slug}"):
        return RedirectResponse(url=f"/c/{slug}/login", status_code=status.HTTP_303_SEE_OTHER)

    async with TENANT_DB_LOCKS[slug]:
        db = await get_tenant_db(slug)
        try:
            await db.execute("UPDATE users SET is_banned = ? WHERE user_id = ?", (int(status), int(user_id)))
            await db.commit()
        finally:
            await db.close()

    return RedirectResponse(url=f"/c/{slug}/admin?tab=tab-users", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/users/subscription")
async def tenant_change_user_sub(slug: str, request: Request, user_id: int = Form(...), plan_name: str = Form("Free")):
    if not request.session.get(f"tenant_auth_{slug}"):
        return RedirectResponse(url=f"/c/{slug}/login", status_code=status.HTTP_303_SEE_OTHER)

    async with TENANT_DB_LOCKS[slug]:
        db = await get_tenant_db(slug)
        try:
            await db.execute("UPDATE users SET premium_status = ? WHERE user_id = ?", (plan_name.strip(), int(user_id)))
            await db.commit()
        finally:
            await db.close()

    bot = bot_engine.active_bots.get(slug)
    if bot:
        if plan_name == "Free":
            try:
                await bot.send_message(user_id, "Your premium subscription has ended. You are now on the Free tier.")
            except Exception:
                pass
        else:
            await bot_engine.notify_payment_approved(slug, user_id, plan_name)

    return RedirectResponse(url=f"/c/{slug}/admin?tab=tab-users", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/revenue/reset")
async def tenant_reset_revenue(slug: str, request: Request):
    if not request.session.get(f"tenant_auth_{slug}"):
        return RedirectResponse(url=f"/c/{slug}/login", status_code=status.HTTP_303_SEE_OTHER)

    async with TENANT_DB_LOCKS[slug]:
        db = await get_tenant_db(slug)
        try:
            await db.execute("DELETE FROM payments")
            await db.commit()
        finally:
            await db.close()

    return RedirectResponse(url=f"/c/{slug}/admin?message=Revenue+reset&tab=tab-dashboard", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/upload-demo-video")
async def tenant_upload_demo(slug: str, request: Request, files: list[UploadFile] = File(...)):
    if not request.session.get(f"tenant_auth_{slug}"):
        return JSONResponse({"status": "error", "message": "Unauthorized"}, status_code=401)

    try:
        base_url = str(request.base_url).rstrip("/")
        if "railway.app" in base_url and base_url.startswith("http://"):
            base_url = base_url.replace("http://", "https://")

        urls = []
        for f in files:
            name = f"{int(datetime.now().timestamp())}_{f.filename.replace(' ', '_')}"
            dest = os.path.join(UPLOAD_DIR, name)
            with open(dest, "wb") as buffer:
                shutil.copyfileobj(f.file, buffer)
            urls.append(f"{base_url}/static/uploads/{name}")

        curr = await get_tenant_setting(slug, "demo_video")
        updated = (curr.strip() + "\n" + "\n".join(urls)).strip()
        await update_tenant_setting(slug, "demo_video", updated)
        return JSONResponse({"status": "success", "urls": urls})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

@app.get("/")
async def root():
    return RedirectResponse(url="/master/login", status_code=status.HTTP_303_SEE_OTHER)

@app.get("/health")
async def health():
    return {"status": "ok", "active_bots": len(bot_engine.active_bots)}
