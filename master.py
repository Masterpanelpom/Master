# -*- coding: utf-8 -*-
import asyncio
import io
import json
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
from aiogram.filters import Command, CommandStart
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

async def update_tenant_plan(slug: str, plan_id: str, name: str, amount: float, validity: str, access_link: str = ""):
    async with TENANT_DB_LOCKS[slug]:
        db = await get_tenant_db(slug)
        try:
            await db.execute(
                "UPDATE plans SET name = ?, amount = ?, validity = ?, access_link = ? WHERE plan_id = ?",
                (name, amount, validity, access_link.strip(), plan_id),
            )
            await db.commit()
        finally:
            await db.close()

async def add_tenant_new_plan(slug: str, plan_id: str, name: str, amount: float, validity: str, access_link: str = ""):
    async with TENANT_DB_LOCKS[slug]:
        db = await get_tenant_db(slug)
        try:
            await db.execute(
                "INSERT INTO plans (plan_id, name, amount, validity, access_link) VALUES (?, ?, ?, ?, ?)",
                (plan_id, name, amount, validity, access_link.strip()),
            )
            await db.commit()
        finally:
            await db.close()

async def delete_tenant_plan(slug: str, plan_id: str):
    async with TENANT_DB_LOCKS[slug]:
        db = await get_tenant_db(slug)
        try:
            await db.execute("DELETE FROM plans WHERE plan_id = ?", (plan_id,))
            await db.commit()
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
                """
                SELECT COUNT(*) FROM users 
                WHERE CAST(user_id AS TEXT) LIKE ? OR username LIKE ? OR full_name LIKE ?
                """,
                (search_query, search_query, search_query)
            ) as cur:
                total_count = (await cur.fetchone())[0]

            async with db.execute(
                """
                SELECT user_id, full_name, username, joined_at, premium_status, is_banned 
                FROM users 
                WHERE CAST(user_id AS TEXT) LIKE ? OR username LIKE ? OR full_name LIKE ?
                ORDER BY joined_at DESC 
                LIMIT ? OFFSET ?
                """,
                (search_query, search_query, search_query, limit, offset),
            ) as cur:
                rows = await cur.fetchall()
        else:
            async with db.execute("SELECT COUNT(*) FROM users") as cur:
                total_count = (await cur.fetchone())[0]

            async with db.execute(
                """
                SELECT user_id, full_name, username, joined_at, premium_status, is_banned 
                FROM users 
                ORDER BY joined_at DESC 
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ) as cur:
                rows = await cur.fetchall()

        return rows, total_count
    finally:
        await db.close()

async def update_tenant_user_subscription(slug: str, user_id: int, plan_name: str):
    async with TENANT_DB_LOCKS[slug]:
        db = await get_tenant_db(slug)
        try:
            await db.execute("UPDATE users SET premium_status = ? WHERE user_id = ?", (plan_name, user_id))
            await db.commit()
        finally:
            await db.close()

async def set_tenant_user_ban_status(slug: str, user_id: int, is_banned: int):
    async with TENANT_DB_LOCKS[slug]:
        db = await get_tenant_db(slug)
        try:
            await db.execute("UPDATE users SET is_banned = ? WHERE user_id = ?", (int(is_banned), int(user_id)))
            await db.commit()
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
# UPI QR GENERATION & NPCI DETECTION
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
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Accept": "application/json"}
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
                    logging.error(f"[{slug}] Polling error: {e}. Retrying in 4s...")
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

        @dp.message(Command("cancel"))
        async def cancel_handler(message: types.Message, state: FSMContext):
            await state.clear()
            await message.answer("❌ Current operation cancelled.")
            await engine.send_welcome_flow(slug, message.chat.id)

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
                msg = await bot.send_message(chat_id=callback.message.chat.id, text="📺 No demo videos available right now.", reply_markup=get_demo_keyboard(0, 0))
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

        # Unified screenshot receiver supporting compressed photos and image files
        @dp.message(PaymentStates.waiting_for_screenshot, F.photo | (F.document & F.document.mime_type.startswith("image/")))
        async def process_payment_proof(message: types.Message, state: FSMContext):
            bot = engine.active_bots.get(slug)
            if not bot:
                return
            data = await state.get_data()
            plan_name = data.get("current_plan", "VIP Plan")
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

            file_id = message.photo[-1].file_id if message.photo else message.document.file_id
            admin_ids = await get_tenant_admin_ids(slug)
            if not admin_ids:
                logging.warning(f"[{slug}] No admin_chat_id configured! Notification skipped.")

            for admin_id in admin_ids:
                try:
                    await bot.send_photo(chat_id=admin_id, photo=file_id, caption=caption, reply_markup=builder.as_markup())
                except Exception as e:
                    logging.error(f"[{slug}] Failed to send proof to admin {admin_id}: {e}")
                    try:
                        await bot.send_message(chat_id=admin_id, text=f"{caption}\n\n⚠️ (Could not render photo)", reply_markup=builder.as_markup())
                    except Exception:
                        pass

        @dp.message(PaymentStates.waiting_for_screenshot)
        async def invalid_proof(message: types.Message, state: FSMContext):
            if message.text and message.text.strip().lower() in ["/cancel", "cancel"]:
                await state.clear()
                await message.answer("❌ Payment verification cancelled.")
                await engine.send_welcome_flow(slug, message.chat.id)
                return
            msg = await message.answer("⚠️ ONLINE AN IMAGE OR SCREENSHOT IS ACCEPTED\n\nPlease upload your payment screenshot image.\nType /cancel to abort.")
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

app = FastAPI(title="Nagato Panel Master Cloud", lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# ==========================================
# MASTER LOGIN PAGE (CYAN/BLUE INPUT LINES)
# ==========================================
MASTER_LOGIN_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Master Console &mdash; Nagato Panel</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@600;700;800;900&family=Sedgwick+Ave+Display&family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body {
            font-family: 'Plus Jakarta Sans', sans-serif;
            background-color: #03010a;
            background-image: radial-gradient(circle at 50% 20%, #170630 0%, #070212 60%, #010006 100%);
            min-height: 100vh;
            margin: 0;
            padding: 0;
        }
        .font-tech { font-family: 'Orbitron', monospace; }
        .font-brush-real {
            font-family: 'Sedgwick Ave Display', cursive;
            letter-spacing: 0.04em;
        }

        /* Outer Frame */
        .console-frame {
            background: linear-gradient(180deg, rgba(14, 8, 30, 0.96) 0%, rgba(6, 3, 16, 0.98) 100%);
            border: 2px solid #00f0ff;
            box-shadow: 0 0 25px rgba(0, 240, 255, 0.35), inset 0 0 20px rgba(168, 85, 247, 0.15);
            border-radius: 26px;
        }

        /* Lightning Crest Badge inside the box */
        .internal-crest {
            width: 48px;
            height: 48px;
            border-radius: 50%;
            background: radial-gradient(circle, #38bdf8 0%, #a855f7 70%, #150630 100%);
            border: 2px solid #ffffff;
            box-shadow: 0 0 16px #00f0ff, 0 0 25px rgba(236, 72, 153, 0.6);
            display: flex;
            align-items: center;
            justify-content: center;
        }

        /* Matching Cyan/Blue Input Boxes for Both Username & Password */
        .input-box-cyan {
            background-color: #080416;
            border: 1.5px solid #00f0ff;
            box-shadow: 0 0 10px rgba(0, 240, 255, 0.25), inset 0 0 8px rgba(0, 240, 255, 0.1);
            transition: all 0.2s ease;
        }
        .input-box-cyan:focus-within {
            border-color: #38bdf8;
            box-shadow: 0 0 15px rgba(56, 189, 248, 0.45), inset 0 0 10px rgba(56, 189, 248, 0.2);
        }

        /* Glow Button */
        .auth-glow-btn {
            background: linear-gradient(90deg, #ec4899 0%, #c026d3 50%, #00f0ff 100%);
            border: 1px solid rgba(255, 255, 255, 0.35);
            box-shadow: 0 0 20px rgba(236, 72, 153, 0.5), 0 0 30px rgba(0, 240, 255, 0.3);
            transition: all 0.2s ease;
        }
        .auth-glow-btn:hover {
            opacity: 0.95;
            box-shadow: 0 0 28px rgba(236, 72, 153, 0.7), 0 0 40px rgba(0, 240, 255, 0.45);
        }

        /* Integrated compact contact developer strip inside card */
        .compact-dev-strip {
            background-color: rgba(9, 5, 24, 0.85);
            border: 1px solid rgba(0, 240, 255, 0.35);
            box-shadow: 0 0 12px rgba(0, 240, 255, 0.1);
            transition: all 0.2s ease;
        }
        .compact-dev-strip:hover {
            border-color: #00f0ff;
            box-shadow: 0 0 18px rgba(0, 240, 255, 0.25);
        }
    </style>
</head>
<body class="flex flex-col items-center justify-center p-4 min-h-screen">

    <!-- Top Status Banner -->
    <div class="flex items-center gap-2 font-tech text-[9px] tracking-[0.25em] text-cyan-300 font-bold uppercase mb-4">
        <span>FAST</span> &bull; <span>SECURE</span> &bull; <span>RELIABLE</span>
        <span class="text-pink-500 font-bold tracking-widest pl-1">///</span>
    </div>

    <!-- Main Card Box -->
    <div class="w-full max-w-[360px]">
        <div class="console-frame py-6 px-6 space-y-4">
            
            <!-- Lightning Emblem (Contained inside the box) -->
            <div class="flex justify-center">
                <div class="internal-crest">
                    <svg class="w-6 h-6 text-white filter drop-shadow-[0_0_6px_#ffffff]" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/>
                    </svg>
                </div>
            </div>

            <!-- Dual-Tone Title: MASTER in Cyan, CONSOLE in Hot Pink -->
            <div class="text-center space-y-1">
                <h1 class="font-brush-real text-3xl leading-tight tracking-wide">
                    <span class="text-cyan-300 filter drop-shadow-[0_0_14px_rgba(0,240,255,0.9)] block">MASTER</span>
                    <span class="text-pink-500 filter drop-shadow-[0_0_14px_rgba(236,72,153,0.9)] block">CONSOLE</span>
                </h1>
                <p class="font-tech text-[8px] tracking-[0.22em] text-cyan-400 font-bold uppercase pt-0.5">
                    MULTI-TENANT BOT CONTROL &amp; PROVISIONING
                </p>
            </div>

            {% if error %}
            <div class="p-2 rounded-xl bg-rose-500/20 border border-rose-500/50 text-rose-300 text-xs font-mono text-center">
                {{ error }}
            </div>
            {% endif %}

            <!-- Login Form -->
            <form method="POST" action="/master/login" class="space-y-3.5 pt-1">
                
                <!-- Master User (Updated to Blue/Cyan Outline matching Password) -->
                <div class="space-y-1">
                    <label class="flex items-center gap-1.5 text-[10px] font-tech font-bold text-cyan-300 uppercase tracking-wider">
                        <svg class="w-3.5 h-3.5 text-cyan-400 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"/>
                        </svg>
                        MASTER USER
                    </label>
                    <div class="input-box-cyan rounded-xl flex items-center px-3 py-2.5 gap-2.5">
                        <svg class="w-4 h-4 text-cyan-400 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z"/>
                        </svg>
                        <input type="text" name="username" required autofocus placeholder="Enter your master username"
                               class="w-full bg-transparent text-xs text-white placeholder-slate-500 focus:outline-none font-mono">
                    </div>
                </div>

                <!-- Master Password (Blue Icons & Matching Blue/Cyan Outline) -->
                <div class="space-y-1">
                    <label class="flex items-center gap-1.5 text-[10px] font-tech font-bold text-cyan-300 uppercase tracking-wider">
                        <svg class="w-3.5 h-3.5 text-cyan-400 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"/>
                        </svg>
                        MASTER PASSWORD
                    </label>
                    <div class="input-box-cyan rounded-xl flex items-center px-3 py-2.5 gap-2.5">
                        <svg class="w-4 h-4 text-cyan-400 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"/>
                        </svg>
                        <input type="password" id="passField" name="password" required placeholder="Enter your password"
                               class="w-full bg-transparent text-xs text-white placeholder-slate-500 focus:outline-none font-mono">
                        <button type="button" onclick="togglePassVisibility()" class="text-cyan-400 hover:text-cyan-300 focus:outline-none">
                            <svg id="eyeIcon" class="w-4 h-4 text-cyan-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" />
                            </svg>
                        </button>
                    </div>
                </div>

                <!-- Authenticate Button -->
                <button type="submit" class="w-full auth-glow-btn font-tech font-bold text-xs uppercase py-3 rounded-xl text-white flex items-center justify-center gap-2 tracking-wider mt-1">
                    <svg class="w-4 h-4 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"/>
                    </svg>
                    <span>AUTHENTICATE &gt;</span>
                </button>
            </form>

            <!-- Compact Contact Developer Section INSIDE the Card -->
            <div class="pt-1">
                <a href="https://t.me/NAGATOxOWNER" target="_blank" rel="noopener noreferrer"
                   class="group compact-dev-strip flex items-center justify-between px-3 py-2 rounded-xl">
                    <div class="flex items-center gap-2">
                        <svg class="w-4 h-4 text-cyan-400" fill="currentColor" viewBox="0 0 24 24">
                            <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm4.64 6.8c-.15 1.58-.8 5.42-1.13 7.19-.14.75-.42 1-.68 1.03-.58.05-1.02-.38-1.58-.75-.88-.58-1.38-.94-2.23-1.5-.99-.65-.35-1.01.22-1.59.15-.15 2.71-2.48 2.76-2.69a.2.2 0 00-.05-.18c-.06-.05-.14-.03-.21-.02-.09.02-1.49.95-4.22 2.79-.4.27-.76.41-1.08.4-.36-.01-1.04-.2-1.55-.37-.63-.2-1.12-.31-1.08-.66.02-.18.27-.36.74-.55 2.92-1.27 4.86-2.11 5.83-2.51 2.78-1.16 3.35-1.36 3.73-1.36.08 0 .27.02.39.12.1.08.13.19.14.27-.01.06.01.24 0 .38z"/>
                        </svg>
                        <span class="text-[11px] font-tech font-semibold text-slate-300 group-hover:text-cyan-300 transition">Contact Developer</span>
                    </div>
                    <span class="text-[10px] text-pink-400 font-mono flex items-center gap-0.5">
                        @NAGATOxOWNER
                        <svg class="w-3 h-3 text-cyan-400 group-hover:translate-x-0.5 transition" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"/>
                        </svg>
                    </span>
                </a>
            </div>

        </div>

        <!-- Bottom Tagline -->
        <div class="flex items-center justify-center gap-2.5 text-[8px] font-tech text-slate-500 tracking-[0.25em] uppercase mt-3">
            <span>BUILD</span> &bull; <span>AUTOMATE</span> &bull; <span>DOMINATE</span>
        </div>
    </div>

    <script>
        function togglePassVisibility() {
            var input = document.getElementById('passField');
            if (!input) return;
            input.type = input.type === 'password' ? 'text' : 'password';
        }
    </script>
</body>
</html>"""

# ==========================================
# MASTER DASHBOARD (3-LINE MENU & 4 TABS)
# ==========================================
MASTER_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Nagato Panel &mdash; Master Control</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@600;700;800;900&family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Plus Jakarta Sans', sans-serif; background-color: #06040c; }
        .font-tech { font-family: 'Orbitron', monospace; }
        .glass-card {
            background: linear-gradient(135deg, rgba(22, 12, 42, 0.85) 0%, rgba(13, 8, 25, 0.92) 100%);
            border: 1px solid rgba(139, 92, 246, 0.25);
        }
        #masterSidebar {
            position: fixed;
            top: 0;
            left: 0;
            bottom: 0;
            width: 260px;
            background-color: #090614;
            border-right: 1px solid rgba(139, 92, 246, 0.25);
            z-index: 50;
            transition: transform 0.25s ease;
            transform: translateX(-100%);
        }
        #masterSidebar.open { transform: translateX(0); }
        @media (min-width: 768px) {
            #masterSidebar { position: static; transform: translateX(0) !important; height: 100vh; }
        }
        #masterBackdrop {
            display: none;
            position: fixed;
            inset: 0;
            background: rgba(0,0,0,0.7);
            backdrop-filter: blur(4px);
            z-index: 40;
        }
        #masterBackdrop.open { display: block; }
    </style>
</head>
<body class="text-slate-100 min-h-screen flex">
    <div id="masterBackdrop" onclick="toggleMasterMenu()"></div>

    <aside id="masterSidebar" class="p-5 flex flex-col justify-between overflow-y-auto">
        <div class="space-y-6">
            <div class="flex items-center justify-between">
                <div class="flex items-center gap-2.5">
                    <svg class="w-6 h-6 text-fuchsia-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/>
                        <path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/>
                        <path d="M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0"/>
                        <path d="M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5"/>
                    </svg>
                    <div>
                        <span class="font-tech font-bold text-sm text-transparent bg-clip-text bg-gradient-to-r from-purple-300 via-fuchsia-300 to-cyan-300 tracking-wider block">Nagato Panel</span>
                        <span class="font-tech text-[10px] tracking-wider text-cyan-400 uppercase block">MASTER RESELLER</span>
                    </div>
                </div>
                <button type="button" onclick="toggleMasterMenu()" class="md:hidden text-purple-400 hover:text-white p-1">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
                </button>
            </div>

            <nav class="space-y-1.5 text-xs font-mono font-medium">
                <button type="button" onclick="switchMasterTab('m-tab-dashboard')" id="btn-m-tab-dashboard" class="m-nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-semibold bg-purple-900/40 border border-fuchsia-500/30 text-cyan-400 transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z"/></svg>
                    Dashboard
                </button>
                <button type="button" onclick="switchMasterTab('m-tab-clients')" id="btn-m-tab-clients" class="m-nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z"/></svg>
                    All Client
                </button>
                <button type="button" onclick="switchMasterTab('m-tab-new')" id="btn-m-tab-new" class="m-nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v3m0 0v3m0-3h3m-3 0H9m12 0a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>
                    New Panel
                </button>
                <button type="button" onclick="switchMasterTab('m-tab-security')" id="btn-m-tab-security" class="m-nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z"/></svg>
                    Security
                </button>
            </nav>
        </div>

        <div class="pt-4 border-t border-purple-900/40">
            <a href="/master/logout" class="w-full flex items-center justify-center gap-2 py-2 rounded-xl text-xs font-mono font-semibold text-rose-400 hover:bg-rose-500/10 transition">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"/></svg>
                Sign Out
            </a>
        </div>
    </aside>

    <main class="flex-1 flex flex-col min-w-0 h-screen overflow-y-auto">
        <header class="sticky top-0 z-20 bg-[#06040c]/95 backdrop-blur-md border-b border-purple-900/40 px-4 md:px-8 py-3.5 flex items-center justify-between">
            <div class="flex items-center gap-3">
                <button type="button" onclick="toggleMasterMenu()" class="p-2 rounded-lg bg-[#120b22] border border-purple-900/40 text-purple-300 hover:text-white focus:outline-none">
                    <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16"/></svg>
                </button>
                <h2 class="font-tech text-base md:text-lg font-bold text-transparent bg-clip-text bg-gradient-to-r from-purple-300 via-fuchsia-300 to-cyan-300 tracking-wider">
                    Nagato Panel
                </h2>
            </div>
            
            <div class="flex items-center gap-3">
                <span class="flex items-center gap-1.5 px-3 py-1 rounded-full bg-[#120b22] border border-cyan-500/40 text-xs font-mono text-cyan-300">
                    <span class="w-2 h-2 rounded-full bg-cyan-400 animate-pulse inline-block"></span>
                    MASTER ACTIVE
                </span>
            </div>
        </header>

        <div class="p-4 md:p-8 max-w-5xl w-full mx-auto space-y-6">
            {% if message %}
            <div class="p-4 rounded-xl bg-cyan-500/10 border border-cyan-500/40 text-cyan-300 text-xs font-mono">
                {{ message }}
            </div>
            {% endif %}

            <div id="m-tab-dashboard" class="m-tab-content space-y-6">
                <div class="grid grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4">
                    <div class="glass-card rounded-2xl p-4 flex flex-col justify-between">
                        <span class="font-tech text-2xl md:text-3xl font-extrabold text-white">{{ clients|length }}</span>
                        <p class="text-[11px] text-purple-300/80 font-mono mt-0.5 uppercase">Total Clients</p>
                    </div>
                    <div class="glass-card rounded-2xl p-4 flex flex-col justify-between">
                        <span class="font-tech text-2xl md:text-3xl font-extrabold text-cyan-400">{{ active_count }}</span>
                        <p class="text-[11px] text-purple-300/80 font-mono mt-0.5 uppercase">Active Bots</p>
                    </div>
                    <div class="glass-card rounded-2xl p-4 flex flex-col justify-between">
                        <span class="font-tech text-2xl md:text-3xl font-extrabold text-emerald-400">&#8377;{{ total_revenue }}</span>
                        <p class="text-[11px] text-purple-300/80 font-mono mt-0.5 uppercase">Total Collected</p>
                    </div>
                    <div class="glass-card rounded-2xl p-4 flex flex-col justify-between">
                        <span class="font-tech text-base md:text-lg font-bold text-fuchsia-300 truncate">SaaS Engine</span>
                        <p class="text-[11px] text-purple-400 mt-0.5 font-mono">Multi-Tenant 3.x</p>
                    </div>
                </div>

                <div class="glass-card rounded-2xl p-6 space-y-3">
                    <h3 class="font-tech text-sm font-bold text-white border-b border-purple-900/40 pb-2">Master Cloud Provisioner</h3>
                    <p class="text-xs text-slate-300 leading-relaxed font-sans">
                        From here you can provision isolated instances, generate custom client admin panels, regulate plan expiration dates, and control concurrent background bot pollers.
                    </p>
                </div>
            </div>

            <div id="m-tab-clients" class="m-tab-content space-y-6 hidden">
                <div class="glass-card rounded-2xl p-5 space-y-4">
                    <div class="flex items-center justify-between border-b border-purple-900/40 pb-3">
                        <h3 class="font-tech text-base font-bold text-white tracking-wide">All Clients</h3>
                        <button type="button" onclick="switchMasterTab('m-tab-new')" class="bg-cyan-600 hover:bg-cyan-500 font-tech text-xs px-3 py-1.5 rounded-xl uppercase transition">+ New Client</button>
                    </div>
                    <div class="overflow-x-auto">
                        <table class="w-full text-left text-xs font-mono">
                            <thead class="text-purple-400 uppercase text-[10px] border-b border-purple-900/40">
                                <tr>
                                    <th class="py-2.5 px-3">Client</th>
                                    <th class="py-2.5 px-3">Make Date</th>
                                    <th class="py-2.5 px-3">Expiry</th>
                                    <th class="py-2.5 px-3">Days Left</th>
                                    <th class="py-2.5 px-3">Price</th>
                                    <th class="py-2.5 px-3">Status</th>
                                    <th class="py-2.5 px-3">Admin URL</th>
                                    <th class="py-2.5 px-3 text-right">Actions</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-purple-900/30 text-slate-300">
                                {% for c in clients %}
                                <tr class="hover:bg-purple-900/20 transition">
                                    <td class="py-2.5 px-3">
                                        <span class="font-bold text-white block">{{ c.client_name }}</span>
                                        <span class="text-[10px] text-fuchsia-400">@{{ c.telegram or 'No Telegram' }}</span>
                                    </td>
                                    <td class="py-2.5 px-3 text-slate-400">{{ c.make_date }}</td>
                                    <td class="py-2.5 px-3 text-slate-400">{{ c.expiry_date }}</td>
                                    <td class="py-2.5 px-3 font-bold {{ 'text-emerald-400' if c.days_left > 5 else ('text-amber-400' if c.days_left > 0 else 'text-rose-400') }}">
                                        {{ c.days_left }} days left
                                    </td>
                                    <td class="py-2.5 px-3 font-semibold text-white">&#8377;{{ c.price }}</td>
                                    <td class="py-2.5 px-3">
                                        <span class="px-2 py-0.5 rounded text-[10px] uppercase font-tech {{ 'text-emerald-400 bg-emerald-500/10' if c.status == 'ACTIVE' and c.days_left > 0 else 'text-rose-400 bg-rose-500/10' }}">
                                            {{ 'ACTIVE' if c.status == 'ACTIVE' and c.days_left > 0 else 'EXPIRED' }}
                                        </span>
                                    </td>
                                    <td class="py-2.5 px-3">
                                        <a href="/c/{{ c.slug }}/admin" target="_blank" class="text-cyan-400 hover:underline">
                                            /c/{{ c.slug }}/admin ↗
                                        </a>
                                    </td>
                                    <td class="py-2.5 px-3 text-right">
                                        <form method="POST" action="/master/client/delete" onsubmit="return confirm('Delete client completely? Database will be purged.');">
                                            <input type="hidden" name="client_id" value="{{ c.id }}">
                                            <button type="submit" class="bg-rose-500/20 hover:bg-rose-600 text-rose-400 hover:text-white px-2.5 py-1 rounded-lg text-[10px] font-tech transition">
                                                Delete
                                            </button>
                                        </form>
                                    </td>
                                </tr>
                                {% else %}
                                <tr><td colspan="8" class="text-center py-8 text-purple-400">No client panels deployed yet.</td></tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

            <div id="m-tab-new" class="m-tab-content space-y-6 hidden">
                <div class="glass-card rounded-2xl p-6 space-y-4">
                    <h3 class="font-tech text-base font-bold text-white border-b border-purple-900/40 pb-3">Create New Client Panel</h3>
                    <form method="POST" action="/master/client/new" class="space-y-4 text-xs font-mono">
                        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div>
                                <label class="text-purple-300 block mb-1 uppercase">Client Name *</label>
                                <input type="text" name="client_name" required placeholder="e.g. Rahul VIP" class="w-full bg-[#070410] border border-purple-900/60 rounded-xl p-2.5 text-white">
                            </div>
                            <div>
                                <label class="text-purple-300 block mb-1 uppercase">Telegram Username</label>
                                <input type="text" name="telegram" placeholder="@client_username" class="w-full bg-[#070410] border border-purple-900/60 rounded-xl p-2.5 text-white">
                            </div>
                        </div>

                        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div>
                                <label class="text-purple-300 block mb-1 uppercase">Folder / URL Slug *</label>
                                <input type="text" name="slug" required placeholder="rahul-deals" class="w-full bg-[#070410] border border-purple-900/60 rounded-xl p-2.5 text-white">
                            </div>
                            <div>
                                <label class="text-purple-300 block mb-1 uppercase">Plan Duration (Days)</label>
                                <input type="number" name="plan_days" value="30" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl p-2.5 text-white">
                            </div>
                        </div>

                        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div>
                                <label class="text-purple-300 block mb-1 uppercase">Sell Price (₹)</label>
                                <input type="number" name="price" value="199" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl p-2.5 text-white">
                            </div>
                            <div>
                                <label class="text-purple-300 block mb-1 uppercase">Telegram Bot Token (Optional)</label>
                                <input type="text" name="bot_token" placeholder="123456:ABC-DEF..." class="w-full bg-[#070410] border border-purple-900/60 rounded-xl p-2.5 text-white">
                            </div>
                        </div>

                        <div class="border-t border-purple-900/40 pt-4 grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div>
                                <label class="text-purple-300 block mb-1 uppercase">Admin Username</label>
                                <input type="text" name="admin_username" value="admin" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl p-2.5 text-white">
                            </div>
                            <div>
                                <label class="text-purple-300 block mb-1 uppercase">Admin Password</label>
                                <input type="text" name="admin_password" placeholder="Passcode (min 6 chars)" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl p-2.5 text-white">
                            </div>
                        </div>

                        <button type="submit" class="w-full bg-gradient-to-r from-fuchsia-600 to-purple-600 text-white font-tech font-bold py-3 rounded-xl uppercase tracking-wider transition">
                            Create &amp; Provision Panel
                        </button>
                    </form>
                </div>
            </div>

            <div id="m-tab-security" class="m-tab-content space-y-6 hidden">
                <form method="POST" action="/master/password/update" class="glass-card rounded-2xl p-6 space-y-4 text-xs font-mono">
                    <h3 class="font-tech text-base font-bold text-white border-b border-purple-900/40 pb-3">Master Security Settings</h3>
                    <div>
                        <label class="block text-purple-300 mb-1 uppercase">Current Master Password</label>
                        <input type="password" name="current_password" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl px-4 py-2.5 text-sm text-white">
                    </div>
                    <div>
                        <label class="block text-purple-300 mb-1 uppercase">New Master Password</label>
                        <input type="password" name="new_password" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl px-4 py-2.5 text-sm text-white">
                    </div>
                    <button type="submit" class="bg-purple-900/60 hover:bg-cyan-600 text-white font-tech px-5 py-2.5 rounded-xl uppercase transition">Update Master Password</button>
                </form>
            </div>
        </div>
    </main>

    <script>
        function toggleMasterMenu() {
            var s = document.getElementById('masterSidebar');
            var b = document.getElementById('masterBackdrop');
            if (s) s.classList.toggle('open');
            if (b) b.classList.toggle('open');
        }

        function switchMasterTab(tabId) {
            document.querySelectorAll('.m-tab-content').forEach(function(el) { el.classList.add('hidden'); });
            var target = document.getElementById(tabId);
            if (target) target.classList.remove('hidden');

            document.querySelectorAll('.m-nav-btn').forEach(function(btn) {
                btn.classList.remove('bg-purple-900/40', 'border', 'border-fuchsia-500/30', 'text-cyan-400');
                btn.classList.add('text-slate-400');
            });

            var activeBtn = document.getElementById('btn-' + tabId);
            if (activeBtn) {
                activeBtn.classList.add('bg-purple-900/40', 'border', 'border-fuchsia-500/30', 'text-cyan-400');
                activeBtn.classList.remove('text-slate-400');
            }

            if (window.innerWidth < 768) {
                var s = document.getElementById('masterSidebar');
                if (s && s.classList.contains('open')) toggleMasterMenu();
            }
        }
    </script>
</body>
</html>"""

# FULL UNALTERED DASHBOARD PAGE TEMPLATE
DASHBOARD_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard &mdash; Nagato Panel</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@500;700;900&family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Plus Jakarta Sans', sans-serif; background-color: #06040c; margin: 0; padding: 0; overflow-x: hidden; }
        .font-tech { font-family: 'Orbitron', monospace; }
        .glass-card {
            background: linear-gradient(135deg, rgba(22, 12, 42, 0.85) 0%, rgba(13, 8, 25, 0.92) 100%);
            border: 1px solid rgba(139, 92, 246, 0.25);
        }
        .neon-border-pink {
            border-color: rgba(255, 0, 127, 0.5) !important;
            box-shadow: 0 0 15px rgba(255, 0, 127, 0.2);
        }
        #sidebar {
            position: fixed;
            top: 0;
            left: 0;
            bottom: 0;
            width: 260px;
            background-color: #090614;
            border-right: 1px solid rgba(139, 92, 246, 0.25);
            z-index: 50;
            transition: transform 0.25s ease;
            transform: translateX(-100%);
        }
        #sidebar.open {
            transform: translateX(0);
        }
        @media (min-width: 768px) {
            #sidebar {
                position: static;
                transform: translateX(0) !important;
                height: 100vh;
            }
        }
        #sidebarBackdrop {
            display: none;
            position: fixed;
            inset: 0;
            background: rgba(0,0,0,0.7);
            backdrop-filter: blur(4px);
            z-index: 40;
        }
        #sidebarBackdrop.open {
            display: block;
        }

        .modal-overlay {
            display: none;
            position: fixed;
            inset: 0;
            background: rgba(0, 0, 0, 0.75);
            backdrop-filter: blur(6px);
            z-index: 99;
            align-items: center;
            justify-content: center;
            padding: 1rem;
        }
        .modal-overlay.active {
            display: flex;
        }
    </style>
</head>
<body class="text-slate-100 min-h-screen flex">
    <div id="sidebarBackdrop" onclick="toggleSidebar()"></div>

    <div id="confirmResetModal" class="modal-overlay">
        <div class="glass-card max-w-sm w-full p-6 rounded-2xl border border-amber-500/40 shadow-[0_0_30px_rgba(245,158,11,0.25)] space-y-5">
            <div class="flex items-center gap-3 border-b border-purple-900/40 pb-3">
                <span class="p-2 rounded-xl bg-amber-500/10 text-amber-400 border border-amber-500/30">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" /></svg>
                </span>
                <div>
                    <h4 class="font-tech text-sm font-bold text-white tracking-wide">Reset Revenue</h4>
                    <p class="text-[11px] text-purple-400/80 font-mono">Zero out financial archive</p>
                </div>
            </div>

            <p class="text-xs text-slate-300 leading-relaxed font-sans">
                Are you sure you want to reset all revenue counters? This will clear all recorded payment logs permanently.
            </p>

            <form method="POST" action="/admin/revenue/reset" class="flex gap-3 pt-2">
                <button type="button" onclick="closeResetModal()" class="flex-1 bg-purple-900/40 hover:bg-purple-900/60 text-slate-300 font-tech text-xs py-2.5 rounded-xl transition">
                    Cancel
                </button>
                <button type="submit" class="flex-1 bg-gradient-to-r from-amber-600 to-yellow-600 hover:from-amber-500 hover:to-yellow-500 text-white font-tech font-bold text-xs py-2.5 rounded-xl uppercase tracking-wider shadow-lg shadow-amber-600/30 transition">
                    Confirm Reset
                </button>
            </form>
        </div>
    </div>

    <div id="confirmDeleteModal" class="modal-overlay">
        <div class="glass-card max-w-sm w-full p-6 rounded-2xl border border-rose-500/40 shadow-[0_0_30px_rgba(244,63,94,0.25)] space-y-5">
            <div class="flex items-center gap-3 border-b border-purple-900/40 pb-3">
                <span class="p-2 rounded-xl bg-rose-500/10 text-rose-400 border border-rose-500/30">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
                </span>
                <div>
                    <h4 class="font-tech text-sm font-bold text-white tracking-wide">Confirm Deletion</h4>
                    <p class="text-[11px] text-purple-400/80 font-mono">This action is permanent.</p>
                </div>
            </div>

            <p class="text-xs text-slate-300 leading-relaxed font-sans">
                Are you sure you want to delete <span id="modalPlanName" class="text-cyan-400 font-semibold"></span> (<span id="modalPlanId" class="text-purple-300 font-mono"></span>)?
            </p>

            <form id="modalDeleteForm" method="POST" action="/admin/plans/delete" class="flex gap-3 pt-2">
                <input type="hidden" id="modalPlanIdInput" name="plan_id" value="">
                <button type="button" onclick="closeDeleteModal()" class="flex-1 bg-purple-900/40 hover:bg-purple-900/60 text-slate-300 font-tech text-xs py-2.5 rounded-xl transition">
                    Cancel
                </button>
                <button type="submit" class="flex-1 bg-gradient-to-r from-rose-600 to-red-600 hover:from-rose-500 hover:to-red-500 text-white font-tech font-bold text-xs py-2.5 rounded-xl uppercase tracking-wider shadow-lg shadow-rose-600/30 transition">
                    Delete
                </button>
            </form>
        </div>
    </div>

    <aside id="sidebar" class="p-5 flex flex-col justify-between overflow-y-auto">
        <div class="space-y-6">
            <div class="flex items-center justify-between">
                <div class="flex items-center gap-2.5">
                    <svg class="w-6 h-6 text-fuchsia-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/>
                        <path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/>
                        <path d="M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0"/>
                        <path d="M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5"/>
                    </svg>
                    <div>
                        <span class="font-tech font-bold text-sm text-transparent bg-clip-text bg-gradient-to-r from-fuchsia-400 to-cyan-300 tracking-wider block">Nagato Panel</span>
                        <span class="font-tech text-[10px] tracking-wider text-cyan-300 uppercase block">Pom Pom Bot</span>
                    </div>
                </div>
                <button type="button" onclick="toggleSidebar()" class="md:hidden text-purple-400 hover:text-white p-1">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>
                </button>
            </div>

            <nav class="space-y-1.5 text-xs">
                <button type="button" onclick="switchTab('tab-dashboard')" id="nav-tab-dashboard" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-semibold bg-purple-900/40 border border-fuchsia-500/30 text-cyan-400 shadow-[0_0_12px_rgba(0,240,255,0.15)] transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z"/></svg>
                    Dashboard
                </button>
                <button type="button" onclick="switchTab('tab-orders')" id="nav-tab-orders" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-6 9l2 2 4-4"/></svg>
                    Orders
                </button>
                <button type="button" onclick="switchTab('tab-users')" id="nav-tab-users" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z"/></svg>
                    Manage Users
                </button>
                <button type="button" onclick="switchTab('tab-broadcast')" id="nav-tab-broadcast" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 5.882V19.24a1.76 1.76 0 01-3.417.592l-2.147-6.15M18 13a3 3 0 100-6M5.436 13.683A4.001 4.001 0 017 6h1.832c4.1 0 7.625-1.234 9.168-3v14c-1.543-1.766-5.067-3-9.168-3H7a3.988 3.988 0 01-1.564-.317z"/></svg>
                    Broadcast
                </button>
                <button type="button" onclick="switchTab('tab-settings')" id="nav-tab-settings" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"/><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/></svg>
                    Setting &amp; UPI
                </button>
                <button type="button" onclick="switchTab('tab-bot')" id="nav-tab-bot" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1121 9z"/></svg>
                    Bot Token Config
                </button>
                <button type="button" onclick="switchTab('tab-media')" id="nav-tab-media" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z"/></svg>
                    Media &amp; Greetings
                </button>
                <button type="button" onclick="switchTab('tab-plans')" id="nav-tab-plans" class="nav-btn w-full flex items-center gap-3 px-3 py-2.5 rounded-xl font-medium text-slate-400 hover:bg-purple-900/30 hover:text-white transition">
                    <svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>
                    Subscription Plans
                </button>
            </nav>
        </div>

        <div class="pt-4 border-t border-purple-900/40 space-y-3">
            <a href="/logout" class="w-full flex items-center justify-center gap-2 py-2 rounded-xl text-xs font-mono font-semibold text-rose-400 hover:bg-rose-500/10 transition">
                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"/></svg>
                Sign Out
            </a>
        </div>
    </aside>

    <main class="flex-1 flex flex-col min-w-0 h-screen overflow-y-auto">
        <header class="sticky top-0 z-20 bg-[#06040c]/95 backdrop-blur-md border-b border-purple-900/40 px-4 md:px-8 py-3.5 flex items-center justify-between">
            <div class="flex items-center gap-3">
                <button type="button" onclick="toggleSidebar()" class="p-2 rounded-lg bg-[#120b22] border border-purple-900/40 text-purple-300 hover:text-white focus:outline-none">
                    <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16"/></svg>
                </button>
                <h2 id="sectionTitle" class="font-tech text-base md:text-lg font-bold text-white tracking-wider">Dashboard</h2>
            </div>
            
            <div class="flex items-center gap-3">
                <span class="flex items-center gap-1.5 px-3 py-1 rounded-full bg-[#120b22] border border-cyan-500/40 text-xs font-mono text-cyan-300">
                    <span class="w-2 h-2 rounded-full {{ 'bg-cyan-400' if is_online else 'bg-amber-400' }} animate-pulse inline-block"></span>
                    {{ 'ONLINE' if is_online else 'STANDBY' }}
                </span>
            </div>
        </header>

        <div class="p-4 md:p-8 max-w-5xl w-full mx-auto space-y-6">
            {% if message %}
            <div class="p-4 rounded-xl bg-cyan-500/10 border border-cyan-500/40 text-cyan-300 text-xs font-mono">
                {{ message }}
            </div>
            {% endif %}

            <div id="tab-dashboard" class="tab-content space-y-6">
                <div class="grid grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4">
                    <div class="glass-card rounded-2xl p-4 flex flex-col justify-between">
                        <span class="font-tech text-2xl md:text-3xl font-extrabold text-white">{{ paid_orders }}</span>
                        <p class="text-[11px] text-purple-300/80 font-mono mt-0.5 uppercase">Paid orders</p>
                    </div>

                    <div class="glass-card rounded-2xl p-4 flex flex-col justify-between">
                        <div class="flex justify-between items-center">
                            <span class="font-tech text-2xl md:text-3xl font-extrabold text-white">&#8377;{{ revenue }}</span>
                            <button type="button" onclick="openResetModal()" class="text-[10px] text-purple-400 hover:text-fuchsia-300 font-mono transition">
                                Reset
                            </button>
                        </div>
                        <p class="text-[11px] text-purple-300/80 font-mono mt-0.5 uppercase">Revenue</p>
                    </div>

                    <div class="glass-card rounded-2xl p-4 flex flex-col justify-between">
                        <span class="font-tech text-2xl md:text-3xl font-extrabold text-white">{{ total_users }}</span>
                        <p class="text-[11px] text-purple-300/80 font-mono mt-0.5 uppercase">Total users</p>
                    </div>

                    <div class="glass-card rounded-2xl p-4 flex flex-col justify-between neon-border-pink">
                        <div class="flex justify-between items-center mb-1">
                            <span class="p-1.5 rounded-lg bg-fuchsia-500/10 text-fuchsia-400 border border-fuchsia-500/30">
                                <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5.121 17.804A13.937 13.937 0 0112 16c2.5 0 4.847.655 6.879 1.804M15 10a3 3 0 11-6 0 3 3 0 016 0zm6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                                </svg>
                            </span>
                            <span class="text-[10px] font-tech px-2 py-0.5 rounded-md bg-fuchsia-500/20 text-fuchsia-300 border border-fuchsia-500/40">
                                ADMIN
                            </span>
                        </div>
                        <div>
                            <span class="font-tech text-base md:text-lg font-black text-transparent bg-clip-text bg-gradient-to-r from-fuchsia-400 to-cyan-300 truncate block">
                                {{ admin_username }}
                            </span>
                            <p class="text-[10px] text-purple-400 mt-0.5 font-mono">Control Panel</p>
                        </div>
                    </div>
                </div>

                <div class="glass-card rounded-2xl p-5 space-y-4">
                    <div class="flex items-center justify-between border-b border-purple-900/40 pb-3">
                        <h3 class="font-tech text-base font-bold text-white tracking-wide">Recent orders</h3>
                        <button type="button" onclick="switchTab('tab-orders')" class="text-xs text-fuchsia-400 hover:text-fuchsia-300 font-mono">View All &rarr;</button>
                    </div>

                    <div class="overflow-x-auto">
                        <table class="w-full text-left text-xs font-mono">
                            <thead class="text-purple-400 uppercase text-[10px] border-b border-purple-900/40">
                                <tr>
                                    <th class="py-2.5 px-3">Order</th>
                                    <th class="py-2.5 px-3">User</th>
                                    <th class="py-2.5 px-3">Item</th>
                                    <th class="py-2.5 px-3">Amount</th>
                                    <th class="py-2.5 px-3 text-right">Status</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-purple-900/30 text-slate-300">
                                {% for oid, uid, uname, item, amt, st, dt in recent_orders %}
                                <tr>
                                    <td class="py-2.5 px-3 text-cyan-400">FT{{ oid }}ORD</td>
                                    <td class="py-2.5 px-3">{{ uid }}<span class="block text-[10px] text-purple-400">@{{ uname }}</span></td>
                                    <td class="py-2.5 px-3 text-white">{{ item }}</td>
                                    <td class="py-2.5 px-3 text-white font-semibold">&#8377;{{ "%.2f"|format(amt) }}</td>
                                    <td class="py-2.5 px-3 text-right">
                                        <span class="px-2 py-0.5 rounded text-[10px] uppercase font-tech {{ 'text-emerald-400 bg-emerald-500/10' if st == 'approved' else ('text-amber-400 bg-amber-500/10' if st == 'pending' else 'text-rose-400 bg-rose-500/10') }}">{{ st }}</span>
                                    </td>
                                </tr>
                                {% else %}
                                <tr><td colspan="5" class="py-6 text-center text-purple-400">No orders recorded yet.</td></tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

            <div id="tab-orders" class="tab-content space-y-6 hidden">
                <div class="glass-card rounded-2xl p-5 space-y-4">
                    <h3 class="font-tech text-base font-bold text-white tracking-wide border-b border-purple-900/40 pb-3">All Customer Orders</h3>
                    <div class="overflow-x-auto">
                        <table class="w-full text-left text-xs font-mono text-slate-300 border-collapse">
                            <thead class="text-purple-400 uppercase text-[10px] border-b border-purple-900/40">
                                <tr>
                                    <th class="py-2.5 px-3">Order</th>
                                    <th class="py-2.5 px-3">User</th>
                                    <th class="py-2.5 px-3">Item</th>
                                    <th class="py-2.5 px-3">Amount</th>
                                    <th class="py-2.5 px-3">Date</th>
                                    <th class="py-2.5 px-3">Status</th>
                                    <th class="py-2.5 px-3 text-right">Action</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-purple-900/30">
                                {% for oid, uid, uname, item, amt, st, dt in all_orders %}
                                <tr class="hover:bg-purple-900/20 transition">
                                    <td class="py-2.5 px-3 text-cyan-400 align-middle">FT{{ oid }}ORD</td>
                                    <td class="py-2.5 px-3 align-middle">{{ uid }}<span class="block text-[10px] text-purple-400">@{{ uname }}</span></td>
                                    <td class="py-2.5 px-3 text-white align-middle">{{ item }}</td>
                                    <td class="py-2.5 px-3 font-semibold text-white align-middle">&#8377;{{ "%.2f"|format(amt) }}</td>
                                    <td class="py-2.5 px-3 text-purple-300/80 text-[11px] align-middle">{{ dt }}</td>
                                    <td class="py-2.5 px-3 align-middle">
                                        <span class="px-2 py-0.5 rounded text-[10px] uppercase font-tech {{ 'text-emerald-400 bg-emerald-500/10' if st == 'approved' else ('text-amber-400 bg-amber-500/10' if st == 'pending' else 'text-rose-400 bg-rose-500/10') }}">
                                            {{ st }}
                                        </span>
                                    </td>
                                    <td class="py-2.5 px-3 text-right align-middle">
                                        {% if st == 'pending' %}
                                        <div class="flex items-center justify-end gap-1.5">
                                            <form method="POST" action="/admin/orders/status">
                                                <input type="hidden" name="order_id" value="{{ oid }}">
                                                <input type="hidden" name="action" value="approved">
                                                <button type="submit" class="bg-emerald-500/20 hover:bg-emerald-600 text-emerald-400 hover:text-white font-tech text-[10px] px-2.5 py-1 rounded-lg uppercase tracking-wider transition">
                                                    Accept
                                                </button>
                                            </form>
                                            <form method="POST" action="/admin/orders/status">
                                                <input type="hidden" name="order_id" value="{{ oid }}">
                                                <input type="hidden" name="action" value="rejected">
                                                <button type="submit" class="bg-rose-500/20 hover:bg-rose-600 text-rose-400 hover:text-white font-tech text-[10px] px-2.5 py-1 rounded-lg uppercase tracking-wider transition">
                                                    Reject
                                                </button>
                                            </form>
                                        </div>
                                        {% else %}
                                        <span class="text-purple-400/50 text-[11px] uppercase font-tech font-bold">Processed</span>
                                        {% endif %}
                                    </td>
                                </tr>
                                {% else %}
                                <tr><td colspan="7" class="py-8 text-center text-purple-400">No orders recorded in database.</td></tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

            <div id="tab-users" class="tab-content space-y-4 hidden">
                <div class="glass-card rounded-2xl p-5 space-y-4">
                    <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3 border-b border-purple-900/40 pb-3">
                        <div>
                            <h3 class="font-tech text-base font-bold text-white tracking-wide">Registered Users</h3>
                            <p class="text-[11px] text-purple-400 font-mono">Showing {{ users_list|length }} of {{ users_total }} total registered accounts</p>
                        </div>
                        <form method="GET" action="/" class="flex items-center gap-2">
                            <input type="hidden" name="tab" value="tab-users">
                            <input type="text" name="user_search" value="{{ user_search }}" placeholder="Search ID or @username..."
                                   class="bg-[#070410] border border-purple-900/60 rounded-xl px-3 py-1.5 text-xs text-white placeholder-purple-400/50 font-mono focus:outline-none focus:border-cyan-400 w-48 sm:w-56">
                            <button type="submit" class="bg-purple-900/60 hover:bg-cyan-600 text-white font-tech text-xs px-3 py-1.5 rounded-xl transition">
                                Find
                            </button>
                            {% if user_search %}
                            <a href="/?tab=tab-users" class="text-rose-400 hover:text-white text-xs font-mono px-1">✕</a>
                            {% endif %}
                        </form>
                    </div>

                    <div class="overflow-x-auto">
                        <table class="w-full text-left text-xs font-mono">
                            <thead class="text-purple-400 uppercase text-[10px] border-b border-purple-900/40">
                                <tr>
                                    <th class="py-2.5 px-3">User ID</th>
                                    <th class="py-2.5 px-3">Username</th>
                                    <th class="py-2.5 px-3">Subscription</th>
                                    <th class="py-2.5 px-3 text-right">Actions</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-purple-900/30 text-slate-300">
                                {% for u_id, u_fname, u_uname, u_joined, u_status, u_banned in users_list %}
                                <tr>
                                    <td class="py-2.5 px-3 text-cyan-400">{{ u_id }}<span class="block text-[10px] text-white">{{ u_fname }}</span></td>
                                    <td class="py-2.5 px-3"><a href="https://t.me/{{ u_uname }}" target="_blank" class="text-fuchsia-400">@{{ u_uname }}</a></td>
                                    <td class="py-2.5 px-3">
                                        <form method="POST" action="/admin/users/subscription" class="flex items-center gap-1.5">
                                            <input type="hidden" name="user_id" value="{{ u_id }}">
                                            <select name="plan_name" class="bg-[#070410] border border-purple-900/60 rounded px-2 py-1 text-[11px] text-white">
                                                <option value="Free" {{ 'selected' if u_status == 'Free' else '' }}>Free</option>
                                                {% for p_id, p_name, _, _, _ in plans %}
                                                <option value="{{ p_name }}" {{ 'selected' if u_status == p_name else '' }}>{{ p_name }}</option>
                                                {% endfor %}
                                            </select>
                                            <button type="submit" class="bg-purple-900/60 px-2 py-1 rounded text-[10px] font-tech text-white">SET</button>
                                        </form>
                                    </td>
                                    <td class="py-2.5 px-3 text-right">
                                        <form method="POST" action="/admin/users/ban">
                                            <input type="hidden" name="user_id" value="{{ u_id }}">
                                            <input type="hidden" name="status" value="{{ 0 if u_banned else 1 }}">
                                            <button type="submit" class="px-2.5 py-1 rounded text-[10px] font-tech {{ 'bg-emerald-500/20 text-emerald-400' if u_banned else 'bg-rose-500/20 text-rose-400' }}">
                                                {{ 'UNBAN' if u_banned else 'BAN' }}
                                            </button>
                                        </form>
                                    </td>
                                </tr>
                                {% else %}
                                <tr><td colspan="4" class="py-8 text-center text-purple-400">No users found.</td></tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>

                    {% if total_pages > 1 %}
                    <div class="flex items-center justify-between pt-3 border-t border-purple-900/40 font-mono text-xs text-purple-300">
                        <div>Page {{ current_page }} of {{ total_pages }}</div>
                        <div class="flex gap-2">
                            {% if current_page > 1 %}
                            <a href="/?tab=tab-users&page={{ current_page - 1 }}&user_search={{ user_search }}" 
                               class="px-3 py-1.5 rounded-lg bg-purple-900/40 hover:bg-cyan-600 text-white font-tech transition">&larr; Prev</a>
                            {% endif %}
                            {% if current_page < total_pages %}
                            <a href="/?tab=tab-users&page={{ current_page + 1 }}&user_search={{ user_search }}" 
                               class="px-3 py-1.5 rounded-lg bg-purple-900/40 hover:bg-cyan-600 text-white font-tech transition">Next &rarr;</a>
                            {% endif %}
                        </div>
                    </div>
                    {% endif %}
                </div>
            </div>

            <div id="tab-broadcast" class="tab-content space-y-6 hidden">
                <form method="POST" action="/admin/broadcast/send" class="glass-card rounded-2xl p-6 space-y-4">
                    <h3 class="font-tech text-base font-bold text-white border-b border-purple-900/40 pb-3">Transmit Broadcast</h3>
                    <div>
                        <label class="block text-xs text-purple-300 mb-2 font-mono uppercase">Message Payload</label>
                        <textarea name="broadcast_message" rows="4" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl p-3 text-sm text-white focus:outline-none"></textarea>
                    </div>
                    <div>
                        <label class="block text-xs text-purple-300 mb-2 font-mono uppercase">Photo URL (Optional)</label>
                        <input type="url" name="broadcast_photo" class="w-full bg-[#070410] border border-purple-900/60 rounded-xl px-4 py-2 text-sm text-white focus:outline-none">
                    </div>
                    <button type="submit" class="bg-gradient-to-r from-fuchsia-600 to-purple-600 text-white font-tech font-bold px-6 py-2.5 rounded-xl text-xs uppercase">Send Broadcast</button>
                </form>
            </div>

            <div id="tab-settings" class="tab-content space-y-6 hidden">
                <form method="POST" action="/admin/settings/upi" class="glass-card rounded-2xl p-6 space-y-4">
                    <h3 class="font-tech text-base font-bold text-white border-b border-purple-900/40 pb-3">UPI Settings</h3>
                    <div>
                        <label class="block text-xs text-purple-300 mb-1 font-mono uppercase">UPI ID</label>
                        <input type="text" id="upi_id_input" name="upi_id" value="{{ upi_id }}" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl px-4 py-2 text-sm text-white font-mono focus:outline-none">
                    </div>
                    <div>
                        <label class="block text-xs text-purple-300 mb-1 font-mono uppercase">Payee Name</label>
                        <input type="text" id="payee_name_input" name="payee_name" value="{{ payee_name }}" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl px-4 py-2 text-sm text-white focus:outline-none">
                    </div>
                    <div>
                        <label class="block text-xs text-purple-300 mb-1 font-mono uppercase">Admin Telegram Chat ID</label>
                        <input type="text" name="admin_chat_id" value="{{ admin_chat_id }}" class="w-full bg-[#070410] border border-purple-900/60 rounded-xl px-4 py-2 text-sm text-white font-mono focus:outline-none">
                    </div>
                    <button type="submit" class="bg-gradient-to-r from-fuchsia-600 to-purple-600 text-white font-tech font-bold px-5 py-2.5 rounded-xl text-xs uppercase">Save UPI</button>
                </form>

                <form method="POST" action="/admin/password/update" class="glass-card rounded-2xl p-6 space-y-4">
                    <h3 class="font-tech text-base font-bold text-white border-b border-purple-900/40 pb-3">Security Key</h3>
                    <div>
                        <label class="block text-xs text-purple-300 mb-1 font-mono uppercase">Current Password</label>
                        <input type="password" name="current_password" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl px-4 py-2 text-sm text-white focus:outline-none">
                    </div>
                    <div>
                        <label class="block text-xs text-purple-300 mb-1 font-mono uppercase">New Password</label>
                        <input type="password" name="new_password" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl px-4 py-2 text-sm text-white focus:outline-none">
                    </div>
                    <button type="submit" class="bg-purple-900/60 text-white font-tech px-5 py-2 rounded-xl text-xs">Update Password</button>
                </form>
            </div>

            <form id="botSaveForm" method="POST" action="/admin/save">
                <div id="tab-bot" class="tab-content space-y-5 hidden">
                    <div class="glass-card rounded-2xl p-6 space-y-4">
                        <h3 class="font-tech text-base font-bold text-white border-b border-purple-900/40 pb-3">Telegram Bot API</h3>
                        <div>
                            <label class="block text-xs text-purple-300 mb-2 font-mono uppercase">Bot Token</label>
                            <input type="text" id="bot_token_input" name="bot_token" value="{{ bot_token }}" class="w-full bg-[#070410] border border-purple-900/60 rounded-xl px-4 py-3 text-sm text-white font-mono focus:outline-none">
                        </div>
                    </div>
                </div>

                <div id="tab-media" class="tab-content space-y-5 hidden">
                    <div class="glass-card rounded-2xl p-6 space-y-4">
                        <h3 class="font-tech text-base font-bold text-white border-b border-purple-900/40 pb-3">Media &amp; Interface Messages</h3>
                        <div>
                            <label class="block text-xs text-purple-300 mb-1 font-mono uppercase">Welcome Greeting</label>
                            <textarea name="welcome_text" rows="3" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl p-3 text-sm text-white">{{ welcome_text }}</textarea>
                        </div>
                        <div>
                            <label class="block text-xs text-purple-300 mb-1 font-mono uppercase">Plans Header Text</label>
                            <textarea name="plans_text" rows="2" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl p-3 text-sm text-white">{{ plans_text }}</textarea>
                        </div>
                        <div>
                            <label class="block text-xs text-purple-300 mb-1 font-mono uppercase">Welcome Image URL</label>
                            <input type="url" name="welcome_photo" value="{{ welcome_photo }}" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl px-4 py-2 text-sm text-white">
                        </div>
                        <div>
                            <label class="block text-xs text-purple-300 mb-1 font-mono uppercase">Demo Videos (1 URL per line)</label>
                            <textarea name="demo_video" rows="4" required class="w-full bg-[#070410] border border-purple-900/60 rounded-xl p-3 text-sm text-white font-mono">{{ demo_video }}</textarea>
                        </div>
                    </div>
                </div>

                <div id="saveBar" class="pt-4 hidden">
                    <button type="submit" class="w-full bg-gradient-to-r from-fuchsia-600 via-purple-600 to-cyan-600 text-white font-tech font-bold py-3 rounded-xl uppercase tracking-wider">
                        Save Interface Parameters
                    </button>
                </div>
            </form>

            <div id="videoUploadContainer" class="tab-content space-y-5 hidden">
                <div class="glass-card rounded-2xl p-5 space-y-3">
                    <div class="flex items-center justify-between">
                        <h4 class="font-tech text-xs font-bold text-cyan-300 uppercase tracking-wider">Direct Video Upload</h4>
                        <span id="uploadBadge" class="text-[10px] font-mono text-purple-400 hidden">Uploading...</span>
                    </div>
                    
                    <div class="flex flex-col sm:flex-row items-stretch sm:items-center gap-3">
                        <label class="flex-1 cursor-pointer">
                            <input type="file" id="demo_file_input" multiple accept="video/mp4,video/*" 
                                   class="block w-full text-xs text-slate-300 font-mono file:mr-3 file:py-2 file:px-4 file:rounded-xl file:border-0 file:text-xs file:font-tech file:bg-purple-900/60 file:text-cyan-400 hover:file:bg-purple-900/90 bg-[#070410] border border-purple-900/60 rounded-xl p-1.5 focus:outline-none">
                        </label>
                        <button type="button" id="uploadBtn" onclick="uploadDemoVideoFile()" 
                                class="bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 text-white font-tech font-bold text-xs py-2.5 px-6 rounded-xl uppercase tracking-wider shadow-lg shadow-cyan-600/30 transition shrink-0">
                            Upload
                        </button>
                    </div>

                    <div id="uploadStatus" class="text-xs font-mono empty:hidden transition-all"></div>
                </div>
            </div>

            <div id="tab-plans" class="tab-content space-y-5 hidden">
                <div class="glass-card rounded-2xl p-6 space-y-4">
                    <h3 class="font-tech text-base font-bold text-white border-b border-purple-900/40 pb-3">Add Subscription Tier</h3>
                    <form method="POST" action="/admin/plans/add" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3 text-xs font-mono">
                        <div>
                            <label class="block text-purple-300 mb-1 uppercase">Plan ID</label>
                            <input type="text" name="plan_id" required placeholder="plan_9" class="w-full bg-[#070410] border border-purple-900/60 rounded px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-purple-300 mb-1 uppercase">Tier Name</label>
                            <input type="text" name="name" required placeholder="VIP ACCESS" class="w-full bg-[#070410] border border-purple-900/60 rounded px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-purple-300 mb-1 uppercase">Price (Rs.)</label>
                            <input type="number" step="any" name="amount" required placeholder="499" class="w-full bg-[#070410] border border-purple-900/60 rounded px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-purple-300 mb-1 uppercase">Validity</label>
                            <input type="text" name="validity" required placeholder="60 Days" class="w-full bg-[#070410] border border-purple-900/60 rounded px-3 py-2 text-white">
                        </div>
                        <div>
                            <label class="block text-purple-300 mb-1 uppercase">Channel Link (Optional)</label>
                            <input type="text" name="access_link" placeholder="https://t.me/+..." class="w-full bg-[#070410] border border-purple-900/60 rounded px-3 py-2 text-white">
                        </div>
                        <div class="sm:col-span-2 lg:col-span-5">
                            <button type="submit" class="bg-cyan-600 text-white font-tech font-bold py-2.5 px-6 rounded-xl uppercase tracking-wider">+ Add Plan</button>
                        </div>
                    </form>
                </div>

                <div class="glass-card rounded-2xl p-6 space-y-4">
                    <h3 class="font-tech text-base font-bold text-white border-b border-purple-900/40 pb-3">Active Plans</h3>
                    <div class="overflow-x-auto">
                        <table class="w-full text-left text-xs font-mono text-slate-300 border-collapse">
                            <thead class="text-purple-400 uppercase text-[10px] border-b border-purple-900/40">
                                <tr>
                                    <th class="p-3">Plan Key</th>
                                    <th class="p-3">Title</th>
                                    <th class="p-3">Price (Rs.)</th>
                                    <th class="p-3">Validity</th>
                                    <th class="p-3">Premium Link</th>
                                    <th class="p-3 text-center">Update Action</th>
                                    <th class="p-3 text-center">Delete Action</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-purple-900/30">
                                {% for pid, name, amount, validity, access_link in plans %}
                                <tr class="hover:bg-purple-900/20 transition">
                                    <td class="p-3 text-cyan-400 align-middle">{{ pid }}</td>
                                    
                                    <form method="POST" action="/admin/plans/update">
                                        <input type="hidden" name="plan_id" value="{{ pid }}">
                                        <td class="p-3 align-middle">
                                            <input type="text" name="name" value="{{ name }}" class="bg-[#070410] border border-purple-900/60 rounded-lg px-2.5 py-1.5 text-white min-w-[130px]">
                                        </td>
                                        <td class="p-3 align-middle">
                                            <input type="number" step="any" name="amount" value="{{ amount }}" class="bg-[#070410] border border-purple-900/60 rounded-lg px-2.5 py-1.5 text-white w-20">
                                        </td>
                                        <td class="p-3 align-middle">
                                            <input type="text" name="validity" value="{{ validity }}" class="bg-[#070410] border border-purple-900/60 rounded-lg px-2.5 py-1.5 text-white w-24">
                                        </td>
                                        <td class="p-3 align-middle">
                                            <input type="text" name="access_link" value="{{ access_link }}" placeholder="https://t.me/+..." class="bg-[#070410] border border-purple-900/60 rounded-lg px-2.5 py-1.5 text-white min-w-[140px]">
                                        </td>
                                        <td class="p-3 text-center align-middle">
                                            <button type="submit" class="bg-purple-900/60 hover:bg-cyan-600 text-white font-tech text-[10px] px-3.5 py-1.5 rounded-lg uppercase tracking-wider transition">
                                                Update
                                            </button>
                                        </td>
                                    </form>

                                    <td class="p-3 text-center align-middle">
                                        <button type="button" onclick="openDeleteModal('{{ pid }}', '{{ name }}')" class="bg-rose-500/20 hover:bg-rose-600 text-rose-400 hover:text-white font-tech text-[10px] px-3.5 py-1.5 rounded-lg uppercase tracking-wider transition">
                                            Delete
                                        </button>
                                    </td>
                                </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

        </div>
    </main>

    <script>
        function toggleSidebar() {
            var sidebar = document.getElementById('sidebar');
            var backdrop = document.getElementById('sidebarBackdrop');
            if (!sidebar) return;
            sidebar.classList.toggle('open');
            if (backdrop) backdrop.classList.toggle('open');
        }

        var titles = {
            'tab-dashboard': 'Dashboard',
            'tab-orders': 'Customer Orders',
            'tab-users': 'Manage Users',
            'tab-broadcast': 'Mass Broadcast',
            'tab-settings': 'Setting & UPI',
            'tab-bot': 'Bot Token Config',
            'tab-media': 'Media & Greetings',
            'tab-plans': 'Subscription Plans'
        };

        function switchTab(tabId) {
            var contents = document.querySelectorAll('.tab-content');
            for (var i = 0; i < contents.length; i++) {
                contents[i].classList.add('hidden');
            }

            var target = document.getElementById(tabId);
            if (target) target.classList.remove('hidden');

            var uploadContainer = document.getElementById('videoUploadContainer');
            if (uploadContainer) {
                if (tabId === 'tab-media') {
                    uploadContainer.classList.remove('hidden');
                } else {
                    uploadContainer.classList.add('hidden');
                }
            }

            var saveBar = document.getElementById('saveBar');
            if (saveBar) {
                if (tabId === 'tab-bot' || tabId === 'tab-media') {
                    saveBar.classList.remove('hidden');
                } else {
                    saveBar.classList.add('hidden');
                }
            }

            var titleEl = document.getElementById('sectionTitle');
            if (titleEl) titleEl.innerText = titles[tabId] || 'Nagato Panel';

            var navBtns = document.querySelectorAll('.nav-btn');
            for (var j = 0; j < navBtns.length; j++) {
                navBtns[j].classList.remove('bg-purple-900/40', 'border', 'border-fuchsia-500/30', 'text-cyan-400');
                navBtns[j].classList.add('text-slate-400');
            }

            var activeNav = document.getElementById('nav-' + tabId);
            if (activeNav) {
                activeNav.classList.add('bg-purple-900/40', 'border', 'border-fuchsia-500/30', 'text-cyan-400');
                activeNav.classList.remove('text-slate-400');
            }

            if (window.innerWidth < 768) {
                var sidebar = document.getElementById('sidebar');
                if (sidebar && sidebar.classList.contains('open')) {
                    toggleSidebar();
                }
            }
        }

        var urlParams = new URLSearchParams(window.location.search);
        var requestedTab = urlParams.get('tab');
        if (requestedTab && titles[requestedTab]) {
            switchTab(requestedTab);
        }

        function openDeleteModal(planId, planName) {
            document.getElementById('modalPlanIdInput').value = planId;
            document.getElementById('modalPlanId').innerText = planId;
            document.getElementById('modalPlanName').innerText = planName;
            document.getElementById('confirmDeleteModal').classList.add('active');
        }

        function closeDeleteModal() {
            document.getElementById('confirmDeleteModal').classList.remove('active');
        }

        function openResetModal() {
            document.getElementById('confirmResetModal').classList.add('active');
        }

        function closeResetModal() {
            document.getElementById('confirmResetModal').classList.remove('active');
        }

        async function uploadDemoVideoFile() {
            var input = document.getElementById('demo_file_input');
            var statusEl = document.getElementById('uploadStatus');
            var btn = document.getElementById('uploadBtn');
            var badge = document.getElementById('uploadBadge');

            if (!input.files || input.files.length === 0) {
                statusEl.innerText = "⚠️ Please select at least one video file.";
                statusEl.className = "text-xs text-rose-400 font-mono mt-1";
                return;
            }

            var count = input.files.length;
            btn.disabled = true;
            btn.innerText = "Uploading...";
            badge.classList.remove('hidden');
            statusEl.innerText = "⏳ Uploading " + count + " video(s)... Please wait.";
            statusEl.className = "text-xs text-cyan-400 font-mono mt-1 animate-pulse";

            var formData = new FormData();
            for (var i = 0; i < count; i++) {
                formData.append("files", input.files[i]);
            }

            try {
                var res = await fetch('/admin/upload-demo-video', {
                    method: 'POST',
                    body: formData
                });

                var data = await res.json();
                btn.disabled = false;
                btn.innerText = "Upload";
                badge.classList.add('hidden');

                if (res.ok && data.status === 'success') {
                    statusEl.innerText = "✅ Successfully uploaded " + (data.urls ? data.urls.length : count) + " video(s)!";
                    statusEl.className = "text-xs text-emerald-400 font-mono mt-1";

                    var txtArea = document.querySelector('textarea[name="demo_video"]');
                    if (txtArea && data.urls) {
                        var existing = txtArea.value.trim();
                        var newUrls = data.urls.join("\\n");
                        txtArea.value = (existing ? existing + "\\n" + newUrls : newUrls).trim();
                    }
                    input.value = "";
                } else {
                    statusEl.innerText = "❌ Upload failed: " + (data.message || "Server rejected request");
                    statusEl.className = "text-xs text-rose-400 font-mono mt-1";
                }
            } catch(e) {
                btn.disabled = false;
                btn.innerText = "Upload";
                badge.classList.add('hidden');
                statusEl.innerText = "❌ Upload failed: Connection interrupted or file too large.";
                statusEl.className = "text-xs text-rose-400 font-mono mt-1";
            }
        }
    </script>
</body>
</html>"""

# ==========================================
# MASTER ROUTE CONTROLLERS
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
    return HTMLResponse(Template(MASTER_LOGIN_PAGE).render(error="Incorrect master credentials."), status_code=401)

@app.get("/master/logout")
async def master_logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/master/login", status_code=status.HTTP_303_SEE_OTHER)

@app.get("/master/clients", response_class=HTMLResponse)
async def master_clients_dashboard(request: Request, message: str | None = None):
    verify_master_auth(request)
    today = datetime.now().date()
    db = await get_master_db()
    try:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM clients ORDER BY id DESC") as cur:
            rows = await cur.fetchall()
        
        clients = []
        active_count = 0
        total_revenue = 0.0
        for r in rows:
            c = dict(r)
            exp_date = datetime.strptime(c["expiry_date"], "%Y-%m-%d").date()
            c["days_left"] = max(0, (exp_date - today).days)
            if c["status"] == "ACTIVE" and c["days_left"] > 0:
                active_count += 1
            total_revenue += float(c["price"] or 0)
            clients.append(c)
    finally:
        await db.close()

    return HTMLResponse(Template(MASTER_HTML).render(
        clients=clients,
        active_count=active_count,
        total_revenue=f"{total_revenue:,.2f}",
        message=message
    ))

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

    return RedirectResponse(url="/master/clients?message=New+panel+provisioned+successfully", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/master/password/update")
async def master_update_password(
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
):
    verify_master_auth(request)
    global MASTER_PASS
    if current_password != MASTER_PASS:
        return RedirectResponse(url="/master/clients?message=Error:+Current+master+password+incorrect", status_code=status.HTTP_303_SEE_OTHER)

    MASTER_PASS = new_password.strip()
    return RedirectResponse(url="/master/clients?message=Master+password+updated+successfully", status_code=status.HTTP_303_SEE_OTHER)

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

    return RedirectResponse(url="/master/clients?message=Client+deleted", status_code=status.HTTP_303_SEE_OTHER)

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

    return HTMLResponse(f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Admin Login &mdash; Nagato Panel</title>
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
        .custom-input:focus {{ outline: none; border-color: #38bdf8; box-shadow: 0 0 12px rgba(56, 189, 248, 0.3); }}
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
                        Nagato Panel
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

            <div class="pt-2 text-center">
                <a href="https://t.me/NAGATOxOWNER" target="_blank" rel="noopener noreferrer"
                   class="inline-flex items-center gap-1.5 text-xs text-cyan-400/80 hover:text-cyan-300 transition font-mono">
                    <svg class="w-3.5 h-3.5" fill="currentColor" viewBox="0 0 24 24">
                        <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm4.64 6.8c-.15 1.58-.8 5.42-1.13 7.19-.14.75-.42 1-.68 1.03-.58.05-1.02-.38-1.58-.75-.88-.58-1.38-.94-2.23-1.5-.99-.65-.35-1.01.22-1.59.15-.15 2.71-2.48 2.76-2.69a.2.2 0 00-.05-.18c-.06-.05-.14-.03-.21-.02-.09.02-1.49.95-4.22 2.79-.4.27-.76.41-1.08.4-.36-.01-1.04-.2-1.55-.37-.63-.2-1.12-.31-1.08-.66.02-.18.27-.36.74-.55 2.92-1.27 4.86-2.11 5.83-2.51 2.78-1.16 3.35-1.36 3.73-1.36.08 0 .27.02.39.12.1.08.13.19.14.27-.01.06.01.24 0 .38z"/>
                    </svg>
                    Contact Developer
                </a>
            </div>
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

    try:
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
    except Exception as err:
        logging.error(f"[{slug}] Dashboard render error: {err}")
        return HTMLResponse(f"""
            <div style="background:#070b14;color:#fff;font-family:sans-serif;padding:30px;">
                <h2 style="color:#f43f5e;">Template Render Exception</h2>
                <pre style="background:#111;padding:15px;border-radius:10px;color:#f87171;">{err}</pre>
            </div>
        """, status_code=500)

@app.post("/c/{slug}/admin/upload-demo-video")
async def tenant_upload_demo_video(slug: str, request: Request, files: list[UploadFile] = File(...)):
    if not request.session.get(f"tenant_auth_{slug}"):
        return JSONResponse({"status": "error", "message": "Unauthorized"}, status_code=401)

    try:
        base_url = str(request.base_url).rstrip("/")
        if "railway.app" in base_url and base_url.startswith("http://"):
            base_url = base_url.replace("http://", "https://")

        curr = await get_tenant_demo_video_list(slug)
        new_urls = []

        for f in files:
            clean_name = f"{int(datetime.now().timestamp())}_{f.filename.replace(' ', '_')}"
            dest_path = os.path.join(UPLOAD_DIR, clean_name)
            with open(dest_path, "wb") as buffer:
                shutil.copyfileobj(f.file, buffer)

            file_url = f"{base_url}/static/uploads/{clean_name}"
            new_urls.append(file_url)
            curr.append(file_url)
            await asyncio.sleep(0.01)

        await update_tenant_setting(slug, "demo_video", "\n".join(curr))
        return JSONResponse({"status": "success", "urls": new_urls})
    except Exception as e:
        logging.error(f"[{slug}] Multiple upload error: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

@app.get("/c/{slug}/api/detect-upi-name")
async def tenant_detect_upi_name(slug: str, upi_id: str, request: Request):
    if not request.session.get(f"tenant_auth_{slug}"):
        return JSONResponse({"status": "failed", "message": "Unauthorized"}, status_code=401)
    name = await detect_payee_name_from_upi(upi_id)
    if name:
        return JSONResponse(content={"status": "success", "upi_id": upi_id, "name": name})
    return JSONResponse(content={"status": "failed", "message": "Live NPCI lookup blocked. Enter name manually."})

@app.post("/c/{slug}/admin/save")
async def tenant_save_general_settings(
    slug: str,
    request: Request,
    bot_token: str = Form(""),
    welcome_text: str = Form(""),
    plans_text: str = Form(""),
    welcome_photo: str = Form(""),
    demo_video: str = Form(""),
):
    if not request.session.get(f"tenant_auth_{slug}"):
        return RedirectResponse(url=f"/c/{slug}/login", status_code=status.HTTP_303_SEE_OTHER)

    cleaned_token = bot_token.strip()
    if cleaned_token:
        await update_tenant_setting(slug, "bot_token", cleaned_token)
    if welcome_text.strip():
        await update_tenant_setting(slug, "welcome_text", welcome_text.strip())
    if plans_text.strip():
        await update_tenant_setting(slug, "plans_text", plans_text.strip())
    if welcome_photo.strip():
        await update_tenant_setting(slug, "welcome_photo", welcome_photo.strip())
    if demo_video.strip():
        await update_tenant_setting(slug, "demo_video", demo_video.strip())

    if cleaned_token:
        async with MASTER_DB_LOCK:
            db = await get_master_db()
            try:
                await db.execute("UPDATE clients SET bot_token = ? WHERE slug = ?", (cleaned_token, slug))
                await db.commit()
            finally:
                await db.close()
        await bot_engine.start_tenant_bot(slug, cleaned_token)

    return RedirectResponse(url=f"/c/{slug}/admin?message=Configurations+applied+and+saved&tab=tab-media", status_code=status.HTTP_303_SEE_OTHER)

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

    return RedirectResponse(url=f"/c/{slug}/admin?message=UPI+and+payee+details+saved+successfully&tab=tab-settings", status_code=status.HTTP_303_SEE_OTHER)

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
        return RedirectResponse(url=f"/c/{slug}/admin?message=Error:+Current+password+does+not+match&tab=tab-settings", status_code=status.HTTP_303_SEE_OTHER)

    async with MASTER_DB_LOCK:
        db = await get_master_db()
        try:
            await db.execute("UPDATE clients SET admin_password = ? WHERE slug = ?", (new_password.strip(), slug))
            await db.commit()
        finally:
            await db.close()

    return RedirectResponse(url=f"/c/{slug}/admin?message=Password+updated+successfully!&tab=tab-settings", status_code=status.HTTP_303_SEE_OTHER)

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
        return RedirectResponse(url=f"/c/{slug}/admin?message=Error:+Bot+is+offline.+Add+token+first.&tab=tab-broadcast", status_code=status.HTTP_303_SEE_OTHER)

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

    return RedirectResponse(url=f"/c/{slug}/admin?message=Broadcast+successfully+sent+to+{sent}+users!&tab=tab-broadcast", status_code=status.HTTP_303_SEE_OTHER)

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
                return RedirectResponse(url=f"/c/{slug}/admin?message=Order+not+found&tab=tab-orders", status_code=status.HTTP_303_SEE_OTHER)

            user_id, plan_name = row
            if action == "approved":
                await db.execute("UPDATE payments SET status = 'approved' WHERE id = ?", (int(order_id),))
                await db.execute("UPDATE users SET premium_status = ? WHERE user_id = ?", (plan_name, user_id))
                await db.commit()
                await bot_engine.notify_payment_approved(slug, user_id, plan_name)
                msg = f"Order #{order_id} approved and user upgraded!"
            else:
                await db.execute("UPDATE payments SET status = 'rejected' WHERE id = ?", (int(order_id),))
                await db.commit()
                bot = bot_engine.active_bots.get(slug)
                if bot:
                    try:
                        await bot.send_message(user_id, "Payment verification failed. Your order has been rejected.")
                    except Exception:
                        pass
                msg = f"Order #{order_id} rejected."
        finally:
            await db.close()

    return RedirectResponse(url=f"/c/{slug}/admin?message={urllib.parse.quote_plus(msg)}&tab=tab-orders", status_code=status.HTTP_303_SEE_OTHER)

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

    await add_tenant_new_plan(slug, plan_id.strip(), name.strip(), amount, validity.strip(), access_link.strip())
    return RedirectResponse(url=f"/c/{slug}/admin?message=New+plan+added+successfully&tab=tab-plans", status_code=status.HTTP_303_SEE_OTHER)

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

    await update_tenant_plan(slug, plan_id.strip(), name.strip(), amount, validity.strip(), access_link.strip())
    return RedirectResponse(url=f"/c/{slug}/admin?message=Plan+details+updated&tab=tab-plans", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/plans/delete")
async def tenant_delete_plan_endpoint(slug: str, request: Request, plan_id: str = Form(...)):
    if not request.session.get(f"tenant_auth_{slug}"):
        return RedirectResponse(url=f"/c/{slug}/login", status_code=status.HTTP_303_SEE_OTHER)

    await delete_tenant_plan(slug, plan_id.strip())
    return RedirectResponse(url=f"/c/{slug}/admin?message=Plan+deleted+successfully&tab=tab-plans", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/users/ban")
async def tenant_ban_user(slug: str, request: Request):
    if not request.session.get(f"tenant_auth_{slug}"):
        return RedirectResponse(url=f"/c/{slug}/login", status_code=status.HTTP_303_SEE_OTHER)

    try:
        form = await request.form()
        user_id_raw = form.get("user_id")
        status_raw = form.get("status")
        if not user_id_raw or status_raw is None:
            raise ValueError("Missing form fields")
        user_id = int(user_id_raw)
        status_int = int(status_raw)
        await set_tenant_user_ban_status(slug, user_id, status_int)
        action_text = "banned" if status_int == 1 else "unbanned"
        msg = f"User {user_id} {action_text} successfully"
    except Exception as e:
        logging.error(f"[{slug}] Ban toggle error: {e}")
        msg = "Error updating user status"

    return RedirectResponse(url=f"/c/{slug}/admin?message={urllib.parse.quote_plus(msg)}&tab=tab-users", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/users/subscription")
async def tenant_change_user_sub(slug: str, request: Request):
    if not request.session.get(f"tenant_auth_{slug}"):
        return RedirectResponse(url=f"/c/{slug}/login", status_code=status.HTTP_303_SEE_OTHER)

    try:
        form = await request.form()
        user_id_raw = form.get("user_id")
        plan_name = str(form.get("plan_name", "Free")).strip()
        if not user_id_raw:
            raise ValueError("Missing user_id")
        user_id = int(user_id_raw)
        await update_tenant_user_subscription(slug, user_id, plan_name)
        bot = bot_engine.active_bots.get(slug)
        if bot:
            if plan_name == "Free":
                try:
                    await bot.send_message(user_id, "Your premium subscription has ended. You are now on the Free tier.")
                except Exception:
                    pass
            else:
                await bot_engine.notify_payment_approved(slug, user_id, plan_name)
        msg = f"User {user_id} subscription updated to {plan_name}"
    except Exception as e:
        logging.error(f"[{slug}] Subscription update error: {e}")
        msg = "Error updating subscription"

    return RedirectResponse(url=f"/c/{slug}/admin?message={urllib.parse.quote_plus(msg)}&tab=tab-users", status_code=status.HTTP_303_SEE_OTHER)

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

    return RedirectResponse(url=f"/c/{slug}/admin?message=Revenue+counters+reset+to+zero&tab=tab-dashboard", status_code=status.HTTP_303_SEE_OTHER)

@app.get("/")
async def root():
    return RedirectResponse(url="/master/login", status_code=status.HTTP_303_SEE_OTHER)

@app.get("/health")
async def health():
    return {"status": "ok", "active_bots": len(bot_engine.active_bots)}