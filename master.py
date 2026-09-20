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

MASTER_USERNAME = os.getenv("MASTER_USER", "admin")
MASTER_PASSWORD = os.getenv("MASTER_PASS", "admin@123")
SESSION_SECRET = os.getenv("SESSION_SECRET", "super-master-key-xyz-9988")

DATA_DIR = os.getenv("DATA_DIR", "/app/data" if os.path.exists("/app/data") else ".")
MASTER_DB_PATH = os.path.join(DATA_DIR, "master_records.db")
TENANTS_DIR = os.path.join(DATA_DIR, "tenants")
STATIC_DIR = os.path.join(os.getcwd(), "static")
UPLOAD_DIR = os.path.join(STATIC_DIR, "uploads")

os.makedirs(TENANTS_DIR, exist_ok=True)
os.makedirs(UPLOAD_DIR, exist_ok=True)

GLOBAL_LOCK = asyncio.Lock()

# ==========================================
# MASTER DATABASE LAYER
# ==========================================
async def get_master_db():
    db = await aiosqlite.connect(MASTER_DB_PATH, timeout=30.0)
    await db.execute("PRAGMA journal_mode = WAL;")
    await db.execute("PRAGMA synchronous = NORMAL;")
    return db

async def init_master_database():
    async with GLOBAL_LOCK:
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
                    notes TEXT DEFAULT '',
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
# TENANT DATABASE INITIALIZATION & MIGRATION
# ==========================================
def get_tenant_db_path(slug: str) -> str:
    return os.path.join(TENANTS_DIR, f"{slug}.db")

async def get_tenant_db(slug: str):
    db = await aiosqlite.connect(get_tenant_db_path(slug), timeout=30.0)
    await db.execute("PRAGMA journal_mode = WAL;")
    await db.execute("PRAGMA synchronous = NORMAL;")
    return db

async def init_tenant_database(slug: str, initial_token: str = ""):
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
        await db.execute("CREATE INDEX IF NOT EXISTS idx_t_users_joined ON users(joined_at DESC);")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_t_users_username ON users(username);")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_t_payments_status ON payments(status);")

        defaults = {
            "bot_token": initial_token,
            "admin_chat_id": "",
            "maintenance": "off",
            "upi_id": "YOUR_UPI@okaxis",
            "payee_name": "VIP Store",
            "welcome_photo": "https://images.unsplash.com/photo-1618005182384-a83a8bd57fbe?auto=format&fit=crop&w=1200&q=80",
            "welcome_text": "🎉 Welcome to VIP Access Bot!\n\n✨ Get exclusive access to premium content\n💰 Affordable plans starting below ₹99\n✨ Daily new uploads & VIP community\n\n👇 Select an option below to proceed:",
            "plans_text": "💎 VIP MEMBERSHIP TIERS\n\n━━━━━━━━━━━━━━━━━\n⚡ Instant automated access upon screenshot confirmation\n━━━━━━━━━━━━━━━━━\n👇 Select your subscription plan:",
            "demo_video": "https://www.w3schools.com/html/mov_bbb.mp4",
        }
        for k, v in defaults.items():
            await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v))

        default_plans = [
            ("plan_1", "Bronze Tier", 49.0, "30 Days", ""),
            ("plan_2", "Silver Tier", 99.0, "30 Days", ""),
            ("plan_3", "Gold Lifetime", 199.0, "Lifetime", ""),
        ]
        for p in default_plans:
            await db.execute("INSERT OR IGNORE INTO plans (plan_id, name, amount, validity, access_link) VALUES (?, ?, ?, ?, ?)", p)
        await db.commit()
    finally:
        await db.close()

# ==========================================
# TENANT HELPER FUNCTIONS
# ==========================================
async def get_tenant_setting(slug: str, key: str) -> str:
    db = await get_tenant_db(slug)
    try:
        async with db.execute("SELECT value FROM settings WHERE key = ?", (key,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else ""
    finally:
        await db.close()

async def update_tenant_setting(slug: str, key: str, value: str):
    db = await get_tenant_db(slug)
    try:
        await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
        await db.commit()
    finally:
        await db.close()

async def get_tenant_plans(slug: str):
    db = await get_tenant_db(slug)
    try:
        async with db.execute("SELECT plan_id, name, amount, validity, COALESCE(access_link, '') FROM plans ORDER BY amount ASC") as cur:
            return await cur.fetchall()
    finally:
        await db.close()

async def get_tenant_metrics(slug: str):
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
            ORDER BY p.id DESC LIMIT 50
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

# ==========================================
# MULTI-TENANT BOT ENGINE (ISOLATED RUNNERS)
# ==========================================
class PaymentStates(StatesGroup):
    waiting_for_screenshot = State()

class MultiBotEngine:
    def __init__(self):
        self.running_tasks: dict[str, asyncio.Task] = {}
        self.active_bots: dict[str, Bot] = {}
        self.active_sessions: dict[str, AiohttpSession] = {}
        self.message_trackers: dict[str, dict[int, list[int]]] = defaultdict(lambda: defaultdict(list))

    def track(self, slug: str, chat_id: int, message_id: int):
        if message_id not in self.message_trackers[slug][chat_id]:
            self.message_trackers[slug][chat_id].append(message_id)

    async def delete_old(self, slug: str, chat_id: int, bot: Bot, exclude: list[int] | None = None):
        exclude_set = set(exclude or [])
        all_ids = [m for m in self.message_trackers[slug][chat_id] if m not in exclude_set]
        self.message_trackers[slug][chat_id] = [m for m in self.message_trackers[slug][chat_id] if m in exclude_set]
        for mid in all_ids:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=mid)
            except Exception:
                pass

    async def start_tenant_bot(self, slug: str, token: str):
        await self.stop_tenant_bot(slug)
        token = token.strip()
        if not token or ":" not in token:
            return

        session = AiohttpSession(timeout=20.0)
        bot = Bot(token=token, session=session, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        dp = Dispatcher(storage=MemoryStorage())
        
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception as e:
            logging.warning(f"[{slug}] Webhook flush warning: {e}")

        self._register_handlers(dp, slug)

        async def bot_runner():
            logging.info(f"[{slug}] Bot polling started successfully.")
            while True:
                try:
                    await dp.start_polling(bot, drop_pending_updates=True, allowed_updates=["message", "callback_query"])
                    break
                except TelegramConflictError:
                    await asyncio.sleep(5)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logging.error(f"[{slug}] Engine error: {e}. Retrying in 4s...")
                    await asyncio.sleep(4)

        self.active_bots[slug] = bot
        self.active_sessions[slug] = session
        self.running_tasks[slug] = asyncio.create_task(bot_runner())

    async def stop_tenant_bot(self, slug: str):
        if slug in self.running_tasks:
            t = self.running_tasks.pop(slug)
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
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

    def _register_handlers(self, dp: Dispatcher, slug: str):
        engine = self

        @dp.message(CommandStart())
        async def on_start(m: types.Message):
            # Track user in client DB
            db = await get_tenant_db(slug)
            try:
                await db.execute("""
                    INSERT INTO users (user_id, full_name, username) VALUES (?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET full_name=excluded.full_name, username=excluded.username
                """, (m.from_user.id, m.from_user.full_name, m.from_user.username or "N/A"))
                await db.commit()
            finally:
                await db.close()

            bot = engine.active_bots.get(slug)
            if not bot:
                return

            welcome_photo = await get_tenant_setting(slug, "welcome_photo")
            welcome_text = await get_tenant_setting(slug, "welcome_text")

            builder = InlineKeyboardBuilder()
            builder.button(text="🎬 View Demo", callback_data="demo")
            builder.button(text="💎 Upgrade", callback_data="plans")
            builder.adjust(2)

            if welcome_photo.startswith(("http://", "https://")):
                sent = await bot.send_photo(m.chat.id, photo=welcome_photo, caption=welcome_text, reply_markup=builder.as_markup())
            else:
                sent = await bot.send_message(m.chat.id, text=welcome_text, reply_markup=builder.as_markup())
            engine.track(slug, m.chat.id, sent.message_id)

        @dp.callback_query(F.data == "plans")
        async def on_plans(cb: types.CallbackQuery, state: FSMContext):
            await state.clear()
            await cb.answer()
            bot = engine.active_bots.get(slug)
            if not bot:
                return
            plans = await get_tenant_plans(slug)
            plans_text = await get_tenant_setting(slug, "plans_text")
            
            kb = InlineKeyboardBuilder()
            for pid, name, amt, val, _ in plans:
                kb.button(text=f"🔥 {name} - ₹{int(amt)}", callback_data=f"buy:{pid}")
            kb.button(text="🏠 Back to Home", callback_data="home")
            kb.adjust(1)

            try:
                await cb.message.delete()
            except Exception:
                pass
            sent = await bot.send_message(cb.message.chat.id, text=plans_text, reply_markup=kb.as_markup())
            engine.track(slug, cb.message.chat.id, sent.message_id)

        @dp.callback_query(F.data.startswith("buy:"))
        async def on_buy(cb: types.CallbackQuery, state: FSMContext):
            await cb.answer()
            pid = cb.data.split(":")[1]
            db = await get_tenant_db(slug)
            try:
                async with db.execute("SELECT plan_id, name, amount, validity FROM plans WHERE plan_id = ?", (pid,)) as cur:
                    plan = await cur.fetchone()
            finally:
                await db.close()

            if not plan:
                return
            _, name, amount, validity = plan
            upi_id = await get_tenant_setting(slug, "upi_id")
            payee_name = await get_tenant_setting(slug, "payee_name")

            # Non-blocking QR generator
            upi_url = f"upi://pay?pa={urllib.parse.quote(upi_id)}&pn={urllib.parse.quote(payee_name)}&am={amount:.2f}&cu=INR&tn=Order"
            qr = segno.make(upi_url, error="m")
            buf = io.BytesIO()
            qr.save(buf, kind="png", scale=7, border=2)
            buf.seek(0)

            caption = (
                f"📲 <b>UPI PAYMENT GATEWAY</b>\n\n"
                f"📦 Plan: <b>{name}</b>\n"
                f"💰 Price: <b>₹{amount:.2f}</b> ({validity})\n"
                f"📱 UPI: <code>{upi_id}</code>\n"
                f"👤 Name: <b>{payee_name}</b>\n\n"
                f"1️⃣ Pay exact amount by scanning QR\n"
                f"2️⃣ Click Check Payment & submit screenshot"
            )
            kb = InlineKeyboardBuilder()
            kb.button(text="📥 CHECK PAYMENT", callback_data="submit_proof")
            kb.button(text="🔙 Back", callback_data="plans")
            kb.adjust(1)

            await state.update_data(plan_name=name, amount=amount)
            try:
                await cb.message.delete()
            except Exception:
                pass
            bot = engine.active_bots[slug]
            sent = await bot.send_photo(cb.message.chat.id, photo=BufferedInputFile(buf.getvalue(), "qr.png"), caption=caption, reply_markup=kb.as_markup())
            engine.track(slug, cb.message.chat.id, sent.message_id)

        @dp.callback_query(F.data == "submit_proof")
        async def on_submit(cb: types.CallbackQuery, state: FSMContext):
            await cb.answer()
            await state.set_state(PaymentStates.waiting_for_screenshot)
            bot = engine.active_bots[slug]
            sent = await bot.send_message(cb.message.chat.id, "📸 <b>Send Payment Screenshot:</b>\n\nPlease send the screenshot image here to verify your order.")
            engine.track(slug, cb.message.chat.id, sent.message_id)

        @dp.message(PaymentStates.waiting_for_screenshot, F.photo)
        async def on_proof_received(m: types.Message, state: FSMContext):
            data = await state.get_data()
            plan_name = data.get("plan_name", "VIP Plan")
            amount = data.get("amount", 0.0)

            db = await get_tenant_db(slug)
            try:
                cur = await db.execute("INSERT INTO payments (user_id, plan_name, amount) VALUES (?, ?, ?)", (m.from_user.id, plan_name, amount))
                order_id = cur.lastrowid
                await db.commit()
            finally:
                await db.close()

            await state.clear()
            bot = engine.active_bots[slug]
            await m.answer("✅ <b>Screenshot Received!</b>\nYour order is pending administrator verification.")

            # Forward to client admin chat ID if set
            admin_chat_str = await get_tenant_setting(slug, "admin_chat_id")
            if admin_chat_str and admin_chat_str.strip().lstrip("-").isdigit():
                admin_chat_id = int(admin_chat_str.strip())
                kb = InlineKeyboardBuilder()
                kb.button(text="✅ Approve", callback_data=f"adm_pay:{order_id}:approved")
                kb.button(text="❌ Reject", callback_data=f"adm_pay:{order_id}:rejected")
                kb.adjust(2)
                caption = f"🔔 <b>New Payment Received (#{order_id})</b>\nUser: @{m.from_user.username}\nID: <code>{m.from_user.id}</code>\nPlan: {plan_name}\nAmount: ₹{amount}"
                try:
                    await bot.send_photo(admin_chat_id, photo=m.photo[-1].file_id, caption=caption, reply_markup=kb.as_markup())
                except Exception:
                    pass

        @dp.callback_query(F.data.startswith("adm_pay:"))
        async def on_admin_action(cb: types.CallbackQuery):
            _, pid, act = cb.data.split(":")
            db = await get_tenant_db(slug)
            try:
                async with db.execute("SELECT user_id, plan_name FROM payments WHERE id = ?", (int(pid),)) as cur:
                    order = await cur.fetchone()
                if not order:
                    await cb.answer("Order not found.")
                    return
                uid, plan_name = order
                if act == "approved":
                    await db.execute("UPDATE payments SET status = 'approved' WHERE id = ?", (int(pid),))
                    await db.execute("UPDATE users SET premium_status = ? WHERE user_id = ?", (plan_name, uid))
                    await db.commit()
                    bot = engine.active_bots[slug]
                    try:
                        await bot.send_message(uid, f"🎉 <b>Payment Approved!</b> Your subscription for {plan_name} is active.")
                    except Exception:
                        pass
                    await cb.message.edit_caption(caption=cb.message.caption + "\n\nSTATUS: APPROVED ✅")
                else:
                    await db.execute("UPDATE payments SET status = 'rejected' WHERE id = ?", (int(pid),))
                    await db.commit()
                    await cb.message.edit_caption(caption=cb.message.caption + "\n\nSTATUS: REJECTED ❌")
            finally:
                await db.close()
            await cb.answer("Processed.")

bot_engine = MultiBotEngine()

# ==========================================
# FASTAPI APP & MASTER LIFECYCLE
# ==========================================
@asynccontextmanager
async def app_lifespan(app: FastAPI):
    await init_master_database()
    
    # Auto-spawn bots for all active clients on server boot
    db = await get_master_db()
    try:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT slug, bot_token, expiry_date, status FROM clients") as cur:
            clients = await cur.fetchall()
        now = datetime.now()
        for c in clients:
            if c["status"] == "ACTIVE" and datetime.strptime(c["expiry_date"], "%Y-%m-%d") > now:
                if c["bot_token"]:
                    await init_tenant_database(c["slug"], c["bot_token"])
                    await bot_engine.start_tenant_bot(c["slug"], c["bot_token"])
    finally:
        await db.close()

    yield

    # Clean shutdown of all running bot dispatchers
    for slug in list(bot_engine.running_tasks.keys()):
        await bot_engine.stop_tenant_bot(slug)

app = FastAPI(title="SaaS Reseller & Telegram Master Cloud", lifespan=app_lifespan)
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# ==========================================
# MASTER ADMIN DASHBOARD HTML TEMPLATES
# ==========================================
MASTER_LOGIN_PAGE = """<!DOCTYPE html>
<html>
<head>
    <title>Master Access &mdash; Reseller Cloud</title>
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

# Layout identical to the user's provided Black Wolf Panel screenshot
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
    <!-- Sidebar (matching screenshot) -->
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
                <a href="/master/clients" class="flex items-center gap-2 px-3 py-2.5 rounded-xl bg-blue-600/20 text-blue-400 border border-blue-500/30">
                    👥 All Clients
                </a>
                <a href="/master/client/new" class="flex items-center gap-2 px-3 py-2.5 rounded-xl text-slate-400 hover:bg-slate-800/50 hover:text-white transition">
                    ➕ New Panel
                </a>
            </nav>
        </div>
        <div>
            <a href="/master/logout" class="block text-center text-xs text-rose-400 hover:bg-rose-500/10 py-2 rounded-lg font-mono">Log Out</a>
        </div>
    </aside>

    <!-- Main Content List -->
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
                                <form method="POST" action="/master/client/delete" onsubmit="return confirm('Delete client completely? Database will be purged.');">
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
                    <input type="text" name="client_name" required placeholder="e.g. VIP Deals Hub" class="w-full bg-[#060913] border border-slate-700 rounded-lg p-2.5 text-white">
                </div>
                <div>
                    <label class="text-slate-400 block mb-1">TELEGRAM USERNAME</label>
                    <input type="text" name="telegram" placeholder="@client_username" class="w-full bg-[#060913] border border-slate-700 rounded-lg p-2.5 text-white">
                </div>
            </div>

            <div class="grid grid-cols-2 gap-4">
                <div>
                    <label class="text-slate-400 block mb-1">FOLDER / URL SLUG *</label>
                    <input type="text" name="slug" required placeholder="vip-deals" class="w-full bg-[#060913] border border-slate-700 rounded-lg p-2.5 text-white">
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
# MASTER ROUTES & AUTH
# ==========================================
def verify_master_session(request: Request):
    if not request.session.get("master_logged_in"):
        raise HTTPException(status_code=status.HTTP_303_SEE_OTHER, headers={"Location": "/master/login"})

@app.get("/master/login", response_class=HTMLResponse)
async def master_login_view(error: str | None = None):
    return HTMLResponse(Template(MASTER_LOGIN_PAGE).render(error=error))

@app.post("/master/login")
async def master_login_post(request: Request, username: str = Form(...), password: str = Form(...)):
    if username == MASTER_USERNAME and password == MASTER_PASSWORD:
        request.session["master_logged_in"] = True
        return RedirectResponse(url="/master/clients", status_code=status.HTTP_303_SEE_OTHER)
    return HTMLResponse(Template(MASTER_LOGIN_PAGE).render(error="Incorrect credentials."), status_code=401)

@app.get("/master/logout")
async def master_logout_view(request: Request):
    request.session.clear()
    return RedirectResponse(url="/master/login", status_code=status.HTTP_303_SEE_OTHER)

@app.get("/master/clients", response_class=HTMLResponse)
async def master_clients_view(request: Request):
    verify_master_session(request)
    now = datetime.now()
    db = await get_master_db()
    try:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM clients ORDER BY id DESC") as cur:
            rows = await cur.fetchall()
        
        clients = []
        for r in rows:
            c = dict(r)
            exp = datetime.strptime(c["expiry_date"], "%Y-%m-%d")
            c["days_left"] = (exp - now).days
            clients.append(c)
    finally:
        await db.close()

    return HTMLResponse(Template(MASTER_DASHBOARD_PAGE).render(clients=clients))

@app.get("/master/client/new", response_class=HTMLResponse)
async def master_new_client_view(request: Request):
    verify_master_session(request)
    return HTMLResponse(MASTER_NEW_CLIENT_PAGE)

@app.post("/master/client/new")
async def master_create_client_post(
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
    verify_master_session(request)
    clean_slug = slug.strip().lower().replace(" ", "-")
    today = datetime.now().strftime("%Y-%m-%d")
    expiry = (datetime.now() + timedelta(days=plan_days)).strftime("%Y-%m-%d")

    async with GLOBAL_LOCK:
        db = await get_master_db()
        try:
            await db.execute("""
                INSERT INTO clients (client_name, telegram, slug, make_date, expiry_date, plan_days, price, admin_username, admin_password, bot_token)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (client_name, telegram, clean_slug, today, expiry, plan_days, price, admin_username, admin_password, bot_token.strip()))
            await db.commit()
        except aiosqlite.IntegrityError:
            raise HTTPException(status_code=400, detail="Slug already taken. Pick a unique one.")
        finally:
            await db.close()

    # Provision client database and launch bot
    await init_tenant_database(clean_slug, bot_token.strip())
    if bot_token.strip():
        await bot_engine.start_tenant_bot(clean_slug, bot_token.strip())

    return RedirectResponse(url="/master/clients", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/master/client/delete")
async def master_delete_client_post(request: Request, client_id: int = Form(...)):
    verify_master_session(request)
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
            
            # Delete client SQLite database file
            db_path = get_tenant_db_path(slug)
            if os.path.exists(db_path):
                os.remove(db_path)
    finally:
        await db.close()

    return RedirectResponse(url="/master/clients", status_code=status.HTTP_303_SEE_OTHER)

# ==========================================
# CLIENT DEDICATED ADMIN PANEL & AUTH
# ==========================================
async def get_tenant_record(slug: str):
    db = await get_master_db()
    try:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM clients WHERE slug = ?", (slug,)) as cur:
            return await cur.fetchone()
    finally:
        await db.close()

@app.get("/c/{slug}/login", response_class=HTMLResponse)
async def tenant_login_view(slug: str, error: str | None = None):
    client = await get_tenant_record(slug)
    if not client:
        raise HTTPException(status_code=404, detail="Store panel does not exist.")
    return HTMLResponse(Template(LOGIN_PAGE).render(error=error))

@app.post("/c/{slug}/login")
async def tenant_login_post(slug: str, request: Request, username: str = Form(...), password: str = Form(...)):
    client = await get_tenant_record(slug)
    if not client:
        raise HTTPException(status_code=404, detail="Store panel not found.")

    if username == client["admin_username"] and password == client["admin_password"]:
        request.session[f"tenant_auth_{slug}"] = True
        return RedirectResponse(url=f"/c/{slug}/admin", status_code=status.HTTP_303_SEE_OTHER)

    return HTMLResponse(Template(LOGIN_PAGE).render(error="Invalid administrator credentials."), status_code=401)

@app.get("/c/{slug}/logout")
async def tenant_logout_view(slug: str, request: Request):
    request.session.pop(f"tenant_auth_{slug}", None)
    return RedirectResponse(url=f"/c/{slug}/login", status_code=status.HTTP_303_SEE_OTHER)

def require_tenant_auth(slug: str, request: Request):
    if not request.session.get(f"tenant_auth_{slug}"):
        raise HTTPException(status_code=status.HTTP_303_SEE_OTHER, headers={"Location": f"/c/{slug}/login"})

# ==========================================
# CLIENT DASHBOARD & API CONTROLLERS
# ==========================================
@app.get("/c/{slug}/admin", response_class=HTMLResponse)
async def tenant_admin_dashboard(
    slug: str,
    request: Request,
    message: str | None = None,
    page: int = 1,
    user_search: str = "",
):
    require_tenant_auth(slug, request)
    client = await get_tenant_record(slug)
    if not client:
        raise HTTPException(status_code=404, detail="Store does not exist.")

    # Check Plan Expiration
    exp_date = datetime.strptime(client["expiry_date"], "%Y-%m-%d")
    if exp_date < datetime.now():
        return HTMLResponse(f"""
            <div style="background:#070b14;color:#fff;font-family:sans-serif;height:100vh;display:flex;flex-direction:column;justify-content:center;align-items:center;">
                <h1 style="color:#f43f5e;">Panel Subscription Expired</h1>
                <p>Your subscription ended on {client['expiry_date']}. Please contact the developer to renew access.</p>
            </div>
        """)

    metrics = await get_tenant_metrics(slug)

    # Paginated user list for this client
    limit = 50
    offset = (max(1, page) - 1) * limit
    db = await get_tenant_db(slug)
    try:
        search_query = f"%{user_search.strip().lstrip('@')}%"
        if user_search.strip():
            async with db.execute("SELECT COUNT(*) FROM users WHERE CAST(user_id AS TEXT) LIKE ? OR username LIKE ? OR full_name LIKE ?", (search_query, search_query, search_query)) as cur:
                users_total = (await cur.fetchone())[0]
            async with db.execute("SELECT user_id, full_name, username, joined_at, premium_status, is_banned FROM users WHERE CAST(user_id AS TEXT) LIKE ? OR username LIKE ? OR full_name LIKE ? ORDER BY joined_at DESC LIMIT ? OFFSET ?", (search_query, search_query, search_query, limit, offset)) as cur:
                users_list = await cur.fetchall()
        else:
            async with db.execute("SELECT COUNT(*) FROM users") as cur:
                users_total = (await cur.fetchone())[0]
            async with db.execute("SELECT user_id, full_name, username, joined_at, premium_status, is_banned FROM users ORDER BY joined_at DESC LIMIT ? OFFSET ?", (limit, offset)) as cur:
                users_list = await cur.fetchall()
    finally:
        await db.close()

    total_pages = max(1, math.ceil(users_total / limit))

    context = {
        "message": message,
        "admin_username": client["admin_username"],
        "is_online": slug in bot_engine.active_bots,
        "paid_orders": metrics["paid_orders"],
        "revenue": metrics["revenue"],
        "total_users": metrics["total_users"],
        "recent_orders": metrics["recent_orders"],
        "all_orders": metrics["all_orders"],
        "users_list": users_list,
        "users_total": users_total,
        "current_page": page,
        "total_pages": total_pages,
        "user_search": user_search,
        "bot_token": await get_tenant_setting(slug, "bot_token"),
        "admin_chat_id": await get_tenant_setting(slug, "admin_chat_id"),
        "upi_id": await get_tenant_setting(slug, "upi_id"),
        "payee_name": await get_tenant_setting(slug, "payee_name"),
        "welcome_text": await get_tenant_setting(slug, "welcome_text"),
        "plans_text": await get_tenant_setting(slug, "plans_text"),
        "welcome_photo": await get_tenant_setting(slug, "welcome_photo"),
        "demo_video": await get_tenant_setting(slug, "demo_video"),
        "plans": await get_tenant_plans(slug),
    }

    # Re-route form action targets dynamically to keep actions contained inside this specific client slug
    adjusted_dashboard = DASHBOARD_PAGE.replace('/admin/', f'/c/{slug}/admin/').replace('/logout', f'/c/{slug}/logout')
    return HTMLResponse(Template(adjusted_dashboard).render(**context))

@app.post("/c/{slug}/admin/save")
async def tenant_save_settings(
    slug: str,
    request: Request,
    bot_token: str = Form(...),
    welcome_text: str = Form(...),
    plans_text: str = Form(...),
    welcome_photo: str = Form(...),
    demo_video: str = Form(...),
):
    require_tenant_auth(slug, request)
    cleaned_token = bot_token.strip()

    await update_tenant_setting(slug, "bot_token", cleaned_token)
    await update_tenant_setting(slug, "welcome_text", welcome_text.strip())
    await update_tenant_setting(slug, "plans_text", plans_text.strip())
    await update_tenant_setting(slug, "welcome_photo", welcome_photo.strip())
    await update_tenant_setting(slug, "demo_video", demo_video.strip())

    # Update master record & restart this client's bot instance
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
    require_tenant_auth(slug, request)
    await update_tenant_setting(slug, "upi_id", upi_id.strip())
    await update_tenant_setting(slug, "payee_name", payee_name.strip())
    await update_tenant_setting(slug, "admin_chat_id", admin_chat_id.strip())
    return RedirectResponse(url=f"/c/{slug}/admin?message=UPI+Credentials+Saved&tab=tab-settings", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/plans/add")
async def tenant_add_plan(
    slug: str,
    request: Request,
    plan_id: str = Form(...),
    name: str = Form(...),
    amount: float = Form(...),
    validity: str = Form(...),
    access_link: str = Form(""),
):
    require_tenant_auth(slug, request)
    db = await get_tenant_db(slug)
    try:
        await db.execute("INSERT INTO plans (plan_id, name, amount, validity, access_link) VALUES (?, ?, ?, ?, ?)", (plan_id.strip(), name.strip(), amount, validity.strip(), access_link.strip()))
        await db.commit()
    finally:
        await db.close()
    return RedirectResponse(url=f"/c/{slug}/admin?message=Plan+Added&tab=tab-plans", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/plans/delete")
async def tenant_delete_plan(slug: str, request: Request, plan_id: str = Form(...)):
    require_tenant_auth(slug, request)
    db = await get_tenant_db(slug)
    try:
        await db.execute("DELETE FROM plans WHERE plan_id = ?", (plan_id.strip(),))
        await db.commit()
    finally:
        await db.close()
    return RedirectResponse(url=f"/c/{slug}/admin?message=Plan+Deleted&tab=tab-plans", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/broadcast/send")
async def tenant_broadcast(
    slug: str,
    request: Request,
    broadcast_message: str = Form(...),
    broadcast_photo: str = Form(""),
):
    require_tenant_auth(slug, request)
    bot = bot_engine.active_bots.get(slug)
    if not bot:
        return RedirectResponse(url=f"/c/{slug}/admin?message=Bot+is+offline.+Add+token+first.&tab=tab-broadcast", status_code=status.HTTP_303_SEE_OTHER)

    db = await get_tenant_db(slug)
    try:
        async with db.execute("SELECT user_id FROM users WHERE is_banned = 0") as cur:
            users = await cur.fetchall()
    finally:
        await db.close()

    sent = 0
    photo = broadcast_photo.strip()
    for (uid,) in users:
        try:
            if photo:
                await bot.send_photo(uid, photo=photo, caption=broadcast_message)
            else:
                await bot.send_message(uid, text=broadcast_message)
            sent += 1
            await asyncio.sleep(0.04)
        except Exception:
            pass

    return RedirectResponse(url=f"/c/{slug}/admin?message=Broadcast+delivered+to+{sent}+users!&tab=tab-broadcast", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/orders/status")
async def tenant_order_status(
    slug: str,
    request: Request,
    order_id: int = Form(...),
    action: str = Form(...),
):
    require_tenant_auth(slug, request)
    db = await get_tenant_db(slug)
    try:
        async with db.execute("SELECT user_id, plan_name FROM payments WHERE id = ?", (int(order_id),)) as cur:
            row = await cur.fetchone()
        if not row:
            return RedirectResponse(url=f"/c/{slug}/admin?tab=tab-orders", status_code=status.HTTP_303_SEE_OTHER)
        uid, plan_name = row

        if action == "approved":
            await db.execute("UPDATE payments SET status = 'approved' WHERE id = ?", (int(order_id),))
            await db.execute("UPDATE users SET premium_status = ? WHERE user_id = ?", (plan_name, uid))
            await db.commit()
            bot = bot_engine.active_bots.get(slug)
            if bot:
                try:
                    await bot.send_message(uid, f"🎉 <b>Payment Approved!</b> Your subscription for {plan_name} is active.")
                except Exception:
                    pass
        else:
            await db.execute("UPDATE payments SET status = 'rejected' WHERE id = ?", (int(order_id),))
            await db.commit()
    finally:
        await db.close()

    return RedirectResponse(url=f"/c/{slug}/admin?tab=tab-orders", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/c/{slug}/admin/upload-demo-video")
async def tenant_upload_demo(slug: str, request: Request, files: list[UploadFile] = File(...)):
    require_tenant_auth(slug, request)
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
