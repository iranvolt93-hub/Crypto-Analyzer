# -*- coding: utf-8 -*-
"""
Crypto Analyzer - Telegram Bot
Production-friendly single-file version for Railway.

IMPORTANT:
1) Railway Start Command must be: python main.py
2) Keep only ONE running instance of this bot token.
3) Required Railway variable: TELEGRAM_BOT_TOKEN
4) Optional variables:
   ADMIN_IDS=123456789,987654321
   SUPPORT_USERNAME=@YourSupport
   PAYMENT_CARD=6037...
   DB_PATH=/app/data/crypto_bot.db
   MAX_WATCHLIST=100

This bot does analysis/signals only. It does NOT execute trades.
"""

import os
import re
import json
import math
import time
import asyncio
import logging
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Optional, Any

import aiohttp

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "").strip()
PAYMENT_CARD = os.getenv("PAYMENT_CARD", "").strip()

DB_PATH = os.getenv("DB_PATH", "/app/data/crypto_bot.db").strip()
MAX_WATCHLIST = max(1, int(os.getenv("MAX_WATCHLIST", "100")))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))

CG_BASE = "https://api.coingecko.com/api/v3"
TELEGRAM_REQUEST_TIMEOUT = 30

PLANS = {
    "30": {"days": 30, "price": 200000, "title": "۳۰ روزه"},
    "90": {"days": 90, "price": 350000, "title": "۹۰ روزه"},
    "180": {"days": 180, "price": 500000, "title": "۱۸۰ روزه"},
}

ASSET_ALIASES = {
    "btc": "bitcoin",
    "bitcoin": "bitcoin",
    "بیتکوین": "bitcoin",
    "eth": "ethereum",
    "ethereum": "ethereum",
    "اتریوم": "ethereum",
    "zec": "zcash",
    "zcash": "zcash",
    "زیک": "zcash",
    "sol": "solana",
    "solana": "solana",
    "سولانا": "solana",
    "bnb": "binancecoin",
    "binance": "binancecoin",
    "xrp": "ripple",
    "ada": "cardano",
    "doge": "dogecoin",
    "dogecoin": "dogecoin",
    "trx": "tron",
    "dot": "polkadot",
    "avax": "avalanche-2",
    "link": "chainlink",
    "matic": "matic-network",
    "pol": "polygon-ecosystem-token",
}

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("crypto-analyzer")

# ---------------------------------------------------------------------------
# DATABASE
# ---------------------------------------------------------------------------

DB_LOCK = asyncio.Lock()


def ensure_db_dir() -> None:
    folder = os.path.dirname(DB_PATH)
    if folder:
        os.makedirs(folder, exist_ok=True)


def db_connect() -> sqlite3.Connection:
    ensure_db_dir()
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db() -> None:
    conn = db_connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                created_at TEXT NOT NULL,
                last_seen_at TEXT,
                blocked INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS watchlist (
                user_id INTEGER NOT NULL,
                coin_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (user_id, coin_id)
            );

            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                plan TEXT NOT NULL,
                start_at TEXT NOT NULL,
                end_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'manual'
            );

            CREATE TABLE IF NOT EXISTS payment_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                plan TEXT NOT NULL,
                amount INTEGER NOT NULL,
                receipt_file_id TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                reviewed_at TEXT,
                reviewed_by INTEGER
            );

            CREATE TABLE IF NOT EXISTS alert_state (
                user_id INTEGER NOT NULL,
                coin_id TEXT NOT NULL,
                last_signal TEXT,
                last_sent_at TEXT,
                PRIMARY KEY (user_id, coin_id)
            );

            CREATE TABLE IF NOT EXISTS settings (
                user_id INTEGER PRIMARY KEY,
                alerts_enabled INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS admin_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                target_user INTEGER,
                created_at TEXT NOT NULL
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


async def upsert_user(update: Update) -> None:
    if not update.effective_user:
        return
    u = update.effective_user
    async with DB_LOCK:
        conn = db_connect()
        try:
            conn.execute(
                """
                INSERT INTO users(user_id, username, first_name, created_at, last_seen_at)
                VALUES(?,?,?,?,?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username=excluded.username,
                    first_name=excluded.first_name,
                    last_seen_at=excluded.last_seen_at
                """,
                (
                    u.id,
                    u.username or "",
                    u.first_name or "",
                    now_iso(),
                    now_iso(),
                ),
            )
            conn.execute(
                "INSERT OR IGNORE INTO settings(user_id, alerts_enabled) VALUES(?,0)",
                (u.id,),
            )
            conn.commit()
        finally:
            conn.close()


def is_blocked(user_id: int) -> bool:
    conn = db_connect()
    try:
        row = conn.execute(
            "SELECT blocked FROM users WHERE user_id=?", (user_id,)
        ).fetchone()
        return bool(row and row["blocked"])
    finally:
        conn.close()


def get_subscription(user_id: int) -> Optional[sqlite3.Row]:
    conn = db_connect()
    try:
        return conn.execute(
            """
            SELECT * FROM subscriptions
            WHERE user_id=? AND datetime(end_at) > datetime('now')
            ORDER BY datetime(end_at) DESC LIMIT 1
            """,
            (user_id,),
        ).fetchone()
    finally:
        conn.close()


def subscription_active(user_id: int) -> bool:
    return get_subscription(user_id) is not None


def add_subscription(user_id: int, plan_key: str, days: int, admin_id: int) -> None:
    current = get_subscription(user_id)
    start = datetime.now(timezone.utc)
    if current:
        old_end = parse_iso(current["end_at"])
        if old_end and old_end > start:
            start = old_end
    end = start + timedelta(days=days)

    conn = db_connect()
    try:
        conn.execute(
            """
            INSERT INTO subscriptions(user_id, plan, start_at, end_at, created_at, source)
            VALUES(?,?,?,?,?,?)
            """,
            (user_id, plan_key, start.isoformat(), end.isoformat(), now_iso(), "admin"),
        )
        conn.execute(
            """
            INSERT INTO admin_log(admin_id, action, target_user, created_at)
            VALUES(?,?,?,?)
            """,
            (admin_id, f"extend_{plan_key}", user_id, now_iso()),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def money(n: int) -> str:
    return f"{n:,}".replace(",", "٬")


def pct(n: float) -> str:
    return f"{n:.1f}%"


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def normalize_symbol(text: str) -> str:
    s = text.strip().upper().replace("/", "").replace("-", "").replace(" ", "")
    if s.endswith("USDT"):
        s = s[:-4]
    return s


def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📋 واچ‌لیست"), KeyboardButton("➕ افزودن ارز")],
            [KeyboardButton("📊 تحلیل"), KeyboardButton("🚨 سیگنال‌ها")],
            [KeyboardButton("💳 خرید اشتراک"), KeyboardButton("👤 وضعیت اشتراک")],
            [KeyboardButton("🔔 هشدارها"), KeyboardButton("ℹ️ راهنما")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📊 آمار", callback_data="admin:stats"),
                InlineKeyboardButton("💳 پرداخت‌ها", callback_data="admin:payments"),
            ],
            [
                InlineKeyboardButton("👥 کاربران", callback_data="admin:users"),
            ],
        ]
    )


async def send_home(update: Update, text: Optional[str] = None) -> None:
    if text is None:
        text = (
            "🤖 <b>Crypto Analyzer</b>\n\n"
            "تحلیل و سیگنال بازار ارزهای دیجیتال\n"
            "بدون اجرای خودکار معامله.\n\n"
            "از منوی پایین یک گزینه را انتخاب کنید."
        )
    await update.effective_message.reply_text(
        text, parse_mode=ParseMode.HTML, reply_markup=main_keyboard()
    )


# ---------------------------------------------------------------------------
# COINGECKO
# ---------------------------------------------------------------------------

async def cg_get(path: str, params: Optional[dict] = None) -> Any:
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    headers = {
        "Accept": "application/json",
        "User-Agent": "CryptoAnalyzer/1.0",
    }
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        async with session.get(CG_BASE + path, params=params or {}) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"CoinGecko HTTP {resp.status}: {text[:300]}")
            return json.loads(text)


async def search_coins(query: str) -> list[dict]:
    q = query.strip()
    if not q:
        return []

    alias = ASSET_ALIASES.get(q.lower())
    if alias:
        q = alias

    data = await cg_get("/search", {"query": q})
    coins = data.get("coins", [])
    return [
        {
            "id": c.get("id", ""),
            "name": c.get("name", ""),
            "symbol": c.get("symbol", ""),
            "rank": c.get("market_cap_rank"),
        }
        for c in coins[:10]
        if c.get("id")
    ]


async def get_market(coin_id: str) -> dict:
    data = await cg_get(
        "/coins/markets",
        {
            "vs_currency": "usd",
            "ids": coin_id,
            "price_change_percentage": "24h,7d,30d",
            "sparkline": "true",
        },
    )
    if not data:
        raise RuntimeError("Asset not found")
    return data[0]


async def get_chart(coin_id: str, days: int = 30) -> list[float]:
    data = await cg_get(
        f"/coins/{coin_id}/market_chart",
        {"vs_currency": "usd", "days": days, "interval": "daily"},
    )
    return [safe_float(x[1]) for x in data.get("prices", []) if len(x) >= 2]


# ---------------------------------------------------------------------------
# TECHNICAL ANALYSIS
# ---------------------------------------------------------------------------

def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    k = 2.0 / (period + 1.0)
    result = [values[0]]
    for price in values[1:]:
        result.append(price * k + result[-1] * (1 - k))
    return result


def rsi(values: list[float], period: int = 14) -> float:
    if len(values) < period + 1:
        return 50.0
    gains = []
    losses = []
    for i in range(1, len(values)):
        d = values[i] - values[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def macd(values: list[float]) -> tuple[float, float, float]:
    if len(values) < 35:
        return 0.0, 0.0, 0.0
    e12 = ema(values, 12)
    e26 = ema(values, 26)
    line = [a - b for a, b in zip(e12[-len(e26):], e26)]
    signal = ema(line, 9)
    m = line[-1]
    s = signal[-1] if signal else 0.0
    return m, s, m - s


def analyze_prices(values: list[float]) -> dict:
    if len(values) < 20:
        return {
            "signal": "WAIT",
            "strength": 0.0,
            "profit_probability": 50.0,
            "rsi": 50.0,
            "ema20": 0.0,
            "ema50": 0.0,
            "macd": 0.0,
            "macd_signal": 0.0,
            "return_7d": 0.0,
            "return_30d": 0.0,
            "reason": "داده کافی برای تحلیل کامل وجود ندارد.",
        }

    e20 = ema(values, 20)[-1]
    e50 = ema(values, min(50, len(values)))[-1]
    rv = rsi(values)
    ml, ms, mh = macd(values)

    ret7 = 0.0
    if len(values) >= 8 and values[-8] != 0:
        ret7 = (values[-1] / values[-8] - 1) * 100

    ret30 = 0.0
    if len(values) >= 31 and values[-31] != 0:
        ret30 = (values[-1] / values[-31] - 1) * 100

    score = 0.0
    reasons = []

    if e20 > e50:
        score += 25
        reasons.append("EMA20 بالاتر از EMA50")
    else:
        score -= 25
        reasons.append("EMA20 پایین‌تر از EMA50")

    if rv >= 55:
        score += 20
        reasons.append("RSI متمایل به قدرت خریداران")
    elif rv <= 45:
        score -= 20
        reasons.append("RSI متمایل به قدرت فروشندگان")
    else:
        reasons.append("RSI در ناحیه میانی")

    if mh > 0:
        score += 20
        reasons.append("MACD مثبت")
    else:
        score -= 20
        reasons.append("MACD منفی")

    if ret7 > 0:
        score += 15
    else:
        score -= 15

    if ret30 > 0:
        score += 20
    else:
        score -= 20

    strength = min(100.0, max(0.0, 50.0 + score / 2.0))

    if score >= 35:
        signal = "BUY"
    elif score <= -35:
        signal = "SELL"
    else:
        signal = "WAIT"

    # Separate from strength: this is a heuristic confidence estimate,
    # not a promise of profit.
    probability = min(
        85.0,
        max(
            15.0,
            50.0
            + abs(score) * 0.35
            + (5.0 if (signal == "BUY" and ret7 > 0) else 0.0)
            + (5.0 if (signal == "SELL" and ret7 < 0) else 0.0),
        ),
    )

    return {
        "signal": signal,
        "strength": strength,
        "profit_probability": probability,
        "rsi": rv,
        "ema20": e20,
        "ema50": e50,
        "macd": ml,
        "macd_signal": ms,
        "return_7d": ret7,
        "return_30d": ret30,
        "reason": "، ".join(reasons),
    }


def signal_fa(signal: str) -> str:
    return {
        "BUY": "🟢 خرید",
        "SELL": "🔴 فروش",
        "WAIT": "🟡 صبر",
    }.get(signal, "🟡 صبر")


# ---------------------------------------------------------------------------
# WATCHLIST
# ---------------------------------------------------------------------------

def get_watchlist(user_id: int) -> list[sqlite3.Row]:
    conn = db_connect()
    try:
        return conn.execute(
            "SELECT * FROM watchlist WHERE user_id=? ORDER BY created_at DESC",
            (user_id,),
        ).fetchall()
    finally:
        conn.close()


def add_watch(user_id: int, coin: dict) -> tuple[bool, str]:
    items = get_watchlist(user_id)
    if len(items) >= MAX_WATCHLIST:
        return False, f"حداکثر {MAX_WATCHLIST} ارز می‌توانید اضافه کنید."

    conn = db_connect()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO watchlist(user_id, coin_id, symbol, name, created_at)
            VALUES(?,?,?,?,?)
            """,
            (
                user_id,
                coin["id"],
                coin["symbol"].upper(),
                coin["name"],
                now_iso(),
            ),
        )
        conn.commit()
        return True, f"✅ {coin['name']} ({coin['symbol'].upper()}) به واچ‌لیست اضافه شد."
    finally:
        conn.close()


def remove_watch(user_id: int, coin_id: str) -> None:
    conn = db_connect()
    try:
        conn.execute(
            "DELETE FROM watchlist WHERE user_id=? AND coin_id=?",
            (user_id, coin_id),
        )
        conn.execute(
            "DELETE FROM alert_state WHERE user_id=? AND coin_id=?",
            (user_id, coin_id),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# HANDLERS
# ---------------------------------------------------------------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await upsert_user(update)
    if is_blocked(update.effective_user.id):
        return

    context.user_data.clear()
    await send_home(update)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await upsert_user(update)
    text = (
        "ℹ️ <b>راهنما</b>\n\n"
        "• «➕ افزودن ارز» برای جستجو و افزودن هر ارز\n"
        "• «📊 تحلیل» برای تحلیل تکنیکال\n"
        "• «🚨 سیگنال‌ها» برای بررسی واچ‌لیست\n"
        "• قیمت فعلی رایگان است؛ تحلیل و سیگنال نیاز به اشتراک دارد.\n"
        "• ربات معامله خودکار انجام نمی‌دهد.\n\n"
        "برای جستجو می‌توانید BTC، ZEC، SOL یا نام ارز را ارسال کنید."
    )
    await update.effective_message.reply_text(
        text, parse_mode=ParseMode.HTML, reply_markup=main_keyboard()
    )


async def watchlist_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await upsert_user(update)
    rows = get_watchlist(update.effective_user.id)
    if not rows:
        await update.effective_message.reply_text(
            "📋 واچ‌لیست شما خالی است.\n\n➕ افزودن ارز را بزنید.",
            reply_markup=main_keyboard(),
        )
        return

    buttons = []
    for r in rows:
        buttons.append(
            [
                InlineKeyboardButton(
                    f"📈 {r['symbol']} — {r['name'][:20]}",
                    callback_data=f"analyze:{r['coin_id']}",
                ),
                InlineKeyboardButton(
                    "❌",
                    callback_data=f"remove:{r['coin_id']}",
                ),
            ]
        )

    await update.effective_message.reply_text(
        "📋 <b>واچ‌لیست شما</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def add_coin_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await upsert_user(update)
    context.user_data["state"] = "search"
    await update.effective_message.reply_text(
        "➕ نام یا نماد ارز را بفرستید.\n\nمثال:\nBTC\nZEC\nSOL\nEthereum",
        reply_markup=main_keyboard(),
    )


async def subscription_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await upsert_user(update)
    row = get_subscription(update.effective_user.id)
    if not row:
        text = "👤 <b>وضعیت اشتراک</b>\n\n❌ اشتراک فعال ندارید."
    else:
        end = parse_iso(row["end_at"])
        end_txt = end.astimezone().strftime("%Y-%m-%d %H:%M") if end else "-"
        text = (
            "👤 <b>وضعیت اشتراک</b>\n\n"
            f"✅ فعال\n"
            f"📅 پایان: <code>{end_txt}</code>\n"
            f"📦 پلن: {PLANS.get(row['plan'], {}).get('title', row['plan'])}"
        )

    await update.effective_message.reply_text(
        text, parse_mode=ParseMode.HTML, reply_markup=main_keyboard()
    )


async def buy_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await upsert_user(update)
    buttons = [
        [
            InlineKeyboardButton(
                f"۳۰ روزه — {money(PLANS['30']['price'])} تومان",
                callback_data="plan:30",
            )
        ],
        [
            InlineKeyboardButton(
                f"۹۰ روزه — {money(PLANS['90']['price'])} تومان",
                callback_data="plan:90",
            )
        ],
        [
            InlineKeyboardButton(
                f"۱۸۰ روزه — {money(PLANS['180']['price'])} تومان",
                callback_data="plan:180",
            )
        ],
    ]
    await update.effective_message.reply_text(
        "💳 <b>خرید اشتراک</b>\n\n"
        "پس از انتخاب پلن، شماره کارت و مراحل ارسال رسید نمایش داده می‌شود.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def alerts_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await upsert_user(update)
    conn = db_connect()
    try:
        row = conn.execute(
            "SELECT alerts_enabled FROM settings WHERE user_id=?",
            (update.effective_user.id,),
        ).fetchone()
    finally:
        conn.close()
    enabled = bool(row and row["alerts_enabled"])
    status = "فعال ✅" if enabled else "خاموش ❌"
    kb = InlineKeyboardMarkup(
        [[
            InlineKeyboardButton(
                "🔕 خاموش" if enabled else "🔔 فعال‌سازی",
                callback_data="alerts:toggle",
            )
        ]]
    )
    await update.effective_message.reply_text(
        f"🔔 هشدارهای سیگنال: <b>{status}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )


async def price_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE, coin_id: str) -> None:
    if not subscription_active(update.effective_user.id):
        await update.effective_message.reply_text(
            "🔒 تحلیل و سیگنال فقط برای کاربران دارای اشتراک فعال است.\n\n"
            "💳 خرید اشتراک را انتخاب کنید."
        )
        return

    try:
        market = await get_market(coin_id)
        prices = await get_chart(coin_id, 30)
        a = analyze_prices(prices)

        name = market.get("name", coin_id)
        symbol = str(market.get("symbol", "")).upper()
        price = safe_float(market.get("current_price"))
        change24 = safe_float(market.get("price_change_percentage_24h"))
        change7 = safe_float(market.get("price_change_percentage_7d_in_currency"))
        change30 = safe_float(market.get("price_change_percentage_30d_in_currency"))

        text = (
            f"📊 <b>تحلیل {name} ({symbol})</b>\n\n"
            f"💰 قیمت: <code>${price:,.8f}</code>\n"
            f"📈 ۲۴ساعت: {change24:+.2f}%\n"
            f"📈 ۷روز: {change7:+.2f}%\n"
            f"📈 ۳۰روز: {change30:+.2f}%\n\n"
            f"🎯 <b>سیگنال:</b> {signal_fa(a['signal'])}\n"
            f"💪 <b>درصد قدرت:</b> {pct(a['strength'])}\n"
            f"💰 <b>احتمال سود:</b> {pct(a['profit_probability'])}\n\n"
            f"RSI: {a['rsi']:.1f}\n"
            f"EMA20: {a['ema20']:.6g}\n"
            f"EMA50: {a['ema50']:.6g}\n"
            f"MACD: {a['macd']:.6g}\n\n"
            f"🧠 {a['reason']}\n\n"
            "⚠️ این تحلیل تخمینی است و تضمین سود نیست."
        )
        await update.effective_message.reply_text(
            text, parse_mode=ParseMode.HTML, reply_markup=main_keyboard()
        )
    except Exception as e:
        logger.exception("analysis error")
        await update.effective_message.reply_text(
            "❌ دریافت داده یا تحلیل انجام نشد. چند لحظه بعد دوباره امتحان کنید."
        )


async def analyze_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await upsert_user(update)
    rows = get_watchlist(update.effective_user.id)
    if not rows:
        await update.effective_message.reply_text(
            "📊 ابتدا حداقل یک ارز به واچ‌لیست اضافه کنید."
        )
        return
    buttons = [
        [InlineKeyboardButton(
            f"📊 {r['symbol']} — {r['name'][:18]}",
            callback_data=f"analyze:{r['coin_id']}"
        )]
        for r in rows
    ]
    await update.effective_message.reply_text(
        "📊 ارز موردنظر را انتخاب کنید:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def signals_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await upsert_user(update)
    if not subscription_active(update.effective_user.id):
        await update.effective_message.reply_text(
            "🔒 سیگنال‌ها نیاز به اشتراک فعال دارند."
        )
        return

    rows = get_watchlist(update.effective_user.id)
    if not rows:
        await update.effective_message.reply_text("🚨 واچ‌لیست شما خالی است.")
        return

    await update.effective_message.reply_text("⏳ در حال بررسی سیگنال‌ها...")
    out = ["🚨 <b>سیگنال‌های واچ‌لیست</b>\n"]
    for r in rows[:20]:
        try:
            prices = await get_chart(r["coin_id"], 30)
            a = analyze_prices(prices)
            out.append(
                f"• <b>{r['symbol']}</b> → {signal_fa(a['signal'])} | "
                f"قدرت {pct(a['strength'])} | احتمال سود {pct(a['profit_probability'])}"
            )
        except Exception:
            out.append(f"• <b>{r['symbol']}</b> → ❌ خطا در دریافت داده")

    await update.effective_message.reply_text(
        "\n".join(out), parse_mode=ParseMode.HTML, reply_markup=main_keyboard()
    )


# ---------------------------------------------------------------------------
# PAYMENT FLOW
# ---------------------------------------------------------------------------

async def handle_plan_callback(query, context: ContextTypes.DEFAULT_TYPE, plan: str) -> None:
    if plan not in PLANS:
        await query.answer("پلن نامعتبر است.", show_alert=True)
        return

    context.user_data["payment_plan"] = plan
    p = PLANS[plan]
    card = PAYMENT_CARD or "شماره کارت در Railway تنظیم نشده است."
    text = (
        f"💳 <b>پلن {p['title']}</b>\n\n"
        f"مبلغ: <b>{money(p['price'])} تومان</b>\n"
        f"شماره کارت:\n<code>{card}</code>\n\n"
        "پس از واریز، تصویر رسید را همین‌جا ارسال کنید.\n"
        "رسید برای مدیر ارسال و بعد از تأیید، اشتراک فعال می‌شود."
    )
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ لغو", callback_data="payment:cancel")]]
        ),
    )
    await query.answer()


async def receipt_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await upsert_user(update)
    plan = context.user_data.get("payment_plan")
    if not plan or plan not in PLANS:
        await update.effective_message.reply_text(
            "ابتدا از بخش «💳 خرید اشتراک» یک پلن انتخاب کنید."
        )
        return

    photo = update.effective_message.photo[-1]
    p = PLANS[plan]
    conn = db_connect()
    try:
        cur = conn.execute(
            """
            INSERT INTO payment_requests
            (user_id, plan, amount, receipt_file_id, status, created_at)
            VALUES(?,?,?,?,?,?)
            """,
            (
                update.effective_user.id,
                plan,
                p["price"],
                photo.file_id,
                "pending",
                now_iso(),
            ),
        )
        request_id = cur.lastrowid
        conn.commit()
    finally:
        conn.close()

    await update.effective_message.reply_text(
        f"✅ رسید ثبت شد.\nکد درخواست: <code>#{request_id}</code>\n"
        "پس از بررسی مدیر، نتیجه اعلام می‌شود.",
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard(),
    )

    caption = (
        f"💳 <b>رسید پرداخت جدید #{request_id}</b>\n"
        f"👤 User ID: <code>{update.effective_user.id}</code>\n"
        f"📦 پلن: {p['title']}\n"
        f"💰 مبلغ: {money(p['price'])} تومان"
    )

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_photo(
                chat_id=admin_id,
                photo=photo.file_id,
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "✅ تأیید",
                                callback_data=f"pay:approve:{request_id}",
                            ),
                            InlineKeyboardButton(
                                "❌ رد",
                                callback_data=f"pay:reject:{request_id}",
                            ),
                        ]
                    ]
                ),
            )
        except Exception:
            logger.exception("cannot notify admin %s", admin_id)


# ---------------------------------------------------------------------------
# ADMIN
# ---------------------------------------------------------------------------

def admin_only(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await upsert_user(update)
    if not admin_only(update.effective_user.id):
        await update.effective_message.reply_text("⛔ دسترسی ندارید.")
        return
    await update.effective_message.reply_text(
        "🛠 <b>پنل مدیریت</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_keyboard(),
    )


async def admin_stats(query) -> None:
    conn = db_connect()
    try:
        users = conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        active = conn.execute(
            """
            SELECT COUNT(DISTINCT user_id) c FROM subscriptions
            WHERE datetime(end_at) > datetime('now')
            """
        ).fetchone()["c"]
        pending = conn.execute(
            "SELECT COUNT(*) c FROM payment_requests WHERE status='pending'"
        ).fetchone()["c"]
    finally:
        conn.close()

    await query.edit_message_text(
        f"📊 <b>آمار</b>\n\n"
        f"👥 کاربران: {users}\n"
        f"🟢 اشتراک فعال: {active}\n"
        f"💳 پرداخت در انتظار: {pending}",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_keyboard(),
    )


async def admin_payments(query) -> None:
    conn = db_connect()
    try:
        rows = conn.execute(
            """
            SELECT * FROM payment_requests
            WHERE status='pending'
            ORDER BY id DESC LIMIT 20
            """
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        text = "💳 پرداخت در انتظار وجود ندارد."
        kb = admin_keyboard()
    else:
        lines = ["💳 <b>پرداخت‌های در انتظار</b>\n"]
        buttons = []
        for r in rows:
            lines.append(
                f"#{r['id']} | user {r['user_id']} | "
                f"{PLANS.get(r['plan'], {}).get('title', r['plan'])} | "
                f"{money(r['amount'])}"
            )
            buttons.append([
                InlineKeyboardButton(
                    f"#{r['id']} بررسی",
                    callback_data=f"pay:view:{r['id']}",
                )
            ])
        kb = InlineKeyboardMarkup(buttons + [
            [InlineKeyboardButton("⬅️ برگشت", callback_data="admin:home")]
        ])
        text = "\n".join(lines)

    await query.edit_message_text(
        text, parse_mode=ParseMode.HTML, reply_markup=kb
    )


async def approve_payment(query, context: ContextTypes.DEFAULT_TYPE, request_id: int) -> None:
    conn = db_connect()
    try:
        row = conn.execute(
            "SELECT * FROM payment_requests WHERE id=?", (request_id,)
        ).fetchone()
        if not row or row["status"] != "pending":
            await query.answer("این درخواست قبلاً بررسی شده است.", show_alert=True)
            return

        conn.execute(
            """
            UPDATE payment_requests
            SET status='approved', reviewed_at=?, reviewed_by=?
            WHERE id=?
            """,
            (now_iso(), query.from_user.id, request_id),
        )
        conn.commit()
    finally:
        conn.close()

    p = PLANS.get(row["plan"])
    add_subscription(
        row["user_id"], row["plan"], p["days"], query.from_user.id
    )

    try:
        await context.bot.send_message(
            row["user_id"],
            f"✅ پرداخت شما تأیید شد.\n"
            f"اشتراک {p['title']} فعال شد.",
        )
    except Exception:
        logger.exception("cannot notify approved user")

    await query.edit_message_reply_markup(reply_markup=None)
    await query.answer("پرداخت تأیید و اشتراک فعال شد.", show_alert=True)


async def reject_payment(query, context: ContextTypes.DEFAULT_TYPE, request_id: int) -> None:
    conn = db_connect()
    try:
        row = conn.execute(
            "SELECT * FROM payment_requests WHERE id=?", (request_id,)
        ).fetchone()
        if not row or row["status"] != "pending":
            await query.answer("این درخواست قبلاً بررسی شده است.", show_alert=True)
            return
        conn.execute(
            """
            UPDATE payment_requests
            SET status='rejected', reviewed_at=?, reviewed_by=?
            WHERE id=?
            """,
            (now_iso(), query.from_user.id, request_id),
        )
        conn.commit()
    finally:
        conn.close()

    try:
        await context.bot.send_message(
            row["user_id"],
            "❌ رسید پرداخت شما رد شد. در صورت نیاز با پشتیبانی تماس بگیرید.",
        )
    except Exception:
        logger.exception("cannot notify rejected user")

    await query.edit_message_reply_markup(reply_markup=None)
    await query.answer("رسید رد شد.", show_alert=True)


# ---------------------------------------------------------------------------
# CALLBACKS
# ---------------------------------------------------------------------------

async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data or ""

    if data.startswith("plan:"):
        await handle_plan_callback(query, context, data.split(":", 1)[1])
        return

    if data == "payment:cancel":
        context.user_data.pop("payment_plan", None)
        await query.edit_message_text("❌ خرید اشتراک لغو شد.")
        return

    if data.startswith("remove:"):
        coin_id = data.split(":", 1)[1]
        remove_watch(query.from_user.id, coin_id)
        await query.edit_message_text("✅ ارز از واچ‌لیست حذف شد.")
        return

    if data.startswith("analyze:"):
        coin_id = data.split(":", 1)[1]
        # Edit first so the user gets immediate feedback.
        await query.edit_message_text("⏳ در حال دریافت داده و تحلیل...")
        fake_update = update
        try:
            if not subscription_active(query.from_user.id):
                await query.edit_message_text(
                    "🔒 تحلیل و سیگنال فقط برای کاربران دارای اشتراک فعال است."
                )
                return

            market = await get_market(coin_id)
            prices = await get_chart(coin_id, 30)
            a = analyze_prices(prices)
            price = safe_float(market.get("current_price"))
            text = (
                f"📊 <b>{market.get('name')} ({str(market.get('symbol','')).upper()})</b>\n\n"
                f"💰 قیمت: <code>${price:,.8f}</code>\n"
                f"🎯 سیگنال: {signal_fa(a['signal'])}\n"
                f"💪 درصد قدرت: {pct(a['strength'])}\n"
                f"💰 احتمال سود: {pct(a['profit_probability'])}\n\n"
                f"RSI: {a['rsi']:.1f}\n"
                f"EMA20: {a['ema20']:.6g}\n"
                f"EMA50: {a['ema50']:.6g}\n"
                f"MACD: {a['macd']:.6g}\n\n"
                f"🧠 {a['reason']}\n\n"
                "⚠️ تضمین سود وجود ندارد."
            )
            await query.edit_message_text(
                text, parse_mode=ParseMode.HTML
            )
        except Exception:
            logger.exception("callback analysis error")
            await query.edit_message_text(
                "❌ تحلیل انجام نشد. دوباره تلاش کنید."
            )
        return

    if data == "alerts:toggle":
        conn = db_connect()
        try:
            row = conn.execute(
                "SELECT alerts_enabled FROM settings WHERE user_id=?",
                (query.from_user.id,),
            ).fetchone()
            current = bool(row and row["alerts_enabled"])
            new_value = 0 if current else 1
            conn.execute(
                """
                INSERT INTO settings(user_id, alerts_enabled)
                VALUES(?,?)
                ON CONFLICT(user_id) DO UPDATE SET alerts_enabled=excluded.alerts_enabled
                """,
                (query.from_user.id, new_value),
            )
            conn.commit()
        finally:
            conn.close()
        await query.edit_message_text(
            "🔔 هشدارها فعال شد." if new_value else "🔕 هشدارها خاموش شد."
        )
        return

    if data.startswith("admin:"):
        if not admin_only(query.from_user.id):
            await query.answer("⛔ دسترسی ندارید.", show_alert=True)
            return
        action = data.split(":", 1)[1]
        if action == "home":
            await query.edit_message_text(
                "🛠 <b>پنل مدیریت</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=admin_keyboard(),
            )
        elif action == "stats":
            await admin_stats(query)
        elif action == "payments":
            await admin_payments(query)
        elif action == "users":
            conn = db_connect()
            try:
                rows = conn.execute(
                    "SELECT user_id, username, first_name FROM users ORDER BY user_id DESC LIMIT 20"
                ).fetchall()
            finally:
                conn.close()
            text = "👥 <b>آخرین کاربران</b>\n\n"
            for r in rows:
                text += f"• {r['user_id']} | @{r['username'] or '-'} | {r['first_name'] or '-'}\n"
            await query.edit_message_text(
                text, parse_mode=ParseMode.HTML, reply_markup=admin_keyboard()
            )
        return

    if data.startswith("pay:"):
        if not admin_only(query.from_user.id):
            await query.answer("⛔ دسترسی ندارید.", show_alert=True)
            return
        _, action, rid = data.split(":")
        rid = int(rid)
        if action == "approve":
            await approve_payment(query, context, rid)
        elif action == "reject":
            await reject_payment(query, context, rid)
        elif action == "view":
            conn = db_connect()
            try:
                row = conn.execute(
                    "SELECT * FROM payment_requests WHERE id=?", (rid,)
                ).fetchone()
            finally:
                conn.close()
            if not row:
                await query.edit_message_text("درخواست پیدا نشد.")
                return
            await query.edit_message_text(
                f"💳 درخواست #{rid}\n"
                f"user: <code>{row['user_id']}</code>\n"
                f"plan: {PLANS.get(row['plan'], {}).get('title', row['plan'])}\n"
                f"amount: {money(row['amount'])}\n\n"
                "برای تأیید/رد، پیام رسیدی که برای مدیر ارسال شده را باز کنید.",
                parse_mode=ParseMode.HTML,
            )
        return


# ---------------------------------------------------------------------------
# TEXT ROUTER
# ---------------------------------------------------------------------------

async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await upsert_user(update)
    if is_blocked(update.effective_user.id):
        return

    text = (update.effective_message.text or "").strip()

    if text == "📋 واچ‌لیست":
        await watchlist_cmd(update, context)
        return
    if text == "➕ افزودن ارز":
        await add_coin_start(update, context)
        return
    if text == "📊 تحلیل":
        await analyze_menu(update, context)
        return
    if text == "🚨 سیگنال‌ها":
        await signals_menu(update, context)
        return
    if text == "💳 خرید اشتراک":
        await buy_subscription(update, context)
        return
    if text == "👤 وضعیت اشتراک":
        await subscription_status(update, context)
        return
    if text == "🔔 هشدارها":
        await alerts_menu(update, context)
        return
    if text == "ℹ️ راهنما":
        await help_cmd(update, context)
        return

    # Search state
    if context.user_data.get("state") == "search":
        context.user_data.pop("state", None)
        await update.effective_message.reply_text("⏳ در حال جستجو...")
        try:
            results = await search_coins(text)
            if not results:
                await update.effective_message.reply_text(
                    "❌ ارزی پیدا نشد. نماد دیگری بفرستید."
                )
                return

            buttons = []
            for c in results[:8]:
                title = f"{c['symbol'].upper()} — {c['name']}"
                buttons.append([
                    InlineKeyboardButton(
                        title[:55],
                        callback_data=f"add:{c['id']}",
                    )
                ])

            await update.effective_message.reply_text(
                "🔎 نتیجه جستجو؛ ارز موردنظر را انتخاب کنید:",
                reply_markup=InlineKeyboardMarkup(buttons),
            )
        except Exception:
            logger.exception("search error")
            await update.effective_message.reply_text(
                "❌ خطا در جستجو. چند لحظه بعد دوباره تلاش کنید."
            )
        return

    await update.effective_message.reply_text(
        "از منوی پایین استفاده کنید یا «➕ افزودن ارز» را بزنید.",
        reply_markup=main_keyboard(),
    )


# Add callback for adding coins separately, kept here to avoid a second router.
async def add_coin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    data = query.data or ""
    if not data.startswith("add:"):
        return
    await query.answer()
    coin_id = data.split(":", 1)[1]

    try:
        results = await cg_get("/coins/" + coin_id, {"localization": "false"})
        coin = {
            "id": coin_id,
            "name": results.get("name", coin_id),
            "symbol": results.get("symbol", "").upper(),
        }
        ok, msg = add_watch(query.from_user.id, coin)
        await query.edit_message_text(msg)
    except Exception:
        logger.exception("add coin error")
        await query.edit_message_text(
            "❌ افزودن ارز انجام نشد. دوباره تلاش کنید."
        )


# ---------------------------------------------------------------------------
# ERROR HANDLER
# ---------------------------------------------------------------------------

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    logger.error("Unhandled exception: %r", err, exc_info=err)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def build_application() -> Application:
    if not BOT_TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is missing. Add it in Railway Variables."
        )

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(TELEGRAM_REQUEST_TIMEOUT)
        .read_timeout(TELEGRAM_REQUEST_TIMEOUT)
        .write_timeout(TELEGRAM_REQUEST_TIMEOUT)
        .pool_timeout(TELEGRAM_REQUEST_TIMEOUT)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))

    # More specific callback handler MUST come before generic callback handler.
    app.add_handler(
        CallbackQueryHandler(add_coin_callback, pattern=r"^add:")
    )
    app.add_handler(CallbackQueryHandler(callbacks))

    app.add_handler(
        MessageHandler(filters.PHOTO, receipt_photo)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, text_router)
    )
    app.add_error_handler(error_handler)
    return app


async def run() -> None:
    init_db()

    logger.info("Starting Crypto Analyzer...")
    logger.info("DB_PATH=%s", DB_PATH)
    logger.info("ADMIN_IDS=%s", sorted(ADMIN_IDS))

    # Important for Railway/Telegram polling:
    # remove an old webhook before polling. This does NOT solve two
    # simultaneous polling processes using the same token; there must be one.
    app = build_application()

    await app.initialize()

    try:
        bot_info = await app.bot.get_me()
        logger.info(
            "Telegram bot connected: @%s (id=%s)",
            bot_info.username,
            bot_info.id,
        )

        # Clear webhook and pending updates so an old webhook/polling setup
        # cannot block the new polling process.
        await app.bot.delete_webhook(drop_pending_updates=True)
        logger.info("Webhook deleted; starting polling.")

        await app.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
        )
        await app.start()

        logger.info("Crypto Analyzer started successfully.")

        # Keep process alive.
        stop_event = asyncio.Event()
        await stop_event.wait()

    finally:
        try:
            if app.updater and app.updater.running:
                await app.updater.stop()
        finally:
            if app.running:
                await app.stop()
            await app.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
    except Exception:
        logger.exception("Fatal startup error.")
        raise
