# -*- coding: utf-8 -*-
"""
Crypto Analyzer Telegram Bot - Railway production build
Analysis/signals only. No automatic trading.

Required:
    TELEGRAM_BOT_TOKEN

Recommended Railway variables:
    ADMIN_IDS=123456789
    PAYMENT_CARD=6037...
    SUPPORT_USERNAME=@username

Optional:
    DB_PATH=/data/crypto_bot.db
    HTTP_TIMEOUT=20
    CACHE_SECONDS=30
"""
import os
import re
import sqlite3
import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone

import aiohttp
import numpy as np
import pandas as pd
from telegram import (
    Update, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ContextTypes,
    CallbackQueryHandler, filters
)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
ADMIN_IDS = {
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}
DB_PATH = os.getenv("DB_PATH", "/data/crypto_bot.db").strip()
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
CACHE_SECONDS = int(os.getenv("CACHE_SECONDS", "30"))
PAYMENT_CARD = os.getenv("PAYMENT_CARD", "ثبت نشده").strip()
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")

os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | crypto-analyzer | %(message)s"
)
log = logging.getLogger("crypto-analyzer")

MAIN_MENU = ReplyKeyboardMarkup(
    [
        [KeyboardButton("➕ افزودن ارز"), KeyboardButton("📋 واچ‌لیست")],
        [KeyboardButton("📊 تحلیل"), KeyboardButton("🚨 سیگنال‌ها")],
        [KeyboardButton("💳 خرید اشتراک"), KeyboardButton("👤 وضعیت اشتراک")],
        [KeyboardButton("🔔 هشدارها"), KeyboardButton("ℹ️ راهنما")],
    ],
    resize_keyboard=True,
)

# ---------------- DATABASE ----------------

def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    return c

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def init_db():
    c = db()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        created_at TEXT NOT NULL,
        last_seen TEXT NOT NULL,
        blocked INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS subscriptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        plan TEXT NOT NULL,
        days INTEGER NOT NULL,
        amount INTEGER NOT NULL DEFAULT 0,
        start_at TEXT NOT NULL,
        end_at TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        source TEXT DEFAULT 'manual',
        payment_request_id INTEGER,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS payment_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        plan TEXT NOT NULL,
        days INTEGER NOT NULL,
        amount INTEGER NOT NULL,
        receipt_file_id TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TEXT NOT NULL,
        reviewed_at TEXT,
        reviewed_by INTEGER
    );

    CREATE TABLE IF NOT EXISTS watchlist (
        user_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        asset_type TEXT NOT NULL DEFAULT 'crypto',
        created_at TEXT NOT NULL,
        PRIMARY KEY(user_id, symbol)
    );

    CREATE TABLE IF NOT EXISTS alert_preferences (
        user_id INTEGER PRIMARY KEY,
        enabled INTEGER NOT NULL DEFAULT 0,
        interval_seconds INTEGER NOT NULL DEFAULT 300
    );

    CREATE TABLE IF NOT EXISTS alert_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        message TEXT NOT NULL,
        created_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_sub_user_end
        ON subscriptions(user_id, end_at);

    CREATE INDEX IF NOT EXISTS idx_pay_status
        ON payment_requests(status);
    """)

    # Additive migration: old databases are preserved.
    cols = {r["name"] for r in c.execute("PRAGMA table_info(users)").fetchall()}
    if "blocked" not in cols:
        c.execute(
            "ALTER TABLE users ADD COLUMN blocked INTEGER NOT NULL DEFAULT 0"
        )

    c.commit()
    c.close()

def ensure_user(tg_user):
    c = db()
    n = now_iso()
    c.execute("""
        INSERT INTO users(user_id,username,first_name,created_at,last_seen)
        VALUES(?,?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            last_seen=excluded.last_seen
    """, (
        tg_user.id,
        tg_user.username or "",
        tg_user.first_name or "",
        n,
        n
    ))
    c.commit()
    c.close()

def is_admin(uid):
    return uid in ADMIN_IDS

def has_analysis_access(uid):
    # Admins can test the analyzer without purchasing a subscription.
    return is_admin(uid) or active_subscription(uid)

def active_subscription(uid):
    c = db()
    r = c.execute("""
        SELECT * FROM subscriptions
        WHERE user_id=? AND status='active' AND end_at>?
        ORDER BY datetime(end_at) DESC
        LIMIT 1
    """, (uid, now_iso())).fetchone()
    c.close()
    return r

def add_subscription(
    uid, plan, days, amount,
    source="manual", payment_request_id=None,
    reviewed_by=None
):
    start = datetime.now(timezone.utc)
    old = active_subscription(uid)

    if old:
        try:
            base = datetime.fromisoformat(old["end_at"])
        except Exception:
            base = start
        if base > start:
            start = base

    end = start + timedelta(days=days)

    c = db()
    c.execute(
        "INSERT INTO subscriptions("
        "user_id,plan,days,amount,start_at,end_at,status,source,"
        "payment_request_id,created_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)",
        (
            uid, plan, days, amount,
            start.isoformat(), end.isoformat(),
            "active", source, payment_request_id, now_iso()
        )
    )

    if payment_request_id:
        c.execute(
            "UPDATE payment_requests "
            "SET status='approved',reviewed_at=?,reviewed_by=? "
            "WHERE id=?",
            (now_iso(), reviewed_by or 0, payment_request_id)
        )

    c.commit()
    c.close()
    return end

# ---------------- MARKET DATA ----------------

_http_session = None
_cache = {}
_cache_lock = asyncio.Lock()

async def http_json(url, params=None):
    global _http_session

    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
            headers={"User-Agent": "CryptoAnalyzer/1.0"}
        )

    async with _http_session.get(url, params=params) as response:
        text = await response.text()

        if response.status != 200:
            raise RuntimeError(
                f"HTTP {response.status}: {text[:200]}"
            )

        try:
            return await response.json()
        except Exception:
            raise RuntimeError("پاسخ سرویس داده قابل خواندن نیست.")

def normalize_symbol(raw):
    s = re.sub(r"[^A-Za-z0-9]", "", raw or "").upper()

    if not s:
        return ""

    if s.endswith("USDT"):
        return s

    return s + "USDT"

async def binance_klines(symbol, interval="1h", limit=200):
    data = await http_json(
        "https://api.binance.com/api/v3/klines",
        {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
    )

    if not isinstance(data, list) or len(data) < 30:
        raise RuntimeError("داده کافی از Binance دریافت نشد.")

    rows = []

    for x in data:
        rows.append({
            "time": pd.to_datetime(int(x[0]), unit="ms", utc=True),
            "open": float(x[1]),
            "high": float(x[2]),
            "low": float(x[3]),
            "close": float(x[4]),
            "volume": float(x[5])
        })

    return pd.DataFrame(rows)

async def coingecko_chart(coin_id, days=30):
    data = await http_json(
        f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart",
        {
            "vs_currency": "usd",
            "days": days,
            "interval": "hourly"
        }
    )

    prices = data.get("prices") or []

    if len(prices) < 30:
        raise RuntimeError("داده کافی از CoinGecko دریافت نشد.")

    closes = [float(x[1]) for x in prices]
    times = [
        pd.to_datetime(int(x[0]), unit="ms", utc=True)
        for x in prices
    ]

    return pd.DataFrame({
        "time": times,
        "open": closes,
        "high": closes,
        "low": closes,
        "close": closes,
        "volume": np.nan
    })

async def get_data(symbol):
    symbol = normalize_symbol(symbol)
    key = ("data", symbol)

    async with _cache_lock:
        item = _cache.get(key)
        if item and time.time() - item[0] < CACHE_SECONDS:
            return item[1].copy()

    try:
        df = await binance_klines(symbol)

    except Exception as binance_error:
        base = symbol[:-4] if symbol.endswith("USDT") else symbol

        ids = {
            "BTC": "bitcoin",
            "ETH": "ethereum",
            "SOL": "solana",
            "ZEC": "zcash",
            "XRP": "ripple",
            "DOGE": "dogecoin",
            "ADA": "cardano",
            "BNB": "binancecoin",
            "TRX": "tron",
            "TON": "the-open-network",
            "DOT": "polkadot"
        }

        if base not in ids:
            raise RuntimeError(
                f"{symbol} در Binance Spot/USDT یا منابع پشتیبان پیدا نشد."
            )

        df = await coingecko_chart(ids[base])

        log.warning(
            "Binance failed for %s; CoinGecko fallback used: %s",
            symbol,
            binance_error
        )

    async with _cache_lock:
        _cache[key] = (time.time(), df.copy())

    return df

# ---------------- ANALYSIS ----------------

def rsi(series, period=14):
    delta = series.diff()

    gain = delta.clip(lower=0).ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    loss = (-delta.clip(upper=0)).ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    rs = gain / loss.replace(0, np.nan)
    result = 100 - (100 / (1 + rs))

    return result.fillna(50)

def calculate(df):
    d = df.copy()
    close = d["close"]

    d["ema9"] = close.ewm(
        span=9,
        adjust=False
    ).mean()

    d["ema21"] = close.ewm(
        span=21,
        adjust=False
    ).mean()

    d["rsi"] = rsi(close)
    d["ret6"] = close.pct_change(6) * 100
    d["ret24"] = close.pct_change(24) * 100
    d["vol_ma"] = d["volume"].rolling(20).mean()

    last = d.iloc[-1]

    score = 0.0
    reasons = []

    if last["ema9"] > last["ema21"]:
        score += 25
        reasons.append("EMA9 بالای EMA21")
    else:
        score -= 25
        reasons.append("EMA9 زیر EMA21")

    if last["rsi"] >= 55:
        score += 20
        reasons.append("RSI مثبت")
    elif last["rsi"] <= 45:
        score -= 20
        reasons.append("RSI منفی")
    else:
        reasons.append("RSI خنثی")

    if last["ret6"] > 0:
        score += 10
    elif last["ret6"] < 0:
        score -= 10

    if last["ret24"] > 0:
        score += 10
    elif last["ret24"] < 0:
        score -= 10

    if (
        pd.notna(last["volume"])
        and pd.notna(last["vol_ma"])
        and last["volume"] > last["vol_ma"]
    ):
        score += 5 if score >= 0 else -5
        reasons.append("حجم بالاتر از میانگین")

    # Separate metrics:
    # strength = strength of the technical setup
    # probability = model-style directional estimate
    strength = min(100, max(0, 50 + abs(score) * 2))
    probability = min(95, max(5, 50 + score * 0.8))

    if score >= 25:
        signal = "🟢 خرید / صعودی"
    elif score <= -25:
        signal = "🔴 فروش / نزولی"
    else:
        signal = "🟡 انتظار / خنثی"

    look = d.tail(48)

    support = float(look["low"].min())
    resistance = float(look["high"].max())

    return {
        "price": float(last["close"]),
        "rsi": float(last["rsi"]),
        "ema9": float(last["ema9"]),
        "ema21": float(last["ema21"]),
        "change6": (
            float(last["ret6"])
            if pd.notna(last["ret6"]) else 0.0
        ),
        "change24": (
            float(last["ret24"])
            if pd.notna(last["ret24"]) else 0.0
        ),
        "score": score,
        "strength": strength,
        "probability": probability,
        "signal": signal,
        "support": support,
        "resistance": resistance,
        "reasons": reasons
    }

async def analyze(symbol):
    df = await get_data(symbol)
    return calculate(df)

def fmt_num(x):
    if x >= 1000:
        return f"{x:,.2f}"
    if x >= 1:
        return f"{x:,.4f}"
    return f"{x:,.8f}"

async def analysis_text(symbol):
    a = await analyze(symbol)

    reasons = "\n".join(
        f"• {x}" for x in a["reasons"]
    )

    return (
        f"📊 تحلیل هوشمند {symbol}\n\n"
        f"💰 قیمت: {fmt_num(a['price'])}\n"
        f"📌 سیگنال: {a['signal']}\n\n"
        f"💪 درصد قدرت: {a['strength']:.0f}%\n"
        f"🎯 احتمال سود: {a['probability']:.0f}%\n\n"
        f"RSI(14): {a['rsi']:.1f}\n"
        f"EMA9: {fmt_num(a['ema9'])}\n"
        f"EMA21: {fmt_num(a['ema21'])}\n"
        f"تغییر 6 کندل: {a['change6']:+.2f}%\n"
        f"تغییر 24 کندل: {a['change24']:+.2f}%\n\n"
        f"🟢 حمایت: {fmt_num(a['support'])}\n"
        f"🔴 مقاومت: {fmt_num(a['resistance'])}\n\n"
        f"🔎 عوامل:\n{reasons}\n\n"
        "⚠️ این تحلیل آموزشی است و تضمین سود نیست."
    )

# ---------------- TELEGRAM HANDLERS ----------------

async def start(update, context):
    ensure_user(update.effective_user)

    await update.message.reply_text(
        "🤖 Crypto Analyzer\n\n"
        "تحلیل و سیگنال بازار ارزهای دیجیتال، "
        "بدون اجرای خودکار معامله.\n\n"
        "از منوی پایین یک گزینه را انتخاب کنید.",
        reply_markup=MAIN_MENU
    )

async def myid(update, context):
    await update.message.reply_text(
        f"🆔 شناسه عددی شما:\n`{update.effective_user.id}`",
        parse_mode="Markdown"
    )

async def subscription_status(update, context):
    ensure_user(update.effective_user)

    s = active_subscription(update.effective_user.id)

    if not s:
        await update.message.reply_text(
            "👤 وضعیت اشتراک\n\n"
            "❌ اشتراک فعال ندارید.\n"
            "برای استفاده از تحلیل و سیگنال، اشتراک تهیه کنید."
        )
        return

    await update.message.reply_text(
        "👤 وضعیت اشتراک\n\n"
        "✅ فعال\n"
        f"📦 طرح: {s['plan']}\n"
        f"📅 پایان: {s['end_at'][:19].replace('T', ' ')} UTC"
    )

async def buy_menu(update, context):
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "۳۰ روز — ۲۰۰,۰۰۰ تومان",
                callback_data="plan:30"
            )
        ],
        [
            InlineKeyboardButton(
                "۹۰ روز — ۳۵۰,۰۰۰ تومان",
                callback_data="plan:90"
            )
        ],
        [
            InlineKeyboardButton(
                "۱۸۰ روز — ۵۰۰,۰۰۰ تومان",
                callback_data="plan:180"
            )
        ]
    ])

    await update.message.reply_text(
        "💳 انتخاب اشتراک:",
        reply_markup=keyboard
    )

async def plan_callback(update, context):
    query = update.callback_query
    await query.answer()

    days = int(query.data.split(":")[1])

    amounts = {
        30: 200000,
        90: 350000,
        180: 500000
    }

    amount = amounts[days]

    context.user_data["pending_plan"] = (days, amount)

    await query.message.reply_text(
        f"💳 اشتراک {days} روزه\n"
        f"مبلغ: {amount:,} تومان\n\n"
        f"شماره کارت:\n{PAYMENT_CARD}\n\n"
        "پس از پرداخت، تصویر رسید را همین‌جا ارسال کنید."
    )

async def receipt(update, context):
    plan = context.user_data.get("pending_plan")

    if not plan:
        await update.message.reply_text(
            "ابتدا از «💳 خرید اشتراک» یک طرح را انتخاب کنید."
        )
        return

    days, amount = plan
    file_id = update.message.photo[-1].file_id

    c = db()

    cur = c.execute(
        "INSERT INTO payment_requests("
        "user_id,plan,days,amount,receipt_file_id,status,created_at"
        ") VALUES(?,?,?,?,?,?,?)",
        (
            update.effective_user.id,
            f"{days} روزه",
            days,
            amount,
            file_id,
            "pending",
            now_iso()
        )
    )

    req_id = cur.lastrowid

    c.commit()
    c.close()

    context.user_data.pop("pending_plan", None)

    await update.message.reply_text(
        "✅ رسید دریافت شد و برای بررسی مدیر ارسال شد."
    )

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_photo(
                chat_id=admin_id,
                photo=file_id,
                caption=(
                    f"💳 درخواست پرداخت #{req_id}\n"
                    f"کاربر: {update.effective_user.id}\n"
                    f"طرح: {days} روزه\n"
                    f"مبلغ: {amount:,} تومان"
                ),
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(
                            "✅ تأیید",
                            callback_data=f"payok:{req_id}"
                        ),
                        InlineKeyboardButton(
                            "❌ رد",
                            callback_data=f"payno:{req_id}"
                        )
                    ]
                ])
            )
        except Exception:
            log.exception(
                "Cannot notify admin %s",
                admin_id
            )

async def payment_callback(update, context):
    query = update.callback_query
    await query.answer()

    if not is_admin(query.from_user.id):
        await query.message.reply_text("⛔ دسترسی ندارید.")
        return

    action, value = query.data.split(":")
    request_id = int(value)

    c = db()

    request = c.execute(
        "SELECT * FROM payment_requests WHERE id=?",
        (request_id,)
    ).fetchone()

    if not request or request["status"] != "pending":
        c.close()
        await query.message.reply_text(
            "این درخواست قبلاً بررسی شده یا وجود ندارد."
        )
        return

    if action == "payno":
        c.execute(
            "UPDATE payment_requests "
            "SET status='rejected',reviewed_at=?,reviewed_by=? "
            "WHERE id=?",
            (
                now_iso(),
                query.from_user.id,
                request_id
            )
        )

        c.commit()
        c.close()

        await query.message.reply_text(
            "❌ پرداخت رد شد."
        )

        try:
            await context.bot.send_message(
                request["user_id"],
                "❌ رسید پرداخت شما رد شد."
            )
        except Exception:
            pass

        return

    c.close()

    end = add_subscription(
        request["user_id"],
        request["plan"],
        request["days"],
        request["amount"],
        source="manual",
        payment_request_id=request_id,
        reviewed_by=query.from_user.id
    )

    await query.message.reply_text(
        "✅ اشتراک فعال شد.\n"
        f"پایان: {end.isoformat()[:19]} UTC"
    )

    try:
        await context.bot.send_message(
            request["user_id"],
            "✅ پرداخت تأیید شد و اشتراک شما فعال شد."
        )
    except Exception:
        pass

async def add_coin_prompt(update, context):
    context.user_data["awaiting_symbol"] = "add"

    await update.message.reply_text(
        "➕ نماد ارز را بفرستید.\n"
        "مثال: BTC یا ZEC یا SOLUSDT"
    )

async def analysis_prompt(update, context):
    if not has_analysis_access(update.effective_user.id):
        await update.message.reply_text(
            "🔒 برای تحلیل، ابتدا اشتراک فعال تهیه کنید."
        )
        return

    context.user_data["awaiting_symbol"] = "analysis"

    await update.message.reply_text(
        "📊 نماد ارز را بفرستید.\n"
        "مثال: BTC یا ZEC"
    )

async def signals(update, context):
    if not has_analysis_access(update.effective_user.id):
        await update.message.reply_text(
            "🔒 برای سیگنال، ابتدا اشتراک فعال تهیه کنید."
        )
        return

    c = db()

    rows = c.execute(
        "SELECT symbol FROM watchlist "
        "WHERE user_id=? ORDER BY symbol",
        (update.effective_user.id,)
    ).fetchall()

    c.close()

    if not rows:
        await update.message.reply_text(
            "📋 واچ‌لیست خالی است. ابتدا ارز اضافه کنید."
        )
        return

    await update.message.reply_text(
        "⏳ در حال تحلیل واچ‌لیست..."
    )

    for row in rows:
        try:
            await update.message.reply_text(
                await analysis_text(row["symbol"])
            )
        except Exception as exc:
            log.exception(
                "watchlist analysis failed for %s",
                row["symbol"]
            )

            await update.message.reply_text(
                f"❌ {row['symbol']}: {exc}"
            )

async def watchlist(update, context):
    c = db()

    rows = c.execute(
        "SELECT symbol FROM watchlist "
        "WHERE user_id=? ORDER BY symbol",
        (update.effective_user.id,)
    ).fetchall()

    c.close()

    if not rows:
        await update.message.reply_text(
            "📋 واچ‌لیست شما خالی است."
        )
        return

    await update.message.reply_text(
        "📋 واچ‌لیست:\n\n" +
        "\n".join(
            f"• {row['symbol']}"
            for row in rows
        )
    )

async def help_cmd(update, context):
    await update.message.reply_text(
        "ℹ️ راهنما\n\n"
        "➕ افزودن ارز: اضافه کردن نماد به واچ‌لیست\n"
        "📊 تحلیل: تحلیل تکنیکال نماد\n"
        "🚨 سیگنال‌ها: تحلیل ارزهای واچ‌لیست\n"
        "👤 وضعیت اشتراک: بررسی اشتراک\n"
        "💳 خرید اشتراک: پرداخت دستی با ارسال رسید\n\n"
        "این ربات معامله خودکار انجام نمی‌دهد."
    )

async def alerts(update, context):
    await update.message.reply_text(
        "🔔 هشدارها\n\n"
        "هشدارهای واچ‌لیست در ساختار دیتابیس فعال است. "
        "برای جلوگیری از ارسال سیگنال‌های تکراری، "
        "فعال‌سازی آن باید از تنظیمات همین ربات انجام شود."
    )

async def admin(update, context):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی ندارید.")
        return

    c = db()

    users = c.execute(
        "SELECT COUNT(*) n FROM users"
    ).fetchone()["n"]

    subs = c.execute(
        "SELECT COUNT(*) n FROM subscriptions "
        "WHERE status='active' AND end_at>?",
        (now_iso(),)
    ).fetchone()["n"]

    pending = c.execute(
        "SELECT COUNT(*) n FROM payment_requests "
        "WHERE status='pending'"
    ).fetchone()["n"]

    c.close()

    await update.message.reply_text(
        "📊 آمار\n\n"
        f"👥 کاربران: {users}\n"
        f"🟢 اشتراک فعال: {subs}\n"
        f"💳 پرداخت در انتظار: {pending}",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "💳 پرداخت‌ها",
                    callback_data="admin:payments"
                )
            ]
        ])
    )

async def admin_callback(update, context):
    query = update.callback_query
    await query.answer()

    if not is_admin(query.from_user.id):
        await query.message.reply_text("⛔ دسترسی ندارید.")
        return

    if query.data == "admin:payments":
        c = db()

        rows = c.execute(
            "SELECT id,user_id,days,amount,created_at "
            "FROM payment_requests "
            "WHERE status='pending' "
            "ORDER BY id DESC LIMIT 20"
        ).fetchall()

        c.close()

        if not rows:
            await query.message.reply_text(
                "پرداخت در انتظار وجود ندارد."
            )
            return

        await query.message.reply_text(
            "💳 پرداخت‌های در انتظار:\n\n" +
            "\n".join(
                f"#{r['id']} | user={r['user_id']} | "
                f"{r['days']} روز | {r['amount']:,} تومان"
                for r in rows
            )
        )

async def text_router(update, context):
    ensure_user(update.effective_user)

    text = (update.message.text or "").strip()
    mode = context.user_data.get("awaiting_symbol")

    if mode in ("add", "analysis"):
        symbol = normalize_symbol(text)

        if not re.fullmatch(r"[A-Z0-9]{4,20}", symbol):
            await update.message.reply_text(
                "❌ نماد نامعتبر است.\n"
                "مثال: BTC یا ZEC"
            )
            return

        context.user_data.pop("awaiting_symbol", None)

        if mode == "add":
            try:
                await get_data(symbol)
            except Exception as exc:
                await update.message.reply_text(
                    f"❌ {symbol} پیدا نشد.\n{exc}"
                )
                return

            c = db()

            c.execute(
                "INSERT OR IGNORE INTO watchlist("
                "user_id,symbol,asset_type,created_at"
                ") VALUES(?,?,?,?)",
                (
                    update.effective_user.id,
                    symbol,
                    "crypto",
                    now_iso()
                )
            )

            c.commit()
            c.close()

            await update.message.reply_text(
                f"✅ {symbol} به واچ‌لیست اضافه شد."
            )

        else:
            if not has_analysis_access(
                update.effective_user.id
            ):
                await update.message.reply_text(
                    "🔒 اشتراک فعال ندارید."
                )
                return

            try:
                await update.message.reply_text(
                    "⏳ در حال دریافت داده و تحلیل..."
                )

                await update.message.reply_text(
                    await analysis_text(symbol)
                )

            except Exception as exc:
                log.exception(
                    "analysis failed for %s",
                    symbol
                )

                await update.message.reply_text(
                    f"❌ تحلیل {symbol} انجام نشد.\n"
                    f"علت: {exc}"
                )

        return

    handlers = {
        "➕ افزودن ارز": add_coin_prompt,
        "📋 واچ‌لیست": watchlist,
        "📊 تحلیل": analysis_prompt,
        "🚨 سیگنال‌ها": signals,
        "💳 خرید اشتراک": buy_menu,
        "👤 وضعیت اشتراک": subscription_status,
        "🔔 هشدارها": alerts,
        "ℹ️ راهنما": help_cmd
    }

    fn = handlers.get(text)

    if fn:
        await fn(update, context)

async def error_handler(update, context):
    log.exception(
        "Unhandled update error",
        exc_info=context.error
    )

# ---------------- STARTUP ----------------

async def shutdown_http():
    global _http_session

    if _http_session and not _http_session.closed:
        await _http_session.close()

async def main():
    init_db()

    log.info("Starting Crypto Analyzer...")
    log.info("DB_PATH=%s", DB_PATH)
    log.info("ADMIN_IDS=%s", sorted(ADMIN_IDS))

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .build()
    )

    application.add_handler(
        CommandHandler("start", start)
    )

    application.add_handler(
        CommandHandler("admin", admin)
    )

    application.add_handler(
        CommandHandler("id", myid)
    )

    application.add_handler(
        CallbackQueryHandler(
            plan_callback,
            pattern=r"^plan:(30|90|180)$"
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            payment_callback,
            pattern=r"^(payok|payno):\d+$"
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            admin_callback,
            pattern=r"^admin:"
        )
    )

    application.add_handler(
        MessageHandler(
            filters.PHOTO,
            receipt
        )
    )

    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            text_router
        )
    )

    application.add_error_handler(
        error_handler
    )

    await application.initialize()
    await application.start()

    # Make sure an old webhook cannot block polling.
    await application.bot.delete_webhook(
        drop_pending_updates=True
    )

    await application.updater.start_polling(
        drop_pending_updates=True
    )

    log.info(
        "Crypto Analyzer started successfully."
    )

    try:
        while True:
            await asyncio.sleep(3600)

    finally:
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        await shutdown_http()

if __name__ == "__main__":
    asyncio.run(main())
