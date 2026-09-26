# -*- coding: utf-8 -*-
"""
Crypto Analyzer Telegram Bot - Railway Stable Production Build
Analysis/signals only. No automatic trading.

Required Railway variable:
    TELEGRAM_BOT_TOKEN

Recommended:
    ADMIN_IDS=123456789,987654321
    PAYMENT_CARD=6037...
    SUPPORT_USERNAME=@username

Optional:
    DB_PATH=/data/crypto_bot.db
    HTTP_TIMEOUT=20
    CACHE_SECONDS=30
    ALERT_INTERVAL_SECONDS=300

Start command:
    python main.py
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
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    CallbackQueryHandler,
    filters,
)


# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}

DB_PATH = os.getenv("DB_PATH", "/data/crypto_bot.db").strip()
HTTP_TIMEOUT = max(5, int(os.getenv("HTTP_TIMEOUT", "20")))
CACHE_SECONDS = max(5, int(os.getenv("CACHE_SECONDS", "30")))
ALERT_INTERVAL_SECONDS = max(
    60,
    int(os.getenv("ALERT_INTERVAL_SECONDS", "300"))
)

PAYMENT_CARD = os.getenv("PAYMENT_CARD", "ثبت نشده").strip()
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")

db_dir = os.path.dirname(DB_PATH)
if db_dir:
    os.makedirs(db_dir, exist_ok=True)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | crypto-analyzer | %(message)s",
)

log = logging.getLogger("crypto-analyzer")


# ============================================================
# UI
# ============================================================

MAIN_MENU = ReplyKeyboardMarkup(
    [
        [KeyboardButton("➕ افزودن ارز"), KeyboardButton("📋 واچ‌لیست")],
        [KeyboardButton("📊 تحلیل"), KeyboardButton("🚨 سیگنال‌ها")],
        [KeyboardButton("💳 خرید اشتراک"), KeyboardButton("👤 وضعیت اشتراک")],
        [KeyboardButton("🔔 هشدارها"), KeyboardButton("ℹ️ راهنما")],
    ],
    resize_keyboard=True,
)


# ============================================================
# DATABASE
# ============================================================

def db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def init_db():
    conn = db()

    conn.executescript(
        """
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
            interval_seconds INTEGER NOT NULL DEFAULT 300,
            last_check_at TEXT
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

        CREATE INDEX IF NOT EXISTS idx_alert_user
            ON alert_events(user_id, created_at);
        """
    )

    # Safe additive migrations.
    user_cols = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(users)").fetchall()
    }

    if "blocked" not in user_cols:
        conn.execute(
            "ALTER TABLE users "
            "ADD COLUMN blocked INTEGER NOT NULL DEFAULT 0"
        )

    alert_cols = {
        row["name"]
        for row in conn.execute(
            "PRAGMA table_info(alert_preferences)"
        ).fetchall()
    }

    if "last_check_at" not in alert_cols:
        conn.execute(
            "ALTER TABLE alert_preferences "
            "ADD COLUMN last_check_at TEXT"
        )

    conn.commit()
    conn.close()


def ensure_user(tg_user):
    if tg_user is None:
        return

    conn = db()
    stamp = now_iso()

    conn.execute(
        """
        INSERT INTO users(
            user_id, username, first_name, created_at, last_seen
        )
        VALUES(?,?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            last_seen=excluded.last_seen
        """,
        (
            tg_user.id,
            tg_user.username or "",
            tg_user.first_name or "",
            stamp,
            stamp,
        ),
    )

    conn.commit()
    conn.close()


def is_admin(user_id):
    return user_id in ADMIN_IDS


def active_subscription(user_id):
    conn = db()

    row = conn.execute(
        """
        SELECT *
        FROM subscriptions
        WHERE user_id=?
          AND status='active'
          AND end_at>?
        ORDER BY datetime(end_at) DESC, id DESC
        LIMIT 1
        """,
        (user_id, now_iso()),
    ).fetchone()

    conn.close()
    return row


def has_analysis_access(user_id):
    return bool(is_admin(user_id) or active_subscription(user_id))


def add_subscription(
    user_id,
    plan,
    days,
    amount,
    source="manual",
    payment_request_id=None,
    reviewed_by=None,
):
    start = datetime.now(timezone.utc)

    conn = db()

    try:
        # Prevent accidental duplicate approval.
        if payment_request_id:
            request = conn.execute(
                """
                SELECT status
                FROM payment_requests
                WHERE id=?
                """,
                (payment_request_id,),
            ).fetchone()

            if not request:
                raise RuntimeError("درخواست پرداخت پیدا نشد.")

            if request["status"] != "pending":
                raise RuntimeError("این درخواست قبلاً بررسی شده است.")

        old = conn.execute(
            """
            SELECT end_at
            FROM subscriptions
            WHERE user_id=?
              AND status='active'
              AND end_at>?
            ORDER BY datetime(end_at) DESC, id DESC
            LIMIT 1
            """,
            (user_id, start.isoformat()),
        ).fetchone()

        if old:
            try:
                old_end = datetime.fromisoformat(old["end_at"])
                if old_end > start:
                    start = old_end
            except Exception:
                pass

        end = start + timedelta(days=days)

        conn.execute(
            """
            INSERT INTO subscriptions(
                user_id, plan, days, amount,
                start_at, end_at, status, source,
                payment_request_id, created_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                user_id,
                plan,
                days,
                amount,
                start.isoformat(),
                end.isoformat(),
                "active",
                source,
                payment_request_id,
                now_iso(),
            ),
        )

        if payment_request_id:
            conn.execute(
                """
                UPDATE payment_requests
                SET status='approved',
                    reviewed_at=?,
                    reviewed_by=?
                WHERE id=? AND status='pending'
                """,
                (
                    now_iso(),
                    reviewed_by or 0,
                    payment_request_id,
                ),
            )

            if conn.total_changes <= 0:
                raise RuntimeError("درخواست پرداخت قبلاً بررسی شده است.")

        conn.commit()
        return end

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


# ============================================================
# MARKET DATA
# ============================================================

_http_session = None
_cache = {}
_cache_lock = None


def get_cache_lock():
    global _cache_lock

    if _cache_lock is None:
        _cache_lock = asyncio.Lock()

    return _cache_lock


async def get_http_session():
    global _http_session

    if _http_session is None or _http_session.closed:
        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)

        _http_session = aiohttp.ClientSession(
            timeout=timeout,
            headers={
                "User-Agent": "CryptoAnalyzer/2.0",
                "Accept": "application/json",
            },
        )

    return _http_session


async def http_json(url, params=None):
    session = await get_http_session()

    last_error = None

    for attempt in range(3):
        try:
            async with session.get(url, params=params) as response:
                text = await response.text()

                if response.status != 200:
                    raise RuntimeError(
                        f"HTTP {response.status}: {text[:200]}"
                    )

                try:
                    return await response.json(
                        content_type=None
                    )
                except Exception as exc:
                    raise RuntimeError(
                        "پاسخ سرویس داده قابل خواندن نیست."
                    ) from exc

        except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as exc:
            last_error = exc

            if attempt < 2:
                await asyncio.sleep(0.7 * (attempt + 1))

    raise RuntimeError(str(last_error))


def normalize_symbol(raw):
    value = re.sub(r"[^A-Za-z0-9]", "", raw or "").upper()

    if not value:
        return ""

    if value.endswith("USDT"):
        return value

    return value + "USDT"


async def binance_klines(symbol, interval="1h", limit=200):
    data = await http_json(
        "https://api.binance.com/api/v3/klines",
        {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        },
    )

    if not isinstance(data, list) or len(data) < 30:
        raise RuntimeError("داده کافی از Binance دریافت نشد.")

    rows = []

    for item in data:
        if len(item) < 6:
            continue

        rows.append(
            {
                "time": pd.to_datetime(
                    int(item[0]),
                    unit="ms",
                    utc=True,
                ),
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]),
            }
        )

    df = pd.DataFrame(rows)

    if len(df) < 30:
        raise RuntimeError("داده کندلی معتبر کافی نیست.")

    return df


async def coingecko_chart(coin_id, days=30):
    data = await http_json(
        f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart",
        {
            "vs_currency": "usd",
            "days": days,
            "interval": "hourly",
        },
    )

    prices = data.get("prices") or []

    if len(prices) < 30:
        raise RuntimeError("داده کافی از CoinGecko دریافت نشد.")

    closes = []
    times = []

    for item in prices:
        if len(item) >= 2:
            times.append(
                pd.to_datetime(
                    int(item[0]),
                    unit="ms",
                    utc=True,
                )
            )
            closes.append(float(item[1]))

    if len(closes) < 30:
        raise RuntimeError("داده معتبر CoinGecko کافی نیست.")

    return pd.DataFrame(
        {
            "time": times,
            "open": closes,
            "high": closes,
            "low": closes,
            "close": closes,
            "volume": np.nan,
        }
    )


async def get_data(symbol):
    symbol = normalize_symbol(symbol)

    if not symbol:
        raise RuntimeError("نماد ارز خالی است.")

    key = ("data", symbol)
    lock = get_cache_lock()

    async with lock:
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
            "DOT": "polkadot",
            "AVAX": "avalanche-2",
            "LINK": "chainlink",
            "LTC": "litecoin",
            "BCH": "bitcoin-cash",
            "ETC": "ethereum-classic",
            "ATOM": "cosmos",
            "NEAR": "near",
            "UNI": "uniswap",
            "APT": "aptos",
            "SUI": "sui",
            "FIL": "filecoin",
            "ARB": "arbitrum",
            "OP": "optimism",
        }

        coin_id = ids.get(base)

        if not coin_id:
            raise RuntimeError(
                f"{symbol} در Binance Spot/USDT پیدا نشد "
                "و منبع پشتیبان شناخته‌شده‌ای برای آن وجود ندارد."
            )

        df = await coingecko_chart(coin_id)

        log.warning(
            "Binance failed for %s; CoinGecko fallback used: %s",
            symbol,
            binance_error,
        )

    async with lock:
        _cache[key] = (time.time(), df.copy())

    return df


# ============================================================
# ANALYSIS
# ============================================================

def rsi(series, period=14):
    delta = series.diff()

    gain = delta.clip(lower=0).ewm(
        alpha=1 / period,
        adjust=False,
    ).mean()

    loss = (-delta.clip(upper=0)).ewm(
        alpha=1 / period,
        adjust=False,
    ).mean()

    rs = gain / loss.replace(0, np.nan)

    result = 100 - (100 / (1 + rs))

    return result.fillna(50)


def calculate(df):
    if df is None or len(df) < 30:
        raise RuntimeError("برای تحلیل، حداقل ۳۰ کندل لازم است.")

    d = df.copy()

    for column in ("open", "high", "low", "close", "volume"):
        if column not in d.columns:
            raise RuntimeError(f"ستون {column} در داده وجود ندارد.")

    close = pd.to_numeric(d["close"], errors="coerce")

    d["ema9"] = close.ewm(
        span=9,
        adjust=False,
    ).mean()

    d["ema21"] = close.ewm(
        span=21,
        adjust=False,
    ).mean()

    d["rsi"] = rsi(close)

    d["ret6"] = close.pct_change(6) * 100
    d["ret24"] = close.pct_change(24) * 100

    d["vol_ma"] = (
        pd.to_numeric(
            d["volume"],
            errors="coerce",
        )
        .rolling(20)
        .mean()
    )

    last = d.iloc[-1]

    score = 0.0
    reasons = []

    if last["ema9"] > last["ema21"]:
        score += 25
        reasons.append("EMA9 بالای EMA21")
    elif last["ema9"] < last["ema21"]:
        score -= 25
        reasons.append("EMA9 زیر EMA21")
    else:
        reasons.append("EMA9 و EMA21 نزدیک")

    if last["rsi"] >= 55:
        score += 20
        reasons.append("RSI مثبت")
    elif last["rsi"] <= 45:
        score -= 20
        reasons.append("RSI منفی")
    else:
        reasons.append("RSI خنثی")

    if pd.notna(last["ret6"]):
        if last["ret6"] > 0:
            score += 10
        elif last["ret6"] < 0:
            score -= 10

    if pd.notna(last["ret24"]):
        if last["ret24"] > 0:
            score += 10
        elif last["ret24"] < 0:
            score -= 10

    volume_value = last["volume"]
    volume_average = last["vol_ma"]

    if (
        pd.notna(volume_value)
        and pd.notna(volume_average)
        and volume_average > 0
    ):
        if volume_value > volume_average:
            score += 5 if score >= 0 else -5
            reasons.append("حجم بالاتر از میانگین")

    score = float(max(-80, min(80, score)))

    # این دو معیار عمداً جدا نگه داشته شده‌اند.
    strength = min(100, max(0, 50 + abs(score) * 2))
    probability = min(95, max(5, 50 + score * 0.8))

    if score >= 25:
        signal = "🟢 خرید / صعودی"
    elif score <= -25:
        signal = "🔴 فروش / نزولی"
    else:
        signal = "🟡 انتظار / خنثی"

    look = d.tail(min(48, len(d)))

    support = float(look["low"].min())
    resistance = float(look["high"].max())

    return {
        "price": float(last["close"]),
        "rsi": float(last["rsi"]),
        "ema9": float(last["ema9"]),
        "ema21": float(last["ema21"]),
        "change6": (
            float(last["ret6"])
            if pd.notna(last["ret6"])
            else 0.0
        ),
        "change24": (
            float(last["ret24"])
            if pd.notna(last["ret24"])
            else 0.0
        ),
        "score": score,
        "strength": float(strength),
        "probability": float(probability),
        "signal": signal,
        "support": support,
        "resistance": resistance,
        "reasons": reasons,
    }


async def analyze(symbol):
    df = await get_data(symbol)
    return calculate(df)


def fmt_num(value):
    value = float(value)

    if value >= 1000:
        return f"{value:,.2f}"

    if value >= 1:
        return f"{value:,.4f}"

    return f"{value:,.8f}"


async def analysis_text(symbol):
    symbol = normalize_symbol(symbol)
    a = await analyze(symbol)

    reasons = "\n".join(
        f"• {reason}"
        for reason in a["reasons"]
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


# ============================================================
# HELPERS
# ============================================================

async def safe_send(bot, chat_id, text, **kwargs):
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            **kwargs,
        )
        return True
    except Exception:
        log.exception("send_message failed for chat_id=%s", chat_id)
        return False


def watchlist_rows(user_id):
    conn = db()

    rows = conn.execute(
        """
        SELECT symbol, asset_type, created_at
        FROM watchlist
        WHERE user_id=?
        ORDER BY symbol
        """,
        (user_id,),
    ).fetchall()

    conn.close()
    return rows


# ============================================================
# BASIC HANDLERS
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user(update.effective_user)

    await update.message.reply_text(
        "🤖 Crypto Analyzer\n\n"
        "تحلیل و سیگنال بازار ارزهای دیجیتال، "
        "بدون اجرای خودکار معامله.\n\n"
        "از منوی پایین یک گزینه را انتخاب کنید.",
        reply_markup=MAIN_MENU,
    )


async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user(update.effective_user)

    await update.message.reply_text(
        f"🆔 شناسه عددی شما:\n{update.effective_user.id}"
    )


# ============================================================
# SUBSCRIPTION
# ============================================================

async def subscription_status(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    ensure_user(update.effective_user)

    subscription = active_subscription(
        update.effective_user.id
    )

    if not subscription:
        await update.message.reply_text(
            "👤 وضعیت اشتراک\n\n"
            "❌ اشتراک فعال ندارید.\n"
            "برای استفاده از تحلیل و سیگنال، اشتراک تهیه کنید."
        )
        return

    try:
        end = datetime.fromisoformat(
            subscription["end_at"]
        ).astimezone(timezone.utc)

        end_text = end.strftime("%Y-%m-%d %H:%M")
    except Exception:
        end_text = subscription["end_at"]

    await update.message.reply_text(
        "👤 وضعیت اشتراک\n\n"
        "✅ فعال\n"
        f"📦 طرح: {subscription['plan']}\n"
        f"📅 پایان: {end_text} UTC"
    )


async def buy_menu(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "۳۰ روز — ۲۰۰,۰۰۰ تومان",
                    callback_data="plan:30",
                )
            ],
            [
                InlineKeyboardButton(
                    "۹۰ روز — ۳۵۰,۰۰۰ تومان",
                    callback_data="plan:90",
                )
            ],
            [
                InlineKeyboardButton(
                    "۱۸۰ روز — ۵۰۰,۰۰۰ تومان",
                    callback_data="plan:180",
                )
            ],
        ]
    )

    await update.message.reply_text(
        "💳 انتخاب اشتراک:",
        reply_markup=keyboard,
    )


async def plan_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    query = update.callback_query
    await query.answer()

    try:
        days = int(query.data.split(":")[1])
    except Exception:
        await query.message.reply_text("❌ طرح نامعتبر است.")
        return

    amounts = {
        30: 200000,
        90: 350000,
        180: 500000,
    }

    if days not in amounts:
        await query.message.reply_text("❌ طرح نامعتبر است.")
        return

    amount = amounts[days]

    context.user_data["pending_plan"] = (
        days,
        amount,
    )

    await query.message.reply_text(
        f"💳 اشتراک {days} روزه\n"
        f"مبلغ: {amount:,} تومان\n\n"
        f"شماره کارت:\n{PAYMENT_CARD}\n\n"
        "پس از پرداخت، تصویر رسید را همین‌جا ارسال کنید."
    )


async def receipt(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    ensure_user(update.effective_user)

    plan = context.user_data.get("pending_plan")

    if not plan:
        await update.message.reply_text(
            "ابتدا از «💳 خرید اشتراک» یک طرح را انتخاب کنید."
        )
        return

    if not update.message.photo:
        await update.message.reply_text(
            "❌ تصویر رسید دریافت نشد."
        )
        return

    days, amount = plan
    file_id = update.message.photo[-1].file_id

    conn = db()

    cur = conn.execute(
        """
        INSERT INTO payment_requests(
            user_id, plan, days, amount,
            receipt_file_id, status, created_at
        )
        VALUES(?,?,?,?,?,?,?)
        """,
        (
            update.effective_user.id,
            f"{days} روزه",
            days,
            amount,
            file_id,
            "pending",
            now_iso(),
        ),
    )

    request_id = cur.lastrowid

    conn.commit()
    conn.close()

    context.user_data.pop("pending_plan", None)

    await update.message.reply_text(
        "✅ رسید دریافت شد.\n"
        "پس از بررسی مدیر، نتیجه برای شما ارسال می‌شود."
    )

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_photo(
                chat_id=admin_id,
                photo=file_id,
                caption=(
                    f"💳 درخواست پرداخت #{request_id}\n"
                    f"کاربر: {update.effective_user.id}\n"
                    f"طرح: {days} روزه\n"
                    f"مبلغ: {amount:,} تومان"
                ),
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "✅ تأیید",
                                callback_data=f"payok:{request_id}",
                            ),
                            InlineKeyboardButton(
                                "❌ رد",
                                callback_data=f"payno:{request_id}",
                            ),
                        ]
                    ]
                ),
            )
        except Exception:
            log.exception(
                "Cannot notify admin %s",
                admin_id,
            )


async def payment_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    query = update.callback_query
    await query.answer()

    if not is_admin(query.from_user.id):
        await query.message.reply_text(
            "⛔ دسترسی ندارید."
        )
        return

    try:
        action, value = query.data.split(":", 1)
        request_id = int(value)
    except Exception:
        await query.message.reply_text(
            "❌ درخواست نامعتبر است."
        )
        return

    conn = db()

    try:
        request = conn.execute(
            """
            SELECT *
            FROM payment_requests
            WHERE id=?
            """,
            (request_id,),
        ).fetchone()

        if not request:
            await query.message.reply_text(
                "❌ درخواست پیدا نشد."
            )
            return

        if request["status"] != "pending":
            await query.message.reply_text(
                "ℹ️ این درخواست قبلاً بررسی شده است."
            )
            return

        if action == "payno":
            conn.execute(
                """
                UPDATE payment_requests
                SET status='rejected',
                    reviewed_at=?,
                    reviewed_by=?
                WHERE id=? AND status='pending'
                """,
                (
                    now_iso(),
                    query.from_user.id,
                    request_id,
                ),
            )

            conn.commit()

            await query.message.reply_text(
                f"❌ پرداخت #{request_id} رد شد."
            )

            await safe_send(
                context.bot,
                request["user_id"],
                "❌ رسید پرداخت شما رد شد."
            )
            return

        if action != "payok":
            await query.message.reply_text(
                "❌ عملیات نامعتبر است."
            )
            return

    finally:
        conn.close()

    try:
        end = add_subscription(
            request["user_id"],
            request["plan"],
            request["days"],
            request["amount"],
            source="manual",
            payment_request_id=request_id,
            reviewed_by=query.from_user.id,
        )
    except Exception as exc:
        await query.message.reply_text(
            f"❌ فعال‌سازی انجام نشد:\n{exc}"
        )
        return

    await query.message.reply_text(
        f"✅ پرداخت #{request_id} تأیید شد.\n"
        f"پایان اشتراک: {end.strftime('%Y-%m-%d %H:%M')} UTC"
    )

    await safe_send(
        context.bot,
        request["user_id"],
        "✅ پرداخت تأیید شد و اشتراک شما فعال شد."
    )


# ============================================================
# WATCHLIST / ANALYSIS
# ============================================================

async def add_coin_prompt(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    context.user_data["awaiting_symbol"] = "add"

    await update.message.reply_text(
        "➕ نماد ارز را بفرستید.\n\n"
        "مثال:\n"
        "BTC\n"
        "ZEC\n"
        "SOLUSDT"
    )


async def analysis_prompt(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
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


async def signals(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not has_analysis_access(update.effective_user.id):
        await update.message.reply_text(
            "🔒 برای سیگنال، ابتدا اشتراک فعال تهیه کنید."
        )
        return

    rows = watchlist_rows(
        update.effective_user.id
    )

    if not rows:
        await update.message.reply_text(
            "📋 واچ‌لیست خالی است.\n"
            "ابتدا ارز اضافه کنید."
        )
        return

    await update.message.reply_text(
        "⏳ در حال تحلیل واچ‌لیست..."
    )

    for row in rows:
        symbol = row["symbol"]

        try:
            await update.message.reply_text(
                await analysis_text(symbol)
            )
        except Exception as exc:
            log.exception(
                "watchlist analysis failed for %s",
                symbol,
            )

            await update.message.reply_text(
                f"❌ {symbol}: {exc}"
            )


async def watchlist(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    rows = watchlist_rows(
        update.effective_user.id
    )

    if not rows:
        await update.message.reply_text(
            "📋 واچ‌لیست شما خالی است."
        )
        return

    keyboard = []

    for row in rows:
        symbol = row["symbol"]

        keyboard.append(
            [
                InlineKeyboardButton(
                    f"📊 {symbol}",
                    callback_data=f"wl:analyze:{symbol}",
                ),
                InlineKeyboardButton(
                    "🗑 حذف",
                    callback_data=f"wl:delete:{symbol}",
                ),
            ]
        )

    await update.message.reply_text(
        "📋 واچ‌لیست شما:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def watchlist_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    query = update.callback_query
    await query.answer()

    if not query.data.startswith("wl:"):
        return

    parts = query.data.split(":", 2)

    if len(parts) != 3:
        return

    action = parts[1]
    symbol = parts[2]

    if action == "delete":
        conn = db()

        conn.execute(
            """
            DELETE FROM watchlist
            WHERE user_id=? AND symbol=?
            """,
            (
                query.from_user.id,
                symbol,
            ),
        )

        conn.commit()
        conn.close()

        await query.message.reply_text(
            f"🗑 {symbol} از واچ‌لیست حذف شد."
        )
        return

    if action == "analyze":
        if not has_analysis_access(query.from_user.id):
            await query.message.reply_text(
                "🔒 برای تحلیل، اشتراک فعال لازم است."
            )
            return

        try:
            await query.message.reply_text(
                await analysis_text(symbol)
            )
        except Exception as exc:
            await query.message.reply_text(
                f"❌ تحلیل {symbol} انجام نشد.\n{exc}"
            )


# ============================================================
# HELP / ALERTS
# ============================================================

async def help_cmd(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    support = (
        f"\n📞 پشتیبانی: {SUPPORT_USERNAME}"
        if SUPPORT_USERNAME
        else ""
    )

    await update.message.reply_text(
        "ℹ️ راهنما\n\n"
        "➕ افزودن ارز: اضافه کردن ارز به واچ‌لیست\n"
        "📋 واچ‌لیست: مشاهده و حذف ارزها\n"
        "📊 تحلیل: تحلیل تکنیکال یک ارز\n"
        "🚨 سیگنال‌ها: تحلیل همه ارزهای واچ‌لیست\n"
        "🔔 هشدارها: فعال/غیرفعال کردن بررسی دوره‌ای\n"
        "💳 خرید اشتراک: پرداخت دستی و ارسال رسید\n"
        "👤 وضعیت اشتراک: مشاهده وضعیت اشتراک\n\n"
        "این ربات معامله خودکار انجام نمی‌دهد."
        + support
    )


async def alerts(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    conn = db()

    row = conn.execute(
        """
        SELECT enabled, interval_seconds
        FROM alert_preferences
        WHERE user_id=?
        """,
        (update.effective_user.id,),
    ).fetchone()

    conn.close()

    enabled = bool(row["enabled"]) if row else False

    status = "🟢 فعال" if enabled else "🔴 غیرفعال"

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🟢 فعال کردن",
                    callback_data="alert:on",
                ),
                InlineKeyboardButton(
                    "🔴 خاموش کردن",
                    callback_data="alert:off",
                ),
            ]
        ]
    )

    await update.message.reply_text(
        "🔔 هشدارهای هوشمند\n\n"
        f"وضعیت: {status}\n"
        f"بررسی هر {ALERT_INTERVAL_SECONDS // 60} دقیقه\n\n"
        "هشدار فقط برای ارزهای موجود در واچ‌لیست بررسی می‌شود.",
        reply_markup=keyboard,
    )


async def alert_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    query = update.callback_query
    await query.answer()

    action = query.data

    if action not in ("alert:on", "alert:off"):
        return

    enabled = 1 if action == "alert:on" else 0

    conn = db()

    conn.execute(
        """
        INSERT INTO alert_preferences(
            user_id, enabled, interval_seconds, last_check_at
        )
        VALUES(?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
            enabled=excluded.enabled,
            interval_seconds=excluded.interval_seconds
        """,
        (
            query.from_user.id,
            enabled,
            ALERT_INTERVAL_SECONDS,
            None,
        ),
    )

    conn.commit()
    conn.close()

    if enabled:
        await query.message.reply_text(
            "🟢 هشدارها فعال شد.\n"
            "واچ‌لیست به‌صورت دوره‌ای بررسی می‌شود."
        )
    else:
        await query.message.reply_text(
            "🔴 هشدارها خاموش شد."
        )


# ============================================================
# ALERT WORKER
# ============================================================

async def alert_worker(application):
    log.info(
        "Alert worker started. interval=%s",
        ALERT_INTERVAL_SECONDS,
    )

    while True:
        try:
            await asyncio.sleep(ALERT_INTERVAL_SECONDS)

            conn = db()

            users = conn.execute(
                """
                SELECT user_id
                FROM alert_preferences
                WHERE enabled=1
                """
            ).fetchall()

            conn.close()

            for row in users:
                user_id = row["user_id"]

                if not has_analysis_access(user_id):
                    continue

                rows = watchlist_rows(user_id)

                for item in rows:
                    symbol = item["symbol"]

                    try:
                        analysis = await analyze(symbol)

                        # Only strong directional setups generate alerts.
                        if analysis["score"] >= 25:
                            direction = "🟢 سیگنال صعودی"
                        elif analysis["score"] <= -25:
                            direction = "🔴 سیگنال نزولی"
                        else:
                            continue

                        message = (
                            f"🚨 هشدار {symbol}\n\n"
                            f"{direction}\n"
                            f"💰 قیمت: {fmt_num(analysis['price'])}\n"
                            f"💪 قدرت: {analysis['strength']:.0f}%\n"
                            f"🎯 احتمال سود: "
                            f"{analysis['probability']:.0f}%\n"
                            f"RSI: {analysis['rsi']:.1f}\n\n"
                            "⚠️ این هشدار تضمین سود نیست."
                        )

                        # Prevent identical repeated alerts.
                        conn = db()

                        previous = conn.execute(
                            """
                            SELECT message
                            FROM alert_events
                            WHERE user_id=? AND symbol=?
                            ORDER BY id DESC
                            LIMIT 1
                            """,
                            (
                                user_id,
                                symbol,
                            ),
                        ).fetchone()

                        if previous and previous["message"] == message:
                            conn.close()
                            continue

                        sent = await safe_send(
                            application.bot,
                            user_id,
                            message,
                        )

                        if sent:
                            conn.execute(
                                """
                                INSERT INTO alert_events(
                                    user_id, symbol, message, created_at
                                )
                                VALUES(?,?,?,?)
                                """,
                                (
                                    user_id,
                                    symbol,
                                    message,
                                    now_iso(),
                                ),
                            )

                            conn.commit()

                        conn.close()

                    except Exception:
                        log.exception(
                            "Alert analysis failed user=%s symbol=%s",
                            user_id,
                            symbol,
                        )

        except asyncio.CancelledError:
            log.info("Alert worker stopped.")
            raise

        except Exception:
            log.exception("Alert worker cycle failed.")
            await asyncio.sleep(10)


# ============================================================
# ADMIN
# ============================================================

async def admin(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text(
            "⛔ دسترسی ندارید."
        )
        return

    conn = db()

    users = conn.execute(
        "SELECT COUNT(*) n FROM users"
    ).fetchone()["n"]

    active_subs = conn.execute(
        """
        SELECT COUNT(*) n
        FROM subscriptions
        WHERE status='active' AND end_at>?
        """,
        (now_iso(),),
    ).fetchone()["n"]

    pending = conn.execute(
        """
        SELECT COUNT(*) n
        FROM payment_requests
        WHERE status='pending'
        """
    ).fetchone()["n"]

    conn.close()

    await update.message.reply_text(
        "📊 پنل مدیریت\n\n"
        f"👥 کاربران: {users}\n"
        f"🟢 اشتراک فعال: {active_subs}\n"
        f"💳 پرداخت در انتظار: {pending}",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "💳 پرداخت‌ها",
                        callback_data="admin:payments",
                    )
                ]
            ]
        ),
    )


async def admin_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    query = update.callback_query
    await query.answer()

    if not is_admin(query.from_user.id):
        await query.message.reply_text(
            "⛔ دسترسی ندارید."
        )
        return

    if query.data != "admin:payments":
        return

    conn = db()

    rows = conn.execute(
        """
        SELECT id, user_id, days, amount, created_at
        FROM payment_requests
        WHERE status='pending'
        ORDER BY id DESC
        LIMIT 20
        """
    ).fetchall()

    conn.close()

    if not rows:
        await query.message.reply_text(
            "💳 پرداخت در انتظار وجود ندارد."
        )
        return

    text = "💳 پرداخت‌های در انتظار:\n\n"

    for row in rows:
        text += (
            f"#{row['id']} | "
            f"user={row['user_id']} | "
            f"{row['days']} روز | "
            f"{row['amount']:,} تومان\n"
        )

    await query.message.reply_text(text)


# ============================================================
# TEXT ROUTER
# ============================================================

async def text_router(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    ensure_user(update.effective_user)

    text = (update.message.text or "").strip()

    mode = context.user_data.get(
        "awaiting_symbol"
    )

    if mode in ("add", "analysis"):
        symbol = normalize_symbol(text)

        if not re.fullmatch(
            r"[A-Z0-9]{4,20}",
            symbol,
        ):
            await update.message.reply_text(
                "❌ نماد نامعتبر است.\n"
                "مثال: BTC یا ZEC"
            )
            return

        context.user_data.pop(
            "awaiting_symbol",
            None,
        )

        if mode == "add":
            try:
                await get_data(symbol)
            except Exception as exc:
                await update.message.reply_text(
                    f"❌ {symbol} پیدا نشد.\n{exc}"
                )
                return

            conn = db()

            conn.execute(
                """
                INSERT OR IGNORE INTO watchlist(
                    user_id, symbol, asset_type, created_at
                )
                VALUES(?,?,?,?)
                """,
                (
                    update.effective_user.id,
                    symbol,
                    "crypto",
                    now_iso(),
                ),
            )

            conn.commit()
            conn.close()

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
                    symbol,
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
        "ℹ️ راهنما": help_cmd,
    }

    fn = handlers.get(text)

    if fn:
        await fn(update, context)


# ============================================================
# ERROR / SHUTDOWN
# ============================================================

async def error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
):
    error = context.error

    if error:
        log.error(
            "Unhandled update error: %s",
            error,
            exc_info=(
                type(error),
                error,
                error.__traceback__,
            ),
        )


async def shutdown_http():
    global _http_session

    if _http_session and not _http_session.closed:
        try:
            await _http_session.close()
        except Exception:
            log.exception("HTTP session close failed")

    _http_session = None


# ============================================================
# MAIN
# ============================================================

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

    # Commands
    application.add_handler(
        CommandHandler("start", start)
    )

    application.add_handler(
        CommandHandler("admin", admin)
    )

    application.add_handler(
        CommandHandler("id", myid)
    )

    # Subscription buttons
    application.add_handler(
        CallbackQueryHandler(
            plan_callback,
            pattern=r"^plan:(30|90|180)$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            payment_callback,
            pattern=r"^(payok|payno):\d+$",
        )
    )

    # Watchlist
    application.add_handler(
        CallbackQueryHandler(
            watchlist_callback,
            pattern=r"^wl:",
        )
    )

    # Alerts
    application.add_handler(
        CallbackQueryHandler(
            alert_callback,
            pattern=r"^alert:(on|off)$",
        )
    )

    # Admin
    application.add_handler(
        CallbackQueryHandler(
            admin_callback,
            pattern=r"^admin:",
        )
    )

    # Receipt
    application.add_handler(
        MessageHandler(
            filters.PHOTO,
            receipt,
        )
    )

    # Text/menu
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            text_router,
        )
    )

    application.add_error_handler(
        error_handler
    )

    alert_task = None

    try:
        await application.initialize()
        await application.start()

        # Remove old webhook so polling is not blocked.
        await application.bot.delete_webhook(
            drop_pending_updates=True
        )

        await application.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
        )

        alert_task = asyncio.create_task(
            alert_worker(application)
        )

        log.info(
            "Crypto Analyzer started successfully."
        )

        while True:
            await asyncio.sleep(3600)

    except asyncio.CancelledError:
        log.info("Main task cancelled.")
        raise

    finally:
        if alert_task:
            alert_task.cancel()

            try:
                await alert_task
            except asyncio.CancelledError:
                pass
            except Exception:
                log.exception("Alert worker shutdown failed")

        try:
            if application.updater.running:
                await application.updater.stop()
        except Exception:
            log.exception("Updater shutdown failed")

        try:
            if application.running:
                await application.stop()
        except Exception:
            log.exception("Application stop failed")

        try:
            await application.shutdown()
        except Exception:
            log.exception("Application shutdown failed")

        await shutdown_http()

        log.info("Crypto Analyzer stopped.")


if __name__ == "__main__":
    asyncio.run(main())
