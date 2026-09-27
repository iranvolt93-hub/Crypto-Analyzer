# -*- coding: utf-8 -*-
"""
Crypto / Global Gold / Iran 18K Gold Telegram Analyzer
Railway production build
Analysis + signals + notifications. No automatic trading.

Required Railway variable:
    TELEGRAM_BOT_TOKEN

Recommended:
    ADMIN_IDS=123456789,987654321
    PAYMENT_CARD=6037...
    SUPPORT_USERNAME=@username

Recommended Railway volume:
    Mount: /data
    DB_PATH=/data/crypto_bot.db

Start command:
    python main.py

IMPORTANT:
- Use ONE running instance for this bot token.
- Never delete /data/crypto_bot.db when deploying a new version.
- Database migrations are additive and preserve users/subscriptions/history.
"""

import os
import re
import sqlite3
import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from html import escape

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
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

# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

ADMIN_IDS = set()
for item in os.getenv("ADMIN_IDS", "").split(","):
    item = item.strip()
    if item:
        try:
            ADMIN_IDS.add(int(item))
        except ValueError:
            pass

DB_PATH = os.getenv("DB_PATH", "/data/crypto_bot.db").strip()
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
CACHE_SECONDS = int(os.getenv("CACHE_SECONDS", "60"))
ALERT_INTERVAL_SECONDS = int(os.getenv("ALERT_INTERVAL_SECONDS", "300"))
MAX_WATCHLIST = int(os.getenv("MAX_WATCHLIST", "100"))

PAYMENT_CARD = os.getenv("PAYMENT_CARD", "ثبت نشده").strip()
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "پشتیبان").strip()

PLANS = {
    "30": (30, 200000),
    "90": (90, 350000),
    "180": (180, 500000),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("market_bot")

CACHE = {}
HTTP_SESSION = None


MAIN_MENU = [
    ["➕ افزودن دارایی", "📋 واچ‌لیست"],
    ["💰 قیمت لحظه‌ای", "📊 تحلیل"],
    ["🚨 سیگنال‌ها", "💳 خرید اشتراک"],
    ["👤 وضعیت اشتراک", "🔔 هشدارها"],
    ["🪙 ارزهای بیشتر", "💬 چت رمز ارز"],
    ["📨 ارتباط با پشتیبان", "ℹ️ راهنما"],
    ["👨‍💼 پنل مدیریت"],
]


# ============================================================
# DATABASE
# ============================================================

def db():
    folder = os.path.dirname(DB_PATH)
    if folder:
        os.makedirs(folder, exist_ok=True)

    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    con.execute("PRAGMA foreign_keys=ON")
    return con


def ensure_column(con, table, column, definition):
    cols = {row[1] for row in con.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        log.info("DB migration: %s.%s added", table, column)


def init_db():
    with db() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS app_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );

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
                amount INTEGER NOT NULL,
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
                asset_type TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(user_id, symbol, asset_type)
            );

            CREATE TABLE IF NOT EXISTS alert_preferences (
                user_id INTEGER PRIMARY KEY,
                enabled INTEGER NOT NULL DEFAULT 1,
                interval_seconds INTEGER NOT NULL DEFAULT 300,
                last_check_at TEXT
            );

            CREATE TABLE IF NOT EXISTS alert_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                signal_key TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS support_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                admin_id INTEGER,
                direction TEXT NOT NULL,
                message TEXT,
                telegram_message_id INTEGER,
                status TEXT NOT NULL DEFAULT 'open',
                created_at TEXT NOT NULL,
                replied_at TEXT
            );

            CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                asset_type TEXT NOT NULL,
                symbol TEXT NOT NULL,
                message TEXT NOT NULL,
                telegram_message_id INTEGER,
                created_at TEXT NOT NULL,
                deleted INTEGER NOT NULL DEFAULT 0,
                deleted_by INTEGER,
                deleted_at TEXT
            );

            CREATE TABLE IF NOT EXISTS chat_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER NOT NULL,
                reporter_id INTEGER NOT NULL,
                reason TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                reviewed_by INTEGER,
                reviewed_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_sub_user_end
                ON subscriptions(user_id, end_at);

            CREATE INDEX IF NOT EXISTS idx_pay_status
                ON payment_requests(status);

            CREATE INDEX IF NOT EXISTS idx_watch_asset
                ON watchlist(asset_type, symbol);

            CREATE INDEX IF NOT EXISTS idx_chat_room
                ON chat_messages(asset_type, symbol, created_at);

            CREATE INDEX IF NOT EXISTS idx_chat_reports
                ON chat_reports(status);

            CREATE INDEX IF NOT EXISTS idx_alert_events
                ON alert_events(user_id, symbol, created_at);
            """
        )

        # Additive migrations for older versions.
        ensure_column(c, "users", "blocked", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(c, "users", "last_seen", "TEXT")
        ensure_column(c, "alert_events", "signal_key", "TEXT DEFAULT ''")
        ensure_column(c, "support_messages", "replied_at", "TEXT")
        ensure_column(c, "chat_messages", "deleted_by", "INTEGER")
        ensure_column(c, "chat_messages", "deleted_at", "TEXT")

        c.execute(
            "INSERT OR REPLACE INTO app_meta(key,value) VALUES('schema_version','3')"
        )


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def ensure_user(user):
    if not user:
        return

    now = now_iso()

    with db() as c:
        c.execute(
            """
            INSERT INTO users(user_id,username,first_name,created_at,last_seen)
            VALUES(?,?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_seen=excluded.last_seen
            """,
            (
                user.id,
                user.username or "",
                user.first_name or "",
                now,
                now,
            ),
        )

        c.execute(
            """
            INSERT OR IGNORE INTO alert_preferences(
                user_id, enabled, interval_seconds
            ) VALUES(?,?,?)
            """,
            (user.id, 1, ALERT_INTERVAL_SECONDS),
        )


def is_admin(uid):
    return uid in ADMIN_IDS


def is_blocked(uid):
    with db() as c:
        row = c.execute(
            "SELECT blocked FROM users WHERE user_id=?",
            (uid,),
        ).fetchone()
        return bool(row and row["blocked"])


def active_subscription(uid):
    now = now_iso()

    with db() as c:
        c.execute(
            """
            UPDATE subscriptions
            SET status='expired'
            WHERE user_id=?
              AND status='active'
              AND end_at<=?
            """,
            (uid, now),
        )

        return c.execute(
            """
            SELECT *
            FROM subscriptions
            WHERE user_id=?
              AND status='active'
              AND end_at>?
            ORDER BY end_at DESC
            LIMIT 1
            """,
            (uid, now),
        ).fetchone()


def has_analysis_access(uid):
    return is_admin(uid) or active_subscription(uid) is not None


def add_subscription(uid, plan, payment_request_id=None, source="manual"):
    if plan not in PLANS:
        return False

    days, amount = PLANS[plan]

    old = active_subscription(uid)
    start = datetime.now(timezone.utc)

    if old:
        try:
            old_end = datetime.fromisoformat(old["end_at"])
            if old_end > start:
                start = old_end
        except Exception:
            pass

    end = start + timedelta(days=days)

    with db() as c:
        c.execute(
            """
            UPDATE subscriptions
            SET status='expired'
            WHERE user_id=?
              AND status='active'
              AND end_at<=?
            """,
            (uid, now_iso()),
        )

        c.execute(
            """
            INSERT INTO subscriptions(
                user_id,plan,days,amount,start_at,end_at,
                status,source,payment_request_id,created_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                uid,
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

    return True


def format_dt(value):
    try:
        return (
            datetime.fromisoformat(value)
            .astimezone()
            .strftime("%Y/%m/%d %H:%M")
        )
    except Exception:
        return str(value)


# ============================================================
# HTTP
# ============================================================

async def get_session():
    global HTTP_SESSION

    if HTTP_SESSION is None or HTTP_SESSION.closed:
        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
        HTTP_SESSION = aiohttp.ClientSession(
            timeout=timeout,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 "
                    "MarketAnalyzerBot/3.0"
                )
            },
        )

    return HTTP_SESSION


async def http_json(url, params=None, retries=3):
    key = (
        url,
        tuple(sorted((params or {}).items())),
    )

    cached = CACHE.get(key)
    if cached and time.time() - cached[0] < CACHE_SECONDS:
        return cached[1]

    session = await get_session()

    for attempt in range(retries):
        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json(content_type=None)
                    CACHE[key] = (time.time(), data)
                    return data

                log.warning(
                    "HTTP %s: %s",
                    response.status,
                    url,
                )

        except Exception as exc:
            log.warning("HTTP error: %s", exc)

        await asyncio.sleep(1 + attempt)

    return None


async def http_text(url, params=None):
    session = await get_session()

    try:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                return await response.text()

            log.warning(
                "HTTP text %s: %s",
                response.status,
                url,
            )

    except Exception as exc:
        log.warning("HTTP text error: %s", exc)

    return None


# ============================================================
# ASSETS
# ============================================================

COINS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "BNB": "binancecoin",
    "SOL": "solana",
    "XRP": "ripple",
    "DOGE": "dogecoin",
    "ADA": "cardano",
    "TRX": "tron",
    "AVAX": "avalanche-2",
    "DOT": "polkadot",
    "LINK": "chainlink",
    "MATIC": "matic-network",
    "POL": "polygon-ecosystem-token",
    "LTC": "litecoin",
    "BCH": "bitcoin-cash",
    "ATOM": "cosmos",
    "ETC": "ethereum-classic",
    "XLM": "stellar",
    "UNI": "uniswap",
    "NEAR": "near",
    "APT": "aptos",
    "ARB": "arbitrum",
    "OP": "optimism",
    "FIL": "filecoin",
    "ICP": "internet-computer",
    "HBAR": "hedera-hashgraph",
    "SUI": "sui",
    "PEPE": "pepe",
    "SHIB": "shiba-inu",
    "TON": "the-open-network",
    "ZEC": "zcash",
    "AAVE": "aave",
    "ALGO": "algorand",
    "VET": "vechain",
    "EOS": "eos",
    "XMR": "monero",
    "TAO": "bittensor",
    "INJ": "injective-protocol",
    "SEI": "sei-network",
    "RUNE": "thorchain",
    "MKR": "maker",
    "CRV": "curve-dao-token",
    "GRT": "the-graph",
    "LDO": "lido-staked-ether",
    "SAND": "the-sandbox",
    "MANA": "decentraland",
    "AXS": "axie-infinity",
    "FTM": "fantom",
    "KAS": "kaspa",
    "WIF": "dogwifcoin",
    "BONK": "bonk",
    "FLOKI": "floki",
}


def norm_symbol(value):
    value = (value or "").strip().upper()

    # Persian/Arabic digits -> Latin digits.
    value = value.translate(
        str.maketrans(
            "۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩",
            "01234567890123456789",
        )
    )

    value = (
        value.replace(" ", "")
        .replace("‌", "")
        .replace("_", "")
    )

    aliases = {
        "GOLD": "XAU",
        "GOLDUSD": "XAU",
        "XAUUSD": "XAU",
        "XAU/USD": "XAU",
        "XAU/USDT": "XAU",
        "XAUUSDT": "XAU",
        "طلا": "XAU",
        "طلایجهانی": "XAU",

        "GERAM18": "GOLD18",
        "GERAM18K": "GOLD18",
        "18K": "GOLD18",
        "GOLD18K": "GOLD18",
        "IRANGOLD": "GOLD18",
        "طلای18": "GOLD18",
        "طلای۱۸": "GOLD18",
        "طلای۱۸عیار": "GOLD18",
        "طلایداخلی": "GOLD18",
    }

    if value in aliases:
        return aliases[value]

    # ZECUSDT / ZEC-USDT / ZEC/USD -> ZEC
    for suffix in ("USDT", "USD"):
        if value.endswith(suffix) and len(value) > len(suffix):
            base = value[:-len(suffix)]
            base = base.replace("/", "").replace("-", "")
            if base in COINS:
                return base

    return value


def asset_type(symbol):
    symbol = norm_symbol(symbol)

    if symbol == "XAU":
        return "gold"

    if symbol == "GOLD18":
        return "gold18"

    return "crypto"


async def crypto_search(query):
    query = (query or "").strip()
    direct = norm_symbol(query)

    if direct in COINS:
        return [
            (
                direct,
                COINS[direct],
                direct,
            )
        ]

    data = await http_json(
        "https://api.coingecko.com/api/v3/search",
        {"query": query},
    )

    results = []
    seen = set()

    for coin in (data or {}).get("coins", [])[:15]:
        symbol = norm_symbol(
            coin.get("symbol") or ""
        )
        coin_id = coin.get("id")
        name = coin.get("name") or symbol

        if not symbol or not coin_id:
            continue

        key = (symbol, coin_id)
        if key in seen:
            continue

        seen.add(key)
        results.append(
            (
                symbol,
                coin_id,
                name,
            )
        )

    return results


async def crypto_data(symbol):
    symbol = norm_symbol(symbol)

    coin_id = COINS.get(symbol)

    if not coin_id:
        results = await crypto_search(symbol)
        if not results:
            return None

        symbol, coin_id, _ = results[0]

    data = await http_json(
        f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart",
        {
            "vs_currency": "usd",
            "days": "2",
            "interval": "hourly",
        },
    )

    if not data or not data.get("prices"):
        return None

    prices = pd.Series(
        [
            float(item[1])
            for item in data["prices"]
        ],
        dtype=float,
    )

    volumes = pd.Series(
        [
            float(item[1])
            for item in data.get("total_volumes", [])
        ],
        dtype=float,
    )

    return symbol, prices, volumes


async def xau_data():
    # Yahoo Finance fallback sources for global gold.
    for ticker in ("XAUUSD=X", "GC=F"):
        data = await http_json(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
            {
                "range": "5d",
                "interval": "1h",
            },
        )

        try:
            result = data["chart"]["result"][0]
            closes = (
                result["indicators"]["quote"][0]["close"]
            )

            values = [
                float(v)
                for v in closes
                if v is not None
            ]

            if len(values) > 10:
                return (
                    "XAU",
                    pd.Series(values, dtype=float),
                    pd.Series(dtype=float),
                )

        except Exception:
            continue

    return None


def extract_gold18_candidates(text):
    if not text:
        return []

    soup = BeautifulSoup(text, "html.parser")

    candidates = []

    # Prefer elements containing price-related labels.
    priority_words = (
        "طلای 18 عیار",
        "طلای ۱۸ عیار",
        "گرم طلای 18",
        "گرم طلای ۱۸",
        "geram18",
    )

    for tag in soup.find_all(
        string=re.compile(r"\d")
    ):
        raw = tag.strip()

        if not raw:
            continue

        clean = (
            raw.replace(",", "")
            .replace("٬", "")
            .replace(" ", "")
        )

        # Values expected for Iranian 18K gold price.
        for match in re.findall(
            r"\d{6,12}",
            clean,
        ):
            try:
                number = float(match)

                if 100_000 <= number <= 500_000_000:
                    score = 1

                    if any(
                        word.lower()
                        in raw.lower()
                        for word in priority_words
                    ):
                        score = 10

                    candidates.append(
                        (score, number)
                    )

            except ValueError:
                pass

    candidates.sort(
        key=lambda item: item[0],
        reverse=True,
    )

    return [
        number
        for _, number in candidates
    ]


async def gold18_data():
    html = await http_text(
        "https://www.tgju.org/profile/geram18"
    )

    if not html:
        return None

    candidates = extract_gold18_candidates(html)

    if not candidates:
        return None

    price = candidates[0]

    # TGJU is currently used as the requested domestic source.
    # A repeated series is used because this source provides a
    # current page value rather than a clean intraday candle series.
    return (
        "GOLD18",
        pd.Series(
            [price] * 30,
            dtype=float,
        ),
        pd.Series(dtype=float),
    )


async def asset_data(symbol):
    symbol = norm_symbol(symbol)

    if symbol == "XAU":
        return await xau_data()

    if symbol == "GOLD18":
        return await gold18_data()

    return await crypto_data(symbol)



async def current_price(symbol):
    """Fetch a current/free price without requiring a subscription."""
    symbol = norm_symbol(symbol)

    if symbol == "GOLD18":
        data = await gold18_data()
        if data:
            return {"symbol": "GOLD18", "price": float(data[1].iloc[-1]), "unit": "قیمت طلای ۱۸ عیار"}
        return None

    if symbol == "XAU":
        # Yahoo 5-minute quote for global gold.
        for ticker in ("XAUUSD=X", "GC=F"):
            data = await http_json(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
                {"range": "1d", "interval": "5m"},
            )
            try:
                result = data["chart"]["result"][0]
                meta = result.get("meta", {})
                value = meta.get("regularMarketPrice")
                if value is None:
                    vals = result["indicators"]["quote"][0]["close"]
                    vals = [float(v) for v in vals if v is not None]
                    value = vals[-1] if vals else None
                if value is not None:
                    return {"symbol": "XAU", "price": float(value), "unit": "دلار به ازای هر اونس"}
            except Exception:
                continue
        return None

    cid = COINS.get(symbol)
    if not cid:
        results = await crypto_search(symbol)
        if not results:
            return None
        symbol, cid, _ = results[0]

    data = await http_json(
        "https://api.coingecko.com/api/v3/simple/price",
        {"ids": cid, "vs_currencies": "usd", "include_24hr_change": "true"},
    )
    try:
        item = data[cid]
        return {
            "symbol": symbol,
            "price": float(item["usd"]),
            "change24": float(item.get("usd_24h_change") or 0),
            "unit": "دلار",
        }
    except Exception:
        return None


def format_live_price(item):
    if not item:
        return "❌ قیمت در حال حاضر در دسترس نیست."
    symbol = escape(item["symbol"])
    price = item["price"]
    if item["symbol"] in ("XAU", "GOLD18"):
        value = f"{price:,.0f}" if price >= 1000 else f"{price:,.2f}"
    else:
        value = f"{price:,.8f}".rstrip("0").rstrip(".")
    text = f"💰 <b>{symbol}</b>\nقیمت فعلی: <b>{value}</b>"
    if item.get("unit"):
        text += f"\nواحد: {escape(item['unit'])}"
    if "change24" in item:
        text += f"\nتغییر ۲۴ ساعت: <b>{item['change24']:+.2f}%</b>"
    return text


async def live_price_menu(update, context):
    uid = update.effective_user.id
    rows = user_assets(uid)
    if not rows:
        await update.message.reply_text(
            "📋 واچ‌لیست شما خالی است.\n"
            "ابتدا از «➕ افزودن دارایی» دارایی اضافه کنید.\n\n"
            "💰 نمایش قیمت لحظه‌ای رایگان است و به اشتراک نیاز ندارد."
        )
        return

    buttons = []
    for row in rows:
        buttons.append([
            InlineKeyboardButton(
                f"💰 {row['symbol']}",
                callback_data=f"price:{row['asset_type']}:{row['symbol']}",
            )
        ])
    await update.message.reply_text(
        "💰 <b>قیمت لحظه‌ای</b>\n\n"
        "یک دارایی از واچ‌لیست را انتخاب کنید.\n"
        "نمایش قیمت رایگان است و نیازی به اشتراک ندارد.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def selected_price_callback(update, context):
    q = update.callback_query
    await q.answer("در حال دریافت قیمت...")
    try:
        _, atype, symbol = q.data.split(":", 2)
        symbol = norm_symbol(symbol)
        if not user_has_asset(q.from_user.id, symbol, atype):
            await q.message.reply_text("❌ این دارایی در واچ‌لیست شما نیست.")
            return
        item = await current_price(symbol)
        await q.message.reply_text(format_live_price(item), parse_mode=ParseMode.HTML)
    except Exception:
        log.exception("live price callback error")
        await q.message.reply_text("⚠️ دریافت قیمت انجام نشد. دوباره تلاش کنید.")


def watchlist_selector_keyboard(rows, prefix):
    buttons = []
    for row in rows:
        label = row["symbol"]
        if row["asset_type"] == "gold":
            label += " — طلای جهانی"
        elif row["asset_type"] == "gold18":
            label += " — طلای ۱۸ عیار"
        buttons.append([
            InlineKeyboardButton(
                f"{label}",
                callback_data=f"{prefix}:{row['asset_type']}:{row['symbol']}",
            )
        ])
    return InlineKeyboardMarkup(buttons)


async def analysis_selector_callback(update, context):
    q = update.callback_query
    await q.answer("در حال تحلیل...")
    uid = q.from_user.id
    if not has_analysis_access(uid):
        await q.message.reply_text("🔒 تحلیل فقط برای مشترکین فعال است. از «💳 خرید اشتراک» استفاده کنید.")
        return
    try:
        _, atype, symbol = q.data.split(":", 2)
        symbol = norm_symbol(symbol)
        if not user_has_asset(uid, symbol, atype):
            await q.message.reply_text("❌ این دارایی در واچ‌لیست شما نیست.")
            return
        analysis = await analyze(symbol)
        await q.message.reply_text(analysis_text(analysis), parse_mode=ParseMode.HTML)
    except Exception:
        log.exception("analysis selector error")
        await q.message.reply_text("⚠️ تحلیل در حال حاضر در دسترس نیست.")


async def signal_selector_callback(update, context):
    q = update.callback_query
    await q.answer("در حال بررسی سیگنال...")
    uid = q.from_user.id
    if not has_analysis_access(uid):
        await q.message.reply_text("🔒 سیگنال‌ها فقط برای مشترکین فعال است. از «💳 خرید اشتراک» استفاده کنید.")
        return
    try:
        _, atype, symbol = q.data.split(":", 2)
        symbol = norm_symbol(symbol)
        if not user_has_asset(uid, symbol, atype):
            await q.message.reply_text("❌ این دارایی در واچ‌لیست شما نیست.")
            return
        analysis = await analyze(symbol)
        await q.message.reply_text(analysis_text(analysis), parse_mode=ParseMode.HTML)
    except Exception:
        log.exception("signal selector error")
        await q.message.reply_text("⚠️ سیگنال در حال حاضر در دسترس نیست.")

# ============================================================
# ANALYSIS
# ============================================================

def analysis_from_series(symbol, prices, volumes=None):
    prices = pd.Series(
        prices,
        dtype=float,
    ).dropna()

    if len(prices) < 5:
        return None

    ema9 = (
        prices.ewm(
            span=9,
            adjust=False,
        )
        .mean()
        .iloc[-1]
    )

    ema21 = (
        prices.ewm(
            span=21,
            adjust=False,
        )
        .mean()
        .iloc[-1]
    )

    delta = prices.diff()

    gain = (
        delta.clip(lower=0)
        .rolling(14)
        .mean()
    )

    loss = (
        -delta.clip(upper=0)
    ).rolling(14).mean()

    rs = gain / loss.replace(0, pd.NA)

    try:
        rsi = float(
            (
                100 - (100 / (1 + rs))
            ).iloc[-1]
        )
    except Exception:
        rsi = 50.0

    if pd.isna(rsi):
        rsi = 50.0

    r1 = 0.0
    r6 = 0.0
    r24 = 0.0

    if len(prices) >= 2 and prices.iloc[-2] != 0:
        r1 = (
            prices.iloc[-1]
            / prices.iloc[-2]
            - 1
        ) * 100

    if len(prices) >= 7 and prices.iloc[-7] != 0:
        r6 = (
            prices.iloc[-1]
            / prices.iloc[-7]
            - 1
        ) * 100

    if len(prices) >= 25 and prices.iloc[-25] != 0:
        r24 = (
            prices.iloc[-1]
            / prices.iloc[-25]
            - 1
        ) * 100
    else:
        r24 = r6

    score = 0

    score += 2 if ema9 > ema21 else -2
    score += (
        2
        if rsi > 52
        else -2
        if rsi < 48
        else 0
    )
    score += (
        1
        if r1 > 0
        else -1
        if r1 < 0
        else 0
    )
    score += (
        1
        if r6 > 0
        else -1
        if r6 < 0
        else 0
    )

    score = max(-6, min(6, score))

    signal = (
        "BUY"
        if score >= 3
        else "SELL"
        if score <= -3
        else "WAIT"
    )

    strength = min(
        99,
        50 + abs(score) * 8,
    )

    probability = min(
        95,
        max(
            5,
            50 + score * 7,
        ),
    )

    return {
        "symbol": symbol,
        "price": float(prices.iloc[-1]),
        "ema9": float(ema9),
        "ema21": float(ema21),
        "rsi": float(rsi),
        "r1": float(r1),
        "r6": float(r6),
        "r24": float(r24),
        "score": int(score),
        "signal": signal,
        "strength": float(strength),
        "probability": float(probability),
    }


async def analyze(symbol):
    data = await asset_data(symbol)

    if not data:
        return None

    return analysis_from_series(
        data[0],
        data[1],
        data[2],
    )


def signal_fa(signal):
    return {
        "BUY": "🟢 خرید",
        "SELL": "🔴 فروش",
        "WAIT": "🟡 انتظار",
    }.get(signal, signal)


def analysis_text(analysis):
    if not analysis:
        return "❌ اطلاعات بازار در دسترس نیست."

    return (
        f"📊 <b>تحلیل {escape(analysis['symbol'])}</b>\n\n"
        f"💰 قیمت: <b>{analysis['price']:,.4f}</b>\n"
        f"📈 EMA9: {analysis['ema9']:,.4f}\n"
        f"📉 EMA21: {analysis['ema21']:,.4f}\n"
        f"RSI14: <b>{analysis['rsi']:.1f}</b>\n"
        f"بازده کوتاه‌مدت: {analysis['r1']:+.2f}%\n"
        f"بازده ۶ دوره: {analysis['r6']:+.2f}%\n"
        f"بازده ۲۴ دوره: {analysis['r24']:+.2f}%\n\n"
        f"🎯 سیگنال: <b>{signal_fa(analysis['signal'])}</b>\n"
        f"💪 درصد قدرت: <b>{analysis['strength']:.0f}%</b>\n"
        f"🎲 احتمال سود: <b>{analysis['probability']:.0f}%</b>\n\n"
        "⚠️ این تحلیل آموزشی است و تضمین سود نیست."
    )


# ============================================================
# WATCHLIST
# ============================================================

def add_watch(uid, symbol, atype):
    symbol = norm_symbol(symbol)

    with db() as c:
        count = c.execute(
            "SELECT COUNT(*) n FROM watchlist WHERE user_id=?",
            (uid,),
        ).fetchone()["n"]

        if count >= MAX_WATCHLIST:
            # If already present, keep it. Otherwise reject.
            exists = c.execute(
                """
                SELECT 1
                FROM watchlist
                WHERE user_id=? AND symbol=? AND asset_type=?
                """,
                (uid, symbol, atype),
            ).fetchone()

            if not exists:
                return False

        c.execute(
            """
            INSERT OR IGNORE INTO watchlist(
                user_id,symbol,asset_type,created_at
            )
            VALUES(?,?,?,?)
            """,
            (
                uid,
                symbol,
                atype,
                now_iso(),
            ),
        )

    return True


def remove_watch(uid, symbol, atype):
    with db() as c:
        c.execute(
            """
            DELETE FROM watchlist
            WHERE user_id=? AND symbol=? AND asset_type=?
            """,
            (
                uid,
                norm_symbol(symbol),
                atype,
            ),
        )


def user_assets(uid, atype=None):
    with db() as c:
        if atype:
            return c.execute(
                """
                SELECT *
                FROM watchlist
                WHERE user_id=? AND asset_type=?
                ORDER BY created_at
                """,
                (uid, atype),
            ).fetchall()

        return c.execute(
            """
            SELECT *
            FROM watchlist
            WHERE user_id=?
            ORDER BY created_at
            """,
            (uid,),
        ).fetchall()


def user_has_asset(uid, symbol, atype="crypto"):
    with db() as c:
        return (
            c.execute(
                """
                SELECT 1
                FROM watchlist
                WHERE user_id=?
                  AND symbol=?
                  AND asset_type=?
                """,
                (
                    uid,
                    norm_symbol(symbol),
                    atype,
                ),
            ).fetchone()
            is not None
        )


# ============================================================
# KEYBOARDS
# ============================================================

def main_kb(uid):
    rows = [row[:] for row in MAIN_MENU]

    if not is_admin(uid):
        rows = [
            row
            for row in rows
            if row != ["👨‍💼 پنل مدیریت"]
        ]

    return ReplyKeyboardMarkup(
        rows,
        resize_keyboard=True,
    )


def admin_kb():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📊 آمار",
                    callback_data="adm:stats",
                ),
                InlineKeyboardButton(
                    "👥 کاربران",
                    callback_data="adm:users:0",
                ),
            ],
            [
                InlineKeyboardButton(
                    "📢 پیام به مشترکین",
                    callback_data="adm:broadcast",
                ),
                InlineKeyboardButton(
                    "📨 پشتیبانی",
                    callback_data="adm:support",
                ),
            ],
            [
                InlineKeyboardButton(
                    "💬 مدیریت چت",
                    callback_data="adm:chat",
                ),
                InlineKeyboardButton(
                    "🚨 گزارش‌ها",
                    callback_data="adm:reports",
                ),
            ],
            [
                InlineKeyboardButton(
                    "💳 پرداخت‌های در انتظار",
                    callback_data="adm:payments",
                ),
            ],
            [
                InlineKeyboardButton(
                    "✉️ پیام به کاربر",
                    callback_data="adm:message",
                ),
                InlineKeyboardButton(
                    "🚫 مسدود/رفع",
                    callback_data="adm:block",
                ),
            ],
        ]
    )


# ============================================================
# BASIC COMMANDS
# ============================================================

async def start(update, context):
    ensure_user(update.effective_user)

    if is_blocked(update.effective_user.id):
        await update.message.reply_text(
            "🚫 دسترسی شما توسط مدیر محدود شده است."
        )
        return

    await update.message.reply_text(
        "سلام 👋\n\n"
        "به ربات تحلیل بازار خوش آمدید.\n"
        "رمزارزها، طلای جهانی و طلای ۱۸ عیار را "
        "می‌توانید جداگانه به واچ‌لیست اضافه کنید.\n\n"
        "از منوی زیر استفاده کنید.",
        reply_markup=main_kb(
            update.effective_user.id
        ),
    )


async def help_text(update, context):
    await update.message.reply_text(
        "ℹ️ راهنما\n\n"
        "➕ افزودن دارایی: افزودن رمز‌ارز، XAU یا GOLD18\n"
        "📋 واچ‌لیست: مشاهده و حذف دارایی‌ها\n"
        "💰 قیمت لحظه‌ای: رایگان و بدون نیاز به اشتراک\n"
        "📊 تحلیل و 🚨 سیگنال‌ها: نیازمند اشتراک فعال؛ انتخاب از واچ‌لیست\n"
        "🔔 هشدارها: اعلان سیگنال جدید برای دارایی‌های واچ‌لیست\n"
        "💬 چت رمز ارز: گفت‌وگوی مشترکین همان رمز‌ارز\n"
        "📨 پشتیبان: ارسال پیام مستقیم به مدیر\n"
        "💳 خرید اشتراک: پرداخت دستی و ارسال رسید\n\n"
        "⚠️ ربات معامله خودکار انجام نمی‌دهد."
    )


# ============================================================
# ADD ASSET
# ============================================================

async def add_asset_prompt(update, context):
    # Clear conflicting temporary states.
    for key in (
        "support_mode",
        "chat_room",
        "payment_plan",
        "admin_reply_to",
        "admin_mode",
        "message_target",
    ):
        context.user_data.pop(key, None)

    context.user_data["awaiting_asset"] = "add"

    await update.message.reply_text(
        "➕ <b>افزودن دارایی</b>\n\n"
        "نام یا نماد دارایی را بفرستید.\n\n"
        "مثال:\n"
        "• BTC\n"
        "• ZEC\n"
        "• ETH\n"
        "• XAU\n"
        "• GOLD18\n"
        "• طلای ۱۸ عیار\n\n"
        "برای لغو /cancel",
        parse_mode=ParseMode.HTML,
    )


async def more_coins(update, context):
    context.user_data["awaiting_asset"] = "add"

    await update.message.reply_text(
        "🪙 نام یا نماد رمز ارز را ارسال کنید.\n\n"
        "مثال: ZEC، BTC، SOL، DOGE\n\n"
        "ربات ابتدا فهرست داخلی و سپس CoinGecko "
        "را جست‌وجو می‌کند."
    )


async def process_add_asset(update, context, text):
    uid = update.effective_user.id
    text = (text or "").strip()

    if not text:
        await update.message.reply_text(
            "❌ نماد دارایی خالی است."
        )
        return

    try:
        symbol = norm_symbol(text)

        # XAU
        if symbol == "XAU":
            if not add_watch(uid, "XAU", "gold"):
                await update.message.reply_text(
                    "⚠️ سقف واچ‌لیست شما پر شده است."
                )
                return

            await update.message.reply_text(
                "✅ <b>طلای جهانی XAU</b> "
                "به واچ‌لیست اضافه شد.",
                parse_mode=ParseMode.HTML,
            )
            return

        # GOLD18
        if symbol == "GOLD18":
            if not add_watch(uid, "GOLD18", "gold18"):
                await update.message.reply_text(
                    "⚠️ سقف واچ‌لیست شما پر شده است."
                )
                return

            await update.message.reply_text(
                "✅ <b>طلای ۱۸ عیار ایران</b> "
                "به واچ‌لیست اضافه شد.",
                parse_mode=ParseMode.HTML,
            )
            return

        results = await crypto_search(text)

        if not results:
            await update.message.reply_text(
                f"❌ دارایی <b>{escape(text)}</b> پیدا نشد.\n\n"
                "نماد را دقیق‌تر وارد کنید؛ مثال ZEC یا BTC.",
                parse_mode=ParseMode.HTML,
            )
            return

        if len(results) == 1:
            sym, _, name = results[0]
            sym = norm_symbol(sym)

            if not add_watch(
                uid,
                sym,
                asset_type(sym),
            ):
                await update.message.reply_text(
                    "⚠️ سقف واچ‌لیست شما پر شده است."
                )
                return

            await update.message.reply_text(
                f"✅ <b>{escape(sym)}</b> "
                "به واچ‌لیست اضافه شد.\n\n"
                f"نام: {escape(name)}",
                parse_mode=ParseMode.HTML,
            )
            return

        # Multiple results.
        buttons = []

        for sym, coin_id, name in results[:10]:
            safe_sym = norm_symbol(sym)

            buttons.append(
                [
                    InlineKeyboardButton(
                        f"{safe_sym} — {name}",
                        callback_data=f"pick:{safe_sym}",
                    )
                ]
            )

        await update.message.reply_text(
            "🔎 چند دارایی پیدا شد.\n"
            "دارایی موردنظر را انتخاب کنید:",
            reply_markup=InlineKeyboardMarkup(
                buttons
            ),
        )

    except Exception:
        log.exception("process_add_asset error")

        await update.message.reply_text(
            "⚠️ هنگام افزودن دارایی خطایی رخ داد.\n"
            "لطفاً دوباره تلاش کنید."
        )


# ============================================================
# WATCHLIST / ANALYSIS
# ============================================================

async def watchlist_menu(update, context):
    rows = user_assets(
        update.effective_user.id
    )

    if not rows:
        await update.message.reply_text(
            "📋 واچ‌لیست خالی است.\n"
            "از «➕ افزودن دارایی» استفاده کنید."
        )
        return

    lines = []

    for row in rows:
        label = row["symbol"]

        if row["asset_type"] == "gold":
            label += " — طلای جهانی"

        elif row["asset_type"] == "gold18":
            label += " — طلای ۱۸ عیار"

        else:
            label += " — رمز ارز"

        lines.append(f"• {label}")

    buttons = []

    for row in rows:
        buttons.append(
            [
                InlineKeyboardButton(
                    f"❌ حذف {row['symbol']}",
                    callback_data=(
                        f"wl:del:"
                        f"{row['asset_type']}:"
                        f"{row['symbol']}"
                    ),
                )
            ]
        )

    await update.message.reply_text(
        "📋 <b>واچ‌لیست شما</b>\n\n"
        + "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            buttons
        ),
    )


async def analysis_prompt(update, context):
    uid = update.effective_user.id
    if not has_analysis_access(uid):
        await update.message.reply_text(
            "🔒 تحلیل فقط برای مشترکین فعال است.\n"
            "از «💳 خرید اشتراک» استفاده کنید."
        )
        return

    rows = user_assets(uid)
    if not rows:
        await update.message.reply_text(
            "📋 واچ‌لیست شما خالی است.\n"
            "ابتدا از «➕ افزودن دارایی» یک رمز ارز یا طلا اضافه کنید."
        )
        return

    await update.message.reply_text(
        "📊 <b>انتخاب دارایی برای تحلیل</b>\n\n"
        "از واچ‌لیست خودتان یک دارایی را انتخاب کنید:",
        parse_mode=ParseMode.HTML,
        reply_markup=watchlist_selector_keyboard(rows, "analysis"),
    )


async def signals_menu(update, context):
    uid = update.effective_user.id
    if not has_analysis_access(uid):
        await update.message.reply_text(
            "🔒 سیگنال‌ها فقط برای مشترکین فعال است.\n"
            "از «💳 خرید اشتراک» استفاده کنید."
        )
        return

    rows = user_assets(uid)
    if not rows:
        await update.message.reply_text(
            "📋 واچ‌لیست شما خالی است.\n"
            "ابتدا یک دارایی اضافه کنید."
        )
        return

    await update.message.reply_text(
        "🚨 <b>انتخاب دارایی برای سیگنال</b>\n\n"
        "فقط همان رمز ارز/دارایی که انتخاب می‌کنید بررسی می‌شود:",
        parse_mode=ParseMode.HTML,
        reply_markup=watchlist_selector_keyboard(rows, "signal"),
    )


# ============================================================
# SUBSCRIPTIONS
# ============================================================

async def buy_menu(update, context):
    buttons = []

    for plan, (days, amount) in PLANS.items():
        buttons.append(
            [
                InlineKeyboardButton(
                    f"{days} روز — {amount:,} تومان",
                    callback_data=f"plan:{plan}",
                )
            ]
        )

    await update.message.reply_text(
        "💳 یکی از پلن‌ها را انتخاب کنید:",
        reply_markup=InlineKeyboardMarkup(
            buttons
        ),
    )


async def status_menu(update, context):
    subscription = active_subscription(
        update.effective_user.id
    )

    if not subscription:
        await update.message.reply_text(
            "👤 اشتراک فعال ندارید."
        )
        return

    await update.message.reply_text(
        "👤 <b>وضعیت اشتراک</b>\n\n"
        f"پلن: {subscription['days']} روز\n"
        f"شروع: {format_dt(subscription['start_at'])}\n"
        f"پایان: {format_dt(subscription['end_at'])}\n"
        "وضعیت: 🟢 فعال",
        parse_mode=ParseMode.HTML,
    )


async def plan_callback(update, context):
    query = update.callback_query
    await query.answer()

    plan = query.data.split(":", 1)[1]

    if plan not in PLANS:
        await query.message.reply_text(
            "❌ پلن نامعتبر است."
        )
        return

    days, amount = PLANS[plan]

    context.user_data["payment_plan"] = plan

    await query.message.reply_text(
        f"💳 <b>پلن {days} روزه</b>\n\n"
        f"مبلغ: <b>{amount:,} تومان</b>\n\n"
        f"شماره کارت پرداخت:\n"
        f"<code>{escape(PAYMENT_CARD)}</code>\n\n"
        "پس از پرداخت، تصویر رسید را همین‌جا "
        "ارسال کنید.",
        parse_mode=ParseMode.HTML,
    )


async def receipt_photo(update, context):
    if context.user_data.get("support_mode"):
        await support_media(update, context)
        return

    plan = context.user_data.get("payment_plan")

    if not plan or plan not in PLANS:
        await update.message.reply_text(
            "ابتدا از «💳 خرید اشتراک» یک پلن انتخاب کنید."
        )
        return

    days, amount = PLANS[plan]

    file_id = update.message.photo[-1].file_id

    with db() as c:
        cursor = c.execute(
            """
            INSERT INTO payment_requests(
                user_id,plan,days,amount,
                receipt_file_id,status,created_at
            )
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                update.effective_user.id,
                plan,
                days,
                amount,
                file_id,
                "pending",
                now_iso(),
            ),
        )

        payment_id = cursor.lastrowid

    context.user_data.pop(
        "payment_plan",
        None,
    )

    await update.message.reply_text(
        "✅ رسید دریافت شد.\n"
        "پس از بررسی مدیر، اشتراک فعال می‌شود."
    )

    for admin_id in ADMIN_IDS:
        try:
            keyboard = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "✅ تایید",
                            callback_data=(
                                f"pay:approve:{payment_id}"
                            ),
                        ),
                        InlineKeyboardButton(
                            "❌ رد",
                            callback_data=(
                                f"pay:reject:{payment_id}"
                            ),
                        ),
                    ]
                ]
            )

            await context.bot.send_photo(
                admin_id,
                file_id,
                caption=(
                    "💳 رسید جدید\n"
                    f"کاربر: {update.effective_user.id}\n"
                    f"پلن: {days} روز\n"
                    f"مبلغ: {amount:,} تومان\n"
                    f"شناسه: {payment_id}"
                ),
                reply_markup=keyboard,
            )

        except Exception:
            log.exception(
                "payment notification error"
            )


# ============================================================
# SUPPORT
# ============================================================

async def support_prompt(update, context):
    context.user_data["support_mode"] = True

    await update.message.reply_text(
        "📨 پیام خود را برای پشتیبان بفرستید.\n"
        "متن، عکس، فایل یا صدا قابل ارسال است.\n\n"
        "برای خروج /cancel"
    )


async def save_support(
    uid,
    message,
    telegram_message_id,
):
    with db() as c:
        c.execute(
            """
            INSERT INTO support_messages(
                user_id,direction,message,
                telegram_message_id,created_at
            )
            VALUES(?,?,?,?,?)
            """,
            (
                uid,
                "user_to_admin",
                message,
                telegram_message_id,
                now_iso(),
            ),
        )


async def support_media(update, context):
    uid = update.effective_user.id

    if update.message.photo:
        saved_message = "[عکس]"

        for admin_id in ADMIN_IDS:
            try:
                keyboard = InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "↩️ پاسخ",
                                callback_data=(
                                    f"sup:reply:{uid}"
                                ),
                            )
                        ]
                    ]
                )

                await context.bot.send_photo(
                    admin_id,
                    update.message.photo[-1].file_id,
                    caption=(
                        "📨 پیام پشتیبانی\n"
                        f"کاربر: {uid}"
                    ),
                    reply_markup=keyboard,
                )

            except Exception:
                log.exception(
                    "support photo error"
                )

    elif update.message.document:
        saved_message = "[فایل]"

        for admin_id in ADMIN_IDS:
            try:
                keyboard = InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "↩️ پاسخ",
                                callback_data=(
                                    f"sup:reply:{uid}"
                                ),
                            )
                        ]
                    ]
                )

                await context.bot.send_document(
                    admin_id,
                    update.message.document.file_id,
                    caption=f"📨 فایل از کاربر {uid}",
                    reply_markup=keyboard,
                )

            except Exception:
                log.exception(
                    "support document error"
                )

    elif update.message.voice:
        saved_message = "[صدا]"

        for admin_id in ADMIN_IDS:
            try:
                keyboard = InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "↩️ پاسخ",
                                callback_data=(
                                    f"sup:reply:{uid}"
                                ),
                            )
                        ]
                    ]
                )

                await context.bot.send_voice(
                    admin_id,
                    update.message.voice.file_id,
                    caption=f"📨 صدا از کاربر {uid}",
                    reply_markup=keyboard,
                )

            except Exception:
                log.exception(
                    "support voice error"
                )

    else:
        return

    await save_support(
        uid,
        saved_message,
        update.message.message_id,
    )

    await update.message.reply_text(
        "✅ پیام شما برای پشتیبان ارسال شد."
    )


async def support_reply_callback(update, context):
    query = update.callback_query
    await query.answer()

    if not is_admin(query.from_user.id):
        return

    uid = int(query.data.split(":")[-1])

    context.user_data["admin_reply_to"] = uid

    await query.message.reply_text(
        f"✍️ پاسخ خود به کاربر {uid} را ارسال کنید."
    )


async def send_support_reply(
    update,
    context,
    text,
):
    uid = context.user_data.pop(
        "admin_reply_to",
        None,
    )

    if not uid:
        return

    try:
        await context.bot.send_message(
            uid,
            f"📨 <b>پاسخ پشتیبان</b>\n\n{escape(text)}",
            parse_mode=ParseMode.HTML,
        )

        with db() as c:
            c.execute(
                """
                INSERT INTO support_messages(
                    user_id,admin_id,direction,
                    message,status,created_at,replied_at
                )
                VALUES(?,?,?,?,?,?,?)
                """,
                (
                    uid,
                    update.effective_user.id,
                    "admin_to_user",
                    text,
                    "closed",
                    now_iso(),
                    now_iso(),
                ),
            )

        await update.message.reply_text(
            "✅ پاسخ ارسال شد."
        )

    except Exception as exc:
        await update.message.reply_text(
            f"❌ ارسال نشد: {escape(str(exc))}",
            parse_mode=ParseMode.HTML,
        )


# ============================================================
# COMMUNITY CRYPTO CHAT
# ============================================================

def chat_name(uid):
    with db() as c:
        row = c.execute(
            """
            SELECT first_name,username
            FROM users
            WHERE user_id=?
            """,
            (uid,),
        ).fetchone()

    if not row:
        return "کاربر"

    if row["first_name"]:
        return row["first_name"]

    if row["username"]:
        return "@" + row["username"]

    return "کاربر"


def chat_members(symbol):
    with db() as c:
        return c.execute(
            """
            SELECT DISTINCT
                u.user_id,
                u.first_name,
                u.username
            FROM users u
            JOIN watchlist w
                ON w.user_id=u.user_id
            JOIN subscriptions s
                ON s.user_id=u.user_id
            WHERE u.blocked=0
              AND w.asset_type='crypto'
              AND w.symbol=?
              AND s.status='active'
              AND s.end_at>?
            """,
            (
                norm_symbol(symbol),
                now_iso(),
            ),
        ).fetchall()


async def crypto_chat_menu(update, context):
    uid = update.effective_user.id

    if not has_analysis_access(uid):
        await update.message.reply_text(
            "🔒 چت رمز ارز فقط برای مشترکین فعال است."
        )
        return

    rows = user_assets(uid, "crypto")

    if not rows:
        await update.message.reply_text(
            "ابتدا حداقل یک رمز‌ارز "
            "به واچ‌لیست اضافه کنید."
        )
        return

    buttons = [
        [
            InlineKeyboardButton(
                f"💬 {row['symbol']}",
                callback_data=(
                    f"chat:open:{row['symbol']}"
                ),
            )
        ]
        for row in rows
    ]

    await update.message.reply_text(
        "رمز‌ارزی را انتخاب کنید تا وارد "
        "اتاق گفت‌وگوی مشترک آن شوید:",
        reply_markup=InlineKeyboardMarkup(
            buttons
        ),
    )


async def chat_open_callback(update, context):
    query = update.callback_query
    await query.answer()

    uid = query.from_user.id

    sym = norm_symbol(
        query.data.split(":", 2)[2]
    )

    if (
        not has_analysis_access(uid)
        or not user_has_asset(uid, sym, "crypto")
    ):
        await query.message.reply_text(
            "🔒 شما مجاز به ورود به این اتاق نیستید."
        )
        return

    context.user_data["chat_room"] = sym

    members = chat_members(sym)

    await query.message.reply_text(
        f"💬 <b>اتاق {escape(sym)}</b>\n\n"
        f"👥 اعضای فعال: {len(members)} نفر\n"
        "پیام شما برای مشترکین همین رمز‌ارز ارسال می‌شود.\n\n"
        "برای خروج /cancel",
        parse_mode=ParseMode.HTML,
    )


async def process_chat_message(update, context):
    room = context.user_data.get("chat_room")

    if (
        not room
        or not update.message
        or not update.message.text
    ):
        return False

    uid = update.effective_user.id

    if (
        not has_analysis_access(uid)
        or not user_has_asset(uid, room, "crypto")
    ):
        context.user_data.pop(
            "chat_room",
            None,
        )
        return False

    text = update.message.text.strip()

    if not text:
        return True

    if len(text) > 1500:
        await update.message.reply_text(
            "❌ حداکثر طول پیام ۱۵۰۰ کاراکتر است."
        )
        return True

    with db() as c:
        cursor = c.execute(
            """
            INSERT INTO chat_messages(
                user_id,asset_type,symbol,
                message,telegram_message_id,created_at
            )
            VALUES(?,?,?,?,?,?)
            """,
            (
                uid,
                "crypto",
                room,
                text,
                update.message.message_id,
                now_iso(),
            ),
        )

        message_id = cursor.lastrowid

    sender = escape(chat_name(uid))
    members = chat_members(room)

    for member in members:
        if member["user_id"] == uid:
            continue

        try:
            await context.bot.send_message(
                member["user_id"],
                (
                    f"💬 <b>{sender}</b> "
                    f"در اتاق {escape(room)}:\n\n"
                    f"{escape(text)}"
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "🚨 گزارش",
                                callback_data=(
                                    f"chat:report:{message_id}"
                                ),
                            )
                        ]
                    ]
                ),
            )

        except Exception:
            log.debug(
                "chat relay failed for %s",
                member["user_id"],
            )

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                admin_id,
                (
                    f"👁 <b>چت {escape(room)}</b>\n"
                    f"کاربر: <code>{uid}</code> "
                    f"({sender})\n"
                    f"پیام #{message_id}:\n"
                    f"{escape(text)}"
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "🗑 حذف",
                                callback_data=(
                                    f"chat:delete:{message_id}"
                                ),
                            ),
                            InlineKeyboardButton(
                                "🚫 مسدود",
                                callback_data=(
                                    f"chat:block:{uid}"
                                ),
                            ),
                        ]
                    ]
                ),
            )

        except Exception:
            log.exception(
                "admin chat notification error"
            )

    return True


async def chat_report_callback(update, context):
    query = update.callback_query
    await query.answer("گزارش ثبت شد.")

    message_id = int(
        query.data.split(":")[-1]
    )

    reporter_id = query.from_user.id

    with db() as c:
        exists = c.execute(
            """
            SELECT 1
            FROM chat_reports
            WHERE message_id=?
              AND reporter_id=?
              AND status='pending'
            """,
            (
                message_id,
                reporter_id,
            ),
        ).fetchone()

        if not exists:
            c.execute(
                """
                INSERT INTO chat_reports(
                    message_id,reporter_id,
                    reason,status,created_at
                )
                VALUES(?,?,?,?,?)
                """,
                (
                    message_id,
                    reporter_id,
                    "گزارش کاربر",
                    "pending",
                    now_iso(),
                ),
            )

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                admin_id,
                (
                    "🚨 گزارش جدید برای پیام "
                    f"چت #{message_id}\n"
                    f"گزارش‌دهنده: {reporter_id}"
                ),
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "🗑 حذف پیام",
                                callback_data=(
                                    f"chat:delete:{message_id}"
                                ),
                            )
                        ]
                    ]
                ),
            )

        except Exception:
            pass


async def chat_admin_callback(update, context):
    query = update.callback_query
    await query.answer()

    if not is_admin(query.from_user.id):
        return

    parts = query.data.split(":")
    action = parts[1]
    target = int(parts[2])

    if action == "delete":
        with db() as c:
            row = c.execute(
                """
                SELECT *
                FROM chat_messages
                WHERE id=?
                """,
                (target,),
            ).fetchone()

            c.execute(
                """
                UPDATE chat_messages
                SET deleted=1,
                    deleted_by=?,
                    deleted_at=?
                WHERE id=?
                """,
                (
                    query.from_user.id,
                    now_iso(),
                    target,
                ),
            )

            c.execute(
                """
                UPDATE chat_reports
                SET status='reviewed',
                    reviewed_by=?,
                    reviewed_at=?
                WHERE message_id=?
                """,
                (
                    query.from_user.id,
                    now_iso(),
                    target,
                ),
            )

        if row:
            members = chat_members(
                row["symbol"]
            )

            for member in members:
                try:
                    await context.bot.send_message(
                        member["user_id"],
                        f"🗑 پیام #{target} توسط مدیر حذف شد.",
                    )
                except Exception:
                    pass

        await query.message.reply_text(
            "✅ پیام حذف شد."
        )

    elif action == "block":
        with db() as c:
            c.execute(
                """
                UPDATE users
                SET blocked=1
                WHERE user_id=?
                """,
                (target,),
            )

        await query.message.reply_text(
            f"🚫 کاربر {target} مسدود شد."
        )


# ============================================================
# ALERTS
# ============================================================

async def alerts_menu(update, context):
    uid = update.effective_user.id

    with db() as c:
        row = c.execute(
            """
            SELECT *
            FROM alert_preferences
            WHERE user_id=?
            """,
            (uid,),
        ).fetchone()

    enabled = (
        bool(row["enabled"])
        if row
        else True
    )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🔕 خاموش"
                    if enabled
                    else "🔔 روشن",
                    callback_data="alert:toggle",
                )
            ]
        ]
    )

    await update.message.reply_text(
        "🔔 هشدار سیگنال: "
        + ("فعال" if enabled else "خاموش"),
        reply_markup=keyboard,
    )


async def alert_callback(update, context):
    query = update.callback_query
    await query.answer()

    uid = query.from_user.id

    with db() as c:
        row = c.execute(
            """
            SELECT enabled
            FROM alert_preferences
            WHERE user_id=?
            """,
            (uid,),
        ).fetchone()

        new_value = (
            0
            if row and row["enabled"]
            else 1
        )

        c.execute(
            """
            INSERT INTO alert_preferences(
                user_id,enabled,interval_seconds
            )
            VALUES(?,?,?)
            ON CONFLICT(user_id)
            DO UPDATE SET
                enabled=excluded.enabled
            """,
            (
                uid,
                new_value,
                ALERT_INTERVAL_SECONDS,
            ),
        )

    await query.message.edit_text(
        "🔔 هشدار سیگنال: "
        + (
            "فعال"
            if new_value
            else "خاموش"
        )
    )


async def alert_worker(app):
    while True:
        try:
            with db() as c:
                users = c.execute(
                    """
                    SELECT user_id
                    FROM alert_preferences
                    WHERE enabled=1
                    """
                ).fetchall()

            for user_row in users:
                uid = user_row["user_id"]

                if not has_analysis_access(uid):
                    continue

                assets = user_assets(uid)

                for asset in assets:
                    symbol = asset["symbol"]

                    try:
                        analysis = await analyze(symbol)

                        if not analysis:
                            continue

                        signal = analysis["signal"]

                        if signal == "WAIT":
                            continue

                        signal_key = (
                            f"{symbol}:{signal}"
                        )

                        # Only alert when this signal has not
                        # already been sent recently. Keeping the
                        # latest signal key prevents duplicate spam,
                        # while allowing a new BUY after SELL and
                        # vice versa.
                        with db() as c:
                            previous = c.execute(
                                """
                                SELECT signal_key
                                FROM alert_events
                                WHERE user_id=?
                                  AND symbol=?
                                ORDER BY id DESC
                                LIMIT 1
                                """,
                                (
                                    uid,
                                    symbol,
                                ),
                            ).fetchone()

                            if (
                                previous
                                and previous["signal_key"]
                                == signal_key
                            ):
                                continue

                            c.execute(
                                """
                                INSERT INTO alert_events(
                                    user_id,symbol,
                                    signal_key,message,created_at
                                )
                                VALUES(?,?,?,?,?)
                                """,
                                (
                                    uid,
                                    symbol,
                                    signal_key,
                                    analysis_text(analysis),
                                    now_iso(),
                                ),
                            )

                        try:
                            await app.bot.send_message(
                                uid,
                                "🔔 <b>سیگنال جدید</b>\n\n"
                                + analysis_text(analysis),
                                parse_mode=ParseMode.HTML,
                            )
                        except Exception:
                            log.debug(
                                "alert send failed: %s",
                                uid,
                            )

                    except Exception:
                        log.exception(
                            "alert asset error: %s",
                            symbol,
                        )

                with db() as c:
                    c.execute(
                        """
                        UPDATE alert_preferences
                        SET last_check_at=?
                        WHERE user_id=?
                        """,
                        (
                            now_iso(),
                            uid,
                        ),
                    )

        except Exception:
            log.exception(
                "alert worker error"
            )

        await asyncio.sleep(
            max(30, ALERT_INTERVAL_SECONDS)
        )


# ============================================================
# ADMIN
# ============================================================

async def admin_panel(update, context):
    if not is_admin(update.effective_user.id):
        return

    await update.message.reply_text(
        "👨‍💼 پنل مدیریت",
        reply_markup=admin_kb(),
    )


async def admin_callback(update, context):
    query = update.callback_query
    await query.answer()

    if not is_admin(query.from_user.id):
        return

    parts = query.data.split(":")
    action = parts[1]

    if action == "stats":
        with db() as c:
            users = c.execute(
                "SELECT COUNT(*) n FROM users"
            ).fetchone()["n"]

            active = c.execute(
                """
                SELECT COUNT(DISTINCT user_id) n
                FROM subscriptions
                WHERE status='active'
                  AND end_at>?
                """,
                (now_iso(),),
            ).fetchone()["n"]

            pending = c.execute(
                """
                SELECT COUNT(*) n
                FROM payment_requests
                WHERE status='pending'
                """
            ).fetchone()["n"]

            chats = c.execute(
                """
                SELECT COUNT(*) n
                FROM chat_messages
                WHERE deleted=0
                """
            ).fetchone()["n"]

            assets = c.execute(
                """
                SELECT COUNT(*) n
                FROM watchlist
                """
            ).fetchone()["n"]

        await query.message.reply_text(
            "📊 <b>آمار</b>\n\n"
            f"👥 کاربران: {users}\n"
            f"💳 مشترک فعال: {active}\n"
            f"⏳ پرداخت در انتظار: {pending}\n"
            f"💬 پیام‌های چت: {chats}\n"
            f"🪙 دارایی‌های واچ‌لیست: {assets}",
            parse_mode=ParseMode.HTML,
        )

    elif action == "broadcast":
        context.user_data["admin_mode"] = "broadcast"

        await query.message.reply_text(
            "📢 متن پیام برای تمام مشترکین فعال "
            "را ارسال کنید.\n"
            "برای لغو /cancel"
        )

    elif action == "message":
        context.user_data["admin_mode"] = "message_uid"

        await query.message.reply_text(
            "شناسه عددی کاربر را ارسال کنید."
        )

    elif action == "block":
        context.user_data["admin_mode"] = "block"

        await query.message.reply_text(
            "شناسه عددی کاربر را ارسال کنید؛ "
            "اگر مسدود باشد رفع مسدود می‌شود."
        )

    elif action == "payments":
        with db() as c:
            rows = c.execute(
                """
                SELECT *
                FROM payment_requests
                WHERE status='pending'
                ORDER BY id DESC
                LIMIT 20
                """
            ).fetchall()

        if not rows:
            await query.message.reply_text(
                "پرداخت در انتظاری وجود ندارد."
            )
            return

        for row in rows:
            await query.message.reply_text(
                f"💳 #{row['id']}\n"
                f"کاربر: {row['user_id']}\n"
                f"پلن: {row['days']} روز\n"
                f"مبلغ: {row['amount']:,}",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "✅ تایید",
                                callback_data=(
                                    f"pay:approve:{row['id']}"
                                ),
                            ),
                            InlineKeyboardButton(
                                "❌ رد",
                                callback_data=(
                                    f"pay:reject:{row['id']}"
                                ),
                            ),
                        ]
                    ]
                ),
            )

    elif action == "users":
        page = (
            int(parts[2])
            if len(parts) > 2
            else 0
        )

        with db() as c:
            rows = c.execute(
                """
                SELECT *
                FROM users
                ORDER BY created_at DESC
                LIMIT 20 OFFSET ?
                """,
                (page * 20,),
            ).fetchall()

        text = (
            "👥 <b>کاربران</b>\n\n"
            + "\n".join(
                (
                    f"{row['user_id']} | "
                    f"{escape(row['first_name'] or '-')}"
                    f" | "
                    f"{'🚫' if row['blocked'] else '✅'}"
                )
                for row in rows
            )
        )

        await query.message.reply_text(
            text if rows else "کاربری نیست.",
            parse_mode=ParseMode.HTML,
        )

    elif action == "support":
        with db() as c:
            rows = c.execute(
                """
                SELECT *
                FROM support_messages
                WHERE direction='user_to_admin'
                ORDER BY id DESC
                LIMIT 20
                """
            ).fetchall()

        if not rows:
            await query.message.reply_text(
                "پیام پشتیبانی وجود ندارد."
            )
            return

        for row in rows:
            await query.message.reply_text(
                f"📨 #{row['id']} از {row['user_id']}\n"
                f"{escape(row['message'] or '[رسانه]')}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "↩️ پاسخ",
                                callback_data=(
                                    f"sup:reply:{row['user_id']}"
                                ),
                            )
                        ]
                    ]
                ),
            )

    elif action == "chat":
        with db() as c:
            rooms = c.execute(
                """
                SELECT symbol,COUNT(*) n
                FROM chat_messages
                WHERE deleted=0
                GROUP BY symbol
                ORDER BY n DESC
                """
            ).fetchall()

        text = (
            "💬 <b>اتاق‌های چت</b>\n\n"
            + (
                "\n".join(
                    f"• {row['symbol']}: "
                    f"{row['n']} پیام"
                    for row in rooms
                )
                if rooms
                else "هنوز پیامی ثبت نشده است."
            )
        )

        await query.message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
        )

    elif action == "reports":
        with db() as c:
            rows = c.execute(
                """
                SELECT *
                FROM chat_reports
                WHERE status='pending'
                ORDER BY id DESC
                LIMIT 20
                """
            ).fetchall()

        text = (
            "🚨 <b>گزارش‌ها</b>\n\n"
            + (
                "\n".join(
                    (
                        f"#{row['id']} "
                        f"پیام #{row['message_id']} "
                        f"توسط {row['reporter_id']}"
                    )
                    for row in rows
                )
                if rows
                else "گزارش جدیدی نیست."
            )
        )

        await query.message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
        )


async def payment_callback(update, context):
    query = update.callback_query
    await query.answer()

    if not is_admin(query.from_user.id):
        return

    _, action, payment_id = (
        query.data.split(":")
    )

    payment_id = int(payment_id)

    with db() as c:
        row = c.execute(
            """
            SELECT *
            FROM payment_requests
            WHERE id=?
            """,
            (payment_id,),
        ).fetchone()

    if not row or row["status"] != "pending":
        await query.message.reply_text(
            "این درخواست قبلاً بررسی شده است."
        )
        return

    if action == "approve":
        add_subscription(
            row["user_id"],
            row["plan"],
            payment_id,
            "manual",
        )

        with db() as c:
            c.execute(
                """
                UPDATE payment_requests
                SET status='approved',
                    reviewed_at=?,
                    reviewed_by=?
                WHERE id=?
                """,
                (
                    now_iso(),
                    query.from_user.id,
                    payment_id,
                ),
            )

        try:
            await context.bot.send_message(
                row["user_id"],
                (
                    "✅ پرداخت شما تایید شد.\n"
                    f"اشتراک {row['days']} روزه فعال گردید."
                ),
            )
        except Exception:
            pass

        await query.message.reply_text(
            "✅ اشتراک فعال شد."
        )

    elif action == "reject":
        with db() as c:
            c.execute(
                """
                UPDATE payment_requests
                SET status='rejected',
                    reviewed_at=?,
                    reviewed_by=?
                WHERE id=?
                """,
                (
                    now_iso(),
                    query.from_user.id,
                    payment_id,
                ),
            )

        try:
            await context.bot.send_message(
                row["user_id"],
                (
                    "❌ رسید پرداخت شما تایید نشد.\n"
                    "برای بررسی با پشتیبان تماس بگیرید."
                ),
            )
        except Exception:
            pass

        await query.message.reply_text(
            "❌ درخواست رد شد."
        )


# ============================================================
# ADMIN TEXT ROUTER
# ============================================================

async def admin_text_action(
    update,
    context,
    text,
):
    uid = update.effective_user.id
    mode = context.user_data.get(
        "admin_mode"
    )

    if not is_admin(uid) or not mode:
        return False

    if text == "/cancel":
        context.user_data.pop(
            "admin_mode",
            None,
        )

        await update.message.reply_text(
            "لغو شد."
        )
        return True

    if mode == "broadcast":
        context.user_data.pop(
            "admin_mode",
            None,
        )

        with db() as c:
            rows = c.execute(
                """
                SELECT DISTINCT u.user_id
                FROM users u
                JOIN subscriptions s
                    ON s.user_id=u.user_id
                WHERE u.blocked=0
                  AND s.status='active'
                  AND s.end_at>?
                """,
                (now_iso(),),
            ).fetchall()

        success = 0
        failed = 0

        for row in rows:
            try:
                await context.bot.send_message(
                    row["user_id"],
                    f"📢 پیام مدیر:\n\n{text}",
                )
                success += 1
            except Exception:
                failed += 1

        await update.message.reply_text(
            f"✅ ارسال موفق: {success}\n"
            f"❌ ناموفق: {failed}"
        )

        return True

    if mode == "message_uid":
        try:
            target = int(text)
        except ValueError:
            await update.message.reply_text(
                "شناسه نامعتبر است."
            )
            return True

        context.user_data["message_target"] = target
        context.user_data["admin_mode"] = "message_text"

        await update.message.reply_text(
            "متن پیام را ارسال کنید."
        )

        return True

    if mode == "message_text":
        target = context.user_data.pop(
            "message_target",
            None,
        )

        context.user_data.pop(
            "admin_mode",
            None,
        )

        try:
            await context.bot.send_message(
                target,
                f"📨 پیام مدیر:\n\n{text}",
            )

            await update.message.reply_text(
                "✅ ارسال شد."
            )

        except Exception as exc:
            await update.message.reply_text(
                f"❌ ارسال نشد: {exc}"
            )

        return True

    if mode == "block":
        try:
            target = int(text)
        except ValueError:
            await update.message.reply_text(
                "شناسه نامعتبر است."
            )
            return True

        with db() as c:
            row = c.execute(
                """
                SELECT blocked
                FROM users
                WHERE user_id=?
                """,
                (target,),
            ).fetchone()

            if not row:
                await update.message.reply_text(
                    "کاربر پیدا نشد."
                )
                return True

            new_value = (
                0
                if row["blocked"]
                else 1
            )

            c.execute(
                """
                UPDATE users
                SET blocked=?
                WHERE user_id=?
                """,
                (
                    new_value,
                    target,
                ),
            )

        context.user_data.pop(
            "admin_mode",
            None,
        )

        await update.message.reply_text(
            (
                "🚫 مسدود شد: "
                if new_value
                else "✅ رفع مسدودی شد: "
            )
            + str(target)
        )

        return True

    return False


# ============================================================
# TEXT ROUTER
# ============================================================

async def text_router(update, context):
    if not update.message:
        return

    ensure_user(update.effective_user)

    uid = update.effective_user.id
    text = (
        update.message.text or ""
    ).strip()

    if is_blocked(uid) and not is_admin(uid):
        await update.message.reply_text(
            "🚫 دسترسی شما توسط مدیر محدود شده است."
        )
        return

    # Cancel always wins.
    if text == "/cancel":
        for key in (
            "support_mode",
            "chat_room",
            "awaiting_asset",
            "payment_plan",
            "admin_mode",
            "admin_reply_to",
            "message_target",
        ):
            context.user_data.pop(
                key,
                None,
            )

        await update.message.reply_text(
            "لغو شد.",
            reply_markup=main_kb(uid),
        )
        return

    # Main menu always has priority.
    handlers = {
        "➕ افزودن دارایی": add_asset_prompt,
        "📋 واچ‌لیست": watchlist_menu,
        "💰 قیمت لحظه‌ای": live_price_menu,
        "📊 تحلیل": analysis_prompt,
        "🚨 سیگنال‌ها": signals_menu,
        "💳 خرید اشتراک": buy_menu,
        "👤 وضعیت اشتراک": status_menu,
        "🔔 هشدارها": alerts_menu,
        "🪙 ارزهای بیشتر": more_coins,
        "💬 چت رمز ارز": crypto_chat_menu,
        "📨 ارتباط با پشتیبان": support_prompt,
        "ℹ️ راهنما": help_text,
        "👨‍💼 پنل مدیریت": admin_panel,
    }

    if text in handlers:
        for key in (
            "support_mode",
            "chat_room",
            "awaiting_asset",
            "payment_plan",
            "admin_reply_to",
            "message_target",
        ):
            context.user_data.pop(
                key,
                None,
            )

        await handlers[text](
            update,
            context,
        )
        return

    # Admin modes.
    if await admin_text_action(
        update,
        context,
        text,
    ):
        return

    # Admin replying to support.
    if (
        is_admin(uid)
        and context.user_data.get(
            "admin_reply_to"
        )
    ):
        await send_support_reply(
            update,
            context,
            text,
        )
        return

    # Support text.
    if context.user_data.get(
        "support_mode"
    ):
        if text:
            context.user_data.pop(
                "support_mode",
                None,
            )

            await save_support(
                uid,
                text,
                update.message.message_id,
            )

            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(
                        admin_id,
                        (
                            f"📨 پیام پشتیبانی از {uid}:\n\n"
                            f"{escape(text)}"
                        ),
                        parse_mode=ParseMode.HTML,
                        reply_markup=InlineKeyboardMarkup(
                            [
                                [
                                    InlineKeyboardButton(
                                        "↩️ پاسخ",
                                        callback_data=(
                                            f"sup:reply:{uid}"
                                        ),
                                    )
                                ]
                            ]
                        ),
                    )

                except Exception:
                    pass

            await update.message.reply_text(
                "✅ پیام شما برای پشتیبان ارسال شد."
            )

        return

    # Existing crypto room.
    if await process_chat_message(
        update,
        context,
    ):
        return

    # Asset input state.
    awaiting_asset = context.user_data.get(
        "awaiting_asset"
    )

    if awaiting_asset:
        mode = context.user_data.pop(
            "awaiting_asset",
            None,
        )

        if mode == "add":
            await process_add_asset(
                update,
                context,
                text,
            )
            return

        if mode == "analysis":
            if not has_analysis_access(uid):
                await update.message.reply_text(
                    "🔒 اشتراک فعال لازم است."
                )
                return

            symbol = norm_symbol(text)

            await update.message.reply_text(
                f"⏳ در حال تحلیل {symbol}..."
            )

            try:
                analysis = await analyze(
                    symbol
                )

                if not analysis:
                    await update.message.reply_text(
                        f"❌ اطلاعات بازار برای "
                        f"{escape(symbol)} "
                        "در دسترس نیست.",
                        parse_mode=ParseMode.HTML,
                    )
                    return

                await update.message.reply_text(
                    analysis_text(analysis),
                    parse_mode=ParseMode.HTML,
                )

            except Exception:
                log.exception(
                    "analysis router error"
                )

                await update.message.reply_text(
                    "⚠️ دریافت اطلاعات بازار "
                    "با خطا مواجه شد."
                )

            return

    # --------------------------------------------------------
    # FALLBACK ASSET DETECTION
    #
    # This is the important fix for the issue where the user
    # sends "Zec" and the bot stays silent because the temporary
    # awaiting_asset state has disappeared.
    # --------------------------------------------------------

    possible_symbol = norm_symbol(text)

    if (
        possible_symbol in COINS
        or possible_symbol in (
            "XAU",
            "GOLD18",
        )
    ):
        await process_add_asset(
            update,
            context,
            text,
        )
        return

    # Helpful fallback instead of silence.
    if text:
        await update.message.reply_text(
            "❓ دستور یا نماد شناخته نشد.\n\n"
            "برای افزودن دارایی از «➕ افزودن دارایی» "
            "استفاده کنید.\n\n"
            "نمونه: BTC، ZEC، XAU، GOLD18"
        )


# ============================================================
# MEDIA ROUTER
# ============================================================

async def media_router(update, context):
    ensure_user(update.effective_user)

    uid = update.effective_user.id

    if is_blocked(uid) and not is_admin(uid):
        return

    if context.user_data.get(
        "support_mode"
    ):
        await support_media(
            update,
            context,
        )
        return

    if update.message.photo:
        await receipt_photo(
            update,
            context,
        )
        return

    await update.message.reply_text(
        "برای این نوع پیام، ابتدا از بخش "
        "پشتیبانی استفاده کنید."
    )


# ============================================================
# CALLBACK ROUTER
# ============================================================

async def misc_callback(update, context):
    query = update.callback_query
    data = query.data

    if data.startswith("pick:"):
        await query.answer()

        try:
            symbol = norm_symbol(
                data.split(":", 1)[1]
            )

            if symbol not in COINS and symbol not in (
                "XAU",
                "GOLD18",
            ):
                await query.message.reply_text(
                    "❌ دارایی نامعتبر است."
                )
                return

            if not add_watch(
                query.from_user.id,
                symbol,
                asset_type(symbol),
            ):
                await query.message.reply_text(
                    "⚠️ سقف واچ‌لیست شما پر شده است."
                )
                return

            await query.message.reply_text(
                f"✅ <b>{escape(symbol)}</b> "
                "به واچ‌لیست اضافه شد.",
                parse_mode=ParseMode.HTML,
            )

        except Exception:
            log.exception(
                "pick asset error"
            )

            await query.message.reply_text(
                "⚠️ افزودن دارایی انجام نشد. "
                "دوباره تلاش کنید."
            )

        return

    if data.startswith("wl:del:"):
        await query.answer("حذف شد")

        try:
            _, _, asset, symbol = data.split(
                ":",
                3,
            )

            remove_watch(
                query.from_user.id,
                symbol,
                asset,
            )

            await query.message.edit_text(
                f"✅ {escape(symbol)} حذف شد.",
                parse_mode=ParseMode.HTML,
            )

        except Exception:
            log.exception(
                "watchlist delete error"
            )

            await query.message.reply_text(
                "⚠️ حذف دارایی انجام نشد."
            )

        return


# ============================================================
# APPLICATION LIFECYCLE
# ============================================================

async def post_init(app):
    global HTTP_SESSION

    init_db()

    try:
        await app.bot.delete_webhook(
            drop_pending_updates=False
        )
    except Exception as exc:
        log.warning(
            "delete webhook: %s",
            exc,
        )

    app.create_task(
        alert_worker(app)
    )

    log.info(
        "Market Analyzer started | DB=%s | admins=%s",
        DB_PATH,
        sorted(ADMIN_IDS),
    )


async def post_shutdown(app):
    global HTTP_SESSION

    if (
        HTTP_SESSION
        and not HTTP_SESSION.closed
    ):
        await HTTP_SESSION.close()


# ============================================================
# MAIN
# ============================================================

def main():
    if not BOT_TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is not set"
        )

    init_db()

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    # Commands
    application.add_handler(
        CommandHandler(
            "start",
            start,
        )
    )

    application.add_handler(
        CommandHandler(
            "cancel",
            text_router,
        )
    )

    # Subscription/payment callbacks
    application.add_handler(
        CallbackQueryHandler(
            plan_callback,
            pattern=r"^plan:",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            payment_callback,
            pattern=r"^pay:",
        )
    )

    # Support
    application.add_handler(
        CallbackQueryHandler(
            support_reply_callback,
            pattern=r"^sup:reply:",
        )
    )

    # Crypto chat
    application.add_handler(
        CallbackQueryHandler(
            chat_open_callback,
            pattern=r"^chat:open:",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            chat_report_callback,
            pattern=r"^chat:report:\d+$",
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            chat_admin_callback,
            pattern=r"^chat:(delete|block):\d+$",
        )
    )

    # Alerts
    application.add_handler(
        CallbackQueryHandler(
            alert_callback,
            pattern=r"^alert:",
        )
    )

    # Admin
    application.add_handler(
        CallbackQueryHandler(
            admin_callback,
            pattern=r"^adm:",
        )
    )

    # Free live-price selector
    application.add_handler(
        CallbackQueryHandler(
            selected_price_callback,
            pattern=r"^price:",
        )
    )

    # Paid analysis/signal selectors
    application.add_handler(
        CallbackQueryHandler(
            analysis_selector_callback,
            pattern=r"^analysis:",
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            signal_selector_callback,
            pattern=r"^signal:",
        )
    )

    # Watchlist/asset selection
    application.add_handler(
        CallbackQueryHandler(
            misc_callback,
            pattern=r"^(pick:|wl:del:)",
        )
    )

    # Photos / documents / voice
    application.add_handler(
        MessageHandler(
            filters.PHOTO
            | filters.Document.ALL
            | filters.VOICE,
            media_router,
        )
    )

    # Text
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            text_router,
        )
    )

    log.info("Starting polling...")

    application.run_polling(
        drop_pending_updates=False,
        allowed_updates=Update.ALL_TYPES,
    )


if __name__ == "__main__":
    main()
