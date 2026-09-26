# -*- coding: utf-8 -*-
"""
Crypto Analyzer - Fast & Stable Railway Build
Telegram bot for crypto analysis/signals.

Start command:
    python main.py

Required:
    TELEGRAM_BOT_TOKEN

Optional:
    ADMIN_IDS=123456789,987654321
    SUPPORT_USERNAME=@YourSupport
    PAYMENT_CARD=6037...
    DB_PATH=/data/crypto_bot.db
    MAX_WATCHLIST=100
    HTTP_TIMEOUT=15
    CACHE_SECONDS=30
    ALERT_INTERVAL=300

IMPORTANT:
- Analysis/signals only. No automatic trading.
- Existing SQLite database is preserved.
- Subscription/payment history is preserved.
- Only one running instance should use the same Telegram bot token.
"""

import os
import json
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

# ============================================================================
# CONFIG
# ============================================================================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}

SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "").strip()
PAYMENT_CARD = os.getenv("PAYMENT_CARD", "").strip()

DB_PATH = os.getenv("DB_PATH", "/data/crypto_bot.db").strip()

MAX_WATCHLIST = max(
    1,
    int(os.getenv("MAX_WATCHLIST", "100"))
)

HTTP_TIMEOUT = max(
    5,
    int(os.getenv("HTTP_TIMEOUT", "15"))
)

CACHE_SECONDS = max(
    5,
    int(os.getenv("CACHE_SECONDS", "30"))
)

ALERT_INTERVAL = max(
    60,
    int(os.getenv("ALERT_INTERVAL", "300"))
)

CG_BASE = "https://api.coingecko.com/api/v3"

TELEGRAM_TIMEOUT = 20

PLANS = {
    "30": {
        "days": 30,
        "price": 200000,
        "title": "۳۰ روزه",
    },
    "90": {
        "days": 90,
        "price": 350000,
        "title": "۹۰ روزه",
    },
    "180": {
        "days": 180,
        "price": 500000,
        "title": "۱۸۰ روزه",
    },
}

ASSET_ALIASES = {
    "btc": "bitcoin",
    "bitcoin": "bitcoin",
    "بیتکوین": "bitcoin",
    "بیت کوین": "bitcoin",

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
    "ripple": "ripple",

    "ada": "cardano",
    "cardano": "cardano",

    "doge": "dogecoin",
    "dogecoin": "dogecoin",

    "trx": "tron",
    "tron": "tron",

    "dot": "polkadot",
    "polkadot": "polkadot",

    "avax": "avalanche-2",
    "avalanche": "avalanche-2",

    "link": "chainlink",
    "chainlink": "chainlink",

    "matic": "matic-network",
    "polygon": "polygon-ecosystem-token",

    "pol": "polygon-ecosystem-token",
}

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("crypto-analyzer")

# ============================================================================
# GLOBAL STATE
# ============================================================================

DB_LOCK = asyncio.Lock()

HTTP_SESSION: Optional[aiohttp.ClientSession] = None

CACHE_LOCK = asyncio.Lock()

MARKET_CACHE: dict[str, tuple[float, dict]] = {}
CHART_CACHE: dict[str, tuple[float, list[float]]] = {}

BACKGROUND_TASK: Optional[asyncio.Task] = None


# ============================================================================
# DATABASE
# ============================================================================

def ensure_db_dir() -> None:
    folder = os.path.dirname(DB_PATH)
    if folder:
        os.makedirs(folder, exist_ok=True)


def db_connect() -> sqlite3.Connection:
    ensure_db_dir()

    conn = sqlite3.connect(
        DB_PATH,
        timeout=8,
        check_same_thread=False,
    )

    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA busy_timeout=8000")
    conn.execute("PRAGMA foreign_keys=ON")

    return conn


def ensure_column(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    definition: str,
) -> None:
    rows = conn.execute(
        f"PRAGMA table_info({table})"
    ).fetchall()

    existing = {
        str(row[1])
        for row in rows
    }

    if column not in existing:
        logger.info(
            "DB migration: adding %s.%s",
            table,
            column,
        )

        conn.execute(
            f"ALTER TABLE {table} ADD COLUMN {column} {definition}"
        )


def init_db() -> None:
    ensure_db_dir()

    conn = db_connect()

    try:
        conn.execute("PRAGMA journal_mode=WAL")

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

        # Legacy migrations.
        ensure_column(
            conn,
            "users",
            "username",
            "TEXT DEFAULT ''",
        )

        ensure_column(
            conn,
            "users",
            "first_name",
            "TEXT DEFAULT ''",
        )

        ensure_column(
            conn,
            "users",
            "created_at",
            "TEXT DEFAULT ''",
        )

        ensure_column(
            conn,
            "users",
            "last_seen_at",
            "TEXT",
        )

        ensure_column(
            conn,
            "users",
            "blocked",
            "INTEGER NOT NULL DEFAULT 0",
        )

        ensure_column(
            conn,
            "subscriptions",
            "source",
            "TEXT NOT NULL DEFAULT 'manual'",
        )

        ensure_column(
            conn,
            "payment_requests",
            "receipt_file_id",
            "TEXT",
        )

        ensure_column(
            conn,
            "payment_requests",
            "status",
            "TEXT NOT NULL DEFAULT 'pending'",
        )

        ensure_column(
            conn,
            "payment_requests",
            "reviewed_at",
            "TEXT",
        )

        ensure_column(
            conn,
            "payment_requests",
            "reviewed_by",
            "INTEGER",
        )

        # Useful indexes.
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sub_user_end
            ON subscriptions(user_id, end_at)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_payment_status
            ON payment_requests(status, id)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_watch_user
            ON watchlist(user_id)
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
        dt = datetime.fromisoformat(value)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt

    except Exception:
        return None


# ============================================================================
# USER / SUBSCRIPTION
# ============================================================================

async def upsert_user(update: Update) -> None:
    user = update.effective_user

    if not user:
        return

    uid = user.id
    username = user.username or ""
    first_name = user.first_name or ""
    current = now_iso()

    async with DB_LOCK:
        conn = db_connect()

        try:
            conn.execute(
                """
                INSERT INTO users(
                    user_id,
                    username,
                    first_name,
                    created_at,
                    last_seen_at
                )
                VALUES(?,?,?,?,?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username=excluded.username,
                    first_name=excluded.first_name,
                    last_seen_at=excluded.last_seen_at
                """,
                (
                    uid,
                    username,
                    first_name,
                    current,
                    current,
                ),
            )

            conn.execute(
                """
                INSERT OR IGNORE INTO settings(
                    user_id,
                    alerts_enabled
                )
                VALUES(?,0)
                """,
                (uid,),
            )

            conn.commit()

        finally:
            conn.close()


def is_blocked(user_id: int) -> bool:
    conn = db_connect()

    try:
        row = conn.execute(
            """
            SELECT blocked
            FROM users
            WHERE user_id=?
            """,
            (user_id,),
        ).fetchone()

        return bool(
            row and row["blocked"]
        )

    finally:
        conn.close()


def get_subscription(
    user_id: int,
) -> Optional[sqlite3.Row]:

    conn = db_connect()

    try:
        return conn.execute(
            """
            SELECT *
            FROM subscriptions
            WHERE user_id=?
              AND datetime(end_at) > datetime('now')
            ORDER BY datetime(end_at) DESC
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()

    finally:
        conn.close()


def subscription_active(user_id: int) -> bool:
    return get_subscription(user_id) is not None


async def add_subscription(
    user_id: int,
    plan_key: str,
    days: int,
    admin_id: int,
) -> None:

    async with DB_LOCK:
        conn = db_connect()

        try:
            current = conn.execute(
                """
                SELECT *
                FROM subscriptions
                WHERE user_id=?
                  AND datetime(end_at) > datetime('now')
                ORDER BY datetime(end_at) DESC
                LIMIT 1
                """,
                (user_id,),
            ).fetchone()

            start = datetime.now(timezone.utc)

            if current:
                old_end = parse_iso(
                    current["end_at"]
                )

                if old_end and old_end > start:
                    start = old_end

            end = start + timedelta(days=days)

            conn.execute(
                """
                INSERT INTO subscriptions(
                    user_id,
                    plan,
                    start_at,
                    end_at,
                    created_at,
                    source
                )
                VALUES(?,?,?,?,?,?)
                """,
                (
                    user_id,
                    plan_key,
                    start.isoformat(),
                    end.isoformat(),
                    now_iso(),
                    "admin",
                ),
            )

            conn.execute(
                """
                INSERT INTO admin_log(
                    admin_id,
                    action,
                    target_user,
                    created_at
                )
                VALUES(?,?,?,?)
                """,
                (
                    admin_id,
                    f"extend_{plan_key}",
                    user_id,
                    now_iso(),
                ),
            )

            conn.commit()

        finally:
            conn.close()


# ============================================================================
# HELPERS
# ============================================================================

def money(value: int) -> str:
    return f"{value:,}".replace(",", "٬")


def pct(value: float) -> str:
    return f"{value:.1f}%"


def safe_float(
    value: Any,
    default: float = 0.0,
) -> float:

    try:
        return float(value)

    except Exception:
        return default


def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [
                KeyboardButton("📋 واچ‌لیست"),
                KeyboardButton("➕ افزودن ارز"),
            ],
            [
                KeyboardButton("📊 تحلیل"),
                KeyboardButton("🚨 سیگنال‌ها"),
            ],
            [
                KeyboardButton("💳 خرید اشتراک"),
                KeyboardButton("👤 وضعیت اشتراک"),
            ],
            [
                KeyboardButton("🔔 هشدارها"),
                KeyboardButton("ℹ️ راهنما"),
            ],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📊 آمار",
                    callback_data="admin:stats",
                ),
                InlineKeyboardButton(
                    "💳 پرداخت‌ها",
                    callback_data="admin:payments",
                ),
            ],
            [
                InlineKeyboardButton(
                    "👥 کاربران",
                    callback_data="admin:users",
                )
            ],
        ]
    )


async def send_home(
    update: Update,
    text: Optional[str] = None,
) -> None:

    if text is None:
        text = (
            "🤖 <b>Crypto Analyzer</b>\n\n"
            "تحلیل و سیگنال بازار ارزهای دیجیتال\n"
            "بدون اجرای خودکار معامله.\n\n"
            "از منوی پایین یک گزینه را انتخاب کنید."
        )

    await update.effective_message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard(),
    )


# ============================================================================
# HTTP / COINGECKO
# ============================================================================

async def init_http() -> None:
    global HTTP_SESSION

    if HTTP_SESSION is not None:
        return

    timeout = aiohttp.ClientTimeout(
        total=HTTP_TIMEOUT,
        connect=min(8, HTTP_TIMEOUT),
    )

    connector = aiohttp.TCPConnector(
        limit=20,
        limit_per_host=10,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )

    headers = {
        "Accept": "application/json",
        "User-Agent": "CryptoAnalyzer/2.0",
    }

    HTTP_SESSION = aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers=headers,
    )


async def close_http() -> None:
    global HTTP_SESSION

    if HTTP_SESSION is not None:
        await HTTP_SESSION.close()
        HTTP_SESSION = None


async def cg_get(
    path: str,
    params: Optional[dict] = None,
) -> Any:

    await init_http()

    if HTTP_SESSION is None:
        raise RuntimeError("HTTP session unavailable")

    url = CG_BASE + path

    last_error = None

    for attempt in range(3):

        try:
            async with HTTP_SESSION.get(
                url,
                params=params or {},
            ) as response:

                text = await response.text()

                if response.status == 429:
                    wait = 1.5 * (attempt + 1)
                    logger.warning(
                        "CoinGecko rate limit. retry=%s",
                        attempt + 1,
                    )
                    await asyncio.sleep(wait)
                    continue

                if response.status != 200:
                    raise RuntimeError(
                        f"CoinGecko HTTP {response.status}: "
                        f"{text[:250]}"
                    )

                return json.loads(text)

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            json.JSONDecodeError,
        ) as exc:

            last_error = exc

            if attempt < 2:
                await asyncio.sleep(
                    0.7 * (attempt + 1)
                )

    raise RuntimeError(
        f"CoinGecko request failed: {last_error}"
    )


async def search_coins(
    query: str,
) -> list[dict]:

    q = query.strip()

    if not q:
        return []

    alias = ASSET_ALIASES.get(
        q.lower()
    )

    if alias:
        q = alias

    data = await cg_get(
        "/search",
        {"query": q},
    )

    coins = data.get(
        "coins",
        [],
    )

    result = []

    for coin in coins[:10]:

        coin_id = coin.get("id")

        if not coin_id:
            continue

        result.append(
            {
                "id": coin_id,
                "name": coin.get(
                    "name",
                    coin_id,
                ),
                "symbol": coin.get(
                    "symbol",
                    "",
                ),
                "rank": coin.get(
                    "market_cap_rank"
                ),
            }
        )

    return result


async def get_market(
    coin_id: str,
) -> dict:

    now = time.time()

    async with CACHE_LOCK:

        cached = MARKET_CACHE.get(
            coin_id
        )

        if cached:
            timestamp, value = cached

            if now - timestamp < CACHE_SECONDS:
                return value

    data = await cg_get(
        "/coins/markets",
        {
            "vs_currency": "usd",
            "ids": coin_id,
            "price_change_percentage": "24h,7d,30d",
            "sparkline": "false",
        },
    )

    if not data:
        raise RuntimeError(
            "Asset not found"
        )

    value = data[0]

    async with CACHE_LOCK:
        MARKET_CACHE[coin_id] = (
            time.time(),
            value,
        )

    return value


async def get_chart(
    coin_id: str,
    days: int = 90,
) -> list[float]:

    cache_key = f"{coin_id}:{days}"

    now = time.time()

    async with CACHE_LOCK:

        cached = CHART_CACHE.get(
            cache_key
        )

        if cached:
            timestamp, value = cached

            # Chart cache is deliberately longer.
            if now - timestamp < max(
                60,
                CACHE_SECONDS * 2,
            ):
                return value

    data = await cg_get(
        f"/coins/{coin_id}/market_chart",
        {
            "vs_currency": "usd",
            "days": days,
            "interval": "daily",
        },
    )

    prices = [
        safe_float(item[1])
        for item in data.get("prices", [])
        if isinstance(item, list)
        and len(item) >= 2
    ]

    if not prices:
        raise RuntimeError(
            "No chart data"
        )

    async with CACHE_LOCK:
        CHART_CACHE[cache_key] = (
            time.time(),
            prices,
        )

    return prices


# ============================================================================
# TECHNICAL ANALYSIS
# ============================================================================

def ema(
    values: list[float],
    period: int,
) -> list[float]:

    if not values:
        return []

    period = max(
        1,
        min(period, len(values)),
    )

    multiplier = 2.0 / (
        period + 1.0
    )

    result = [values[0]]

    for price in values[1:]:
        result.append(
            price * multiplier
            + result[-1]
            * (1 - multiplier)
        )

    return result


def rsi(
    values: list[float],
    period: int = 14,
) -> float:

    if len(values) < period + 1:
        return 50.0

    gains = []
    losses = []

    for i in range(1, len(values)):

        diff = (
            values[i]
            - values[i - 1]
        )

        gains.append(
            max(diff, 0)
        )

        losses.append(
            max(-diff, 0)
        )

    avg_gain = (
        sum(gains[:period])
        / period
    )

    avg_loss = (
        sum(losses[:period])
        / period
    )

    for i in range(
        period,
        len(gains),
    ):

        avg_gain = (
            (
                avg_gain
                * (period - 1)
            )
            + gains[i]
        ) / period

        avg_loss = (
            (
                avg_loss
                * (period - 1)
            )
            + losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100.0 - (
        100.0
        / (1.0 + rs)
    )


def macd(
    values: list[float],
) -> tuple[float, float, float]:

    if len(values) < 35:
        return 0.0, 0.0, 0.0

    e12 = ema(
        values,
        12,
    )

    e26 = ema(
        values,
        26,
    )

    line = [
        a - b
        for a, b in zip(
            e12[-len(e26):],
            e26,
        )
    ]

    signal = ema(
        line,
        9,
    )

    macd_line = line[-1]

    signal_line = (
        signal[-1]
        if signal
        else 0.0
    )

    histogram = (
        macd_line
        - signal_line
    )

    return (
        macd_line,
        signal_line,
        histogram,
    )


def analyze_prices(
    values: list[float],
) -> dict:

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
            "reason": (
                "داده کافی برای تحلیل "
                "کامل وجود ندارد."
            ),
        }

    e20 = ema(
        values,
        20,
    )[-1]

    e50 = ema(
        values,
        50,
    )[-1]

    rv = rsi(values)

    ml, ms, mh = macd(values)

    ret7 = 0.0

    if (
        len(values) >= 8
        and values[-8] != 0
    ):
        ret7 = (
            values[-1]
            / values[-8]
            - 1
        ) * 100

    ret30 = 0.0

    if (
        len(values) >= 31
        and values[-31] != 0
    ):
        ret30 = (
            values[-1]
            / values[-31]
            - 1
        ) * 100

    score = 0.0
    reasons = []

    # EMA trend
    if e20 > e50:
        score += 25
        reasons.append(
            "EMA20 بالاتر از EMA50"
        )
    else:
        score -= 25
        reasons.append(
            "EMA20 پایین‌تر از EMA50"
        )

    # RSI
    if rv >= 55:
        score += 20
        reasons.append(
            "RSI متمایل به خریداران"
        )

    elif rv <= 45:
        score -= 20
        reasons.append(
            "RSI متمایل به فروشندگان"
        )

    else:
        reasons.append(
            "RSI در ناحیه میانی"
        )

    # MACD
    if mh > 0:
        score += 20
        reasons.append(
            "MACD مثبت"
        )
    else:
        score -= 20
        reasons.append(
            "MACD منفی"
        )

    # 7 day
    if ret7 > 0:
        score += 15
    else:
        score -= 15

    # 30 day
    if ret30 > 0:
        score += 20
    else:
        score -= 20

    strength = min(
        100.0,
        max(
            0.0,
            50.0 + score / 2,
        ),
    )

    if score >= 35:
        signal = "BUY"

    elif score <= -35:
        signal = "SELL"

    else:
        signal = "WAIT"

    # Separate heuristic probability.
    profit_probability = min(
        85.0,
        max(
            15.0,
            50.0
            + abs(score) * 0.35
            + (
                5.0
                if (
                    signal == "BUY"
                    and ret7 > 0
                )
                else 0.0
            )
            + (
                5.0
                if (
                    signal == "SELL"
                    and ret7 < 0
                )
                else 0.0
            ),
        ),
    )

    return {
        "signal": signal,
        "strength": strength,
        "profit_probability": profit_probability,
        "rsi": rv,
        "ema20": e20,
        "ema50": e50,
        "macd": ml,
        "macd_signal": ms,
        "return_7d": ret7,
        "return_30d": ret30,
        "reason": "، ".join(reasons),
    }


def signal_fa(
    signal: str,
) -> str:

    return {
        "BUY": "🟢 خرید",
        "SELL": "🔴 فروش",
        "WAIT": "🟡 صبر",
    }.get(
        signal,
        "🟡 صبر",
    )


# ============================================================================
# WATCHLIST
# ============================================================================

def get_watchlist(
    user_id: int,
) -> list[sqlite3.Row]:

    conn = db_connect()

    try:
        return conn.execute(
            """
            SELECT *
            FROM watchlist
            WHERE user_id=?
            ORDER BY created_at DESC
            """,
            (user_id,),
        ).fetchall()

    finally:
        conn.close()


async def add_watch(
    user_id: int,
    coin: dict,
) -> tuple[bool, str]:

    async with DB_LOCK:

        conn = db_connect()

        try:
            count = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM watchlist
                WHERE user_id=?
                """,
                (user_id,),
            ).fetchone()["c"]

            if count >= MAX_WATCHLIST:
                return (
                    False,
                    f"حداکثر {MAX_WATCHLIST} "
                    "ارز می‌توانید اضافه کنید.",
                )

            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO watchlist(
                    user_id,
                    coin_id,
                    symbol,
                    name,
                    created_at
                )
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

            if cursor.rowcount == 0:
                return (
                    False,
                    "ℹ️ این ارز قبلاً "
                    "در واچ‌لیست شما وجود دارد.",
                )

            return (
                True,
                f"✅ {coin['name']} "
                f"({coin['symbol'].upper()}) "
                "به واچ‌لیست اضافه شد.",
            )

        finally:
            conn.close()


async def remove_watch(
    user_id: int,
    coin_id: str,
) -> None:

    async with DB_LOCK:

        conn = db_connect()

        try:
            conn.execute(
                """
                DELETE FROM watchlist
                WHERE user_id=?
                  AND coin_id=?
                """,
                (
                    user_id,
                    coin_id,
                ),
            )

            conn.execute(
                """
                DELETE FROM alert_state
                WHERE user_id=?
                  AND coin_id=?
                """,
                (
                    user_id,
                    coin_id,
                ),
            )

            conn.commit()

        finally:
            conn.close()


# ============================================================================
# ANALYSIS TEXT
# ============================================================================

def build_analysis_text(
    market: dict,
    analysis: dict,
) -> str:

    name = market.get(
        "name",
        "Unknown",
    )

    symbol = str(
        market.get(
            "symbol",
            "",
        )
    ).upper()

    price = safe_float(
        market.get(
            "current_price"
        )
    )

    change24 = safe_float(
        market.get(
            "price_change_percentage_24h"
        )
    )

    change7 = safe_float(
        market.get(
            "price_change_percentage_7d_in_currency"
        )
    )

    change30 = safe_float(
        market.get(
            "price_change_percentage_30d_in_currency"
        )
    )

    return (
        f"📊 <b>تحلیل {name} "
        f"({symbol})</b>\n\n"

        f"💰 قیمت: "
        f"<code>${price:,.8f}</code>\n"

        f"📈 ۲۴ ساعت: "
        f"{change24:+.2f}%\n"

        f"📈 ۷ روز: "
        f"{change7:+.2f}%\n"

        f"📈 ۳۰ روز: "
        f"{change30:+.2f}%\n\n"

        f"🎯 <b>سیگنال:</b> "
        f"{signal_fa(analysis['signal'])}\n"

        f"💪 <b>درصد قدرت:</b> "
        f"{pct(analysis['strength'])}\n"

        f"💰 <b>احتمال سود:</b> "
        f"{pct(analysis['profit_probability'])}\n\n"

        f"RSI: "
        f"{analysis['rsi']:.1f}\n"

        f"EMA20: "
        f"{analysis['ema20']:.6g}\n"

        f"EMA50: "
        f"{analysis['ema50']:.6g}\n"

        f"MACD: "
        f"{analysis['macd']:.6g}\n\n"

        f"🧠 {analysis['reason']}\n\n"

        "⚠️ این تحلیل تخمینی است "
        "و تضمین سود نیست."
    )


async def fetch_analysis(
    coin_id: str,
) -> tuple[dict, dict]:

    # Market and chart are independent.
    market_task = asyncio.create_task(
        get_market(coin_id)
    )

    chart_task = asyncio.create_task(
        get_chart(coin_id, 90)
    )

    market, prices = await asyncio.gather(
        market_task,
        chart_task,
    )

    return (
        market,
        analyze_prices(prices),
    )


# ============================================================================
# BASIC HANDLERS
# ============================================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:

    await upsert_user(update)

    if is_blocked(
        update.effective_user.id
    ):
        return

    context.user_data.clear()

    await send_home(update)


async def help_cmd(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:

    await upsert_user(update)

    support = ""

    if SUPPORT_USERNAME:
        support = (
            f"\n📞 پشتیبانی: "
            f"{SUPPORT_USERNAME}\n"
        )

    text = (
        "ℹ️ <b>راهنمای ربات</b>\n\n"

        "• «➕ افزودن ارز» "
        "برای جستجو و افزودن ارز\n"

        "• «📊 تحلیل» "
        "برای تحلیل تکنیکال\n"

        "• «🚨 سیگنال‌ها» "
        "برای بررسی واچ‌لیست\n"

        "• «🔔 هشدارها» "
        "برای دریافت هشدار سیگنال\n"

        "• قیمت فعلی رایگان است؛ "
        "تحلیل و سیگنال نیاز به اشتراک دارد.\n"

        "• ربات معامله خودکار انجام نمی‌دهد.\n"

        f"{support}\n"

        "نمونه جستجو:\n"
        "BTC\n"
        "ZEC\n"
        "SOL\n"
        "Ethereum"
    )

    await update.effective_message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard(),
    )


async def watchlist_cmd(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:

    await upsert_user(update)

    rows = get_watchlist(
        update.effective_user.id
    )

    if not rows:

        await update.effective_message.reply_text(
            "📋 واچ‌لیست شما خالی است.\n\n"
            "➕ افزودن ارز را بزنید.",
            reply_markup=main_keyboard(),
        )

        return

    buttons = []

    for row in rows:

        buttons.append(
            [
                InlineKeyboardButton(
                    f"📈 {row['symbol']} — "
                    f"{row['name'][:20]}",
                    callback_data=(
                        f"analyze:{row['coin_id']}"
                    ),
                ),
                InlineKeyboardButton(
                    "❌",
                    callback_data=(
                        f"remove:{row['coin_id']}"
                    ),
                ),
            ]
        )

    await update.effective_message.reply_text(
        "📋 <b>واچ‌لیست شما</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            buttons
        ),
    )


async def add_coin_start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:

    await upsert_user(update)

    context.user_data["state"] = "search"

    await update.effective_message.reply_text(
        "➕ نام یا نماد ارز را بفرستید.\n\n"
        "مثال:\n"
        "BTC\n"
        "ZEC\n"
        "SOL\n"
        "Ethereum",
        reply_markup=main_keyboard(),
    )


async def subscription_status(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:

    await upsert_user(update)

    row = get_subscription(
        update.effective_user.id
    )

    if not row:

        text = (
            "👤 <b>وضعیت اشتراک</b>\n\n"
            "❌ اشتراک فعال ندارید."
        )

    else:

        end = parse_iso(
            row["end_at"]
        )

        end_text = (
            end.astimezone().strftime(
                "%Y-%m-%d %H:%M"
            )
            if end
            else "-"
        )

        text = (
            "👤 <b>وضعیت اشتراک</b>\n\n"
            "✅ فعال\n"
            f"📅 پایان: "
            f"<code>{end_text}</code>\n"
            f"📦 پلن: "
            f"{PLANS.get(row['plan'], {}).get('title', row['plan'])}"
        )

    await update.effective_message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard(),
    )


async def buy_subscription(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:

    await upsert_user(update)

    buttons = [
        [
            InlineKeyboardButton(
                f"۳۰ روزه — "
                f"{money(PLANS['30']['price'])} تومان",
                callback_data="plan:30",
            )
        ],
        [
            InlineKeyboardButton(
                f"۹۰ روزه — "
                f"{money(PLANS['90']['price'])} تومان",
                callback_data="plan:90",
            )
        ],
        [
            InlineKeyboardButton(
                f"۱۸۰ روزه — "
                f"{money(PLANS['180']['price'])} تومان",
                callback_data="plan:180",
            )
        ],
    ]

    await update.effective_message.reply_text(
        "💳 <b>خرید اشتراک</b>\n\n"
        "پس از انتخاب پلن، "
        "شماره کارت و مراحل ارسال رسید "
        "نمایش داده می‌شود.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            buttons
        ),
    )


# ============================================================================
# ALERTS
# ============================================================================

async def alerts_menu(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:

    await upsert_user(update)

    conn = db_connect()

    try:
        row = conn.execute(
            """
            SELECT alerts_enabled
            FROM settings
            WHERE user_id=?
            """,
            (update.effective_user.id,),
        ).fetchone()

    finally:
        conn.close()

    enabled = bool(
        row and row["alerts_enabled"]
    )

    status = (
        "فعال ✅"
        if enabled
        else "خاموش ❌"
    )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🔕 خاموش"
                    if enabled
                    else "🔔 فعال‌سازی",
                    callback_data="alerts:toggle",
                )
            ]
        ]
    )

    await update.effective_message.reply_text(
        f"🔔 هشدارهای سیگنال: "
        f"<b>{status}</b>\n\n"
        "در حالت فعال، ربات واچ‌لیست شما "
        "را به‌صورت دوره‌ای بررسی می‌کند.",
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
    )


async def alert_loop(
    application: Application,
) -> None:

    logger.info(
        "Alert monitor started. interval=%ss",
        ALERT_INTERVAL,
    )

    while True:

        try:

            await asyncio.sleep(
                ALERT_INTERVAL
            )

            conn = db_connect()

            try:

                rows = conn.execute(
                    """
                    SELECT
                        s.user_id,
                        w.coin_id,
                        w.symbol,
                        w.name
                    FROM settings s
                    JOIN watchlist w
                      ON w.user_id=s.user_id
                    WHERE s.alerts_enabled=1
                    """
                ).fetchall()

            finally:
                conn.close()

            if not rows:
                continue

            # Group watchlist items by user.
            grouped = {}

            for row in rows:
                grouped.setdefault(
                    row["user_id"],
                    [],
                ).append(row)

            for user_id, items in grouped.items():

                try:

                    if not subscription_active(
                        user_id
                    ):
                        continue

                    # Avoid hammering CoinGecko.
                    for item in items[:20]:

                        try:

                            _, analysis = (
                                await fetch_analysis(
                                    item["coin_id"]
                                )
                            )

                            signal = analysis[
                                "signal"
                            ]

                            # WAIT alerts are not sent.
                            if signal == "WAIT":
                                continue

                            conn = db_connect()

                            try:

                                state = conn.execute(
                                    """
                                    SELECT *
                                    FROM alert_state
                                    WHERE user_id=?
                                      AND coin_id=?
                                    """,
                                    (
                                        user_id,
                                        item["coin_id"],
                                    ),
                                ).fetchone()

                            finally:
                                conn.close()

                            last_signal = (
                                state["last_signal"]
                                if state
                                else None
                            )

                            last_sent = parse_iso(
                                state["last_sent_at"]
                                if state
                                else None
                            )

                            now = datetime.now(
                                timezone.utc
                            )

                            # Same signal is not sent repeatedly
                            # for six hours.
                            if (
                                last_signal == signal
                                and last_sent
                                and (
                                    now
                                    - last_sent
                                ).total_seconds()
                                < 21600
                            ):
                                continue

                            text = (
                                "🚨 <b>هشدار سیگنال</b>\n\n"
                                f"🪙 {item['name']} "
                                f"({item['symbol']})\n\n"
                                f"🎯 سیگنال: "
                                f"{signal_fa(signal)}\n"
                                f"💪 قدرت: "
                                f"{pct(analysis['strength'])}\n"
                                f"💰 احتمال سود: "
                                f"{pct(analysis['profit_probability'])}\n\n"
                                f"RSI: "
                                f"{analysis['rsi']:.1f}\n\n"
                                "⚠️ این هشدار تحلیلی است "
                                "و تضمین سود نیست."
                            )

                            await application.bot.send_message(
                                chat_id=user_id,
                                text=text,
                                parse_mode=ParseMode.HTML,
                            )

                            async with DB_LOCK:

                                conn = db_connect()

                                try:

                                    conn.execute(
                                        """
                                        INSERT INTO alert_state(
                                            user_id,
                                            coin_id,
                                            last_signal,
                                            last_sent_at
                                        )
                                        VALUES(?,?,?,?)
                                        ON CONFLICT(
                                            user_id,
                                            coin_id
                                        )
                                        DO UPDATE SET
                                            last_signal=
                                                excluded.last_signal,
                                            last_sent_at=
                                                excluded.last_sent_at
                                        """,
                                        (
                                            user_id,
                                            item["coin_id"],
                                            signal,
                                            now.isoformat(),
                                        ),
                                    )

                                    conn.commit()

                                finally:
                                    conn.close()

                        except Exception:
                            logger.exception(
                                "alert error user=%s coin=%s",
                                user_id,
                                item["coin_id"],
                            )

                except Exception:
                    logger.exception(
                        "alert user error user=%s",
                        user_id,
                    )

        except asyncio.CancelledError:
            logger.info(
                "Alert monitor stopped."
            )
            raise

        except Exception:
            logger.exception(
                "alert loop error"
            )


# ============================================================================
# ANALYSIS
# ============================================================================

async def price_analysis(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    coin_id: str,
) -> None:

    if not subscription_active(
        update.effective_user.id
    ):

        await update.effective_message.reply_text(
            "🔒 تحلیل و سیگنال فقط "
            "برای کاربران دارای اشتراک فعال است.\n\n"
            "💳 خرید اشتراک را انتخاب کنید."
        )

        return

    try:

        await update.effective_message.reply_text(
            "⏳ در حال دریافت داده و تحلیل..."
        )

        market, analysis = (
            await fetch_analysis(
                coin_id
            )
        )

        text = build_analysis_text(
            market,
            analysis,
        )

        await update.effective_message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=main_keyboard(),
        )

    except Exception:

        logger.exception(
            "analysis error coin=%s",
            coin_id,
        )

        await update.effective_message.reply_text(
            "❌ دریافت داده یا تحلیل انجام نشد.\n"
            "چند لحظه بعد دوباره امتحان کنید.",
            reply_markup=main_keyboard(),
        )


async def analyze_menu(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:

    await upsert_user(update)

    rows = get_watchlist(
        update.effective_user.id
    )

    if not rows:

        await update.effective_message.reply_text(
            "📊 ابتدا حداقل یک ارز "
            "به واچ‌لیست اضافه کنید.",
            reply_markup=main_keyboard(),
        )

        return

    buttons = []

    for row in rows:

        buttons.append(
            [
                InlineKeyboardButton(
                    f"📊 {row['symbol']} — "
                    f"{row['name'][:18]}",
                    callback_data=(
                        f"analyze:{row['coin_id']}"
                    ),
                )
            ]
        )

    await update.effective_message.reply_text(
        "📊 ارز موردنظر را انتخاب کنید:",
        reply_markup=InlineKeyboardMarkup(
            buttons
        ),
    )


async def signals_menu(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:

    await upsert_user(update)

    if not subscription_active(
        update.effective_user.id
    ):

        await update.effective_message.reply_text(
            "🔒 سیگنال‌ها نیاز به "
            "اشتراک فعال دارند.",
            reply_markup=main_keyboard(),
        )

        return

    rows = get_watchlist(
        update.effective_user.id
    )

    if not rows:

        await update.effective_message.reply_text(
            "🚨 واچ‌لیست شما خالی است.",
            reply_markup=main_keyboard(),
        )

        return

    await update.effective_message.reply_text(
        "⏳ در حال بررسی سیگنال‌های واچ‌لیست..."
    )

    selected = rows[:20]

    async def one_signal(row):

        try:

            _, analysis = (
                await fetch_analysis(
                    row["coin_id"]
                )
            )

            return (
                f"• <b>{row['symbol']}</b> → "
                f"{signal_fa(analysis['signal'])} | "
                f"قدرت {pct(analysis['strength'])} | "
                f"احتمال سود "
                f"{pct(analysis['profit_probability'])}"
            )

        except Exception:

            logger.exception(
                "signal error coin=%s",
                row["coin_id"],
            )

            return (
                f"• <b>{row['symbol']}</b> → "
                "❌ خطا در دریافت داده"
            )

    # Parallel analysis instead of one-by-one requests.
    results = await asyncio.gather(
        *[
            one_signal(row)
            for row in selected
        ]
    )

    text = (
        "🚨 <b>سیگنال‌های واچ‌لیست</b>\n\n"
        + "\n".join(results)
    )

    await update.effective_message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard(),
    )


# ============================================================================
# PAYMENT
# ============================================================================

async def handle_plan_callback(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    plan: str,
) -> None:

    if plan not in PLANS:

        await query.answer(
            "پلن نامعتبر است.",
            show_alert=True,
        )

        return

    context.user_data[
        "payment_plan"
    ] = plan

    p = PLANS[plan]

    card = (
        PAYMENT_CARD
        or "شماره کارت در Railway تنظیم نشده است."
    )

    text = (
        f"💳 <b>پلن {p['title']}</b>\n\n"
        f"مبلغ: "
        f"<b>{money(p['price'])} تومان</b>\n\n"
        "شماره کارت:\n"
        f"<code>{card}</code>\n\n"
        "پس از واریز، تصویر رسید را "
        "همین‌جا ارسال کنید.\n"
        "رسید برای مدیر ارسال و پس از تأیید، "
        "اشتراک فعال می‌شود."
    )

    await query.edit_message_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "❌ لغو",
                        callback_data="payment:cancel",
                    )
                ]
            ]
        ),
    )


async def receipt_photo(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:

    await upsert_user(update)

    plan = context.user_data.get(
        "payment_plan"
    )

    if (
        not plan
        or plan not in PLANS
    ):

        await update.effective_message.reply_text(
            "ابتدا از بخش "
            "«💳 خرید اشتراک» "
            "یک پلن انتخاب کنید.",
            reply_markup=main_keyboard(),
        )

        return

    photo = (
        update.effective_message.photo[-1]
    )

    p = PLANS[plan]

    async with DB_LOCK:

        conn = db_connect()

        try:

            cursor = conn.execute(
                """
                INSERT INTO payment_requests(
                    user_id,
                    plan,
                    amount,
                    receipt_file_id,
                    status,
                    created_at
                )
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

            request_id = cursor.lastrowid

            conn.commit()

        finally:
            conn.close()

    context.user_data.pop(
        "payment_plan",
        None,
    )

    await update.effective_message.reply_text(
        f"✅ رسید ثبت شد.\n"
        f"کد درخواست: "
        f"<code>#{request_id}</code>\n"
        "پس از بررسی مدیر، نتیجه اعلام می‌شود.",
        parse_mode=ParseMode.HTML,
        reply_markup=main_keyboard(),
    )

    caption = (
        f"💳 <b>رسید پرداخت جدید "
        f"#{request_id}</b>\n"
        f"👤 User ID: "
        f"<code>{update.effective_user.id}</code>\n"
        f"📦 پلن: {p['title']}\n"
        f"💰 مبلغ: "
        f"{money(p['price'])} تومان"
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
                                callback_data=(
                                    f"pay:approve:{request_id}"
                                ),
                            ),
                            InlineKeyboardButton(
                                "❌ رد",
                                callback_data=(
                                    f"pay:reject:{request_id}"
                                ),
                            ),
                        ]
                    ]
                ),
            )

        except Exception:

            logger.exception(
                "cannot notify admin %s",
                admin_id,
            )


# ============================================================================
# ADMIN
# ============================================================================

def admin_only(
    user_id: int,
) -> bool:

    return user_id in ADMIN_IDS


async def admin_cmd(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:

    await upsert_user(update)

    if not admin_only(
        update.effective_user.id
    ):

        await update.effective_message.reply_text(
            "⛔ دسترسی ندارید."
        )

        return

    await update.effective_message.reply_text(
        "🛠 <b>پنل مدیریت</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_keyboard(),
    )


async def admin_stats(
    query,
) -> None:

    conn = db_connect()

    try:

        users = conn.execute(
            "SELECT COUNT(*) c FROM users"
        ).fetchone()["c"]

        active = conn.execute(
            """
            SELECT COUNT(DISTINCT user_id) c
            FROM subscriptions
            WHERE datetime(end_at)
                  > datetime('now')
            """
        ).fetchone()["c"]

        pending = conn.execute(
            """
            SELECT COUNT(*) c
            FROM payment_requests
            WHERE status='pending'
            """
        ).fetchone()["c"]

    finally:
        conn.close()

    await query.edit_message_text(
        "📊 <b>آمار</b>\n\n"
        f"👥 کاربران: {users}\n"
        f"🟢 اشتراک فعال: {active}\n"
        f"💳 پرداخت در انتظار: {pending}",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_keyboard(),
    )


async def admin_payments(
    query,
) -> None:

    conn = db_connect()

    try:

        rows = conn.execute(
            """
            SELECT *
            FROM payment_requests
            WHERE status='pending'
            ORDER BY id DESC
            LIMIT 20
            """
        ).fetchall()

    finally:
        conn.close()

    if not rows:

        await query.edit_message_text(
            "💳 پرداخت در انتظار وجود ندارد.",
            reply_markup=admin_keyboard(),
        )

        return

    lines = [
        "💳 <b>پرداخت‌های در انتظار</b>\n"
    ]

    buttons = []

    for row in rows:

        lines.append(
            f"#{row['id']} | "
            f"user {row['user_id']} | "
            f"{PLANS.get(row['plan'], {}).get('title', row['plan'])} | "
            f"{money(row['amount'])}"
        )

        buttons.append(
            [
                InlineKeyboardButton(
                    f"#{row['id']} بررسی",
                    callback_data=(
                        f"pay:view:{row['id']}"
                    ),
                )
            ]
        )

    buttons.append(
        [
            InlineKeyboardButton(
                "⬅️ برگشت",
                callback_data="admin:home",
            )
        ]
    )

    await query.edit_message_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            buttons
        ),
    )


async def approve_payment(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    request_id: int,
) -> None:

    async with DB_LOCK:

        conn = db_connect()

        try:

            row = conn.execute(
                """
                SELECT *
                FROM payment_requests
                WHERE id=?
                """,
                (request_id,),
            ).fetchone()

            if (
                not row
                or row["status"] != "pending"
            ):

                await query.answer(
                    "این درخواست قبلاً بررسی شده است.",
                    show_alert=True,
                )

                return

            conn.execute(
                """
                UPDATE payment_requests
                SET
                    status='approved',
                    reviewed_at=?,
                    reviewed_by=?
                WHERE id=?
                  AND status='pending'
                """,
                (
                    now_iso(),
                    query.from_user.id,
                    request_id,
                ),
            )

            conn.commit()

        finally:
            conn.close()

    plan = PLANS.get(
        row["plan"]
    )

    if not plan:

        await query.answer(
            "پلن پرداخت نامعتبر است.",
            show_alert=True,
        )

        return

    await add_subscription(
        row["user_id"],
        row["plan"],
        plan["days"],
        query.from_user.id,
    )

    try:

        await context.bot.send_message(
            row["user_id"],
            "✅ پرداخت شما تأیید شد.\n"
            f"اشتراک {plan['title']} فعال شد.",
        )

    except Exception:

        logger.exception(
            "cannot notify approved user"
        )

    try:
        await query.edit_message_reply_markup(
            reply_markup=None
        )
    except Exception:
        pass

    await query.answer(
        "پرداخت تأیید و اشتراک فعال شد.",
        show_alert=True,
    )


async def reject_payment(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    request_id: int,
) -> None:

    async with DB_LOCK:

        conn = db_connect()

        try:

            row = conn.execute(
                """
                SELECT *
                FROM payment_requests
                WHERE id=?
                """,
                (request_id,),
            ).fetchone()

            if (
                not row
                or row["status"] != "pending"
            ):

                await query.answer(
                    "این درخواست قبلاً بررسی شده است.",
                    show_alert=True,
                )

                return

            conn.execute(
                """
                UPDATE payment_requests
                SET
                    status='rejected',
                    reviewed_at=?,
                    reviewed_by=?
                WHERE id=?
                  AND status='pending'
                """,
                (
                    now_iso(),
                    query.from_user.id,
                    request_id,
                ),
            )

            conn.commit()

        finally:
            conn.close()

    try:

        await context.bot.send_message(
            row["user_id"],
            "❌ رسید پرداخت شما رد شد.\n"
            "در صورت نیاز با پشتیبانی تماس بگیرید.",
        )

    except Exception:

        logger.exception(
            "cannot notify rejected user"
        )

    try:
        await query.edit_message_reply_markup(
            reply_markup=None
        )
    except Exception:
        pass

    await query.answer(
        "رسید رد شد.",
        show_alert=True,
    )


# ============================================================================
# CALLBACKS
# ============================================================================

async def callbacks(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:

    query = update.callback_query
    data = query.data or ""

    # Answer exactly once at the beginning.
    try:
        await query.answer()
    except Exception:
        pass

    # ---------------------------------------------------------------------
    # PAYMENT PLAN
    # ---------------------------------------------------------------------

    if data.startswith("plan:"):

        await handle_plan_callback(
            query,
            context,
            data.split(
                ":",
                1,
            )[1],
        )

        return

    if data == "payment:cancel":

        context.user_data.pop(
            "payment_plan",
            None,
        )

        await query.edit_message_text(
            "❌ خرید اشتراک لغو شد."
        )

        return

    # ---------------------------------------------------------------------
    # ADD COIN
    # ---------------------------------------------------------------------

    if data.startswith("add:"):

        coin_id = data.split(
            ":",
            1,
        )[1]

        try:

            # Do not make another CoinGecko request.
            # Search result is cached in callback data context.
            pending = context.user_data.get(
                "search_results",
                {},
            )

            coin = pending.get(
                coin_id
            )

            if not coin:

                # Fallback only if callback
                # originated from an old message.
                result = await cg_get(
                    f"/coins/{coin_id}",
                    {
                        "localization": "false",
                        "tickers": "false",
                        "market_data": "false",
                        "community_data": "false",
                        "developer_data": "false",
                    },
                )

                coin = {
                    "id": coin_id,
                    "name": result.get(
                        "name",
                        coin_id,
                    ),
                    "symbol": result.get(
                        "symbol",
                        "",
                    ),
                }

            ok, message = await add_watch(
                query.from_user.id,
                coin,
            )

            await query.edit_message_text(
                message
            )

        except Exception:

            logger.exception(
                "add coin error"
            )

            await query.edit_message_text(
                "❌ افزودن ارز انجام نشد.\n"
                "دوباره تلاش کنید."
            )

        return

    # ---------------------------------------------------------------------
    # REMOVE
    # ---------------------------------------------------------------------

    if data.startswith("remove:"):

        coin_id = data.split(
            ":",
            1,
        )[1]

        await remove_watch(
            query.from_user.id,
            coin_id,
        )

        await query.edit_message_text(
            "✅ ارز از واچ‌لیست حذف شد."
        )

        return

    # ---------------------------------------------------------------------
    # ANALYZE
    # ---------------------------------------------------------------------

    if data.startswith("analyze:"):

        coin_id = data.split(
            ":",
            1,
        )[1]

        if not subscription_active(
            query.from_user.id
        ):

            await query.edit_message_text(
                "🔒 تحلیل و سیگنال فقط "
                "برای کاربران دارای "
                "اشتراک فعال است."
            )

            return

        await query.edit_message_text(
            "⏳ در حال دریافت داده و تحلیل..."
        )

        try:

            market, analysis = (
                await fetch_analysis(
                    coin_id
                )
            )

            text = build_analysis_text(
                market,
                analysis,
            )

            await query.edit_message_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "⬅️ بازگشت به واچ‌لیست",
                                callback_data="back:watch",
                            )
                        ]
                    ]
                ),
            )

        except Exception:

            logger.exception(
                "callback analysis error "
                "coin=%s",
                coin_id,
            )

            await query.edit_message_text(
                "❌ تحلیل انجام نشد.\n"
                "دوباره تلاش کنید."
            )

        return

    # ---------------------------------------------------------------------
    # BACK
    # ---------------------------------------------------------------------

    if data == "back:watch":

        rows = get_watchlist(
            query.from_user.id
        )

        if not rows:

            await query.edit_message_text(
                "📋 واچ‌لیست شما خالی است."
            )

            return

        buttons = []

        for row in rows:

            buttons.append(
                [
                    InlineKeyboardButton(
                        f"📈 {row['symbol']} — "
                        f"{row['name'][:20]}",
                        callback_data=(
                            f"analyze:{row['coin_id']}"
                        ),
                    ),
                    InlineKeyboardButton(
                        "❌",
                        callback_data=(
                            f"remove:{row['coin_id']}"
                        ),
                    ),
                ]
            )

        await query.edit_message_text(
            "📋 <b>واچ‌لیست شما</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(
                buttons
            ),
        )

        return

    # ---------------------------------------------------------------------
    # ALERT TOGGLE
    # ---------------------------------------------------------------------

    if data == "alerts:toggle":

        async with DB_LOCK:

            conn = db_connect()

            try:

                row = conn.execute(
                    """
                    SELECT alerts_enabled
                    FROM settings
                    WHERE user_id=?
                    """,
                    (
                        query.from_user.id,
                    ),
                ).fetchone()

                current = bool(
                    row
                    and row["alerts_enabled"]
                )

                new_value = (
                    0
                    if current
                    else 1
                )

                conn.execute(
                    """
                    INSERT INTO settings(
                        user_id,
                        alerts_enabled
                    )
                    VALUES(?,?)
                    ON CONFLICT(user_id)
                    DO UPDATE SET
                        alerts_enabled=
                            excluded.alerts_enabled
                    """,
                    (
                        query.from_user.id,
                        new_value,
                    ),
                )

                conn.commit()

            finally:
                conn.close()

        await query.edit_message_text(
            "🔔 هشدارها فعال شد."
            if new_value
            else "🔕 هشدارها خاموش شد."
        )

        return

    # ---------------------------------------------------------------------
    # ADMIN
    # ---------------------------------------------------------------------

    if data.startswith("admin:"):

        if not admin_only(
            query.from_user.id
        ):

            await query.answer(
                "⛔ دسترسی ندارید.",
                show_alert=True,
            )

            return

        action = data.split(
            ":",
            1,
        )[1]

        if action == "home":

            await query.edit_message_text(
                "🛠 <b>پنل مدیریت</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=admin_keyboard(),
            )

        elif action == "stats":

            await admin_stats(
                query
            )

        elif action == "payments":

            await admin_payments(
                query
            )

        elif action == "users":

            conn = db_connect()

            try:

                rows = conn.execute(
                    """
                    SELECT
                        user_id,
                        username,
                        first_name
                    FROM users
                    ORDER BY user_id DESC
                    LIMIT 20
                    """
                ).fetchall()

            finally:
                conn.close()

            lines = [
                "👥 <b>آخرین کاربران</b>\n"
            ]

            for row in rows:

                lines.append(
                    f"• {row['user_id']} | "
                    f"@{row['username'] or '-'} | "
                    f"{row['first_name'] or '-'}"
                )

            await query.edit_message_text(
                "\n".join(lines),
                parse_mode=ParseMode.HTML,
                reply_markup=admin_keyboard(),
            )

        return

    # ---------------------------------------------------------------------
    # PAYMENT ADMIN
    # ---------------------------------------------------------------------

    if data.startswith("pay:"):

        if not admin_only(
            query.from_user.id
        ):

            await query.answer(
                "⛔ دسترسی ندارید.",
                show_alert=True,
            )

            return

        parts = data.split(":")

        if len(parts) != 3:
            return

        action = parts[1]

        try:
            request_id = int(
                parts[2]
            )
        except ValueError:
            return

        if action == "approve":

            await approve_payment(
                query,
                context,
                request_id,
            )

        elif action == "reject":

            await reject_payment(
                query,
                context,
                request_id,
            )

        elif action == "view":

            conn = db_connect()

            try:

                row = conn.execute(
                    """
                    SELECT *
                    FROM payment_requests
                    WHERE id=?
                    """,
                    (request_id,),
                ).fetchone()

            finally:
                conn.close()

            if not row:

                await query.edit_message_text(
                    "درخواست پیدا نشد."
                )

                return

            await query.edit_message_text(
                f"💳 درخواست #{request_id}\n"
                f"user: "
                f"<code>{row['user_id']}</code>\n"
                f"plan: "
                f"{PLANS.get(row['plan'], {}).get('title', row['plan'])}\n"
                f"amount: "
                f"{money(row['amount'])}\n\n"
                "برای تأیید/رد، "
                "پیام رسیدی که برای مدیر "
                "ارسال شده را باز کنید.",
                parse_mode=ParseMode.HTML,
            )

        return


# ============================================================================
# TEXT ROUTER
# ============================================================================

async def text_router(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:

    await upsert_user(update)

    if is_blocked(
        update.effective_user.id
    ):
        return

    text = (
        update.effective_message.text
        or ""
    ).strip()

    # Main menu.
    if text == "📋 واچ‌لیست":

        await watchlist_cmd(
            update,
            context,
        )

        return

    if text == "➕ افزودن ارز":

        await add_coin_start(
            update,
            context,
        )

        return

    if text == "📊 تحلیل":

        await analyze_menu(
            update,
            context,
        )

        return

    if text == "🚨 سیگنال‌ها":

        await signals_menu(
            update,
            context,
        )

        return

    if text == "💳 خرید اشتراک":

        await buy_subscription(
            update,
            context,
        )

        return

    if text == "👤 وضعیت اشتراک":

        await subscription_status(
            update,
            context,
        )

        return

    if text == "🔔 هشدارها":

        await alerts_menu(
            update,
            context,
        )

        return

    if text == "ℹ️ راهنما":

        await help_cmd(
            update,
            context,
        )

        return

    # Search state.
    if (
        context.user_data.get(
            "state"
        )
        == "search"
    ):

        context.user_data.pop(
            "state",
            None,
        )

        await update.effective_message.reply_text(
            "⏳ در حال جستجو..."
        )

        try:

            results = await search_coins(
                text
            )

            if not results:

                await update.effective_message.reply_text(
                    "❌ ارزی پیدا نشد.\n"
                    "نماد دیگری بفرستید."
                )

                return

            # Keep search result locally so
            # adding a coin does not need another API call.
            context.user_data[
                "search_results"
            ] = {
                item["id"]: item
                for item in results
            }

            buttons = []

            for coin in results[:8]:

                title = (
                    f"{coin['symbol'].upper()} "
                    f"— {coin['name']}"
                )

                buttons.append(
                    [
                        InlineKeyboardButton(
                            title[:55],
                            callback_data=(
                                f"add:{coin['id']}"
                            ),
                        )
                    ]
                )

            await update.effective_message.reply_text(
                "🔎 نتیجه جستجو؛ "
                "ارز موردنظر را انتخاب کنید:",
                reply_markup=InlineKeyboardMarkup(
                    buttons
                ),
            )

        except Exception:

            logger.exception(
                "search error"
            )

            await update.effective_message.reply_text(
                "❌ خطا در جستجو.\n"
                "چند لحظه بعد دوباره تلاش کنید.",
                reply_markup=main_keyboard(),
            )

        return

    await update.effective_message.reply_text(
        "از منوی پایین استفاده کنید "
        "یا «➕ افزودن ارز» را بزنید.",
        reply_markup=main_keyboard(),
    )


# ============================================================================
# ERROR HANDLER
# ============================================================================

async def error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:

    error = context.error

    logger.error(
        "Unhandled exception: %r",
        error,
        exc_info=error,
    )


# ============================================================================
# APPLICATION
# ============================================================================

def build_application() -> Application:

    if not BOT_TOKEN:

        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is missing. "
            "Add it in Railway Variables."
        )

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(
            TELEGRAM_TIMEOUT
        )
        .read_timeout(
            TELEGRAM_TIMEOUT
        )
        .write_timeout(
            TELEGRAM_TIMEOUT
        )
        .pool_timeout(
            TELEGRAM_TIMEOUT
        )
        .build()
    )

    application.add_handler(
        CommandHandler(
            "start",
            start,
        )
    )

    application.add_handler(
        CommandHandler(
            "help",
            help_cmd,
        )
    )

    application.add_handler(
        CommandHandler(
            "admin",
            admin_cmd,
        )
    )

    # One callback router is enough.
    application.add_handler(
        CallbackQueryHandler(
            callbacks
        )
    )

    application.add_handler(
        MessageHandler(
            filters.PHOTO,
            receipt_photo,
        )
    )

    application.add_handler(
        MessageHandler(
            filters.TEXT
            & ~filters.COMMAND,
            text_router,
        )
    )

    application.add_error_handler(
        error_handler
    )

    return application


# ============================================================================
# RUN
# ============================================================================

async def run() -> None:

    global BACKGROUND_TASK

    init_db()

    logger.info(
        "=================================================="
    )

    logger.info(
        "Starting Crypto Analyzer..."
    )

    logger.info(
        "DB_PATH=%s",
        DB_PATH,
    )

    logger.info(
        "ADMIN_IDS=%s",
        sorted(ADMIN_IDS),
    )

    logger.info(
        "CACHE_SECONDS=%s",
        CACHE_SECONDS,
    )

    logger.info(
        "ALERT_INTERVAL=%s",
        ALERT_INTERVAL,
    )

    await init_http()

    application = build_application()

    await application.initialize()

    try:

        bot_info = await application.bot.get_me()

        logger.info(
            "Telegram connected: @%s id=%s",
            bot_info.username,
            bot_info.id,
        )

        # Telegram polling must be exclusive.
        await application.bot.delete_webhook(
            drop_pending_updates=True
        )

        logger.info(
            "Webhook deleted."
        )

        await application.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
            poll_interval=0.5,
            timeout=15,
        )

        await application.start()

        # Start alert monitor only after Telegram is running.
        BACKGROUND_TASK = asyncio.create_task(
            alert_loop(application)
        )

        logger.info(
            "Crypto Analyzer started successfully."
        )

        # Keep process alive.
        stop_event = asyncio.Event()

        await stop_event.wait()

    finally:

        if BACKGROUND_TASK:

            BACKGROUND_TASK.cancel()

            try:
                await BACKGROUND_TASK
            except asyncio.CancelledError:
                pass

            BACKGROUND_TASK = None

        try:

            if (
                application.updater
                and application.updater.running
            ):
                await application.updater.stop()

        except Exception:

            logger.exception(
                "updater stop error"
            )

        try:

            if application.running:
                await application.stop()

        except Exception:

            logger.exception(
                "application stop error"
            )

        try:

            await application.shutdown()

        except Exception:

            logger.exception(
                "application shutdown error"
            )

        await close_http()


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            run()
        )

    except KeyboardInterrupt:

        logger.info(
            "Stopped by user."
        )

    except Exception:

        logger.exception(
            "Fatal startup error."
        )

        raise

این نسخه چند تغییر اساسی دارد: اتصال HTTP مشترک و Cache، دریافت همزمان داده‌های بازار و نمودار، بررسی همزمان چند ارز در سیگنال‌ها، جلوگیری از درخواست اضافه هنگام افزودن ارز، SQLite با WAL و قفل کنترل‌شده، و مانیتور واقعی هشدارها.

نکته مهم: چون این نسخه از "asyncio" و "aiohttp" استفاده می‌کند، "requirements.txt" فعلی باید حداقل این‌ها را داشته باشد:

python-telegram-bot>=21,<23
aiohttp>=3.9

بعد از قرار دادن فایل در GitHub، Railway باید با همان Start Command یعنی:

python main.py

اجرا شود.

قدم بعدی این است که همین "main.py" را جایگزین فایل فعلی GitHub کنی و Deploy جدید Railway را بگیری؛ بعد لاگ Railway را بفرست تا اگر خطایی در محیط واقعی وجود داشت، همان را دقیق برطرف کنیم.
