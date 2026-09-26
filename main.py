# -*- coding: utf-8 -*-
"""
Crypto / Global Gold / Iran 18K Gold Telegram Analyzer
Railway production build
Analysis + signals + notifications. No automatic trading.

Required:
    TELEGRAM_BOT_TOKEN

Recommended:
    ADMIN_IDS=123456789
    PAYMENT_CARD=6037...
    SUPPORT_USERNAME=@username

Optional:
    DB_PATH=/data/crypto_bot.db
    HTTP_TIMEOUT=20
    CACHE_SECONDS=60
    ALERT_INTERVAL_SECONDS=300

Start:
    python main.py

Data sources:
    Crypto       -> CoinGecko
    Global gold  -> Yahoo Finance XAUUSD=X, fallback GC=F
    Iran 18K     -> TGJU profile/geram18

IMPORTANT:
    Telegram bot must have only ONE running polling instance.
"""

import os
import re
import sqlite3
import asyncio
import logging
import time
import shutil
from datetime import datetime, timedelta, timezone

import aiohttp
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

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
HTTP_TIMEOUT = max(8, int(os.getenv("HTTP_TIMEOUT", "20")))
CACHE_SECONDS = max(15, int(os.getenv("CACHE_SECONDS", "60")))
ALERT_INTERVAL_SECONDS = max(
    60, int(os.getenv("ALERT_INTERVAL_SECONDS", "300"))
)

PAYMENT_CARD = os.getenv("PAYMENT_CARD", "ثبت نشده").strip()
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")

db_dir = os.path.dirname(DB_PATH)
if db_dir:
    os.makedirs(db_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | market-analyzer | %(message)s",
)
log = logging.getLogger("market-analyzer")

# ============================================================
# UI
# ============================================================

MAIN_MENU = ReplyKeyboardMarkup(
    [
        [KeyboardButton("➕ افزودن دارایی"), KeyboardButton("📋 واچ‌لیست")],
        [KeyboardButton("📊 تحلیل"), KeyboardButton("🚨 سیگنال‌ها")],
        [KeyboardButton("💳 خرید اشتراک"), KeyboardButton("👤 وضعیت اشتراک")],
        [KeyboardButton("🔔 هشدارها"), KeyboardButton("🪙 ارزهای بیشتر")],
        [KeyboardButton("📨 ارتباط با پشتیبان"), KeyboardButton("ℹ️ راهنما")],
        [KeyboardButton("👨‍💼 پنل مدیریت")],
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


def verify_storage():
    """Warn loudly when Railway Volume is not mounted.

    SQLite data can only survive a Railway redeploy when DB_PATH points to a
    persistent Volume. The bot never deletes/recreates the database itself.
    """
    if DB_PATH.startswith("/data/") and not os.path.ismount("/data"):
        log.warning(
            "PERSISTENCE WARNING: /data is not a mounted Railway Volume. "
            "User/subscription data may be lost on redeploy. Mount a Volume at /data."
        )


def backup_database():
    if not os.path.exists(DB_PATH):
        return None
    backup_dir = os.path.join(os.path.dirname(DB_PATH) or ".", "backups")
    try:
        os.makedirs(backup_dir, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        target = os.path.join(backup_dir, f"crypto_bot_{stamp}.db")
        shutil.copy2(DB_PATH, target)
        # Keep the newest 10 local backups. They live on the same persistent Volume.
        files = sorted(
            [os.path.join(backup_dir, x) for x in os.listdir(backup_dir) if x.endswith(".db")],
            key=lambda x: os.path.getmtime(x),
            reverse=True,
        )
        for old in files[10:]:
            try:
                os.remove(old)
            except OSError:
                pass
        return target
    except Exception:
        log.exception("Database backup failed")
        return None


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
            signal_key TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_sub_user_end
            ON subscriptions(user_id, end_at);

        CREATE INDEX IF NOT EXISTS idx_pay_status
            ON payment_requests(status);

        CREATE INDEX IF NOT EXISTS idx_watch_user
            ON watchlist(user_id);

        CREATE INDEX IF NOT EXISTS idx_alert_user_symbol
            ON alert_events(user_id, symbol, id);

        CREATE TABLE IF NOT EXISTS support_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            admin_id INTEGER,
            direction TEXT NOT NULL DEFAULT 'user_to_admin',
            message TEXT NOT NULL,
            telegram_message_id INTEGER,
            status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT NOT NULL,
            replied_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_support_user
            ON support_messages(user_id, id);
        CREATE INDEX IF NOT EXISTS idx_support_status
            ON support_messages(status, id);
        """
    )

    # Safe migration for old installations.
    cols = {
        r["name"] for r in conn.execute(
            "PRAGMA table_info(alert_events)"
        ).fetchall()
    }
    if "signal_key" not in cols:
        conn.execute(
            "ALTER TABLE alert_events "
            "ADD COLUMN signal_key TEXT NOT NULL DEFAULT ''"
        )

    # Legacy installations: keep every existing table and record.
    # Never DROP or recreate user/subscription/payment tables.
    conn.execute("PRAGMA user_version = 4")

    conn.commit()
    conn.close()


def ensure_user(tg_user):
    if not tg_user:
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
    user_id, plan, days, amount,
    source="manual", payment_request_id=None, reviewed_by=None
):
    start = datetime.now(timezone.utc)
    conn = db()
    try:
        if payment_request_id:
            request = conn.execute(
                "SELECT status FROM payment_requests WHERE id=?",
                (payment_request_id,),
            ).fetchone()
            if not request:
                raise RuntimeError("درخواست پرداخت پیدا نشد.")
            if request["status"] != "pending":
                raise RuntimeError("این درخواست قبلاً بررسی شده است.")

        old = conn.execute(
            """
            SELECT end_at FROM subscriptions
            WHERE user_id=? AND status='active' AND end_at>?
            ORDER BY datetime(end_at) DESC, id DESC LIMIT 1
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
                user_id, plan, days, amount, start_at, end_at,
                status, source, payment_request_id, created_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                user_id, plan, days, amount,
                start.isoformat(), end.isoformat(),
                "active", source, payment_request_id, now_iso(),
            ),
        )

        if payment_request_id:
            cur = conn.execute(
                """
                UPDATE payment_requests
                SET status='approved', reviewed_at=?, reviewed_by=?
                WHERE id=? AND status='pending'
                """,
                (now_iso(), reviewed_by or 0, payment_request_id),
            )
            if cur.rowcount != 1:
                raise RuntimeError("درخواست پرداخت قبلاً بررسی شده است.")

        conn.commit()
        return end
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ============================================================
# HTTP / CACHE
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
        _http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Android; Mobile) "
                    "AppleWebKit/537.36 "
                    "Chrome/130 Safari/537.36"
                ),
                "Accept": "application/json,text/html,*/*",
            },
        )
    return _http_session


async def http_text(url, params=None):
    session = await get_http_session()
    last = None
    for attempt in range(3):
        try:
            async with session.get(url, params=params) as r:
                text = await r.text()
                if r.status != 200:
                    raise RuntimeError(f"HTTP {r.status}: {text[:180]}")
                return text
        except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as exc:
            last = exc
            if attempt < 2:
                await asyncio.sleep(0.8 * (attempt + 1))
    raise RuntimeError(str(last))


async def http_json(url, params=None):
    text = await http_text(url, params=params)
    try:
        import json
        return json.loads(text)
    except Exception as exc:
        raise RuntimeError("پاسخ سرویس JSON معتبر نیست.") from exc


def cache_get(key):
    item = _cache.get(key)
    if item and time.time() - item[0] < CACHE_SECONDS:
        return item[1]
    return None


def cache_put(key, value):
    _cache[key] = (time.time(), value)


# ============================================================
# ASSET HELPERS
# ============================================================

def clean_text(s):
    return re.sub(r"\s+", " ", str(s or "")).strip()


def normalize_crypto_symbol(raw):
    s = re.sub(r"[^A-Za-z0-9]", "", raw or "").upper()
    if s.endswith("USDT"):
        s = s[:-4]
    return s


def asset_label(asset_type, symbol):
    if asset_type == "crypto":
        return f"🪙 {symbol}"
    if asset_type == "gold_global":
        return "🥇 طلای جهانی XAU/USD"
    if asset_type == "gold18":
        return "🇮🇷 طلای ۱۸ عیار"
    return symbol


def parse_asset(text):
    raw = clean_text(text)
    low = raw.lower()

    if low in {
        "xau", "xauusd", "xau/usd", "gold",
        "طلای جهانی", "طلای جهانی xau"
    }:
        return "gold_global", "XAUUSD"

    if low in {
        "gold18", "geram18", "geram 18", "18k",
        "طلای ۱۸", "طلای ۱۸ عیار", "طلای داخلی"
    }:
        return "gold18", "GERAM18"

    symbol = normalize_crypto_symbol(raw)
    if re.fullmatch(r"[A-Z0-9]{2,20}", symbol):
        return "crypto", symbol

    return None, None


# ============================================================
# CRYPTO - COINGECKO
# ============================================================

CRYPTO_ALIASES = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "ZEC": "zcash",
    "SOL": "solana",
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
    "MKR": "maker", "AAVE": "aave", "CRV": "curve-dao-token",
    "MATIC": "matic-network", "POL": "polygon-ecosystem-token",
    "PEPE": "pepe", "SHIB": "shiba-inu", "WIF": "dogwifhat",
    "BONK": "bonk", "FLOKI": "floki", "SEI": "sei-network",
    "INJ": "injective-protocol", "TIA": "celestia", "IMX": "immutable-x",
    "RUNE": "thorchain", "EGLD": "elrond-erd-2", "ALGO": "algorand",
    "VET": "vechain", "ICP": "internet-computer", "HBAR": "hedera-hashgraph",
    "XLM": "stellar", "XMR": "monero", "EOS": "eos",
    "XTZ": "tezos", "MANA": "decentraland", "SAND": "the-sandbox",
    "AXS": "axie-infinity", "GRT": "the-graph", "THETA": "theta-token",
    "FLOW": "flow", "QNT": "quant-network", "KAS": "kaspa",
    "JASMY": "jasmycoin", "LDO": "lido-staked-ether", "STX": "stacks",
    "FET": "fetch-ai", "RENDER": "render-token", "RNDR": "render-token",
    "TAO": "bittensor", "AR": "arweave", "KAVA": "kava",
    "GALA": "gala", "APE": "apecoin", "CHZ": "chiliz",
    "ENJ": "enjincoin", "SNX": "havven", "COMP": "compound-governance-token",
    "SUSHI": "sushi", "1INCH": "1inch", "BAT": "basic-attention-token",
    "ZIL": "zilliqa", "ONE": "harmony", "IOTA": "iota",
    "MINA": "mina-protocol", "ROSE": "oasis-network", "CELO": "celo",
    "WLD": "worldcoin-wld", "STRK": "starknet", "JUP": "jupiter-exchange-solana",
    "PYTH": "pyth-network", "ONDO": "ondo-finance", "ENA": "ethena",
    "NOT": "notcoin", "DOGS": "dogs-2", "EIGEN": "eigenlayer",
}

POPULAR_CRYPTO_TEXT = (
    "🪙 ارزهای قابل استفاده\n\n"
    "BTC  ETH  ZEC  SOL  XRP  BNB  DOGE  ADA\n"
    "TRX  TON  DOT  AVAX  LINK  LTC  BCH  ETC\n"
    "ATOM  NEAR  UNI  APT  SUI  FIL  ARB  OP\n"
    "MKR  AAVE  CRV  MATIC  POL  PEPE  SHIB  WIF\n"
    "BONK  FLOKI  SEI  INJ  TIA  IMX  RUNE  ALGO\n"
    "VET  ICP  HBAR  XLM  XMR  EOS  XTZ  MANA\n"
    "SAND  AXS  GRT  THETA  FLOW  QNT  KAS  LDO\n"
    "STX  FET  RENDER  TAO  AR  GALA  APE  CHZ\n\n"
    "یا تقریباً هر نماد دیگری را ارسال کنید؛ ربات آن را در CoinGecko جست‌وجو می‌کند.\n"
    "مثال: LINK یا RENDER"
)


async def resolve_coingecko_id(symbol):
    symbol = normalize_crypto_symbol(symbol)
    if symbol in CRYPTO_ALIASES:
        return CRYPTO_ALIASES[symbol]

    key = ("cg_search", symbol)
    cached = cache_get(key)
    if cached:
        return cached

    data = await http_json(
        "https://api.coingecko.com/api/v3/search",
        {"query": symbol},
    )
    coins = data.get("coins") or []

    exact = []
    for c in coins:
        cs = str(c.get("symbol", "")).upper()
        if cs == symbol:
            exact.append(c)

    candidates = exact or coins
    if not candidates:
        raise RuntimeError(f"ارز {symbol} در CoinGecko پیدا نشد.")

    # Prefer exact ticker with the highest market cap rank.
    candidates.sort(
        key=lambda x: (
            x.get("market_cap_rank")
            if isinstance(x.get("market_cap_rank"), int)
            else 10**9
        )
    )
    coin_id = candidates[0].get("id")
    if not coin_id:
        raise RuntimeError(f"شناسه {symbol} در CoinGecko معتبر نیست.")

    cache_put(key, coin_id)
    return coin_id


async def crypto_data(symbol):
    symbol = normalize_crypto_symbol(symbol)
    coin_id = await resolve_coingecko_id(symbol)

    key = ("crypto_data", coin_id)
    cached = cache_get(key)
    if cached is not None:
        return cached.copy()

    data = await http_json(
        f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart",
        {
            "vs_currency": "usd",
            "days": "2",
            "interval": "hourly",
        },
    )

    prices = data.get("prices") or []
    if len(prices) < 30:
        raise RuntimeError("داده کافی از CoinGecko دریافت نشد.")

    times = []
    closes = []
    for item in prices:
        if len(item) >= 2:
            times.append(
                pd.to_datetime(int(item[0]), unit="ms", utc=True)
            )
            closes.append(float(item[1]))

    if len(closes) < 30:
        raise RuntimeError("داده معتبر کریپتو کافی نیست.")

    df = pd.DataFrame(
        {
            "time": times,
            "open": closes,
            "high": closes,
            "low": closes,
            "close": closes,
            "volume": np.nan,
        }
    )

    cache_put(key, df.copy())
    return df


# ============================================================
# GLOBAL GOLD - YAHOO FINANCE
# ============================================================

async def yahoo_chart(symbol):
    return await http_json(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
        {
            "range": "5d",
            "interval": "1h",
            "includePrePost": "true",
        },
    )


async def global_gold_data():
    key = ("gold_global",)
    cached = cache_get(key)
    if cached is not None:
        return cached.copy()

    errors = []
    for ticker in ("XAUUSD=X", "GC=F"):
        try:
            data = await yahoo_chart(ticker)
            result = (data.get("chart") or {}).get("result") or []
            if not result:
                raise RuntimeError("داده Yahoo خالی است.")

            r = result[0]
            timestamps = r.get("timestamp") or []
            quote = (r.get("indicators") or {}).get("quote") or []
            if not quote:
                raise RuntimeError("داده قیمت Yahoo موجود نیست.")

            q = quote[0]
            rows = []
            for i, ts in enumerate(timestamps):
                close = q.get("close", [])[i]
                op = q.get("open", [])[i]
                hi = q.get("high", [])[i]
                lo = q.get("low", [])[i]
                vol = q.get("volume", [])[i]
                if close is None:
                    continue
                rows.append(
                    {
                        "time": pd.to_datetime(
                            int(ts), unit="s", utc=True
                        ),
                        "open": float(op if op is not None else close),
                        "high": float(hi if hi is not None else close),
                        "low": float(lo if lo is not None else close),
                        "close": float(close),
                        "volume": float(vol or 0),
                    }
                )

            df = pd.DataFrame(rows)
            if len(df) < 30:
                raise RuntimeError("داده کافی طلای جهانی دریافت نشد.")

            # XAUUSD=X is spot-like. GC=F is futures fallback.
            cache_put(key, df.copy())
            return df
        except Exception as exc:
            errors.append(f"{ticker}: {exc}")

    raise RuntimeError("طلای جهانی در دسترس نیست | " + " | ".join(errors))


# ============================================================
# IRAN 18K GOLD - TGJU
# ============================================================

TGJU_URL = "https://www.tgju.org/profile/geram18"


def persian_to_english_numbers(value):
    table = str.maketrans(
        "۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩",
        "01234567890123456789",
    )
    return str(value).translate(table)


def parse_price(value):
    s = persian_to_english_numbers(value)
    s = s.replace(",", "").replace("٬", "").replace(" ", "")
    nums = re.findall(r"\d+(?:\.\d+)?", s)
    if not nums:
        return None
    try:
        return float(nums[0])
    except Exception:
        return None


def extract_tgju_gold18(html):
    # First try known TGJU selectors.
    soup = BeautifulSoup(html, "html.parser")
    selectors = [
        "#l-geram18",
        "#geram18",
        '[data-field="geram18"]',
        ".geram18",
    ]

    candidates = []
    for selector in selectors:
        for node in soup.select(selector):
            txt = clean_text(node.get_text(" ", strip=True))
            if txt:
                candidates.append(txt)

    # Also inspect elements carrying geram18 in attributes.
    for node in soup.find_all(True):
        attrs = " ".join(
            str(v) for v in node.attrs.values()
        )
        if "geram18" in attrs.lower():
            txt = clean_text(node.get_text(" ", strip=True))
            if txt:
                candidates.append(txt)

    # Prefer values in the normal Iranian 18K range.
    for text in candidates:
        value = parse_price(text)
        if value is None:
            continue
        if 100_000 <= value <= 1_000_000_000:
            return value

    # Last fallback: search the page text around "گرم 18".
    body = clean_text(soup.get_text(" ", strip=True))
    body = persian_to_english_numbers(body)
    patterns = [
        r"گرم\s*طلا\s*18[^\d]{0,80}([\d,]+)",
        r"طلای\s*18[^\d]{0,80}([\d,]+)",
        r"geram18[^\d]{0,80}([\d,]+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, body, re.I)
        if m:
            value = parse_price(m.group(1))
            if value and 100_000 <= value <= 1_000_000_000:
                return value

    raise RuntimeError("قیمت طلای ۱۸ عیار از TGJU استخراج نشد.")


async def iran_gold18_data():
    key = ("gold18",)
    cached = cache_get(key)
    if cached is not None:
        return cached.copy()

    html = await http_text(TGJU_URL)

    current = extract_tgju_gold18(html)

    # TGJU current page does not always expose a clean intraday candle
    # series. We keep a local price history so analysis is based on
    # actual repeated live observations.
    conn = db()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gold18_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            price REAL NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "INSERT INTO gold18_history(price, created_at) VALUES(?,?)",
        (current, now_iso()),
    )

    # Keep database bounded.
    conn.execute(
        """
        DELETE FROM gold18_history
        WHERE id NOT IN (
            SELECT id FROM gold18_history
            ORDER BY id DESC LIMIT 1000
        )
        """
    )
    rows = conn.execute(
        """
        SELECT id, price, created_at
        FROM gold18_history
        ORDER BY id ASC
        LIMIT 500
        """
    ).fetchall()
    conn.commit()
    conn.close()

    if len(rows) < 30:
        # Return repeated current price until local history builds.
        # Analysis will remain neutral because changes are zero.
        prices = [current] * 30
        times = [
            pd.Timestamp.now(tz="UTC") - pd.Timedelta(minutes=5 * i)
            for i in range(29, -1, -1)
        ]
    else:
        prices = [float(r["price"]) for r in rows]
        times = [
            pd.to_datetime(r["created_at"], utc=True)
            for r in rows
        ]

    df = pd.DataFrame(
        {
            "time": times,
            "open": prices,
            "high": prices,
            "low": prices,
            "close": prices,
            "volume": np.nan,
        }
    )

    cache_put(key, df.copy())
    return df


# ============================================================
# DATA ROUTER
# ============================================================

async def get_data(asset_type, symbol):
    if asset_type == "crypto":
        return await crypto_data(symbol)
    if asset_type == "gold_global":
        return await global_gold_data()
    if asset_type == "gold18":
        return await iran_gold18_data()
    raise RuntimeError("نوع دارایی نامعتبر است.")


# ============================================================
# ANALYSIS
# ============================================================

def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).ewm(
        alpha=1 / period, adjust=False
    ).mean()
    loss = (-delta.clip(upper=0)).ewm(
        alpha=1 / period, adjust=False
    ).mean()
    rs = gain / loss.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.replace([np.inf, -np.inf], np.nan).fillna(50)


def calculate(df):
    if df is None or len(df) < 30:
        raise RuntimeError("حداقل ۳۰ داده برای تحلیل لازم است.")

    d = df.copy()
    close = pd.to_numeric(d["close"], errors="coerce")

    d["ema9"] = close.ewm(span=9, adjust=False).mean()
    d["ema21"] = close.ewm(span=21, adjust=False).mean()
    d["rsi"] = rsi(close)
    d["ret6"] = close.pct_change(6) * 100
    d["ret24"] = close.pct_change(24) * 100

    volume = pd.to_numeric(d["volume"], errors="coerce")
    d["vol_ma"] = volume.rolling(20).mean()

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

    if (
        pd.notna(last["volume"])
        and pd.notna(last["vol_ma"])
        and last["vol_ma"] > 0
    ):
        if last["volume"] > last["vol_ma"]:
            score += 5 if score >= 0 else -5
            reasons.append("حجم بالاتر از میانگین")

    score = float(np.clip(score, -80, 80))

    strength = float(np.clip(50 + abs(score) * 2, 0, 100))
    probability = float(np.clip(50 + score * 0.8, 5, 95))

    if score >= 25:
        signal = "🟢 خرید / صعودی"
        signal_key = "BUY"
    elif score <= -25:
        signal = "🔴 فروش / نزولی"
        signal_key = "SELL"
    else:
        signal = "🟡 انتظار / خنثی"
        signal_key = "WAIT"

    look = d.tail(min(48, len(d)))
    support = float(pd.to_numeric(look["low"]).min())
    resistance = float(pd.to_numeric(look["high"]).max())

    return {
        "price": float(last["close"]),
        "rsi": float(last["rsi"]),
        "ema9": float(last["ema9"]),
        "ema21": float(last["ema21"]),
        "change6": float(last["ret6"]) if pd.notna(last["ret6"]) else 0.0,
        "change24": float(last["ret24"]) if pd.notna(last["ret24"]) else 0.0,
        "score": score,
        "strength": strength,
        "probability": probability,
        "signal": signal,
        "signal_key": signal_key,
        "support": support,
        "resistance": resistance,
        "reasons": reasons,
    }


async def analyze(asset_type, symbol):
    return calculate(await get_data(asset_type, symbol))


def fmt_num(value):
    value = float(value)
    if value >= 1000:
        return f"{value:,.2f}"
    if value >= 1:
        return f"{value:,.4f}"
    return f"{value:,.8f}"


def price_suffix(asset_type):
    if asset_type == "gold18":
        return " تومان"
    return " USD"


def analysis_text(asset_type, symbol, a):
    title = asset_label(asset_type, symbol)
    reasons = "\n".join(f"• {x}" for x in a["reasons"])

    return (
        f"📊 تحلیل هوشمند\n{title}\n\n"
        f"💰 قیمت: {fmt_num(a['price'])}{price_suffix(asset_type)}\n"
        f"📌 سیگنال: {a['signal']}\n\n"
        f"💪 درصد قدرت: {a['strength']:.0f}%\n"
        f"🎯 احتمال سود: {a['probability']:.0f}%\n\n"
        f"RSI(14): {a['rsi']:.1f}\n"
        f"EMA9: {fmt_num(a['ema9'])}\n"
        f"EMA21: {fmt_num(a['ema21'])}\n"
        f"تغییر 6 دوره: {a['change6']:+.2f}%\n"
        f"تغییر 24 دوره: {a['change24']:+.2f}%\n\n"
        f"🟢 حمایت: {fmt_num(a['support'])}\n"
        f"🔴 مقاومت: {fmt_num(a['resistance'])}\n\n"
        f"🔎 عوامل:\n{reasons}\n\n"
        "⚠️ تحلیل و سیگنال تضمین سود نیست و معامله خودکار انجام نمی‌شود."
    )


# ============================================================
# WATCHLIST
# ============================================================

def watchlist_rows(user_id):
    conn = db()
    rows = conn.execute(
        """
        SELECT symbol, asset_type, created_at
        FROM watchlist
        WHERE user_id=?
        ORDER BY asset_type, symbol
        """,
        (user_id,),
    ).fetchall()
    conn.close()
    return rows


def add_watch(user_id, asset_type, symbol):
    conn = db()
    conn.execute(
        """
        INSERT OR IGNORE INTO watchlist(
            user_id, symbol, asset_type, created_at
        )
        VALUES(?,?,?,?)
        """,
        (user_id, symbol, asset_type, now_iso()),
    )
    conn.commit()
    conn.close()


def remove_watch(user_id, symbol, asset_type):
    conn = db()
    conn.execute(
        """
        DELETE FROM watchlist
        WHERE user_id=? AND symbol=? AND asset_type=?
        """,
        (user_id, symbol, asset_type),
    )
    conn.commit()
    conn.close()


# ============================================================
# SAFE SEND
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
        log.exception("send_message failed chat_id=%s", chat_id)
        return False


# ============================================================
# BASIC
# ============================================================

async def start(update, context):
    ensure_user(update.effective_user)
    await update.message.reply_text(
        "🤖 ربات تحلیل هوشمند بازار\n\n"
        "🪙 ارزهای دیجیتال\n"
        "🥇 طلای جهانی\n"
        "🇮🇷 طلای ۱۸ عیار ایران\n\n"
        "از منوی پایین استفاده کنید.",
        reply_markup=MAIN_MENU,
    )


async def myid(update, context):
    ensure_user(update.effective_user)
    await update.message.reply_text(
        f"🆔 شناسه شما:\n{update.effective_user.id}"
    )


# ============================================================
# SUBSCRIPTIONS
# ============================================================

async def subscription_status(update, context):
    ensure_user(update.effective_user)
    sub = active_subscription(update.effective_user.id)

    if not sub:
        await update.message.reply_text(
            "👤 وضعیت اشتراک\n\n"
            "❌ اشتراک فعال ندارید."
        )
        return

    try:
        end = datetime.fromisoformat(sub["end_at"]).astimezone(timezone.utc)
        end_text = end.strftime("%Y-%m-%d %H:%M")
    except Exception:
        end_text = sub["end_at"]

    await update.message.reply_text(
        "👤 وضعیت اشتراک\n\n"
        "✅ فعال\n"
        f"📦 طرح: {sub['plan']}\n"
        f"📅 پایان: {end_text} UTC"
    )


async def buy_menu(update, context):
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(
                "۳۰ روز — ۲۰۰,۰۰۰ تومان",
                callback_data="plan:30"
            )],
            [InlineKeyboardButton(
                "۹۰ روز — ۳۵۰,۰۰۰ تومان",
                callback_data="plan:90"
            )],
            [InlineKeyboardButton(
                "۱۸۰ روز — ۵۰۰,۰۰۰ تومان",
                callback_data="plan:180"
            )],
        ]
    )
    await update.message.reply_text(
        "💳 انتخاب اشتراک:",
        reply_markup=keyboard,
    )


async def plan_callback(update, context):
    query = update.callback_query
    await query.answer()

    days = int(query.data.split(":")[1])
    amounts = {30: 200000, 90: 350000, 180: 500000}
    amount = amounts[days]
    context.user_data["pending_plan"] = (days, amount)

    await query.message.reply_text(
        f"💳 اشتراک {days} روزه\n"
        f"مبلغ: {amount:,} تومان\n\n"
        f"شماره کارت:\n{PAYMENT_CARD}\n\n"
        "بعد از پرداخت، تصویر رسید را همین‌جا ارسال کنید."
    )


async def receipt(update, context):
    ensure_user(update.effective_user)
    plan = context.user_data.get("pending_plan")

    if not plan:
        await update.message.reply_text(
            "ابتدا از «💳 خرید اشتراک» طرح را انتخاب کنید."
        )
        return

    if not update.message.photo:
        return

    days, amount = plan
    file_id = update.message.photo[-1].file_id

    conn = db()
    cur = conn.execute(
        """
        INSERT INTO payment_requests(
            user_id, plan, days, amount, receipt_file_id,
            status, created_at
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
                    [[
                        InlineKeyboardButton(
                            "✅ تأیید",
                            callback_data=f"payok:{request_id}"
                        ),
                        InlineKeyboardButton(
                            "❌ رد",
                            callback_data=f"payno:{request_id}"
                        ),
                    ]]
                ),
            )
        except Exception:
            log.exception("admin receipt notify failed")


async def payment_callback(update, context):
    query = update.callback_query
    await query.answer()

    if not is_admin(query.from_user.id):
        await query.message.reply_text("⛔ دسترسی ندارید.")
        return

    action, value = query.data.split(":", 1)
    request_id = int(value)

    conn = db()
    request = conn.execute(
        "SELECT * FROM payment_requests WHERE id=?",
        (request_id,),
    ).fetchone()

    if not request:
        conn.close()
        await query.message.reply_text("❌ درخواست پیدا نشد.")
        return

    if request["status"] != "pending":
        conn.close()
        await query.message.reply_text("ℹ️ قبلاً بررسی شده است.")
        return

    if action == "payno":
        conn.execute(
            """
            UPDATE payment_requests
            SET status='rejected', reviewed_at=?, reviewed_by=?
            WHERE id=? AND status='pending'
            """,
            (now_iso(), query.from_user.id, request_id),
        )
        conn.commit()
        conn.close()

        await query.message.reply_text(
            f"❌ پرداخت #{request_id} رد شد."
        )
        await safe_send(
            context.bot,
            request["user_id"],
            "❌ رسید پرداخت شما رد شد.",
        )
        return

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
        await query.message.reply_text(f"❌ فعال‌سازی نشد:\n{exc}")
        return

    await query.message.reply_text(
        f"✅ پرداخت #{request_id} تأیید شد.\n"
        f"پایان اشتراک: {end.strftime('%Y-%m-%d %H:%M')} UTC"
    )

    await safe_send(
        context.bot,
        request["user_id"],
        "✅ پرداخت تأیید شد و اشتراک شما فعال شد.",
    )


# ============================================================
# ADD / ANALYSIS
# ============================================================

async def add_asset_prompt(update, context):
    context.user_data["awaiting_asset"] = "add"
    await update.message.reply_text(
        "➕ دارایی را ارسال کنید.\n\n"
        "کریپتو:\n"
        "BTC یا ZEC یا SOL\n\n"
        "طلای جهانی:\n"
        "XAU یا XAUUSD\n\n"
        "طلای داخلی:\n"
        "طلای ۱۸ عیار یا GERAM18"
    )


async def analysis_prompt(update, context):
    if not has_analysis_access(update.effective_user.id):
        await update.message.reply_text(
            "🔒 برای تحلیل اشتراک فعال لازم است."
        )
        return

    context.user_data["awaiting_asset"] = "analysis"
    await update.message.reply_text(
        "📊 دارایی را ارسال کنید:\n"
        "BTC / ZEC / XAU / طلای ۱۸ عیار"
    )


async def signals(update, context):
    user_id = update.effective_user.id
    if not has_analysis_access(user_id):
        await update.message.reply_text(
            "🔒 برای سیگنال اشتراک فعال لازم است."
        )
        return

    rows = watchlist_rows(user_id)
    if not rows:
        await update.message.reply_text(
            "📋 واچ‌لیست خالی است."
        )
        return

    await update.message.reply_text("⏳ در حال تحلیل واچ‌لیست...")

    for row in rows:
        try:
            a = await analyze(row["asset_type"], row["symbol"])
            await update.message.reply_text(
                analysis_text(row["asset_type"], row["symbol"], a)
            )
        except Exception as exc:
            log.exception("manual analysis failed")
            await update.message.reply_text(
                f"❌ {asset_label(row['asset_type'], row['symbol'])}\n{exc}"
            )


async def watchlist(update, context):
    rows = watchlist_rows(update.effective_user.id)
    if not rows:
        await update.message.reply_text(
            "📋 واچ‌لیست شما خالی است."
        )
        return

    keyboard = []
    for row in rows:
        symbol = row["symbol"]
        atype = row["asset_type"]
        keyboard.append(
            [
                InlineKeyboardButton(
                    f"📊 {asset_label(atype, symbol)}",
                    callback_data=f"wl:a:{atype}:{symbol}",
                ),
                InlineKeyboardButton(
                    "🗑 حذف",
                    callback_data=f"wl:d:{atype}:{symbol}",
                ),
            ]
        )

    await update.message.reply_text(
        "📋 واچ‌لیست:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def watchlist_callback(update, context):
    query = update.callback_query
    await query.answer()

    parts = query.data.split(":", 3)
    if len(parts) != 4:
        return

    _, action, atype, symbol = parts

    if action == "d":
        remove_watch(query.from_user.id, symbol, atype)
        await query.message.reply_text(
            f"🗑 {asset_label(atype, symbol)} حذف شد."
        )
        return

    if action == "a":
        if not has_analysis_access(query.from_user.id):
            await query.message.reply_text(
                "🔒 اشتراک فعال لازم است."
            )
            return
        try:
            a = await analyze(atype, symbol)
            await query.message.reply_text(
                analysis_text(atype, symbol, a)
            )
        except Exception as exc:
            await query.message.reply_text(
                f"❌ تحلیل انجام نشد:\n{exc}"
            )


# ============================================================
# ALERTS
# ============================================================

async def alerts(update, context):
    conn = db()
    row = conn.execute(
        """
        SELECT enabled FROM alert_preferences
        WHERE user_id=?
        """,
        (update.effective_user.id,),
    ).fetchone()
    conn.close()

    enabled = bool(row["enabled"]) if row else False
    status = "🟢 فعال" if enabled else "🔴 غیرفعال"

    keyboard = InlineKeyboardMarkup(
        [[
            InlineKeyboardButton(
                "🟢 فعال",
                callback_data="alert:on"
            ),
            InlineKeyboardButton(
                "🔴 خاموش",
                callback_data="alert:off"
            ),
        ]]
    )

    await update.message.reply_text(
        "🔔 هشدارهای سیگنال\n\n"
        f"وضعیت: {status}\n"
        f"بررسی بازار: هر {ALERT_INTERVAL_SECONDS // 60} دقیقه\n\n"
        "وقتی سیگنال از وضعیت قبلی تغییر کند، "
        "نوتیفیکیشن برای شما ارسال می‌شود.",
        reply_markup=keyboard,
    )


async def alert_callback(update, context):
    query = update.callback_query
    await query.answer()

    enabled = 1 if query.data == "alert:on" else 0
    conn = db()
    conn.execute(
        """
        INSERT INTO alert_preferences(
            user_id, enabled, interval_seconds, last_check_at
        )
        VALUES(?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
            enabled=excluded.enabled,
            interval_seconds=excluded.interval_seconds,
            last_check_at=CASE
                WHEN excluded.enabled=0 THEN NULL
                ELSE alert_preferences.last_check_at
            END
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

    await query.message.reply_text(
        "🟢 هشدارها فعال شد."
        if enabled else
        "🔴 هشدارها خاموش شد."
    )


async def get_last_signal(user_id, symbol):
    conn = db()
    row = conn.execute(
        """
        SELECT signal_key, message
        FROM alert_events
        WHERE user_id=? AND symbol=?
        ORDER BY id DESC LIMIT 1
        """,
        (user_id, symbol),
    ).fetchone()
    conn.close()
    return row


async def alert_worker(application):
    log.info(
        "Alert worker started interval=%s",
        ALERT_INTERVAL_SECONDS,
    )

    while True:
        try:
            await asyncio.sleep(ALERT_INTERVAL_SECONDS)

            conn = db()
            users = conn.execute(
                """
                SELECT user_id FROM alert_preferences
                WHERE enabled=1
                """
            ).fetchall()
            conn.close()

            for u in users:
                user_id = u["user_id"]

                if not has_analysis_access(user_id):
                    continue

                rows = watchlist_rows(user_id)

                for item in rows:
                    atype = item["asset_type"]
                    symbol = item["symbol"]

                    try:
                        a = await analyze(atype, symbol)

                        # WAIT does not generate a notification by itself.
                        # A notification is sent on a real directional signal
                        # or when an existing directional signal changes.
                        if a["signal_key"] == "BUY":
                            direction = "🟢 سیگنال خرید / صعودی"
                        elif a["signal_key"] == "SELL":
                            direction = "🔴 سیگنال فروش / نزولی"
                        else:
                            continue

                        message = (
                            "🚨 سیگنال جدید\n\n"
                            f"{asset_label(atype, symbol)}\n\n"
                            f"{direction}\n\n"
                            f"💰 قیمت: {fmt_num(a['price'])}"
                            f"{price_suffix(atype)}\n"
                            f"💪 درصد قدرت: {a['strength']:.0f}%\n"
                            f"🎯 احتمال سود: {a['probability']:.0f}%\n\n"
                            f"RSI: {a['rsi']:.1f}\n"
                            f"EMA9: {fmt_num(a['ema9'])}\n"
                            f"EMA21: {fmt_num(a['ema21'])}\n"
                            f"🟢 حمایت: {fmt_num(a['support'])}\n"
                            f"🔴 مقاومت: {fmt_num(a['resistance'])}\n\n"
                            "⚠️ این سیگنال تضمین سود نیست."
                        )

                        previous = await get_last_signal(
                            user_id, symbol
                        )

                        # Same directional state -> no duplicate.
                        if previous and previous["signal_key"] == a["signal_key"]:
                            continue

                        sent = await safe_send(
                            application.bot,
                            user_id,
                            message,
                        )

                        if sent:
                            conn = db()
                            conn.execute(
                                """
                                INSERT INTO alert_events(
                                    user_id, symbol, signal_key,
                                    message, created_at
                                )
                                VALUES(?,?,?,?,?)
                                """,
                                (
                                    user_id,
                                    symbol,
                                    a["signal_key"],
                                    message,
                                    now_iso(),
                                ),
                            )
                            conn.execute(
                                """
                                UPDATE alert_preferences
                                SET last_check_at=?
                                WHERE user_id=?
                                """,
                                (now_iso(), user_id),
                            )
                            conn.commit()
                            conn.close()

                    except Exception:
                        log.exception(
                            "Alert failed user=%s type=%s symbol=%s",
                            user_id, atype, symbol,
                        )

        except asyncio.CancelledError:
            log.info("Alert worker stopped")
            raise
        except Exception:
            log.exception("Alert worker cycle failed")
            await asyncio.sleep(10)


# ============================================================
# USER SUPPORT
# ============================================================

async def support_prompt(update, context):
    context.user_data["support_mode"] = True
    await update.message.reply_text(
        "📨 ارتباط با پشتیبان\n\n"
        "پیام خود را ارسال کنید. پیام مستقیماً برای مدیران ربات ارسال می‌شود.\n"
        "برای خروج، «لغو» را بفرستید."
    )


async def forward_support_message(update, context):
    ensure_user(update.effective_user)
    text = clean_text(update.message.text)
    if text == "لغو":
        context.user_data.pop("support_mode", None)
        await update.message.reply_text("لغو شد.", reply_markup=MAIN_MENU)
        return True

    conn = db()
    cur = conn.execute(
        """INSERT INTO support_messages(
            user_id, direction, message, telegram_message_id, status, created_at
        ) VALUES(?,?,?,?,?,?)""",
        (update.effective_user.id, "user_to_admin", text,
         update.message.message_id, "open", now_iso()),
    )
    ticket_id = cur.lastrowid
    conn.commit()
    conn.close()
    context.user_data.pop("support_mode", None)

    user = update.effective_user
    caption = (
        f"📨 پیام پشتیبانی #{ticket_id}\n"
        f"👤 {user.first_name or ''} @{user.username or '-'}\n"
        f"🆔 {user.id}\n\n{text}"
    )
    for admin_id in ADMIN_IDS:
        await safe_send(
            context.bot, admin_id, caption,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                "↩️ پاسخ به کاربر", callback_data=f"support:reply:{user.id}:{ticket_id}"
            )]])
        )
    await update.message.reply_text(
        "✅ پیام شما برای پشتیبان ارسال شد.\n"
        f"کد پیگیری: #{ticket_id}"
    )
    return True


async def support_reply_callback(update, context):
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("⛔ دسترسی ندارید.")
        return
    _, _, user_id, ticket_id = query.data.split(":", 3)
    context.user_data["admin_reply_to"] = (int(user_id), int(ticket_id))
    await query.message.reply_text(
        f"✍️ پاسخ به کاربر {user_id}\n"
        "متن پاسخ را ارسال کنید."
    )


async def send_admin_reply(update, context):
    target = context.user_data.get("admin_reply_to")
    if not target:
        return False
    user_id, ticket_id = target
    text = clean_text(update.message.text)
    if not text:
        return True
    sent = await safe_send(context.bot, user_id, "📩 پاسخ پشتیبانی:\n\n" + text)
    conn = db()
    conn.execute(
        """INSERT INTO support_messages(
            user_id, admin_id, direction, message, status, created_at, replied_at
        ) VALUES(?,?,?,?,?,?,?)""",
        (user_id, update.effective_user.id, "admin_to_user", text,
         "closed" if sent else "open", now_iso(), now_iso() if sent else None),
    )
    conn.execute(
        "UPDATE support_messages SET status='closed', replied_at=? WHERE id=?",
        (now_iso(), ticket_id),
    )
    conn.commit()
    conn.close()
    context.user_data.pop("admin_reply_to", None)
    await update.message.reply_text(
        "✅ پاسخ ارسال شد." if sent else "❌ ارسال پاسخ ناموفق بود. احتمالاً کاربر ربات را بلاک کرده است."
    )
    return True


# ============================================================
# ADMIN PANEL
# ============================================================

async def admin_panel(update, context):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی ندارید.")
        return
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 آمار کامل", callback_data="adm:stats"),
         InlineKeyboardButton("👥 کاربران", callback_data="adm:users:0")],
        [InlineKeyboardButton("💳 پرداخت‌ها", callback_data="adm:payments"),
         InlineKeyboardButton("📨 پیام‌ها", callback_data="adm:support")],
        [InlineKeyboardButton("📢 ارسال همگانی", callback_data="adm:broadcast"),
         InlineKeyboardButton("👤 پیام به کاربر", callback_data="adm:message")],
        [InlineKeyboardButton("🚫 مسدود/رفع مسدودی", callback_data="adm:block")],
    ])
    await update.message.reply_text("👨‍💼 پنل مدیریت حرفه‌ای", reply_markup=keyboard)


async def admin_callback(update, context):
    query = update.callback_query
    await query.answer()
    if not is_admin(query.from_user.id):
        await query.message.reply_text("⛔ دسترسی ندارید.")
        return
    data = query.data
    if data == "adm:stats":
        conn=db()
        vals={
            "users": conn.execute("SELECT COUNT(*) n FROM users").fetchone()["n"],
            "active": conn.execute("SELECT COUNT(*) n FROM subscriptions WHERE status='active' AND end_at>?",(now_iso(),)).fetchone()["n"],
            "pending": conn.execute("SELECT COUNT(*) n FROM payment_requests WHERE status='pending'").fetchone()["n"],
            "watch": conn.execute("SELECT COUNT(*) n FROM watchlist").fetchone()["n"],
            "alerts": conn.execute("SELECT COUNT(*) n FROM alert_preferences WHERE enabled=1").fetchone()["n"],
            "open": conn.execute("SELECT COUNT(*) n FROM support_messages WHERE status='open'").fetchone()["n"],
        }
        conn.close()
        await query.message.reply_text(
            "📊 آمار کامل\n\n"
            f"👥 کاربران: {vals['users']}\n"
            f"🟢 اشتراک فعال: {vals['active']}\n"
            f"💳 پرداخت در انتظار: {vals['pending']}\n"
            f"📋 آیتم‌های واچ‌لیست: {vals['watch']}\n"
            f"🔔 هشدار فعال: {vals['alerts']}\n"
            f"📨 پیام باز: {vals['open']}"
        )
        return
    if data.startswith("adm:users:"):
        page=int(data.rsplit(":",1)[1]); per=10; offset=page*per
        conn=db(); rows=conn.execute("SELECT user_id,first_name,username,last_seen,blocked FROM users ORDER BY last_seen DESC LIMIT ? OFFSET ?",(per,offset)).fetchall(); total=conn.execute("SELECT COUNT(*) n FROM users").fetchone()["n"]; conn.close()
        if not rows:
            await query.message.reply_text("کاربری در این صفحه نیست."); return
        lines=["👥 کاربران:\n"]
        for r in rows:
            lines.append(f"🆔 {r['user_id']} | {r['first_name'] or '-'} | @{r['username'] or '-'} | {'🚫' if r['blocked'] else '🟢'}")
        buttons=[]
        if page>0: buttons.append(InlineKeyboardButton("⬅️ قبلی",callback_data=f"adm:users:{page-1}"))
        if offset+per<total: buttons.append(InlineKeyboardButton("بعدی ➡️",callback_data=f"adm:users:{page+1}"))
        await query.message.reply_text("\n".join(lines), reply_markup=InlineKeyboardMarkup([buttons] if buttons else [])); return
    if data == "adm:payments":
        conn=db(); rows=conn.execute("SELECT id,user_id,days,amount,created_at FROM payment_requests WHERE status='pending' ORDER BY id DESC LIMIT 20").fetchall(); conn.close()
        if not rows: await query.message.reply_text("💳 پرداخت در انتظار وجود ندارد."); return
        await query.message.reply_text("💳 پرداخت‌های در انتظار:\n\n"+"\n".join(f"#{r['id']} | {r['user_id']} | {r['days']} روز | {r['amount']:,} تومان" for r in rows)); return
    if data == "adm:support":
        conn=db(); rows=conn.execute("SELECT id,user_id,message,created_at FROM support_messages WHERE direction='user_to_admin' AND status='open' ORDER BY id DESC LIMIT 15").fetchall(); conn.close()
        if not rows: await query.message.reply_text("📨 پیام باز ندارید."); return
        await query.message.reply_text("📨 پیام‌های باز:\n\n"+"\n\n".join(f"#{r['id']} | user={r['user_id']}\n{r['message'][:500]}" for r in rows)); return
    if data == "adm:broadcast":
        context.user_data["admin_mode"]="broadcast"
        await query.message.reply_text("📢 متن پیام همگانی را ارسال کنید. برای لغو: لغو")
        return
    if data == "adm:message":
        context.user_data["admin_mode"]="message"
        await query.message.reply_text("👤 ابتدا شناسه عددی کاربر را بفرستید.")
        return
    if data == "adm:block":
        context.user_data["admin_mode"]="block"
        await query.message.reply_text("🚫 شناسه کاربر را بفرستید؛ سپس وضعیت مسدودی تغییر می‌کند.")
        return


async def admin_text_action(update, context):
    if not is_admin(update.effective_user.id): return False
    mode=context.user_data.get("admin_mode")
    if not mode: return False
    text=clean_text(update.message.text)
    if text=="لغو": context.user_data.pop("admin_mode",None); await update.message.reply_text("لغو شد."); return True
    if mode in ("message","block"):
        if not text.isdigit(): await update.message.reply_text("❌ شناسه عددی معتبر بفرستید."); return True
        uid=int(text)
        if mode=="block":
            conn=db(); conn.execute("UPDATE users SET blocked=CASE WHEN blocked=0 THEN 1 ELSE 0 END WHERE user_id=?",(uid,)); row=conn.execute("SELECT blocked FROM users WHERE user_id=?",(uid,)).fetchone(); conn.commit(); conn.close()
            await update.message.reply_text("🚫 وضعیت کاربر: " + ("مسدود" if row and row["blocked"] else "فعال")); context.user_data.pop("admin_mode",None); return True
        context.user_data["admin_message_user"]=uid; context.user_data["admin_mode"]="message_text"; await update.message.reply_text("✍️ متن پیام را بفرستید."); return True
    if mode=="message_text":
        uid=context.user_data.get("admin_message_user"); sent=await safe_send(context.bot,uid,"📩 پیام مدیر:\n\n"+text); await update.message.reply_text("✅ ارسال شد." if sent else "❌ ارسال نشد."); context.user_data.pop("admin_mode",None); context.user_data.pop("admin_message_user",None); return True
    if mode=="broadcast":
        conn=db(); rows=conn.execute("SELECT user_id FROM users WHERE blocked=0").fetchall(); conn.close(); ok=0; fail=0
        for r in rows:
            if await safe_send(context.bot,r["user_id"],"📢 پیام مدیریت:\n\n"+text): ok+=1
            else: fail+=1
            await asyncio.sleep(0.04)
        context.user_data.pop("admin_mode",None); await update.message.reply_text(f"📢 ارسال تمام شد.\n✅ {ok}\n❌ {fail}"); return True
    return False


# ============================================================
# HELP / ADMIN
# ============================================================

async def crypto_list(update, context):
    await update.message.reply_text(POPULAR_CRYPTO_TEXT)


async def help_cmd(update, context):
    support = (
        f"\n📞 پشتیبانی: {SUPPORT_USERNAME}"
        if SUPPORT_USERNAME else ""
    )
    await update.message.reply_text(
        "ℹ️ راهنما\n\n"
        "🪙 کریپتو: CoinGecko\n"
        "🥇 طلای جهانی: XAU/USD\n"
        "🇮🇷 طلای ۱۸ عیار: TGJU\n\n"
        "➕ افزودن دارایی\n"
        "📋 مدیریت واچ‌لیست\n"
        "📊 تحلیل\n"
        "🚨 سیگنال‌ها\n"
        "🔔 نوتیفیکیشن سیگنال\n"
        "💳 خرید اشتراک\n\n"
        "ربات معامله خودکار انجام نمی‌دهد."
        + support
    )


async def admin(update, context):
    await admin_panel(update, context)


# ============================================================
# TEXT ROUTER
# ============================================================

async def text_router(update, context):
    ensure_user(update.effective_user)
    text = clean_text(update.message.text)

    if await admin_text_action(update, context):
        return
    if context.user_data.get("support_mode"):
        await forward_support_message(update, context)
        return
    if context.user_data.get("admin_reply_to"):
        await send_admin_reply(update, context)
        return

    mode = context.user_data.get("awaiting_asset")

    if mode in ("add", "analysis"):
        atype, symbol = parse_asset(text)

        if not atype:
            await update.message.reply_text(
                "❌ دارایی شناخته نشد.\n\n"
                "مثال: BTC / ZEC / XAU / طلای ۱۸ عیار"
            )
            return

        context.user_data.pop("awaiting_asset", None)

        if mode == "add":
            try:
                await get_data(atype, symbol)
            except Exception as exc:
                await update.message.reply_text(
                    f"❌ داده {asset_label(atype, symbol)} دریافت نشد.\n"
                    f"{exc}"
                )
                return

            add_watch(
                update.effective_user.id,
                atype,
                symbol,
            )
            await update.message.reply_text(
                f"✅ {asset_label(atype, symbol)} به واچ‌لیست اضافه شد."
            )
            return

        if not has_analysis_access(update.effective_user.id):
            await update.message.reply_text(
                "🔒 اشتراک فعال ندارید."
            )
            return

        try:
            await update.message.reply_text(
                "⏳ در حال دریافت داده و تحلیل..."
            )
            a = await analyze(atype, symbol)
            await update.message.reply_text(
                analysis_text(atype, symbol, a)
            )
        except Exception as exc:
            log.exception("analysis failed")
            await update.message.reply_text(
                f"❌ تحلیل انجام نشد.\n{exc}"
            )
        return

    handlers = {
        "➕ افزودن دارایی": add_asset_prompt,
        "➕ افزودن ارز": add_asset_prompt,
        "📋 واچ‌لیست": watchlist,
        "📊 تحلیل": analysis_prompt,
        "🚨 سیگنال‌ها": signals,
        "💳 خرید اشتراک": buy_menu,
        "👤 وضعیت اشتراک": subscription_status,
        "🔔 هشدارها": alerts,
        "🪙 ارزهای بیشتر": crypto_list,
        "📨 ارتباط با پشتیبان": support_prompt,
        "👨‍💼 پنل مدیریت": admin_panel,
        "ℹ️ راهنما": help_cmd,
    }

    fn = handlers.get(text)
    if fn:
        await fn(update, context)


# ============================================================
# ERRORS / SHUTDOWN
# ============================================================

async def error_handler(update, context):
    error = context.error
    if error:
        log.error(
            "Unhandled error: %s",
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
    verify_storage()
    init_db()
    backup_database()

    log.info("Starting Market Analyzer...")
    log.info("DB_PATH=%s", DB_PATH)
    log.info("ADMIN_IDS=%s", sorted(ADMIN_IDS))

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin))
    application.add_handler(CommandHandler("id", myid))

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
    application.add_handler(
        CallbackQueryHandler(
            watchlist_callback,
            pattern=r"^wl:",
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            alert_callback,
            pattern=r"^alert:(on|off)$",
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            admin_callback,
            pattern=r"^adm:",
        )
    )
    application.add_handler(
        CallbackQueryHandler(
            support_reply_callback,
            pattern=r"^support:reply:",
        )
    )

    application.add_handler(
        MessageHandler(filters.PHOTO, receipt)
    )

    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            text_router,
        )
    )

    application.add_error_handler(error_handler)

    alert_task = None

    try:
        await application.initialize()
        await application.start()

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

        log.info("Market Analyzer started successfully.")

        while True:
            await asyncio.sleep(3600)

    except asyncio.CancelledError:
        log.info("Main task cancelled")
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
            log.exception("Updater stop failed")

        try:
            if application.running:
                await application.stop()
        except Exception:
            log.exception("Application stop failed")

        try:
            await application.shutdown()
        except Exception:
            log.exception("Application shutdown failed")

        backup_database()
        await shutdown_http()
        log.info("Market Analyzer stopped.")


if __name__ == "__main__":
    asyncio.run(main())
