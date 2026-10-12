# -*- coding: utf-8 -*-
"""
Crypto / Global Gold / Iran 18K Gold Telegram Analyzer
Railway production build — REVISED v14
Analysis + signals + notifications. No automatic trading.

Sources:
- Crypto OHLCV : OKX spot (professional multi-timeframe engine)
- Crypto market: CoinGecko (universe scan, fallback close-only)
- Iran 18K Gold: TGJU / profile/geram18
- Global Gold  : TGJU / profile/ons

Required:
    TELEGRAM_BOT_TOKEN

Recommended Railway variables:
    ADMIN_IDS=123456789
    PAYMENT_CARD=6037...
    SUPPORT_USERNAME=@username
    DB_PATH=/data/crypto_bot.db

Start:
    python main.py

IMPORTANT:
- Attach a Railway Volume at /data.
- Keep only ONE running instance for this bot token.
- Never delete /data/crypto_bot.db.
- Database migrations are additive.

CHANGELOG v14 (keeps existing database schema):
- Added sourced closed-candle analysis on 5m, 15m, 1H, 4H, 1D, and 1W timeframes.
- Added timeframe-by-timeframe dashboard output and weekly trend veto for counter-trend entries.
- Signal confirmation buckets now support 5m and 1W.
- Historical crypto calculations use OKX OHLCV candles, not CoinGecko daily closes.

CHANGELOG v13 (keeps existing database schema):
- Fixed GOLD18 Rial/Toman ambiguity with strict value ranges.
- Fixed XAU parser to reject silver/platinum noise.
- Signal confirmation now bucket-based (per closed candle), not wall-minute.
- LRU+TTL cache with size cap (no memory leak).
- BTC market gate stricter (BUY or strength>=65).
- Alert cycle key wall-clock based (survives restarts).
- RSI gates for BUY/SELL symmetric and safer.
- Volume series properly aligned in fallback analysis.
- growth_scan unified on professional engine.
- Analysis labels clarified (candle vs day).
- Market scan default reduced to prevent long unresponsive scans; explicit maximum capped at 10 pages.
- Added CoinGecko market-cap, liquidity, rank, supply, and 24h/7d/30d context to crypto dashboards.
- Added bounded timeouts and clearer feedback for long-running analysis callbacks.
- Opportunity filter balanced; non-confirmed candidates are labeled for monitoring, never as BUY.
- Fixed stale-symbol price reuse when resetting whole-market alert confirmations.
- Growth scan sends complete per-asset messages instead of slicing HTML tags.
"""

import os
import re
import sqlite3
import shutil
from pathlib import Path
from collections import OrderedDict
import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from html import escape

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)

# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

ADMIN_IDS = set()
_admin_raw = os.getenv("ADMIN_IDS", "").strip()
if not _admin_raw:
    _admin_raw = os.getenv("ADMIN_ID", "").strip()
for x in _admin_raw.split(","):
    try:
        if x.strip():
            ADMIN_IDS.add(int(x.strip()))
    except ValueError:
        print(f"Invalid admin id ignored: {x!r}")

DB_PATH = os.getenv("DB_PATH", "/data/crypto_bot.db").strip()
BACKUP_DIR = os.getenv("BACKUP_DIR", "/data/backups").strip()
BACKUP_INTERVAL_SECONDS = max(900, int(os.getenv("BACKUP_INTERVAL_SECONDS", "21600")))
BACKUP_RETENTION = max(3, int(os.getenv("BACKUP_RETENTION", "14")))
AUTO_RESTORE_BACKUP = os.getenv("AUTO_RESTORE_BACKUP", "1").lower() not in {"0", "false", "no"}

HTTP_TIMEOUT = max(5, int(os.getenv("HTTP_TIMEOUT", "15")))
CACHE_SECONDS = max(10, int(os.getenv("CACHE_SECONDS", "45")))
PRICE_CACHE_SECONDS = max(10, int(os.getenv("PRICE_CACHE_SECONDS", "20")))
ANALYSIS_CACHE_SECONDS = max(15, int(os.getenv("ANALYSIS_CACHE_SECONDS", "45")))
ALERT_INTERVAL_SECONDS = max(60, int(os.getenv("ALERT_INTERVAL_SECONDS", "300")))
MAX_WATCHLIST = max(1, int(os.getenv("MAX_WATCHLIST", "100")))

GOLD_HISTORY_INTERVAL_SECONDS = max(60, int(os.getenv("GOLD_HISTORY_INTERVAL_SECONDS", "300")))
GOLD_MIN_HISTORY_POINTS = max(50, int(os.getenv("GOLD_MIN_HISTORY_POINTS", "50")))
SIGNAL_MIN_SCORE = max(70, min(100, int(os.getenv("SIGNAL_MIN_SCORE", "85"))))
ALERT_OPPORTUNITY_MIN = max(85, min(100, int(os.getenv("ALERT_OPPORTUNITY_MIN", "85"))))
ALERT_STRENGTH_MIN = max(72, min(100, int(os.getenv("ALERT_STRENGTH_MIN", "72"))))
ALERT_PROBABILITY_MIN = max(70, min(100, int(os.getenv("ALERT_PROBABILITY_MIN", "70"))))
ALERT_RSI_MIN = max(40, min(60, int(os.getenv("ALERT_RSI_MIN", "48"))))
ALERT_RSI_MAX = max(65, min(78, int(os.getenv("ALERT_RSI_MAX", "70"))))
ALERT_MIN_TURNOVER = max(0.01, float(os.getenv("ALERT_MIN_TURNOVER", "0.03")))
SIGNAL_CONFIRMATIONS_REQUIRED = max(2, int(os.getenv("SIGNAL_CONFIRMATIONS_REQUIRED", "2")))
SIGNAL_CONFIRM_TIMEFRAME = os.getenv("SIGNAL_CONFIRM_TIMEFRAME", "4H").strip()
MARKET_SCAN_PAGES = max(1, min(10, int(os.getenv("MARKET_SCAN_PAGES", "5"))))
MARKET_SCAN_PER_PAGE = max(50, min(250, int(os.getenv("MARKET_SCAN_PER_PAGE", "250"))))
MARKET_SCAN_TOP = max(5, min(30, int(os.getenv("MARKET_SCAN_TOP", "10"))))
MARKET_SCAN_SECONDS = max(300, int(os.getenv("MARKET_SCAN_SECONDS", "300")))
MARKET_SCAN_DEEP = max(5, min(20, int(os.getenv("MARKET_SCAN_DEEP", "8"))))
MARKET_SCAN_CACHE = None
SHORT_TERM_SCAN_CACHE = None
OKX_BASE_URL = os.getenv("OKX_BASE_URL", "https://www.okx.com").rstrip("/")
OKX_CANDLE_CACHE_SECONDS = max(20, int(os.getenv("OKX_CANDLE_CACHE_SECONDS", "60")))
OKX_MAX_CONCURRENCY = max(2, min(12, int(os.getenv("OKX_MAX_CONCURRENCY", "8"))))
OKX_CANDLE_CACHE = {}

PAYMENT_CARD = os.getenv("PAYMENT_CARD", "ثبت نشده").strip()
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "پشتیبان").strip()

GOLD18_URL = "https://www.tgju.org/profile/geram18"
XAU_TGJU_URL = "https://www.tgju.org/profile/ons"
GOLD18_PRICE_SELECTOR = os.getenv("GOLD18_PRICE_SELECTOR", "").strip()
XAU_PRICE_SELECTOR = os.getenv("XAU_PRICE_SELECTOR", "").strip()
GOLD18_SOURCE_UNIT = os.getenv("GOLD18_SOURCE_UNIT", "auto").strip().lower()

# Real Iranian 18K price band (Toman per gram). Update if market shifts.
GOLD18_TOMAN_MIN = int(os.getenv("GOLD18_TOMAN_MIN", "15000000"))
GOLD18_TOMAN_MAX = int(os.getenv("GOLD18_TOMAN_MAX", "90000000"))
XAU_USD_MIN = float(os.getenv("XAU_USD_MIN", "500"))
XAU_USD_MAX = float(os.getenv("XAU_USD_MAX", "10000"))

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

HTTP_SESSION = None


# ============================================================
# LRU + TTL CACHE (bounded)
# ============================================================

class LRUTTLCache:
    """Bounded ordered cache. Prevents unbounded memory growth."""
    def __init__(self, maxsize=2000):
        self._data = OrderedDict()
        self._max = max(64, int(maxsize))

    def get(self, key, ttl):
        item = self._data.get(key)
        if not item:
            return None
        ts, val = item
        if time.monotonic() - ts >= ttl:
            self._data.pop(key, None)
            return None
        self._data.move_to_end(key)
        return val

    def set(self, key, val):
        self._data[key] = (time.monotonic(), val)
        self._data.move_to_end(key)
        while len(self._data) > self._max:
            self._data.popitem(last=False)

    def __len__(self):
        return len(self._data)


CACHE = LRUTTLCache(maxsize=int(os.getenv("CACHE_MAX_ENTRIES", "2000")))
PRICE_CACHE = {}
ANALYSIS_CACHE = {}
GOLD18_CACHE = None
XAU_CACHE = None

MAIN_MENU = [
    ["🪙 افزودن دارایی", "📋 واچ‌لیست"],
    ["💰 قیمت لحظه‌ای", "📊 تحلیل تکنیکال"],
    ["📡 سیگنال معاملاتی", "🎯 فرصت‌های خرید"],
    ["💳 خرید اشتراک", "👤 وضعیت اشتراک"],
    ["🔔 هشدارهای هوشمند", "📨 پشتیبانی"],
    ["ℹ️ راهنمای ربات"],
    ["👨‍💼 پنل مدیریت"],
]

COINS = {
    "BTC":"bitcoin","ETH":"ethereum","BNB":"binancecoin","SOL":"solana",
    "XRP":"ripple","DOGE":"dogecoin","ADA":"cardano","TRX":"tron",
    "AVAX":"avalanche-2","DOT":"polkadot","LINK":"chainlink",
    "MATIC":"matic-network","POL":"polygon-ecosystem-token","LTC":"litecoin",
    "BCH":"bitcoin-cash","ATOM":"cosmos","ETC":"ethereum-classic",
    "XLM":"stellar","UNI":"uniswap","NEAR":"near","APT":"aptos",
    "ARB":"arbitrum","OP":"optimism","FIL":"filecoin",
    "ICP":"internet-computer","HBAR":"hedera-hashgraph","SUI":"sui",
    "PEPE":"pepe","SHIB":"shiba-inu","TON":"the-open-network",
    "ZEC":"zcash","AAVE":"aave","ALGO":"algorand","VET":"vechain",
    "EOS":"eos","XMR":"monero","TAO":"bittensor","INJ":"injective-protocol",
    "SEI":"sei-network","RUNE":"thorchain","MKR":"maker","CRV":"curve-dao-token",
    "GRT":"the-graph","LDO":"lido-staked-ether","SAND":"the-sandbox",
    "MANA":"decentraland","AXS":"axie-infinity","FTM":"fantom",
    "KAS":"kaspa","WIF":"dogwifcoin","BONK":"bonk","FLOKI":"floki",
}

# ============================================================
# DATABASE / PERSISTENCE
# ============================================================

def _db_has_tables(path):
    try:
        con = sqlite3.connect(str(path), timeout=10)
        names = {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        check = con.execute("PRAGMA integrity_check").fetchone()
        con.close()
        return "users" in names and "subscriptions" in names and bool(check and check[0] == "ok")
    except Exception:
        return False

def _db_user_count(path):
    try:
        con = sqlite3.connect(str(path), timeout=10)
        row = con.execute("SELECT COUNT(*) FROM users").fetchone()
        con.close()
        return int(row[0]) if row else 0
    except Exception:
        return 0

def _backup_files():
    try:
        return sorted(
            Path(BACKUP_DIR).glob("crypto_bot_*.db"),
            key=lambda x: x.stat().st_mtime,
            reverse=True,
        )
    except Exception:
        return []

def _restore_latest_backup(target):
    for src in _backup_files():
        try:
            if _db_user_count(src) <= 0:
                continue
            tmp = target.with_suffix(target.suffix + ".restore.tmp")
            shutil.copy2(src, tmp)
            for sidecar in (Path(str(target)+"-wal"), Path(str(target)+"-shm")):
                try:
                    sidecar.unlink()
                except Exception:
                    pass
            os.replace(tmp, target)
            log.warning("DATABASE RESTORED FROM BACKUP: %s -> %s", src, target)
            return True
        except Exception as e:
            log.error("Backup restore failed %s: %s", src, e)
    return False

def _prepare_persistent_db():
    target = Path(DB_PATH)
    target.parent.mkdir(parents=True, exist_ok=True)
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)

    if target.exists() and _db_has_tables(target):
        users = _db_user_count(target)
        if users > 0:
            return
        if AUTO_RESTORE_BACKUP and _restore_latest_backup(target):
            return

    recovery_candidates = [
        Path("/app/crypto_bot.db"),
        Path("/app/data/crypto_bot.db"),
        Path("/app/crypto_bot_old.db"),
        Path("/app/data/crypto_bot_old.db"),
        Path("/app/subscriptions.db"),
        Path("/app/data/subscriptions.db"),
        Path("/app/zec_bot.db"),
        Path("/app/data/zec_bot.db"),
        Path("crypto_bot.db"),
        Path("subscriptions.db"),
        Path("zec_bot.db"),
    ]
    for raw in os.getenv("LEGACY_DB_PATHS", "").split(","):
        raw = raw.strip()
        if raw:
            recovery_candidates.append(Path(raw))

    seen = set()
    for src in recovery_candidates:
        try:
            src = src.resolve()
            target_resolved = target.resolve()
            if src in seen or src == target_resolved:
                continue
            seen.add(src)
            if not src.exists() or not _db_has_tables(src):
                continue
            users = _db_user_count(src)
            if users <= 0:
                continue
            if target.exists() and target.stat().st_size > 0 and _db_user_count(target) == 0:
                try:
                    quarantine = target.with_name(target.name + ".empty-before-recovery")
                    if not quarantine.exists():
                        shutil.copy2(target, quarantine)
                except Exception:
                    pass
            tmp = target.with_suffix(target.suffix + ".migrate.tmp")
            shutil.copy2(src, tmp)
            os.replace(tmp, target)
            log.warning("RECOVERED USER DATABASE: %s -> %s | users=%s", src, target, users)
            return
        except Exception as e:
            log.warning("Legacy database recovery failed %s: %s", src, e)

    if AUTO_RESTORE_BACKUP:
        _restore_latest_backup(target)

def db():
    _prepare_persistent_db()
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA busy_timeout=30000")
    con.execute("PRAGMA foreign_keys=ON")
    return con

def ensure_column(con, table, column, definition):
    cols = {r[1] for r in con.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

def backup_database(reason="scheduled"):
    source = Path(DB_PATH)
    if not source.exists() or not _db_has_tables(source):
        return None
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    final = Path(BACKUP_DIR) / f"crypto_bot_{stamp}_{reason}.db"
    tmp = Path(BACKUP_DIR) / f".crypto_bot_{stamp}_{reason}.tmp.db"
    src = dst = None
    try:
        src = sqlite3.connect(str(source), timeout=30)
        dst = sqlite3.connect(str(tmp), timeout=30)
        with dst:
            src.backup(dst)
        dst.close(); dst = None
        src.close(); src = None
        os.replace(tmp, final)
        for old in _backup_files()[BACKUP_RETENTION:]:
            try:
                old.unlink()
            except Exception:
                pass
        return str(final)
    except Exception as e:
        log.error("Database backup failed: %s", e)
        for con in (src, dst):
            try:
                if con:
                    con.close()
            except Exception:
                pass
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        return None

def database_diagnostics():
    try:
        p = Path(DB_PATH)
        size = p.stat().st_size if p.exists() else 0
        backups = _backup_files()
        return {
            "path": str(p),
            "exists": p.exists(),
            "size": size,
            "persistent_path": str(p).startswith("/data/"),
            "users": _db_user_count(p) if p.exists() else 0,
            "backup_count": len(backups),
            "latest_backup": backups[0].name if backups else "-",
        }
    except Exception:
        return {
            "path": str(DB_PATH), "exists": False, "size": 0,
            "persistent_path": False, "users": 0,
            "backup_count": 0, "latest_backup": "-"
        }

def _preflight_legacy_schema():
    path = Path(DB_PATH)
    if not path.exists():
        return
    try:
        con = sqlite3.connect(str(path), timeout=30)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA busy_timeout=30000")

        tables = {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}

        if "subscriptions" in tables:
            cols = {r[1] for r in con.execute("PRAGMA table_info(subscriptions)")}
            if "end_at" not in cols:
                con.execute("ALTER TABLE subscriptions ADD COLUMN end_at TEXT")
                if "end_date" in cols:
                    con.execute("UPDATE subscriptions SET end_at=end_date WHERE end_at IS NULL OR end_at=''")
                elif "expires_at" in cols:
                    con.execute("UPDATE subscriptions SET end_at=expires_at WHERE end_at IS NULL OR end_at=''")
                elif "expiry" in cols:
                    con.execute("UPDATE subscriptions SET end_at=expiry WHERE end_at IS NULL OR end_at=''")
                elif "start_at" in cols and "days" in cols:
                    rows = con.execute("SELECT id,start_at,days FROM subscriptions WHERE end_at IS NULL OR end_at=''").fetchall()
                    for row in rows:
                        try:
                            start = datetime.fromisoformat(str(row[1]))
                            end = start + timedelta(days=int(row[2] or 0))
                            con.execute("UPDATE subscriptions SET end_at=? WHERE id=?", (end.isoformat(), row[0]))
                        except Exception:
                            pass
            if "status" in cols:
                con.execute("UPDATE subscriptions SET status='expired' WHERE (end_at IS NULL OR end_at='') AND status='active'")

        if "watchlist" in tables:
            cols = {r[1] for r in con.execute("PRAGMA table_info(watchlist)")}
            if "asset_key" in cols:
                con.execute("UPDATE watchlist SET asset_key=upper(symbol) WHERE asset_key IS NULL OR asset_key=''")

        con.commit()
        con.close()
    except Exception:
        log.exception("Legacy schema preflight failed")

def init_db():
    _preflight_legacy_schema()
    try:
        live = Path(DB_PATH)
        if live.exists() and _db_has_tables(live) and _db_user_count(live) > 0:
            backup_database("pre_migration")
    except Exception:
        log.exception("pre-migration backup")

    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS app_meta(key TEXT PRIMARY KEY,value TEXT);
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,username TEXT,first_name TEXT,
            created_at TEXT NOT NULL,last_seen TEXT NOT NULL,
            blocked INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS subscriptions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,
            plan TEXT NOT NULL,days INTEGER NOT NULL,amount INTEGER NOT NULL,
            start_at TEXT NOT NULL,end_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            source TEXT DEFAULT 'manual',
            payment_request_id INTEGER,created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS payment_requests(
            id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,
            plan TEXT NOT NULL,days INTEGER NOT NULL,amount INTEGER NOT NULL,
            receipt_file_id TEXT,status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,reviewed_at TEXT,reviewed_by INTEGER);
        CREATE TABLE IF NOT EXISTS watchlist(
            user_id INTEGER NOT NULL,symbol TEXT NOT NULL,
            asset_type TEXT NOT NULL,created_at TEXT NOT NULL,
            PRIMARY KEY(user_id,symbol,asset_type));
        CREATE TABLE IF NOT EXISTS alert_preferences(
            user_id INTEGER PRIMARY KEY,enabled INTEGER NOT NULL DEFAULT 1,
            interval_seconds INTEGER NOT NULL DEFAULT 300,last_check_at TEXT);
        CREATE TABLE IF NOT EXISTS alert_events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,signal_key TEXT NOT NULL,
            message TEXT NOT NULL,created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS market_history(
            id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT NOT NULL,
            asset_type TEXT NOT NULL,price REAL NOT NULL,created_at TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_market_history_asset
        ON market_history(symbol,asset_type,created_at);
        CREATE TABLE IF NOT EXISTS signal_state(
            symbol TEXT PRIMARY KEY,candidate TEXT NOT NULL,
            confirmations INTEGER NOT NULL DEFAULT 0,
            last_candidate_at TEXT,last_price REAL,updated_at TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_signal_state_candidate
        ON signal_state(candidate,confirmations);
        CREATE TABLE IF NOT EXISTS market_alert_state(
            symbol TEXT PRIMARY KEY,candidate TEXT NOT NULL,
            confirmations INTEGER NOT NULL DEFAULT 0,
            last_observation_key TEXT,last_price REAL,updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS support_messages(
            id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,
            admin_id INTEGER,direction TEXT NOT NULL,message TEXT,
            telegram_message_id INTEGER,status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT NOT NULL,replied_at TEXT);
        CREATE TABLE IF NOT EXISTS chat_messages(
            id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,
            asset_type TEXT NOT NULL,symbol TEXT NOT NULL,message TEXT NOT NULL,
            telegram_message_id INTEGER,created_at TEXT NOT NULL,
            deleted INTEGER NOT NULL DEFAULT 0,deleted_by INTEGER,deleted_at TEXT);
        CREATE TABLE IF NOT EXISTS chat_reports(
            id INTEGER PRIMARY KEY AUTOINCREMENT,message_id INTEGER NOT NULL,
            reporter_id INTEGER NOT NULL,reason TEXT,
            status TEXT NOT NULL DEFAULT 'pending',created_at TEXT NOT NULL,
            reviewed_by INTEGER,reviewed_at TEXT);
        """)

        ensure_column(c, "users", "username", "TEXT")
        ensure_column(c, "users", "first_name", "TEXT")
        ensure_column(c, "users", "created_at", "TEXT")
        ensure_column(c, "users", "blocked", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(c, "users", "last_seen", "TEXT")

        ensure_column(c, "payment_requests", "user_id", "INTEGER")
        ensure_column(c, "payment_requests", "plan", "TEXT")
        ensure_column(c, "payment_requests", "days", "INTEGER")
        ensure_column(c, "payment_requests", "amount", "INTEGER")
        ensure_column(c, "payment_requests", "receipt_file_id", "TEXT")
        ensure_column(c, "payment_requests", "status", "TEXT DEFAULT 'pending'")
        ensure_column(c, "payment_requests", "created_at", "TEXT")
        ensure_column(c, "payment_requests", "reviewed_at", "TEXT")
        ensure_column(c, "payment_requests", "reviewed_by", "INTEGER")

        ensure_column(c, "subscriptions", "plan", "TEXT")
        ensure_column(c, "subscriptions", "days", "INTEGER")
        ensure_column(c, "subscriptions", "amount", "INTEGER")
        ensure_column(c, "subscriptions", "start_at", "TEXT")
        ensure_column(c, "subscriptions", "end_at", "TEXT")
        ensure_column(c, "subscriptions", "status", "TEXT DEFAULT 'expired'")
        ensure_column(c, "subscriptions", "source", "TEXT")
        ensure_column(c, "subscriptions", "payment_request_id", "INTEGER")
        ensure_column(c, "subscriptions", "created_at", "TEXT")

        ensure_column(c, "watchlist", "user_id", "INTEGER")
        ensure_column(c, "watchlist", "symbol", "TEXT")
        ensure_column(c, "watchlist", "asset_type", "TEXT")
        ensure_column(c, "watchlist", "created_at", "TEXT")
        ensure_column(c, "watchlist", "asset_key", "TEXT DEFAULT ''")
        c.execute("UPDATE watchlist SET asset_key=upper(symbol) WHERE asset_key IS NULL OR asset_key=''")

        ensure_column(c, "alert_events", "signal_key", "TEXT DEFAULT ''")
        ensure_column(c, "market_alert_state", "cycle_id", "TEXT")
        ensure_column(c, "support_messages", "replied_at", "TEXT")
        ensure_column(c, "chat_messages", "deleted_by", "INTEGER")
        ensure_column(c, "chat_messages", "deleted_at", "TEXT")

        c.execute("CREATE INDEX IF NOT EXISTS idx_sub_user_end ON subscriptions(user_id,end_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_pay_status ON payment_requests(status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_watch_asset ON watchlist(asset_type,symbol)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_chat_room ON chat_messages(asset_type,symbol,created_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_chat_reports ON chat_reports(status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_alert_events ON alert_events(user_id,symbol,created_at)")

        c.execute("INSERT OR REPLACE INTO app_meta(key,value) VALUES('schema_version','12')")
        c.execute("INSERT OR REPLACE INTO app_meta(key,value) VALUES('db_path',?)", (DB_PATH,))

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def ensure_user(user):
    if not user:
        return
    n = now_iso()
    with db() as c:
        c.execute("""
        INSERT INTO users(user_id,username,first_name,created_at,last_seen)
        VALUES(?,?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
        username=excluded.username,
        first_name=excluded.first_name,
        last_seen=excluded.last_seen
        """, (user.id, user.username or "", user.first_name or "", n, n))
        c.execute("""
        INSERT OR IGNORE INTO alert_preferences(user_id,enabled,interval_seconds)
        VALUES(?,?,?)
        """, (user.id, 1, ALERT_INTERVAL_SECONDS))

def is_admin(uid):
    return uid in ADMIN_IDS

def is_blocked(uid):
    with db() as c:
        r = c.execute("SELECT blocked FROM users WHERE user_id=?", (uid,)).fetchone()
        return bool(r and r["blocked"])

def active_subscription(uid):
    n = now_iso()
    with db() as c:
        c.execute("""
        UPDATE subscriptions SET status='expired'
        WHERE user_id=? AND status='active' AND end_at<=?
        """, (uid, n))
        return c.execute("""
        SELECT * FROM subscriptions
        WHERE user_id=? AND status='active' AND end_at>?
        ORDER BY end_at DESC LIMIT 1
        """, (uid, n)).fetchone()

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
        c.execute("""
        UPDATE subscriptions SET status='expired'
        WHERE user_id=? AND status='active' AND end_at<=?
        """, (uid, now_iso()))
        c.execute("""
        INSERT INTO subscriptions(
        user_id,plan,days,amount,start_at,end_at,status,source,
        payment_request_id,created_at)
        VALUES(?,?,?,?,?,?,?,?,?,?)
        """, (
            uid, plan, days, amount, start.isoformat(), end.isoformat(),
            "active", source, payment_request_id, now_iso()
        ))
    try:
        backup_database("subscription")
    except Exception:
        log.exception("subscription backup")
    return True

def format_dt(v):
    try:
        return datetime.fromisoformat(v).astimezone().strftime("%Y/%m/%d %H:%M")
    except Exception:
        return str(v)

# ============================================================
# HTTP / CACHE
# ============================================================

async def get_session():
    global HTTP_SESSION
    if HTTP_SESSION is None or HTTP_SESSION.closed:
        HTTP_SESSION = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
            connector=aiohttp.TCPConnector(limit=30, ttl_dns_cache=300),
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Linux; Android 10) "
                    "AppleWebKit/537.36 Chrome/120.0 Mobile Safari/537.36"
                )
            },
        )
    return HTTP_SESSION

def cache_key(url, params):
    return (url, tuple(sorted((params or {}).items())))

async def http_json(url, params=None, ttl=None, retries=2):
    key = cache_key(url, params)
    ttl = CACHE_SECONDS if ttl is None else ttl
    cached = CACHE.get(key, ttl)
    if cached is not None:
        return cached

    session = await get_session()
    for attempt in range(retries):
        try:
            async with session.get(url, params=params) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    CACHE.set(key, data)
                    return data
                log.warning("HTTP %s %s", r.status, url)
        except Exception as e:
            log.warning("HTTP error %s: %s", url, e)
        if attempt + 1 < retries:
            await asyncio.sleep(0.6 * (attempt + 1))
    return None

async def http_text(url, ttl=None):
    key = ("TEXT", url)
    ttl = CACHE_SECONDS if ttl is None else ttl
    cached = CACHE.get(key, ttl)
    if cached is not None:
        return cached
    try:
        session = await get_session()
        async with session.get(url) as r:
            if r.status == 200:
                text = await r.text()
                CACHE.set(key, text)
                return text
            log.warning("HTTP text %s: %s", r.status, url)
    except Exception as e:
        log.warning("HTTP text error: %s", e)
    return None

# ============================================================
# ASSET HELPERS
# ============================================================

def norm_symbol(value):
    value = (value or "").strip().upper()
    value = value.translate(str.maketrans(
        "۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩",
        "01234567890123456789"
    ))
    value = value.replace(" ", "").replace("‌", "").replace("_", "")
    aliases = {
        "GOLD":"XAU","GOLDUSD":"XAU","XAUUSD":"XAU","XAU/USD":"XAU",
        "XAU/USDT":"XAU","XAUUSDT":"XAU","طلا":"XAU","طلایجهانی":"XAU",
        "GERAM18":"GOLD18","GERAM18K":"GOLD18","18K":"GOLD18",
        "GOLD18K":"GOLD18","IRANGOLD":"GOLD18","طلای18":"GOLD18",
        "طلای۱۸":"GOLD18","طلای۱۸عیار":"GOLD18","طلایداخلی":"GOLD18",
    }
    if value in aliases:
        return aliases[value]
    for suffix in ("USDT", "USD"):
        if value.endswith(suffix) and len(value) > len(suffix):
            base = value[:-len(suffix)].replace("/", "").replace("-", "")
            if base in COINS:
                return base
    return value

def asset_type(symbol):
    s = norm_symbol(symbol)
    return "gold" if s == "XAU" else "gold18" if s == "GOLD18" else "crypto"

async def crypto_search(query):
    query = (query or "").strip()
    direct = norm_symbol(query)
    if direct in COINS:
        return [(direct, COINS[direct], direct)]
    data = await http_json(
        "https://api.coingecko.com/api/v3/search",
        {"query": query}, ttl=60
    )
    out, seen = [], set()
    for coin in (data or {}).get("coins", [])[:20]:
        s = norm_symbol(coin.get("symbol") or "")
        cid = coin.get("id")
        name = coin.get("name") or s
        if not s or not cid or (s, cid) in seen:
            continue
        seen.add((s, cid))
        out.append((s, cid, name))
    return out

async def crypto_data(symbol, coin_id=None):
    s = norm_symbol(symbol)
    cid = coin_id or COINS.get(s)
    if not cid:
        res = await crypto_search(s)
        if not res:
            return None
        s, cid, _ = res[0]

    data = await http_json(
        f"https://api.coingecko.com/api/v3/coins/{cid}/market_chart",
        {"vs_currency":"usd","days":"2","interval":"hourly"},
        ttl=CACHE_SECONDS
    )
    if not data or not data.get("prices"):
        return None

    prices = pd.Series([float(x[1]) for x in data["prices"]], dtype=float)
    vols = pd.Series([float(x[1]) for x in data.get("total_volumes", [])], dtype=float)
    return s, prices, vols

async def market_universe():
    global MARKET_SCAN_CACHE
    now = time.monotonic()
    if MARKET_SCAN_CACHE and now - MARKET_SCAN_CACHE[0] < MARKET_SCAN_SECONDS:
        return MARKET_SCAN_CACHE[1]

    all_rows, seen = [], set()
    for page in range(1, MARKET_SCAN_PAGES + 1):
        data = await http_json(
            "https://api.coingecko.com/api/v3/coins/markets",
            {
                "vs_currency":"usd", "order":"market_cap_desc",
                "per_page":MARKET_SCAN_PER_PAGE, "page":page,
                "sparkline":"true",
                "price_change_percentage":"1h,24h,7d,14d,30d,200d,1y",
            }, ttl=MARKET_SCAN_SECONDS, retries=2
        )
        if not data:
            break
        for x in data:
            cid = x.get("id")
            if not cid or cid in seen:
                continue
            seen.add(cid)
            all_rows.append(x)
        if len(data) < MARKET_SCAN_PER_PAGE:
            break
        await asyncio.sleep(0.15)

    MARKET_SCAN_CACHE = (now, all_rows)
    log.info("MARKET SCAN universe=%s pages=%s", len(all_rows), MARKET_SCAN_PAGES)
    return all_rows

def _clamp(v, lo=0, hi=100):
    return max(lo, min(hi, float(v)))

def _safe_float(v, default=0.0):
    try:
        x = float(v)
        return default if pd.isna(x) else x
    except Exception:
        return default

def _market_growth_score(x, ta=None):
    p = float(x.get("current_price") or 0)
    if p <= 0:
        return None
    ch1 = float(x.get("price_change_percentage_1h_in_currency") or 0)
    ch24 = float(x.get("price_change_percentage_24h_in_currency") or 0)
    ch7 = float(x.get("price_change_percentage_7d_in_currency") or 0)
    ch14 = float(x.get("price_change_percentage_14d_in_currency") or 0)
    ch30 = float(x.get("price_change_percentage_30d_in_currency") or 0)
    vol = float(x.get("total_volume") or 0)
    mcap = float(x.get("market_cap") or 0)
    if not mcap or not vol:
        return None

    score = 50.0
    score += _clamp(ch1 * 2.0, -8, 8)
    score += _clamp(ch24 * 0.65, -10, 10)
    score += _clamp(ch7 * 0.40, -8, 8)
    score += _clamp(ch14 * 0.20, -5, 5)
    score += _clamp(ch30 * 0.12, -5, 5)

    turnover = vol / mcap
    score += _clamp((turnover - 0.05) * 80, -5, 8)

    if ch24 > 25: score -= 7
    if ch7 > 60: score -= 5
    if ch24 < -20 and ch7 < -20: score -= 8

    if ta:
        score += _clamp((ta.get("strength",50)-50)*0.18, -8, 8)
        if ta.get("trend") in ("BULLISH", "BULLISH_WEAK"): score += 5
        if ta.get("rsi",50) > 75: score -= 5
        elif 52 <= ta.get("rsi",50) <= 68: score += 4
        if ta.get("macd_hist",0) > 0: score += 4
        if ta.get("adx",0) >= 20: score += 3

    return _clamp(score)

def _growth_probability(score, x):
    rank = float(x.get("market_cap_rank") or 10000)
    rank_bonus = _clamp(10 - (rank / 1000), -5, 10)
    return _clamp(50 + (score - 50) * 0.75 + rank_bonus, 5, 95)

async def growth_scan():
    """Unified with professional engine."""
    rows = await market_universe()
    if not rows:
        return []

    rough = []
    for x in rows:
        score = _market_growth_score(x)
        if score is None:
            continue
        rough.append((score, x))
    rough.sort(key=lambda z: z[0], reverse=True)

    top = rough[:MARKET_SCAN_DEEP]
    async def deep(x):
        sym = norm_symbol(x.get("symbol") or "")
        try:
            return x, await professional_crypto_analysis(sym)
        except Exception:
            return x, None
    results = await asyncio.gather(*(deep(x) for _, x in top), return_exceptions=True)

    out = []
    for item in results:
        if isinstance(item, Exception):
            continue
        x, ta = item
        if not ta:
            continue
        score = _market_growth_score(x, ta) or 50.0
        out.append({
            "id": x.get("id"), "symbol": norm_symbol(x.get("symbol") or ""),
            "name": x.get("name") or norm_symbol(x.get("symbol") or ""),
            "price": float(x.get("current_price") or 0),
            "rank": int(x.get("market_cap_rank") or 0),
            "mcap": float(x.get("market_cap") or 0),
            "volume": float(x.get("total_volume") or 0),
            "ch24": float(x.get("price_change_percentage_24h_in_currency") or 0),
            "ch7": float(x.get("price_change_percentage_7d_in_currency") or 0),
            "ch30": float(x.get("price_change_percentage_30d_in_currency") or 0),
            "growth_score": score,
            "probability": _growth_probability(score, x),
            "strength": _safe_float(ta.get("strength", score)),
            "trend": ta.get("trend") or "MARKET",
            "rsi": _safe_float(ta.get("rsi"), 0),
        })
    out.sort(key=lambda z: z["growth_score"], reverse=True)
    return out[:MARKET_SCAN_TOP]

# ============================================================
# NUMBER / TGJU PARSING (REVISED)
# ============================================================

def digits_to_latin(s):
    return (s or "").translate(str.maketrans(
        "۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩",
        "01234567890123456789"
    ))

def _numbers_with_positions(text):
    raw = digits_to_latin(text or "").replace("٬", ",").replace("\u00a0", " ")
    out = []
    for m in re.finditer(
        r"(?<!\d)(\d{1,3}(?:[,\s]\d{3})+|\d{5,12})(?!\d)",
        raw
    ):
        try:
            out.append((m.start(), float(m.group(1).replace(",", "").replace(" ", ""))))
        except Exception:
            pass
    return out

def _unit_factor_strict(text, forced_unit="auto"):
    t = digits_to_latin(text or "").replace(" ", "").lower()
    if forced_unit in ("toman", "تومان"):
        return 1.0, "toman"
    if forced_unit in ("rial", "ریال"):
        return 0.1, "rial"
    if "تومان" in t:
        return 1.0, "toman"
    if "ریال" in t:
        return 0.1, "rial"
    return None, None

def _context(tag, levels=4, limit=5000):
    parts = []
    node = tag
    for _ in range(levels):
        if node is None:
            break
        text = node.get_text(" ", strip=True)
        if text:
            parts.append(text)
        node = node.parent
    return " ".join(dict.fromkeys(parts))[:limit]

def _score_gold18_node(tag):
    attrs = " ".join(f"{k}={v}" for k, v in tag.attrs.items()).lower()
    text = tag.get_text(" ", strip=True)
    low = text.lower()
    score = 0
    if "geram18" in attrs: score += 300
    if "geram18" in low.replace(" ", ""): score += 250
    if "geram" in attrs and "18" in attrs: score += 180
    for word in ("طلای 18 عیار", "طلای ۱۸ عیار", "گرم طلای 18", "گرم طلای ۱۸"):
        if word in text: score += 160
    for word in ("قیمت", "نرخ", "آخرین", "ارزش", "price", "value", "current"):
        if word in low: score += 12
    if "ریال" in text or "تومان" in text: score += 30
    if len(text) > 1500: score -= 150
    if len(text) > 5000: score -= 300
    return score

# ============================================================
# TGJU GOLD18 (STRICT)
# ============================================================

def find_gold18_value(html):
    """Robust TGJU geram18 parser. Returns Toman per gram or None."""
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    candidates = []

    def consider(raw, ctx, score=0):
        try:
            raw = float(raw)
        except Exception:
            return
        factor, unit = _unit_factor_strict(ctx, GOLD18_SOURCE_UNIT)
        if factor is None and GOLD18_SOURCE_UNIT == "auto":
            if 150_000_000 <= raw <= 900_000_000:
                factor = 0.1; unit = "rial"
            elif GOLD18_TOMAN_MIN <= raw <= GOLD18_TOMAN_MAX:
                factor = 1.0; unit = "toman"
            else:
                return
        if factor is None:
            return
        toman = raw * factor
        if not (GOLD18_TOMAN_MIN <= toman <= GOLD18_TOMAN_MAX):
            return
        candidates.append((score, toman, unit, raw))

    if GOLD18_PRICE_SELECTOR:
        try:
            for tag in soup.select(GOLD18_PRICE_SELECTOR):
                text = _context(tag, levels=5, limit=6000)
                for _, raw in _numbers_with_positions(tag.get_text(" ", strip=True)):
                    consider(raw, text, 1000)
        except Exception as e:
            log.warning("Invalid GOLD18 selector: %s", e)

    selectors = [
        '[data-symbol="geram18"]', '[data-profile="geram18"]',
        '[data-code="geram18"]', '[data-item="geram18"]',
        '#geram18', '.geram18', '[id*="geram18"]',
        '[class*="geram18"]', '[data-field*="geram18"]',
        '[data-symbol*="geram18"]',
    ]
    seen = set()
    for selector in selectors:
        try:
            nodes = soup.select(selector)
        except Exception:
            nodes = []
        for tag in nodes:
            if id(tag) in seen:
                continue
            seen.add(id(tag))
            ctx = _context(tag, levels=6, limit=8000)
            for _, raw in _numbers_with_positions(tag.get_text(" ", strip=True)):
                consider(raw, ctx, 900 + _score_gold18_node(tag))

    keywords = (
        "geram18", "گرم طلای 18", "گرم طلای ۱۸",
        "طلای 18 عیار", "طلای ۱۸ عیار", "طلای18", "طلای۱۸"
    )
    for tag in soup.find_all(["tr", "li", "article", "section", "td", "div"]):
        text = tag.get_text(" ", strip=True)
        if len(text) > 1200:
            continue
        compact = text.lower().replace(" ", "")
        if not any(k.lower().replace(" ", "") in compact for k in keywords):
            continue
        for _, raw in _numbers_with_positions(text):
            consider(raw, text, 500 + _score_gold18_node(tag))

    raw_html = digits_to_latin(html)
    for m in re.finditer(r"geram18", raw_html, flags=re.I):
        lo = max(0, m.start() - 2500)
        hi = min(len(raw_html), m.end() + 5000)
        chunk = raw_html[lo:hi]
        key_bonus = 180 if re.search(r"(?:price|value|current|last|close|p|v)\s*[:=]", chunk, re.I) else 0
        for _, raw in _numbers_with_positions(chunk):
            consider(raw, chunk, 700 + key_bonus)

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: (x[0], 1 if 20_000_000 <= x[1] <= 60_000_000 else 0),
        reverse=True
    )
    value = float(candidates[0][1])
    log.info("TGJU GOLD18 parsed: %.0f Toman/gram (unit=%s raw=%.0f)",
             value, candidates[0][2], candidates[0][3])
    return value

def save_market_snapshot(symbol, atype, price):
    if price is None or price <= 0:
        return
    try:
        with db() as c:
            last = c.execute(
                "SELECT price,created_at FROM market_history WHERE symbol=? AND asset_type=? ORDER BY id DESC LIMIT 1",
                (symbol, atype)
            ).fetchone()
            if last and abs(float(last["price"]) - float(price)) < 1e-12:
                try:
                    age = datetime.now(timezone.utc) - datetime.fromisoformat(last["created_at"])
                    if age.total_seconds() < GOLD_HISTORY_INTERVAL_SECONDS * 0.8:
                        return
                except Exception:
                    pass
            c.execute(
                "INSERT INTO market_history(symbol,asset_type,price,created_at) VALUES(?,?,?,?)",
                (symbol, atype, float(price), now_iso())
            )
            c.execute(
                "DELETE FROM market_history WHERE symbol=? AND asset_type=? AND id NOT IN (SELECT id FROM market_history WHERE symbol=? AND asset_type=? ORDER BY id DESC LIMIT 1000)",
                (symbol, atype, symbol, atype)
            )
    except Exception:
        log.exception("market snapshot save: %s", symbol)

def get_market_history(symbol, atype, limit=500):
    try:
        with db() as c:
            rows = c.execute(
                "SELECT price FROM market_history WHERE symbol=? AND asset_type=? ORDER BY id DESC LIMIT ?",
                (symbol, atype, int(limit))
            ).fetchall()
        return pd.Series([float(r["price"]) for r in reversed(rows)], dtype=float)
    except Exception:
        return pd.Series(dtype=float)

async def gold18_data():
    global GOLD18_CACHE
    now = time.monotonic()
    if GOLD18_CACHE and now - GOLD18_CACHE[0] < PRICE_CACHE_SECONDS:
        return GOLD18_CACHE[1]

    html = await http_text(GOLD18_URL, ttl=PRICE_CACHE_SECONDS)
    if not html:
        log.error("TGJU geram18 unavailable")
        return None
    price = find_gold18_value(html)
    if price is None or not (GOLD18_TOMAN_MIN <= price <= GOLD18_TOMAN_MAX):
        log.error("TGJU geram18 price could not be identified safely: %s", price)
        return None

    save_market_snapshot("GOLD18", "gold18", price)
    history = get_market_history("GOLD18", "gold18")
    if len(history) < GOLD_MIN_HISTORY_POINTS:
        history = history if len(history) else pd.Series([price], dtype=float)
    result = ("GOLD18", history, pd.Series(dtype=float))
    GOLD18_CACHE = (now, result)
    log.info("TGJU GOLD18: %.0f Toman/gram | history=%s", price, len(history))
    return result

# ============================================================
# TGJU GLOBAL GOLD / ONS (STRICT)
# ============================================================

def find_xau_value(html):
    """Strict TGJU /profile/ons parser. Returns USD per troy ounce or None."""
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    candidates = []

    def consider(value, ctx, base_score):
        try:
            v = float(value)
        except Exception:
            return
        if not (XAU_USD_MIN <= v <= XAU_USD_MAX):
            return
        score = base_score
        low = ctx.lower()
        if "انس" in ctx or "اونس" in ctx: score += 150
        if "طلا" in ctx or "gold" in low: score += 100
        if "دلار" in ctx or "usd" in low: score += 80
        for noise in ("نقره", "نقره‌ای", "silver", "platinum", "پلاتین", "palladium", "پالادیوم"):
            if noise.lower() in low:
                score -= 400
        candidates.append((score, v))

    if XAU_PRICE_SELECTOR:
        try:
            for tag in soup.select(XAU_PRICE_SELECTOR):
                text = tag.get_text(" ", strip=True)
                for _, value in _numbers_with_positions(text):
                    consider(value, text, 1000)
        except Exception as e:
            log.warning("Invalid XAU selector: %s", e)

    selectors = [
        '[data-symbol="ons"]', '[data-profile="ons"]', '[data-code="ons"]',
        '[data-item="ons"]', '#ons', '.ons',
        '[id*="ons"]', '[class*="ons"]', '[data-symbol*="ons"]',
    ]
    seen = set()
    for selector in selectors:
        try:
            nodes = soup.select(selector)
        except Exception:
            nodes = []
        for tag in nodes:
            if id(tag) in seen:
                continue
            seen.add(id(tag))
            text = tag.get_text(" ", strip=True)
            if len(text) > 3000:
                continue
            for _, value in _numbers_with_positions(text):
                consider(value, text, 500)

    keywords = ("انس طلا", "انس جهانی طلا", "انس جهانی", "اونس طلا", "اونس جهانی", "gold", "xau")
    for tag in soup.find_all(["tr", "li", "article", "section", "td", "div"]):
        text = tag.get_text(" ", strip=True)
        if len(text) > 1200:
            continue
        compact = text.lower().replace(" ", "")
        if not any(k.lower().replace(" ", "") in compact for k in keywords):
            continue
        for _, value in _numbers_with_positions(text):
            consider(value, text, 100)

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    if candidates[0][0] < 100:
        log.warning("XAU parse confidence too low (%s); returning None", candidates[0][0])
        return None
    return float(candidates[0][1])

async def xau_tgju_price():
    global XAU_CACHE
    now = time.monotonic()
    if XAU_CACHE and now - XAU_CACHE[0] < PRICE_CACHE_SECONDS:
        return XAU_CACHE[1]

    html = await http_text(XAU_TGJU_URL, ttl=PRICE_CACHE_SECONDS)
    if not html:
        log.error("TGJU ons unavailable")
        return None

    price = find_xau_value(html)
    if price is None or not (XAU_USD_MIN <= price <= XAU_USD_MAX):
        log.error("TGJU ons price could not be identified safely: %s", price)
        return None

    XAU_CACHE = (now, float(price))
    log.info("TGJU XAU: %.2f USD/oz", price)
    return float(price)

async def xau_data():
    price = await xau_tgju_price()
    if price is None:
        return None
    save_market_snapshot("XAU", "gold", price)
    history = get_market_history("XAU", "gold")
    if len(history) < GOLD_MIN_HISTORY_POINTS:
        history = history if len(history) else pd.Series([price], dtype=float)
    return "XAU", history, pd.Series(dtype=float)

# ============================================================
# ASSET DATA / CURRENT PRICE
# ============================================================

async def asset_data(symbol):
    s = norm_symbol(symbol)
    if s == "XAU":
        return await xau_data()
    if s == "GOLD18":
        return await gold18_data()
    return await crypto_data(s)

async def current_price(symbol):
    s = norm_symbol(symbol)
    item = PRICE_CACHE.get(s)
    if item and time.monotonic() - item[0] < PRICE_CACHE_SECONDS:
        return item[1]

    result = None

    if s == "GOLD18":
        data = await gold18_data()
        if data:
            result = {
                "symbol": "GOLD18",
                "price": float(data[1].iloc[-1]),
                "unit": "تومان برای هر گرم طلای ۱۸ عیار ایران",
                "source": "TGJU",
            }
    elif s == "XAU":
        value = await xau_tgju_price()
        if value is not None:
            result = {
                "symbol": "XAU",
                "price": float(value),
                "unit": "دلار برای هر اونس",
                "source": "TGJU",
            }
    else:
        cid = COINS.get(s)
        if not cid:
            res = await crypto_search(s)
            if res:
                s, cid, _ = res[0]
        if cid:
            data = await http_json(
                "https://api.coingecko.com/api/v3/simple/price",
                {"ids": cid, "vs_currencies": "usd", "include_24hr_change": "true"},
                ttl=PRICE_CACHE_SECONDS
            )
            try:
                it = data[cid]
                result = {
                    "symbol": s, "price": float(it["usd"]),
                    "change24": float(it.get("usd_24h_change") or 0),
                    "unit": "دلار", "source": "CoinGecko",
                }
            except Exception:
                pass

    if result:
        PRICE_CACHE[s] = (time.monotonic(), result)
    return result

def format_live_price(item):
    if not item:
        return "❌ قیمت در حال حاضر در دسترس نیست."
    s = escape(item["symbol"])
    p = item["price"]
    if item["symbol"] == "GOLD18":
        value = f"{p:,.0f}"
    elif item["symbol"] == "XAU":
        value = f"{p:,.2f}"
    else:
        value = f"{p:,.8f}".rstrip("0").rstrip(".")
    text = (
        f"💰 <b>{s}</b>\n"
        f"قیمت فعلی: <b>{value}</b>\n"
        f"واحد: {escape(item['unit'])}"
    )
    if item.get("source"):
        text += f"\nمنبع: {escape(item['source'])}"
    if "change24" in item:
        text += f"\nتغییر ۲۴ ساعت: <b>{item['change24']:+.2f}%</b>"
    return text

# ============================================================
# PROFESSIONAL OHLCV (OKX)
# ============================================================

async def okx_candles(symbol, bar="1H", limit=240):
    s = norm_symbol(symbol)
    inst = f"{s}-USDT"
    key = (inst, bar, int(limit))
    cached = OKX_CANDLE_CACHE.get(key)
    if cached and time.monotonic() - cached[0] < OKX_CANDLE_CACHE_SECONDS:
        return cached[1].copy()

    data = await http_json(
        f"{OKX_BASE_URL}/api/v5/market/candles",
        {"instId": inst, "bar": bar, "limit": str(min(300, int(limit)))},
        ttl=OKX_CANDLE_CACHE_SECONDS, retries=2,
    )
    rows = (data or {}).get("data") or []
    parsed = []
    for row in rows:
        if len(row) < 6:
            continue
        try:
            parsed.append({
                "ts": int(row[0]),
                "open": float(row[1]), "high": float(row[2]),
                "low": float(row[3]), "close": float(row[4]),
                "volume": float(row[5]),
                "confirm": int(row[8]) if len(row) > 8 and str(row[8]).isdigit() else 1,
            })
        except Exception:
            continue
    if not parsed:
        return None

    df = pd.DataFrame(parsed).drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
    if len(df) > 1:
        confirmed = df[df["confirm"] == 1].copy()
        if not confirmed.empty:
            df = confirmed
    OKX_CANDLE_CACHE[key] = (time.monotonic(), df.copy())
    return df

def _wilder_rsi(close, period=14):
    close = pd.to_numeric(close, errors="coerce").astype(float)
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    ag = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    al = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = ag / al.replace(0, float("nan"))
    out = 100 - (100 / (1 + rs))
    out = out.where(al != 0, 100)
    return out.astype(float).fillna(50.0)

def _advanced_indicators(df):
    if df is None or df.empty:
        return None
    x = df.copy()
    required = ("open", "high", "low", "close", "volume")
    for col in required:
        if col not in x.columns:
            return None
        x[col] = pd.to_numeric(x[col], errors="coerce")
    x = x.replace([float("inf"), float("-inf")], float("nan"))
    x = x.dropna(subset=list(required)).reset_index(drop=True)
    if not x.empty:
        valid = (
            (x["open"] > 0) & (x["high"] > 0) & (x["low"] > 0) &
            (x["close"] > 0) & (x["volume"] >= 0) & (x["high"] >= x["low"])
        )
        x = x.loc[valid].reset_index(drop=True)
    if len(x) < 100:
        return None

    c = x["close"].astype(float); h = x["high"].astype(float)
    l = x["low"].astype(float); v = x["volume"].astype(float)

    ema9 = c.ewm(span=9, adjust=False).mean()
    ema21 = c.ewm(span=21, adjust=False).mean()
    ema50 = c.ewm(span=50, adjust=False).mean()
    ema200 = c.ewm(span=200, adjust=False, min_periods=100).mean()
    rsi = _wilder_rsi(c, 14)
    macd = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
    macd_sig = macd.ewm(span=9, adjust=False).mean()
    macd_hist = macd - macd_sig

    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()

    up = h.diff(); down = -l.diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    atr_w = tr.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean() / atr_w.replace(0, float('nan'))
    minus_di = 100 * minus_dm.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean() / atr_w.replace(0, float('nan'))
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, float('nan'))
    adx = dx.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()

    bb_mid = c.rolling(20).mean(); bb_std = c.rolling(20).std(ddof=0)
    bb_upper = bb_mid + 2 * bb_std; bb_lower = bb_mid - 2 * bb_std
    bb_width = (bb_upper - bb_lower) / bb_mid.replace(0, float('nan')) * 100

    vol_ma20 = v.rolling(20).mean(); vol_ma50 = v.rolling(50).mean()
    obv_step = c.diff().apply(lambda z: 1 if z > 0 else -1 if z < 0 else 0) * v
    obv = obv_step.cumsum()
    typical = (h + l + c) / 3
    vwap = (typical * v).rolling(48, min_periods=10).sum() / v.rolling(48, min_periods=10).sum().replace(0, float('nan'))

    lowest14 = l.rolling(14).min(); highest14 = h.rolling(14).max()
    stoch_k = 100 * (c - lowest14) / (highest14 - lowest14).replace(0, float('nan'))
    stoch_d = stoch_k.rolling(3).mean()
    roc5 = c.pct_change(5) * 100; roc20 = c.pct_change(20) * 100
    returns = c.pct_change() * 100
    realized_vol = returns.rolling(30).std()

    return {
        "c": c, "h": h, "l": l, "v": v,
        "ema9": ema9, "ema21": ema21, "ema50": ema50, "ema200": ema200,
        "rsi": rsi, "macd": macd, "macd_sig": macd_sig, "macd_hist": macd_hist,
        "tr": tr, "atr": atr, "plus_di": plus_di, "minus_di": minus_di, "adx": adx,
        "bb_mid": bb_mid, "bb_upper": bb_upper, "bb_lower": bb_lower, "bb_width": bb_width,
        "vol_ma20": vol_ma20, "vol_ma50": vol_ma50, "obv": obv, "vwap": vwap,
        "stoch_k": stoch_k, "stoch_d": stoch_d, "roc5": roc5, "roc20": roc20,
        "realized_vol": realized_vol,
    }

def _market_structure(df, lookback=80):
    x = df.tail(max(40, lookback)).reset_index(drop=True)
    h, l, c = x["high"], x["low"], x["close"]
    swing_highs, swing_lows = [], []
    radius = 2
    for i in range(radius, len(x) - radius):
        if h.iloc[i] >= h.iloc[i-radius:i+radius+1].max():
            swing_highs.append((i, float(h.iloc[i])))
        if l.iloc[i] <= l.iloc[i-radius:i+radius+1].min():
            swing_lows.append((i, float(l.iloc[i])))
    last_highs = swing_highs[-4:]; last_lows = swing_lows[-4:]
    hh = len(last_highs) >= 2 and last_highs[-1][1] > last_highs[-2][1]
    hl = len(last_lows) >= 2 and last_lows[-1][1] > last_lows[-2][1]
    lh = len(last_highs) >= 2 and last_highs[-1][1] < last_highs[-2][1]
    ll = len(last_lows) >= 2 and last_lows[-1][1] < last_lows[-2][1]
    current = float(c.iloc[-1])
    prev_res = last_highs[-1][1] if last_highs else float(h.tail(20).max())
    prev_sup = last_lows[-1][1] if last_lows else float(l.tail(20).min())
    bos_up = current > prev_res and len(last_highs) >= 2
    bos_down = current < prev_sup and len(last_lows) >= 2
    structure = (
        "BULLISH" if hh and hl
        else "BEARISH" if lh and ll
        else "TRANSITION" if hh or hl or lh or ll
        else "RANGE"
    )
    return {
        "structure": structure, "hh": hh, "hl": hl, "lh": lh, "ll": ll,
        "bos_up": bos_up, "bos_down": bos_down,
        "swing_high": prev_res, "swing_low": prev_sup,
    }

def _divergence(close, rsi, window=35):
    c = close.tail(window).reset_index(drop=True)
    r = rsi.tail(window).reset_index(drop=True)
    if len(c) < 15:
        return "NONE"
    half = max(5, len(c) // 2)
    c1, c2 = float(c.iloc[:half].min()), float(c.iloc[half:].min())
    r1, r2 = float(r.iloc[:half].min()), float(r.iloc[half:].min())
    h1, h2 = float(c.iloc[:half].max()), float(c.iloc[half:].max())
    rh1, rh2 = float(r.iloc[:half].max()), float(r.iloc[half:].max())
    if c2 < c1 and r2 > r1 + 2: return "BULLISH"
    if h2 > h1 and rh2 < rh1 - 2: return "BEARISH"
    return "NONE"

def _tf_decision(symbol, df, timeframe):
    if df is None or len(df) < 100:
        return None
    z = _advanced_indicators(df)
    if not z:
        return None
    c, h, l, v = z["c"], z["h"], z["l"], z["v"]
    current = float(c.iloc[-1])
    e9, e21, e50 = map(lambda s: float(s.iloc[-1]), (z["ema9"], z["ema21"], z["ema50"]))
    e200 = _safe_float(z["ema200"].iloc[-1], e50)
    rsi = _safe_float(z["rsi"].iloc[-1], 50)
    macd = _safe_float(z["macd"].iloc[-1]); macd_sig = _safe_float(z["macd_sig"].iloc[-1])
    hist = _safe_float(z["macd_hist"].iloc[-1]); prev_hist = _safe_float(z["macd_hist"].iloc[-2], hist)
    atr = max(_safe_float(z["atr"].iloc[-1]), current * 0.005)
    atr_pct = atr / current * 100 if current else 0
    adx = _safe_float(z["adx"].iloc[-1])
    dip = _safe_float(z["plus_di"].iloc[-1]); dim = _safe_float(z["minus_di"].iloc[-1])
    vwap = _safe_float(z["vwap"].iloc[-1], current)
    bb_u = _safe_float(z["bb_upper"].iloc[-1], current)
    bb_l = _safe_float(z["bb_lower"].iloc[-1], current)
    bb_m = _safe_float(z["bb_mid"].iloc[-1], current)
    bb_width = _safe_float(z["bb_width"].iloc[-1])
    vol_ratio = _safe_float(v.iloc[-1] / z["vol_ma20"].iloc[-1], 1) if _safe_float(z["vol_ma20"].iloc[-1]) > 0 else 1
    stoch_k = _safe_float(z["stoch_k"].iloc[-1], 50); stoch_d = _safe_float(z["stoch_d"].iloc[-1], 50)
    roc5 = _safe_float(z["roc5"].iloc[-1]); roc20 = _safe_float(z["roc20"].iloc[-1])
    structure = _market_structure(df)
    divergence = _divergence(c, z["rsi"])

    trend = (
        "BULLISH" if current > e21 > e50 > e200
        else "BEARISH" if current < e21 < e50 < e200
        else "BULLISH_WEAK" if current > e50
        else "BEARISH_WEAK" if current < e50
        else "NEUTRAL"
    )

    bull, bear = 0.0, 0.0
    rb, rs = [], []

    if current > e21 > e50 > e200:
        bull += 20; rb.append("روند اصلی صعودی و EMAها هم‌راستا هستند")
    elif current < e21 < e50 < e200:
        bear += 20; rs.append("روند اصلی نزولی و EMAها هم‌راستا هستند")
    elif current > e50:
        bull += 9; rb.append("قیمت بالای EMA50 است")
    elif current < e50:
        bear += 9; rs.append("قیمت زیر EMA50 است")

    if structure["bos_up"]:
        bull += 14; rb.append("شکست ساختار صعودی (BOS) تأیید شده")
    elif structure["bos_down"]:
        bear += 14; rs.append("شکست ساختار نزولی (BOS) تأیید شده")
    if structure["structure"] == "BULLISH":
        bull += 6; rb.append("ساختار HH/HL صعودی")
    elif structure["structure"] == "BEARISH":
        bear += 6; rs.append("ساختار LH/LL نزولی")

    if macd > macd_sig and hist > prev_hist:
        bull += 10; rb.append("MACD و مومنتوم در حال تقویت")
    elif macd < macd_sig and hist < prev_hist:
        bear += 10; rs.append("MACD و مومنتوم در حال تضعیف")

    if 52 <= rsi <= 68:
        bull += 5; rb.append("RSI در محدوده سازنده")
    elif 32 <= rsi < 45:
        bear += 3; rs.append("RSI ضعیف است")
    elif rsi > 74:
        bear += 5; rs.append("RSI بیش‌خرید و ریسک اصلاح")
    elif rsi < 28:
        bull += 3; rb.append("RSI اشباع فروش؛ نیازمند تأیید ساختار")

    if adx >= 25 and dip > dim:
        bull += 8; rb.append("ADX و +DI روند صعودی را تأیید می‌کنند")
    elif adx >= 25 and dim > dip:
        bear += 8; rs.append("ADX و -DI روند نزولی را تأیید می‌کنند")
    elif adx < 18:
        rb.append("بازار روند قدرتمندی ندارد")

    if vol_ratio >= 1.30 and current > c.iloc[-2]:
        bull += 8; rb.append("افزایش حجم همراه حرکت صعودی")
    elif vol_ratio >= 1.30 and current < c.iloc[-2]:
        bear += 8; rs.append("افزایش حجم همراه حرکت نزولی")
    elif vol_ratio < 0.70:
        rb.append("حجم پایین؛ شکست نیازمند احتیاط است")

    if current > vwap: bull += 3; rb.append("قیمت بالای VWAP")
    else: bear += 3; rs.append("قیمت زیر VWAP")

    if current > bb_m and current < bb_u: bull += 3
    elif current < bb_m and current > bb_l: bear += 3

    if stoch_k > stoch_d and stoch_k < 85: bull += 4
    elif stoch_k < stoch_d and stoch_k > 15: bear += 4

    if divergence == "BULLISH":
        bull += 6; rb.append("واگرایی مثبت RSI")
    elif divergence == "BEARISH":
        bear += 6; rs.append("واگرایی منفی RSI")

    extension = (current - e21) / atr if atr else 0
    if extension > 2.0:
        bull -= 9; rb.append("قیمت از EMA21 بیش از حد کشیده شده")
    if extension < -2.0:
        bear -= 9; rs.append("قیمت از EMA21 بیش از حد نزولی کشیده شده")

    recent_res = float(h.iloc[-21:-1].max()) if len(h) >= 21 else float(h.max())
    recent_sup = float(l.iloc[-21:-1].min()) if len(l) >= 21 else float(l.min())
    breakout_up = current > recent_res
    breakout_down = current < recent_sup
    breakout_confirmed = False
    fakeout_risk = False
    if breakout_up:
        if vol_ratio >= 1.15 and current > recent_res + 0.15 * atr:
            bull += 7; rb.append("شکست مقاومت با حجم و فاصله کافی"); breakout_confirmed = True
        else:
            bull -= 5; rb.append("شکست مقاومت بدون تأیید کافی؛ ریسک فیک‌اوت"); fakeout_risk = True
    elif breakout_down:
        if vol_ratio >= 1.15 and current < recent_sup - 0.15 * atr:
            bear += 7; rs.append("شکست حمایت با حجم و فاصله کافی"); breakout_confirmed = True
        else:
            bear -= 5; rs.append("شکست حمایت بدون تأیید کافی؛ ریسک فیک‌اوت"); fakeout_risk = True

    bull, bear = max(0.0, bull), max(0.0, bear)
    edge = bull - bear
    strength = _clamp(45 + max(bull, bear) * 0.70 + (8 if adx >= 25 else 0))
    signal_score = _clamp(50 + abs(edge) * 1.15)

    # SYMMETRIC RSI GATES (fix #12)
    buy_ok = (
        bull >= 62 and edge >= 18 and
        trend in ("BULLISH", "BULLISH_WEAK") and
        structure["structure"] != "BEARISH" and
        40 < rsi < 72 and
        not fakeout_risk
    )
    sell_ok = (
        bear >= 62 and edge <= -18 and
        trend in ("BEARISH", "BEARISH_WEAK") and
        structure["structure"] != "BULLISH" and
        35 < rsi < 68 and
        not fakeout_risk
    )
    candidate = "BUY" if buy_ok else "SELL" if sell_ok else "WAIT"

    recent_low = float(l.tail(20).min()); recent_high = float(h.tail(20).max())
    if candidate == "BUY":
        stop = min(recent_low, current - 1.35 * atr)
        risk = current - stop
        if risk <= 0: risk = 1.35 * atr; stop = current - risk
        targets = [current + 1.5 * risk, current + 2.5 * risk, current + 3.5 * risk]
    elif candidate == "SELL":
        stop = max(recent_high, current + 1.35 * atr)
        risk = stop - current
        if risk <= 0: risk = 1.35 * atr; stop = current + risk
        targets = [current - 1.5 * risk, current - 2.5 * risk, current - 3.5 * risk]
    else:
        stop, risk, targets = 0.0, 0.0, [0.0, 0.0, 0.0]

    rr = abs((targets[1] - current) / risk) if risk else 0.0
    if candidate != "WAIT" and rr < 1.8:
        candidate = "WAIT"

    return {
        "timeframe": timeframe, "price": current,
        "ema9": e9, "ema21": e21, "ema50": e50, "ema200": e200,
        "rsi": rsi, "macd": macd, "macd_signal": macd_sig, "macd_hist": hist,
        "bb_upper": bb_u, "bb_mid": bb_m, "bb_lower": bb_l,
        "bb_position": ((current - bb_l) / (bb_u - bb_l) * 100 if bb_u > bb_l else 50),
        "bb_width": bb_width, "atr": atr, "atr_pct": atr_pct,
        "adx": adx, "di_plus": dip, "di_minus": dim,
        "vwap": vwap, "stoch_k": stoch_k, "stoch_d": stoch_d,
        "volume_ratio": vol_ratio, "roc5": roc5, "roc20": roc20,
        "support": recent_low, "resistance": recent_res,
        "trend": trend, "structure": structure["structure"],
        "bos_up": structure["bos_up"], "bos_down": structure["bos_down"],
        "divergence": divergence, "breakout_confirmed": breakout_confirmed,
        "fakeout_risk": fakeout_risk,
        "buy_score": bull, "sell_score": bear,
        "signal_candidate": candidate, "signal_score": signal_score,
        "strength": strength,
        "probability": _clamp(50 + abs(edge) * 0.72 + (7 if vol_ratio >= 1.15 else 0)),
        "reasons_buy": list(dict.fromkeys(rb))[:8],
        "reasons_sell": list(dict.fromkeys(rs))[:8],
        "stop": stop, "targets": targets, "rr": rr,
        "history_points": len(df), "data_source": "OKX OHLCV professional",
    }

async def professional_crypto_analysis(symbol):
    # All signals are calculated from historical, CLOSED OKX candles.
    # The short timeframes are for timing; 4H/1D/1W control the larger trend.
    bars = {"5m": 240, "15m": 240, "1H": 240, "4H": 240, "1D": 240, "1W": 240}
    sem = asyncio.Semaphore(OKX_MAX_CONCURRENCY)

    async def one(tf, limit):
        async with sem:
            return tf, await okx_candles(symbol, tf, limit)

    pairs = await asyncio.gather(*(one(tf, n) for tf, n in bars.items()), return_exceptions=True)

    tfs = {}
    for item in pairs:
        if isinstance(item, Exception):
            log.warning("TF analysis failed %s: %s", symbol, item)
            continue
        tf, df = item
        try:
            a = _tf_decision(symbol, df, tf)
            if a: tfs[tf] = a
        except Exception:
            log.exception("TF engine error %s %s", symbol, tf)

    if len(tfs) < 3:
        return None

    weights = {"5m": 0.05, "15m": 0.10, "1H": 0.20, "4H": 0.30, "1D": 0.25, "1W": 0.10}
    total_w = sum(weights[k] for k in tfs)
    weights = {k: weights[k] / total_w for k in tfs}

    bull = sum(weights[k] * tfs[k]["buy_score"] for k in tfs)
    bear = sum(weights[k] * tfs[k]["sell_score"] for k in tfs)
    edge = bull - bear

    buy_agreement = sum(weights[k] for k in tfs if tfs[k]["signal_candidate"] == "BUY")
    sell_agreement = sum(weights[k] for k in tfs if tfs[k]["signal_candidate"] == "SELL")
    bullish_trend_weight = sum(weights[k] for k in tfs if tfs[k]["trend"] in ("BULLISH", "BULLISH_WEAK"))
    bearish_trend_weight = sum(weights[k] for k in tfs if tfs[k]["trend"] in ("BEARISH", "BEARISH_WEAK"))

    main = tfs.get("4H") or tfs.get("1H") or tfs.get("1D")
    trend4 = tfs.get("4H", main).get("trend")
    structure4 = tfs.get("4H", main).get("structure")
    trend1d = tfs.get("1D", main).get("trend")
    trend1w = tfs.get("1W", main).get("trend")
    structure1d = tfs.get("1D", main).get("structure")

    # Higher-timeframe guard: short-term momentum cannot override a clearly
    # bearish daily/weekly structure for a BUY (and vice versa for SELL).
    buy_veto = (
        trend4 in ("BEARISH", "BEARISH_WEAK") or structure4 == "BEARISH"
        or trend1d == "BEARISH" or structure1d == "BEARISH"
        or trend1w == "BEARISH" or bullish_trend_weight < 0.50
    )
    sell_veto = (
        trend4 in ("BULLISH", "BULLISH_WEAK") or structure4 == "BULLISH"
        or trend1d == "BULLISH" or structure1d == "BULLISH"
        or trend1w == "BULLISH" or bearish_trend_weight < 0.50
    )

    candidate = "WAIT"
    if bull >= 63 and edge >= 18 and buy_agreement >= 0.55 and not buy_veto:
        candidate = "BUY"
    elif bear >= 63 and edge <= -18 and sell_agreement >= 0.55 and not sell_veto:
        candidate = "SELL"

    agreement = buy_agreement if candidate == "BUY" else sell_agreement if candidate == "SELL" else max(buy_agreement, sell_agreement)
    agreement_score = _clamp(agreement * 100)

    strength = _clamp(
        sum(weights[k] * _safe_float(tfs[k]["strength"], 50) for k in tfs)
        + (8 if agreement >= 0.70 else 3 if agreement >= 0.55 else 0)
    )

    confidence = _clamp(
        50 + abs(edge) * 0.70
        + max(0, agreement_score - 50) * 0.22
        + (5 if main.get("adx", 0) >= 25 else 0)
    )

    if candidate == "BUY":
        reasons = list(dict.fromkeys(
            sum((tfs[k].get("reasons_buy") or []) for k in ("15m", "1H", "4H", "1D", "1W") if k in tfs)
        ))[:8]
    elif candidate == "SELL":
        reasons = list(dict.fromkeys(
            sum((tfs[k].get("reasons_sell") or []) for k in ("15m", "1H", "4H", "1D", "1W") if k in tfs)
        ))[:8]
    else:
        reasons = ["شرایط هم‌زمان روند، ساختار و تأیید چندتایم‌فریمی برای ورود کافی نیست"]

    out = dict(main)
    out.update({
        "symbol": norm_symbol(symbol),
        "signal_candidate": candidate,
        "signal_score": _clamp(50 + abs(edge) * 1.15),
        "strength": strength,
        "probability": confidence,
        "reasons_buy": reasons if candidate == "BUY" else [],
        "reasons_sell": reasons if candidate == "SELL" else [],
        "reasons": reasons,
        "mtf": tfs,
        "mtf_agreement": agreement_score,
        "bull_weight": bull,
        "bear_weight": bear,
        "buy_agreement": buy_agreement * 100,
        "sell_agreement": sell_agreement * 100,
        "data_source": "OKX OHLCV professional multi-timeframe",
        "history_points": sum(v["history_points"] for v in tfs.values()),
    })
    return out

# ============================================================
# FALLBACK ANALYSIS (close-only, for GOLD18/XAU)
# ============================================================

def _rsi(series, period=14):
    series = pd.to_numeric(series, errors="coerce").astype(float)
    delta = series.diff()
    gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, float('nan'))
    out = 100 - (100 / (1 + rs))
    out = out.where(avg_loss != 0, 100)
    return out

def _atr(p, period=14):
    tr = p.diff().abs()
    return tr.ewm(alpha=1/period, adjust=False, min_periods=period).mean()

def _adx(p, period=14):
    """Close-only ADX approximation.
    For real OHLC ADX use _advanced_indicators(). Used only for GOLD18/XAU
    fallback where only close prices are available."""
    move = p.diff()
    up = move.clip(lower=0); down = (-move).clip(lower=0); tr = move.abs()
    atr = tr.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    dip = 100 * up.ewm(alpha=1/period, adjust=False, min_periods=period).mean() / atr.replace(0, float('nan'))
    dim = 100 * down.ewm(alpha=1/period, adjust=False, min_periods=period).mean() / atr.replace(0, float('nan'))
    dx = (100 * (dip - dim).abs() / (dip + dim).replace(0, float('nan')))
    adx = dx.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    return adx, dip, dim

def _format_price(symbol, value):
    if symbol == "GOLD18": return f"{value:,.0f}"
    if symbol == "XAU": return f"{value:,.2f}"
    if abs(value) >= 1000: return f"{value:,.2f}"
    if abs(value) >= 1: return f"{value:,.4f}"
    return f"{value:,.8f}".rstrip("0").rstrip(".")

def technical_analysis(symbol, prices, volumes=None):
    p = pd.to_numeric(pd.Series(prices), errors="coerce")
    p = p.replace([float("inf"), float("-inf")], float("nan")).dropna().astype(float).reset_index(drop=True)
    if len(p) < 50:
        return None

    ema9 = p.ewm(span=9, adjust=False).mean()
    ema21 = p.ewm(span=21, adjust=False).mean()
    ema50 = p.ewm(span=50, adjust=False).mean()
    ema200 = p.ewm(span=200, adjust=False, min_periods=50).mean()

    rsi_s = _rsi(p, 14)
    rsi = _safe_float(rsi_s.iloc[-1], 50)

    macd_line = p.ewm(span=12, adjust=False).mean() - p.ewm(span=26, adjust=False).mean()
    macd_signal = macd_line.ewm(span=9, adjust=False).mean()
    macd_hist = macd_line - macd_signal

    bb_mid = p.rolling(20).mean()
    bb_std = p.rolling(20).std(ddof=0)
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std

    atr_s = _atr(p, 14)
    atr = _safe_float(atr_s.iloc[-1], 0)
    adx_s, dip_s, dim_s = _adx(p, 14)
    adx = _safe_float(adx_s.iloc[-1], 0)
    dip = _safe_float(dip_s.iloc[-1], 0)
    dim = _safe_float(dim_s.iloc[-1], 0)

    current = float(p.iloc[-1])
    e9, e21, e50 = float(ema9.iloc[-1]), float(ema21.iloc[-1]), float(ema50.iloc[-1])
    e200 = _safe_float(ema200.iloc[-1], e50)
    mline, msignal, mhist = float(macd_line.iloc[-1]), float(macd_signal.iloc[-1]), float(macd_hist.iloc[-1])
    prev_hist = _safe_float(macd_hist.iloc[-2], mhist)
    mid = _safe_float(bb_mid.iloc[-1], current)
    upper = _safe_float(bb_upper.iloc[-1], current)
    lower = _safe_float(bb_lower.iloc[-1], current)

    r1 = ((current / p.iloc[-2]) - 1) * 100 if p.iloc[-2] else 0
    r6 = ((current / p.iloc[-7]) - 1) * 100 if len(p) >= 7 and p.iloc[-7] else r1
    r24 = ((current / p.iloc[-25]) - 1) * 100 if len(p) >= 25 and p.iloc[-25] else r6
    r72 = ((current / p.iloc[-73]) - 1) * 100 if len(p) >= 73 and p.iloc[-73] else r24

    recent = p.tail(min(50, len(p)))
    resistance = float(recent.max()); support = float(recent.min())
    atr_pct = (atr / current * 100) if current else 0
    bb_position = ((current - lower) / (upper - lower) * 100) if upper > lower else 50

    if current > e9 > e21 > e50 and current > e200:
        trend = "BULLISH"
    elif current < e9 < e21 < e50 and current < e200:
        trend = "BEARISH"
    elif current > e50:
        trend = "BULLISH_WEAK"
    elif current < e50:
        trend = "BEARISH_WEAK"
    else:
        trend = "NEUTRAL"

    components = [
        1 if current > e21 else -1,
        1 if e9 > e21 else -1,
        1 if e21 > e50 else -1,
        1 if current > e200 else -1,
        1 if 50 <= rsi <= 68 else -1 if rsi < 40 or rsi > 75 else 0,
        1 if mhist > 0 else -1,
        1 if adx >= 20 else 0,
        1 if r6 > 0 else -1 if r6 < 0 else 0,
    ]
    raw_strength = 50 + sum(components) * 5
    strength = float(max(10, min(95, raw_strength)))

    buy_points = 0.0; sell_points = 0.0
    reasons_buy, reasons_sell = [], []

    if current > e21 > e50:
        buy_points += 18; reasons_buy.append("روند و EMA تأیید")
    elif current < e21 < e50:
        sell_points += 18; reasons_sell.append("روند و EMA تأیید")

    if mline > msignal and mhist > prev_hist:
        buy_points += 16; reasons_buy.append("MACD صعودی")
    elif mline < msignal and mhist < prev_hist:
        sell_points += 16; reasons_sell.append("MACD نزولی")

    if 52 <= rsi <= 68:
        buy_points += 12; reasons_buy.append("RSI مناسب خرید")
    elif 32 <= rsi <= 48:
        sell_points += 12; reasons_sell.append("RSI مناسب فروش")

    if adx >= 25 and dip > dim:
        buy_points += 14; reasons_buy.append("قدرت روند +DI")
    elif adx >= 25 and dim > dip:
        sell_points += 14; reasons_sell.append("قدرت روند -DI")

    if current > mid and current < upper * 0.995:
        buy_points += 10; reasons_buy.append("موقعیت Bollinger مناسب")
    elif current < mid and current > lower * 1.005:
        sell_points += 10; reasons_sell.append("موقعیت Bollinger مناسب")

    if r6 > 0 and r24 > 0:
        buy_points += 10; reasons_buy.append("مومنتوم مثبت")
    elif r6 < 0 and r24 < 0:
        sell_points += 10; reasons_sell.append("مومنتوم منفی")

    # FIX #14: proper volume alignment
    vol_confirm = None
    if volumes is not None:
        v = pd.Series(volumes, dtype=float)
        if len(v) > len(p):
            v = v.iloc[-len(p):].reset_index(drop=True)
        elif len(v) < len(p):
            pad = len(p) - len(v)
            v = pd.concat([pd.Series([float("nan")] * pad), v], ignore_index=True)
        v = v.dropna()
        if len(v) >= 20:
            vma = v.rolling(20).mean().iloc[-1]
            if vma and vma > 0:
                vol_confirm = bool(v.iloc[-1] >= vma * 1.10)
                if vol_confirm and r1 > 0:
                    buy_points += 10; reasons_buy.append("حجم تأییدکننده")
                elif vol_confirm and r1 < 0:
                    sell_points += 10; reasons_sell.append("حجم تأییدکننده")

    best = max(buy_points, sell_points)
    second = min(buy_points, sell_points)
    direction = "BUY" if buy_points > sell_points else "SELL" if sell_points > buy_points else "WAIT"
    score = float(best)
    candidate = direction if score >= SIGNAL_MIN_SCORE and (score - second) >= 20 else "WAIT"

    if candidate == "BUY":
        stop = current - max(atr * 1.5, current * 0.005)
        risk = max(current - stop, current * 0.003)
        targets = [current + risk * 1.5, current + risk * 2.5, current + risk * 3.5]
    elif candidate == "SELL":
        stop = current + max(atr * 1.5, current * 0.005)
        risk = max(stop - current, current * 0.003)
        targets = [current - risk * 1.5, current - risk * 2.5, current - risk * 3.5]
    else:
        stop = 0.0; targets = [0.0, 0.0, 0.0]; risk = 0.0

    rr = 0.0 if not risk else abs((targets[1] - current) / risk)
    if candidate != "WAIT" and rr < 1.5:
        candidate = "WAIT"

    probability = float(max(5, min(95, 50 + (score - second) * 0.9)))

    return {
        "symbol": symbol, "price": current,
        "ema9": e9, "ema21": e21, "ema50": e50, "ema200": e200,
        "rsi": rsi, "macd": mline, "macd_signal": msignal, "macd_hist": mhist,
        "bb_upper": upper, "bb_mid": mid, "bb_lower": lower, "bb_position": bb_position,
        "atr": atr, "atr_pct": atr_pct, "adx": adx, "di_plus": dip, "di_minus": dim,
        "r1": float(r1), "r6": float(r6), "r24": float(r24), "r72": float(r72),
        "support": support, "resistance": resistance, "trend": trend,
        "strength": strength, "signal_candidate": candidate, "signal_score": score,
        "buy_score": buy_points, "sell_score": sell_points,
        "probability": probability, "reasons_buy": reasons_buy, "reasons_sell": reasons_sell,
        "stop": stop, "targets": targets, "rr": rr, "volume_confirm": vol_confirm,
        "history_points": len(p),
        "signal": "WAIT", "confirmed": False, "confirmations": 0,
        "data_source": "TGJU (close-only)" if symbol in ("GOLD18", "XAU") else "CoinGecko (close-only)",
    }

# ============================================================
# SIGNAL CONFIRMATION (BUCKET-BASED, FIX #5)
# ============================================================

def _timeframe_bucket(timeframe):
    tf_seconds = {"5m": 300, "15m": 900, "1H": 3600, "4H": 14400, "1D": 86400, "1W": 604800}.get(timeframe, 3600)
    bucket = int(time.time() // tf_seconds)
    return f"{timeframe}:{bucket}"

def confirm_signal(symbol, candidate, price, timeframe=None):
    """Confirm a signal on successive CLOSED candles of the given timeframe."""
    timeframe = timeframe or SIGNAL_CONFIRM_TIMEFRAME
    if candidate == "WAIT":
        with db() as c:
            c.execute("DELETE FROM signal_state WHERE symbol=?", (symbol,))
        return "WAIT", False, 0

    observation_key = _timeframe_bucket(timeframe)
    iso_now = now_iso()
    with db() as c:
        row = c.execute("SELECT * FROM signal_state WHERE symbol=?", (symbol,)).fetchone()
        if row and row["candidate"] == candidate:
            confirmations = int(row["confirmations"])
            if row["last_candidate_at"] != observation_key:
                confirmations = min(SIGNAL_CONFIRMATIONS_REQUIRED, confirmations + 1)
        else:
            confirmations = 1
        c.execute("""
            INSERT INTO signal_state(symbol,candidate,confirmations,last_candidate_at,last_price,updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(symbol) DO UPDATE SET
                candidate=excluded.candidate,
                confirmations=excluded.confirmations,
                last_candidate_at=excluded.last_candidate_at,
                last_price=excluded.last_price,
                updated_at=excluded.updated_at
        """, (symbol, candidate, confirmations, observation_key, float(price), iso_now))
    confirmed = confirmations >= SIGNAL_CONFIRMATIONS_REQUIRED
    return (candidate if confirmed else "WAIT"), confirmed, confirmations

async def crypto_market_context(symbol):
    """Supplement technical OHLCV with independent market/liquidity context."""
    sym = norm_symbol(symbol)
    cid = COINS.get(sym)
    if not cid:
        return None
    rows = await http_json(
        "https://api.coingecko.com/api/v3/coins/markets",
        {"vs_currency": "usd", "ids": cid,
         "price_change_percentage": "24h,7d,30d"},
        ttl=90, retries=2,
    )
    if not rows or not isinstance(rows, list):
        return None
    x = rows[0]
    try:
        price = _safe_float(x.get("current_price"))
        if price <= 0:
            return None
        return {
            "price": price,
            "market_cap": _safe_float(x.get("market_cap")),
            "volume_24h": _safe_float(x.get("total_volume")),
            "rank": int(x.get("market_cap_rank") or 0),
            "change_24h": _safe_float(x.get("price_change_percentage_24h_in_currency", x.get("price_change_percentage_24h"))),
            "change_7d": _safe_float(x.get("price_change_percentage_7d_in_currency", x.get("price_change_percentage_7d"))),
            "change_30d": _safe_float(x.get("price_change_percentage_30d_in_currency", x.get("price_change_percentage_30d"))),
            "circulating_supply": _safe_float(x.get("circulating_supply")),
            "max_supply": _safe_float(x.get("max_supply")),
            "source": "CoinGecko",
        }
    except Exception:
        log.exception("market context parse failed: %s", sym)
        return None

async def analyze(symbol):
    s = norm_symbol(symbol)
    cached = ANALYSIS_CACHE.get(s)
    if cached and time.monotonic() - cached[0] < ANALYSIS_CACHE_SECONDS:
        return cached[1]

    if s not in ("GOLD18", "XAU"):
        result = await professional_crypto_analysis(s)
        if result:
            confirmed_signal, confirmed, confirmations = confirm_signal(
                s, result["signal_candidate"], result["price"], SIGNAL_CONFIRM_TIMEFRAME
            )
            result["signal"] = confirmed_signal
            result["confirmed"] = confirmed
            result["confirmations"] = confirmations
            try:
                result["market_context"] = await asyncio.wait_for(crypto_market_context(s), timeout=10)
            except Exception as e:
                log.warning("market context unavailable %s: %s", s, e)
                result["market_context"] = None
            ANALYSIS_CACHE[s] = (time.monotonic(), result)
            return result

    data = await asset_data(s)
    if not data:
        return None
    if s in ("GOLD18", "XAU") and len(data[1]) < GOLD_MIN_HISTORY_POINTS:
        return {
            "symbol": s, "price": float(data[1].iloc[-1]),
            "insufficient_history": True, "history_points": len(data[1]),
            "signal": "WAIT"
        }
    result = technical_analysis(data[0], data[1], data[2])
    if not result:
        return None
    confirmed_signal, confirmed, confirmations = confirm_signal(
        s, result["signal_candidate"], result["price"], SIGNAL_CONFIRM_TIMEFRAME
    )
    result["signal"] = confirmed_signal
    result["confirmed"] = confirmed
    result["confirmations"] = confirmations
    ANALYSIS_CACHE[s] = (time.monotonic(), result)
    return result

def signal_fa(s):
    return {"BUY":"🟢 خرید","SELL":"🔴 فروش","WAIT":"🟡 انتظار"}.get(s, s)

def _market_context_text(ctx):
    if not ctx:
        return "📊 آمار بازار تکمیلی: در دسترس نیست.\n"
    def money(v):
        v = _safe_float(v)
        if v >= 1_000_000_000:
            return f"${v/1_000_000_000:.2f}B"
        if v >= 1_000_000:
            return f"${v/1_000_000:.2f}M"
        if v >= 1_000:
            return f"${v/1_000:.2f}K"
        return f"${v:.2f}"
    rank = f"#{ctx['rank']}" if ctx.get("rank") else "نامشخص"
    supply = ctx.get("circulating_supply", 0)
    supply_text = f"{supply:,.0f}" if supply else "نامشخص"
    max_supply = ctx.get("max_supply", 0)
    max_text = f"{max_supply:,.0f}" if max_supply else "نامحدود/نامشخص"
    return (
        "🌐 <b>نمای کلی بازار (CoinGecko)</b>\n"
        f"رتبه: {rank} | ارزش بازار: {money(ctx.get('market_cap'))} | حجم ۲۴ساعته: {money(ctx.get('volume_24h'))}\n"
        f"تغییر: ۲۴ساعت {ctx.get('change_24h', 0):+.2f}% | ۷روز {ctx.get('change_7d', 0):+.2f}% | ۳۰روز {ctx.get('change_30d', 0):+.2f}%\n"
        f"عرضه در گردش: {supply_text} | حداکثر عرضه: {max_text}\n"
    )

def analysis_text(a):
    """Pure technical dashboard. No trade signal."""
    if not a:
        return "❌ اطلاعات بازار در دسترس نیست."
    if a.get("insufficient_history"):
        return (f"📊 <b>داشبورد تحلیل {escape(a['symbol'])}</b>\n\n"
                f"💰 قیمت فعلی: <b>{_format_price(a['symbol'], a['price'])}</b>\n"
                f"📚 تاریخچه قابل استفاده: {a['history_points']} نقطه از {GOLD_MIN_HISTORY_POINTS} نقطه لازم\n\n"
                "⏳ برای محاسبه اندیکاتورها هنوز تاریخچه واقعی کافی جمع نشده است.")
    unit = "تومان" if a["symbol"] == "GOLD18" else "دلار"
    trend_fa = {"BULLISH":"صعودی قوی","BULLISH_WEAK":"صعودی ضعیف",
                "BEARISH":"نزولی قوی","BEARISH_WEAK":"نزولی ضعیف",
                "NEUTRAL":"خنثی"}.get(a["trend"], a["trend"])
    source = a.get("data_source") or ("TGJU (close-only)" if a["symbol"] in ("GOLD18","XAU") else "CoinGecko (close-only)")
    is_close_only = a["symbol"] in ("GOLD18", "XAU") or "close-only" in source
    adx_label = "ADX≈" if is_close_only else "ADX"
    if a["symbol"] in ("GOLD18", "XAU"):
        candle_note = "⏱ داده‌های نقطه‌ای TGJU (تقریباً هر ۵ دقیقه؛ کندل OHLC واقعی نیست)\\n"
    elif is_close_only:
        candle_note = "⏱ داده‌های قیمت CoinGecko؛ کندل OHLC کامل در دسترس نیست\\n"
    else:
        candle_note = "⏱ کندل‌های بسته‌شده OKX در چند تایم‌فریم (نمای اصلی 4H)\\n"
    mtf = a.get("mtf") or {}
    tf_labels = {"5m": "۵دقیقه", "15m": "۱۵دقیقه", "1H": "۱ساعت", "4H": "۴ساعت", "1D": "روزانه", "1W": "هفتگی"}
    tf_lines = []
    for tf in ("5m", "15m", "1H", "4H", "1D", "1W"):
        item = mtf.get(tf)
        if not item:
            tf_lines.append(f"• {tf_labels[tf]}: داده کافی/در دسترس نیست")
            continue
        tr = {"BULLISH": "صعودی قوی", "BULLISH_WEAK": "صعودی ضعیف", "BEARISH": "نزولی قوی", "BEARISH_WEAK": "نزولی ضعیف", "NEUTRAL": "خنثی"}.get(item.get("trend"), item.get("trend", "نامشخص"))
        sig = {"BUY": "خرید", "SELL": "فروش", "WAIT": "انتظار"}.get(item.get("signal_candidate"), "انتظار")
        tf_lines.append(f"• {tf_labels[tf]}: {tr} | RSI {item.get('rsi', 0):.1f} | {sig}")
    mtf_text = "\n".join(tf_lines)
    return (
        f"📊 <b>داشبورد تحلیل تکنیکال {escape(a['symbol'])}</b>\n\n"
        f"💰 قیمت: <b>{_format_price(a['symbol'], a['price'])} {unit}</b>\n"
        f"📈 ساختار روند اصلی: <b>{trend_fa}</b>\n\n"
        f"🕒 <b>تأیید چندتایم‌فریمی (تاریخچه کندل‌های بسته‌شده)</b>\n{mtf_text}\n\n"
        f"EMA9: {_format_price(a['symbol'], a['ema9'])} | EMA21: {_format_price(a['symbol'], a['ema21'])}\n"
        f"EMA50: {_format_price(a['symbol'], a['ema50'])} | EMA200: {_format_price(a['symbol'], a['ema200'])}\n"
        f"RSI14: <b>{a['rsi']:.1f}</b> | {adx_label}: <b>{a['adx']:.1f}</b>\n"
        f"MACD: {a['macd']:.5f} | Histogram: {a['macd_hist']:+.5f}\n"
        f"Bollinger: <b>{a['bb_position']:.1f}%</b> | ATR: {_format_price(a['symbol'], a['atr'])} ({a['atr_pct']:.2f}%)\n"
        f"حمایت: {_format_price(a['symbol'], a['support'])} | مقاومت: {_format_price(a['symbol'], a['resistance'])}\n\n"
        f"مومنتوم: 1 کندل {a.get('r1', 0):+.2f}% | 6 کندل {a.get('r6', 0):+.2f}% | "
        f"24 کندل {a.get('r24', 0):+.2f}% | 72 کندل {a.get('r72', 0):+.2f}%\n"
        f"{candle_note}\n"
        f"💪 <b>قدرت تکنیکال: {a['strength']:.0f}%</b>\n"
        f"🧭 هم‌جهتی تایم‌فریم‌ها: <b>{a.get('mtf_agreement', 0):.0f}%</b>\n"
        f"🔬 وضعیت ساختار: {'قوی' if a['strength'] >= 75 else 'متوسط' if a['strength'] >= 55 else 'ضعیف'}\n"
        f"📚 تعداد داده: {a['history_points']} | منبع: {escape(source)}\n"
        + ("📥 تاریخچه رمزارز از کندل‌های تاریخی OKX دریافت شده است؛ تایم‌فریم‌های در دسترس بسته به نماد/محدودیت منبع ممکن است متفاوت باشد.\n"
           if a.get("mtf") else "")
        + (_market_context_text(a.get("market_context")) if a["symbol"] not in ("GOLD18", "XAU") else "")
        + ("\nℹ️ آمار بازار تکمیلی است و جایگزین بررسی بنیادی پروژه، اخبار و توکنومیک نیست.\n"
           if a["symbol"] not in ("GOLD18", "XAU") else
           "\nℹ️ تحلیل طلا بر پایه نقاط قیمت TGJU است؛ داده OHLC کامل و اطلاعات بنیادی در این موتور موجود نیست.\n")
        + "📡 برای تصمیم معاملاتی از منوی «سیگنال‌ها» استفاده کنید."
    )

def signal_text(a):
    if not a:
        return "❌ سیگنال قابل محاسبه نیست."
    if a.get("insufficient_history"):
        return f"📡 <b>سیگنال {escape(a['symbol'])}</b>\n\n⏳ تاریخچه واقعی کافی نیست؛ فعلاً <b>انتظار</b>."
    status = a.get("signal", "WAIT")
    status_fa = signal_fa(status)
    candidate = a.get("signal_candidate", "WAIT")
    candidate_fa = signal_fa(candidate)
    reasons = a.get("reasons_buy") if candidate == "BUY" else a.get("reasons_sell") if candidate == "SELL" else []
    reasons_text = "\n".join("• " + escape(x) for x in (reasons or [])[:6]) or "• تأیید کافی برای اقدام وجود ندارد"
    direction_note = "خرید" if candidate == "BUY" else "فروش" if candidate == "SELL" else "انتظار"
    setup = "🟢" if status == "BUY" else "🔴" if status == "SELL" else "🟡"
    levels = ""
    if status in ("BUY", "SELL"):
        levels = (
            f"\n💰 ورود: <b>{_format_price(a['symbol'], a['price'])}</b>\n"
            f"🛑 حد ضرر: <b>{_format_price(a['symbol'], a['stop'])}</b>\n"
            f"🎯 TP1: <b>{_format_price(a['symbol'], a['targets'][0])}</b>\n"
            f"🎯 TP2: <b>{_format_price(a['symbol'], a['targets'][1])}</b>\n"
            f"🎯 TP3: <b>{_format_price(a['symbol'], a['targets'][2])}</b>\n"
            f"⚖️ R/R: <b>1:{a['rr']:.2f}</b>\n"
        )
    return (
        f"📡 <b>سیگنال معاملاتی {escape(a['symbol'])}</b>\n\n"
        f"{setup} وضعیت فعلی: <b>{status_fa}</b>\n"
        f"🔎 کاندیدا: <b>{candidate_fa}</b>\n"
        f"🎯 امتیاز سیگنال: <b>{a['signal_score']:.0f}%</b>\n"
        f"🧠 اعتماد مدل: <b>{a['probability']:.0f}%</b>\n"
        f"🔁 تأیید متوالی: <b>{a['confirmations']}/{SIGNAL_CONFIRMATIONS_REQUIRED}</b> ({SIGNAL_CONFIRM_TIMEFRAME})\n"
        f"💪 قدرت تکنیکال: <b>{a['strength']:.0f}%</b>\n"
        f"📈 روند: <b>{a['trend']}</b> | RSI: <b>{a['rsi']:.1f}</b> | ADX: <b>{a['adx']:.1f}</b>\n\n"
        f"🧩 منطق {direction_note}:\n{reasons_text}\n"
        f"{levels}\n"
        "⚠️ این یک خروجی مدل تحلیلی است و سود قطعی یا تضمین‌شده نیست."
    )

# ============================================================
# WATCHLIST
# ============================================================

def _repair_watchlist_table():
    """Normalize legacy watchlist schemas without losing known watchlist data."""
    stamp = now_iso()
    with db() as c:
        info = c.execute("PRAGMA table_info(watchlist)").fetchall()
        if not info:
            c.execute("""
                CREATE TABLE IF NOT EXISTS watchlist(
                    user_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    asset_type TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    asset_key TEXT DEFAULT '',
                    PRIMARY KEY(user_id,symbol,asset_type)
                )
            """)
            return

        names = {r[1] for r in info}
        required = {"user_id", "symbol", "asset_type", "created_at"}
        if required.issubset(names):
            if "asset_key" in names:
                c.execute("UPDATE watchlist SET asset_key=upper(symbol) WHERE asset_key IS NULL OR asset_key=''")
            return

        legacy = f"watchlist_legacy_{int(time.time())}"
        c.execute("DROP INDEX IF EXISTS idx_watch_asset")
        c.execute(f'ALTER TABLE watchlist RENAME TO "{legacy}"')
        c.execute("""
            CREATE TABLE watchlist(
                user_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                asset_type TEXT NOT NULL,
                created_at TEXT NOT NULL,
                asset_key TEXT DEFAULT '',
                PRIMARY KEY(user_id,symbol,asset_type)
            )
        """)

        old_names = {r[1] for r in info}
        if "user_id" in old_names:
            symbol_expr = "symbol" if "symbol" in old_names else ("asset_key" if "asset_key" in old_names else "''")
            type_expr = "asset_type" if "asset_type" in old_names else "'crypto'"
            created_expr = "created_at" if "created_at" in old_names else "?"
            params = [stamp, stamp] if created_expr == "?" else []
            c.execute(f"""
                INSERT OR IGNORE INTO watchlist(user_id,symbol,asset_type,created_at,asset_key)
                SELECT user_id,
                       upper(COALESCE(NULLIF({symbol_expr},''), '')),
                       COALESCE({type_expr}, 'crypto'),
                       COALESCE({created_expr}, ?),
                       upper(COALESCE(NULLIF({symbol_expr},''), ''))
                FROM "{legacy}"
                WHERE user_id IS NOT NULL
                  AND COALESCE({symbol_expr}, '') <> ''
            """, params + ([stamp] if created_expr != "?" else []))
        c.execute("CREATE INDEX IF NOT EXISTS idx_watch_asset ON watchlist(asset_type,symbol)")
        log.warning("WATCHLIST SCHEMA NORMALIZED; legacy table preserved as %s", legacy)


def add_watch(uid, symbol, atype, asset_key=None):
    uid = int(uid)
    s = norm_symbol(symbol)
    atype = str(atype or asset_type(s))
    stamp = now_iso()
    asset_key = (asset_key or s).strip()

    def _insert_once():
        with db() as c:
            c.execute("""
                INSERT OR IGNORE INTO users(
                    user_id, username, first_name, created_at, last_seen
                ) VALUES(?,?,?,?,?)
            """, (uid, "", "", stamp, stamp))

            if c.execute("""
                SELECT 1 FROM watchlist
                WHERE user_id=? AND symbol=? AND asset_type=? LIMIT 1
            """, (uid, s, atype)).fetchone():
                return True

            n = int(c.execute(
                "SELECT COUNT(*) AS n FROM watchlist WHERE user_id=?", (uid,)
            ).fetchone()["n"] or 0)
            if n >= MAX_WATCHLIST:
                log.warning("WATCHLIST LIMIT uid=%s count=%s", uid, n)
                return False

            c.execute("""
                INSERT OR IGNORE INTO watchlist(
                    user_id,symbol,asset_type,created_at,asset_key
                ) VALUES(?,?,?,?,?)
            """, (uid, s, atype, stamp, asset_key))

            return c.execute("""
                SELECT 1 FROM watchlist
                WHERE user_id=? AND symbol=? AND asset_type=? LIMIT 1
            """, (uid, s, atype)).fetchone() is not None

    try:
        saved = _insert_once()
    except Exception as first_error:
        log.exception("WATCHLIST INSERT ERROR (first) uid=%s symbol=%s type=%s: %s", uid, s, atype, first_error)
        try:
            _repair_watchlist_table()
            saved = _insert_once()
        except Exception as second_error:
            log.exception("WATCHLIST INSERT ERROR (after repair) uid=%s symbol=%s type=%s: %s", uid, s, atype, second_error)
            return False

    if not saved:
        return False

    try:
        with db() as c:
            committed = c.execute("""
                SELECT 1 FROM watchlist
                WHERE user_id=? AND symbol=? AND asset_type=? LIMIT 1
            """, (uid, s, atype)).fetchone() is not None
        log.info("WATCHLIST SAVED uid=%s symbol=%s asset_type=%s db=%s ok=%s", uid, s, atype, DB_PATH, committed)
        return committed
    except Exception:
        log.exception("WATCHLIST COMMIT VERIFY FAILED uid=%s symbol=%s type=%s", uid, s, atype)
        return False

def remove_watch(uid, symbol, atype):
    with db() as c:
        c.execute("""
        DELETE FROM watchlist
        WHERE user_id=? AND symbol=? AND asset_type=?
        """, (uid, norm_symbol(symbol), atype))

def user_assets(uid, atype=None):
    uid = int(uid)
    with db() as c:
        if atype:
            rows = c.execute("""
                SELECT user_id, symbol, asset_type, created_at
                FROM watchlist
                WHERE user_id=? AND asset_type=?
                ORDER BY created_at ASC, symbol ASC
            """, (uid, atype)).fetchall()
        else:
            rows = c.execute("""
                SELECT user_id, symbol, asset_type, created_at
                FROM watchlist
                WHERE user_id=?
                ORDER BY created_at ASC, symbol ASC
            """, (uid,)).fetchall()
        log.info("WATCHLIST READ uid=%s count=%s db=%s", uid, len(rows), DB_PATH)
        return rows

def user_has_asset(uid, symbol, atype):
    with db() as c:
        return c.execute("""
        SELECT 1 FROM watchlist
        WHERE user_id=? AND symbol=? AND asset_type=?
        """, (uid, norm_symbol(symbol), atype)).fetchone() is not None

# ============================================================
# KEYBOARDS
# ============================================================

def main_kb(uid):
    rows = [r[:] for r in MAIN_MENU]
    if not is_admin(uid):
        rows = [r for r in rows if r != ["👨‍💼 پنل مدیریت"]]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=False,
                               input_field_placeholder="یک بخش را انتخاب کنید…")

def admin_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 داشبورد آماری", callback_data="adm:stats"),
         InlineKeyboardButton("👥 کاربران", callback_data="adm:users:0")],
        [InlineKeyboardButton("💳 پرداخت‌ها", callback_data="adm:payments"),
         InlineKeyboardButton("📢 ارسال همگانی", callback_data="adm:broadcast")],
        [InlineKeyboardButton("📨 پشتیبانی", callback_data="adm:support"),
         InlineKeyboardButton("✉️ پیام مستقیم", callback_data="adm:message")],
        [InlineKeyboardButton("🚫 مدیریت دسترسی", callback_data="adm:block"),
         InlineKeyboardButton("🗄 وضعیت دیتابیس", callback_data="adm:db")],
    ])

def watchlist_selector_keyboard(rows, prefix):
    buttons = []
    for r in rows:
        label = r["symbol"]
        if r["asset_type"] == "gold":
            label += " — طلای جهانی"
        elif r["asset_type"] == "gold18":
            label += " — طلای ۱۸ عیار"
        buttons.append([
            InlineKeyboardButton(label, callback_data=f"{prefix}:{r['asset_type']}:{r['symbol']}")
        ])
    return InlineKeyboardMarkup(buttons)

# ============================================================
# BASIC COMMANDS
# ============================================================

async def start(update, context):
    ensure_user(update.effective_user)
    uid = update.effective_user.id
    if is_blocked(uid):
        await update.message.reply_text("🚫 دسترسی شما توسط مدیر محدود شده است.")
        return
    await update.message.reply_text(
        "🤖 به ربات تحلیلگر بازار خوش آمدید.\n\n"
        "📊 تحلیل هوشمند رمز ارزها، طلای جهانی (XAU) و طلای ۱۸ عیار ایران (GOLD18)\n\n"
        "✨ امکانات ربات:\n"
        "• 📐 تحلیل تکنیکال: روند، اندیکاتورها، حمایت و مقاومت\n"
        "• 📊 داشبورد تحلیل تکنیکال مستقل\n"
        "• 📡 موتور سیگنال مستقل با ورود، حدضرر و اهداف\n"
        "• 🎯 اسکن کل بازار برای فرصت‌های خرید تأییدشده\n"
        "• 🔔 هشدارهای بازار\n\n"
        "⚙️ روش استفاده:\n"
        "1️⃣ دارایی را به واچ‌لیست اضافه کنید.\n"
        "2️⃣ از بخش تحلیل، بررسی کامل دریافت کنید.\n"
        "3️⃣ از بخش سیگنال‌ها وضعیت بازار را مشاهده کنید.\n\n"
        "⚠️ این ربات ابزار تحلیل و تصمیم‌یار بازار است و معامله خودکار انجام نمی‌دهد.",
        reply_markup=main_kb(uid)
    )

async def help_text(update, context):
    await update.message.reply_text(
        "ℹ️ راهنما\n\n"
        "➕ افزودن دارایی: رمز‌ارز، XAU یا GOLD18\n"
        "📋 واچ‌لیست: مشاهده و حذف دارایی‌ها\n"
        "💰 قیمت لحظه‌ای: رایگان\n"
        "📊 تحلیل تکنیکال: داشبورد مستقل وضعیت بازار\n"
        "📡 سیگنال معاملاتی: تصمیم‌یار مستقل با ورود/حدضرر/اهداف\n"
        "🎯 فرصت‌های خرید: فهرست فرصت‌های تأییدشده کل بازار\n"
        "🔔 هشدارهای هوشمند: اعلان فرصت‌های جدید\n"
        "📨 پشتیبان: ارتباط مستقیم\n"
        "💳 خرید اشتراک: پرداخت دستی و ارسال رسید\n\n"
        f"تأیید سیگنال روی تایم‌فریم: {SIGNAL_CONFIRM_TIMEFRAME}\n"
        "منبع GOLD18: TGJU / geram18\n"
        "منبع XAU: TGJU / ons\n\n"
        "⚠️ ربات معامله خودکار انجام نمی‌دهد."
    )

async def add_asset_prompt(update, context):
    for k in ("support_mode","chat_room","payment_plan",
              "admin_reply_to","admin_mode","message_target"):
        context.user_data.pop(k, None)
    context.user_data["awaiting_asset"] = "add"
    await update.message.reply_text(
        "➕ <b>افزودن دارایی</b>\n\n"
        "نمونه: BTC، ZEC، ETH، XAU، GOLD18 یا طلای ۱۸ عیار\n\n"
        "برای لغو /cancel",
        parse_mode=ParseMode.HTML
    )

async def process_add_asset(update, context, text):
    uid = update.effective_user.id
    text = (text or "").strip()
    if not text:
        await update.message.reply_text("❌ نماد خالی است.")
        return
    try:
        s = norm_symbol(text)
        if s in ("XAU", "GOLD18"):
            at = asset_type(s)
            if not add_watch(uid, s, at):
                await update.message.reply_text("⚠️ سقف واچ‌لیست پر شده است یا ذخیره انجام نشد.")
                return
            if not user_has_asset(uid, s, at):
                await update.message.reply_text("❌ دارایی در پایگاه‌داده ذخیره نشد. لطفاً دوباره تلاش کنید.")
                return
            label = "طلای جهانی XAU" if s == "XAU" else "طلای ۱۸ عیار ایران"
            await update.message.reply_text(f"✅ <b>{label}</b> به واچ‌لیست اضافه شد.", parse_mode=ParseMode.HTML)
            return

        res = await crypto_search(text)
        if not res:
            await update.message.reply_text(f"❌ دارایی <b>{escape(text)}</b> پیدا نشد.", parse_mode=ParseMode.HTML)
            return
        if len(res) == 1:
            sym, _, name = res[0]
            if not add_watch(uid, sym, "crypto"):
                await update.message.reply_text("⚠️ سقف واچ‌لیست پر شده است یا ذخیره انجام نشد.")
                return
            if not user_has_asset(uid, sym, "crypto"):
                await update.message.reply_text("❌ دارایی در پایگاه‌داده ذخیره نشد. لطفاً دوباره تلاش کنید.")
                return
            await update.message.reply_text(
                f"✅ <b>{escape(sym)}</b> به واچ‌لیست اضافه شد.\nنام: {escape(name)}",
                parse_mode=ParseMode.HTML
            )
            return

        buttons = [[
            InlineKeyboardButton(f"{sym} — {name}", callback_data=f"pick:{cid}:{norm_symbol(sym)}")
        ] for sym, cid, name in res[:10]]
        await update.message.reply_text("🔎 چند دارایی پیدا شد:", reply_markup=InlineKeyboardMarkup(buttons))
    except Exception:
        log.exception("add asset")
        await update.message.reply_text("⚠️ افزودن دارایی انجام نشد.")

async def watchlist_menu(update, context):
    rows = user_assets(update.effective_user.id)
    if not rows:
        await update.message.reply_text("📋 واچ‌لیست خالی است.")
        return
    lines, buttons = [], []
    for r in rows:
        label = r["symbol"] + " — " + (
            "طلای جهانی" if r["asset_type"] == "gold"
            else "طلای ۱۸ عیار" if r["asset_type"] == "gold18"
            else "رمز ارز"
        )
        lines.append("• " + label)
        buttons.append([
            InlineKeyboardButton(f"❌ حذف {r['symbol']}",
                                 callback_data=f"wl:del:{r['asset_type']}:{r['symbol']}")
        ])
    await update.message.reply_text(
        "📋 <b>واچ‌لیست شما</b>\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def live_price_menu(update, context):
    rows = user_assets(update.effective_user.id)
    if not rows:
        await update.message.reply_text("📋 واچ‌لیست خالی است؛ ابتدا دارایی اضافه کنید.")
        return
    buttons = [[
        InlineKeyboardButton(f"💰 {r['symbol']}",
                             callback_data=f"price:{r['asset_type']}:{r['symbol']}")
    ] for r in rows]
    await update.message.reply_text(
        "💰 <b>قیمت لحظه‌ای</b>\nرایگان و بدون نیاز به اشتراک:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def selected_price_callback(update, context):
    q = update.callback_query
    await q.answer("دریافت قیمت...")
    try:
        _, at, s = q.data.split(":", 2)
        s = norm_symbol(s)
        if not user_has_asset(q.from_user.id, s, at):
            await q.message.reply_text("❌ این دارایی در واچ‌لیست شما نیست.")
            return
        item = await current_price(s)
        await q.message.reply_text(format_live_price(item), parse_mode=ParseMode.HTML)
    except Exception:
        log.exception("price callback")
        await q.message.reply_text("⚠️ دریافت قیمت انجام نشد.")

async def analysis_prompt(update, context):
    uid = update.effective_user.id
    if not has_analysis_access(uid):
        await update.message.reply_text("🔒 تحلیل فقط برای مشترکین فعال است.")
        return
    rows = user_assets(uid)
    if not rows:
        await update.message.reply_text("📋 واچ‌لیست خالی است.")
        return
    await update.message.reply_text(
        "📊 <b>انتخاب دارایی برای تحلیل</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=watchlist_selector_keyboard(rows, "analysis")
    )

async def signals_menu(update, context):
    uid = update.effective_user.id
    if not has_analysis_access(uid):
        await update.message.reply_text("🔒 سیگنال‌ها فقط برای مشترکین فعال است.")
        return
    rows = user_assets(uid)
    if not rows:
        await update.message.reply_text("📋 واچ‌لیست خالی است.")
        return
    await update.message.reply_text(
        "🚨 <b>انتخاب دارایی برای سیگنال</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=watchlist_selector_keyboard(rows, "signal")
    )

# ============================================================
# MARKET-WIDE GROWTH SCANNER
# ============================================================

def _short_term_scan_metrics(x, ta):
    price = _safe_float(x.get("current_price"))
    if price <= 0:
        return None
    ch1 = _safe_float(x.get("price_change_percentage_1h_in_currency"))
    ch24 = _safe_float(x.get("price_change_percentage_24h_in_currency"))
    ch7 = _safe_float(x.get("price_change_percentage_7d_in_currency"))
    ch14 = _safe_float(x.get("price_change_percentage_14d_in_currency"))
    volume = _safe_float(x.get("total_volume"))
    mcap = _safe_float(x.get("market_cap"))
    rank = int(x.get("market_cap_rank") or 99999)
    ath_change = _safe_float(x.get("ath_change_percentage"))
    turnover = (volume / mcap) if mcap > 0 else 0

    spark = ((x.get("sparkline_in_7d") or {}).get("price") or [])
    volatility = 0.0
    if len(spark) >= 24:
        ps = pd.Series(spark, dtype=float).replace([float("inf"), float("-inf")], float('nan')).dropna()
        rets = ps.pct_change().dropna() * 100
        if len(rets) >= 12:
            volatility = float(rets.tail(72).std())

    score = 50.0
    reasons = []

    score += _clamp(ch1 * 2.0, -8, 8)
    score += _clamp(ch24 * 0.55, -10, 10)
    score += _clamp(ch7 * 0.35, -8, 8)
    score += _clamp(ch14 * 0.15, -4, 4)
    if ch1 > 0 and ch24 > 0 and ch7 > 0:
        score += 5; reasons.append("مومنتوم چندبازه‌ای مثبت")
    if ch24 > 20:
        score -= 8; reasons.append("رشد ۲۴ساعته شدید؛ ریسک تعقیب قیمت")
    if ch7 > 60:
        score -= 6; reasons.append("رشد ۷روزه شدید؛ احتمال اشباع کوتاه‌مدت")

    if turnover >= 0.25:
        score += 8; reasons.append("نقدشوندگی و گردش معاملات بالا")
    elif turnover >= 0.10:
        score += 5; reasons.append("گردش معاملات مناسب")
    elif turnover < 0.02:
        score -= 8; reasons.append("نقدشوندگی پایین")

    if rank <= 50: score += 5
    elif rank <= 200: score += 3
    elif rank > 1000: score -= 4

    if -25 <= ath_change <= -5:
        score += 5; reasons.append("فاصله مناسب از سقف تاریخی")
    elif ath_change < -90:
        score -= 5; reasons.append("افت بسیار عمیق از ATH")
    elif ath_change > -5:
        score -= 4; reasons.append("نزدیک سقف تاریخی")

    strength = float(ta.get("strength", 50)) if ta else 50.0
    if ta:
        trend = ta.get("trend")
        rsi = _safe_float(ta.get("rsi"), 50)
        adx = _safe_float(ta.get("adx"))
        mhist = _safe_float(ta.get("macd_hist"))
        e9 = _safe_float(ta.get("ema9"))
        e21 = _safe_float(ta.get("ema21"))
        e50 = _safe_float(ta.get("ema50"))

        if trend == "BULLISH":
            score += 10; reasons.append("EMA و روند صعودی تأییدشده")
        elif trend == "BULLISH_WEAK":
            score += 5; reasons.append("روند صعودی در حال شکل‌گیری")
        elif trend in ("BEARISH", "BEARISH_WEAK"):
            score -= 8; reasons.append("روند نزولی")

        if 52 <= rsi <= 68:
            score += 6; reasons.append("RSI در محدوده سازنده")
        elif rsi > 75:
            score -= 6; reasons.append("RSI بالا و ریسک اصلاح")
        elif rsi < 35:
            score -= 3

        if mhist > 0: score += 5; reasons.append("MACD مثبت")
        if adx >= 25: score += 4; reasons.append("قدرت روند مناسب")
        if e9 > e21 > e50: score += 5; reasons.append("چیدمان EMA صعودی")

    if 0.5 <= volatility <= 3.0:
        score += 4; reasons.append("نوسان مناسب برای معاملات کوتاه‌مدت")
    elif volatility > 6:
        score -= 6; reasons.append("نوسان بسیار بالا")

    score = _clamp(score)
    probability = _clamp(50 + (score - 50) * 0.78 + min(8, max(0, turnover * 20)))

    if score >= 75 and probability >= 68:
        setup = "قوی"
    elif score >= 65 and probability >= 58:
        setup = "مناسب"
    elif score >= 55:
        setup = "تحت نظر"
    else:
        setup = "ضعیف"

    if volatility > 6 or (ta and _safe_float(ta.get("rsi"), 50) > 78):
        risk = "زیاد"
    elif rank > 1000 or turnover < 0.03:
        risk = "زیاد"
    elif volatility > 3.5:
        risk = "متوسط"
    else:
        risk = "کنترل‌شده"

    return {
        "score": score, "probability": probability, "strength": strength,
        "setup": setup, "risk": risk, "reasons": reasons[:5],
        "volatility": volatility, "turnover": turnover, "ath_change": ath_change,
        "rank": rank, "price": price,
        "ch1": ch1, "ch24": ch24, "ch7": ch7, "ch14": ch14,
        "ta": ta,
    }

async def short_term_growth_scan():
    global SHORT_TERM_SCAN_CACHE
    now = time.monotonic()
    if SHORT_TERM_SCAN_CACHE and now - SHORT_TERM_SCAN_CACHE[0] < MARKET_SCAN_SECONDS:
        return SHORT_TERM_SCAN_CACHE[1]

    rows = await market_universe()
    if not rows:
        return []

    rough = []
    for x in rows:
        price = _safe_float(x.get("current_price"))
        volume = _safe_float(x.get("total_volume"))
        mcap = _safe_float(x.get("market_cap"))
        if price <= 0 or volume <= 0 or mcap <= 0:
            continue
        ch24 = _safe_float(x.get("price_change_percentage_24h_in_currency"))
        ch7 = _safe_float(x.get("price_change_percentage_7d_in_currency"))
        turnover = volume / mcap
        rough_score = 50 + _clamp(ch24 * .55, -12, 12) + _clamp(ch7 * .30, -8, 8)
        rough_score += _clamp((turnover - .05) * 70, -7, 9)
        if ch24 > 25: rough_score -= 8
        if ch7 > 70: rough_score -= 6
        rough.append((rough_score, x))

    rough.sort(key=lambda z: z[0], reverse=True)
    candidates = rough[:MARKET_SCAN_DEEP]
    result = []

    async def deep_one(x):
        sym = norm_symbol(x.get("symbol") or "")
        try:
            ta = await professional_crypto_analysis(sym)
            return x, ta
        except Exception:
            return x, None
    deep_results = await asyncio.gather(*(deep_one(x) for _, x in candidates), return_exceptions=True)
    for item in deep_results:
        if isinstance(item, Exception):
            continue
        x, ta = item
        if not ta:
            continue
        m = _short_term_scan_metrics(x, ta)
        if not m:
            continue
        result.append({
            "id": x.get("id"), "symbol": norm_symbol(x.get("symbol") or ""),
            "name": x.get("name") or norm_symbol(x.get("symbol") or ""),
            "image": x.get("image"), "market_cap": _safe_float(x.get("market_cap")),
            "volume": _safe_float(x.get("total_volume")), "m": m,
        })

    result.sort(key=lambda z: (z["m"]["score"], z["m"]["probability"]), reverse=True)
    result = result[:MARKET_SCAN_TOP]
    SHORT_TERM_SCAN_CACHE = (now, result)
    log.info("SHORT TERM SCAN universe=%s candidates=%s top=%s", len(rows), len(candidates), len(result))
    return result

async def short_term_growth_menu(update, context):
    uid = update.effective_user.id
    if not has_analysis_access(uid):
        await update.message.reply_text("🔒 شکار رشد کوتاه‌مدت فقط برای مشترکین فعال است.")
        return
    status = await update.message.reply_text(
        "🚀 <b>شکار رشد کوتاه‌مدت</b>\n\n"
        f"🌐 تا {MARKET_SCAN_PAGES * MARKET_SCAN_PER_PAGE:,} دارایی بازار غربال می‌شود.\n"
        "📊 تحلیل چندتایم‌فریمی و نقدشوندگی بررسی می‌شود.\n"
        "⏳ ممکن است اسکن اولیه کمی زمان ببرد...",
        parse_mode=ParseMode.HTML,
    )
    try:
        items = await asyncio.wait_for(short_term_growth_scan(), timeout=90)
        if not items:
            await status.edit_text(
                "❌ این بار نامزد قابل ارزیابی پیدا نشد. ممکن است منبع داده محدود شده یا کندل‌های OKX برای دارایی‌ها در دسترس نباشد.\n"
                "۳۰ تا ۶۰ ثانیه بعد دوباره تلاش کنید.",
            )
            return
        await status.edit_text(
            "🚀 <b>نامزدهای رشد کوتاه‌مدت</b>\n\n"
            "📌 این‌ها نامزدهای غربالگری هستند، نه توصیه خرید قطعی.\n"
            "💪 قدرت تکنیکال و 🧠 احتمال مدل دو معیار جدا هستند.",
            parse_mode=ParseMode.HTML,
        )
        for i, item in enumerate(items, 1):
            m = item["m"]; ta = m.get("ta") or {}
            rsi = _safe_float(ta.get("rsi"), 0)
            trend = {"BULLISH":"صعودی قوی","BULLISH_WEAK":"صعودی",
                     "BEARISH":"نزولی قوی","BEARISH_WEAK":"نزولی",
                     "NEUTRAL":"خنثی"}.get(ta.get("trend"), "نامشخص")
            block = (
                f"<b>{i}. {escape(item['symbol'])}</b> — {escape(item['name'])}\n"
                f"🎯 امتیاز فرصت: <b>{m['score']:.0f}%</b> | وضعیت: <b>{m['setup']}</b>\n"
                f"💪 قدرت تکنیکال: <b>{m['strength']:.0f}%</b> | 🧠 احتمال مدل: <b>{m['probability']:.0f}%</b>\n"
                f"📈 روند: {trend} | RSI: {rsi:.1f}\n"
                f"⚡ 1h: {m['ch1']:+.2f}% | 24h: {m['ch24']:+.2f}% | 7d: {m['ch7']:+.2f}%\n"
                f"💧 حجم/ارزش بازار: {m['turnover']*100:.1f}% | رتبه: #{m['rank']} | ریسک: <b>{escape(m['risk'])}</b>\n"
                f"💵 قیمت: ${m['price']:.8g}\n"
                f"🧩 " + "، ".join(escape(r) for r in m["reasons"][:4])
            )
            await update.message.reply_text(block, parse_mode=ParseMode.HTML)
        await update.message.reply_text("⚠️ قبل از ورود، سیگنال دارایی و حد ضرر را جداگانه بررسی کنید.")
    except asyncio.TimeoutError:
        await status.edit_text("⏳ اسکن بازار بیش از ۹۰ ثانیه طول کشید و برای جلوگیری از انتظار بی‌پایان متوقف شد. کمی بعد دوباره تلاش کنید.")
    except Exception:
        log.exception("short term growth scan")
        try:
            await status.edit_text("⚠️ اسکن کامل نشد؛ خطا در لاگ ثبت شد. چند دقیقه بعد دوباره تلاش کنید.")
        except Exception:
            pass

async def growth_scan_menu(update, context):
    uid = update.effective_user.id
    if not has_analysis_access(uid):
        await update.message.reply_text("🔒 فرصت‌های رشد فقط برای مشترکین فعال است.")
        return
    await update.message.reply_text(
        "🔎 در حال اسکن بازار...\n\n"
        f"🌐 حداکثر {MARKET_SCAN_PAGES * MARKET_SCAN_PER_PAGE:,} دارایی بررسی می‌شود.\n"
        "⏳"
    )
    try:
        items = await asyncio.wait_for(growth_scan(), timeout=90)
        if not items:
            await update.message.reply_text("❌ داده کافی از بازار دریافت نشد.")
            return
        lines = ["🔎 <b>فرصت‌های رشد بازار</b>", "", f"📡 نامزدهای برتر: {len(items)}", ""]
        for i, x in enumerate(items, 1):
            rsi = f" | RSI {x['rsi']:.0f}" if x.get('rsi') else ""
            lines.append(
                f"<b>{i}. {escape(x['symbol'])}</b> — {escape(x['name'])}\n"
                f"📈 امتیاز رشد: <b>{x['growth_score']:.0f}%</b> | 💪 تکنیکال: <b>{x['strength']:.0f}%</b>\n"
                f"🧠 احتمال مدل: <b>{x['probability']:.0f}%</b> | 24h: {x['ch24']:+.2f}% | 7d: {x['ch7']:+.2f}%{rsi}\n"
                f"💰 قیمت: ${x['price']:,.8f} | رتبه: #{x['rank']}\n"
            )
        lines.append("⚠️ این رتبه‌بندی مدل تحلیلی است؛ تضمین سود نیست.")
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
    except Exception:
        log.exception("growth scan")
        await update.message.reply_text("⚠️ اسکن بازار کامل نشد.")

# ============================================================
# PROFESSIONAL BUY-OPPORTUNITY MENU
# ============================================================

def _buy_opportunity_text(item):
    m = item.get("m") or {}
    ta = m.get("ta") or {}
    symbol = item.get("symbol") or "?"
    name = item.get("name") or symbol
    trend_fa = {"BULLISH":"صعودی قوی","BULLISH_WEAK":"صعودی",
                "BEARISH":"نزولی","BEARISH_WEAK":"نزولی ضعیف",
                "NEUTRAL":"خنثی"}.get(ta.get("trend"), "نامشخص")
    confirmations = int(m.get("confirmations", 0))
    return (
        f"🎯 <b>{escape(symbol)} — {escape(name)}</b>\n"
        f"🟢 <b>فرصت خرید تأییدشده</b> | 🔁 {confirmations}/{SIGNAL_CONFIRMATIONS_REQUIRED}\n"
        f"💎 فرصت: <b>{_safe_float(m.get('score')):.0f}%</b> | 💪 تکنیکال: <b>{_safe_float(m.get('strength')):.0f}%</b> | 🧠 مدل: <b>{_safe_float(m.get('probability')):.0f}%</b>\n"
        f"📈 روند: <b>{trend_fa}</b> | RSI: <b>{_safe_float(ta.get('rsi')):.1f}</b> | ADX: <b>{_safe_float(ta.get('adx')):.1f}</b>\n"
        f"📊 مومنتوم: 1H {_safe_float(m.get('ch1')):+.2f}% | 24H {_safe_float(m.get('ch24')):+.2f}% | 7D {_safe_float(m.get('ch7')):+.2f}%\n"
        f"💰 قیمت: <b>{_format_price(symbol, _safe_float(m.get('price')))}</b>\n"
        f"🛑 SL: <b>{_format_price(symbol, _safe_float(ta.get('stop')))}</b> | 🎯 TP1: <b>{_format_price(symbol, _safe_float((ta.get('targets') or [0])[0]))}</b>\n"
        f"🎯 TP2: <b>{_format_price(symbol, _safe_float((ta.get('targets') or [0,0])[1]))}</b> | TP3: <b>{_format_price(symbol, _safe_float((ta.get('targets') or [0,0,0])[2]))}</b>\n"
        f"⚖️ R/R: <b>1:{_safe_float(ta.get('rr')):.2f}</b> | ⚠️ ریسک: <b>{escape(str(m.get('risk') or 'نامشخص'))}</b>"
    )

async def buy_opportunities_menu(update, context):
    uid = update.effective_user.id
    if not has_analysis_access(uid):
        await update.message.reply_text("🔒 فرصت‌های خرید حرفه‌ای فقط برای مشترکین فعال است.")
        return
    msg = await update.message.reply_text(
        "🔎 <b>موتور حرفه‌ای فرصت خرید</b>\n\n"
        f"🌐 حداکثر {MARKET_SCAN_PAGES * MARKET_SCAN_PER_PAGE:,} دارایی غربال می‌شود.\n"
        "📊 EMA / RSI / MACD / ADX / ATR / مومنتوم / نقدشوندگی / ریسک / BTC بررسی می‌شود.\n"
        "⏳ در حال تحلیل بازار...",
        parse_mode=ParseMode.HTML,
    )
    try:
        opportunities, btc_status, observation_key = await asyncio.wait_for(_market_alert_scan(), timeout=90)
        if not opportunities:
            await msg.edit_text(
                "⏳ <b>فرصت خرید معتبری پیدا نشد.</b>\n\n"
                "داده کافی برای تحلیل حرفه‌ای دریافت نشد یا نامزد مناسبی در اسکن بازار وجود ندارد.",
                parse_mode=ParseMode.HTML,
            )
            return

        if not btc_status or not btc_status.get("ok"):
            nearest = sorted(
                opportunities,
                key=lambda z: _safe_float((z.get("m") or {}).get("score")),
                reverse=True,
            )[:3]
            body = [
                "🟡 <b>فعلاً خرید تأییدشده‌ای وجود ندارد.</b>",
                "₿ فیلتر بازار BTC مثبت نیست؛ برای کاهش ریسک، سیگنال خرید صادر نمی‌شود.",
                "",
            ]
            for item in nearest:
                m = item.get("m") or {}
                ta = m.get("ta") or {}
                body.append(
                    f"• <b>{escape(item.get('symbol','?'))}</b> — تحت نظر، نه سیگنال خرید | "
                    f"امتیاز {_safe_float(m.get('score')):.0f}% | "
                    f"قدرت {_safe_float(m.get('strength')):.0f}% | "
                    f"روند {escape(str(ta.get('trend') or 'نامشخص'))}"
                )
            body.append("⚠️ این دارایی‌ها صرفاً برای پایش‌اند؛ تا بهبود وضعیت BTC منتظر بمانید.")
            await msg.edit_text("\n".join(body), parse_mode=ParseMode.HTML)
            return

        qualified_items, pending_items = [], []
        for item in opportunities:
            if not _alert_candidate_ok(item, btc_status):
                continue
            m = item["m"]
            confirmed, confirmations, cycle_id = confirm_market_alert(
                item["symbol"], "BUY", m["price"], observation_key
            )
            m.update({
                "confirmed": confirmed,
                "confirmations": confirmations,
                "cycle_id": cycle_id,
                "signal": "BUY" if confirmed else "WAIT",
            })
            if confirmed:
                qualified_items.append(item)
            else:
                pending_items.append(item)

        qualified_items.sort(
            key=lambda z: (
                _safe_float(z["m"].get("score")),
                _safe_float(z["m"].get("strength")),
                _safe_float(z["m"].get("probability")),
            ), reverse=True
        )

        if not qualified_items:
            nearest = sorted(
                [z for z in opportunities if z.get("m")],
                key=lambda z: _safe_float((z.get("m") or {}).get("score")),
                reverse=True,
            )[:3]
            body = [
                "🟡 <b>فعلاً سیگنال خرید تأییدشده نداریم.</b>",
                f"نامزدهای منطبق با فیلتر اصلی: <b>{len(pending_items)}</b>",
                f"برای تأیید نهایی، {SIGNAL_CONFIRMATIONS_REQUIRED} مشاهده در چرخه‌های جداگانه لازم است.",
                "",
            ]
            if nearest:
                body.append("<b>نزدیک‌ترین دارایی‌ها برای پایش (نه توصیه خرید):</b>")
                for item in nearest:
                    m = item.get("m") or {}
                    ta = m.get("ta") or {}
                    body.append(
                        f"• <b>{escape(item.get('symbol','?'))}</b> | "
                        f"امتیاز {_safe_float(m.get('score')):.0f}% | "
                        f"قدرت {_safe_float(m.get('strength')):.0f}% | "
                        f"روند {escape(str(ta.get('trend') or 'نامشخص'))}"
                    )
            body.append("⏳ کمی بعد دوباره اسکن کنید؛ اگر شرایط تأیید نشود، ربات خرید پیشنهاد نمی‌کند.")
            await msg.edit_text("\n".join(body), parse_mode=ParseMode.HTML)
            return

        await msg.delete()
        for i, item in enumerate(qualified_items, 1):
            symbol = item["symbol"]
            text = (f"🎯 <b>فرصت #{i}</b>\n\n" + _buy_opportunity_text(item) +
                    "\n\n⚠️ خروجی مدل است؛ سود قطعی تضمین نمی‌شود.")
            markup = InlineKeyboardMarkup([[
                InlineKeyboardButton("📊 تحلیل", callback_data=f"op:analysis:{symbol}"),
                InlineKeyboardButton("📡 سیگنال", callback_data=f"op:signal:{symbol}"),
            ]])
            await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
    except asyncio.TimeoutError:
        await msg.edit_text("⏳ اسکن بیش از ۹۰ ثانیه طول کشید و متوقف شد تا ربات معطل نماند. کمی بعد دوباره تلاش کنید.")
    except Exception:
        log.exception("professional buy opportunity menu")
        try:
            await msg.edit_text("⚠️ تحلیل بازار کامل نشد؛ خطا در لاگ Railway ثبت شد. کمی بعد دوباره تلاش کنید.")
        except Exception:
            pass

# ============================================================
# SUBSCRIPTIONS
# ============================================================

async def buy_menu(update, context):
    buttons = [[
        InlineKeyboardButton(f"{d} روز — {a:,} تومان", callback_data=f"plan:{p}")
    ] for p, (d, a) in PLANS.items()]
    await update.message.reply_text("💳 یکی از پلن‌ها را انتخاب کنید:",
                                    reply_markup=InlineKeyboardMarkup(buttons))

async def status_menu(update, context):
    s = active_subscription(update.effective_user.id)
    if not s:
        await update.message.reply_text("👤 اشتراک فعال ندارید.")
        return
    await update.message.reply_text(
        "👤 <b>وضعیت اشتراک</b>\n\n"
        f"پلن: {s['days']} روز\n"
        f"شروع: {format_dt(s['start_at'])}\n"
        f"پایان: {format_dt(s['end_at'])}\n"
        "وضعیت: 🟢 فعال",
        parse_mode=ParseMode.HTML
    )

async def plan_callback(update, context):
    q = update.callback_query
    await q.answer()
    plan = q.data.split(":", 1)[1]
    if plan not in PLANS:
        await q.message.reply_text("❌ پلن نامعتبر است.")
        return
    days, amount = PLANS[plan]
    context.user_data["payment_plan"] = plan
    await q.message.reply_text(
        f"💳 <b>پلن {days} روزه</b>\n\n"
        f"مبلغ: <b>{amount:,} تومان</b>\n\n"
        f"شماره کارت:\n<code>{escape(PAYMENT_CARD)}</code>\n\n"
        "پس از پرداخت تصویر رسید را همین‌جا ارسال کنید.",
        parse_mode=ParseMode.HTML
    )

async def receipt_photo(update, context):
    if context.user_data.get("support_mode"):
        await support_media(update, context)
        return
    plan = context.user_data.get("payment_plan")
    if plan not in PLANS:
        await update.message.reply_text("ابتدا از «💳 خرید اشتراک» یک پلن انتخاب کنید.")
        return
    days, amount = PLANS[plan]
    fid = update.message.photo[-1].file_id
    with db() as c:
        cur = c.execute("""
        INSERT INTO payment_requests(
            user_id,plan,days,amount,receipt_file_id,status,created_at
        ) VALUES(?,?,?,?,?,?,?)
        """, (update.effective_user.id, plan, days, amount, fid, "pending", now_iso()))
        pid = cur.lastrowid
    context.user_data.pop("payment_plan", None)
    await update.message.reply_text("✅ رسید دریافت شد؛ پس از بررسی مدیر اشتراک فعال می‌شود.")
    for aid in ADMIN_IDS:
        try:
            await context.bot.send_photo(
                aid, fid,
                caption=(f"💳 رسید جدید\nکاربر: {update.effective_user.id}\n"
                         f"پلن: {days} روز\nمبلغ: {amount:,} تومان\nشناسه: {pid}"),
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ تایید", callback_data=f"pay:approve:{pid}"),
                    InlineKeyboardButton("❌ رد", callback_data=f"pay:reject:{pid}")
                ]])
            )
        except Exception:
            log.exception("payment notify")

# ============================================================
# SUPPORT
# ============================================================

async def support_prompt(update, context):
    context.user_data["support_mode"] = True
    await update.message.reply_text(
        "📨 پیام خود را برای پشتیبان بفرستید. متن، عکس، فایل یا صدا.\n/cancel برای خروج"
    )

async def save_support(uid, message, mid):
    with db() as c:
        c.execute("""
        INSERT INTO support_messages(
            user_id,direction,message,telegram_message_id,created_at
        ) VALUES(?,?,?,?,?)
        """, (uid, "user_to_admin", message, mid, now_iso()))

async def support_media(update, context):
    uid = update.effective_user.id
    saved = None
    if update.message.photo:
        saved = "[عکس]"
        for aid in ADMIN_IDS:
            try:
                await context.bot.send_photo(
                    aid, update.message.photo[-1].file_id,
                    caption=f"📨 پیام پشتیبانی\nکاربر: {uid}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("↩️ پاسخ", callback_data=f"sup:reply:{uid}")
                    ]])
                )
            except Exception:
                pass
    elif update.message.document:
        saved = "[فایل]"
        for aid in ADMIN_IDS:
            try:
                await context.bot.send_document(
                    aid, update.message.document.file_id,
                    caption=f"📨 فایل از کاربر {uid}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("↩️ پاسخ", callback_data=f"sup:reply:{uid}")
                    ]])
                )
            except Exception:
                pass
    elif update.message.voice:
        saved = "[صدا]"
        for aid in ADMIN_IDS:
            try:
                await context.bot.send_voice(
                    aid, update.message.voice.file_id,
                    caption=f"📨 صدا از کاربر {uid}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("↩️ پاسخ", callback_data=f"sup:reply:{uid}")
                    ]])
                )
            except Exception:
                pass
    else:
        return
    await save_support(uid, saved, update.message.message_id)
    await update.message.reply_text("✅ پیام شما برای پشتیبان ارسال شد.")

async def support_reply_callback(update, context):
    q = update.callback_query
    await q.answer()
    if not is_admin(q.from_user.id):
        return
    uid = int(q.data.split(":")[-1])
    context.user_data["admin_reply_to"] = uid
    await q.message.reply_text(f"✍️ پاسخ به کاربر {uid} را ارسال کنید.")

async def send_support_reply(update, context, text):
    uid = context.user_data.pop("admin_reply_to", None)
    if not uid:
        return
    try:
        await context.bot.send_message(
            uid,
            f"📨 <b>پاسخ پشتیبان</b>\n\n{escape(text)}",
            parse_mode=ParseMode.HTML
        )
        with db() as c:
            c.execute("""
            INSERT INTO support_messages(
                user_id,admin_id,direction,message,status,
                created_at,replied_at
            ) VALUES(?,?,?,?,?,?,?)
            """, (uid, update.effective_user.id, "admin_to_user",
                  text, "closed", now_iso(), now_iso()))
        await update.message.reply_text("✅ پاسخ ارسال شد.")
    except Exception as e:
        await update.message.reply_text(f"❌ ارسال نشد: {escape(str(e))}", parse_mode=ParseMode.HTML)

# ============================================================
# BACKUP / ALERT WORKERS
# ============================================================

async def backup_worker():
    while True:
        try:
            await asyncio.to_thread(backup_database, "scheduled")
        except Exception:
            log.exception("scheduled database backup")
        await asyncio.sleep(BACKUP_INTERVAL_SECONDS)

async def alerts_menu(update, context):
    uid = update.effective_user.id
    with db() as c:
        r = c.execute("SELECT enabled FROM alert_preferences WHERE user_id=?", (uid,)).fetchone()
    enabled = bool(r["enabled"]) if r else True
    await update.message.reply_text(
        "🔔 هشدار سیگنال: " + ("فعال" if enabled else "خاموش"),
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔕 خاموش" if enabled else "🔔 روشن", callback_data="alert:toggle")
        ]])
    )

async def alert_callback(update, context):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    with db() as c:
        r = c.execute("SELECT enabled FROM alert_preferences WHERE user_id=?", (uid,)).fetchone()
        new = 0 if r and r["enabled"] else 1
        c.execute("""
        INSERT INTO alert_preferences(user_id,enabled,interval_seconds) VALUES(?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET enabled=excluded.enabled
        """, (uid, new, ALERT_INTERVAL_SECONDS))
    await q.message.edit_text("🔔 هشدار سیگنال: " + ("فعال" if new else "خاموش"))

async def market_snapshot_worker():
    while True:
        try:
            await asyncio.gather(
                current_price("GOLD18"),
                current_price("XAU"),
                return_exceptions=True
            )
        except Exception:
            log.exception("market snapshot worker")
        await asyncio.sleep(GOLD_HISTORY_INTERVAL_SECONDS)

def confirm_market_alert(symbol, candidate, price, observation_key):
    """Track confirmations for a whole-market opportunity cycle."""
    now = now_iso()
    with db() as c:
        if candidate == "WAIT":
            c.execute("DELETE FROM market_alert_state WHERE symbol=?", (symbol,))
            return False, 0, None
        row = c.execute("SELECT * FROM market_alert_state WHERE symbol=?", (symbol,)).fetchone()
        if row and row["candidate"] == candidate:
            cycle_id = row["cycle_id"] or row["last_observation_key"] or observation_key
            if row["last_observation_key"] == observation_key:
                confirmations = int(row["confirmations"])
            else:
                confirmations = min(SIGNAL_CONFIRMATIONS_REQUIRED, int(row["confirmations"]) + 1)
        else:
            confirmations = 1
            cycle_id = observation_key
        c.execute("""
            INSERT INTO market_alert_state(
                symbol,candidate,confirmations,last_observation_key,last_price,updated_at,cycle_id
            ) VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(symbol) DO UPDATE SET
                candidate=excluded.candidate,
                confirmations=excluded.confirmations,
                last_observation_key=excluded.last_observation_key,
                last_price=excluded.last_price,
                updated_at=excluded.updated_at,
                cycle_id=excluded.cycle_id
        """, (symbol, candidate, confirmations, observation_key, float(price), now, cycle_id))
    return confirmations >= SIGNAL_CONFIRMATIONS_REQUIRED, confirmations, cycle_id

def _alert_candidate_ok(item, btc_metrics):
    m = item.get("m") or {}
    ta = m.get("ta") or {}
    score = _safe_float(m.get("score"))
    strength = _safe_float(m.get("strength"))
    probability = _safe_float(m.get("probability"))
    rsi = _safe_float(ta.get("rsi"), 50)
    trend = ta.get("trend")
    turnover = _safe_float(m.get("turnover"))
    risk = m.get("risk")
    ch1 = _safe_float(m.get("ch1"))
    ch24 = _safe_float(m.get("ch24"))
    ch7 = _safe_float(m.get("ch7"))

    btc_ok = bool(btc_metrics and btc_metrics.get("ok"))
    signal_candidate = str(ta.get("signal_candidate") or "WAIT")
    signal_score = _safe_float(ta.get("signal_score"))
    rr = _safe_float(ta.get("rr"))
    # Require a positive multi-factor setup, but do not make every metric
    # an all-or-nothing gate: that made valid setups almost impossible to show.
    return (
        score >= max(78, ALERT_OPPORTUNITY_MIN - 5)
        and strength >= max(65, ALERT_STRENGTH_MIN - 7)
        and probability >= max(62, ALERT_PROBABILITY_MIN - 8)
        and signal_candidate == "BUY"
        and signal_score >= 65
        and trend in ("BULLISH", "BULLISH_WEAK")
        and 42 <= rsi <= 73
        and ch1 >= -0.5 and ch24 >= -1.5 and ch7 >= -4.0
        and turnover >= min(ALERT_MIN_TURNOVER, 0.02)
        and risk != "زیاد"
        and rr >= 1.3
        and btc_ok
    )

async def _btc_market_status(rows):
    """STRICTER BTC gate (fix #8)."""
    ta = await professional_crypto_analysis("BTC")
    btc = next((x for x in rows
                if str(x.get("id") or "").lower() == "bitcoin"
                or str(x.get("symbol") or "").upper() == "BTC"), None)
    if not ta or not btc:
        return {"ok": False}

    ch1 = _safe_float(btc.get("price_change_percentage_1h_in_currency"))
    ch24 = _safe_float(btc.get("price_change_percentage_24h_in_currency"))
    ch7 = _safe_float(btc.get("price_change_percentage_7d_in_currency"))
    trend = ta.get("trend")
    rsi = _safe_float(ta.get("rsi"), 50)
    strength = _safe_float(ta.get("strength"), 0)
    cand = ta.get("signal_candidate")

    btc_bullish = (
        cand == "BUY"
        or (strength >= 65 and trend in ("BULLISH", "BULLISH_WEAK"))
    )
    macro_ok = ch1 >= -0.5 and ch24 >= -1.0 and ch7 >= -3.0
    ok = btc_bullish and macro_ok and rsi < 75 and cand != "SELL"

    return {"ok": ok, "ch1": ch1, "ch24": ch24, "ch7": ch7,
            "rsi": rsi, "trend": trend, "strength": strength, "candidate": cand}

async def _market_alert_scan():
    rows = await market_universe()
    if not rows:
        return [], None, None

    # FIX #10: wall-clock bucket so restarts don't reset the cycle.
    bucket = int(time.time() // max(300, ALERT_INTERVAL_SECONDS))
    scan_observation_key = f"market:{bucket}"

    btc_status = await _btc_market_status(rows)
    rough = []
    for x in rows:
        price = _safe_float(x.get("current_price"))
        volume = _safe_float(x.get("total_volume"))
        mcap = _safe_float(x.get("market_cap"))
        if price <= 0 or volume <= 0 or mcap <= 0:
            continue
        ch1 = _safe_float(x.get("price_change_percentage_1h_in_currency"))
        ch24 = _safe_float(x.get("price_change_percentage_24h_in_currency"))
        ch7 = _safe_float(x.get("price_change_percentage_7d_in_currency"))
        turnover = volume / mcap
        rough_score = 50 + _clamp(ch1 * 2.0, -8, 8) + _clamp(ch24 * .55, -10, 10) + _clamp(ch7 * .35, -8, 8)
        rough_score += _clamp((turnover - .05) * 70, -7, 9)
        if ch24 > 25: rough_score -= 8
        if ch7 > 70: rough_score -= 6
        rough.append((rough_score, x))

    rough.sort(key=lambda z: z[0], reverse=True)
    candidates = rough[:MARKET_SCAN_DEEP]
    result = []

    async def deep_alert_one(x):
        sym = norm_symbol(x.get("symbol") or "")
        try:
            return x, await professional_crypto_analysis(sym)
        except Exception:
            return x, None
    deep_results = await asyncio.gather(*(deep_alert_one(x) for _, x in candidates), return_exceptions=True)
    for item in deep_results:
        if isinstance(item, Exception):
            continue
        x, ta = item
        if not ta:
            continue
        m = _short_term_scan_metrics(x, ta)
        if not m:
            continue
        result.append({
            "id": x.get("id"), "symbol": norm_symbol(x.get("symbol") or ""),
            "name": x.get("name") or norm_symbol(x.get("symbol") or ""),
            "image": x.get("image"), "market_cap": _safe_float(x.get("market_cap")),
            "volume": _safe_float(x.get("total_volume")), "m": m,
        })

    result.sort(key=lambda z: _safe_float((z.get("m") or {}).get("score")), reverse=True)
    return result, btc_status, scan_observation_key

async def alert_worker(app):
    while True:
        try:
            opportunities, btc_status, observation_key = await asyncio.wait_for(_market_alert_scan(), timeout=90)
            qualified = {}

            for item in opportunities:
                symbol = item["symbol"]
                if _alert_candidate_ok(item, btc_status):
                    qualified[symbol] = item
                    m = item["m"]
                    ta = m.get("ta") or {}
                    candidate = "BUY" if ta.get("trend") == "BULLISH" else "WAIT"
                    confirmed, confirmations, cycle_id = confirm_market_alert(
                        symbol, candidate, m["price"], observation_key
                    )
                    m["confirmed"] = confirmed
                    m["confirmations"] = confirmations
                    m["cycle_id"] = cycle_id
                    m["signal"] = candidate if confirmed else "WAIT"
                else:
                    # Reset this symbol only; never reuse a stale `m` from a prior loop iteration.
                    confirm_market_alert(symbol, "WAIT", _safe_float((item.get("m") or {}).get("price")), observation_key)

            with db() as c:
                active_states = c.execute(
                    "SELECT symbol FROM market_alert_state WHERE candidate='BUY' AND confirmations>=?",
                    (SIGNAL_CONFIRMATIONS_REQUIRED,)
                ).fetchall()
            for row in active_states:
                if row["symbol"] not in qualified:
                    confirm_market_alert(row["symbol"], "WAIT", 0.0, observation_key)

            with db() as c:
                users = c.execute("""
                    SELECT DISTINCT a.user_id
                    FROM alert_preferences a
                    JOIN subscriptions s ON s.user_id=a.user_id
                    WHERE a.enabled=1 AND s.status='active' AND s.end_at>?
                """, (now_iso(),)).fetchall()

            for ur in users:
                uid = ur["user_id"]
                for symbol, item in qualified.items():
                    m = item["m"]
                    if m.get("signal") != "BUY" or not m.get("confirmed"):
                        continue
                    ta = m.get("ta") or {}
                    a = dict(ta)
                    a.update({
                        "symbol": symbol, "price": m["price"],
                        "strength": _safe_float(m.get("strength")),
                        "probability": _safe_float(m.get("probability")),
                        "opportunity_score": _safe_float(m.get("score")),
                        "signal": "BUY", "confirmed": True,
                        "confirmations": int(m.get("confirmations", 0)),
                        "signal_candidate": "BUY",
                        "signal_score": _safe_float(ta.get("signal_score"), 0),
                        "reasons_buy": list(m.get("reasons") or []) + list(ta.get("reasons_buy") or []),
                        "reasons_sell": [],
                    })
                    cycle_id = m.get("cycle_id") or observation_key
                    key = f"{symbol}:BUY:cycle:{cycle_id}"
                    with db() as c:
                        prev = c.execute(
                            "SELECT signal_key FROM alert_events WHERE user_id=? AND symbol=? ORDER BY id DESC LIMIT 1",
                            (uid, symbol)
                        ).fetchone()
                        if prev and prev["signal_key"] == key:
                            continue
                        c.execute(
                            "INSERT INTO alert_events(user_id,symbol,signal_key,message,created_at) VALUES(?,?,?,?,?)",
                            (uid, symbol, key, analysis_text(a), now_iso())
                        )
                    try:
                        await app.bot.send_message(
                            uid,
                            "🔔 <b>فرصت خرید تأییدشده در کل بازار</b>\n\n" + signal_text(a),
                            parse_mode=ParseMode.HTML
                        )
                    except Exception:
                        log.exception("alert send failed user=%s symbol=%s", uid, symbol)

                with db() as c:
                    c.execute("UPDATE alert_preferences SET last_check_at=? WHERE user_id=?",
                              (now_iso(), uid))
        except Exception:
            log.exception("alert worker")
        await asyncio.sleep(ALERT_INTERVAL_SECONDS)

# ============================================================
# ADMIN
# ============================================================

async def db_status_command(update, context):
    if not is_admin(update.effective_user.id):
        return
    d = database_diagnostics()
    with db() as c:
        users = c.execute("SELECT COUNT(*) n FROM users").fetchone()["n"]
        subs = c.execute("SELECT COUNT(*) n FROM subscriptions").fetchone()["n"]
        active = c.execute("SELECT COUNT(*) n FROM subscriptions WHERE status='active' AND end_at>?",
                           (now_iso(),)).fetchone()["n"]
    await update.message.reply_text(
        "🗄 <b>وضعیت دیتابیس</b>\n\n"
        f"مسیر: <code>{escape(d['path'])}</code>\n"
        f"وجود فایل: {'✅' if d['exists'] else '❌'}\n"
        f"حجم: {d['size']:,} بایت\n"
        f"مسیر پایدار /data: {'✅' if d['persistent_path'] else '❌'}\n"
        f"کاربران: {users}\n"
        f"رکورد اشتراک: {subs}\n"
        f"اشتراک فعال: {active}\n"
        f"تعداد Backup: {d['backup_count']}\n"
        f"آخرین Backup: {escape(d['latest_backup'])}\n\n"
        "Railway Volume باید روی /data متصل باشد.",
        parse_mode=ParseMode.HTML
    )

async def backup_command(update, context):
    if not is_admin(update.effective_user.id):
        return
    path = await asyncio.to_thread(backup_database, "manual")
    if path:
        await update.message.reply_text(f"✅ Backup ساخته شد:\n<code>{escape(path)}</code>",
                                        parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("❌ ساخت Backup انجام نشد؛ لاگ Railway را بررسی کن.")

async def admin_panel(update, context):
    if is_admin(update.effective_user.id):
        await update.message.reply_text(
            "👨‍💼 <b>پنل مدیریت MARKET AI</b>\n\nمدیریت کاربران، پرداخت‌ها، پیام‌ها، پشتیبانی و وضعیت دیتابیس از اینجا انجام می‌شود.",
            parse_mode=ParseMode.HTML, reply_markup=admin_kb()
        )
        return
    await update.message.reply_text(
        "⛔ دسترسی به پنل مدیریت برای این حساب فعال نیست.\n\nاگر مدیر ربات هستید، متغیر <code>ADMIN_IDS</code> یا <code>ADMIN_ID</code> را در Railway با شناسه عددی تلگرام خود تنظیم کنید.",
        parse_mode=ParseMode.HTML
    )

async def admin_callback(update, context):
    q = update.callback_query
    await q.answer()
    if not is_admin(q.from_user.id):
        return
    p = q.data.split(":")
    action = p[1]

    if action == "stats":
        with db() as c:
            users = c.execute("SELECT COUNT(*) n FROM users").fetchone()["n"]
            active = c.execute("SELECT COUNT(DISTINCT user_id) n FROM subscriptions WHERE status='active' AND end_at>?",
                               (now_iso(),)).fetchone()["n"]
            pending = c.execute("SELECT COUNT(*) n FROM payment_requests WHERE status='pending'").fetchone()["n"]
            assets = c.execute("SELECT COUNT(*) n FROM watchlist").fetchone()["n"]
        await q.message.reply_text(
            f"📊 <b>آمار</b>\n\n👥 کاربران: {users}\n💳 مشترک فعال: {active}\n⏳ پرداخت: {pending}\n🪙 دارایی: {assets}",
            parse_mode=ParseMode.HTML
        )
    elif action == "broadcast":
        context.user_data["admin_mode"] = "broadcast"
        await q.message.reply_text("📢 متن پیام برای مشترکین فعال را ارسال کنید.")
    elif action == "message":
        context.user_data["admin_mode"] = "message_uid"
        await q.message.reply_text("شناسه عددی کاربر را ارسال کنید.")
    elif action == "block":
        context.user_data["admin_mode"] = "block"
        await q.message.reply_text("شناسه کاربر را ارسال کنید.")
    elif action == "payments":
        with db() as c:
            rows = c.execute("SELECT * FROM payment_requests WHERE status='pending' ORDER BY id DESC LIMIT 20").fetchall()
        if not rows:
            await q.message.reply_text("پرداخت در انتظاری نیست.")
            return
        for r in rows:
            await q.message.reply_text(
                f"💳 #{r['id']}\nکاربر: {r['user_id']}\nپلن: {r['days']} روز\nمبلغ: {r['amount']:,}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ تایید", callback_data=f"pay:approve:{r['id']}"),
                    InlineKeyboardButton("❌ رد", callback_data=f"pay:reject:{r['id']}")
                ]])
            )
    elif action == "users":
        page = int(p[2]) if len(p) > 2 else 0
        with db() as c:
            rows = c.execute("SELECT * FROM users ORDER BY created_at DESC LIMIT 20 OFFSET ?",
                             (page * 20,)).fetchall()
        txt = ("👥 <b>کاربران</b>\n\n" +
               "\n".join(f"{r['user_id']} | {escape(r['first_name'] or '-')} | "
                         f"{'🚫' if r['blocked'] else '✅'}" for r in rows))
        await q.message.reply_text(txt if rows else "کاربری نیست.", parse_mode=ParseMode.HTML)
    elif action == "db":
        d = database_diagnostics()
        await q.message.reply_text(
            "🗄 <b>وضعیت دیتابیس</b>\n\n"
            f"مسیر: <code>{escape(d['path'])}</code>\n"
            f"فایل: {'✅' if d['exists'] else '❌'} | حجم: {d['size']:,} بایت\n"
            f"Volume /data: {'✅' if d['persistent_path'] else '❌'}\n"
            f"Backup: {d['backup_count']}\n"
            f"آخرین Backup: {escape(d['latest_backup'])}",
            parse_mode=ParseMode.HTML
        )
    elif action == "support":
        with db() as c:
            rows = c.execute("SELECT * FROM support_messages WHERE direction='user_to_admin' ORDER BY id DESC LIMIT 20").fetchall()
        if not rows:
            await q.message.reply_text("پیام پشتیبانی نیست.")
            return
        for r in rows:
            await q.message.reply_text(
                f"📨 #{r['id']} از {r['user_id']}\n{escape(r['message'] or '[رسانه]')}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("↩️ پاسخ", callback_data=f"sup:reply:{r['user_id']}")
                ]])
            )

# ============================================================
# CALLBACKS
# ============================================================

async def pick_asset_callback(update, context):
    q = update.callback_query
    await q.answer()
    try:
        _, cid, sym = q.data.split(":", 2)
        sym = norm_symbol(sym)
        if not add_watch(q.from_user.id, sym, "crypto"):
            await q.message.reply_text("⚠️ سقف واچ‌لیست پر شده است یا ذخیره انجام نشد.")
            return
        await q.message.reply_text(f"✅ <b>{escape(sym)}</b> به واچ‌لیست اضافه شد.",
                                   parse_mode=ParseMode.HTML)
    except Exception:
        log.exception("pick asset")
        await q.message.reply_text("⚠️ افزودن دارایی انجام نشد.")

async def watchlist_delete_callback(update, context):
    q = update.callback_query
    await q.answer()
    try:
        _, _, atype, sym = q.data.split(":", 3)
        remove_watch(q.from_user.id, sym, atype)
        await q.message.reply_text(f"✅ {escape(sym)} از واچ‌لیست حذف شد.",
                                   parse_mode=ParseMode.HTML)
    except Exception:
        log.exception("watchlist delete")
        await q.message.reply_text("⚠️ حذف انجام نشد.")

async def selected_analysis_callback(update, context):
    q = update.callback_query
    await q.answer("در حال ساخت داشبورد تحلیل...")
    try:
        _, atype, sym = q.data.split(":", 2)
        if not has_analysis_access(q.from_user.id):
            await q.message.reply_text("🔒 تحلیل فقط برای مشترکین فعال است.")
            return
        if not user_has_asset(q.from_user.id, sym, atype):
            await q.message.reply_text("❌ این دارایی در واچ‌لیست شما نیست.")
            return
        a = await asyncio.wait_for(analyze(norm_symbol(sym)), timeout=45)
        await q.message.reply_text(analysis_text(a), parse_mode=ParseMode.HTML)
    except asyncio.TimeoutError:
        await q.message.reply_text("⏳ دریافت داده بیش از حد طول کشید. ۳۰ تا ۶۰ ثانیه دیگر دوباره تلاش کنید.")
    except Exception:
        log.exception("analysis callback")
        await q.message.reply_text("⚠️ داشبورد تحلیل ساخته نشد؛ خطا ثبت شد، دوباره تلاش کنید.")

async def selected_signal_callback(update, context):
    q = update.callback_query
    await q.answer("در حال محاسبه سیگنال...")
    try:
        _, atype, sym = q.data.split(":", 2)
        if not has_analysis_access(q.from_user.id):
            await q.message.reply_text("🔒 سیگنال فقط برای مشترکین فعال است.")
            return
        if not user_has_asset(q.from_user.id, sym, atype):
            await q.message.reply_text("❌ این دارایی در واچ‌لیست شما نیست.")
            return
        a = await asyncio.wait_for(analyze(norm_symbol(sym)), timeout=45)
        await q.message.reply_text(signal_text(a), parse_mode=ParseMode.HTML)
    except asyncio.TimeoutError:
        await q.message.reply_text("⏳ دریافت داده بیش از حد طول کشید. ۳۰ تا ۶۰ ثانیه دیگر دوباره تلاش کنید.")
    except Exception:
        log.exception("signal callback")
        await q.message.reply_text("⚠️ محاسبه سیگنال انجام نشد؛ خطا ثبت شد، دوباره تلاش کنید.")

async def opportunity_action_callback(update, context):
    q = update.callback_query
    await q.answer()
    try:
        _, action, sym = q.data.split(":", 2)
        if not has_analysis_access(q.from_user.id):
            await q.message.reply_text("🔒 این بخش فقط برای مشترکین فعال است.")
            return
        a = await asyncio.wait_for(analyze(norm_symbol(sym)), timeout=45)
        if action == "analysis":
            await q.message.reply_text(analysis_text(a), parse_mode=ParseMode.HTML)
        else:
            await q.message.reply_text(signal_text(a), parse_mode=ParseMode.HTML)
    except asyncio.TimeoutError:
        await q.message.reply_text("⏳ دریافت داده بیش از حد طول کشید. کمی بعد دوباره تلاش کنید.")
    except Exception:
        log.exception("opportunity action callback")
        await q.message.reply_text("⚠️ عملیات روی فرصت انجام نشد؛ خطا ثبت شد، دوباره تلاش کنید.")

async def payment_admin_callback(update, context):
    q = update.callback_query
    await q.answer()
    if not is_admin(q.from_user.id):
        return
    try:
        _, action, pid_s = q.data.split(":", 2)
        pid = int(pid_s)
        with db() as c:
            r = c.execute("SELECT * FROM payment_requests WHERE id=?", (pid,)).fetchone()
            if not r:
                await q.message.reply_text("❌ درخواست پرداخت پیدا نشد.")
                return
            if r["status"] != "pending":
                await q.message.reply_text("ℹ️ این درخواست قبلاً بررسی شده است.")
                return
            if action == "approve":
                days = int(r["days"]); uid = int(r["user_id"])
                start = datetime.now(timezone.utc)
                existing = c.execute("""SELECT * FROM subscriptions WHERE user_id=? AND status='active' AND end_at>? ORDER BY end_at DESC LIMIT 1""",
                                     (uid, now_iso())).fetchone()
                if existing:
                    old_end = datetime.fromisoformat(existing["end_at"])
                    end = old_end + timedelta(days=days)
                    c.execute("UPDATE subscriptions SET end_at=?, status='active' WHERE id=?",
                              (end.isoformat(), existing["id"]))
                else:
                    end = start + timedelta(days=days)
                    c.execute("""INSERT INTO subscriptions(user_id,plan,days,amount,start_at,end_at,status,source,payment_request_id,created_at)
                                 VALUES(?,?,?,?,?,?,?,?,?,?)""",
                              (uid,r["plan"],days,r["amount"],start.isoformat(),end.isoformat(),
                               "active","manual",pid,now_iso()))
                c.execute("UPDATE payment_requests SET status='approved', reviewed_at=?, reviewed_by=? WHERE id=?",
                          (now_iso(), q.from_user.id, pid))
                await q.message.reply_text("✅ پرداخت تأیید و اشتراک فعال شد.")
                try:
                    await context.bot.send_message(uid, f"✅ اشتراک شما فعال شد.\n📅 مدت: {days} روز\n⏰ پایان: {format_dt(end.isoformat())}")
                except Exception:
                    pass
            elif action == "reject":
                c.execute("UPDATE payment_requests SET status='rejected', reviewed_at=?, reviewed_by=? WHERE id=?",
                          (now_iso(), q.from_user.id, pid))
                await q.message.reply_text("❌ پرداخت رد شد.")
                try:
                    await context.bot.send_message(int(r["user_id"]), "❌ رسید پرداخت شما رد شد. برای پیگیری با پشتیبانی تماس بگیرید.")
                except Exception:
                    pass
    except Exception:
        log.exception("payment admin callback")
        await q.message.reply_text("⚠️ بررسی پرداخت انجام نشد.")

# ============================================================
# TEXT ROUTER
# ============================================================

async def text_router(update, context):
    if not update.message or not update.message.text:
        return
    ensure_user(update.effective_user)
    uid = update.effective_user.id
    if is_blocked(uid) and not is_admin(uid):
        return
    text = update.message.text.strip()

    routes = {
        "🪙 افزودن دارایی": add_asset_prompt,
        "➕ افزودن دارایی": add_asset_prompt,
        "📋 واچ‌لیست": watchlist_menu,
        "💰 قیمت لحظه‌ای": live_price_menu,
        "📊 تحلیل تکنیکال": analysis_prompt,
        "📊 تحلیل": analysis_prompt,
        "📡 سیگنال معاملاتی": signals_menu,
        "🚨 سیگنال‌ها": signals_menu,
        "🎯 فرصت‌های خرید": buy_opportunities_menu,
        "💳 خرید اشتراک": buy_menu,
        "👤 وضعیت اشتراک": status_menu,
        "🔔 هشدارهای هوشمند": alerts_menu,
        "🔔 هشدارها": alerts_menu,
        "📨 پشتیبانی": support_prompt,
        "📨 ارتباط با پشتیبان": support_prompt,
        "ℹ️ راهنمای ربات": help_text,
        "ℹ️ راهنما": help_text,
        "👨‍💼 پنل مدیریت": admin_panel,
    }
    fn = routes.get(text)
    if fn:
        await fn(update, context)
        return

    if context.user_data.get("support_mode"):
        await save_support(uid, text, update.message.message_id)
        context.user_data["support_mode"] = False
        await update.message.reply_text("✅ پیام شما برای پشتیبانی ارسال شد.")
        return

    if context.user_data.get("awaiting_asset"):
        context.user_data.pop("awaiting_asset", None)
        await process_add_asset(update, context, text)
        return

    if context.user_data.get("admin_reply_to"):
        await send_support_reply(update, context, text)
        return

    if context.user_data.get("admin_mode"):
        mode = context.user_data.pop("admin_mode")
        if mode == "broadcast":
            with db() as c:
                rows = c.execute("""SELECT DISTINCT s.user_id FROM subscriptions s WHERE s.status='active' AND s.end_at>?""",
                                 (now_iso(),)).fetchall()
            sent = 0
            for r in rows:
                try:
                    await context.bot.send_message(r["user_id"], f"📢 <b>اطلاعیه</b>\n\n{escape(text)}",
                                                   parse_mode=ParseMode.HTML)
                    sent += 1
                except Exception:
                    pass
            await update.message.reply_text(f"✅ ارسال شد به {sent} کاربر.")
            return
        elif mode == "message_uid":
            try:
                parts = text.split(maxsplit=1)
                uid = int(parts[0]); msg = parts[1] if len(parts) > 1 else ""
                await context.bot.send_message(uid, f"✉️ <b>پیام مدیر</b>\n\n{escape(msg)}",
                                               parse_mode=ParseMode.HTML)
                await update.message.reply_text("✅ پیام ارسال شد.")
            except Exception as e:
                await update.message.reply_text(f"❌ خطا: {escape(str(e))}", parse_mode=ParseMode.HTML)
            return
        elif mode == "block":
            try:
                target = int(text.strip())
                with db() as c:
                    c.execute("UPDATE users SET blocked=1 WHERE user_id=?", (target,))
                await update.message.reply_text(f"🚫 کاربر {target} مسدود شد.")
            except Exception as e:
                await update.message.reply_text(f"❌ خطا: {escape(str(e))}", parse_mode=ParseMode.HTML)
            return

    if norm_symbol(text) in COINS or norm_symbol(text) in ("XAU", "GOLD18"):
        await process_add_asset(update, context, text)
        return

    await update.message.reply_text("❓ گزینه نامعتبر است. از منوی پایین استفاده کنید.",
                                    reply_markup=main_kb(uid))

async def cancel_command(update, context):
    for k in ("awaiting_asset","support_mode","chat_room","payment_plan",
              "admin_reply_to","admin_mode","message_target"):
        context.user_data.pop(k, None)
    await update.message.reply_text("✅ لغو شد.", reply_markup=main_kb(update.effective_user.id))

async def error_handler(update, context):
    log.exception("Unhandled Telegram error", exc_info=context.error)

# ============================================================
# STARTUP / MAIN
# ============================================================

async def post_init(app):
    init_db()
    try:
        await app.bot.delete_webhook(drop_pending_updates=False)
    except Exception as e:
        log.warning("delete webhook: %s", e)
    app.create_task(alert_worker(app))
    app.create_task(backup_worker())
    app.create_task(market_snapshot_worker())
    log.info("MARKET AI v13 started | professional buy engine enabled | cache=%s",
             len(CACHE))

async def post_shutdown(app):
    global HTTP_SESSION
    if HTTP_SESSION and not HTTP_SESSION.closed:
        await HTTP_SESSION.close()

def main():
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    init_db()
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_text))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CommandHandler("backup", backup_command))
    app.add_handler(CommandHandler("dbstatus", db_status_command))
    app.add_handler(CommandHandler("admin", admin_panel))
    app.add_handler(CallbackQueryHandler(admin_callback, pattern=r"^adm:"))
    app.add_handler(CallbackQueryHandler(payment_admin_callback, pattern=r"^pay:(approve|reject):"))
    app.add_handler(CallbackQueryHandler(plan_callback, pattern=r"^plan:"))
    app.add_handler(CallbackQueryHandler(alert_callback, pattern=r"^alert:toggle$"))
    app.add_handler(CallbackQueryHandler(pick_asset_callback, pattern=r"^pick:"))
    app.add_handler(CallbackQueryHandler(watchlist_delete_callback, pattern=r"^wl:del:"))
    app.add_handler(CallbackQueryHandler(selected_price_callback, pattern=r"^price:"))
    app.add_handler(CallbackQueryHandler(selected_analysis_callback, pattern=r"^analysis:"))
    app.add_handler(CallbackQueryHandler(selected_signal_callback, pattern=r"^signal:"))
    app.add_handler(CallbackQueryHandler(opportunity_action_callback, pattern=r"^op:"))
    app.add_handler(CallbackQueryHandler(support_reply_callback, pattern=r"^sup:reply:"))
    app.add_handler(MessageHandler(filters.PHOTO, receipt_photo))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))
    app.add_error_handler(error_handler)
    log.info("MARKET AI polling started")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=False)

if __name__ == "__main__":
    main()
