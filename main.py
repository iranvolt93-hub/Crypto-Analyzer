# -*- coding: utf-8 -*-
"""
Crypto / Global Gold / Iran 18K Gold Telegram Analyzer
Nobitex market-data build — v18.0 (Nobitex OHLCV + stricter multi-timeframe confirmation)
"""

import os
import re
import sqlite3
import shutil
import asyncio
import logging
import time
import json
from pathlib import Path
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from html import escape
from zoneinfo import ZoneInfo

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

BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()

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

ALERT_INTERVAL_SECONDS = max(300, int(os.getenv("ALERT_INTERVAL_SECONDS", "600")))
MAX_WATCHLIST = max(1, int(os.getenv("MAX_WATCHLIST", "100")))

GOLD_HISTORY_INTERVAL_SECONDS = max(60, int(os.getenv("GOLD_HISTORY_INTERVAL_SECONDS", "300")))
GOLD_MIN_HISTORY_POINTS = max(50, int(os.getenv("GOLD_MIN_HISTORY_POINTS", "50")))
SIGNAL_MIN_SCORE = max(70, min(100, int(os.getenv("SIGNAL_MIN_SCORE", "85"))))
ALERT_OPPORTUNITY_MIN = max(85, min(100, int(os.getenv("ALERT_OPPORTUNITY_MIN", "85"))))
ALERT_STRENGTH_MIN = max(72, min(100, int(os.getenv("ALERT_STRENGTH_MIN", "72"))))
ALERT_PROBABILITY_MIN = max(70, min(100, int(os.getenv("ALERT_PROBABILITY_MIN", "70"))))
ALERT_RSI_MIN = max(40, min(60, int(os.getenv("ALERT_RSI_MIN", "48"))))
ALERT_RSI_MAX = max(65, min(78, int(os.getenv("ALERT_RSI_MAX", "70"))))
ALERT_MIN_TURNOVER = max(0.01, float(os.getenv("ALERT_MIN_TURNOVER", "0.05")))
SIGNAL_CONFIRMATIONS_REQUIRED = max(2, int(os.getenv("SIGNAL_CONFIRMATIONS_REQUIRED", "2")))
SIGNAL_CONFIRM_TIMEFRAME = os.getenv("SIGNAL_CONFIRM_TIMEFRAME", "4H").strip()

ADX_MIN_TREND = max(15, min(30, int(os.getenv("ADX_MIN_TREND", "20"))))
MIN_RR_RATIO = max(1.5, min(3.0, float(os.getenv("MIN_RR_RATIO", "1.8"))))
MIN_TURNOVER_SIGNAL = max(0.02, min(0.30, float(os.getenv("MIN_TURNOVER_SIGNAL", "0.05"))))
MAX_DAILY_SIGNALS_PER_USER = max(1, min(10, int(os.getenv("MAX_DAILY_SIGNALS_PER_USER", "2"))))

MARKET_SCAN_PAGES = max(1, min(10, int(os.getenv("MARKET_SCAN_PAGES", "2"))))
MARKET_SCAN_PER_PAGE = max(50, min(250, int(os.getenv("MARKET_SCAN_PER_PAGE", "250"))))
MARKET_SCAN_TOP = max(5, min(30, int(os.getenv("MARKET_SCAN_TOP", "10"))))
MARKET_SCAN_SECONDS = max(300, int(os.getenv("MARKET_SCAN_SECONDS", "600")))
MARKET_SCAN_DEEP = max(5, min(12, int(os.getenv("MARKET_SCAN_DEEP", "8"))))
MARKET_SCAN_CACHE = None
SHORT_TERM_SCAN_CACHE = None
NOBITEX_BASE_URL = os.getenv("NOBITEX_BASE_URL", "https://apiv2.nobitex.ir").rstrip("/")
NOBITEX_QUOTE = "USDT"  # Entire crypto analysis engine uses USDT-quoted Nobitex markets
NOBITEX_CANDLE_CACHE_SECONDS = max(20, int(os.getenv("NOBITEX_CANDLE_CACHE_SECONDS", "60")))
NOBITEX_MAX_CONCURRENCY = max(2, min(8, int(os.getenv("NOBITEX_MAX_CONCURRENCY", "4"))))

PAYMENT_CARD = os.getenv("PAYMENT_CARD", "ثبت نشده").strip()
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "پشتیبان").strip()

GOLD18_URL = "https://www.tgju.org/profile/geram18"
XAU_TGJU_URL = "https://www.tgju.org/profile/ons"
GOLD18_PRICE_SELECTOR = os.getenv("GOLD18_PRICE_SELECTOR", "").strip()
XAU_PRICE_SELECTOR = os.getenv("XAU_PRICE_SELECTOR", "").strip()
GOLD18_SOURCE_UNIT = os.getenv("GOLD18_SOURCE_UNIT", "auto").strip().lower()

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
IRAN_TZ = ZoneInfo("Asia/Tehran")

# ============================================================
# LRUTTLCache
# ============================================================

class LRUTTLCache:
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

    def pop(self, key, default=None):
        return self._data.pop(key, (None, default))[1]

    def clear(self):
        self._data.clear()

    def __len__(self):
        return len(self._data)


CACHE = LRUTTLCache(maxsize=int(os.getenv("CACHE_MAX_ENTRIES", "2000")))
PRICE_CACHE = LRUTTLCache(maxsize=int(os.getenv("PRICE_CACHE_MAX", "500")))
ANALYSIS_CACHE = LRUTTLCache(maxsize=int(os.getenv("ANALYSIS_CACHE_MAX", "300")))
NOBITEX_CANDLE_CACHE = LRUTTLCache(maxsize=int(os.getenv("NOBITEX_CANDLE_MAX", "600")))
GOLD18_CACHE = None
XAU_CACHE = None

_DB_PREPARED = False
_DB_PREPARE_LOCK = asyncio.Lock()
_NOBITEX_SEM = asyncio.Semaphore(NOBITEX_MAX_CONCURRENCY)

MAIN_MENU = [
    ["🪙 افزودن دارایی", "📋 واچ‌لیست"],
    ["💰 قیمت لحظه‌ای", "📊 تحلیل تکنیکال"],
    ["📡 سیگنال معاملاتی", "🎯 فرصت‌های خرید"],
    ["🎁 تست رایگان", "💳 خرید اشتراک"],
    ["👤 وضعیت اشتراک", "🔔 هشدارهای هوشمند"],
    ["📨 پشتیبانی", "ℹ️ راهنمای ربات"],
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
            if not _db_has_tables(src) or _db_user_count(src) <= 0:
                log.warning("Skipping invalid or empty backup: %s", src)
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
    global _DB_PREPARED
    if _DB_PREPARED:
        return
    target = Path(DB_PATH)
    target.parent.mkdir(parents=True, exist_ok=True)
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)

    if target.exists() and _db_has_tables(target):
        users = _db_user_count(target)
        if users > 0:
            _DB_PREPARED = True
            return
        if AUTO_RESTORE_BACKUP and _restore_latest_backup(target):
            _DB_PREPARED = True
            return

    recovery_candidates = [
        Path("/app/crypto_bot.db"), Path("/app/data/crypto_bot.db"),
        Path("/app/crypto_bot_old.db"), Path("/app/data/crypto_bot_old.db"),
        Path("/app/subscriptions.db"), Path("/app/data/subscriptions.db"),
        Path("/app/zec_bot.db"), Path("/app/data/zec_bot.db"),
        Path("crypto_bot.db"), Path("subscriptions.db"), Path("zec_bot.db"),
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
            _DB_PREPARED = True
            return
        except Exception as e:
            log.warning("Legacy database recovery failed %s: %s", src, e)

    if AUTO_RESTORE_BACKUP:
        _restore_latest_backup(target)
    _DB_PREPARED = True

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
            "path": str(p), "exists": p.exists(), "size": size,
            "persistent_path": str(p).startswith("/data/"),
            "users": _db_user_count(p) if p.exists() else 0,
            "backup_count": len(backups),
            "latest_backup": backups[0].name if backups else "-",
        }
    except Exception:
        return {"path": str(DB_PATH), "exists": False, "size": 0,
                "persistent_path": False, "users": 0,
                "backup_count": 0, "latest_backup": "-"}

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
            asset_key TEXT DEFAULT '',
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
        c.execute("CREATE INDEX IF NOT EXISTS idx_alert_events_user_date ON alert_events(user_id,created_at)")

        c.execute("INSERT OR REPLACE INTO app_meta(key,value) VALUES('schema_version','14')")
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

TRIAL_DAYS = 5
TRIAL_PLAN = "TRIAL5"
TRIAL_ENABLED = os.getenv("TRIAL_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}

def _csv_env(name):
    raw = os.getenv(name, "")
    return [part.strip() for part in re.split(r"[,\n;]+", raw) if part.strip()]

TRIAL_CHANNELS = _csv_env("TRIAL_CHANNELS")
TRIAL_CHANNEL_LINKS = _csv_env("TRIAL_CHANNEL_LINKS")

if TRIAL_CHANNELS and TRIAL_CHANNEL_LINKS and len(TRIAL_CHANNELS) != len(TRIAL_CHANNEL_LINKS):
    log.warning(
        "TRIAL_CHANNELS (%d) and TRIAL_CHANNEL_LINKS (%d) length mismatch.",
        len(TRIAL_CHANNELS), len(TRIAL_CHANNEL_LINKS),
    )

def trial_channel_rows():
    rows = []
    for i, channel in enumerate(TRIAL_CHANNELS):
        link = TRIAL_CHANNEL_LINKS[i] if i < len(TRIAL_CHANNEL_LINKS) else ""
        if not link and channel.startswith("@"):
            link = "https://t.me/" + channel[1:]
        label = channel if channel.startswith("@") else f"کانال {i + 1}"
        rows.append({"chat": channel, "link": link, "label": label})
    return rows

def has_used_trial(uid):
    with db() as c:
        row = c.execute(
            "SELECT 1 FROM subscriptions WHERE user_id=? AND source='trial' LIMIT 1",
            (uid,),
        ).fetchone()
    return row is not None

def start_free_trial(uid):
    now = datetime.now(timezone.utc)
    end = now + timedelta(days=TRIAL_DAYS)
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        already = c.execute(
            "SELECT 1 FROM subscriptions WHERE user_id=? AND source='trial' LIMIT 1",
            (uid,),
        ).fetchone()
        if already:
            return False, "used"
        active = c.execute(
            "SELECT 1 FROM subscriptions WHERE user_id=? AND status='active' AND end_at>? LIMIT 1",
            (uid, now.isoformat()),
        ).fetchone()
        if active:
            return False, "active"
        c.execute("""
            INSERT INTO subscriptions(
                user_id,plan,days,amount,start_at,end_at,status,source,
                payment_request_id,created_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
        """, (uid, TRIAL_PLAN, TRIAL_DAYS, 0, now.isoformat(), end.isoformat(),
              "active", "trial", None, now.isoformat()))
    try:
        asyncio.create_task(asyncio.to_thread(backup_database, "free_trial"))
    except Exception:
        log.exception("free trial backup schedule")
    return True, end.isoformat()

def has_analysis_access(uid):
    return is_admin(uid) or active_subscription(uid) is not None

def has_paid_subscription(uid):
    if is_admin(uid):
        return True
    sub = active_subscription(uid)
    return bool(sub and sub["source"] != "trial" and sub["plan"] != TRIAL_PLAN)

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
        asyncio.create_task(asyncio.to_thread(backup_database, "subscription"))
    except Exception:
        log.exception("subscription backup schedule")
    return True

def format_dt(v):
    try:
        return datetime.fromisoformat(v).astimezone(IRAN_TZ).strftime("%Y/%m/%d %H:%M")
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
                if r.status == 429:
                    ra = r.headers.get("Retry-After")
                    wait = float(ra) if ra and str(ra).replace(".", "", 1).isdigit() else 5.0
                    log.warning("HTTP 429 %s — waiting %.1fs", url, wait)
                    await asyncio.sleep(min(60.0, wait))
                    continue
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
# ASSET HELPERS — v17 (accurate parsing)
# ============================================================

def norm_symbol(value):
    value = (value or "").strip().upper()
    value = value.translate(str.maketrans(
        "۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩",
        "01234567890123456789"
    ))
    value = value.replace(" ", "").replace("‌", "").replace("_", "")
    # Accept common exchange notation such as BTC/USDT, BTC-USDT and BTCUSDT.
    value = value.replace("\\", "/")
    if "/" in value:
        left, right = value.split("/", 1)
        if right in {"USDT", "USDC", "USD", "BTC", "ETH"}:
            value = left
    elif "-" in value:
        left, right = value.rsplit("-", 1)
        if right in {"USDT", "USDC", "USD", "BTC", "ETH"}:
            value = left
    aliases = {
        "GOLD":"XAU","GOLDUSD":"XAU","XAUUSD":"XAU","XAU/USD":"XAU",
        "XAU/USDT":"XAU","XAUUSDT":"XAU","طلا":"XAU","طلایجهانی":"XAU",
        "GERAM18":"GOLD18","GERAM18K":"GOLD18","18K":"GOLD18",
        "GOLD18K":"GOLD18","IRANGOLD":"GOLD18","طلای18":"GOLD18",
        "طلای۱۸":"GOLD18","طلای۱۸عیار":"GOLD18",
        "طلای18":"GOLD18","طلای18عیار":"GOLD18","طلایداخلی":"GOLD18",
    }
    if value in aliases:
        return aliases[value]
    for suffix in ("USDT", "USDC", "USD"):
        if value.endswith(suffix) and len(value) > len(suffix):
            base = value[:-len(suffix)].replace("/", "").replace("-", "")
            if base.isalnum():
                return base
    return value

def asset_type(symbol):
    s = norm_symbol(symbol)
    return "gold" if s == "XAU" else "gold18" if s == "GOLD18" else "crypto"

async def crypto_search(query):
    """Resolve a crypto ticker only when a matching Nobitex spot market exists."""
    raw = (query or "").strip()
    if not raw:
        return []
    direct = norm_symbol(raw)
    rows = await market_universe()
    matches = [x for x in rows if x.get("symbol", "").casefold() == direct.casefold()]
    if not matches:
        # Search by known display names only after the ticker is normalized.
        matches = [x for x in rows if str(x.get("name", "")).casefold() == raw.casefold()]
    return [(x["symbol"], x["market_symbol"], x.get("name", x["symbol"])) for x in matches[:20]]

async def crypto_data(symbol, coin_id=None):
    """Hourly OHLCV history from Nobitex; all values are in the market quote unit."""
    s = norm_symbol(symbol)
    df = await nobitex_candles(s, "1H", 500)
    if df is None or len(df) < 50:
        return None
    return s, df["close"].astype(float).reset_index(drop=True), df["volume"].astype(float).reset_index(drop=True)

async def market_universe():
    """Build a liquid Nobitex market universe and enrich top markets with local OHLC returns."""
    global MARKET_SCAN_CACHE
    now = time.monotonic()
    if MARKET_SCAN_CACHE and now - MARKET_SCAN_CACHE[0] < MARKET_SCAN_SECONDS:
        return MARKET_SCAN_CACHE[1]

    data = await http_json(f"{NOBITEX_BASE_URL}/market/stats", {"dstCurrency": "usdt"}, ttl=MARKET_SCAN_SECONDS, retries=2)
    stats = (data or {}).get("stats") or {}
    rows = []
    for key, item in stats.items():
        if not isinstance(item, dict) or "-" not in key or item.get("isClosed") is True:
            continue
        base, quote = key.lower().split("-", 1)
        if quote != "usdt":
            continue
        sym = base.upper()
        if not sym or sym in {"USDT", "USDC", "DAI", "TUSD", "FDUSD", "USDE"}:
            continue
        # Nobitex USDT market stats are already denominated in Tether.
        raw_price = _safe_float(item.get("latest") or item.get("dayClose"))
        price = raw_price
        quote_volume = _safe_float(item.get("volumeDst"))
        base_volume = _safe_float(item.get("volumeSrc"))
        if price <= 0 or quote_volume <= 0:
            continue
        rows.append({
            "id": f"{sym.lower()}-{quote}", "symbol": sym, "name": sym,
            "market_symbol": f"{sym}{NOBITEX_QUOTE}",
            "current_price": price, "total_volume": quote_volume,
            "volume_src": base_volume, "market_cap": 0.0,
            "market_cap_rank": 0,
            "price_change_percentage_1h_in_currency": 0.0,
            "price_change_percentage_24h_in_currency": _safe_float(item.get("dayChange")),
            "price_change_percentage_7d_in_currency": 0.0,
            "price_change_percentage_14d_in_currency": 0.0,
            "price_change_percentage_30d_in_currency": 0.0,
            "price_change_percentage_200d_in_currency": 0.0,
            "price_change_percentage_1y_in_currency": 0.0,
            "ath_change_percentage": 0.0, "atl_change_percentage": 0.0,
            "day_high": _safe_float(item.get("dayHigh")),
            "day_low": _safe_float(item.get("dayLow")),
            "day_open": _safe_float(item.get("dayOpen")),
            "source": "Nobitex",
        })

    # Only USDT-quoted spot markets enter the analysis universe.
    rows.sort(key=lambda x: x["total_volume"], reverse=True)
    unique, seen = [], set()
    for row in rows:
        if row["symbol"] in seen:
            continue
        seen.add(row["symbol"])
        unique.append(row)
    unique.sort(key=lambda x: x["total_volume"], reverse=True)
    for rank, row in enumerate(unique, 1):
        row["market_cap_rank"] = rank  # local liquidity rank, not global market-cap rank

    # 25 × (1H + 1D) requests stay below the documented public OHLC request budget.
    candidates = unique[:12]
    sem = asyncio.Semaphore(4)
    async def enrich(row):
        async with sem:
            try:
                h1, d1 = await asyncio.gather(
                    nobitex_candles(row["symbol"], "1H", 3),
                    nobitex_candles(row["symbol"], "1D", 250),
                )
                if h1 is not None and len(h1) >= 2:
                    row["price_change_percentage_1h_in_currency"] = (float(h1.close.iloc[-1]) / float(h1.close.iloc[-2]) - 1) * 100
                    row["current_price"] = float(h1.close.iloc[-1])
                if d1 is not None and len(d1) >= 2:
                    c = d1.close.astype(float)
                    for days, field in ((7,"price_change_percentage_7d_in_currency"),(14,"price_change_percentage_14d_in_currency"),(30,"price_change_percentage_30d_in_currency"),(200,"price_change_percentage_200d_in_currency")):
                        if len(c) > days:
                            row[field] = (float(c.iloc[-1]) / float(c.iloc[-days-1]) - 1) * 100
                    peak = float(d1.high.max()) if len(d1) else 0
                    if peak > 0:
                        row["ath_change_percentage"] = (float(c.iloc[-1]) / peak - 1) * 100
                    row["sparkline_in_7d"] = {"price": c.tail(7).tolist()}
            except Exception as exc:
                log.warning("Nobitex market enrichment failed %s: %s", row["symbol"], exc)
    await asyncio.gather(*(enrich(row) for row in candidates), return_exceptions=True)

    MARKET_SCAN_CACHE = (now, unique)
    log.info("NOBITEX MARKET SCAN markets=%s enriched=%s", len(unique), len(candidates))
    return unique

def _clamp(v, lo=0, hi=100):
    return max(lo, min(hi, float(v)))

def _safe_float(v, default=0.0):
    try:
        x = float(v)
        return default if pd.isna(x) else x
    except Exception:
        return default

def _market_growth_score(x, ta=None):
    p = _safe_float(x.get("current_price"))
    if p <= 0:
        return None
    ch1 = _safe_float(x.get("price_change_percentage_1h_in_currency"))
    ch24 = _safe_float(x.get("price_change_percentage_24h_in_currency"))
    ch7 = _safe_float(x.get("price_change_percentage_7d_in_currency"))
    ch14 = _safe_float(x.get("price_change_percentage_14d_in_currency"))
    ch30 = _safe_float(x.get("price_change_percentage_30d_in_currency"))
    vol = _safe_float(x.get("total_volume"))
    mcap = _safe_float(x.get("market_cap"))
    if vol <= 0:
        return None

    changes = [ch1, ch24, ch7, ch14, ch30]
    nonzero = sum(1 for c in changes if abs(c) > 0.001)
    if nonzero < 2:
        return None

    score = 50.0
    score += _clamp(ch1 * 2.0, -8, 8)
    score += _clamp(ch24 * 0.65, -10, 10)
    score += _clamp(ch7 * 0.40, -8, 8)
    score += _clamp(ch14 * 0.20, -5, 5)
    score += _clamp(ch30 * 0.12, -5, 5)
    # Nobitex does not publish circulating supply/market cap in public stats.
    # Use exchange quote-volume rank as a transparent liquidity proxy.
    rank = int(x.get("market_cap_rank") or 999)
    score += _clamp((26 - rank) * 0.18, -4, 4)
    if ch24 > 6: score -= 10
    if ch24 > 12: score -= 10
    if ch7 > 18: score -= 10
    if ch7 > 30: score -= 10
    if ch24 < -20 and ch7 < -20: score -= 8
    if ta:
        if ta.get("late_move_risk") or _safe_float(ta.get("ema21_distance_atr"), 99) > 1.20:
            score -= 22
        score += _clamp((ta.get("strength",50)-50)*0.18, -8, 8)
        if ta.get("trend") in ("BULLISH", "BULLISH_WEAK"): score += 5
        if ta.get("rsi",50) > 75: score -= 5
        elif 52 <= ta.get("rsi",50) <= 68: score += 4
        if ta.get("macd_hist",0) > 0: score += 4
        if ta.get("adx",0) >= 20: score += 3
    return _clamp(score, 0, 100)

def _growth_probability(score, x):
    rank = float(x.get("market_cap_rank") or 10000)
    rank_bonus = _clamp(10 - (rank / 25), -5, 10)
    return _clamp(50 + (score - 50) * 0.75 + rank_bonus, 5, 95)

async def growth_scan():
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
        if (ta.get("late_move_risk")
                or _safe_float(ta.get("ema21_distance_atr"), 99) > 1.20
                or _safe_float(ta.get("rsi"), 50) >= 68
                or _safe_float(x.get("price_change_percentage_24h_in_currency")) > 6
                or _safe_float(x.get("price_change_percentage_7d_in_currency")) > 18):
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

# ---- number / TGJU parsing ----

def digits_to_latin(s):
    return (s or "").translate(str.maketrans(
        "۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩",
        "01234567890123456789"
    ))

def _numbers_with_positions(text, allow_decimal=False):
    raw = digits_to_latin(text or "").replace("٬", ",").replace("\u00a0", " ")
    out = []
    pattern = (
        r"(?<![\d.,])(\d{1,3}(?:[,\s]\d{3})+(?:\.\d{1,6})?|\d{1,6}\.\d{1,6}|\d{5,12})(?![\d.,])"
        if allow_decimal else
        r"(?<!\d)(\d{1,3}(?:[,\s]\d{3})+|\d{5,12})(?!\d)"
    )
    for m in re.finditer(pattern, raw):
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

def _score_xau_node(tag):
    attrs = " ".join(f"{k}={v}" for k, v in tag.attrs.items()).lower()
    text = tag.get_text(" ", strip=True)
    low = text.lower()
    score = 0
    if any(k in attrs for k in ("ons", "xau", "gold")):
        score += 250
    for word in ("انس طلا", "انس جهانی طلا", "اونس طلا", "اونس جهانی", "gold ounce"):
        if word in text:
            score += 200
    if "انس" in text or "اونس" in text:
        score += 60
    if "طلا" in text or "gold" in low:
        score += 60
    if "دلار" in text or "usd" in low:
        score += 40
    for noise in ("نقره", "silver", "platinum", "پلاتین", "palladium", "پالادیوم"):
        if noise in low:
            score -= 800
    if len(text) > 1500:
        score -= 150
    return score

def find_gold18_value(html):
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
            if unit == "rial" and GOLD18_TOMAN_MIN <= raw <= GOLD18_TOMAN_MAX:
                candidates.append((score - 50, raw, "toman", raw))
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

    keywords = ("geram18", "گرم طلای 18", "گرم طلای ۱۸",
                "طلای 18 عیار", "طلای ۱۸ عیار", "طلای18", "طلای۱۸")
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
    best_conf, best_val = candidates[0][0], candidates[0][1]
    if best_conf < 400:
        log.warning("GOLD18 parse confidence too low (%s); rejecting", best_conf)
        return None
    close_vals = [v for c, v, *_ in candidates if c >= 300]
    if close_vals:
        close_vals.sort()
        median = close_vals[len(close_vals) // 2]
        if abs(best_val - median) / max(median, 1) > 0.03:
            log.warning("GOLD18 candidates disagree (best=%.0f median=%.0f); rejecting",
                        best_val, median)
            return None
    log.info("TGJU GOLD18 parsed: %.0f Toman/gram (conf=%s)", best_val, best_conf)
    return float(best_val)

def _score_xau_node_from_ctx(ctx):
    low = (ctx or "").lower()
    s = 0
    if "انس" in ctx or "اونس" in ctx or "ounce" in low: s += 150
    if "طلا" in ctx or "gold" in low: s += 100
    if "دلار" in ctx or "usd" in low: s += 80
    for noise in ("نقره", "نقره‌ای", "silver", "platinum", "پلاتین", "palladium", "پالادیوم"):
        if noise.lower() in low:
            s -= 800
    return s

def find_xau_value(html):
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")

    def is_xau_context(ctx):
        low = (ctx or "").lower()
        has_gold = ("انس" in ctx or "اونس" in ctx or "ounce" in low or "xau" in low)
        has_usd = ("دلار" in ctx or "usd" in low or "$" in ctx)
        for noise in ("نقره", "silver", "پلاتین", "platinum", "پالادیوم", "palladium"):
            if noise in low:
                return False
        return has_gold and has_usd

    candidates = []

    strict_selectors = [
        '[data-symbol="ons"]', '[data-profile="ons"]',
        '[data-code="ons"]', '[data-item="ons"]',
        '[data-symbol="xauusd"]', '[data-code="xauusd"]',
        '#ons', '#xauusd', '.ons-price', '.xauusd-price',
    ]

    for selector in strict_selectors:
        try:
            nodes = soup.select(selector)
        except Exception:
            continue
        for tag in nodes:
            ctx = _context(tag, levels=4, limit=2000)
            if not is_xau_context(ctx):
                continue
            for _, v in _numbers_with_positions(tag.get_text(" ", strip=True), allow_decimal=True):
                if not (XAU_USD_MIN <= v <= XAU_USD_MAX) or v > 20000:
                    continue
                candidates.append((900, v))

    if XAU_PRICE_SELECTOR:
        try:
            for tag in soup.select(XAU_PRICE_SELECTOR):
                text = tag.get_text(" ", strip=True)
                if not is_xau_context(text):
                    continue
                for _, v in _numbers_with_positions(text, allow_decimal=True):
                    if (XAU_USD_MIN <= v <= XAU_USD_MAX) and v <= 20000:
                        candidates.append((1000, v))
        except Exception as e:
            log.warning("Invalid XAU selector: %s", e)

    keywords = ("انس طلا", "انس جهانی طلا", "اونس طلا", "اونس جهانی", "gold ounce", "xau/usd")
    for tag in soup.find_all(["tr", "li", "article", "section", "td", "div"]):
        text = tag.get_text(" ", strip=True)
        if not text or len(text) > 800:
            continue
        compact = text.lower().replace(" ", "")
        if not any(k.lower().replace(" ", "") in compact for k in keywords):
            continue
        if any(n in text.lower() for n in ("نقره", "silver", "platinum", "پلاتین")):
            continue
        for _, v in _numbers_with_positions(text, allow_decimal=True):
            if (XAU_USD_MIN <= v <= XAU_USD_MAX) and v <= 20000:
                candidates.append((500, v))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    top_conf, top_val = candidates[0]
    if top_conf < 500:
        log.warning("XAU parse confidence too low (%s); rejecting", top_conf)
        return None
    close_vals = [v for c, v in candidates if c >= 400]
    close_vals.sort()
    median = close_vals[len(close_vals) // 2]
    if abs(top_val - median) / max(median, 1) > 0.05:
        log.warning("XAU candidates disagree (top=%s median=%s); rejecting", top_val, median)
        return None
    return float(top_val)

def save_market_snapshot(symbol, atype, price):
    if price is None or price <= 0:
        return
    try:
        with db() as c:
            last = c.execute(
                "SELECT price,created_at FROM market_history WHERE symbol=? AND asset_type=? ORDER BY id DESC LIMIT 1",
                (symbol, atype)
            ).fetchone()
            if last:
                try:
                    age = datetime.now(timezone.utc) - datetime.fromisoformat(last["created_at"])
                    if age.total_seconds() < GOLD_HISTORY_INTERVAL_SECONDS * 0.8:
                        return
                    if abs(float(last["price"]) - float(price)) < 1e-9:
                        return
                except Exception:
                    pass
            c.execute(
                "INSERT INTO market_history(symbol,asset_type,price,created_at) VALUES(?,?,?,?)",
                (symbol, atype, float(price), now_iso())
            )
            c.execute(
                "DELETE FROM market_history WHERE symbol=? AND asset_type=? AND id NOT IN ("
                "SELECT id FROM market_history WHERE symbol=? AND asset_type=? ORDER BY id DESC LIMIT 1000)",
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
    if len(history) == 0:
        history = pd.Series([price], dtype=float)
    elif abs(float(history.iloc[-1]) - float(price)) > 1e-12:
        # Use the freshly fetched quote for analysis without forcing an extra DB row.
        history = pd.concat([history, pd.Series([price], dtype=float)], ignore_index=True)
    result = ("GOLD18", history, pd.Series(dtype=float))
    GOLD18_CACHE = (now, result)
    log.info("TGJU GOLD18: %.0f Toman/gram | history=%s", price, len(history))
    return result

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

async def asset_data(symbol, coin_id=None):
    s = norm_symbol(symbol)
    if s == "XAU":
        return await xau_data()
    if s == "GOLD18":
        return await gold18_data()
    return await crypto_data(s, coin_id=coin_id)

async def current_price(symbol, coin_id=None):
    s = norm_symbol(symbol)
    cache_key_price = (s, "nobitex_usdt")
    cached = PRICE_CACHE.get(cache_key_price, PRICE_CACHE_SECONDS)
    if cached is not None:
        return cached
    if s == "GOLD18":
        data = await gold18_data()
        result = {"symbol": s, "price": float(data[1].iloc[-1]), "unit": "تومان برای هر گرم طلای ۱۸ عیار ایران", "source": "TGJU"} if data else None
    elif s == "XAU":
        value = await xau_tgju_price()
        result = {"symbol": s, "price": float(value), "unit": "دلار برای هر اونس", "source": "TGJU"} if value is not None else None
    else:
        data = await http_json(f"{NOBITEX_BASE_URL}/market/stats", {"srcCurrency": s.lower(), "dstCurrency": "usdt"}, ttl=PRICE_CACHE_SECONDS, retries=2)
        stats = (data or {}).get("stats") or {}
        item = stats.get(f"{s.lower()}-usdt")
        if item:
            price = _safe_float(item.get("latest") or item.get("dayClose"))
            result = {"symbol": s, "price": price, "change24": _safe_float(item.get("dayChange")),
                      "unit": "USDT", "source": "Nobitex"}
        else:
            result = None
    if result:
        PRICE_CACHE.set(cache_key_price, result)
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
        number = f"{p:,.0f}" if p >= 1000 else (f"{p:,.4f}".rstrip("0").rstrip(".") if p >= 1 else f"{p:,.8f}".rstrip("0").rstrip("."))
        value = number + " USDT"
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
# PROFESSIONAL OHLCV (NOBITEX) — v18
# ============================================================

async def nobitex_candles(symbol, bar="1H", limit=240):
    """Nobitex OHLC candles. All crypto OHLC values are quoted in USDT."""
    s = norm_symbol(symbol)
    resolution = {"5m":"5", "15m":"15", "1H":"60", "4H":"240", "1D":"D", "D":"D"}.get(bar, str(bar))
    if bar == "1W":
        # Weekly candles are not natively offered; derive them from daily candles.
        daily = await nobitex_candles(s, "1D", min(500, max(240, int(limit))))
        if daily is None or len(daily) < 200:
            return None
        weekly = daily.set_index(pd.to_datetime(daily["ts"], unit="s", utc=True)).resample("W-SUN").agg(
            {"open":"first", "high":"max", "low":"min", "close":"last", "volume":"sum"}
        ).dropna().reset_index(names="ts")
        weekly["ts"] = (weekly["ts"].astype("int64") // 10**9).astype(int)
        return weekly.tail(min(500, int(limit))).reset_index(drop=True) if len(weekly) >= 200 else None

    market_symbol = f"{s}{NOBITEX_QUOTE}"
    key = ("NOBITEX", market_symbol, resolution, int(limit))
    cached = NOBITEX_CANDLE_CACHE.get(key, NOBITEX_CANDLE_CACHE_SECONDS)
    if cached is not None:
        return cached.copy()
    params = {"symbol": market_symbol, "resolution": resolution,
              "to": str(int(time.time())), "countback": str(min(500, max(2, int(limit))))}
    async with _NOBITEX_SEM:
        data = await http_json(f"{NOBITEX_BASE_URL}/market/udf/history", params,
                               ttl=NOBITEX_CANDLE_CACHE_SECONDS, retries=2)
    # No fallback to IRT/RLS: mixing quote currencies would corrupt indicators,
    # support/resistance, risk levels and backtests. Missing USDT pairs fail clearly.
    price_factor = 1.0
    if not data or data.get("s") != "ok":
        return None
    try:
        ts, oo, hh, ll, cc, vv = (data.get(k) or [] for k in ("t","o","h","l","c","v"))
        n = min(map(len, (ts,oo,hh,ll,cc,vv)))
        if n < 2:
            return None
        df = pd.DataFrame({"ts":ts[:n], "open":oo[:n], "high":hh[:n], "low":ll[:n], "close":cc[:n], "volume":vv[:n]})
        for col in ("open","high","low","close"):
            df[col] = pd.to_numeric(df[col], errors="coerce") * price_factor
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        df["ts"] = pd.to_numeric(df["ts"], errors="coerce")
        df = df.dropna().drop_duplicates("ts").sort_values("ts").reset_index(drop=True)
        seconds = {"1":"60", "5":"300", "15":"900", "30":"1800", "60":"3600", "180":"10800", "240":"14400", "360":"21600", "720":"43200", "D":"86400"}.get(resolution, 3600)
        # Exclude the currently forming candle; indicators only see closed bars.
        now = int(time.time())
        df = df[(df["ts"].astype(int) + seconds) <= now].copy().reset_index(drop=True)
        required_bars = min(200, max(2, int(limit)))
        if len(df) < required_bars:
            log.warning("Nobitex confirmed candle history short %s %s: %s/%s", market_symbol, resolution, len(df), required_bars)
            return None
        df["confirm"] = 1
        NOBITEX_CANDLE_CACHE.set(key, df.copy())
        return df.tail(min(500, int(limit))).reset_index(drop=True)
    except Exception:
        log.exception("Nobitex OHLC parse failed %s %s", market_symbol, resolution)
        return None

def _wilder_smooth(series, period=14):
    """Wilder smoothing seeded with the first period's simple average."""
    values = pd.to_numeric(pd.Series(series), errors="coerce").astype(float).tolist()
    out = [float("nan")] * len(values)
    if period < 1 or len(values) < period:
        return pd.Series(out, index=getattr(series, "index", None), dtype=float)

    seed_end = None
    for end in range(period - 1, len(values)):
        window = values[end - period + 1:end + 1]
        if all(pd.notna(v) for v in window):
            seed_end = end
            out[end] = sum(window) / period
            break
    if seed_end is not None:
        for i in range(seed_end + 1, len(values)):
            value = values[i]
            if pd.notna(value):
                out[i] = (out[i - 1] * (period - 1) + value) / period
            else:
                out[i] = out[i - 1]
    return pd.Series(out, index=getattr(series, "index", None), dtype=float)


def _wilder_rsi(close, period=14):
    close = pd.to_numeric(pd.Series(close), errors="coerce").astype(float)
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = _wilder_smooth(gain, period)
    avg_loss = _wilder_smooth(loss, period)
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    out = 100 - (100 / (1 + rs))
    out = out.where(avg_loss != 0, 100.0)
    out = out.where(avg_gain != 0, 0.0)
    out = out.where((avg_gain != 0) | (avg_loss != 0), 50.0)
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
            (x["close"] > 0) & (x["volume"] >= 0) &
            (x["high"] >= x["low"]) &
            (x["high"] >= x[["open", "close"]].max(axis=1)) &
            (x["low"] <= x[["open", "close"]].min(axis=1))
        )
        x = x.loc[valid].reset_index(drop=True)
    if len(x) < 200:
        return None

    c = x["close"].astype(float); h = x["high"].astype(float)
    l = x["low"].astype(float); v = x["volume"].astype(float)

    ema9 = c.ewm(span=9, adjust=False).mean()
    ema21 = c.ewm(span=21, adjust=False).mean()
    ema50 = c.ewm(span=50, adjust=False).mean()
    ema200 = c.ewm(span=200, adjust=False, min_periods=200).mean()
    rsi = _wilder_rsi(c, 14)
    macd = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
    macd_sig = macd.ewm(span=9, adjust=False).mean()
    macd_hist = macd - macd_sig

    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    atr = _wilder_smooth(tr, 14)

    up = h.diff(); down = -l.diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    atr_w = _wilder_smooth(tr, 14)
    plus_di = 100 * _wilder_smooth(plus_dm, 14) / atr_w.replace(0, float('nan'))
    minus_di = 100 * _wilder_smooth(minus_dm, 14) / atr_w.replace(0, float('nan'))
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, float('nan'))
    adx = _wilder_smooth(dx, 14)

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
    """v17: strict entry gates — trend==BULLISH, structure==BULLISH, ADX≥20, vol≥1.10, RR≥1.8"""
    if df is None or len(df) < 200:
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

    extension = (current - e21) / atr if atr else 99.0
    prev_close = float(c.iloc[-2]) if len(c) >= 2 else current
    prev_e21 = _safe_float(z["ema21"].iloc[-2], e21) if len(z["ema21"]) >= 2 else e21
    prev_extension = (prev_close - prev_e21) / atr if atr else 99.0
    if extension > 1.35:
        bull -= 12; rb.append("قیمت از EMA21 فاصله گرفته؛ ورود تعقیبی پرریسک است")
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

    # Early-entry detector: prefer the first controlled pullback/turn before
    # a fully mature trend. This is deliberately separate from a late breakout.
    # It requires improving momentum, a non-bearish structure and price near EMA21.
    ema21_distance_atr = extension
    # EARLY means a controlled first reclaim/pullback near EMA21, not an already
    # extended green candle. Requiring the previous close near/below EMA21
    # prevents a mature rally from being relabelled as an early entry.
    reclaim_or_pullback = (
        (-0.35 <= ema21_distance_atr <= 0.75)
        and (prev_extension <= 0.45 or (prev_close <= prev_e21 and current > e21))
    )
    early_entry = (
        timeframe in ("15m", "1H", "4H")
        and trend in ("BULLISH", "BULLISH_WEAK", "NEUTRAL")
        and structure["structure"] != "BEARISH"
        and e9 >= e21 * 0.995
        and reclaim_or_pullback
        and 50 <= rsi <= 62
        and roc5 > 0
        and roc20 > -0.50
        and hist > prev_hist
        and dip >= dim
        and adx >= 12
        and vol_ratio >= 0.85
        and not fakeout_risk
        and not breakout_up
    )
    # Hard anti-chasing: reject stretched price, overbought RSI, or an impulse
    # candle that has already travelled too far from its short-term mean.
    late_move_risk = (
        ema21_distance_atr > 1.25
        or rsi >= 68
        or roc5 >= 5.0
        or (breakout_up and not breakout_confirmed)
    )
    standard_buy_ok = (
        bull >= 65 and edge >= 22 and
        trend == "BULLISH" and
        structure["structure"] == "BULLISH" and
        45 < rsi < 66 and
        adx >= ADX_MIN_TREND and
        vol_ratio >= 1.10 and
        ema21_distance_atr <= 1.20 and
        not fakeout_risk and not late_move_risk
    )
    buy_ok = standard_buy_ok or (early_entry and bull >= 38 and edge >= 8)
    sell_ok = (
        bear >= 65 and edge <= -22 and
        trend == "BEARISH" and
        structure["structure"] == "BEARISH" and
        32 < rsi < 55 and
        adx >= ADX_MIN_TREND and
        vol_ratio >= 1.10 and
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
    if candidate != "WAIT" and rr < MIN_RR_RATIO:
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
        "early_entry": bool(early_entry),
        "entry_stage": "EARLY" if (buy_ok and early_entry and not standard_buy_ok) else "CONFIRMED" if buy_ok else "WAIT",
        "ema21_distance_atr": ema21_distance_atr,
        "prev_ema21_distance_atr": prev_extension,
        "late_move_risk": bool(late_move_risk),
        "roc5": roc5, "roc20": roc20,
        "buy_score": bull, "sell_score": bear,
        "signal_candidate": candidate, "signal_score": signal_score,
        "strength": strength,
        "probability": _clamp(50 + abs(edge) * 0.72 + (7 if vol_ratio >= 1.15 else 0)),
        "reasons_buy": list(dict.fromkeys(rb))[:8],
        "reasons_sell": list(dict.fromkeys(rs))[:8],
        "stop": stop, "targets": targets, "rr": rr,
        "history_points": len(df), "data_source": "Nobitex OHLCV professional",
    }


# ============================================================
# FALLBACK ANALYSIS (GOLD18/XAU, close-only)
# ============================================================

def _rsi(series, period=14):
    series = pd.to_numeric(series, errors="coerce").astype(float)
    delta = series.diff()
    gain = delta.clip(lower=0); loss = -delta.clip(upper=0)
    avg_gain = _wilder_smooth(gain, period)
    avg_loss = _wilder_smooth(loss, period)
    rs = avg_gain / avg_loss.replace(0, float('nan'))
    out = 100 - (100 / (1 + rs))
    out = out.where(avg_loss != 0, 100.0)
    out = out.where(avg_gain != 0, 0.0)
    out = out.where((avg_gain != 0) | (avg_loss != 0), 50.0)
    return out

def _atr(p, period=14):
    tr = p.diff().abs()
    return _wilder_smooth(tr, period)

def _adx(p, period=14):
    """Close-only directional-strength proxy; not standard OHLC Wilder ADX."""
    move = p.diff()
    up = move.clip(lower=0); down = (-move).clip(lower=0); tr = move.abs()
    atr = _wilder_smooth(tr, period)
    dip = 100 * _wilder_smooth(up, period) / atr.replace(0, float('nan'))
    dim = 100 * _wilder_smooth(down, period) / atr.replace(0, float('nan'))
    dx = (100 * (dip - dim).abs() / (dip + dim).replace(0, float('nan')))
    adx = _wilder_smooth(dx, period)
    return adx, dip, dim

def _format_price(symbol, value):
    value = _safe_float(value)
    if symbol == "GOLD18": return f"{value:,.0f} تومان"
    if symbol == "XAU": return f"{value:,.2f} دلار"
    if abs(value) >= 1000:
        number = f"{value:,.0f}"
    elif abs(value) >= 1:
        number = f"{value:,.4f}".rstrip("0").rstrip(".")
    else:
        number = f"{value:,.8f}".rstrip("0").rstrip(".")
    return number + " USDT"

def technical_analysis(symbol, prices, volumes=None):
    p = pd.to_numeric(pd.Series(prices), errors="coerce")
    p = p.replace([float("inf"), float("-inf")], float("nan")).dropna().astype(float).reset_index(drop=True)
    if len(p) < 50:
        return None

    ema9 = p.ewm(span=9, adjust=False).mean()
    ema21 = p.ewm(span=21, adjust=False).mean()
    ema50 = p.ewm(span=50, adjust=False).mean()
    ema200 = p.ewm(span=200, adjust=False, min_periods=200).mean()

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
    ema200_available = bool(pd.notna(ema200.iloc[-1]))
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

    if ema200_available and current > e9 > e21 > e50 and current > e200:
        trend = "BULLISH"
    elif ema200_available and current < e9 < e21 < e50 and current < e200:
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
        (1 if current > e200 else -1) if ema200_available else 0,
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
    candidate = direction if score >= SIGNAL_MIN_SCORE and (score - second) >= 25 else "WAIT"

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
    if candidate != "WAIT" and rr < MIN_RR_RATIO:
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
        "ema200": e200 if ema200_available else None,
        "ema200_available": ema200_available,
        "history_points": len(p),
        "adx_method": "close-only directional proxy",
        "data_quality_warning": "این تحلیل از قیمت بسته‌شدن استفاده می‌کند؛ ADX/DI آن جایگزین ADX استاندارد مبتنی بر OHLC نیست.",
        "signal": "WAIT", "confirmed": False, "confirmations": 0,
        "data_source": "TGJU (close-only)" if symbol in ("GOLD18", "XAU") else "Nobitex (close-only fallback)",
    }

# ============================================================
# BACKTEST
# ============================================================

def walk_forward_backtest(prices, horizon=6, fee_pct=0.10, min_trades=4):
    try:
        p = pd.to_numeric(pd.Series(prices), errors="coerce").replace([float("inf"), float("-inf")], float("nan")).dropna().astype(float).reset_index(drop=True)
        p = p[p > 0].reset_index(drop=True)
        if len(p) < max(100, horizon + 40):
            return {"available": False, "points": int(len(p)), "reason": "تاریخچه واقعی کافی نیست"}
        ema9 = p.ewm(span=9, adjust=False).mean()
        ema21 = p.ewm(span=21, adjust=False).mean()
        delta = p.diff()
        gain = delta.clip(lower=0).ewm(alpha=1/14, adjust=False, min_periods=14).mean()
        loss = (-delta.clip(upper=0)).ewm(alpha=1/14, adjust=False, min_periods=14).mean()
        rs = gain / loss.replace(0, float("nan"))
        rsi = (100 - 100 / (1 + rs)).fillna(50)
        trades = []
        last_signal_index = len(p) - horizon - 1
        i = 30
        while i <= last_signal_index:
            if ema9.iloc[i] > ema21.iloc[i] and 48 <= rsi.iloc[i] <= 70:
                direction = 1
            elif ema9.iloc[i] < ema21.iloc[i] and 30 <= rsi.iloc[i] <= 52:
                direction = -1
            else:
                i += 1
                continue
            entry_i = i + 1
            exit_i = min(entry_i + int(horizon), len(p) - 1)
            if exit_i <= entry_i:
                i += 1
                continue
            gross = (float(p.iloc[exit_i]) / float(p.iloc[entry_i]) - 1.0) * 100.0 * direction
            net = gross - max(0.0, float(fee_pct))
            trades.append({"net": net, "gross": gross, "entry": entry_i, "exit": exit_i})
            i = exit_i
        if len(trades) < min_trades:
            return {"available": False, "points": int(len(p)), "trades": len(trades),
                    "reason": f"تعداد معاملات تاریخی کم است ({len(trades)} از حداقل {min_trades})"}
        returns = [t["net"] for t in trades]
        equity = 1.0
        curve = [equity]
        for ret in returns:
            equity *= max(0.000001, 1 + ret / 100.0)
            curve.append(equity)
        peak = curve[0]
        max_dd = 0.0
        for val in curve:
            peak = max(peak, val)
            if peak > 0:
                max_dd = max(max_dd, (peak - val) / peak * 100.0)
        return {
            "available": True, "points": int(len(p)), "trades": len(trades),
            "win_rate": sum(1 for x in returns if x > 0) / len(returns) * 100.0,
            "avg_return": sum(returns) / len(returns),
            "total_return": (equity - 1) * 100.0,
            "max_drawdown": max_dd, "horizon": int(horizon),
            "fee_pct": max(0.0, float(fee_pct)),
            "method": "EMA9/EMA21 + RSI، ورود کندل بعد، خروج پس از افق ثابت",
        }
    except Exception as exc:
        log.warning("walk-forward backtest failed: %s", exc)
        return {"available": False, "reason": "محاسبه بک‌تست ناموفق بود"}

def backtest_text(result, symbol):
    if symbol == "BTC":
        return ""
    if not result:
        return "🧪 بک‌تست: داده قابل ارزیابی در دسترس نیست.\n"
    if not result.get("available"):
        pts = result.get("points")
        pts_text = f" | نقاط: {pts}" if pts is not None else ""
        return f"🧪 بک‌تست: <b>قابل گزارش نیست</b>{pts_text}\nعلت: {escape(str(result.get('reason', 'تاریخچه کافی نیست')))}\n"
    is_gold = symbol in ("GOLD18", "XAU")
    source_line = ("منبع: اسنپ‌شات‌های ذخیره‌شده TGJU (قیمت نقطه‌ای؛ نه کندل OHLC)\n"
                   if is_gold else "منبع: کندل‌های بسته‌شده ۱ساعته نوبیتکس\n")
    if is_gold:
        minutes_per_point = max(1, GOLD_HISTORY_INTERVAL_SECONDS // 60)
        horizon_label = f"اسنپ‌شات TGJU (~{minutes_per_point} دقیقه/نقطه → افق ≈ {result['horizon'] * minutes_per_point} دقیقه)"
    else:
        horizon_label = "کندل ۱ساعته"
    return (
        "🧪 <b>بک‌تست تاریخی تشخیصی</b>\n"
        f"نمونه: {result['points']} نقطه | معاملات: {result['trades']} | افق: {result['horizon']} {horizon_label}\n"
        + source_line
        + f"درصد معاملات مثبت: <b>{result['win_rate']:.1f}%</b> | میانگین بازده خالص/معامله: <b>{result['avg_return']:+.2f}%</b>\n"
        + f"بازده تجمعی شبیه‌سازی: <b>{result['total_return']:+.2f}%</b> | افت سرمایه بیشینه: <b>{result['max_drawdown']:.2f}%</b>\n"
        + f"کارمزد/اصطکاک لحاظ‌شده: {result['fee_pct']:.2f}% رفت‌وبرگشت\n"
        + "⚠️ این آزمون از راهبرد ساده EMA/RSI روی تاریخچه موجود استفاده می‌کند؛ نتیجه آینده را تضمین نمی‌کند.\n"
    )


# ============================================================
# UNIVERSAL CRYPTO ADVANCED ANALYSIS ENGINE — user strategy specification
# ============================================================

UNIVERSAL_CRYPTO_ANALYSIS_SPEC = {
    "timeframes": ("1H", "4H", "1D"),
    "rsi_period": 14,
    "macd": (12, 26, 9),
    "bollinger": (20, 2),
    "ichimoku": (9, 26, 52),
    "volume_profile_bins": 24,
    "backtest_days": 180,
    "minimum_rr": 3.0,
    "risk_per_trade_pct": 2.0,
}


def _crypto_ichimoku_profile(df):
    """Calculate Ichimoku and a volume-by-price approximation from real OHLCV."""
    if df is None or len(df) < 60:
        return {}
    h = pd.to_numeric(df["high"], errors="coerce")
    l = pd.to_numeric(df["low"], errors="coerce")
    c = pd.to_numeric(df["close"], errors="coerce")
    v = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
    tenkan = (h.rolling(9).max() + l.rolling(9).min()) / 2
    kijun = (h.rolling(26).max() + l.rolling(26).min()) / 2
    # Compare price with the cloud projected 26 candles ahead (shifted series).
    span_a = ((tenkan + kijun) / 2).shift(26)
    span_b = ((h.rolling(52).max() + l.rolling(52).min()) / 2).shift(26)
    price = float(c.iloc[-1])
    sa, sb = _safe_float(span_a.iloc[-1]), _safe_float(span_b.iloc[-1])
    cloud_top, cloud_bottom = max(sa, sb), min(sa, sb)
    cloud_state = "ABOVE" if price > cloud_top else "BELOW" if price < cloud_bottom else "INSIDE"

    # Approximate volume profile: volume is assigned to each candle's close.
    sample = pd.DataFrame({"price": c.tail(180), "volume": v.tail(180)}).dropna()
    sample = sample[sample["volume"] > 0]
    vp = {}
    if len(sample) >= 20 and sample["price"].max() > sample["price"].min():
        bins = pd.cut(sample["price"], bins=24, duplicates="drop")
        grouped = sample.groupby(bins, observed=False)["volume"].sum()
        grouped = grouped[grouped > 0].sort_values(ascending=False)
        if len(grouped):
            poc_interval = grouped.index[0]
            poc = float((poc_interval.left + poc_interval.right) / 2)
            total = float(grouped.sum())
            selected, accum = [], 0.0
            for interval, vol in grouped.items():
                selected.append(interval)
                accum += float(vol)
                if accum >= total * 0.70:
                    break
            vp = {
                "poc": poc,
                "value_area_low": float(min(x.left for x in selected)),
                "value_area_high": float(max(x.right for x in selected)),
            }
    return {
        "tenkan": _safe_float(tenkan.iloc[-1]),
        "kijun": _safe_float(kijun.iloc[-1]),
        "cloud_top": cloud_top, "cloud_bottom": cloud_bottom,
        "cloud_state": cloud_state, **vp,
    }


def _crypto_pattern_scan(df):
    """Conservative swing-pattern hints; patterns are never used alone as signals."""
    if df is None or len(df) < 45:
        return []
    x = df.tail(100).reset_index(drop=True)
    highs, lows = [], []
    for i in range(2, len(x) - 2):
        hi = float(x["high"].iloc[i])
        lo = float(x["low"].iloc[i])
        if hi >= float(x["high"].iloc[i-2:i+3].max()):
            highs.append((i, hi))
        if lo <= float(x["low"].iloc[i-2:i+3].min()):
            lows.append((i, lo))
    found = []
    if len(highs) >= 2:
        a, b = highs[-2], highs[-1]
        if abs(a[1] - b[1]) / max(a[1], b[1]) <= 0.015:
            found.append("احتمال سقف دوقلو؛ نیازمند شکست خط گردن")
    if len(lows) >= 2:
        a, b = lows[-2], lows[-1]
        if abs(a[1] - b[1]) / max(a[1], b[1]) <= 0.015:
            found.append("احتمال کف دوقلو؛ نیازمند شکست خط گردن")
    if len(highs) >= 3 and len(lows) >= 3:
        h3, l3 = highs[-3:], lows[-3:]
        if h3[0][1] > h3[1][1] > h3[2][1] and l3[0][1] < l3[1][1] < l3[2][1]:
            found.append("مثلث متقارن/فشردگی قیمت")
        if h3[1][1] > h3[0][1] * 1.01 and h3[1][1] > h3[2][1] * 1.01:
            found.append("ساختار احتمالی سر و شانه؛ تأیید با شکست گردن لازم است")
    return found[:3]


def _crypto_backtest_six_months(df, symbol, fee_pct=0.10):
    """Daily OHLCV walk-forward simulation, using only information available at entry."""
    if df is None or len(df) < 100:
        return {"available": False, "reason": "کمتر از ۱۰۰ کندل روزانه موجود است"}
    x = df.tail(190).reset_index(drop=True).copy()
    c, h, l, o, v = (x[k].astype(float) for k in ("close", "high", "low", "open", "volume"))
    rsi = _wilder_rsi(c, 14)
    sma50 = c.rolling(50).mean()
    macd = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
    macd_sig = macd.ewm(span=9, adjust=False).mean()
    vol_avg14 = v.rolling(14).mean()
    trades = []
    i = 60
    while i < len(x) - 2:
        div = _divergence(c.iloc[:i+1], rsi.iloc[:i+1], window=35)
        macd_div = _divergence(c.iloc[:i+1], macd.iloc[:i+1], window=35)
        vol_ok = vol_avg14.iloc[i] > 0 and v.iloc[i] > vol_avg14.iloc[i]
        bullish = (
            c.iloc[i] > sma50.iloc[i] and rsi.iloc[i] < 30
            and vol_ok and (div == "BULLISH" or macd_div == "BULLISH")
        )
        change7 = ((float(c.iloc[i]) / float(c.iloc[i-7])) - 1) * 100 if c.iloc[i-7] else 0
        bearish = bool(
            vol_avg14.iloc[i] > 0 and rsi.iloc[i] > 70
            and (div == "BEARISH" or macd_div == "BEARISH")
            and v.iloc[i] < vol_avg14.iloc[i]
            and change7 >= 8.0
        )
        # Entry is next day's open; 0.75% stop and 2.25% target (RR 3:1).
        direction = 1 if bullish else -1 if bearish else 0
        if not direction:
            i += 1
            continue
        entry_i = i + 1
        entry = float(o.iloc[entry_i])
        if entry <= 0:
            i += 1
            continue
        stop = entry * (1 - 0.0075 * direction)
        target = entry * (1 + 0.0225 * direction)
        exit_i, exit_price, reason = min(entry_i + 10, len(x)-1), float(c.iloc[min(entry_i+10, len(x)-1)]), "time"
        for j in range(entry_i, min(entry_i + 11, len(x))):
            hit_stop = float(l.iloc[j]) <= stop if direction == 1 else float(h.iloc[j]) >= stop
            hit_target = float(h.iloc[j]) >= target if direction == 1 else float(l.iloc[j]) <= target
            if hit_stop and hit_target:
                exit_i, exit_price, reason = j, stop, "stop_first_conservative"
                break
            if hit_stop:
                exit_i, exit_price, reason = j, stop, "stop"
                break
            if hit_target:
                exit_i, exit_price, reason = j, target, "target"
                break
        gross = ((exit_price / entry) - 1) * 100 * direction
        net = gross - max(0.0, fee_pct)
        trades.append({"net": net, "gross": gross, "reason": reason})
        i = exit_i + 1
    if len(trades) < 4:
        return {"available": False, "points": len(x), "trades": len(trades),
                "reason": f"تنها {len(trades)} معامله واجد شرایط؛ برای آمار معتبر حداقل ۴ لازم است"}
    rets = [t["net"] for t in trades]
    wins = [z for z in rets if z > 0]
    losses = [z for z in rets if z < 0]
    gross_profit, gross_loss = sum(wins), abs(sum(losses))
    equity, peak, max_dd = 1.0, 1.0, 0.0
    for ret in rets:
        equity *= max(0.000001, 1 + ret / 100)
        peak = max(peak, equity)
        max_dd = max(max_dd, (peak - equity) / peak * 100)
    return {
        "available": True, "points": len(x), "trades": len(trades),
        "win_rate": len(wins) / len(trades) * 100,
        "profit_factor": gross_profit / gross_loss if gross_loss else None,
        "max_drawdown": max_dd, "total_return": (equity - 1) * 100,
        "avg_return": sum(rets) / len(rets), "fee_pct": fee_pct,
        "method": f"{symbol} daily RSI14 + SMA50 + volume14 + divergence/MACD; next-open entry; SL 0.75%, TP 2.25%, max 10 days",
    }


async def _universal_crypto_prompt_analysis(symbol, tfs):
    """Apply the requested advanced strategy to every supported crypto symbol."""
    fetched = await asyncio.gather(
        nobitex_candles(symbol, "1H", 240),
        nobitex_candles(symbol, "4H", 240),
        nobitex_candles(symbol, "1D", 240),
        return_exceptions=True,
    )
    frames = {}
    for tf, frame in zip(("1H", "4H", "1D"), fetched):
        if isinstance(frame, Exception) or frame is None:
            continue
        frames[tf] = frame
    if "1D" not in frames or "4H" not in frames or "1H" not in frames:
        return {"data_complete": False, "missing": [x for x in ("1H", "4H", "1D") if x not in frames]}
    daily = frames["1D"]
    daily_close = daily["close"].astype(float)
    daily_vol = daily["volume"].astype(float)
    daily_sma50 = _safe_float(daily_close.rolling(50).mean().iloc[-1])
    daily_rsi = _safe_float(_wilder_rsi(daily_close, 14).iloc[-1], 50)
    daily_vol_avg14 = _safe_float(daily_vol.rolling(14).mean().iloc[-1])
    daily_vol_ratio = daily_vol.iloc[-1] / daily_vol_avg14 if daily_vol_avg14 > 0 else 0
    daily_div = _divergence(daily_close, _wilder_rsi(daily_close, 14))
    metrics, patterns = {}, {}
    for tf, frame in frames.items():
        z = _advanced_indicators(frame)
        if not z:
            continue
        profile = _crypto_ichimoku_profile(frame)
        metrics[tf] = {
            "price": float(z["c"].iloc[-1]),
            "rsi": _safe_float(z["rsi"].iloc[-1], 50),
            "macd": _safe_float(z["macd"].iloc[-1]),
            "macd_signal": _safe_float(z["macd_sig"].iloc[-1]),
            "macd_hist": _safe_float(z["macd_hist"].iloc[-1]),
            "macd_hist_prev": _safe_float(z["macd_hist"].iloc[-2]),
            "volume_ratio": (_safe_float(z["v"].iloc[-1]) /
                             _safe_float(z["vol_ma20"].iloc[-1], 1)
                             if _safe_float(z["vol_ma20"].iloc[-1]) > 0 else 0),
            "bollinger_position": (
                (_safe_float(z["c"].iloc[-1]) - _safe_float(z["bb_lower"].iloc[-1])) /
                (_safe_float(z["bb_upper"].iloc[-1]) - _safe_float(z["bb_lower"].iloc[-1])) * 100
                if _safe_float(z["bb_upper"].iloc[-1]) > _safe_float(z["bb_lower"].iloc[-1]) else 50
            ),
            "bollinger_upper": _safe_float(z["bb_upper"].iloc[-1]),
            "bollinger_lower": _safe_float(z["bb_lower"].iloc[-1]),
            "obv_rising": bool(z["obv"].iloc[-1] > z["obv"].iloc[-6]) if len(z["obv"]) >= 6 else False,
            "divergence": _divergence(z["c"], z["rsi"]),
            "macd_divergence": _divergence(z["c"], z["macd"]),
            "support": float(frame["low"].tail(30).min()),
            "resistance": float(frame["high"].tail(30).max()),
            "ichimoku": profile,
            "pattern": _crypto_pattern_scan(frame),
        }
    m1, m4, md = metrics.get("1H", {}), metrics.get("4H", {}), metrics.get("1D", {})
    price = _safe_float(md.get("price"))
    # The user's oversold/overbought criteria are intentionally strict.
    bullish_divergence = any(
        m.get(key) == "BULLISH"
        for m in (m1, m4, md)
        for key in ("divergence", "macd_divergence")
    ) or daily_div == "BULLISH"
    bearish_divergence = any(
        m.get(key) == "BEARISH"
        for m in (m1, m4, md)
        for key in ("divergence", "macd_divergence")
    ) or daily_div == "BEARISH"
    buy_condition = (
        daily_sma50 > 0 and price > daily_sma50
        and min(_safe_float(m1.get("rsi"), 50), _safe_float(m4.get("rsi"), 50)) < 30
        and daily_vol_ratio > 1.0 and bullish_divergence
    )
    # X/social sentiment feeds are not connected: price expansion is only a proxy,
    # never represented as a measured Fear & Greed or social-media reading.
    daily_change7 = ((price / float(daily_close.iloc[-8])) - 1) * 100 if len(daily_close) >= 8 and daily_close.iloc[-8] else 0
    fomo_proxy = daily_change7 >= 8
    bearish_condition = (
        max(_safe_float(m1.get("rsi"), 50), _safe_float(m4.get("rsi"), 50)) > 70
        and bearish_divergence
        and daily_vol_ratio < 1.0 and fomo_proxy
    )
    candidate = "BUY" if buy_condition else "SELL" if bearish_condition else "WAIT"
    entry = price
    stop = target1 = target2 = rr = 0.0
    if candidate in ("BUY", "SELL") and price > 0:
        # Fixed risk envelope respects the requested 0.5–1% stop and >=3:1 RR.
        stop_pct = 0.0075
        target_pct = 0.0225
        direction = 1 if candidate == "BUY" else -1
        stop = entry * (1 - stop_pct * direction)
        target1 = entry * (1 + target_pct * direction)
        target2 = entry * (1 + 0.03 * direction)
        rr = target_pct / stop_pct
    backtest = _crypto_backtest_six_months(daily, symbol)
    return {
        "data_complete": True, "timeframes": metrics,
        "daily_sma50": daily_sma50, "daily_rsi": daily_rsi,
        "daily_volume_ratio": daily_vol_ratio, "daily_divergence": daily_div,
        "daily_change7": daily_change7, "fomo_proxy": fomo_proxy,
        "candidate": candidate, "entry": entry, "stop": stop,
        "target1": target1, "target2": target2, "rr": rr,
        "backtest": backtest,
        "fundamental_status": "داده آن‌چین، اخبار کلان و شبکه‌های اجتماعی در این نسخه به API معتبر متصل نیست؛ در نتیجه برای آن‌ها عدد یا نتیجه ساختگی تولید نمی‌شود.",
    }


def universal_crypto_prompt_report(a):
    b = a.get("prompt_analysis") or {}
    symbol = a.get("symbol", "")
    if not b:
        return ""
    if not b.get("data_complete"):
        missing = "، ".join(b.get("missing") or [])
        return f"\n🧠 <b>موتور تحلیل جامع {escape(symbol)}</b>\nداده چندتایم‌فریمی کامل نیست؛ تایم‌فریم‌های ناقص: {escape(missing)}. سیگنال خرید/فروش صادر نشد.\n"
    tf_names = {"1H": "۱ساعته", "4H": "۴ساعته", "1D": "روزانه"}
    tf_lines = []
    for tf in ("1H", "4H", "1D"):
        m = b.get("timeframes", {}).get(tf)
        if not m:
            continue
        ichi = m.get("ichimoku") or {}
        cloud = {"ABOVE": "بالای ابر", "BELOW": "زیر ابر", "INSIDE": "داخل ابر"}.get(ichi.get("cloud_state"), "نامشخص")
        div = {"BULLISH": "واگرایی مثبت", "BEARISH": "واگرایی منفی", "NONE": "بدون واگرایی"}.get(m.get("divergence"), "نامشخص")
        tf_lines.append(
            f"• {tf_names[tf]}: RSI {m['rsi']:.1f} | بولینگر {m.get('bollinger_position', 50):.0f}% | "
            f"حجم {m['volume_ratio']:.2f}× میانگین | OBV {'صعودی' if m['obv_rising'] else 'نزولی/خنثی'} | {div} | ایچیموکو: {cloud}"
        )
    profile = b.get("timeframes", {}).get("4H", {}).get("ichimoku", {})
    vp = (f"POC: {_format_price(symbol, profile['poc'])} | ناحیه ارزش: "
          f"{_format_price(symbol, profile['value_area_low'])} تا {_format_price(symbol, profile['value_area_high'])}"
          if profile.get("poc") else "پروفایل حجم: داده کافی برای محاسبه نیست")
    patterns = []
    for tf in ("4H", "1D"):
        patterns.extend(b.get("timeframes", {}).get(tf, {}).get("pattern", []))
    pattern_text = "، ".join(dict.fromkeys(patterns)) if patterns else "الگوی کلاسیک قابل اتکایی تشخیص داده نشد"
    sig = {"BUY": "🟢 خرید", "SELL": "🔴 فروش", "WAIT": "🟡 انتظار"}.get(b.get("candidate"), "انتظار")
    levels = ""
    if b.get("candidate") in ("BUY", "SELL"):
        levels = (
            f"\nورود مرجع: <b>{_format_price(symbol, b['entry'])}</b> | "
            f"حد ضرر: <b>{_format_price(symbol, b['stop'])}</b>\n"
            f"هدف ۱: <b>{_format_price(symbol, b['target1'])}</b> | "
            f"هدف ۲: <b>{_format_price(symbol, b['target2'])}</b> | RR: <b>1:{b['rr']:.1f}</b>\n"
        )
    bt = b.get("backtest") or {}
    if bt.get("available"):
        pf = f"{bt['profit_factor']:.2f}" if bt.get("profit_factor") is not None else "نامحدود/بدون زیان ثبت‌شده"
        bt_text = (
            f"بک‌تست ۶ماهه روزانه: {bt['trades']} معامله | برد {bt['win_rate']:.1f}% | "
            f"Profit Factor {pf} | Max Drawdown {bt['max_drawdown']:.2f}% | "
            f"بازده شبیه‌سازی {bt['total_return']:+.2f}%\n"
        )
    else:
        bt_text = f"بک‌تست ۶ماهه: قابل گزارش نیست؛ {escape(str(bt.get('reason', 'داده ناکافی')))}\n"
    return (
        f"\n🧠 <b>موتور تحلیل جامع {escape(symbol)}</b>\n"
        + "\n".join(tf_lines) + "\n"
        + f"📍 MA50 روزانه: {_format_price(symbol, b.get('daily_sma50', 0))} | "
          f"حجم روزانه: {b.get('daily_volume_ratio', 0):.2f}× میانگین ۱۴روزه | "
          f"واگرایی روزانه: {b.get('daily_divergence', 'NONE')}\n"
        + f"📊 پروفایل حجم ۴ساعته: {vp}\n"
        + f"🔺 الگوهای احتمالی: {escape(pattern_text)}\n"
        + f"🎯 خروجی شروط سخت‌گیرانه استراتژی: <b>{sig}</b>\n"
        + levels
        + bt_text
        + "📰 آن‌چین/اخبار/ترس‌وطمع: " + escape(b.get("fundamental_status", "داده معتبر در دسترس نیست")) + "\n"
        + "🛡 مدیریت ریسک: ریسک هر معامله حداکثر ۲٪ سرمایه؛ حدضرر مرجع ۰.۷۵٪، هدف اول ۲.۲۵٪ (RR حداقل ۳:۱)، استفاده از تریلینگ‌استاپ پس از حرکت قیمت به سود.\n"
        + "⚠️ بک‌تست و امتیازها تضمین عملکرد آینده نیستند؛ اجرای سفارش و اندازه موقعیت را خودتان کنترل کنید.\n"
    )


async def nobitex_orderbook_metrics(symbol):
    """Top-20 orderbook imbalance and spread; public endpoint, no API token."""
    market_symbol = f"{norm_symbol(symbol)}{NOBITEX_QUOTE}"
    data = await http_json(f"{NOBITEX_BASE_URL}/v3/orderbook/{market_symbol}", ttl=10, retries=1)
    if not data or data.get("status") != "ok":
        return None
    bids, asks = data.get("bids") or [], data.get("asks") or []
    if not bids or not asks:
        return None
    try:
        bid_value = sum(float(p) * float(q) for p, q in bids[:20])
        ask_value = sum(float(p) * float(q) for p, q in asks[:20])
        total = bid_value + ask_value
        best_bid, best_ask = float(bids[0][0]), float(asks[0][0])
        mid = (best_bid + best_ask) / 2
        if total <= 0 or mid <= 0 or best_ask < best_bid:
            return None
        return {"imbalance": (bid_value - ask_value) / total,
                "spread_pct": (best_ask - best_bid) / mid * 100,
                "bid_value": bid_value, "ask_value": ask_value}
    except (TypeError, ValueError, IndexError, ZeroDivisionError):
        return None


async def professional_crypto_analysis(symbol):
    s = norm_symbol(symbol)
    cache_key_a = ("prof", s)
    cached = ANALYSIS_CACHE.get(cache_key_a, ANALYSIS_CACHE_SECONDS)
    if cached is not None:
        return cached

    bars = {"5m": 240, "15m": 240, "1H": 240, "4H": 240, "1D": 240}

    async def one(tf, limit):
        return tf, await nobitex_candles(symbol, tf, limit)

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

    # 4H and 1D are mandatory regime filters. Substituting a lower
    # timeframe here can falsely validate a signal when higher-TF data is absent.
    if len(tfs) < 3 or "4H" not in tfs or "1D" not in tfs:
        log.warning(
            "Insufficient MTF coverage for %s: available=%s required=4H,1D",
            s, sorted(tfs)
        )
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
    trend1w = tfs.get("1W", {}).get("trend", "UNAVAILABLE")
    structure1d = tfs.get("1D", main).get("structure")

    early_buy_weight = sum(
        weights[k] for k in tfs
        if tfs[k].get("early_entry") and k in ("15m", "1H", "4H")
    )
    # Mature setups need full 4H structure confirmation. Early setups may pass
    # while 4H structure is forming, but daily/weekly bearish regimes still veto.
    # Direction safety: a short-term bounce must never be promoted to a BUY
    # while the 4H/daily/weekly regime is bearish (including weak-bear states).
    bearish_regime = ("BEARISH", "BEARISH_WEAK")
    bullish_regime = ("BULLISH", "BULLISH_WEAK")
    early_buy_veto = (
        trend4 in bearish_regime or structure4 == "BEARISH"
        or trend1d in bearish_regime or structure1d == "BEARISH"
        or trend1w in bearish_regime
        or bullish_trend_weight < 0.45
    )
    mature_buy_veto = (
        trend4 != "BULLISH" or structure4 != "BULLISH"
        or trend1d in bearish_regime or structure1d == "BEARISH"
        or trend1w in bearish_regime or bullish_trend_weight < 0.60
    )
    buy_veto = mature_buy_veto and (early_buy_weight < 0.25 or early_buy_veto)
    # Symmetric protection: do not issue SELL into a confirmed higher-timeframe
    # bullish regime. Weak-bull states count as bullish too.
    sell_veto = (
        trend4 not in bearish_regime or structure4 != "BEARISH"
        or trend1d in bullish_regime or structure1d == "BULLISH"
        or trend1w in bullish_regime or bearish_trend_weight < 0.60
    )

    entry_tfs = [tfs[k] for k in ("15m", "1H", "4H") if k in tfs]
    early_tfs = [v for v in entry_tfs if v.get("early_entry")]
    late_tfs = [v for v in entry_tfs if v.get("late_move_risk")]
    aggregate_late_risk = bool(late_tfs) or any(
        _safe_float(v.get("ema21_distance_atr"), 99) > 1.25
        or _safe_float(v.get("rsi"), 50) >= 68
        for v in entry_tfs
    )
    aggregate_early_entry = len(early_tfs) >= 2 and not aggregate_late_risk
    aggregate_extension = max(
        (_safe_float(v.get("ema21_distance_atr"), 99) for v in entry_tfs),
        default=_safe_float(main.get("ema21_distance_atr"), 99),
    )

    candidate = "WAIT"
    mature_buy = bull >= 65 and edge >= 22 and buy_agreement >= 0.55 and not mature_buy_veto
    early_buy = (
        bull >= 48 and edge >= 12
        and early_buy_weight >= 0.25
        and buy_agreement >= 0.20
        and not early_buy_veto
    )
    if (mature_buy or early_buy) and not buy_veto and not aggregate_late_risk and bull > bear and edge > 0:
        candidate = "BUY"
    elif bear >= 65 and edge <= -22 and sell_agreement >= 0.55 and not sell_veto and bear > bull:
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
    # The aggregate execution-timeframe flags above are also surfaced for the UI.
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
        "early_entry": bool(candidate == "BUY" and early_buy and not mature_buy and aggregate_early_entry),
        "entry_stage": "EARLY" if candidate == "BUY" and early_buy and not mature_buy and aggregate_early_entry else "CONFIRMED" if candidate == "BUY" and not aggregate_late_risk else "WAIT",
        "late_move_risk": aggregate_late_risk,
        "ema21_distance_atr": aggregate_extension,
        "early_timeframes": len(early_tfs),
        "execution_timeframes": len(entry_tfs),
        "early_buy_weight": early_buy_weight * 100,
        "data_source": "Nobitex OHLCV professional multi-timeframe (USDT)",
        "history_points": sum(v["history_points"] for v in tfs.values()),
    })
    try:
        hist_1h = await nobitex_candles(symbol, "1H", 240)
        out["backtest"] = walk_forward_backtest(
            hist_1h["close"] if hist_1h is not None and "close" in hist_1h else [],
            horizon=6,
        )
    except Exception as e:
        log.warning("backtest history unavailable %s: %s", symbol, e)
        out["backtest"] = {"available": False, "reason": "کندل تاریخی کافی از نوبیتکس دریافت نشد"}

    try:
        prompt_analysis = await asyncio.wait_for(_universal_crypto_prompt_analysis(s, tfs), timeout=35)
        out["prompt_analysis"] = prompt_analysis
        # The requested strategy is the final signal gate for EVERY crypto symbol.
        out["signal_candidate"] = prompt_analysis.get("candidate", "WAIT")
        if out["signal_candidate"] in ("BUY", "SELL"):
            out["price"] = prompt_analysis["entry"]
            out["stop"] = prompt_analysis["stop"]
            out["targets"] = [prompt_analysis["target1"], prompt_analysis["target2"],
                              prompt_analysis["target2"]]
            out["rr"] = prompt_analysis["rr"]
            out["reasons_buy"] = (
                ["شرایط استراتژی جامع: RSI اشباع فروش، حجم روزانه بالاتر از میانگین ۱۴روزه، "
                 "واگرایی مثبت RSI/MACD و قیمت بالای MA50 روزانه"]
                if out["signal_candidate"] == "BUY" else []
            )
            out["reasons_sell"] = (
                ["شرایط استراتژی جامع: RSI اشباع خرید، واگرایی منفی، افت حجم و نشانه جهش/هیجان قیمت"]
                if out["signal_candidate"] == "SELL" else []
            )
            out["signal_score"] = max(_safe_float(out.get("signal_score")), 75.0)
        else:
            out["stop"] = 0.0
            out["targets"] = [0.0, 0.0, 0.0]
            out["rr"] = 0.0
        out["backtest"] = prompt_analysis.get("backtest")
    except Exception as e:
        log.exception("Universal crypto strategy overlay failed %s: %s", s, e)
        out["prompt_analysis"] = {"data_complete": False, "missing": ["دریافت/محاسبه داده"]}
        out["signal_candidate"] = "WAIT"
        out["stop"] = 0.0
        out["targets"] = [0.0, 0.0, 0.0]
        out["rr"] = 0.0

    # Exchange microstructure is a secondary filter, never a standalone signal.
    ob = None
    if out.get("signal_candidate") in ("BUY", "SELL"):
        try:
            ob = await asyncio.wait_for(nobitex_orderbook_metrics(s), timeout=8)
        except Exception as exc:
            log.warning("Nobitex orderbook unavailable %s: %s", s, exc)
    out["orderbook"] = ob
    if ob:
        imbalance = _safe_float(ob.get("imbalance"))
        spread = _safe_float(ob.get("spread_pct"))
        if spread > 0.80:
            out["signal_candidate"] = "WAIT"
            out["stop"] = 0.0; out["targets"] = [0.0, 0.0, 0.0]; out["rr"] = 0.0
            out["reasons"] = list(out.get("reasons") or []) + ["اسپرد اردربوک نوبیتکس زیاد است؛ ورود به‌صرفه نیست"]
        elif out.get("signal_candidate") == "BUY" and imbalance < -0.35:
            out["signal_candidate"] = "WAIT"
            out["stop"] = 0.0; out["targets"] = [0.0, 0.0, 0.0]; out["rr"] = 0.0
            out["reasons_buy"] = []
            out["reasons"] = list(out.get("reasons") or []) + ["فشار سفارش‌های فروش در اردربوک بر خرید غالب است"]
        elif out.get("signal_candidate") == "SELL" and imbalance > 0.35:
            out["signal_candidate"] = "WAIT"
            out["stop"] = 0.0; out["targets"] = [0.0, 0.0, 0.0]; out["rr"] = 0.0
            out["reasons_sell"] = []
            out["reasons"] = list(out.get("reasons") or []) + ["فشار سفارش‌های خرید در اردربوک بر فروش غالب است"]

    ANALYSIS_CACHE.set(cache_key_a, out)
    return out


# ============================================================
# SIGNAL CONFIRMATION — v17
# ============================================================

def _timeframe_bucket(timeframe):
    tf_seconds = {"5m": 300, "15m": 900, "1H": 3600, "4H": 14400, "1D": 86400, "1W": 604800}.get(timeframe, 3600)
    bucket = int(time.time() // tf_seconds)
    return f"{timeframe}:{bucket}"

def confirm_signal(symbol, candidate, price, timeframe=None):
    timeframe = timeframe or SIGNAL_CONFIRM_TIMEFRAME
    if candidate == "WAIT":
        with db() as c:
            c.execute("DELETE FROM signal_state WHERE symbol=?", (symbol,))
        return "WAIT", False, 0

    observation_key = _timeframe_bucket(timeframe)
    iso_now = now_iso()
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
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

def confirm_market_alert(symbol, candidate, price, observation_key):
    now = now_iso()
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
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

async def crypto_market_context(symbol):
    sym = norm_symbol(symbol)
    rows = await market_universe()
    x = next((r for r in rows if r.get("symbol") == sym), None)
    if not x:
        return None
    return {"price": _safe_float(x.get("current_price")), "market_cap": 0,
            "volume_24h": _safe_float(x.get("total_volume")),
            "rank": int(x.get("market_cap_rank") or 0),
            "change_24h": _safe_float(x.get("price_change_percentage_24h_in_currency")),
            "change_7d": _safe_float(x.get("price_change_percentage_7d_in_currency")),
            "change_30d": _safe_float(x.get("price_change_percentage_30d_in_currency")),
            "circulating_supply": 0, "max_supply": 0, "source": "Nobitex"}

async def analyze(symbol, coin_id=None):
    s = norm_symbol(symbol)
    cache_key_a = ("analyze", s, coin_id or COINS.get(s) or s)
    cached = ANALYSIS_CACHE.get(cache_key_a, ANALYSIS_CACHE_SECONDS)
    if cached is not None:
        return cached

    # All crypto symbols are resolved against Nobitex markets; legacy coin IDs
    # are ignored so the same ticker cannot silently switch data providers.
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
            ANALYSIS_CACHE.set(cache_key_a, result)
            return result

    data = await asset_data(s, coin_id=coin_id)
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
    if s in ("GOLD18", "XAU"):
        result["backtest"] = walk_forward_backtest(data[1], horizon=6, fee_pct=0.05)
        result["backtest_source"] = "saved TGJU price snapshots (close-only)"
    ANALYSIS_CACHE.set(cache_key_a, result)
    return result

def signal_fa(s):
    return {"BUY":"🟢 خرید","SELL":"🔴 فروش","WAIT":"🟡 انتظار"}.get(s, s)

def _market_context_text(ctx):
    if not ctx:
        return "📊 آمار بازار تکمیلی: در دسترس نیست.\n"
    def money(v):
        v = _safe_float(v)
        if v >= 1_000_000_000:
            return f"{v/1_000_000_000:.2f} میلیارد USDT"
        if v >= 1_000_000:
            return f"{v/1_000_000:.2f} میلیون USDT"
        if v >= 1_000:
            return f"{v/1_000:.2f} هزار USDT"
        return f"{v:.2f} USDT"
    rank = f"#{ctx['rank']}" if ctx.get("rank") else "نامشخص"
    supply = ctx.get("circulating_supply", 0)
    supply_text = f"{supply:,.0f}" if supply else "نامشخص"
    max_supply = ctx.get("max_supply", 0)
    max_text = f"{max_supply:,.0f}" if max_supply else "نامحدود/نامشخص"
    return (
        "🌐 <b>نمای کلی بازار نوبیتکس</b>\n"
        f"رتبه نقدشوندگی نوبیتکس: {rank} | ارزش بازار: در API عمومی موجود نیست | حجم ۲۴ساعته: {money(ctx.get('volume_24h'))}\n"
        f"تغییر: ۲۴ساعت {ctx.get('change_24h', 0):+.2f}% | ۷روز {ctx.get('change_7d', 0):+.2f}% | ۳۰روز {ctx.get('change_30d', 0):+.2f}%\n"
        f"عرضه در گردش: {supply_text} | حداکثر عرضه: {max_text}\n"
    )

def analysis_text(a):
    if not a:
        return "❌ اطلاعات بازار در دسترس نیست."
    if a.get("insufficient_history"):
        return (f"📊 <b>داشبورد تحلیل {escape(a['symbol'])}</b>\n\n"
                f"💰 قیمت فعلی: <b>{_format_price(a['symbol'], a['price'])}</b>\n"
                f"📚 تاریخچه قابل استفاده: {a['history_points']} نقطه از {GOLD_MIN_HISTORY_POINTS} نقطه لازم\n\n"
                "⏳ برای محاسبه اندیکاتورها هنوز تاریخچه واقعی کافی جمع نشده است.")
    trend_fa = {"BULLISH":"صعودی قوی","BULLISH_WEAK":"صعودی ضعیف",
                "BEARISH":"نزولی قوی","BEARISH_WEAK":"نزولی ضعیف",
                "NEUTRAL":"خنثی"}.get(a["trend"], a["trend"])
    source = a.get("data_source") or ("TGJU (close-only)" if a["symbol"] in ("GOLD18","XAU") else "Nobitex OHLCV")
    is_close_only = a["symbol"] in ("GOLD18", "XAU") or "close-only" in source
    adx_label = "ADX≈" if is_close_only else "ADX"
    if a["symbol"] in ("GOLD18", "XAU"):
        candle_note = "⏱ داده‌های نقطه‌ای TGJU (تقریباً هر ۵ دقیقه؛ کندل OHLC واقعی نیست)\n"
    elif is_close_only:
        candle_note = "⏱ داده‌های بسته‌شده OHLCV نوبیتکس (واحد قیمت: USDT)\n"
    else:
        candle_note = "⏱ کندل‌های بسته‌شده نوبیتکس در چند تایم‌فریم (نمای اصلی 4H)\n"

    mtf = a.get("mtf") or {}
    mtf_text = ""
    if mtf:
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
        mtf_text = "🕒 <b>تأیید چندتایم‌فریمی (تاریخچه کندل‌های بسته‌شده)</b>\n" + "\n".join(tf_lines) + "\n\n"

    ema200_text = (_format_price(a["symbol"], a["ema200"])
                   if a.get("ema200_available", a.get("ema200") is not None) and a.get("ema200") is not None
                   else "داده کافی نیست")
    price = _safe_float(a.get("price"))
    support = _safe_float(a.get("support"))
    resistance = _safe_float(a.get("resistance"))
    atr = _safe_float(a.get("atr"))
    trend = a.get("trend", "NEUTRAL")
    stop = _safe_float(a.get("stop"))
    targets = list(a.get("targets") or [])
    if price > 0:
        if trend in ("BULLISH", "BULLISH_WEAK"):
            if stop <= 0 or stop >= price:
                stop = min(support, price - 1.5 * atr) if support > 0 and atr > 0 else price - max(1.5 * atr, price * 0.02)
            risk = max(price - stop, 0.0)
            target1 = resistance if resistance > price else price + 1.5 * risk
            target2 = max(target1, price + 2.5 * risk)
            target3 = max(target2, price + 3.5 * risk)
        elif trend in ("BEARISH", "BEARISH_WEAK"):
            if stop <= price:
                stop = max(resistance, price + 1.5 * atr) if resistance > price and atr > 0 else price + max(1.5 * atr, price * 0.02)
            risk = max(stop - price, 0.0)
            target1 = support if 0 < support < price else price - 1.5 * risk
            target2 = min(target1, price - 2.5 * risk)
            target3 = min(target2, price - 3.5 * risk)
        else:
            risk = max(1.5 * atr, price * 0.02)
            stop = price - risk
            target1, target2, target3 = price + 1.5 * risk, price + 2.5 * risk, price + 3.5 * risk
        if len(targets) >= 3 and _safe_float(targets[0]) > 0 and a.get("signal_candidate") in ("BUY", "SELL"):
            target1, target2, target3 = [_safe_float(x) for x in targets[:3]]
    else:
        stop = target1 = target2 = target3 = 0.0

    level_text = (
        "🎯 <b>سطوح مهم و مدیریت ریسک (تقریبی)</b>\n"
        f"🧱 حمایت: <b>{_format_price(a['symbol'], support)}</b> | مقاومت: <b>{_format_price(a['symbol'], resistance)}</b>\n"
        f"🛑 حد ضرر مرجع: <b>{_format_price(a['symbol'], stop)}</b>\n"
        f"🎯 هدف ۱: <b>{_format_price(a['symbol'], target1)}</b> | هدف ۲: <b>{_format_price(a['symbol'], target2)}</b> | هدف ۳: <b>{_format_price(a['symbol'], target3)}</b>\n"
        "ℹ️ این سطوح تخمینی‌اند؛ در روند خنثی صرفاً مرجع محاسباتی هستند و سیگنال ورود محسوب نمی‌شوند.\n\n"
        if price > 0 else ""
    )

    mtf_agreement_line = ""
    if mtf:
        mtf_agreement_line = f"🧭 هم‌جهتی تایم‌فریم‌ها: <b>{a.get('mtf_agreement', 0):.0f}%</b>\n"

    return (
        f"📊 <b>داشبورد تحلیل تکنیکال {escape(a['symbol'])}</b>\n\n"
        f"💰 قیمت: <b>{_format_price(a['symbol'], a['price'])}</b>\n"
        f"📈 ساختار روند اصلی: <b>{trend_fa}</b>\n\n"
        + mtf_text
        + f"EMA9: {_format_price(a['symbol'], a['ema9'])} | EMA21: {_format_price(a['symbol'], a['ema21'])}\n"
        + f"EMA50: {_format_price(a['symbol'], a['ema50'])} | EMA200: {ema200_text}\n"
        + f"RSI14: <b>{a['rsi']:.1f}</b> | {adx_label}: <b>{a['adx']:.1f}</b>\n"
        + f"MACD: {a['macd']:.5f} | Histogram: {a['macd_hist']:+.5f}\n"
        + f"Bollinger: <b>{a['bb_position']:.1f}%</b> | ATR: {_format_price(a['symbol'], a['atr'])} ({a['atr_pct']:.2f}%)\n"
        + level_text
        + f"مومنتوم: 1 کندل {a.get('r1', 0):+.2f}% | 6 کندل {a.get('r6', 0):+.2f}% | "
        + f"24 کندل {a.get('r24', 0):+.2f}% | 72 کندل {a.get('r72', 0):+.2f}%\n"
        + f"{candle_note}\n"
        + f"💪 <b>قدرت تکنیکال: {a['strength']:.0f}%</b>\n"
        + mtf_agreement_line
        + f"🔬 وضعیت ساختار: {'قوی' if a['strength'] >= 75 else 'متوسط' if a['strength'] >= 55 else 'ضعیف'}\n"
        + f"📚 تعداد داده: {a['history_points']} | منبع: {escape(source)}\n"
        + (f"📚 اردربوک نوبیتکس: عدم‌تعادل {a['orderbook']['imbalance']*100:+.1f}% | اسپرد {a['orderbook']['spread_pct']:.3f}%\n" if a.get("orderbook") else "📚 اردربوک نوبیتکس: داده در دسترس نیست\n")
        + (f"⚠️ {escape(a['data_quality_warning'])}\n" if a.get("data_quality_warning") else "")
        + ("📥 تاریخچه رمزارز از کندل‌های تاریخی نوبیتکس دریافت شده است؛ تایم‌فریم‌های در دسترس بسته به نماد/محدودیت منبع ممکن است متفاوت باشد.\n"
           if a.get("mtf") else "")
        + (_market_context_text(a.get("market_context")) if a["symbol"] not in ("GOLD18", "XAU") else "")
        + ("\nℹ️ آمار بازار تکمیلی است و جایگزین بررسی بنیادی پروژه، اخبار و توکنومیک نیست.\n"
           if a["symbol"] not in ("GOLD18", "XAU") else
           "\nℹ️ تحلیل طلا بر پایه نقاط قیمت TGJU است؛ داده OHLC کامل و اطلاعات بنیادی در این موتور موجود نیست.\n")
        + "\n" + backtest_text(a.get("backtest"), a["symbol"])
        + universal_crypto_prompt_report(a)
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
        f"🧠 امتیاز اطمینان محاسباتی (کالیبره‌نشده): <b>{a['probability']:.0f}%</b>\n"
        f"🔁 تأیید متوالی: <b>{a['confirmations']}/{SIGNAL_CONFIRMATIONS_REQUIRED}</b> ({SIGNAL_CONFIRM_TIMEFRAME})\n"
        f"💪 قدرت تکنیکال: <b>{a['strength']:.0f}%</b>\n"
        f"📈 روند: <b>{a['trend']}</b> | RSI: <b>{a['rsi']:.1f}</b> | ADX: <b>{a['adx']:.1f}</b>\n"
        + (f"📚 اردربوک نوبیتکس: عدم‌تعادل {a['orderbook']['imbalance']*100:+.1f}% | اسپرد {a['orderbook']['spread_pct']:.3f}%\n\n" if a.get("orderbook") else "📚 اردربوک: داده در دسترس نیست\n\n")
        + f"🧩 منطق {direction_note}:\n{reasons_text}\n"
        f"{levels}\n"
        + backtest_text(a.get("backtest"), a["symbol"])
        + universal_crypto_prompt_report(a)
        + "⚠️ این یک خروجی مدل تحلیلی است و سود قطعی یا تضمین‌شده نیست."
    )

def _short_term_scan_metrics(x, ta):
    price = _safe_float(x.get("current_price"))
    if price <= 0:
        return None
    ch1 = _safe_float(x.get("price_change_percentage_1h_in_currency"))
    ch24 = _safe_float(x.get("price_change_percentage_24h_in_currency"))
    ch7 = _safe_float(x.get("price_change_percentage_7d_in_currency"))
    ch14 = _safe_float(x.get("price_change_percentage_14d_in_currency"))
    volume = _safe_float(x.get("total_volume"))
    rank = int(x.get("market_cap_rank") or 99999)  # Nobitex quote-volume rank
    ath_change = _safe_float(x.get("ath_change_percentage"))
    turnover = min(0.30, max(0.01, 0.30 / max(1.0, rank ** 0.5)))

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
    if ch24 > 10:
        score -= 10; reasons.append("رشد ۲۴ساعته زیاد؛ احتمال ورود دیرهنگام")
    elif ch24 > 6:
        score -= 5; reasons.append("حرکت روزانه تا حدی انجام شده؛ ورود روی پولبک بهتر است")
    if ch7 > 35:
        score -= 10; reasons.append("رشد ۷روزه زیاد؛ ریسک تعقیب موج")
    elif ch7 > 20:
        score -= 5; reasons.append("بخش قابل‌توجهی از حرکت هفتگی انجام شده")
    if turnover >= 0.25:
        score += 8; reasons.append("رتبه حجم معاملات نوبیتکس در محدوده برتر است")
    elif turnover >= 0.10:
        score += 5; reasons.append("رتبه حجم معاملات نوبیتکس مناسب است")
    elif turnover < 0.02:
        score -= 8; reasons.append("رتبه حجم معاملات نوبیتکس پایین است")
    if rank <= 5: score += 5
    elif rank <= 15: score += 3
    elif rank > 25: score -= 4
    if -25 <= ath_change <= -5:
        score += 5; reasons.append("فاصله مناسب از سقف تاریخی")
    elif ath_change < -90:
        score -= 5; reasons.append("افت بسیار عمیق از سقف دوره")
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
            score += 7; reasons.append("روند صعودی تأییدشده")
        elif trend == "BULLISH_WEAK":
            score += 7; reasons.append("روند صعودی در حال شکل‌گیری")
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
        if e9 > e21 > e50: score += 3; reasons.append("چیدمان EMA صعودی")
        if ta.get("early_entry"):
            score += 8; reasons.append("ورود زودهنگام نزدیک EMA21؛ پیش از کشیدگی قیمت")
        if ta.get("late_move_risk") or _safe_float(ta.get("ema21_distance_atr"), 99) > 1.20:
            score -= 22; reasons.append("حرکت از نقطه ورود مناسب عبور کرده؛ خرید تعقیبی ممنوع")

    if 0.5 <= volatility <= 3.0:
        score += 4; reasons.append("نوسان مناسب برای معاملات کوتاه‌مدت")
    elif volatility > 6:
        score -= 6; reasons.append("نوسان بسیار بالا")

    score = _clamp(score)
    if V2_ENABLED:
        score = v2_apply_adaptive_score(score, ta)
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
    elif rank > 20:
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

async def _btc_market_status(rows):
    ta = await professional_crypto_analysis("BTC")
    if not ta:
        return {"ok": False}
    btc = next((x for x in rows
                if str(x.get("id") or "").lower() == "bitcoin"
                or str(x.get("symbol") or "").upper() == "BTC"), None)
    if not btc:
        return {"ok": False}

    ch1 = _safe_float(btc.get("price_change_percentage_1h_in_currency"))
    ch24 = _safe_float(btc.get("price_change_percentage_24h_in_currency"))
    ch7 = _safe_float(btc.get("price_change_percentage_7d_in_currency"))
    trend = ta.get("trend")
    rsi = _safe_float(ta.get("rsi"), 50)
    strength = _safe_float(ta.get("strength"), 0)
    cand = ta.get("signal_candidate")
    adx = _safe_float(ta.get("adx"), 0)

    btc_bullish = (
        cand == "BUY"
        or (strength >= 65 and trend in ("BULLISH", "BULLISH_WEAK") and adx >= 20)
    )
    macro_ok = ch1 >= -0.5 and ch24 >= -1.0 and ch7 >= -3.0
    ok = btc_bullish and macro_ok and rsi < 72 and cand != "SELL"

    return {"ok": ok, "ch1": ch1, "ch24": ch24, "ch7": ch7,
            "rsi": rsi, "trend": trend, "strength": strength,
            "candidate": cand, "adx": adx}

def _alert_candidate_ok(item, btc_metrics):
    m = item.get("m") or {}
    ta = m.get("ta") or {}

    score = _safe_float(m.get("score"))
    strength = _safe_float(m.get("strength"))
    probability = _safe_float(m.get("probability"))
    trend = ta.get("trend")
    turnover = _safe_float(m.get("turnover"))
    risk = m.get("risk")
    ch1 = _safe_float(m.get("ch1"))
    ch24 = _safe_float(m.get("ch24"))
    ch7 = _safe_float(m.get("ch7"))
    signal_candidate = str(ta.get("signal_candidate") or "WAIT")
    signal_score = _safe_float(ta.get("signal_score"))
    rr = _safe_float(ta.get("rr"))
    adx = _safe_float(ta.get("adx"), 0)

    # BTC is retained as macro context, but must not veto an otherwise valid
    # asset-specific setup. Each asset passes its own trend/structure/risk gates.
    rsi = _safe_float(ta.get("rsi"), 50)
    early_entry = bool(ta.get("early_entry"))
    if early_entry:
        if score < min(ALERT_OPPORTUNITY_MIN, 76):
            return False
        if strength < min(ALERT_STRENGTH_MIN, 58):
            return False
        if probability < min(ALERT_PROBABILITY_MIN, 58):
            return False
        if not (43 <= rsi <= 63):
            return False
    else:
        if score < ALERT_OPPORTUNITY_MIN:
            return False
        if strength < ALERT_STRENGTH_MIN:
            return False
        if probability < ALERT_PROBABILITY_MIN:
            return False
        if not (ALERT_RSI_MIN <= rsi <= ALERT_RSI_MAX):
            return False
    if turnover < ALERT_MIN_TURNOVER:
        return False

    if signal_candidate != "BUY":
        return False
    if early_entry:
        if trend not in ("BULLISH", "BULLISH_WEAK", "NEUTRAL"):
            return False
        if ta.get("structure") == "BEARISH":
            return False
        if _safe_float(ta.get("ema21_distance_atr"), 99) > 0.90:
            return False
    else:
        if trend != "BULLISH" or ta.get("structure") != "BULLISH":
            return False
    if risk == "زیاد":
        return False
    if ch24 < -1.5 or ch7 < -4.0:
        return False
    # Hard anti-chasing guard: do not issue a fresh buy after a vertical move.
    if (ch24 > 6.0 or ch7 > 18.0 or ch1 > 3.0
            or ta.get("late_move_risk")
            or _safe_float(ta.get("ema21_distance_atr"), 99) > 1.20
            or rsi >= 68):
        return False
    if turnover < max(0.02, MIN_TURNOVER_SIGNAL):
        return False
    if adx < (12 if early_entry else ADX_MIN_TREND):
        return False
    if rr < MIN_RR_RATIO:
        return False

    weighted = (
        score * 0.35
        + strength * 0.25
        + probability * 0.15
        + signal_score * 0.15
        + min(100, rr * 30) * 0.10
    )
    if weighted < (64 if early_entry else 72):
        return False
    if signal_score < (54 if early_entry else 60):
        return False
    return True

async def _market_alert_scan():
    rows = await market_universe()
    if not rows:
        return [], None, None

    bucket = int(time.time() // max(300, ALERT_INTERVAL_SECONDS))
    scan_observation_key = f"market:{bucket}"

    btc_status = await _btc_market_status(rows)
    rough = []
    for x in rows:
        price = _safe_float(x.get("current_price"))
        volume = _safe_float(x.get("total_volume"))
        if price <= 0 or volume <= 0:
            continue
        ch1 = _safe_float(x.get("price_change_percentage_1h_in_currency"))
        ch24 = _safe_float(x.get("price_change_percentage_24h_in_currency"))
        ch7 = _safe_float(x.get("price_change_percentage_7d_in_currency"))
        turnover = min(0.30, max(0.01, 0.30 / max(1.0, float(x.get("market_cap_rank") or 999) ** 0.5)))
        # Rank for early formation, not coins that have already pumped.
        # Seek the first acceleration phase: modest positive 1h momentum while
        # 24h/7d performance is still contained. Do not let a large turnover
        # score promote a coin that has already made its move.
        rough_score = 50 + _clamp(ch1 * 1.10, -4, 4)
        rough_score += _clamp(ch24 * 0.10, -3, 3) + _clamp(ch7 * 0.035, -3, 3)
        rough_score += _clamp((turnover - .05) * 45, -5, 6)
        if 0.15 <= ch1 <= 2.5: rough_score += 4
        if -2.0 <= ch24 <= 4.5: rough_score += 4
        if -5.0 <= ch7 <= 12.0: rough_score += 4
        if ch1 > 3.0: rough_score -= 10
        if ch24 > 6.0: rough_score -= 14
        if ch24 > 10.0: rough_score -= 10
        if ch7 > 18.0: rough_score -= 12
        if ch7 > 30.0: rough_score -= 10
        if ch24 < -12.0: rough_score -= 5
        if ch7 < -25.0: rough_score -= 5
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


# ============================================================
# KEYBOARDS
# ============================================================

def main_kb(uid):
    rows = [r[:] for r in MAIN_MENU]
    if not is_admin(uid):
        rows = [r for r in rows if r != ["👨‍💼 پنل مدیریت"]]
        sub = active_subscription(uid)
        has_access = sub is not None
        has_paid = bool(sub and sub["source"] != "trial" and sub["plan"] != TRIAL_PLAN)
        locked_labels = {
            "📊 تحلیل تکنیکال": not has_access,
            "🎯 فرصت‌های خرید": not has_access,
            "📡 سیگنال معاملاتی": not has_paid,
            "🔔 هشدارهای هوشمند": not has_paid,
        }
        rows = [[label + (" 🔒" if locked_labels.get(label, False) else "")
                 for label in row] for row in rows]
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
        [InlineKeyboardButton("🛡 مدیریت دسترسی و اشتراک", callback_data="adm:block")],
        [InlineKeyboardButton("🗄 وضعیت دیتابیس", callback_data="adm:db")],
    ])

def watchlist_selector_keyboard(rows, prefix):
    buttons = []
    for r in rows:
        label = r["symbol"]
        if r["asset_type"] == "gold":
            label += " — طلای جهانی"
        elif r["asset_type"] == "gold18":
            label += " — طلای ۱۸ عیار"
        callback = f"{prefix}:{r['asset_type']}:{r['symbol']}"
        buttons.append([InlineKeyboardButton(label, callback_data=callback)])
    return InlineKeyboardMarkup(buttons)

# ============================================================
# WATCHLIST
# ============================================================

def _repair_watchlist_table():
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
            symbol_expr = ("symbol" if "symbol" in old_names
                           else ("asset_key" if "asset_key" in old_names else "''"))
            type_expr = "asset_type" if "asset_type" in old_names else "'crypto'"
            has_created = "created_at" in old_names

            if has_created:
                c.execute(f"""
                    INSERT OR IGNORE INTO watchlist(user_id,symbol,asset_type,created_at,asset_key)
                    SELECT user_id,
                           upper(COALESCE(NULLIF({symbol_expr},''), '')),
                           COALESCE({type_expr}, 'crypto'),
                           COALESCE(created_at, ?),
                           upper(COALESCE(NULLIF({symbol_expr},''), ''))
                    FROM "{legacy}"
                    WHERE user_id IS NOT NULL AND COALESCE({symbol_expr}, '') <> ''
                """, (stamp,))
            else:
                c.execute(f"""
                    INSERT OR IGNORE INTO watchlist(user_id,symbol,asset_type,created_at,asset_key)
                    SELECT user_id,
                           upper(COALESCE(NULLIF({symbol_expr},''), '')),
                           COALESCE({type_expr}, 'crypto'),
                           ?,
                           upper(COALESCE(NULLIF({symbol_expr},''), ''))
                    FROM "{legacy}"
                    WHERE user_id IS NOT NULL AND COALESCE({symbol_expr}, '') <> ''
                """, (stamp,))

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

            existing = c.execute("""
                SELECT asset_key FROM watchlist
                WHERE user_id=? AND symbol=? AND asset_type=? LIMIT 1
            """, (uid, s, atype)).fetchone()
            if existing:
                if asset_key and asset_key != s and existing["asset_key"] != asset_key:
                    c.execute("""
                        UPDATE watchlist SET asset_key=?
                        WHERE user_id=? AND symbol=? AND asset_type=?
                    """, (asset_key, uid, s, atype))
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

    return saved

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
                SELECT user_id, symbol, asset_type, created_at, asset_key
                FROM watchlist
                WHERE user_id=? AND asset_type=?
                ORDER BY created_at ASC, symbol ASC
            """, (uid, atype)).fetchall()
        else:
            rows = c.execute("""
                SELECT user_id, symbol, asset_type, created_at, asset_key
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


def user_asset_key(uid, symbol, atype):
    with db() as c:
        row = c.execute("""
        SELECT asset_key FROM watchlist
        WHERE user_id=? AND symbol=? AND asset_type=? LIMIT 1
        """, (int(uid), norm_symbol(symbol), str(atype))).fetchone()
    return str(row["asset_key"] or "").strip() if row else None

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
        "🎁 تست رایگان ۵ روزه از دکمه «🎁 تست رایگان» در منوی اصلی فعال می‌شود و هر شناسه کاربری فقط یک‌بار می‌تواند از آن استفاده کند.\n\n"
        "⚙️ روش استفاده:\n"
        "1️⃣ دارایی را به واچ‌لیست اضافه کنید.\n"
        "2️⃣ از بخش تحلیل، بررسی کامل دریافت کنید.\n"
        "3️⃣ از بخش سیگنال‌ها وضعیت بازار را مشاهده کنید.\n\n"
        "⚠️ این ربات ابزار تحلیل و تصمیم‌یار بازار است و معامله خودکار انجام نمی‌دهد.",
        reply_markup=main_kb(uid)
    )

async def help_text(update, context):
    support = SUPPORT_USERNAME or "پشتیبان"
    if not support.startswith("@") and support != "پشتیبان":
        support = "@" + support
    await update.message.reply_text(
        "ℹ️ <b>راهنمای استفاده از ربات تحلیل بازار</b>\n\n"
        "۱) <b>شروع کار و افزودن دارایی</b>\n"
        "از «🪙 افزودن دارایی» استفاده کنید و نماد رمز‌ارز را وارد کنید؛ "
        "برای نمونه BTC، ZEC یا ETH. برای طلای جهانی XAU و طلای ۱۸ عیار ایران GOLD18 را وارد کنید.\n\n"
        "۲) <b>قیمت لحظه‌ای و واچ‌لیست</b>\n"
        "در «💰 قیمت لحظه‌ای» قیمت دارایی‌های ذخیره‌شده را ببینید. "
        "در «📋 واچ‌لیست» دارایی‌های خود را مشاهده یا حذف کنید.\n\n"
        "۳) <b>تحلیل تکنیکال</b>\n"
        "از «📊 تحلیل تکنیکال» دارایی را انتخاب کنید تا روند، تایم‌فریم‌ها، "
        "اندیکاتورها، حمایت و مقاومت، حد ضرر مرجع و اهداف تقریبی را ببینید.\n\n"
        "۴) <b>سیگنال معاملاتی و فرصت‌های خرید</b>\n"
        "«📡 سیگنال معاملاتی» بازار را خودکار بررسی می‌کند. "
        "اگر تأیید کافی نباشد، ربات ممکن است وضعیت «انتظار» نمایش دهد؛ برای گرفتن سیگنال عجله نکنید.\n\n"
        "۵) <b>هشدارها</b>\n"
        "از «🔔 هشدارهای هوشمند» برای مدیریت اعلان‌های فرصت‌های جدید استفاده کنید.\n\n"
        "۶) <b>خرید و وضعیت اشتراک</b>\n"
        "در «💳 خرید اشتراک» پلن را انتخاب کنید، پرداخت را انجام دهید و تصویر رسید را بفرستید. "
        "اشتراک پس از بررسی و تأیید مدیر فعال می‌شود. وضعیت را از «👤 وضعیت اشتراک» ببینید.\n\n"
        "۷) <b>ارتباط با پشتیبانی</b>\n"
        "⚠️ <b>هر زمان با خطا، اختلال، اطلاعات ناقص یا هر مشکل دیگری روبه‌رو شدید، "
        "حتماً از گزینه «📨 پشتیبانی» پیام بفرستید تا موضوع پیگیری شود.</b>\n"
        + (f"در صورت نیاز می‌توانید به پشتیبان پیام دهید: {escape(support)}\n\n" if support != "پشتیبان" else "")
        + "🔎 منابع داده ممکن است موقتاً قطع یا نوسانی باشند؛ اگر قیمت یا تحلیل نمایش داده نشد، "
        "کمی بعد دوباره تلاش کنید.\n\n"
        "⚠️ ربات معامله خودکار انجام نمی‌دهد و تحلیل‌ها تضمین‌کننده سود نیستند. "
        "مدیریت سرمایه و بررسی شخصی را فراموش نکنید.",
        parse_mode=ParseMode.HTML,
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
            sym, coin_id, name = res[0]
            if not add_watch(uid, sym, "crypto", asset_key=coin_id):
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
        InlineKeyboardButton(
            f"💰 {r['symbol']}",
            callback_data=f"price:{r['asset_type']}:{r['symbol']}"
        )
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
        parts = q.data.split(":", 3)
        _, at, s = parts[:3]
        coin_id = parts[3] if len(parts) > 3 else None
        s = norm_symbol(s)
        if not user_has_asset(q.from_user.id, s, at):
            await q.message.reply_text("❌ این دارایی در واچ‌لیست شما نیست.")
            return
        coin_id = coin_id or user_asset_key(q.from_user.id, s, at)
        item = await current_price(s, coin_id=coin_id)
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
    if not has_paid_subscription(uid):
        await update.message.reply_text(
            "🔒 بخش سیگنال معاملاتی فقط با اشتراک پولی فعال می‌شود.\n\n"
            "برای مشاهده پلن‌ها، «💳 خرید اشتراک» را انتخاب کنید.",
            reply_markup=main_kb(uid),
        )
        return

    msg = await update.message.reply_text(
        "🔎 <b>اسکن خودکار بازار برای سیگنال معاملاتی</b>\n\n"
        "واچ‌لیست شما شرط این بخش نیست؛ ربات بازار را بررسی می‌کند و "
        "چند نامزد مناسب را برای ارزیابی نشان می‌دهد.\n"
        "⏳ در حال دریافت داده و بررسی تایم‌فریم‌ها...",
        parse_mode=ParseMode.HTML,
    )
    try:
        opportunities, btc_status, observation_key = await asyncio.wait_for(
            _market_alert_scan(), timeout=90
        )
        if not opportunities:
            await msg.edit_text(
                "⏳ <b>فعلاً نامزد قابل بررسی پیدا نشد.</b>\n\n"
                "داده کافی از بازار دریافت نشد یا نامزدها از فیلتر اولیه عبور نکردند. "
                "کمی بعد دوباره تلاش کنید.",
                parse_mode=ParseMode.HTML,
            )
            return

        ranked = sorted(
            [x for x in opportunities if x.get("m") and x.get("m", {}).get("ta")],
            key=lambda x: (
                1 if _alert_candidate_ok(x, btc_status) else 0,
                _safe_float((x.get("m") or {}).get("score")),
                _safe_float((x.get("m") or {}).get("strength")),
                _safe_float((x.get("m") or {}).get("probability")),
            ),
            reverse=True,
        )[:5]

        if not ranked:
            await msg.edit_text(
                "⏳ <b>تحلیل کامل بازار نتیجه قابل اتکایی نداد.</b>\n\n"
                "داده تاریخی کافی برای چند تایم‌فریم دریافت نشد؛ بعداً دوباره تلاش کنید.",
                parse_mode=ParseMode.HTML,
            )
            return

        lines = ["📡 <b>نتیجه اسکن خودکار بازار</b>",
                 "ربات بدون نیاز به واچ‌لیست، نامزدها را از بازار پیدا کرده است.", ""]
        if not btc_status or not btc_status.get("ok"):
            lines.append("₿ وضعیت BTC ضعیف یا نامطمئن است؛ ریسک کلان بالاتر است، اما هر دارایی مستقل و با فیلترهای خودش ارزیابی می‌شود.")
        else:
            lines.append("₿ وضعیت BTC مثبت است؛ با این حال هر دارایی مستقل و با فیلترهای خودش ارزیابی می‌شود.")
        lines.append("")

        keyboard_buttons = []
        for idx, item in enumerate(ranked, 1):
            m = item.get("m") or {}
            ta = m.get("ta") or {}
            qualified = _alert_candidate_ok(item, btc_status)
            label = "نامزد خرید" if qualified else "تحت نظر؛ هنوز تأیید خرید نیست"
            trend_fa = {"BULLISH": "صعودی", "BULLISH_WEAK": "صعودی ضعیف",
                        "BEARISH": "نزولی", "BEARISH_WEAK": "نزولی ضعیف",
                        "NEUTRAL": "خنثی"}.get(ta.get("trend"), "نامشخص")
            lines.append(
                f"{idx}. <b>{escape(item.get('symbol', '?'))}</b> — {label}\n"
                f"   امتیاز فرصت: {_safe_float(m.get('score')):.0f}% | "
                f"قدرت تکنیکال: {_safe_float(m.get('strength')):.0f}%\n"
                f"   روند: {trend_fa} | RSI: {_safe_float(ta.get('rsi'), 50):.1f} | "
                f"ریسک: {escape(str(m.get('risk') or 'نامشخص'))}"
            )
            sym = norm_symbol(item.get("symbol") or "")
            if sym:
                keyboard_buttons.append([
                    InlineKeyboardButton(f"📊 {sym}", callback_data=f"op:analysis:{sym}"),
                    InlineKeyboardButton(f"📡 {sym}", callback_data=f"op:signal:{sym}"),
                ])

        lines.extend([
            "",
            "📌 برای هر دارایی می‌توانید تحلیل کامل یا جزئیات سیگنال را باز کنید.",
            "⚠️ امتیازها تضمین سود یا احتمال سودِ کالیبره‌شده نیستند؛ تا زمان تأیید، موارد «تحت نظر» سیگنال خرید محسوب نمی‌شوند.",
        ])
        await msg.edit_text(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard_buttons) if keyboard_buttons else None,
        )
    except asyncio.TimeoutError:
        await msg.edit_text(
            "⏳ اسکن بازار بیش از ۹۰ ثانیه طول کشید و متوقف شد تا ربات معطل نماند. "
            "کمی بعد دوباره تلاش کنید.",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        log.exception("market-wide trading signal scan")
        try:
            await msg.edit_text(
                "⚠️ اسکن سیگنال کامل نشد؛ خطا در لاگ Railway ثبت شد. کمی بعد دوباره تلاش کنید.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

# ============================================================
# SUBSCRIPTIONS
# ============================================================

async def buy_menu(update, context):
    buttons = [
        [InlineKeyboardButton(f"{d} روز — {a:,} تومان", callback_data=f"plan:{p}")]
        for p, (d, a) in PLANS.items()
    ]
    await update.message.reply_text(
        "💳 پلن اشتراک موردنظر را انتخاب کنید.\n\n"
        "پس از انتخاب پلن، مبلغ و شماره کارت نمایش داده می‌شود. رسید پرداخت را برای بررسی مدیر ارسال کنید.",
        reply_markup=InlineKeyboardMarkup(buttons),
    )

async def trial_menu(update, context):
    uid = update.effective_user.id
    if is_blocked(uid) and not is_admin(uid):
        await update.message.reply_text("🚫 دسترسی شما توسط مدیر محدود شده است.")
        return
    if has_used_trial(uid):
        await update.message.reply_text(
            "ℹ️ تست رایگان قبلاً برای این شناسه کاربری فعال شده و فقط یک‌بار قابل استفاده است."
        )
        return
    if active_subscription(uid):
        await update.message.reply_text(
            "ℹ️ در حال حاضر اشتراک فعالی دارید؛ تست رایگان در زمان داشتن اشتراک فعال قابل شروع نیست."
        )
        return
    await update.message.reply_text(
        "🎁 <b>تست رایگان ۵ روزه</b>\n\n"
        "این تست برای هر شناسه کاربری فقط یک‌بار فعال می‌شود و شامل تحلیل تکنیکال و بررسی فرصت‌های بازار است. سیگنال‌های معاملاتی اختصاصی اشتراک پولی هستند.\n"
        "برای ادامه، دکمه زیر را بزنید.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🚀 فعال‌سازی تست رایگان", callback_data="trial:start")
        ]]),
    )

async def _check_trial_memberships(bot, uid):
    rows = trial_channel_rows()
    missing = []
    failed = False
    for row in rows:
        try:
            member = await bot.get_chat_member(chat_id=row["chat"], user_id=uid)
            status = getattr(member, "status", "")
            if status not in {"creator", "administrator", "member"}:
                missing.append(row["label"])
        except Exception:
            log.exception("Trial channel membership check failed for %s", row["chat"])
            failed = True
            missing.append(row["label"])
    return not missing and not failed, missing, failed

async def _activate_trial_and_reply(q, uid):
    ok, detail = start_free_trial(uid)
    if ok:
        await q.message.reply_text(
            f"🎉 <b>دوره رایگان {TRIAL_DAYS} روزه شما فعال شد!</b>\n\n"
            "✅ تحلیل تکنیکال\n✅ بررسی فرصت‌های بازار\n🔒 سیگنال‌های معاملاتی اختصاصی اشتراک پولی هستند\n\n"
            f"📅 پایان دوره: <b>{format_dt(detail)}</b>\n\n"
            "این دوره فقط یک‌بار برای هر حساب قابل استفاده است.",
            parse_mode=ParseMode.HTML,
            reply_markup=main_kb(uid),
        )
    elif detail == "used":
        await q.message.reply_text(
            "ℹ️ دوره رایگان قبلاً برای این حساب استفاده شده است. برای ادامه، یکی از اشتراک‌های پولی را انتخاب کنید."
        )
    else:
        await q.message.reply_text(
            "ℹ️ در حال حاضر برای حساب شما اشتراک فعالی وجود دارد؛ دوره آزمایشی در زمان داشتن اشتراک فعال قابل شروع نیست."
        )

async def trial_callback(update, context):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    ensure_user(q.from_user)
    if is_blocked(uid):
        await q.message.reply_text("🚫 دسترسی شما توسط مدیر محدود شده است.")
        return
    if not TRIAL_ENABLED:
        await q.message.reply_text("🎁 فعال‌سازی دوره آزمایشی در حال حاضر غیرفعال است.")
        return
    if has_used_trial(uid):
        await q.message.reply_text("ℹ️ دوره رایگان قبلاً برای این حساب استفاده شده است.")
        return
    if active_subscription(uid):
        await q.message.reply_text("ℹ️ در حال حاضر برای حساب شما اشتراک فعالی وجود دارد؛ دوره آزمایشی در زمان داشتن اشتراک فعال قابل شروع نیست.")
        return
    rows = trial_channel_rows()
    if not rows:
        await q.message.reply_text(
            "⚠️ کانال‌های اجباری دوره آزمایشی هنوز در Railway Variables تنظیم نشده‌اند. لطفاً بعداً دوباره تلاش کنید."
        )
        return
    buttons = []
    for i, row in enumerate(rows, 1):
        if row["link"]:
            buttons.append([InlineKeyboardButton(f"📢 عضویت در {row['label']}", url=row["link"])])
        else:
            buttons.append([InlineKeyboardButton(f"📢 {row['label']} (لینک تنظیم نشده)", callback_data="trial:check")])
    buttons.append([InlineKeyboardButton("✅ بررسی عضویت و فعال‌سازی", callback_data="trial:check")])
    await q.message.reply_text(
        f"🎁 <b>دوره رایگان {TRIAL_DAYS} روزه</b>\n\n"
        "برای فعال‌سازی، ابتدا عضو تمام کانال‌های زیر شوید و سپس دکمه بررسی عضویت را بزنید.\n"
        "دوره فقط پس از تأیید عضویت همه کانال‌ها فعال می‌شود.",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons),
    )

async def trial_check_callback(update, context):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    ensure_user(q.from_user)
    if is_blocked(uid):
        await q.message.reply_text("🚫 دسترسی شما توسط مدیر محدود شده است.")
        return
    if not TRIAL_ENABLED:
        await q.message.reply_text("🎁 فعال‌سازی دوره آزمایشی در حال حاضر غیرفعال است.")
        return
    if has_used_trial(uid):
        await q.message.reply_text("ℹ️ دوره رایگان قبلاً برای این حساب استفاده شده است.")
        return
    if active_subscription(uid):
        await q.message.reply_text("ℹ️ حساب شما همین حالا اشتراک فعال دارد؛ دوره آزمایشی شروع نشد.")
        return
    if not TRIAL_CHANNELS:
        await q.message.reply_text("⚠️ کانال‌های دوره آزمایشی در Railway Variables تنظیم نشده‌اند.")
        return
    all_joined, missing, failed = await _check_trial_memberships(context.bot, uid)
    if not all_joined:
        if failed:
            extra = "\n⚠️ بررسی بعضی کانال‌ها ناموفق بود. مدیر باید ربات را به کانال‌ها اضافه/ادمین کند و آیدی‌ها را بررسی کند."
        else:
            extra = ""
        await q.message.reply_text(
            "❌ عضویت شما در همه کانال‌های لازم تأیید نشد.\n"
            "پس از عضویت در کانال‌های باقی‌مانده، دوباره بررسی کنید.\n"
            f"موارد تأییدنشده: {', '.join(missing)}{extra}"
        )
        return
    await _activate_trial_and_reply(q, uid)

async def status_menu(update, context):
    uid = update.effective_user.id
    s = active_subscription(uid)
    if not s:
        trial_note = "تست رایگان قبلاً برای این شناسه استفاده شده است." if has_used_trial(uid) else "برای فعال‌سازی تست رایگان ۵ روزه، از دکمه «🎁 تست رایگان» در منوی اصلی استفاده کنید."
        await update.message.reply_text(
            "👤 اشتراک فعالی ندارید.\n" + trial_note,
            reply_markup=main_kb(uid),
        )
        return
    trial = s["source"] == "trial" or s["plan"] == TRIAL_PLAN
    label = "دوره آزمایشی رایگان" if trial else f"اشتراک پولی — پلن {s['days']} روزه"
    await update.message.reply_text(
        "👤 <b>وضعیت اشتراک</b>\n\n"
        f"نوع: {label}\n"
        f"شروع: {format_dt(s['start_at'])}\n"
        f"پایان: {format_dt(s['end_at'])}\n"
        "وضعیت: 🟢 فعال",
        parse_mode=ParseMode.HTML,
        reply_markup=main_kb(uid),
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
    if not update.message:
        return
    if context.user_data.pop("_support_media_handled_id", None) == update.message.message_id:
        return
    if context.user_data.get("support_mode"):
        await support_media(update, context)
        return
    user = update.effective_user
    if not user or not update.message:
        return
    ensure_user(user)
    uid = user.id
    if is_blocked(uid) and not is_admin(uid):
        await update.message.reply_text("🚫 دسترسی شما توسط مدیر محدود شده است.")
        return
    plan = context.user_data.get("payment_plan")
    if plan not in PLANS:
        await update.message.reply_text("ابتدا از «💳 خرید اشتراک» یک پلن انتخاب کنید.")
        return
    if not update.message.photo:
        await update.message.reply_text("❌ لطفاً تصویر رسید را ارسال کنید.")
        return
    days, amount = PLANS[plan]
    fid = update.message.photo[-1].file_id
    with db() as c:
        c.execute("BEGIN IMMEDIATE")
        pending = c.execute(
            "SELECT id FROM payment_requests WHERE user_id=? AND status='pending' ORDER BY id DESC LIMIT 1",
            (uid,),
        ).fetchone()
        if pending:
            await update.message.reply_text(
                f"⏳ درخواست پرداخت شماره {pending['id']} شما هنوز در انتظار بررسی است. "
                "لطفاً تا تعیین نتیجه، رسید جدید ارسال نکنید."
            )
            return
        cur = c.execute("""
        INSERT INTO payment_requests(
            user_id,plan,days,amount,receipt_file_id,status,created_at
        ) VALUES(?,?,?,?,?,?,?)
        """, (uid, plan, days, amount, fid, "pending", now_iso()))
        pid = cur.lastrowid
    context.user_data.pop("payment_plan", None)
    await update.message.reply_text("✅ رسید دریافت شد؛ پس از بررسی مدیر اشتراک فعال می‌شود.")
    if not ADMIN_IDS:
        await update.message.reply_text("⚠️ مدیر برای دریافت اعلان پرداخت تنظیم نشده است؛ لطفاً با پشتیبانی تماس بگیرید.")
    for aid in ADMIN_IDS:
        try:
            await context.bot.send_photo(
                aid, fid,
                caption=(f"💳 رسید جدید\nکاربر: {uid}\n"
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
    if not update.message or not context.user_data.get("support_mode"):
        return
    uid = update.effective_user.id
    msg = update.message
    context.user_data["_support_media_handled_id"] = msg.message_id
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("↩️ پاسخ", callback_data=f"sup:reply:{uid}")
    ]])
    try:
        if msg.photo:
            saved = "[عکس]"
            sender = lambda aid: context.bot.send_photo(
                chat_id=aid, photo=msg.photo[-1].file_id,
                caption=f"📨 پیام پشتیبانی\nکاربر: {uid}", reply_markup=markup)
        elif msg.document:
            saved = "[فایل]"
            sender = lambda aid: context.bot.send_document(
                chat_id=aid, document=msg.document.file_id,
                caption=f"📨 فایل پشتیبانی از کاربر {uid}", reply_markup=markup)
        elif msg.voice:
            saved = "[صدا]"
            sender = lambda aid: context.bot.send_voice(
                chat_id=aid, voice=msg.voice.file_id,
                caption=f"📨 صدای پشتیبانی از کاربر {uid}", reply_markup=markup)
        else:
            return
        delivered = 0
        for aid in ADMIN_IDS:
            try:
                await sender(aid)
                delivered += 1
            except Exception:
                log.exception("support media delivery failed for admin_id=%s", aid)
        if not delivered:
            await msg.reply_text("❌ فایل به پشتیبان ارسال نشد. لطفاً دوباره تلاش کنید.")
            return
        await save_support(uid, saved, msg.message_id)
        context.user_data["support_mode"] = False
        await msg.reply_text("✅ پیام شما به پشتیبان ارسال شد.")
    except Exception:
        log.exception("support media handler failed")
        await msg.reply_text("❌ ارسال پیام به پشتیبان انجام نشد. لطفاً دوباره تلاش کنید.")

async def support_reply_callback(update, context):
    q = update.callback_query
    if not is_admin(q.from_user.id):
        await q.answer("دسترسی مدیر ندارید.", show_alert=True)
        return
    try:
        uid = int(q.data.rsplit(":", 1)[-1])
        if uid <= 0:
            raise ValueError("invalid user id")
    except (TypeError, ValueError):
        await q.answer("آیدی کاربر نامعتبر است.", show_alert=True)
        return
    await q.answer("حالت پاسخ فعال شد.")
    context.user_data["admin_reply_to"] = uid
    await q.message.reply_text(
        f"✍️ حالت پاسخ فعال شد.\n"
        f"👤 کاربر مقصد: <code>{uid}</code>\n\n"
        "اکنون متن پاسخ را در همین گفتگوی خصوصی با ربات ارسال کنید.",
        parse_mode=ParseMode.HTML,
    )

async def send_support_reply(update, context, text):
    uid = context.user_data.get("admin_reply_to")
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
        context.user_data.pop("admin_reply_to", None)
        await update.message.reply_text("✅ پاسخ به کاربر ارسال شد.")
    except Exception as e:
        log.exception("support reply delivery failed for user_id=%s", uid)
        await update.message.reply_text(
            f"❌ پاسخ ارسال نشد. حالت پاسخ فعال باقی ماند؛ پس از رفع مشکل دوباره تلاش کنید.\n"
            f"جزئیات: <code>{escape(str(e))}</code>",
            parse_mode=ParseMode.HTML,
        )

# ============================================================
# ALERTS MENU
# ============================================================

async def alerts_menu(update, context):
    uid = update.effective_user.id
    if is_blocked(uid) and not is_admin(uid):
        await update.message.reply_text("🚫 دسترسی شما توسط مدیر محدود شده است.")
        return
    if not has_paid_subscription(uid):
        await update.message.reply_text("🔒 هشدارهای سیگنال فقط برای مشترکین پولی فعال است.")
        return
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
    if is_blocked(uid) and not is_admin(uid):
        await q.message.reply_text("🚫 دسترسی شما توسط مدیر محدود شده است.")
        return
    if not has_paid_subscription(uid):
        await q.message.reply_text("🔒 هشدارهای سیگنال فقط برای مشترکین پولی فعال است.")
        return
    with db() as c:
        r = c.execute("SELECT enabled FROM alert_preferences WHERE user_id=?", (uid,)).fetchone()
        new = 0 if r and r["enabled"] else 1
        c.execute("""
        INSERT INTO alert_preferences(user_id,enabled,interval_seconds) VALUES(?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET enabled=excluded.enabled
        """, (uid, new, ALERT_INTERVAL_SECONDS))
    await q.message.edit_text("🔔 هشدار سیگنال: " + ("فعال" if new else "خاموش"))


    # ============================================================
# GROWTH SCAN (short-term)
# ============================================================

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
        if price <= 0 or volume <= 0:
            continue
        ch24 = _safe_float(x.get("price_change_percentage_24h_in_currency"))
        ch7 = _safe_float(x.get("price_change_percentage_7d_in_currency"))
        turnover = min(0.30, max(0.01, 0.30 / max(1.0, float(x.get("market_cap_rank") or 999) ** 0.5)))
        # Prefer quiet accumulation / first momentum, penalize late vertical moves.
        ch1 = _safe_float(x.get("price_change_percentage_1h_in_currency"))
        rough_score = 50 + _clamp(ch1 * 1.10, -4, 4) + _clamp(ch24 * 0.10, -3, 3) + _clamp(ch7 * 0.035, -3, 3)
        rough_score += _clamp((turnover - .05) * 45, -5, 6)
        if 0.15 <= ch1 <= 2.5: rough_score += 4
        if -2.0 <= ch24 <= 4.5: rough_score += 4
        if -5.0 <= ch7 <= 12.0: rough_score += 4
        if ch1 > 3.0: rough_score -= 10
        if ch24 > 6.0: rough_score -= 14
        if ch24 > 10.0: rough_score -= 10
        if ch7 > 18.0: rough_score -= 12
        if ch7 > 30.0: rough_score -= 10
        if ch24 < -12.0: rough_score -= 5
        if ch7 < -25.0: rough_score -= 5
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
        if (ta.get("late_move_risk")
                or _safe_float(ta.get("ema21_distance_atr"), 99) > 1.20
                or _safe_float(ta.get("rsi"), 50) >= 68
                or _safe_float(x.get("price_change_percentage_24h_in_currency")) > 6
                or _safe_float(x.get("price_change_percentage_7d_in_currency")) > 18):
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
        f"🌐 تا ۲۵ بازار فعال نوبیتکس بر اساس حجم معاملات غربال می‌شود.\n"
        "📊 تحلیل چندتایم‌فریمی و نقدشوندگی بررسی می‌شود.\n"
        "⏳ ممکن است اسکن اولیه کمی زمان ببرد...",
        parse_mode=ParseMode.HTML,
    )
    try:
        items = await asyncio.wait_for(short_term_growth_scan(), timeout=90)
        if not items:
            await status.edit_text(
                "❌ این بار نامزد قابل ارزیابی پیدا نشد. ممکن است API نوبیتکس یا کندل‌های این بازار در دسترس نباشد.\n"
                "۳۰ تا ۶۰ ثانیه بعد دوباره تلاش کنید.",
            )
            return
        await status.edit_text(
            "🚀 <b>نامزدهای رشد کوتاه‌مدت</b>\n\n"
            "📌 این‌ها نامزدهای غربالگری هستند، نه توصیه خرید قطعی.\n"
            "💪 قدرت تکنیکال و امتیاز مدل دو معیار جدا هستند؛ امتیاز مدل احتمال سود آماریِ کالیبره‌شده نیست.",
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
                f"💪 قدرت تکنیکال: <b>{m['strength']:.0f}%</b> | 🧠 امتیاز مدل (کالیبره‌نشده): <b>{m['probability']:.0f}%</b>\n"
                f"📈 روند: {trend} | RSI: {rsi:.1f}\n"
                f"⚡ 1h: {m['ch1']:+.2f}% | 24h: {m['ch24']:+.2f}% | 7d: {m['ch7']:+.2f}%\n"
                f"💧 شاخص نقدشوندگی تخمینی: {m['turnover']*100:.1f}% | رتبه حجم نوبیتکس: #{m['rank']} | ریسک: <b>{escape(m['risk'])}</b>\n"
                f"💵 قیمت: {_format_price(item['symbol'], m['price'])}\n"
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
        f"🌐 تا ۲۵ بازار فعال نوبیتکس بر اساس حجم معاملات بررسی می‌شود.\n"
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
                f"🧠 امتیاز مدل (کالیبره‌نشده): <b>{x['probability']:.0f}%</b> | 24h: {x['ch24']:+.2f}% | 7d: {x['ch7']:+.2f}%{rsi}\n"
                f"💰 قیمت: {_format_price(x['symbol'], x['price'])} | رتبه نقدشوندگی: #{x['rank']}\n"
            )
        lines.append("⚠️ این رتبه‌بندی مدل تحلیلی است؛ تضمین سود نیست.")
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
    except Exception:
        log.exception("growth scan")
        await update.message.reply_text("⚠️ اسکن بازار کامل نشد.")

# ============================================================
# BUY OPPORTUNITY MENU
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
        f"🟢 <b>{'ورود زودهنگام' if ta.get('entry_stage') == 'EARLY' or ta.get('early_entry') else 'فرصت خرید تأییدشده'}</b> | 🔁 {confirmations}/{SIGNAL_CONFIRMATIONS_REQUIRED}\n"
        f"💎 فرصت: <b>{_safe_float(m.get('score')):.0f}%</b> | 💪 تکنیکال: <b>{_safe_float(m.get('strength')):.0f}%</b> | 🧠 مدل: <b>{_safe_float(m.get('probability')):.0f}%</b>\n"
        f"📈 روند: <b>{trend_fa}</b> | RSI: <b>{_safe_float(ta.get('rsi')):.1f}</b> | ADX: <b>{_safe_float(ta.get('adx')):.1f}</b>\n"
        f"🎯 فاصله از EMA21: <b>{_safe_float(ta.get('ema21_distance_atr')):.2f} ATR</b> | تایم‌فریم‌های ورود زودهنگام: <b>{int(ta.get('early_timeframes', 0))}/{int(ta.get('execution_timeframes', 0))}</b>\n"
        f"📊 مومنتوم: 1H {_safe_float(m.get('ch1')):+.2f}% | 24H {_safe_float(m.get('ch24')):+.2f}% | 7D {_safe_float(m.get('ch7')):+.2f}%\n"
        f"💰 قیمت: <b>{_format_price(symbol, _safe_float(m.get('price')))}</b>\n"
        f"🛑 SL: <b>{_format_price(symbol, _safe_float(ta.get('stop')))}</b> | 🎯 TP1: <b>{_format_price(symbol, _safe_float((ta.get('targets') or [0])[0]))}</b>\n"
        f"🎯 TP2: <b>{_format_price(symbol, _safe_float((ta.get('targets') or [0,0])[1]))}</b> | TP3: <b>{_format_price(symbol, _safe_float((ta.get('targets') or [0,0,0])[2]))}</b>\n"
        f"⚖️ R/R: <b>1:{_safe_float(ta.get('rr')):.2f}</b> | ⚠️ ریسک: <b>{escape(str(m.get('risk') or 'نامشخص'))}</b>"
    )

async def _cycle_bottom_and_growth_scan(rows=None):
    rows = rows if rows is not None else await market_universe()
    if not rows:
        return {"bottom": [], "growth": []}

    liquid = []
    for x in rows:
        price = _safe_float(x.get("current_price"))
        volume = _safe_float(x.get("total_volume"))
        if price <= 0 or volume <= 0:
            continue
        symbol = norm_symbol(x.get("symbol") or "")
        if not symbol or symbol in {"USDT", "USDC", "DAI", "FDUSD", "TUSD", "USDE", "USDS"}:
            continue
        ath_drop = _safe_float(x.get("ath_change_percentage"))
        atl_change = _safe_float(x.get("atl_change_percentage"), 999)
        ch24 = _safe_float(x.get("price_change_percentage_24h_in_currency"))
        ch7 = _safe_float(x.get("price_change_percentage_7d_in_currency"))
        ch14 = _safe_float(x.get("price_change_percentage_14d_in_currency"))
        ch200 = _safe_float(x.get("price_change_percentage_200d_in_currency"))
        ch1y = _safe_float(x.get("price_change_percentage_1y_in_currency"))
        bottom_score = (min(45, max(0, abs(min(0, ath_drop)) - 30))
                        + min(18, max(-18, ch7 * 0.45))
                        + min(12, max(-12, ch14 * 0.25))
                        + (5 if ch24 > 0 else -3 if ch24 < -8 else 0)
                        + (4 if ch200 < 0 else 0) + (3 if ch1y < 0 else 0))
        growth_score = (min(18, max(-18, ch24 * 0.45))
                        + min(22, max(-12, ch7 * 0.45))
                        + min(14, max(-10, ch14 * 0.25))
                        + min(8, max(-6, ch200 * 0.04))
                        + (5 if ch24 > 0 and ch7 > 0 else 0)
                        - (10 if ch24 > 20 else 0) - (8 if ch7 > 55 else 0))
        xcopy = dict(x)
        xcopy["_bottom_score"] = bottom_score
        xcopy["_growth_score"] = growth_score
        xcopy["_ath_drop"] = ath_drop
        xcopy["_atl_change"] = atl_change
        xcopy["_ch200"] = ch200
        xcopy["_ch1y"] = ch1y
        liquid.append(xcopy)

    bottom_pool = [x for x in liquid if x["_ath_drop"] <= -45 and x["_ch200"] < 0]
    bottom_pool.sort(key=lambda x: x["_bottom_score"], reverse=True)
    growth_pool = [x for x in liquid
                   if 0 < _safe_float(x.get("price_change_percentage_7d_in_currency")) <= 18
                   and _safe_float(x.get("price_change_percentage_24h_in_currency")) <= 6]
    growth_pool.sort(key=lambda x: x["_growth_score"], reverse=True)
    chosen = []
    seen = set()
    for lane, pool in (("bottom", bottom_pool[:4]), ("growth", growth_pool[:4])):
        for x in pool:
            key = str(x.get("id") or x.get("symbol") or "")
            if key not in seen:
                seen.add(key)
                chosen.append((lane, x))

    async def analyze(lane, x):
        try:
            ta = await professional_crypto_analysis(norm_symbol(x.get("symbol") or ""))
            return lane, x, ta
        except Exception as exc:
            log.warning("cycle/growth candidate analysis failed %s: %s", x.get("symbol"), exc)
            return lane, x, None

    analyzed = await asyncio.gather(*(analyze(lane, x) for lane, x in chosen))
    out = {"bottom": [], "growth": []}
    for lane, x, ta in analyzed:
        if not ta:
            continue
        trend = str(ta.get("trend") or "NEUTRAL")
        rsi = _safe_float(ta.get("rsi"), 50)
        signal = str(ta.get("signal_candidate") or "WAIT")
        # Opportunity lists must be selected by the same universal prompt engine;
        # a trend/price-rank candidate without a prompt BUY is not advertised as a buy.
        if signal != "BUY":
            continue
        late_entry = (bool(ta.get("late_move_risk"))
                      or _safe_float(ta.get("ema21_distance_atr"), 99) > 1.20
                      or rsi >= 68)
        if late_entry:
            continue
        if lane == "bottom":
            if trend not in ("BULLISH", "BULLISH_WEAK") or not (32 <= rsi <= 64):
                continue
        else:
            if trend not in ("BULLISH", "BULLISH_WEAK") or not (42 <= rsi <= 64):
                continue
        out[lane].append({"symbol": norm_symbol(x.get("symbol") or ""),
                          "name": x.get("name") or norm_symbol(x.get("symbol") or ""),
                          "price": _safe_float(x.get("current_price")), "ta": ta,
                          "ath_drop": _safe_float(x.get("_ath_drop")),
                          "atl_change": _safe_float(x.get("_atl_change"), 999),
                          "ch24": _safe_float(x.get("price_change_percentage_24h_in_currency")),
                          "ch7": _safe_float(x.get("price_change_percentage_7d_in_currency")),
                          "score": _safe_float(x.get("_bottom_score" if lane == "bottom" else "_growth_score")),
                          "signal": signal})
    for lane in out:
        out[lane].sort(key=lambda z: (z["signal"] == "BUY", z["score"], _safe_float(z["ta"].get("strength"))), reverse=True)
        out[lane] = out[lane][:3]
    return out

def _cycle_growth_report(data):
    lines = ["🧭 <b>فرصت‌های کف اصلاحی و رشد احتمالی</b>",
             "این فهرست نامزد پایش است، نه ادعای تشخیص کف قطعی یا تضمین رشد.", ""]
    labels = (("bottom", "🪙 <b>اصلاح عمیق با نشانه‌های تثبیت</b>", "افت از سقف تاریخی"),
              ("growth", "🚀 <b>رمزارزهای دارای مومنتوم رشد</b>", "تغییر ۷روزه"))
    for key, title, metric in labels:
        lines.append(title)
        items = data.get(key) or []
        if not items:
            lines.append("• موردی با داده کافی و تأیید تکنیکال مناسب پیدا نشد.")
            lines.append("")
            continue
        for item in items:
            ta = item["ta"]
            signal = "🟢 خرید تکنیکال" if item["signal"] == "BUY" else "🟡 فقط پایش"
            if key == "bottom":
                measure = f"فاصله از سقف دوره: {item['ath_drop']:.1f}%"
            else:
                measure = f"۷روزه: {item['ch7']:+.1f}%"
            lines.append(f"• <b>{escape(item['symbol'])}</b> — {signal} | {measure} | RSI {_safe_float(ta.get('rsi'), 50):.1f} | قدرت {_safe_float(ta.get('strength')):.0f}%")
        lines.append("")
    lines.append("⚠️ «اصلاح عمیق» به معنی ارزان‌بودن قطعی نیست؛ رتبه‌بندی بازار و OHLC بر اساس داده نوبیتکس است. رتبه نمایش‌داده‌شده رتبه حجم معاملات در نوبیتکس است، نه رتبه جهانی؛ سقف محاسبه‌شده نیز فقط سقف دوره کندل‌های دریافت‌شده است.")
    return "\n".join(lines)

async def buy_opportunities_menu(update, context):
    uid = update.effective_user.id
    if not has_analysis_access(uid):
        await update.message.reply_text("🔒 فرصت‌های خرید حرفه‌ای فقط برای مشترکین فعال است.")
        return
    msg = await update.message.reply_text(
        "🔎 <b>موتور حرفه‌ای فرصت خرید</b>\n\n"
        f"🌐 تا ۲۵ بازار فعال نوبیتکس بر اساس حجم معاملات غربال می‌شود.\n"
        "📊 EMA / RSI / MACD / ADX / ATR / مومنتوم / نقدشوندگی / ریسک / BTC بررسی می‌شود.\n"
        "⏳ در حال تحلیل بازار...",
        parse_mode=ParseMode.HTML,
    )
    try:
        opportunities, btc_status, observation_key = await asyncio.wait_for(_market_alert_scan(), timeout=90)
        cycle_data = await asyncio.wait_for(_cycle_bottom_and_growth_scan(), timeout=90)
        cycle_report = _cycle_growth_report(cycle_data)
        if not opportunities:
            await msg.edit_text(
                "⏳ <b>فرصت خرید تأییدشده‌ای پیدا نشد.</b>\n\n"
                "داده کافی برای صدور سیگنال قطعی دریافت نشد. فهرست نامزدهای اصلاح عمیق و رشد را هم بررسی کنید.\n\n"
                + cycle_report,
                parse_mode=ParseMode.HTML,
            )
            return
        await update.message.reply_text(cycle_report, parse_mode=ParseMode.HTML)

        qualified_items, pending_items = [], []
        for item in opportunities:
            if not _alert_candidate_ok(item, btc_status):
                continue
            m = item["m"]
            confirmed, confirmations, cycle_id = confirm_market_alert(
                item["symbol"], "BUY", m["price"], observation_key
            )
            m.update({"confirmed": confirmed, "confirmations": confirmations,
                      "cycle_id": cycle_id, "signal": "BUY" if confirmed else "WAIT"})
            if confirmed:
                qualified_items.append(item)
            else:
                pending_items.append(item)

        qualified_items.sort(
            key=lambda z: (_safe_float(z["m"].get("score")),
                           _safe_float(z["m"].get("strength")),
                           _safe_float(z["m"].get("probability"))), reverse=True)

        if not qualified_items:
            nearest = sorted([z for z in opportunities if z.get("m")],
                             key=lambda z: _safe_float((z.get("m") or {}).get("score")), reverse=True)[:3]
            body = ["🟡 <b>فعلاً سیگنال خرید تأییدشده نداریم.</b>",
                    f"نامزدهای منطبق با فیلتر اصلی: <b>{len(pending_items)}</b>",
                    f"برای تأیید نهایی، {SIGNAL_CONFIRMATIONS_REQUIRED} مشاهده در چرخه‌های جداگانه لازم است.", ""]
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
# ALERT WORKER
# ============================================================

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
                      AND COALESCE(s.source,'')<>'trial'
                      AND COALESCE(s.plan,'')<>?
                """, (now_iso(), TRIAL_PLAN)).fetchall()

            # نسخه ۲: یک سیگنال جهانی برای هر نماد/چرخه ثبت می‌شود؛ نه یک رکورد برای هر کاربر.
            try:
                v2_update_paper_positions(await market_universe())
                for _sym, _item in qualified.items():
                    _mm = _item.get("m") or {}
                    if _mm.get("signal") == "BUY" and _mm.get("confirmed"):
                        v2_register_signal(_item, _mm.get("cycle_id") or observation_key)
            except Exception:
                log.exception("v2 signal/paper registration")

            for ur in users:
                uid = ur["user_id"]
                utc_day_start = datetime.now(timezone.utc).date().isoformat()
                with db() as c:
                    daily_sent = int(c.execute(
                        "SELECT COUNT(*) FROM alert_events WHERE user_id=? AND created_at>=?",
                        (uid, utc_day_start),
                    ).fetchone()[0] or 0)
                if daily_sent >= MAX_DAILY_SIGNALS_PER_USER:
                    continue
                for symbol, item in qualified.items():
                    if daily_sent >= MAX_DAILY_SIGNALS_PER_USER:
                        break
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
                        c.execute("BEGIN IMMEDIATE")
                        prev = c.execute(
                            "SELECT signal_key FROM alert_events WHERE user_id=? AND symbol=? ORDER BY id DESC LIMIT 1",
                            (uid, symbol)
                        ).fetchone()
                        daily_sent = int(c.execute(
                            "SELECT COUNT(*) FROM alert_events WHERE user_id=? AND created_at>=?",
                            (uid, utc_day_start),
                        ).fetchone()[0] or 0)
                        if (prev and prev["signal_key"] == key) or daily_sent >= MAX_DAILY_SIGNALS_PER_USER:
                            continue
                        cur = c.execute(
                            "INSERT INTO alert_events(user_id,symbol,signal_key,message,created_at) VALUES(?,?,?,?,?)",
                            (uid, symbol, key, signal_text(a), now_iso())
                        )
                        event_id = cur.lastrowid
                    try:
                        if V2_ENABLED:
                            await app.bot.send_message(
                                uid, v2_signal_markdown(a),
                                parse_mode=ParseMode.MARKDOWN,
                                disable_web_page_preview=True,
                            )
                        else:
                            await app.bot.send_message(
                                uid,
                                "🔔 <b>فرصت خرید تأییدشده در کل بازار</b>\\n\\n" + signal_text(a),
                                parse_mode=ParseMode.HTML
                            )
                        daily_sent += 1
                    except Exception:
                        log.exception("alert send failed user=%s symbol=%s", uid, symbol)
                        with db() as c:
                            c.execute("DELETE FROM alert_events WHERE id=?", (event_id,))

                with db() as c:
                    c.execute("UPDATE alert_preferences SET last_check_at=? WHERE user_id=?",
                              (now_iso(), uid))
        except Exception:
            log.exception("alert worker")
        await asyncio.sleep(ALERT_INTERVAL_SECONDS)

async def backup_worker():
    while True:
        try:
            await asyncio.to_thread(backup_database, "scheduled")
        except Exception:
            log.exception("scheduled database backup")
        await asyncio.sleep(BACKUP_INTERVAL_SECONDS)

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
        await q.message.reply_text(
            "🛡 <b>مدیریت دسترسی و اشتراک کاربران</b>\n\nیک عملیات را انتخاب کنید:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🚫 مسدودسازی کاربر", callback_data="adm:access:block")],
                [InlineKeyboardButton("✅ رفع مسدودی کاربر", callback_data="adm:access:unblock")],
                [InlineKeyboardButton("💳 فعال‌سازی دستی اشتراک", callback_data="adm:access:sub_on")],
                [InlineKeyboardButton("⛔ غیرفعال‌سازی اشتراک", callback_data="adm:access:sub_off")],
            ])
        )
    elif action == "access":
        operation = p[2] if len(p) > 2 else ""
        modes = {
            "block": ("block", "شناسه عددی کاربر برای مسدودسازی را ارسال کنید."),
            "unblock": ("unblock", "شناسه عددی کاربر برای رفع مسدودی را ارسال کنید."),
            "sub_on": ("sub_activate_uid", "شناسه عددی کاربر را برای فعال‌سازی دستی اشتراک ارسال کنید."),
            "sub_off": ("sub_deactivate", "شناسه عددی کاربر را برای غیرفعال‌سازی اشتراک ارسال کنید."),
        }
        if operation in modes:
            context.user_data["admin_mode"] = modes[operation][0]
            await q.message.reply_text(modes[operation][1])
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
    if is_blocked(q.from_user.id) and not is_admin(q.from_user.id):
        await q.message.reply_text("🚫 دسترسی شما توسط مدیر محدود شده است.")
        return
    try:
        _, cid, sym = q.data.split(":", 2)
        sym = norm_symbol(sym)
        if not cid or len(cid) > 64:
            await q.message.reply_text("❌ شناسه دارایی نامعتبر است؛ دوباره جست‌وجو کنید.")
            return
        if not add_watch(q.from_user.id, sym, "crypto", asset_key=cid):
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
    if is_blocked(q.from_user.id) and not is_admin(q.from_user.id):
        await q.message.reply_text("🚫 دسترسی شما توسط مدیر محدود شده است.")
        return
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
    if is_blocked(q.from_user.id) and not is_admin(q.from_user.id):
        await q.message.reply_text("🚫 دسترسی شما توسط مدیر محدود شده است.")
        return
    try:
        parts = q.data.split(":", 3)
        _, atype, sym = parts[:3]
        coin_id = parts[3] if len(parts) > 3 else None
        if not has_analysis_access(q.from_user.id):
            await q.message.reply_text("🔒 تحلیل فقط برای مشترکین فعال است.")
            return
        if not user_has_asset(q.from_user.id, sym, atype):
            await q.message.reply_text("❌ این دارایی در واچ‌لیست شما نیست.")
            return
        coin_id = coin_id or user_asset_key(q.from_user.id, sym, atype)
        a = await asyncio.wait_for(analyze(norm_symbol(sym), coin_id=coin_id), timeout=45)
        await q.message.reply_text(analysis_text(a), parse_mode=ParseMode.HTML)
    except asyncio.TimeoutError:
        await q.message.reply_text("⏳ دریافت داده بیش از حد طول کشید. ۳۰ تا ۶۰ ثانیه دیگر دوباره تلاش کنید.")
    except Exception:
        log.exception("analysis callback")
        await q.message.reply_text("⚠️ داشبورد تحلیل ساخته نشد؛ خطا ثبت شد، دوباره تلاش کنید.")

async def selected_signal_callback(update, context):
    q = update.callback_query
    await q.answer("در حال محاسبه سیگنال...")
    if is_blocked(q.from_user.id) and not is_admin(q.from_user.id):
        await q.message.reply_text("🚫 دسترسی شما توسط مدیر محدود شده است.")
        return
    try:
        parts = q.data.split(":", 3)
        _, atype, sym = parts[:3]
        coin_id = parts[3] if len(parts) > 3 else None
        if not has_paid_subscription(q.from_user.id):
            await q.message.reply_text("🔒 سیگنال معاملاتی فقط برای مشترکین پولی فعال است.")
            return
        if not user_has_asset(q.from_user.id, sym, atype):
            await q.message.reply_text("❌ این دارایی در واچ‌لیست شما نیست.")
            return
        coin_id = coin_id or user_asset_key(q.from_user.id, sym, atype)
        a = await asyncio.wait_for(analyze(norm_symbol(sym), coin_id=coin_id), timeout=45)
        await q.message.reply_text(signal_text(a), parse_mode=ParseMode.HTML)
    except asyncio.TimeoutError:
        await q.message.reply_text("⏳ دریافت داده بیش از حد طول کشید. ۳۰ تا ۶۰ ثانیه دیگر دوباره تلاش کنید.")
    except Exception:
        log.exception("signal callback")
        await q.message.reply_text("⚠️ محاسبه سیگنال انجام نشد؛ خطا ثبت شد، دوباره تلاش کنید.")

async def opportunity_action_callback(update, context):
    q = update.callback_query
    await q.answer()
    if is_blocked(q.from_user.id) and not is_admin(q.from_user.id):
        await q.message.reply_text("🚫 دسترسی شما توسط مدیر محدود شده است.")
        return
    try:
        _, action, sym = q.data.split(":", 2)
        if action not in {"analysis", "signal"}:
            await q.message.reply_text("❌ عملیات نامعتبر است.")
            return
        if action == "signal":
            if not has_paid_subscription(q.from_user.id):
                await q.message.reply_text("🔒 مشاهده سیگنال معاملاتی فقط برای مشترکین پولی فعال است.")
                return
        elif not has_analysis_access(q.from_user.id):
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
        if action not in {"approve", "reject"}:
            return
        pid = int(pid_s)
        with db() as c:
            c.execute("BEGIN IMMEDIATE")
            r = c.execute("SELECT * FROM payment_requests WHERE id=?", (pid,)).fetchone()
            if not r:
                await q.message.reply_text("❌ درخواست پرداخت پیدا نشد.")
                return
            if r["status"] != "pending":
                await q.message.reply_text("ℹ️ این درخواست قبلاً بررسی شده است.")
                return

            uid = int(r["user_id"])
            if action == "approve":
                days = int(r["days"])
                start = datetime.now(timezone.utc)
                existing = c.execute("""
                    SELECT * FROM subscriptions
                    WHERE user_id=? AND status='active' AND end_at>?
                    ORDER BY end_at DESC LIMIT 1
                """, (uid, now_iso())).fetchone()
                if existing:
                    old_end = datetime.fromisoformat(existing["end_at"])
                    end = old_end + timedelta(days=days)
                    c.execute(
                        """UPDATE subscriptions
                           SET plan=?, days=?, amount=?, end_at=?, status='active',
                               source='manual', payment_request_id=?
                           WHERE id=?""",
                        (r["plan"], days, r["amount"], end.isoformat(), pid, existing["id"]),
                    )
                else:
                    end = start + timedelta(days=days)
                    c.execute("""
                        INSERT INTO subscriptions(
                            user_id,plan,days,amount,start_at,end_at,status,source,
                            payment_request_id,created_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?)
                    """, (
                        uid, r["plan"], days, r["amount"], start.isoformat(),
                        end.isoformat(), "active", "manual", pid, now_iso()
                    ))

                changed = c.execute("""
                    UPDATE payment_requests
                    SET status='approved', reviewed_at=?, reviewed_by=?
                    WHERE id=? AND status='pending'
                """, (now_iso(), q.from_user.id, pid)).rowcount
                if changed != 1:
                    raise RuntimeError("Payment request state changed during approval")
            else:
                changed = c.execute("""
                    UPDATE payment_requests
                    SET status='rejected', reviewed_at=?, reviewed_by=?
                    WHERE id=? AND status='pending'
                """, (now_iso(), q.from_user.id, pid)).rowcount
                if changed != 1:
                    raise RuntimeError("Payment request state changed during rejection")

        if action == "approve":
            await q.message.reply_text("✅ پرداخت تأیید و اشتراک فعال شد.")
            try:
                await context.bot.send_message(
                    uid,
                    f"✅ اشتراک شما فعال شد.\n📅 مدت: {days} روز\n"
                    f"⏰ پایان: {format_dt(end.isoformat())}\n\nمنوی شما به‌روزرسانی شد.",
                    reply_markup=main_kb(uid),
                )
            except Exception:
                log.exception("Could not refresh keyboard after payment approval for uid=%s", uid)
            try:
                await asyncio.to_thread(backup_database, "payment_approved")
            except Exception:
                log.exception("payment approval backup failed")
        else:
            await q.message.reply_text("❌ پرداخت رد شد.")
            try:
                await context.bot.send_message(
                    uid, "❌ رسید پرداخت شما رد شد. برای پیگیری با پشتیبانی تماس بگیرید."
                )
            except Exception:
                log.exception("payment rejection notice failed for uid=%s", uid)
    except Exception:
        log.exception("payment admin callback")
        await q.message.reply_text("⚠️ بررسی پرداخت انجام نشد؛ وضعیت درخواست را در پنل پرداخت‌ها دوباره بررسی کنید.")

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
    text = re.sub(r"\s*🔒\s*$", "", text).strip()

    if context.user_data.get("admin_reply_to") is not None and is_admin(uid):
        await send_support_reply(update, context, text)
        return

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
        "🎁 تست رایگان": trial_menu,
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
        delivered = 0
        markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("↩️ پاسخ", callback_data=f"sup:reply:{uid}")
        ]])
        for aid in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    chat_id=aid,
                    text=(f"📨 <b>پیام پشتیبانی</b>\n"
                          f"👤 کاربر: <code>{uid}</code>\n\n{escape(text)}"),
                    parse_mode=ParseMode.HTML,
                    reply_markup=markup,
                )
                delivered += 1
            except Exception:
                log.exception("support message delivery failed for admin_id=%s", aid)
        if delivered:
            await save_support(uid, text, update.message.message_id)
            context.user_data["support_mode"] = False
            await update.message.reply_text("✅ پیام شما به پشتیبان ارسال شد.")
        else:
            await update.message.reply_text(
                "❌ پیام به پشتیبان ارسال نشد. لطفاً کمی بعد دوباره تلاش کنید یا به مدیر اطلاع دهید."
            )
        return

    if context.user_data.get("awaiting_asset"):
        context.user_data.pop("awaiting_asset", None)
        await process_add_asset(update, context, text)
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
        elif mode in ("block", "unblock"):
            try:
                target = int(text.strip())
                with db() as c:
                    cur = c.execute("UPDATE users SET blocked=? WHERE user_id=?",
                                    (1 if mode == "block" else 0, target))
                if cur.rowcount:
                    if mode == "block":
                        await update.message.reply_text(f"🚫 کاربر {target} مسدود شد.")
                    else:
                        await update.message.reply_text(f"✅ مسدودی کاربر {target} برداشته شد.")
                else:
                    await update.message.reply_text("❌ کاربر با این شناسه در دیتابیس پیدا نشد.")
            except Exception as e:
                await update.message.reply_text(f"❌ خطا: {escape(str(e))}", parse_mode=ParseMode.HTML)
            return
        elif mode == "sub_activate_uid":
            try:
                target = int(text.strip())
                with db() as c:
                    exists = c.execute("SELECT 1 FROM users WHERE user_id=?", (target,)).fetchone()
                if not exists:
                    await update.message.reply_text("❌ کاربر در دیتابیس پیدا نشد. ابتدا باید ربات را /start کرده باشد.")
                    return
                context.user_data["sub_target_uid"] = target
                context.user_data["admin_mode"] = "sub_activate_days"
                await update.message.reply_text("مدت اشتراک را به تعداد روز ارسال کنید (مثلاً 30، 90 یا 180).")
            except Exception as e:
                await update.message.reply_text(f"❌ شناسه نامعتبر است: {escape(str(e))}", parse_mode=ParseMode.HTML)
            return
        elif mode == "sub_activate_days":
            try:
                days = int(text.strip())
                if days < 1 or days > 3650:
                    raise ValueError("مدت باید بین 1 تا 3650 روز باشد")
                target = int(context.user_data.pop("sub_target_uid"))
                now = datetime.now(timezone.utc)
                old = active_subscription(target)
                start = now
                if old:
                    try:
                        old_end = datetime.fromisoformat(old["end_at"])
                        if old_end > start:
                            start = old_end
                    except Exception:
                        pass
                end = start + timedelta(days=days)
                with db() as c:
                    c.execute("UPDATE subscriptions SET status='cancelled' WHERE user_id=? AND status='active' AND end_at>?",
                              (target, now_iso()))
                    c.execute("""INSERT INTO subscriptions(user_id,plan,days,amount,start_at,end_at,status,source,payment_request_id,created_at)
                                 VALUES(?,?,?,?,?,?,?,?,?,?)""",
                              (target, f"MANUAL{days}", days, 0, now.isoformat(), end.isoformat(),
                               "active", "admin_manual", None, now_iso()))
                try:
                    asyncio.create_task(asyncio.to_thread(backup_database, "admin_manual_subscription"))
                except Exception:
                    log.exception("manual subscription backup")
                await update.message.reply_text(f"✅ اشتراک کاربر {target} به‌صورت دستی فعال شد.\n📅 مدت: {days} روز\n⏰ پایان: {format_dt(end.isoformat())}")
                try:
                    await context.bot.send_message(
                        target,
                        f"✅ اشتراک شما توسط مدیر فعال شد.\n📅 مدت: {days} روز\n⏰ پایان: {format_dt(end.isoformat())}\n\nمنوی شما به‌روزرسانی شد.",
                        reply_markup=main_kb(target),
                    )
                except Exception:
                    log.exception("Could not refresh keyboard after manual activation for uid=%s", target)
            except Exception as e:
                context.user_data.pop("sub_target_uid", None)
                await update.message.reply_text(f"❌ فعال‌سازی انجام نشد: {escape(str(e))}", parse_mode=ParseMode.HTML)
            return
        elif mode == "sub_deactivate":
            try:
                target = int(text.strip())
                with db() as c:
                    exists = c.execute("SELECT 1 FROM users WHERE user_id=?", (target,)).fetchone()
                    if exists:
                        cur = c.execute("UPDATE subscriptions SET status='cancelled' WHERE user_id=? AND status='active' AND end_at>?",
                                        (target, now_iso()))
                    else:
                        cur = None
                if not exists:
                    await update.message.reply_text("❌ کاربر در دیتابیس پیدا نشد.")
                elif cur.rowcount:
                    try:
                        asyncio.create_task(asyncio.to_thread(backup_database, "admin_deactivate_subscription"))
                    except Exception:
                        log.exception("subscription cancellation backup")
                    await update.message.reply_text(f"⛔ اشتراک فعال کاربر {target} غیرفعال شد.")
                    try:
                        await context.bot.send_message(
                            target,
                            "⛔ اشتراک شما توسط مدیر غیرفعال شد.\nمنوی شما به‌روزرسانی شد.",
                            reply_markup=main_kb(target),
                        )
                    except Exception:
                        log.exception("Could not refresh keyboard after manual deactivation for uid=%s", target)
                else:
                    await update.message.reply_text("ℹ️ این کاربر اشتراک فعالی برای غیرفعال‌سازی ندارد.")
            except Exception as e:
                await update.message.reply_text(f"❌ خطا: {escape(str(e))}", parse_mode=ParseMode.HTML)
            return

    if norm_symbol(text) in ("XAU", "GOLD18"):
        await process_add_asset(update, context, text)
        return
    try:
        if await crypto_search(text):
            await process_add_asset(update, context, text)
            return
    except Exception:
        log.exception("Nobitex symbol lookup failed for user input")

    await update.message.reply_text("❓ گزینه نامعتبر است. از منوی پایین استفاده کنید.",
                                    reply_markup=main_kb(uid))

async def cancel_command(update, context):
    for k in ("awaiting_asset","support_mode","chat_room","payment_plan",
              "admin_reply_to","admin_mode","message_target"):
        context.user_data.pop(k, None)
    await update.message.reply_text("✅ لغو شد.", reply_markup=main_kb(update.effective_user.id))

async def error_handler(update, context):
    log.error("Unhandled Telegram error", exc_info=context.error)


# ============================================================
# MARKET AI v2 — LEARNING / BAYESIAN WEIGHTS / PAPER TRADING
# ============================================================

V2_ENABLED = os.getenv("V2_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
V2_PAPER_ENABLED = os.getenv("V2_PAPER_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
V2_PAPER_CAPITAL = max(100.0, float(os.getenv("V2_PAPER_CAPITAL", "10000")))
V2_RISK_PER_TRADE_PCT = min(2.0, max(0.1, float(os.getenv("V2_RISK_PER_TRADE_PCT", "1.0"))))
V2_MAX_OPEN_PAPER = max(1, min(3, int(os.getenv("V2_MAX_OPEN_PAPER", "3"))))
V2_LEARNING_HORIZON_HOURS = max(24, int(os.getenv("V2_LEARNING_HORIZON_HOURS", "168")))
V2_MIN_LEARNING_SAMPLES = max(3, int(os.getenv("V2_MIN_LEARNING_SAMPLES", "8")))
V2_TRADINGVIEW_BASE = "https://www.tradingview.com/chart/?symbol=BINANCE%3A"

V2_CRITERIA = (
    "bull_trend", "rsi_zone", "macd_positive",
    "adx_strong", "volume_confirm", "early_entry",
)

def v2_init_db():
    """ساخت جداول نسخه ۲ بدون حذف یا بازنویسی جداول نسخه قبلی."""
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS v2_signal_history(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_key TEXT NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            created_at TEXT NOT NULL,
            entry REAL NOT NULL,
            stop REAL NOT NULL,
            target1 REAL NOT NULL,
            target2 REAL NOT NULL,
            score REAL NOT NULL DEFAULT 0,
            feature_json TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'OPEN',
            exit_price REAL,
            pnl_pct REAL,
            outcome TEXT,
            resolved_at TEXT,
            learning_done INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_v2_signal_status
            ON v2_signal_history(status, created_at);
        CREATE TABLE IF NOT EXISTS v2_adaptive_weights(
            criterion TEXT PRIMARY KEY,
            alpha REAL NOT NULL DEFAULT 2,
            beta REAL NOT NULL DEFAULT 2,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS v2_paper_positions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_key TEXT NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            opened_at TEXT NOT NULL,
            entry REAL NOT NULL,
            stop REAL NOT NULL,
            target1 REAL NOT NULL,
            target2 REAL NOT NULL,
            quantity REAL NOT NULL,
            risk_amount REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'OPEN',
            exit_price REAL,
            pnl_usd REAL,
            pnl_pct REAL,
            close_reason TEXT,
            closed_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_v2_paper_status
            ON v2_paper_positions(status, opened_at);
        CREATE TABLE IF NOT EXISTS v2_scan_metrics(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scanned_at TEXT NOT NULL,
            universe_size INTEGER NOT NULL DEFAULT 0,
            candidates INTEGER NOT NULL DEFAULT 0,
            confirmed INTEGER NOT NULL DEFAULT 0,
            note TEXT DEFAULT ''
        );
        """)
        stamp = now_iso()
        for criterion in V2_CRITERIA:
            c.execute(
                "INSERT OR IGNORE INTO v2_adaptive_weights(criterion,alpha,beta,updated_at) VALUES(?,?,?,?)",
                (criterion, 2.0, 2.0, stamp),
            )
        c.execute("INSERT OR REPLACE INTO app_meta(key,value) VALUES('v2_schema_version','2')")

def v2_feature_snapshot(ta):
    """تبدیل وضعیت اندیکاتورها به شواهد دودویی قابل یادگیری."""
    ta = ta or {}
    rsi = _safe_float(ta.get("rsi"), 50)
    return {
        "bull_trend": int(ta.get("trend") in ("BULLISH", "BULLISH_WEAK")),
        "rsi_zone": int(45 <= rsi <= 65),
        "macd_positive": int(_safe_float(ta.get("macd_hist")) > 0),
        "adx_strong": int(_safe_float(ta.get("adx")) >= 20),
        "volume_confirm": int(bool(ta.get("volume_confirm"))),
        "early_entry": int(bool(ta.get("early_entry"))),
    }

def v2_bayesian_stats():
    """محاسبه نرخ موفقیت پسین معیارها با prior بتا(۲،۲)."""
    if not V2_ENABLED:
        return {}
    try:
        v2_init_db()
        with db() as c:
            rows = c.execute(
                "SELECT criterion,alpha,beta FROM v2_adaptive_weights"
            ).fetchall()
        return {
            r["criterion"]: {
                "alpha": float(r["alpha"]),
                "beta": float(r["beta"]),
                "samples": max(0, int(r["alpha"] + r["beta"] - 4)),
                "win_rate": float(r["alpha"]) / max(0.0001, float(r["alpha"] + r["beta"])),
            }
            for r in rows
        }
    except Exception:
        log.exception("v2 Bayesian stats unavailable")
        return {}

def v2_apply_adaptive_score(base_score, ta):
    """اعمال تعدیل محدود بر اساس عملکرد واقعی معیارها؛ امتیاز خام حفظ می‌شود."""
    try:
        stats = v2_bayesian_stats()
        features = v2_feature_snapshot(ta)
        deltas, weights = [], []
        for name, active in features.items():
            if not active:
                continue
            item = stats.get(name) or {}
            if int(item.get("samples", 0)) < V2_MIN_LEARNING_SAMPLES:
                continue
            rate = float(item.get("win_rate", 0.5))
            confidence = min(1.0, int(item.get("samples", 0)) / 30.0)
            # معیار فقط وقتی فعال است از سابقه موفقیت خودش اثر می‌گیرد.
            weight = confidence * (0.5 + abs(rate - 0.5))
            deltas.append((rate - 0.5) * 8.0 * confidence)
            weights.append(weight)
        if not deltas or sum(weights) <= 0:
            return float(base_score)
        adjustment = sum(d * w for d, w in zip(deltas, weights)) / sum(weights)
        return _clamp(float(base_score) + _clamp(adjustment, -4.0, 4.0))
    except Exception:
        log.exception("v2 adaptive score failed")
        return float(base_score)

def v2_register_signal(item, cycle_id=None):
    """ثبت یک سیگنال تأییدشده و ایجاد معامله فرضی؛ از ثبت تکراری جلوگیری می‌کند."""
    if not V2_ENABLED:
        return False
    m = (item or {}).get("m") or {}
    ta = m.get("ta") or {}
    symbol = norm_symbol(item.get("symbol") or ta.get("symbol") or "")
    entry = _safe_float(m.get("price") or ta.get("price"))
    stop = _safe_float(ta.get("stop"))
    targets = ta.get("targets") or []
    t1 = _safe_float(targets[0]) if len(targets) > 0 else 0
    t2 = _safe_float(targets[1]) if len(targets) > 1 else 0
    if not symbol or entry <= 0 or stop <= 0 or t1 <= entry or t2 <= entry:
        return False
    risk_per_unit = entry - stop
    if risk_per_unit <= 0:
        return False
    rr = (t1 - entry) / risk_per_unit
    if rr < 1.0:
        return False
    key = f"{symbol}:{cycle_id or int(time.time() // 600)}"
    features = v2_feature_snapshot(ta)
    stamp = now_iso()
    try:
        v2_init_db()
        with db() as c:
            c.execute("BEGIN IMMEDIATE")
            cur = c.execute("""
                INSERT OR IGNORE INTO v2_signal_history(
                    signal_key,symbol,created_at,entry,stop,target1,target2,score,feature_json
                ) VALUES(?,?,?,?,?,?,?,?,?)
            """, (key, symbol, stamp, entry, stop, t1, t2,
                  _safe_float(m.get("score")), json.dumps(features, ensure_ascii=False)))
            inserted = cur.rowcount > 0
            if inserted and V2_PAPER_ENABLED:
                open_count = int(c.execute(
                    "SELECT COUNT(*) FROM v2_paper_positions WHERE status='OPEN'"
                ).fetchone()[0] or 0)
                already = c.execute(
                    "SELECT 1 FROM v2_paper_positions WHERE symbol=? AND status='OPEN' LIMIT 1",
                    (symbol,),
                ).fetchone()
                if open_count < V2_MAX_OPEN_PAPER and not already:
                    risk_amount = V2_PAPER_CAPITAL * V2_RISK_PER_TRADE_PCT / 100.0
                    quantity = risk_amount / risk_per_unit
                    c.execute("""
                        INSERT OR IGNORE INTO v2_paper_positions(
                            signal_key,symbol,opened_at,entry,stop,target1,target2,
                            quantity,risk_amount
                        ) VALUES(?,?,?,?,?,?,?,?,?)
                    """, (key, symbol, stamp, entry, stop, t1, t2, quantity, risk_amount))
        return inserted
    except Exception:
        log.exception("v2 signal registration failed")
        return False

def v2_resolve_learning(signal_key, exit_price, outcome, pnl_pct):
    """ثبت نتیجه و به‌روزرسانی posterior معیارهایی که هنگام سیگنال فعال بوده‌اند."""
    try:
        with db() as c:
            row = c.execute(
                "SELECT feature_json,learning_done FROM v2_signal_history WHERE signal_key=?",
                (signal_key,),
            ).fetchone()
            if not row or row["learning_done"]:
                return
            features = json.loads(row["feature_json"] or "{}")
            won = outcome == "WIN"
            stamp = now_iso()
            for criterion in V2_CRITERIA:
                if int(features.get(criterion, 0)):
                    c.execute("""
                        UPDATE v2_adaptive_weights
                        SET alpha=alpha+?, beta=beta+?, updated_at=?
                        WHERE criterion=?
                    """, (1.0 if won else 0.0, 0.0 if won else 1.0, stamp, criterion))
            c.execute("""
                UPDATE v2_signal_history
                SET status='CLOSED',exit_price=?,pnl_pct=?,outcome=?,resolved_at=?,learning_done=1
                WHERE signal_key=?
            """, (exit_price, pnl_pct, outcome, stamp, signal_key))
    except Exception:
        log.exception("v2 learning resolution failed")

def v2_update_paper_positions(market_rows):
    """به‌روزرسانی معاملات فرضی با آخرین قیمت‌های اسکن؛ کارمزد رفت‌وبرگشت لحاظ می‌شود."""
    if not V2_ENABLED or not V2_PAPER_ENABLED:
        return
    prices = {}
    for row in market_rows or []:
        sym = norm_symbol(row.get("symbol") or "")
        px = _safe_float(row.get("current_price"))
        if sym and px > 0:
            prices[sym] = px
    if not prices:
        return
    try:
        v2_init_db()
        with db() as c:
            positions = c.execute(
                "SELECT * FROM v2_paper_positions WHERE status='OPEN'"
            ).fetchall()
        for pos in positions:
            price = prices.get(pos["symbol"])
            if not price:
                continue
            entry, stop, t1 = float(pos["entry"]), float(pos["stop"]), float(pos["target1"])
            age_hours = (datetime.now(timezone.utc) -
                         datetime.fromisoformat(pos["opened_at"]).astimezone(timezone.utc)).total_seconds() / 3600
            reason = None
            exit_price = price
            if price <= stop:
                reason, exit_price = "STOP_LOSS", stop
            elif price >= t1:
                reason, exit_price = "TAKE_PROFIT_1", t1
            elif age_hours >= V2_LEARNING_HORIZON_HOURS:
                reason = "TIME_EXIT"
            if not reason:
                continue
            # 0.20 درصد هزینه تخمینی مجموع ورود و خروج؛ قابل تنظیم از env.
            fee_pct = max(0.0, float(os.getenv("V2_PAPER_FEE_PCT", "0.20")))
            gross_pct = (exit_price / entry - 1.0) * 100.0
            net_pct = gross_pct - fee_pct
            qty = float(pos["quantity"])
            pnl_usd = (exit_price - entry) * qty - (entry * qty * fee_pct / 100.0)
            outcome = "WIN" if net_pct > 0 else "LOSS"
            with db() as c:
                c.execute("""
                    UPDATE v2_paper_positions SET status='CLOSED',exit_price=?,pnl_usd=?,
                    pnl_pct=?,close_reason=?,closed_at=? WHERE id=? AND status='OPEN'
                """, (exit_price, pnl_usd, net_pct, reason, now_iso(), pos["id"]))
            v2_resolve_learning(pos["signal_key"], exit_price, outcome, net_pct)
    except Exception:
        log.exception("v2 paper position update failed")

def v2_paper_summary():
    """آمار معاملات فرضی بسته و باز را از دیتابیس استخراج می‌کند."""
    v2_init_db()
    with db() as c:
        total = int(c.execute("SELECT COUNT(*) FROM v2_paper_positions WHERE status='CLOSED'").fetchone()[0] or 0)
        wins = int(c.execute("SELECT COUNT(*) FROM v2_paper_positions WHERE status='CLOSED' AND pnl_usd>0").fetchone()[0] or 0)
        opened = int(c.execute("SELECT COUNT(*) FROM v2_paper_positions WHERE status='OPEN'").fetchone()[0] or 0)
        pnl = float(c.execute("SELECT COALESCE(SUM(pnl_usd),0) FROM v2_paper_positions WHERE status='CLOSED'").fetchone()[0] or 0)
        rows = c.execute("""
            SELECT symbol,entry,stop,target1,opened_at FROM v2_paper_positions
            WHERE status='OPEN' ORDER BY opened_at DESC LIMIT 10
        """).fetchall()
    return {"closed": total, "wins": wins, "win_rate": (wins / total * 100 if total else 0),
            "open": opened, "pnl": pnl, "positions": rows}

def v2_performance_text():
    """گزارش فارسی عملکرد Paper Trading و دقت معیارهای یادگرفته‌شده."""
    summary = v2_paper_summary()
    stats = v2_bayesian_stats()
    lines = [
        "📒 *گزارش Paper Trading نسخه ۲*",
        f"معاملات بسته: *{summary['closed']}*",
        f"نرخ برد: *{summary['win_rate']:.1f}%*",
        f"معاملات باز: *{summary['open']}*",
        f"سود/زیان فرضی خالص: *{summary['pnl']:+.2f} USDT*",
        "",
        "*عملکرد معیارها (Bayesian posterior)*",
    ]
    for name, item in stats.items():
        label = {
            "bull_trend": "روند صعودی", "rsi_zone": "RSI متعادل",
            "macd_positive": "MACD مثبت", "adx_strong": "قدرت ADX",
            "volume_confirm": "تأیید حجم", "early_entry": "ورود زودهنگام",
        }.get(name, name)
        lines.append(f"• {label}: {item['win_rate']*100:.1f}% | نمونه مؤثر: {item['samples']}")
    lines += [
        "",
        "توجه: تا جمع‌شدن نمونه کافی، نرخ‌ها صرفاً تخمینی و مبتنی بر prior هستند.",
        "داده on-chain، GitHub، کاتالیزور و شبکه اجتماعی در نسخه فعلی منبع قابل اتکای فعال ندارند و در امتیاز دخالت داده نشده‌اند.",
    ]
    return "\n".join(lines)

async def performance_command(update, context):
    """نمایش عملکرد یادگیری و معاملات فرضی در تلگرام."""
    if not is_admin(update.effective_user.id) and not has_paid_subscription(update.effective_user.id):
        await update.message.reply_text("🔒 گزارش عملکرد نسخه ۲ مخصوص مشترکین فعال است.")
        return
    try:
        await update.message.reply_text(v2_performance_text(), parse_mode=ParseMode.MARKDOWN)
    except Exception:
        log.exception("v2 performance command")
        await update.message.reply_text("گزارش عملکرد فعلاً در دسترس نیست.")

def v2_signal_markdown(a):
    """ساخت هشدار Markdown با سطوح معامله و لینک مستقیم TradingView."""
    symbol = norm_symbol(a.get("symbol") or "")
    entry = _safe_float(a.get("price"))
    stop = _safe_float(a.get("stop"))
    targets = a.get("targets") or []
    t1 = _safe_float(targets[0]) if len(targets) > 0 else 0
    t2 = _safe_float(targets[1]) if len(targets) > 1 else 0
    risk = entry - stop if entry > stop > 0 else 0
    rr = (t1 - entry) / risk if risk > 0 and t1 > entry else 0
    tv = V2_TRADINGVIEW_BASE + symbol + "USDT"
    return (
        "🚨 *سیگنال خرید MARKET AI v2*\n\n"
        f"*دارایی:* `{symbol}/USDT`\n"
        f"*امتیاز فرصت:* `{_safe_float(a.get('opportunity_score')):.1f}/100`\n"
        f"*ورود مرجع:* `{_format_price(symbol, entry)}`\n"
        f"*حد ضرر:* `{_format_price(symbol, stop)}`\n"
        f"*هدف اول:* `{_format_price(symbol, t1)}`\n"
        f"*هدف دوم:* `{_format_price(symbol, t2)}`\n"
        f"*ریسک به بازده تا هدف اول:* `{rr:.2f}`\n"
        f"*حجم ریسک Paper:* `{V2_RISK_PER_TRADE_PCT:.2f}%` از سرمایه فرضی\n\n"
        f"[بازکردن نمودار TradingView]({tv})\n\n"
        "_سیگنال تحلیلی است؛ معامله واقعی به‌صورت خودکار انجام نمی‌شود._"
    )

def v2_walk_forward_optimize(prices, train_ratio=0.60, fee_pct=0.20, folds=4):
    """بهینه‌سازی روی پنجره‌های گذشته و آزمون روی پنجره بعدی؛ بدون استفاده از آینده."""
    p = pd.to_numeric(pd.Series(prices), errors="coerce").dropna().astype(float).reset_index(drop=True)
    if len(p) < 240:
        return {"ok": False, "reason": "حداقل ۲۴۰ کندل برای walk-forward چندمرحله‌ای لازم است."}
    candidates = [
        (40, 60, 9, 21), (42, 62, 9, 26), (45, 65, 12, 30),
        (38, 58, 8, 24), (44, 64, 10, 30),
    ]
    fold_size = max(30, int(len(p) * (1.0 - train_ratio) / max(1, folds)))
    results, all_test_returns = [], []

    def strategy_returns(series, params):
        low, high, fast, slow = params
        rsi = _rsi(series, 14)
        ef = series.ewm(span=fast, adjust=False).mean()
        es = series.ewm(span=slow, adjust=False).mean()
        raw_signal = (rsi.between(low, high) & (ef > es)).astype(float)
        # پوزیشن در کندل بعدی اجرا می‌شود تا look-ahead bias کاهش یابد.
        position = raw_signal.shift(1).fillna(0)
        asset_ret = series.pct_change().fillna(0)
        turnover = position.diff().abs().fillna(position.abs())
        return position * asset_ret - turnover * (fee_pct / 100.0)

    first_test = max(120, int(len(p) * train_ratio))
    for fold in range(max(1, folds)):
        test_start = first_test + fold * fold_size
        test_end = min(len(p), test_start + fold_size)
        if test_start >= len(p) or test_end - test_start < 20:
            break
        train = p.iloc[:test_start]
        test = p.iloc[test_start:test_end]
        trials = []
        for params in candidates:
            r = strategy_returns(train, params)
            trials.append((float((1 + r).prod() - 1), params))
        best_train_return, best_params = max(trials, key=lambda x: x[0])
        test_r = strategy_returns(test, best_params)
        all_test_returns.extend(test_r.tolist())
        test_equity = (1 + test_r).cumprod()
        peak = test_equity.cummax()
        dd = (test_equity / peak - 1.0) * 100.0
        active = test_r[test_r != 0]
        wins = int((active > 0).sum())
        losses = int((active < 0).sum())
        gross_profit = float(active[active > 0].sum())
        gross_loss = abs(float(active[active < 0].sum()))
        results.append({
            "fold": fold + 1,
            "train_bars": len(train),
            "test_bars": len(test),
            "parameters": {"rsi_low": best_params[0], "rsi_high": best_params[1],
                           "ema_fast": best_params[2], "ema_slow": best_params[3]},
            "train_return_pct": best_train_return * 100.0,
            "test_return_pct": (float((1 + test_r).prod()) - 1.0) * 100.0,
            "test_win_rate_pct": wins / max(1, wins + losses) * 100.0,
            "test_profit_factor": gross_profit / gross_loss if gross_loss > 0 else (999.0 if gross_profit else 0.0),
            "test_max_drawdown_pct": float(dd.min()) if len(dd) else 0.0,
        })
    if not results:
        return {"ok": False, "reason": "داده کافی برای ساخت fold مستقل وجود ندارد."}
    aggregate = pd.Series(all_test_returns, dtype=float)
    equity = (1 + aggregate).cumprod()
    drawdown = (equity / equity.cummax() - 1.0) * 100.0
    active = aggregate[aggregate != 0]
    wins = int((active > 0).sum())
    losses = int((active < 0).sum())
    gp = float(active[active > 0].sum())
    gl = abs(float(active[active < 0].sum()))
    return {
        "ok": True, "folds_completed": len(results), "fold_results": results,
        "aggregate_test_return_pct": (float((1 + aggregate).prod()) - 1.0) * 100.0,
        "aggregate_win_rate_pct": wins / max(1, wins + losses) * 100.0,
        "aggregate_profit_factor": gp / gl if gl > 0 else (999.0 if gp else 0.0),
        "aggregate_max_drawdown_pct": float(drawdown.min()) if len(drawdown) else 0.0,
        "warning": "داده ورودی قیمت بسته‌شدن است؛ کارمزد تخمینی لحاظ شده، اما لغزش، spread و اجرای درون‌کندلی مدل نشده‌اند.",
    }

def dashboard_main():
    """داشبورد اختیاری Streamlit که با RUN_DASHBOARD=1 اجرا می‌شود."""
    try:
        import streamlit as st
    except ImportError as exc:
        raise RuntimeError("برای داشبورد باید streamlit نصب شود: pip install streamlit") from exc
    st.set_page_config(page_title="MARKET AI v2", layout="wide")
    st.title("📊 MARKET AI v2 — Learning & Paper Trading")
    v2_init_db()
    summary = v2_paper_summary()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("معاملات بسته", summary["closed"])
    c2.metric("نرخ برد", f"{summary['win_rate']:.1f}%")
    c3.metric("معاملات باز", summary["open"])
    c4.metric("PnL فرضی (USDT)", f"{summary['pnl']:+.2f}")
    with db() as con:
        signals = pd.read_sql_query(
            "SELECT symbol,created_at,entry,stop,target1,target2,score,status,outcome,pnl_pct "
            "FROM v2_signal_history ORDER BY id DESC LIMIT 100", con)
        paper = pd.read_sql_query(
            "SELECT symbol,opened_at,entry,stop,target1,quantity,status,pnl_usd,pnl_pct,close_reason "
            "FROM v2_paper_positions ORDER BY id DESC LIMIT 100", con)
    st.subheader("آخرین سیگنال‌ها")
    st.dataframe(signals, use_container_width=True)
    st.subheader("معاملات فرضی")
    st.dataframe(paper, use_container_width=True)
    st.subheader("عملکرد معیارها")
    stats = v2_bayesian_stats()
    stat_df = pd.DataFrame([{"معیار": k, "Win rate پسین": v["win_rate"],
                             "نمونه مؤثر": v["samples"]} for k, v in stats.items()])
    if not stat_df.empty:
        st.bar_chart(stat_df.set_index("معیار")["Win rate پسین"])
        st.dataframe(stat_df, use_container_width=True)
    st.caption("داده on-chain و بنیادی تا زمان اتصال API معتبر، در امتیازدهی استفاده نمی‌شود.")


# ============================================================
# STARTUP / MAIN
# ============================================================

async def live_status_command(update, context):
    """Check public Nobitex market-data API health."""
    data = await http_json(f"{NOBITEX_BASE_URL}/market/stats", {"srcCurrency":"btc", "dstCurrency":"usdt"}, ttl=5, retries=1)
    stats = (data or {}).get("stats") or {}
    btc = stats.get("btc-usdt")
    if btc:
        price = _safe_float(btc.get("latest") or btc.get("dayClose"))
        status = "🟢 API نوبیتکس در دسترس است"
        detail = f"\nقیمت BTC: <b>{price:,.2f} USDT</b>\nتغییر ۲۴ساعت: <b>{_safe_float(btc.get('dayChange')):+.2f}%</b>"
    else:
        status = "🔴 دریافت اطلاعات از API نوبیتکس ناموفق بود"
        detail = "\nاتصال شبکه، محدودیت IP یا وضعیت API را بررسی کنید."
    await update.effective_message.reply_text(
        "📡 <b>وضعیت منبع داده بازار</b>\n\n"
        f"منبع رمزارز: <b>Nobitex Public API</b>\n{status}{detail}\n"
        "قیمت، کندل‌ها، اندیکاتورها و تمام سطوح تحلیل رمزارز بر اساس بازارهای USDT نوبیتکس محاسبه می‌شوند.",
        parse_mode=ParseMode.HTML,
    )


async def post_init(app):
    init_db()
    v2_init_db()
    try:
        await app.bot.delete_webhook(drop_pending_updates=False)
    except Exception as e:
        log.warning("delete webhook: %s", e)
    app.create_task(alert_worker(app))
    app.create_task(backup_worker())
    app.create_task(market_snapshot_worker())
    log.info("MARKET AI started | data_source=Nobitex | cache=%s", len(CACHE))

async def post_shutdown(app):
    global HTTP_SESSION
    if HTTP_SESSION and not HTTP_SESSION.closed:
        await HTTP_SESSION.close()

def main():
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    init_db()
    v2_init_db()
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_text))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CommandHandler("backup", backup_command))
    app.add_handler(CommandHandler("dbstatus", db_status_command))
    app.add_handler(CommandHandler("admin", admin_panel))
    app.add_handler(CommandHandler("performance", performance_command))
    app.add_handler(CommandHandler("live", live_status_command))
    app.add_handler(CommandHandler("paper", performance_command))
    app.add_handler(CallbackQueryHandler(admin_callback, pattern=r"^adm:"))
    app.add_handler(CallbackQueryHandler(payment_admin_callback, pattern=r"^pay:(approve|reject):"))
    app.add_handler(CallbackQueryHandler(plan_callback, pattern=r"^plan:"))
    app.add_handler(CallbackQueryHandler(trial_callback, pattern=r"^trial:start$"))
    app.add_handler(CallbackQueryHandler(trial_check_callback, pattern=r"^trial:check$"))
    app.add_handler(CallbackQueryHandler(alert_callback, pattern=r"^alert:toggle$"))
    app.add_handler(CallbackQueryHandler(pick_asset_callback, pattern=r"^pick:"))
    app.add_handler(CallbackQueryHandler(watchlist_delete_callback, pattern=r"^wl:del:"))
    app.add_handler(CallbackQueryHandler(selected_price_callback, pattern=r"^price:"))
    app.add_handler(CallbackQueryHandler(selected_analysis_callback, pattern=r"^analysis:"))
    app.add_handler(CallbackQueryHandler(selected_signal_callback, pattern=r"^signal:"))
    app.add_handler(CallbackQueryHandler(opportunity_action_callback, pattern=r"^op:"))
    app.add_handler(CallbackQueryHandler(support_reply_callback, pattern=r"^sup:reply:"))
    app.add_handler(MessageHandler(
        (filters.PHOTO | filters.Document.ALL | filters.VOICE) & ~filters.COMMAND,
        support_media
    ), group=-1)
    app.add_handler(MessageHandler(filters.PHOTO, receipt_photo))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))
    app.add_error_handler(error_handler)
    log.info("MARKET AI v2 polling started")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=False)

if __name__ == "__main__":
    if os.getenv("RUN_DASHBOARD", "0").strip().lower() in {"1", "true", "yes"}:
        dashboard_main()
    else:
        main()
