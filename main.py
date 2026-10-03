# -*- coding: utf-8 -*-
"""
Crypto / Global Gold / Iran 18K Gold Telegram Analyzer
Railway production build
Analysis + signals + notifications. No automatic trading.

Sources:
- Crypto: CoinGecko
- Iran 18K Gold: TGJU / profile/geram18
- Global Gold XAU: TGJU / profile/ons

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
"""

import os
import re
import sqlite3
import shutil
from pathlib import Path
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
for x in os.getenv("ADMIN_IDS", "").split(","):
    try:
        if x.strip():
            ADMIN_IDS.add(int(x.strip()))
    except ValueError:
        pass

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

# Smart technical-analysis engine
GOLD_HISTORY_INTERVAL_SECONDS = max(60, int(os.getenv("GOLD_HISTORY_INTERVAL_SECONDS", "300")))
GOLD_MIN_HISTORY_POINTS = max(50, int(os.getenv("GOLD_MIN_HISTORY_POINTS", "50")))
SIGNAL_MIN_SCORE = max(70, min(100, int(os.getenv("SIGNAL_MIN_SCORE", "78"))))
SIGNAL_CONFIRMATIONS_REQUIRED = max(2, int(os.getenv("SIGNAL_CONFIRMATIONS_REQUIRED", "2")))
MARKET_SCAN_PAGES = max(1, min(40, int(os.getenv("MARKET_SCAN_PAGES", "10"))))
MARKET_SCAN_PER_PAGE = max(50, min(250, int(os.getenv("MARKET_SCAN_PER_PAGE", "250"))))
MARKET_SCAN_TOP = max(5, min(30, int(os.getenv("MARKET_SCAN_TOP", "10"))))
MARKET_SCAN_SECONDS = max(300, int(os.getenv("MARKET_SCAN_SECONDS", "900")))
MARKET_SCAN_DEEP = max(5, min(30, int(os.getenv("MARKET_SCAN_DEEP", "15"))))
MARKET_SCAN_CACHE = None

PAYMENT_CARD = os.getenv("PAYMENT_CARD", "ثبت نشده").strip()
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "پشتیبان").strip()

# TGJU sources
GOLD18_URL = "https://www.tgju.org/profile/geram18"
XAU_TGJU_URL = "https://www.tgju.org/profile/ons"

# Optional explicit selectors if TGJU changes HTML.
GOLD18_PRICE_SELECTOR = os.getenv("GOLD18_PRICE_SELECTOR", "").strip()
XAU_PRICE_SELECTOR = os.getenv("XAU_PRICE_SELECTOR", "").strip()

# auto = detect, toman/rial = force source unit
GOLD18_SOURCE_UNIT = os.getenv("GOLD18_SOURCE_UNIT", "auto").strip().lower()
XAU_SOURCE_UNIT = os.getenv("XAU_SOURCE_UNIT", "auto").strip().lower()

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
CACHE = {}
PRICE_CACHE = {}
ANALYSIS_CACHE = {}
GOLD18_CACHE = None
XAU_CACHE = None

MAIN_MENU = [
    ["➕ افزودن دارایی", "📋 واچ‌لیست"],
    ["💰 قیمت لحظه‌ای", "📊 تحلیل"],
    ["🚨 سیگنال‌ها", "🔎 فرصت‌های رشد"],
    ["💳 خرید اشتراک", "👤 وضعیت اشتراک"],
    ["🔔 هشدارها", "🪙 ارزهای بیشتر"],
    ["💬 چت رمز ارز", "📨 ارتباط با پشتیبان"],
    ["ℹ️ راهنما"],
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
    """Prepare the persistent DB without ever replacing a healthy user database.

    Priority:
      1) Existing /data database with users/subscriptions.
      2) Latest valid backup on /data/backups.
      3) Legacy database left in the container/repo.
      4) Only then allow SQLite to create a fresh database.

    This is intentionally conservative so a code update cannot wipe subscriptions.
    """
    target = Path(DB_PATH)
    target.parent.mkdir(parents=True, exist_ok=True)
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)

    # Never replace a healthy persistent database.
    if target.exists() and _db_has_tables(target):
        users = _db_user_count(target)
        if users > 0:
            return

        # Empty DB: try backup first, then legacy DB.
        if AUTO_RESTORE_BACKUP and _restore_latest_backup(target):
            return

    # If target exists but is corrupt/empty, preserve it before attempting recovery.
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

    # Optional comma-separated legacy paths can be supplied during migration.
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

            # If target is a bad/empty file, preserve it for diagnosis.
            if target.exists() and target.stat().st_size > 0 and _db_user_count(target) == 0:
                try:
                    quarantine = target.with_name(
                        target.name + ".empty-before-recovery"
                    )
                    if not quarantine.exists():
                        shutil.copy2(target, quarantine)
                except Exception:
                    pass

            tmp = target.with_suffix(target.suffix + ".migrate.tmp")
            shutil.copy2(src, tmp)
            os.replace(tmp, target)
            log.warning(
                "RECOVERED USER DATABASE: %s -> %s | users=%s",
                src, target, users
            )
            return
        except Exception as e:
            log.warning("Legacy database recovery failed %s: %s", src, e)

    # Final backup attempt after legacy search.
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
    """Repair columns that older production databases may be missing before indexes/queries run."""
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
                # Older versions used different names for the subscription expiry.
                if "end_date" in cols:
                    con.execute("UPDATE subscriptions SET end_at=end_date WHERE end_at IS NULL OR end_at=''" )
                elif "expires_at" in cols:
                    con.execute("UPDATE subscriptions SET end_at=expires_at WHERE end_at IS NULL OR end_at=''" )
                elif "expiry" in cols:
                    con.execute("UPDATE subscriptions SET end_at=expiry WHERE end_at IS NULL OR end_at=''" )
                elif "start_at" in cols and "days" in cols:
                    rows = con.execute("SELECT id,start_at,days FROM subscriptions WHERE end_at IS NULL OR end_at=''" ).fetchall()
                    for row in rows:
                        try:
                            start = datetime.fromisoformat(str(row[1]))
                            end = start + timedelta(days=int(row[2] or 0))
                            con.execute("UPDATE subscriptions SET end_at=? WHERE id=?", (end.isoformat(), row[0]))
                        except Exception:
                            pass

            # Ensure legacy rows with no expiry cannot break active-subscription queries.
            if "status" in cols or "status" in {r[1] for r in con.execute("PRAGMA table_info(subscriptions)")}:
                con.execute("UPDATE subscriptions SET status='expired' WHERE (end_at IS NULL OR end_at='') AND status='active'")

        if "watchlist" in tables:
            cols = {r[1] for r in con.execute("PRAGMA table_info(watchlist)")}
            if "asset_key" in cols:
                # Old schema requires asset_key on INSERT. Existing rows are left intact.
                con.execute("UPDATE watchlist SET asset_key=upper(symbol) WHERE asset_key IS NULL OR asset_key=''" )

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
        CREATE TABLE IF NOT EXISTS app_meta(
            key TEXT PRIMARY KEY,value TEXT
        );

        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            created_at TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            blocked INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS subscriptions(
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

        CREATE TABLE IF NOT EXISTS payment_requests(
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

        CREATE TABLE IF NOT EXISTS watchlist(
            user_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(user_id,symbol,asset_type)
        );

        CREATE TABLE IF NOT EXISTS alert_preferences(
            user_id INTEGER PRIMARY KEY,
            enabled INTEGER NOT NULL DEFAULT 1,
            interval_seconds INTEGER NOT NULL DEFAULT 300,
            last_check_at TEXT
        );

        CREATE TABLE IF NOT EXISTS alert_events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            signal_key TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS market_history(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            price REAL NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_market_history_asset
        ON market_history(symbol,asset_type,created_at);

        CREATE TABLE IF NOT EXISTS signal_state(
            symbol TEXT PRIMARY KEY,
            candidate TEXT NOT NULL,
            confirmations INTEGER NOT NULL DEFAULT 0,
            last_candidate_at TEXT,
            last_price REAL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_signal_state_candidate
        ON signal_state(candidate,confirmations);

        CREATE TABLE IF NOT EXISTS support_messages(
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

        CREATE TABLE IF NOT EXISTS chat_messages(
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

        CREATE TABLE IF NOT EXISTS chat_reports(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER NOT NULL,
            reporter_id INTEGER NOT NULL,
            reason TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,
            reviewed_by INTEGER,
            reviewed_at TEXT
        );

        """)
        # Additive migrations for older databases.  The bot must keep
        # existing users/subscriptions/watchlists after code upgrades.
        ensure_column(c, "users", "username", "TEXT")
        ensure_column(c, "users", "first_name", "TEXT")
        ensure_column(c, "users", "created_at", "TEXT")
        ensure_column(c, "users", "blocked", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(c, "users", "last_seen", "TEXT")

        # Subscription migrations for databases created by older bot versions.
        ensure_column(c, "subscriptions", "plan", "TEXT")
        ensure_column(c, "subscriptions", "days", "INTEGER")
        ensure_column(c, "subscriptions", "amount", "INTEGER")
        ensure_column(c, "subscriptions", "start_at", "TEXT")
        ensure_column(c, "subscriptions", "end_at", "TEXT")
        ensure_column(c, "subscriptions", "status", "TEXT DEFAULT 'expired'")
        ensure_column(c, "subscriptions", "source", "TEXT")
        ensure_column(c, "subscriptions", "payment_request_id", "INTEGER")
        ensure_column(c, "subscriptions", "created_at", "TEXT")

        # Watchlist migrations. Some old production DBs have a mandatory
        # asset_key column, so add/populate it and make inserts aware of it.
        ensure_column(c, "watchlist", "user_id", "INTEGER")
        ensure_column(c, "watchlist", "symbol", "TEXT")
        ensure_column(c, "watchlist", "asset_type", "TEXT")
        ensure_column(c, "watchlist", "created_at", "TEXT")
        # Some production databases created by an older version have a
        # mandatory asset_key column. Add it safely when missing, and fill
        # empty legacy rows before any watchlist INSERT is attempted.
        ensure_column(c, "watchlist", "asset_key", "TEXT DEFAULT ''")
        c.execute("UPDATE watchlist SET asset_key=upper(symbol) WHERE asset_key IS NULL OR asset_key='' ")
        ensure_column(c, "alert_events", "signal_key", "TEXT DEFAULT ''")
        ensure_column(c, "support_messages", "replied_at", "TEXT")
        ensure_column(c, "chat_messages", "deleted_by", "INTEGER")
        ensure_column(c, "chat_messages", "deleted_at", "TEXT")

        # Create indexes only AFTER additive migrations. Older production
        # databases may not have columns such as subscriptions.end_at yet.
        c.execute("CREATE INDEX IF NOT EXISTS idx_sub_user_end ON subscriptions(user_id,end_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_pay_status ON payment_requests(status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_watch_asset ON watchlist(asset_type,symbol)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_chat_room ON chat_messages(asset_type,symbol,created_at)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_chat_reports ON chat_reports(status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_alert_events ON alert_events(user_id,symbol,created_at)")

        c.execute("INSERT OR REPLACE INTO app_meta(key,value) VALUES('schema_version','11')")
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
    item = CACHE.get(key)
    if item and time.monotonic() - item[0] < ttl:
        return item[1]

    session = await get_session()
    for attempt in range(retries):
        try:
            async with session.get(url, params=params) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    CACHE[key] = (time.monotonic(), data)
                    return data
                log.warning("HTTP %s %s", r.status, url)
        except Exception as e:
            log.warning("HTTP error %s: %s", url, e)
        if attempt + 1 < retries:
            await asyncio.sleep(0.4 * (attempt + 1))
    return None

async def http_text(url, ttl=None):
    key = ("TEXT", url)
    ttl = CACHE_SECONDS if ttl is None else ttl
    item = CACHE.get(key)
    if item and time.monotonic() - item[0] < ttl:
        return item[1]
    try:
        session = await get_session()
        async with session.get(url) as r:
            if r.status == 200:
                text = await r.text()
                CACHE[key] = (time.monotonic(), text)
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
    """Fetch a broad CoinGecko market universe, paginated and cached.
    This is the universe the bot can actually inspect; it is broader than the
    hand-maintained COINS dictionary and can be tuned with MARKET_SCAN_PAGES.
    """
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

def _market_growth_score(x, ta=None):
    """Score growth setup, not guaranteed profit. Strength stays separate."""
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
    # Momentum, with a penalty for extreme one-day spikes.
    score += _clamp(ch1 * 2.0, -8, 8)
    score += _clamp(ch24 * 0.65, -10, 10)
    score += _clamp(ch7 * 0.40, -8, 8)
    score += _clamp(ch14 * 0.20, -5, 5)
    score += _clamp(ch30 * 0.12, -5, 5)

    turnover = vol / mcap
    score += _clamp((turnover - 0.05) * 80, -5, 8)

    # Prefer constructive momentum rather than already-parabolic moves.
    if ch24 > 25:
        score -= 7
    if ch7 > 60:
        score -= 5
    if ch24 < -20 and ch7 < -20:
        score -= 8

    if ta:
        score += _clamp((ta.get("strength",50)-50)*0.18, -8, 8)
        if ta.get("trend") in ("BULLISH", "BULLISH_WEAK"):
            score += 5
        if ta.get("rsi",50) > 75:
            score -= 5
        elif 52 <= ta.get("rsi",50) <= 68:
            score += 4
        if ta.get("macd_hist",0) > 0:
            score += 4
        if ta.get("adx",0) >= 20:
            score += 3

    return _clamp(score)

def _growth_probability(score, x):
    """Model confidence-like metric, explicitly not a guaranteed probability."""
    rank = float(x.get("market_cap_rank") or 10000)
    rank_bonus = _clamp(10 - (rank / 1000), -5, 10)
    return _clamp(50 + (score - 50) * 0.75 + rank_bonus, 5, 95)

async def growth_scan():
    rows = await market_universe()
    if not rows:
        return []

    # First pass over the entire market universe using market-wide fields.
    rough = []
    for x in rows:
        score = _market_growth_score(x)
        if score is None:
            continue
        rough.append((score, x))
    rough.sort(key=lambda z: z[0], reverse=True)

    # Deep technical pass only on the strongest candidates, avoiding hundreds
    # of expensive market_chart calls while still screening the whole universe.
    result = []
    for rough_score, x in rough[:MARKET_SCAN_DEEP]:
        prices = (x.get("sparkline_in_7d") or {}).get("price") or []
        ta = None
        if len(prices) >= 50:
            ta = technical_analysis(
                norm_symbol(x.get("symbol") or ""),
                pd.Series(prices, dtype=float),
                None
            )
        score = _market_growth_score(x, ta) or rough_score
        result.append({
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
            "strength": float(ta.get("strength", score)) if ta else score,
            "trend": ta.get("trend") if ta else "MARKET",
            "rsi": ta.get("rsi") if ta else None,
        })
    result.sort(key=lambda z: z["growth_score"], reverse=True)
    return result[:MARKET_SCAN_TOP]

# ============================================================
# NUMBER / TGJU PARSING
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
            out.append((
                m.start(),
                float(m.group(1).replace(",", "").replace(" ", ""))
            ))
        except Exception:
            pass
    return out

def number_candidates(text):
    return [
        v for _, v in _numbers_with_positions(text)
        if 100_000 <= v <= 500_000_000
    ]

def _unit_factor(text, forced_unit="auto"):
    t = digits_to_latin(text or "").replace(" ", "").lower()

    if forced_unit in {"toman", "تومان"}:
        return 1.0
    if forced_unit in {"rial", "ریال"}:
        return 0.1

    if "تومان" in t:
        return 1.0
    if "ریال" in t:
        return 0.1

    return None

def _infer_gold18_factor(raw_value):
    """TGJU sometimes omits the unit beside the numeric value.

    For GOLD18, a value in the hundreds of millions is the usual Rial
    representation of a roughly tens-of-millions Toman price. A value in
    the tens of millions can already be Toman. This is only used after the
    parser has tied the number to the geram18 price context.
    """
    try:
        v = float(raw_value)
    except Exception:
        return None
    if 50_000_000 <= v <= 500_000_000:
        return 0.1
    if 5_000_000 <= v < 50_000_000:
        return 1.0
    return None

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

def _candidate_from_node(tag, forced_unit="auto", min_value=100_000, max_value=500_000_000):
    text = tag.get_text(" ", strip=True)
    factor = _unit_factor(text, forced_unit)
    if factor is None:
        factor = _unit_factor(_context(tag), forced_unit)

    nums = [
        x for x in _numbers_with_positions(text)
        if min_value <= x[1] <= max_value
    ]
    if not nums or factor is None:
        return None

    low = digits_to_latin(text).lower()
    labels = []
    for kw in ("قیمت", "نرخ", "آخرین", "price", "value", "current"):
        pos = low.find(kw)
        if pos >= 0:
            labels.append(pos)

    if labels:
        raw = min(
            nums,
            key=lambda x: min(abs(x[0] - p) for p in labels)
        )[1]
    else:
        raw = nums[0][1]

    value = raw * factor
    if not min_value <= value <= max_value:
        return None
    return value

def _score_gold18_node(tag):
    attrs = " ".join(f"{k}={v}" for k, v in tag.attrs.items()).lower()
    text = tag.get_text(" ", strip=True)
    low = text.lower()
    score = 0
    if "geram18" in attrs:
        score += 300
    if "geram18" in low.replace(" ", ""):
        score += 250
    if "geram" in attrs and "18" in attrs:
        score += 180
    for word in ("طلای 18 عیار", "طلای ۱۸ عیار", "گرم طلای 18", "گرم طلای ۱۸"):
        if word in text:
            score += 160
    for word in ("قیمت", "نرخ", "آخرین", "ارزش", "price", "value", "current"):
        if word in low:
            score += 12
    if "ریال" in text or "تومان" in text:
        score += 30
    if len(text) > 1500:
        score -= 150
    if len(text) > 5000:
        score -= 300
    return score

# ============================================================
# TGJU GOLD18
# ============================================================

def find_gold18_value(html):
    """Find TGJU geram18 price robustly, including JSON/script markup."""
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    candidates = []

    def add_candidate(raw_value, context_text, score=0):
        try:
            raw = float(raw_value)
        except Exception:
            return
        if not (5_000_000 <= raw <= 500_000_000):
            return

        factor = _unit_factor(context_text, GOLD18_SOURCE_UNIT)
        if factor is None and GOLD18_SOURCE_UNIT == "auto":
            factor = _infer_gold18_factor(raw)
        if factor is None:
            return

        value = raw * factor
        if 5_000_000 <= value <= 100_000_000:
            candidates.append((score, value, len(context_text)))

    if GOLD18_PRICE_SELECTOR:
        try:
            for tag in soup.select(GOLD18_PRICE_SELECTOR):
                text = _context(tag, levels=5, limit=6000)
                for _, raw in _numbers_with_positions(tag.get_text(" ", strip=True)):
                    add_candidate(raw, text, 1000)
        except Exception as e:
            log.warning("Invalid GOLD18 selector: %s", e)

    # Strongly prefer elements/ancestors explicitly tied to geram18.
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
            context = _context(tag, levels=6, limit=8000)
            for _, raw in _numbers_with_positions(tag.get_text(" ", strip=True)):
                add_candidate(raw, context, 900 + _score_gold18_node(tag))

    # Scan compact rows/blocks containing geram18.
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
            add_candidate(raw, text, 500 + _score_gold18_node(tag))

    # TGJU may put the price in inline JavaScript/JSON where BeautifulSoup
    # does not expose a useful element. Search only a bounded neighborhood
    # around explicit geram18 references.
    raw_html = digits_to_latin(html)
    for m in re.finditer(r"geram18", raw_html, flags=re.I):
        lo = max(0, m.start() - 2500)
        hi = min(len(raw_html), m.end() + 5000)
        chunk = raw_html[lo:hi]
        # Price-like keys get extra score.
        key_bonus = 0
        if re.search(r"(?:price|value|current|last|close|p|v)\s*[:=]", chunk, re.I):
            key_bonus = 180
        for _, raw in _numbers_with_positions(chunk):
            add_candidate(raw, chunk, 700 + key_bonus)

    if not candidates:
        return None

    # Highest context score first; for equally relevant candidates prefer the
    # value that is in the normal Iranian 18K range.
    candidates.sort(
        key=lambda x: (x[0], 1 if 15_000_000 <= x[1] <= 50_000_000 else 0, -x[2]),
        reverse=True
    )
    value = float(candidates[0][1])
    log.info("TGJU GOLD18 parsed: %.0f Toman/gram", value)
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
            # Do not store duplicate observations from the same cached price.
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
            # Keep a bounded local history.
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
    if price is None or not (5_000_000 <= price <= 100_000_000):
        log.error("TGJU geram18 price could not be identified safely: %s", price)
        return None

    save_market_snapshot("GOLD18", "gold18", price)
    history = get_market_history("GOLD18", "gold18")
    if len(history) < GOLD_MIN_HISTORY_POINTS:
        # Return the real observations only; never manufacture candles.
        history = history if len(history) else pd.Series([price], dtype=float)
    result = ("GOLD18", history, pd.Series(dtype=float))
    GOLD18_CACHE = (now, result)
    log.info("TGJU GOLD18: %.0f Toman/gram | history=%s", price, len(history))
    return result

# ============================================================
# TGJU GLOBAL GOLD / ONS
# ============================================================

def find_xau_value(html):
    """
    Strict parser for TGJU /profile/ons.
    Expected output: USD per troy ounce.
    """
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    candidates = []

    if XAU_PRICE_SELECTOR:
        try:
            for tag in soup.select(XAU_PRICE_SELECTOR):
                text = tag.get_text(" ", strip=True)
                nums = _numbers_with_positions(text)
                for _, value in nums:
                    if 500 <= value <= 10000:
                        score = 1000
                        if "دلار" in text or "usd" in text.lower():
                            score += 100
                        candidates.append((score, value, len(text)))
        except Exception as e:
            log.warning("Invalid XAU selector: %s", e)

    selectors = [
        '[data-symbol="ons"]',
        '[data-profile="ons"]',
        '[data-code="ons"]',
        '[data-item="ons"]',
        '#ons',
        '.ons',
        '[id*="ons"]',
        '[class*="ons"]',
        '[data-symbol*="ons"]',
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
                if 500 <= value <= 10000:
                    score = 500
                    low = text.lower()
                    if "انس" in text or "اونس" in text:
                        score += 150
                    if "طلا" in text:
                        score += 100
                    if "gold" in low:
                        score += 100
                    if "دلار" in text or "usd" in low:
                        score += 80
                    candidates.append((score, value, len(text)))

    keywords = (
        "انس طلا", "انس جهانی طلا", "انس جهانی",
        "اونس طلا", "اونس جهانی", "gold", "xau"
    )

    for tag in soup.find_all(["tr","li","article","section","td","div"]):
        text = tag.get_text(" ", strip=True)
        if len(text) > 1200:
            continue

        compact = text.lower().replace(" ", "")
        if not any(k.lower().replace(" ", "") in compact for k in keywords):
            continue

        for _, value in _numbers_with_positions(text):
            if 500 <= value <= 10000:
                score = 100
                if "انس" in text or "اونس" in text:
                    score += 100
                if "طلا" in text:
                    score += 100
                if "دلار" in text or "usd" in text.lower():
                    score += 70
                candidates.append((score, value, len(text)))

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x[0], -x[2]), reverse=True)
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

    if price is None or not (500 <= price <= 10000):
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
                {
                    "ids": cid,
                    "vs_currencies": "usd",
                    "include_24hr_change": "true",
                },
                ttl=PRICE_CACHE_SECONDS
            )
            try:
                it = data[cid]
                result = {
                    "symbol": s,
                    "price": float(it["usd"]),
                    "change24": float(it.get("usd_24h_change") or 0),
                    "unit": "دلار",
                    "source": "CoinGecko",
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
# ANALYSIS
# ============================================================

def _safe_float(v, default=0.0):
    try:
        x = float(v)
        return default if pd.isna(x) else x
    except Exception:
        return default

def _rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    out = 100 - (100 / (1 + rs))
    out = out.where(avg_loss != 0, 100)
    return out

def _atr(p, period=14):
    # We only have close prices, so true range is represented by close-to-close movement.
    tr = p.diff().abs()
    return tr.ewm(alpha=1/period, adjust=False, min_periods=period).mean()

def _adx(p, period=14):
    # Close-only approximation. This is intentionally labeled as trend-strength, not OHLC ADX.
    move = p.diff()
    up = move.clip(lower=0)
    down = (-move).clip(lower=0)
    tr = move.abs()
    atr = tr.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    dip = 100 * up.ewm(alpha=1/period, adjust=False, min_periods=period).mean() / atr.replace(0, pd.NA)
    dim = 100 * down.ewm(alpha=1/period, adjust=False, min_periods=period).mean() / atr.replace(0, pd.NA)
    dx = (100 * (dip - dim).abs() / (dip + dim).replace(0, pd.NA))
    adx = dx.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    return adx, dip, dim

def _format_price(symbol, value):
    if symbol == "GOLD18":
        return f"{value:,.0f}"
    if symbol == "XAU":
        return f"{value:,.2f}"
    if abs(value) >= 1000:
        return f"{value:,.2f}"
    if abs(value) >= 1:
        return f"{value:,.4f}"
    return f"{value:,.8f}".rstrip("0").rstrip(".")

def technical_analysis(symbol, prices, volumes=None):
    p = pd.Series(prices, dtype=float).dropna().reset_index(drop=True)
    if len(p) < 50:
        return None

    ema9 = p.ewm(span=9, adjust=False).mean()
    ema21 = p.ewm(span=21, adjust=False).mean()
    ema50 = p.ewm(span=50, adjust=False).mean()
    ema200 = p.ewm(span=200, adjust=False, min_periods=50).mean()

    delta = p.diff()
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
    mid, upper, lower = _safe_float(bb_mid.iloc[-1], current), _safe_float(bb_upper.iloc[-1], current), _safe_float(bb_lower.iloc[-1], current)

    r1 = ((current / p.iloc[-2]) - 1) * 100 if p.iloc[-2] else 0
    r6 = ((current / p.iloc[-7]) - 1) * 100 if len(p) >= 7 and p.iloc[-7] else r1
    r24 = ((current / p.iloc[-25]) - 1) * 100 if len(p) >= 25 and p.iloc[-25] else r6
    r72 = ((current / p.iloc[-73]) - 1) * 100 if len(p) >= 73 and p.iloc[-73] else r24

    recent = p.tail(min(50, len(p)))
    resistance = float(recent.max())
    support = float(recent.min())
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

    # Analysis strength is descriptive and independent from signal score.
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

    # Independent SMART SIGNAL engine.
    buy_points = 0.0
    sell_points = 0.0
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
        v = pd.Series(volumes, dtype=float).dropna()
        if len(v) >= 20:
            v = v.tail(min(len(v), len(p)))
            vma = v.rolling(20).mean().iloc[-1]
            vol_confirm = bool(v.iloc[-1] >= vma * 1.10)
            if vol_confirm and r1 > 0:
                buy_points += 10; reasons_buy.append("حجم تأییدکننده")
            elif vol_confirm and r1 < 0:
                sell_points += 10; reasons_sell.append("حجم تأییدکننده")

    best = max(buy_points, sell_points)
    second = min(buy_points, sell_points)
    direction = "BUY" if buy_points > sell_points else "SELL" if sell_points > buy_points else "WAIT"
    score = float(best)
    # Require a clear edge and strong absolute score.
    candidate = direction if score >= SIGNAL_MIN_SCORE and (score - second) >= 20 else "WAIT"

    # Risk levels use ATR rather than arbitrary percentages.
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

    # Model probability is a model score, not a guaranteed statistical probability.
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
    }

def confirm_signal(symbol, candidate, price):
    if candidate == "WAIT":
        with db() as c:
            c.execute("DELETE FROM signal_state WHERE symbol=?", (symbol,))
        return "WAIT", False, 0
    now = now_iso()
    with db() as c:
        row = c.execute("SELECT * FROM signal_state WHERE symbol=?", (symbol,)).fetchone()
        if row and row["candidate"] == candidate:
            confirmations = min(SIGNAL_CONFIRMATIONS_REQUIRED, int(row["confirmations"]) + 1)
        else:
            confirmations = 1
        c.execute("""
            INSERT INTO signal_state(symbol,candidate,confirmations,last_candidate_at,last_price,updated_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(symbol) DO UPDATE SET
                candidate=excluded.candidate, confirmations=excluded.confirmations,
                last_candidate_at=excluded.last_candidate_at, last_price=excluded.last_price,
                updated_at=excluded.updated_at
        """, (symbol, candidate, confirmations, now, float(price), now))
    confirmed = confirmations >= SIGNAL_CONFIRMATIONS_REQUIRED
    return (candidate if confirmed else "WAIT"), confirmed, confirmations

async def analyze(symbol):
    s = norm_symbol(symbol)
    cached = ANALYSIS_CACHE.get(s)
    if cached and time.monotonic() - cached[0] < ANALYSIS_CACHE_SECONDS:
        return cached[1]
    data = await asset_data(s)
    if not data:
        return None
    if s in ("GOLD18", "XAU") and len(data[1]) < GOLD_MIN_HISTORY_POINTS:
        return {"symbol": s, "price": float(data[1].iloc[-1]), "insufficient_history": True, "history_points": len(data[1]), "signal": "WAIT"}
    result = technical_analysis(data[0], data[1], data[2])
    if not result:
        return None
    confirmed_signal, confirmed, confirmations = confirm_signal(s, result["signal_candidate"], result["price"])
    result["signal"] = confirmed_signal
    result["confirmed"] = confirmed
    result["confirmations"] = confirmations
    ANALYSIS_CACHE[s] = (time.monotonic(), result)
    return result

def signal_fa(s):
    return {"BUY":"🟢 خرید","SELL":"🔴 فروش","WAIT":"🟡 انتظار"}.get(s, s)

def analysis_text(a):
    if not a:
        return "❌ اطلاعات بازار در دسترس نیست."
    if a.get("insufficient_history"):
        return (f"📊 <b>تحلیل {escape(a['symbol'])}</b>\n\n"
                f"💰 قیمت فعلی: <b>{_format_price(a['symbol'], a['price'])}</b>\n"
                f"📚 تاریخچه قابل استفاده: {a['history_points']} نقطه از {GOLD_MIN_HISTORY_POINTS} نقطه لازم\n\n"
                "⏳ تحلیل تکنیکال و سیگنال پس از جمع‌آوری تاریخچه واقعی فعال می‌شود.\n"
                "❌ هیچ داده مصنوعی یا قیمت تکراری برای ساخت اندیکاتورها استفاده نمی‌شود.")
    unit = "تومان" if a["symbol"] == "GOLD18" else "دلار"
    trend_fa = {"BULLISH":"صعودی قوی","BULLISH_WEAK":"صعودی ضعیف","BEARISH":"نزولی قوی","BEARISH_WEAK":"نزولی ضعیف","NEUTRAL":"خنثی"}.get(a["trend"], a["trend"])
    reasons = a["reasons_buy"] if a["signal_candidate"] == "BUY" else a["reasons_sell"] if a["signal_candidate"] == "SELL" else []
    reasons_text = "\n".join("✅ " + escape(x) for x in reasons[:6]) or "⚪ تأیید کافی وجود ندارد"
    return (
        f"📊 <b>تحلیل تکنیکال {escape(a['symbol'])}</b>\n\n"
        f"💰 قیمت: <b>{_format_price(a['symbol'], a['price'])} {unit}</b>\n"
        f"📈 روند: <b>{trend_fa}</b>\n\n"
        f"EMA9: {_format_price(a['symbol'], a['ema9'])} | EMA21: {_format_price(a['symbol'], a['ema21'])}\n"
        f"EMA50: {_format_price(a['symbol'], a['ema50'])} | EMA200: {_format_price(a['symbol'], a['ema200'])}\n"
        f"RSI14: <b>{a['rsi']:.1f}</b> | ADX: <b>{a['adx']:.1f}</b>\n"
        f"MACD: {a['macd']:.5f} | Histogram: {a['macd_hist']:+.5f}\n"
        f"Bollinger Position: {a['bb_position']:.1f}%\n"
        f"ATR: {_format_price(a['symbol'], a['atr'])} ({a['atr_pct']:.2f}%)\n"
        f"حمایت: {_format_price(a['symbol'], a['support'])} | مقاومت: {_format_price(a['symbol'], a['resistance'])}\n\n"
        f"بازده 1 دوره: {a['r1']:+.2f}% | 6 دوره: {a['r6']:+.2f}%\n"
        f"بازده 24 دوره: {a['r24']:+.2f}% | 72 دوره: {a['r72']:+.2f}%\n\n"
        f"💪 قدرت تکنیکال: <b>{a['strength']:.0f}%</b>\n\n"
        f"🎯 <b>موتور سیگنال مستقل</b>\n"
        f"وضعیت: <b>{signal_fa(a['signal'])}</b>\n"
        f"قدرت سیگنال: <b>{a['signal_score']:.0f}%</b>\n"
        f"احتمال سود مدل: <b>{a['probability']:.0f}%</b>\n"
        f"تأیید متوالی: <b>{a['confirmations']}/{SIGNAL_CONFIRMATIONS_REQUIRED}</b>\n\n"
        f"{reasons_text}\n\n"
        + (f"💰 ورود: {_format_price(a['symbol'], a['price'])}\n🛑 حد ضرر: {_format_price(a['symbol'], a['stop'])}\n🎯 هدف 1: {_format_price(a['symbol'], a['targets'][0])}\n🎯 هدف 2: {_format_price(a['symbol'], a['targets'][1])}\n🎯 هدف 3: {_format_price(a['symbol'], a['targets'][2])}\n⚖️ R/R: 1:{a['rr']:.2f}\n\n" if a['signal'] != 'WAIT' else "\n⏳ برای ارسال BUY/SELL، تأیید کامل و متوالی لازم است.\n\n")
        + "⚠️ این خروجی مدل تحلیلی است و هیچ سیگنال بازار تضمین‌شده نیست.")

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
        has_bad_required = any(
            bool(r[3]) and r[4] is None and not bool(r[5]) and r[1] not in required | {"asset_key"}
            for r in info
        )
        if required.issubset(names) and not has_bad_required:
            # Fill legacy asset_key where present and missing.
            if "asset_key" in names:
                c.execute("UPDATE watchlist SET asset_key=upper(symbol) WHERE asset_key IS NULL OR asset_key='' ")
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

        # Copy only fields used by the current bot. This preserves existing
        # watchlist entries even if the old table had incompatible columns.
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
    """Persist a watchlist asset and verify it after commit."""
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

            # Use only the canonical columns. Legacy schema repair is handled
            # separately instead of guessing values for arbitrary constraints.
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
        log.exception("WATCHLIST INSERT ERROR (first attempt) uid=%s symbol=%s type=%s: %s", uid, s, atype, first_error)
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
    """Read the current user's persisted watchlist from the same DB path."""
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

        log.info(
            "WATCHLIST READ uid=%s count=%s db=%s",
            uid, len(rows), DB_PATH
        )
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
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def admin_kb():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 آمار", callback_data="adm:stats"),
            InlineKeyboardButton("👥 کاربران", callback_data="adm:users:0")
        ],
        [
            InlineKeyboardButton("📢 پیام به مشترکین", callback_data="adm:broadcast"),
            InlineKeyboardButton("📨 پشتیبانی", callback_data="adm:support")
        ],
        [
            InlineKeyboardButton("💬 مدیریت چت", callback_data="adm:chat"),
            InlineKeyboardButton("🚨 گزارش‌ها", callback_data="adm:reports")
        ],
        [
            InlineKeyboardButton("💳 پرداخت‌های در انتظار", callback_data="adm:payments")
        ],
        [
            InlineKeyboardButton("✉️ پیام به کاربر", callback_data="adm:message"),
            InlineKeyboardButton("🚫 مسدود/رفع", callback_data="adm:block")
        ],
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
            InlineKeyboardButton(
                label,
                callback_data=f"{prefix}:{r['asset_type']}:{r['symbol']}"
            )
        ])

    return InlineKeyboardMarkup(buttons)

# ============================================================
# BASIC
# ============================================================

async def start(update, context):
    ensure_user(update.effective_user)
    uid = update.effective_user.id

    if is_blocked(uid):
        await update.message.reply_text("🚫 دسترسی شما توسط مدیر محدود شده است.")
        return

    await update.message.reply_text(
        "سلام 👋\n\n"
        "به ربات تحلیل بازار خوش آمدید.\n"
        "رمزارزها، طلای جهانی و طلای ۱۸ عیار را جداگانه "
        "به واچ‌لیست اضافه کنید.\n\n"
        "🟡 GOLD18 و 🌍 XAU هر دو از TGJU دریافت می‌شوند.",
        reply_markup=main_kb(uid)
    )

async def help_text(update, context):
    await update.message.reply_text(
        "ℹ️ راهنما\n\n"
        "➕ افزودن دارایی: رمز‌ارز، XAU یا GOLD18\n"
        "📋 واچ‌لیست: مشاهده و حذف دارایی‌ها\n"
        "💰 قیمت لحظه‌ای: رایگان\n"
        "📊 تحلیل و 🚨 سیگنال‌ها: اشتراک فعال لازم دارد\n"
        "🔔 هشدارها: اعلان سیگنال جدید\n"
        "💬 چت رمز ارز: گفت‌وگوی مشترکین\n"
        "📨 پشتیبان: ارتباط مستقیم\n"
        "💳 خرید اشتراک: پرداخت دستی و ارسال رسید\n\n"
        "منبع GOLD18: TGJU / geram18\n"
        "منبع XAU: TGJU / ons\n\n"
        "⚠️ ربات معامله خودکار انجام نمی‌دهد."
    )

async def add_asset_prompt(update, context):
    for k in (
        "support_mode","chat_room","payment_plan",
        "admin_reply_to","admin_mode","message_target"
    ):
        context.user_data.pop(k, None)

    context.user_data["awaiting_asset"] = "add"

    await update.message.reply_text(
        "➕ <b>افزودن دارایی</b>\n\n"
        "نمونه: BTC، ZEC، ETH، XAU، GOLD18 یا طلای ۱۸ عیار\n\n"
        "برای لغو /cancel",
        parse_mode=ParseMode.HTML
    )

async def more_coins(update, context):
    context.user_data["awaiting_asset"] = "add"
    await update.message.reply_text(
        "🪙 نام یا نماد رمز ارز را ارسال کنید؛ مثال ZEC، BTC، SOL."
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
                await update.message.reply_text(
                    "⚠️ سقف واچ‌لیست پر شده است یا ذخیره انجام نشد."
                )
                return

            # Final read-back check: never tell the user that the asset was
            # added unless the same database connection can see it.
            if not user_has_asset(uid, s, at):
                await update.message.reply_text(
                    "❌ دارایی در پایگاه‌داده ذخیره نشد. لطفاً دوباره تلاش کنید."
                )
                return

            label = "طلای جهانی XAU" if s == "XAU" else "طلای ۱۸ عیار ایران"
            await update.message.reply_text(
                f"✅ <b>{label}</b> به واچ‌لیست اضافه شد.",
                parse_mode=ParseMode.HTML
            )
            return

        res = await crypto_search(text)

        if not res:
            await update.message.reply_text(
                f"❌ دارایی <b>{escape(text)}</b> پیدا نشد.",
                parse_mode=ParseMode.HTML
            )
            return

        if len(res) == 1:
            sym, _, name = res[0]

            if not add_watch(uid, sym, "crypto"):
                await update.message.reply_text("⚠️ سقف واچ‌لیست پر شده است یا ذخیره انجام نشد.")
                return

            if not user_has_asset(uid, sym, "crypto"):
                await update.message.reply_text(
                    "❌ دارایی در پایگاه‌داده ذخیره نشد. لطفاً دوباره تلاش کنید."
                )
                return

            await update.message.reply_text(
                f"✅ <b>{escape(sym)}</b> به واچ‌لیست اضافه شد.\n"
                f"نام: {escape(name)}",
                parse_mode=ParseMode.HTML
            )
            return

        buttons = []
        for sym, cid, name in res[:10]:
            buttons.append([
                InlineKeyboardButton(
                    f"{sym} — {name}",
                    callback_data=f"pick:{cid}:{norm_symbol(sym)}"
                )
            ])

        await update.message.reply_text(
            "🔎 چند دارایی پیدا شد:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    except Exception:
        log.exception("add asset")
        await update.message.reply_text("⚠️ افزودن دارایی انجام نشد.")

async def watchlist_menu(update, context):
    rows = user_assets(update.effective_user.id)

    if not rows:
        await update.message.reply_text("📋 واچ‌لیست خالی است.")
        return

    lines = []
    buttons = []

    for r in rows:
        label = r["symbol"] + " — " + (
            "طلای جهانی" if r["asset_type"] == "gold"
            else "طلای ۱۸ عیار" if r["asset_type"] == "gold18"
            else "رمز ارز"
        )

        lines.append("• " + label)
        buttons.append([
            InlineKeyboardButton(
                f"❌ حذف {r['symbol']}",
                callback_data=f"wl:del:{r['asset_type']}:{r['symbol']}"
            )
        ])

    await update.message.reply_text(
        "📋 <b>واچ‌لیست شما</b>\n\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def live_price_menu(update, context):
    rows = user_assets(update.effective_user.id)

    if not rows:
        await update.message.reply_text(
            "📋 واچ‌لیست خالی است؛ ابتدا دارایی اضافه کنید."
        )
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
        _, at, s = q.data.split(":", 2)
        s = norm_symbol(s)

        if not user_has_asset(q.from_user.id, s, at):
            await q.message.reply_text(
                "❌ این دارایی در واچ‌لیست شما نیست."
            )
            return

        item = await current_price(s)

        await q.message.reply_text(
            format_live_price(item),
            parse_mode=ParseMode.HTML
        )

    except Exception:
        log.exception("price callback")
        await q.message.reply_text("⚠️ دریافت قیمت انجام نشد.")

async def analysis_prompt(update, context):
    uid = update.effective_user.id

    if not has_analysis_access(uid):
        await update.message.reply_text(
            "🔒 تحلیل فقط برای مشترکین فعال است."
        )
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
        await update.message.reply_text(
            "🔒 سیگنال‌ها فقط برای مشترکین فعال است."
        )
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

async def growth_scan_menu(update, context):
    uid = update.effective_user.id
    if not has_analysis_access(uid):
        await update.message.reply_text("🔒 فرصت‌های رشد فقط برای مشترکین فعال است.")
        return
    await update.message.reply_text(
        "🔎 در حال اسکن بازار...\n\n"
        f"🌐 کل بازار بررسی می‌شود؛ حداکثر {MARKET_SCAN_PAGES * MARKET_SCAN_PER_PAGE:,} دارایی در هر چرخه.\n"
        "سپس روی نامزدهای برتر تحلیل تکنیکال عمیق انجام می‌شود. ⏳"
    )
    try:
        items = await growth_scan()
        if not items:
            await update.message.reply_text("❌ داده کافی از بازار دریافت نشد؛ چند دقیقه بعد دوباره تلاش کنید.")
            return
        lines=["🔎 <b>فرصت‌های رشد بازار</b>", "", f"📡 نامزدهای برتر از اسکن بازار: {len(items)}", ""]
        for i, x in enumerate(items, 1):
            rsi = f" | RSI {x['rsi']:.0f}" if x.get('rsi') is not None else ""
            lines.append(
                f"<b>{i}. {escape(x['symbol'])}</b> — {escape(x['name'])}\n"
                f"📈 امتیاز فرصت رشد: <b>{x['growth_score']:.0f}%</b> | قدرت تکنیکال: <b>{x['strength']:.0f}%</b>\n"
                f"🧠 احتمال سود مدل: <b>{x['probability']:.0f}%</b> | 24h: {x['ch24']:+.2f}% | 7d: {x['ch7']:+.2f}%{rsi}\n"
                f"💰 قیمت: ${x['price']:,.8f} | رتبه ارزش بازار: #{x['rank']}\n"
            )
        lines.append("⚠️ این رتبه‌بندی مدل تحلیلی است؛ «احتمال سود» تضمین سود یا پیش‌بینی قطعی نیست.")
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
    except Exception:
        log.exception("growth scan")
        await update.message.reply_text("⚠️ اسکن بازار کامل نشد. لاگ Railway را بررسی کنید.")

# ============================================================
# SUBSCRIPTIONS
# ============================================================

async def buy_menu(update, context):
    buttons = [[
        InlineKeyboardButton(
            f"{d} روز — {a:,} تومان",
            callback_data=f"plan:{p}"
        )
    ] for p, (d, a) in PLANS.items()]

    await update.message.reply_text(
        "💳 یکی از پلن‌ها را انتخاب کنید:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

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
        await update.message.reply_text(
            "ابتدا از «💳 خرید اشتراک» یک پلن انتخاب کنید."
        )
        return

    days, amount = PLANS[plan]
    fid = update.message.photo[-1].file_id

    with db() as c:
        cur = c.execute("""
        INSERT INTO payment_requests(
            user_id,plan,days,amount,receipt_file_id,status,created_at
        ) VALUES(?,?,?,?,?,?,?)
        """, (
            update.effective_user.id, plan, days, amount,
            fid, "pending", now_iso()
        ))
        pid = cur.lastrowid

    context.user_data.pop("payment_plan", None)

    await update.message.reply_text(
        "✅ رسید دریافت شد؛ پس از بررسی مدیر اشتراک فعال می‌شود."
    )

    for aid in ADMIN_IDS:
        try:
            await context.bot.send_photo(
                aid,
                fid,
                caption=(
                    f"💳 رسید جدید\n"
                    f"کاربر: {update.effective_user.id}\n"
                    f"پلن: {days} روز\n"
                    f"مبلغ: {amount:,} تومان\n"
                    f"شناسه: {pid}"
                ),
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "✅ تایید",
                        callback_data=f"pay:approve:{pid}"
                    ),
                    InlineKeyboardButton(
                        "❌ رد",
                        callback_data=f"pay:reject:{pid}"
                    )
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
        "📨 پیام خود را برای پشتیبان بفرستید. "
        "متن، عکس، فایل یا صدا.\n/cancel برای خروج"
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

    if update.message.photo:
        saved = "[عکس]"
        for aid in ADMIN_IDS:
            try:
                await context.bot.send_photo(
                    aid,
                    update.message.photo[-1].file_id,
                    caption=f"📨 پیام پشتیبانی\nکاربر: {uid}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton(
                            "↩️ پاسخ",
                            callback_data=f"sup:reply:{uid}"
                        )
                    ]])
                )
            except Exception:
                pass

    elif update.message.document:
        saved = "[فایل]"
        for aid in ADMIN_IDS:
            try:
                await context.bot.send_document(
                    aid,
                    update.message.document.file_id,
                    caption=f"📨 فایل از کاربر {uid}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton(
                            "↩️ پاسخ",
                            callback_data=f"sup:reply:{uid}"
                        )
                    ]])
                )
            except Exception:
                pass

    elif update.message.voice:
        saved = "[صدا]"
        for aid in ADMIN_IDS:
            try:
                await context.bot.send_voice(
                    aid,
                    update.message.voice.file_id,
                    caption=f"📨 صدا از کاربر {uid}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton(
                            "↩️ پاسخ",
                            callback_data=f"sup:reply:{uid}"
                        )
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

    await q.message.reply_text(
        f"✍️ پاسخ به کاربر {uid} را ارسال کنید."
    )

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
            """, (
                uid,
                update.effective_user.id,
                "admin_to_user",
                text,
                "closed",
                now_iso(),
                now_iso()
            ))

        await update.message.reply_text("✅ پاسخ ارسال شد.")

    except Exception as e:
        await update.message.reply_text(
            f"❌ ارسال نشد: {escape(str(e))}",
            parse_mode=ParseMode.HTML
        )

# ============================================================
# CRYPTO CHAT
# ============================================================

def chat_name(uid):
    with db() as c:
        r = c.execute(
            "SELECT first_name,username FROM users WHERE user_id=?",
            (uid,)
        ).fetchone()

    if not r:
        return "کاربر"
    if r["first_name"]:
        return r["first_name"]
    if r["username"]:
        return "@" + r["username"]
    return "کاربر"

def chat_members(symbol):
    with db() as c:
        return c.execute("""
        SELECT DISTINCT u.user_id
        FROM users u
        JOIN watchlist w ON w.user_id=u.user_id
        JOIN subscriptions s ON s.user_id=u.user_id
        WHERE u.blocked=0
        AND w.asset_type='crypto'
        AND w.symbol=?
        AND s.status='active'
        AND s.end_at>?
        """, (norm_symbol(symbol), now_iso())).fetchall()

async def crypto_chat_menu(update, context):
    uid = update.effective_user.id

    if not has_analysis_access(uid):
        await update.message.reply_text(
            "🔒 چت فقط برای مشترکین فعال است."
        )
        return

    rows = user_assets(uid, "crypto")

    if not rows:
        await update.message.reply_text(
            "ابتدا رمز ارز به واچ‌لیست اضافه کنید."
        )
        return

    await update.message.reply_text(
        "💬 رمز ارز را انتخاب کنید:",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    f"💬 {r['symbol']}",
                    callback_data=f"chat:open:{r['symbol']}"
                )
            ] for r in rows
        ])
    )

async def chat_open_callback(update, context):
    q = update.callback_query
    await q.answer()

    uid = q.from_user.id
    sym = norm_symbol(q.data.split(":", 2)[2])

    if not has_analysis_access(uid) or not user_has_asset(uid, sym, "crypto"):
        await q.message.reply_text("🔒 شما مجاز نیستید.")
        return

    context.user_data["chat_room"] = sym

    await q.message.reply_text(
        f"💬 <b>اتاق {escape(sym)}</b>\n"
        f"👥 اعضای فعال: {len(chat_members(sym))}\n"
        "پیام شما برای مشترکین همین رمز ارز ارسال می‌شود.\n"
        "/cancel برای خروج",
        parse_mode=ParseMode.HTML
    )

async def process_chat_message(update, context):
    room = context.user_data.get("chat_room")

    if not room or not update.message or not update.message.text:
        return False

    uid = update.effective_user.id

    if not has_analysis_access(uid) or not user_has_asset(uid, room, "crypto"):
        context.user_data.pop("chat_room", None)
        return False

    text = update.message.text.strip()

    if not text:
        return True

    if len(text) > 1500:
        await update.message.reply_text("❌ حداکثر ۱۵۰۰ کاراکتر.")
        return True

    with db() as c:
        cur = c.execute("""
        INSERT INTO chat_messages(
            user_id,asset_type,symbol,message,
            telegram_message_id,created_at
        ) VALUES(?,?,?,?,?,?)
        """, (
            uid, "crypto", room, text,
            update.message.message_id, now_iso()
        ))
        mid = cur.lastrowid

    sender = escape(chat_name(uid))

    for m in chat_members(room):
        if m["user_id"] == uid:
            continue
        try:
            await context.bot.send_message(
                m["user_id"],
                f"💬 <b>{sender}</b> در اتاق {escape(room)}:\n\n"
                f"{escape(text)}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "🚨 گزارش",
                        callback_data=f"chat:report:{mid}"
                    )
                ]])
            )
        except Exception:
            pass

    return True

async def chat_report_callback(update, context):
    q = update.callback_query
    await q.answer("گزارش ثبت شد.")

    mid = int(q.data.split(":")[-1])
    rid = q.from_user.id

    with db() as c:
        exists = c.execute("""
        SELECT 1 FROM chat_reports
        WHERE message_id=? AND reporter_id=? AND status='pending'
        """, (mid, rid)).fetchone()

        if not exists:
            c.execute("""
            INSERT INTO chat_reports(
                message_id,reporter_id,reason,status,created_at
            ) VALUES(?,?,?,?,?)
            """, (
                mid, rid, "گزارش کاربر",
                "pending", now_iso()
            ))

    for aid in ADMIN_IDS:
        try:
            await context.bot.send_message(
                aid,
                f"🚨 گزارش پیام چت #{mid}\n"
                f"گزارش‌دهنده: {rid}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "🗑 حذف پیام",
                        callback_data=f"chat:delete:{mid}"
                    )
                ]])
            )
        except Exception:
            pass

async def chat_admin_callback(update, context):
    q = update.callback_query
    await q.answer()

    if not is_admin(q.from_user.id):
        return

    parts = q.data.split(":")
    action = parts[1]
    target = int(parts[2])

    if action == "delete":
        with db() as c:
            c.execute("""
            UPDATE chat_messages
            SET deleted=1,deleted_by=?,deleted_at=?
            WHERE id=?
            """, (q.from_user.id, now_iso(), target))

            c.execute("""
            UPDATE chat_reports
            SET status='reviewed',reviewed_by=?,reviewed_at=?
            WHERE message_id=?
            """, (q.from_user.id, now_iso(), target))

        await q.message.reply_text("✅ پیام حذف شد.")

    elif action == "block":
        with db() as c:
            c.execute(
                "UPDATE users SET blocked=1 WHERE user_id=?",
                (target,)
            )
        await q.message.reply_text(
            f"🚫 کاربر {target} مسدود شد."
        )

# ============================================================
# BACKUP / ALERT WORKERS
# ============================================================

async def backup_worker():
    while True:
        try:
            await asyncio.to_thread(
                backup_database,
                "scheduled"
            )
        except Exception:
            log.exception("scheduled database backup")
        await asyncio.sleep(BACKUP_INTERVAL_SECONDS)

async def alerts_menu(update, context):
    uid = update.effective_user.id

    with db() as c:
        r = c.execute(
            "SELECT enabled FROM alert_preferences WHERE user_id=?",
            (uid,)
        ).fetchone()

    enabled = bool(r["enabled"]) if r else True

    await update.message.reply_text(
        "🔔 هشدار سیگنال: " + ("فعال" if enabled else "خاموش"),
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "🔕 خاموش" if enabled else "🔔 روشن",
                callback_data="alert:toggle"
            )
        ]])
    )

async def alert_callback(update, context):
    q = update.callback_query
    await q.answer()

    uid = q.from_user.id

    with db() as c:
        r = c.execute(
            "SELECT enabled FROM alert_preferences WHERE user_id=?",
            (uid,)
        ).fetchone()

        new = 0 if r and r["enabled"] else 1

        c.execute("""
        INSERT INTO alert_preferences(
            user_id,enabled,interval_seconds
        ) VALUES(?,?,?)
        ON CONFLICT(user_id)
        DO UPDATE SET enabled=excluded.enabled
        """, (uid, new, ALERT_INTERVAL_SECONDS))

    await q.message.edit_text(
        "🔔 هشدار سیگنال: " + ("فعال" if new else "خاموش")
    )

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

async def alert_worker(app):
    while True:
        try:
            with db() as c:
                rows = c.execute("""
                SELECT DISTINCT w.symbol FROM watchlist w
                JOIN alert_preferences a ON a.user_id=w.user_id
                JOIN subscriptions s ON s.user_id=w.user_id
                WHERE a.enabled=1 AND s.status='active' AND s.end_at>?
                """, (now_iso(),)).fetchall()
            symbols = [r["symbol"] for r in rows]
            analyses = {}
            if symbols:
                results = await asyncio.gather(*(analyze(s) for s in symbols), return_exceptions=True)
                for s, a in zip(symbols, results):
                    if not isinstance(a, Exception) and a:
                        analyses[s] = a

            with db() as c:
                users = c.execute("""
                SELECT DISTINCT a.user_id FROM alert_preferences a
                JOIN subscriptions s ON s.user_id=a.user_id
                JOIN watchlist w ON w.user_id=a.user_id
                WHERE a.enabled=1 AND s.status='active' AND s.end_at>?
                """, (now_iso(),)).fetchall()

            for ur in users:
                uid = ur["user_id"]
                for asset in user_assets(uid):
                    a = analyses.get(asset["symbol"])
                    if not a or a.get("signal") == "WAIT" or not a.get("confirmed"):
                        continue
                    key = f"{a['symbol']}:{a['signal']}:{a.get('confirmations',0)}:{round(a['price'],8)}"
                    with db() as c:
                        prev = c.execute("SELECT signal_key FROM alert_events WHERE user_id=? AND symbol=? ORDER BY id DESC LIMIT 1", (uid,a["symbol"])).fetchone()
                        if prev and prev["signal_key"].startswith(f"{a['symbol']}:{a['signal']}"):
                            continue
                        c.execute("INSERT INTO alert_events(user_id,symbol,signal_key,message,created_at) VALUES(?,?,?,?,?)", (uid,a["symbol"],key,analysis_text(a),now_iso()))
                    try:
                        await app.bot.send_message(uid, "🔔 <b>سیگنال تأییدشده</b>\n\n" + analysis_text(a), parse_mode=ParseMode.HTML)
                    except Exception:
                        pass
                with db() as c:
                    c.execute("UPDATE alert_preferences SET last_check_at=? WHERE user_id=?", (now_iso(),uid))
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
        users = c.execute(
            "SELECT COUNT(*) n FROM users"
        ).fetchone()["n"]

        subs = c.execute(
            "SELECT COUNT(*) n FROM subscriptions"
        ).fetchone()["n"]

        active = c.execute("""
        SELECT COUNT(*) n
        FROM subscriptions
        WHERE status='active' AND end_at>?
        """, (now_iso(),)).fetchone()["n"]

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

    path = await asyncio.to_thread(
        backup_database,
        "manual"
    )

    if path:
        await update.message.reply_text(
            f"✅ Backup ساخته شد:\n"
            f"<code>{escape(path)}</code>",
            parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text(
            "❌ ساخت Backup انجام نشد؛ لاگ Railway را بررسی کن."
        )

async def admin_panel(update, context):
    if is_admin(update.effective_user.id):
        await update.message.reply_text(
            "👨‍💼 پنل مدیریت",
            reply_markup=admin_kb()
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
            users = c.execute(
                "SELECT COUNT(*) n FROM users"
            ).fetchone()["n"]

            active = c.execute("""
            SELECT COUNT(DISTINCT user_id) n
            FROM subscriptions
            WHERE status='active' AND end_at>?
            """, (now_iso(),)).fetchone()["n"]

            pending = c.execute("""
            SELECT COUNT(*) n
            FROM payment_requests
            WHERE status='pending'
            """).fetchone()["n"]

            assets = c.execute(
                "SELECT COUNT(*) n FROM watchlist"
            ).fetchone()["n"]

        await q.message.reply_text(
            f"📊 <b>آمار</b>\n\n"
            f"👥 کاربران: {users}\n"
            f"💳 مشترک فعال: {active}\n"
            f"⏳ پرداخت: {pending}\n"
            f"🪙 دارایی: {assets}",
            parse_mode=ParseMode.HTML
        )

    elif action == "broadcast":
        context.user_data["admin_mode"] = "broadcast"
        await q.message.reply_text(
            "📢 متن پیام برای مشترکین فعال را ارسال کنید."
        )

    elif action == "message":
        context.user_data["admin_mode"] = "message_uid"
        await q.message.reply_text(
            "شناسه عددی کاربر را ارسال کنید."
        )

    elif action == "block":
        context.user_data["admin_mode"] = "block"
        await q.message.reply_text(
            "شناسه کاربر را ارسال کنید."
        )

    elif action == "payments":
        with db() as c:
            rows = c.execute("""
            SELECT * FROM payment_requests
            WHERE status='pending'
            ORDER BY id DESC LIMIT 20
            """).fetchall()

        if not rows:
            await q.message.reply_text(
                "پرداخت در انتظاری نیست."
            )
            return

        for r in rows:
            await q.message.reply_text(
                f"💳 #{r['id']}\n"
                f"کاربر: {r['user_id']}\n"
                f"پلن: {r['days']} روز\n"
                f"مبلغ: {r['amount']:,}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "✅ تایید",
                        callback_data=f"pay:approve:{r['id']}"
                    ),
                    InlineKeyboardButton(
                        "❌ رد",
                        callback_data=f"pay:reject:{r['id']}"
                    )
                ]])
            )

    elif action == "users":
        page = int(p[2]) if len(p) > 2 else 0

        with db() as c:
            rows = c.execute("""
            SELECT * FROM users
            ORDER BY created_at DESC
            LIMIT 20 OFFSET ?
            """, (page * 20,)).fetchall()

        txt = (
            "👥 <b>کاربران</b>\n\n" +
            "\n".join(
                f"{r['user_id']} | "
                f"{escape(r['first_name'] or '-')} | "
                f"{'🚫' if r['blocked'] else '✅'}"
                for r in rows
            )
        )

        await q.message.reply_text(
            txt if rows else "کاربری نیست.",
            parse_mode=ParseMode.HTML
        )

    elif action == "support":
        with db() as c:
            rows = c.execute("""
            SELECT * FROM support_messages
            WHERE direction='user_to_admin'
            ORDER BY id DESC LIMIT 20
            """).fetchall()

        if not rows:
            await q.message.reply_text(
                "پیام پشتیبانی نیست."
            )
            return

        for r in rows:
            await q.message.reply_text(
                f"📨 #{r['id']} از {r['user_id']}\n"
                f"{escape(r['message'] or '[رسانه]')}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "↩️ پاسخ",
                        callback_data=f"sup:reply:{r['user_id']}"
                    )
                ]])
            )

    elif action == "chat":
        with db() as c:
            rows = c.execute("""
            SELECT symbol,COUNT(*) n
            FROM chat_messages
            WHERE deleted=0
            GROUP BY symbol
            ORDER BY n DESC
            """).fetchall()

        await q.message.reply_text(
            "💬 <b>اتاق‌های چت</b>\n\n" +
            (
                "\n".join(
                    f"• {r['symbol']}: {r['n']} پیام"
                    for r in rows
                )
                if rows else "خالی"
            ),
            parse_mode=ParseMode.HTML
        )

    elif action == "reports":
        with db() as c:
            rows = c.execute("""
            SELECT * FROM chat_reports
            WHERE status='pending'
            ORDER BY id DESC LIMIT 20
            """).fetchall()

        await q.message.reply_text(
            "🚨 <b>گزارش‌ها</b>\n\n" +
            (
                "\n".join(
                    f"#{r['id']} پیام #{r['message_id']} "
                    f"توسط {r['reporter_id']}"
                    for r in rows
                )
                if rows else "گزارشی نیست."
            ),
            parse_mode=ParseMode.HTML
        )

async def payment_callback(update, context):
    q = update.callback_query
    await q.answer()

    if not is_admin(q.from_user.id):
        return

    _, action, pid = q.data.split(":")
    pid = int(pid)

    with db() as c:
        row = c.execute(
            "SELECT * FROM payment_requests WHERE id=?",
            (pid,)
        ).fetchone()

    if not row or row["status"] != "pending":
        await q.message.reply_text(
            "این درخواست قبلاً بررسی شده است."
        )
        return

    if action == "approve":
        add_subscription(
            row["user_id"],
            row["plan"],
            pid,
            "manual"
        )

        with db() as c:
            c.execute("""
            UPDATE payment_requests
            SET status='approved',
                reviewed_at=?,
                reviewed_by=?
            WHERE id=?
            """, (now_iso(), q.from_user.id, pid))

        try:
            await context.bot.send_message(
                row["user_id"],
                f"✅ پرداخت تایید شد.\n"
                f"اشتراک {row['days']} روزه فعال شد."
            )
        except Exception:
            pass

        await q.message.reply_text(
            "✅ اشتراک فعال شد."
        )

    else:
        with db() as c:
            c.execute("""
            UPDATE payment_requests
            SET status='rejected',
                reviewed_at=?,
                reviewed_by=?
            WHERE id=?
            """, (now_iso(), q.from_user.id, pid))

        try:
            await context.bot.send_message(
                row["user_id"],
                "❌ رسید تایید نشد؛ با پشتیبان تماس بگیرید."
            )
        except Exception:
            pass

        await q.message.reply_text(
            "❌ درخواست رد شد."
        )

async def admin_text_action(update, context, text):
    uid = update.effective_user.id
    mode = context.user_data.get("admin_mode")

    if not is_admin(uid) or not mode:
        return False

    if text == "/cancel":
        context.user_data.pop("admin_mode", None)
        await update.message.reply_text("لغو شد.")
        return True

    if mode == "broadcast":
        context.user_data.pop("admin_mode", None)

        with db() as c:
            rows = c.execute("""
            SELECT DISTINCT u.user_id
            FROM users u
            JOIN subscriptions s ON s.user_id=u.user_id
            WHERE u.blocked=0
            AND s.status='active'
            AND s.end_at>?
            """, (now_iso(),)).fetchall()

        ok = bad = 0

        for r in rows:
            try:
                await context.bot.send_message(
                    r["user_id"],
                    "📢 پیام مدیر:\n\n" + text
                )
                ok += 1
            except Exception:
                bad += 1

        await update.message.reply_text(
            f"✅ موفق: {ok}\n❌ ناموفق: {bad}"
        )
        return True

    if mode == "message_uid":
        try:
            target = int(text)
        except ValueError:
            await update.message.reply_text(
                "شناسه نامعتبر."
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
            None
        )
        context.user_data.pop("admin_mode", None)

        try:
            await context.bot.send_message(
                target,
                "📨 پیام مدیر:\n\n" + text
            )
            await update.message.reply_text(
                "✅ ارسال شد."
            )
        except Exception as e:
            await update.message.reply_text(
                f"❌ ارسال نشد: {e}"
            )
        return True

    if mode == "block":
        try:
            target = int(text)
        except ValueError:
            await update.message.reply_text(
                "شناسه نامعتبر."
            )
            return True

        with db() as c:
            r = c.execute(
                "SELECT blocked FROM users WHERE user_id=?",
                (target,)
            ).fetchone()

            if not r:
                await update.message.reply_text(
                    "کاربر پیدا نشد."
                )
                return True

            new = 0 if r["blocked"] else 1

            c.execute(
                "UPDATE users SET blocked=? WHERE user_id=?",
                (new, target)
            )

        context.user_data.pop("admin_mode", None)

        await update.message.reply_text(
            ("🚫 مسدود شد: " if new else "✅ رفع مسدودی شد: ") +
            str(target)
        )
        return True

    return False

# ============================================================
# CALLBACKS
# ============================================================

async def analysis_selector_callback(update, context):
    q = update.callback_query
    await q.answer("در حال تحلیل...")

    uid = q.from_user.id

    if not has_analysis_access(uid):
        await q.message.reply_text(
            "🔒 اشتراک فعال لازم است."
        )
        return

    try:
        _, at, s = q.data.split(":", 2)
        s = norm_symbol(s)

        if not user_has_asset(uid, s, at):
            await q.message.reply_text(
                "❌ این دارایی در واچ‌لیست نیست."
            )
            return

        a = await analyze(s)

        await q.message.reply_text(
            analysis_text(a),
            parse_mode=ParseMode.HTML
        )

    except Exception:
        log.exception("analysis selector")
        await q.message.reply_text(
            "⚠️ تحلیل در دسترس نیست."
        )

async def signal_selector_callback(update, context):
    q = update.callback_query
    await q.answer("در حال بررسی...")

    uid = q.from_user.id

    if not has_analysis_access(uid):
        await q.message.reply_text(
            "🔒 اشتراک فعال لازم است."
        )
        return

    try:
        _, at, s = q.data.split(":", 2)
        s = norm_symbol(s)

        if not user_has_asset(uid, s, at):
            await q.message.reply_text(
                "❌ این دارایی در واچ‌لیست نیست."
            )
            return

        a = await analyze(s)

        await q.message.reply_text(
            analysis_text(a),
            parse_mode=ParseMode.HTML
        )

    except Exception:
        log.exception("signal selector")
        await q.message.reply_text(
            "⚠️ سیگنال در دسترس نیست."
        )

async def misc_callback(update, context):
    q = update.callback_query
    data = q.data

    if data.startswith("pick:"):
        await q.answer()

        parts = data.split(":", 2)
        if len(parts) == 3:
            cid, s = parts[1], norm_symbol(parts[2])
        else:
            cid, s = "", norm_symbol(parts[1])

        if not s or (not cid and s not in COINS and s not in ("XAU", "GOLD18")):
            await q.message.reply_text("❌ دارایی نامعتبر.")
            return

        if not add_watch(q.from_user.id, s, asset_type(s), asset_key=cid or s):
            await q.message.reply_text(
                "⚠️ سقف واچ‌لیست پر شده."
            )
            return

        if not user_has_asset(q.from_user.id, s, asset_type(s)):
            await q.message.reply_text("❌ ذخیره در واچ‌لیست تأیید نشد؛ دوباره تلاش کنید.")
            return

        await q.message.reply_text(
            f"✅ <b>{escape(s)}</b> به واچ‌لیست اضافه شد.",
            parse_mode=ParseMode.HTML
        )

    elif data.startswith("wl:del:"):
        await q.answer("حذف شد")

        try:
            _, _, at, s = data.split(":", 3)
            remove_watch(
                q.from_user.id,
                s,
                at
            )
            await q.message.edit_text(
                f"✅ {escape(s)} حذف شد.",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            await q.message.reply_text(
                "⚠️ حذف انجام نشد."
            )

# ============================================================
# TEXT / MEDIA ROUTERS
# ============================================================

async def text_router(update, context):
    if not update.message:
        return

    ensure_user(update.effective_user)
    uid = update.effective_user.id
    text = (update.message.text or "").strip()

    if is_blocked(uid) and not is_admin(uid):
        await update.message.reply_text(
            "🚫 دسترسی شما محدود شده است."
        )
        return

    if text == "/cancel":
        for k in (
            "support_mode","chat_room","awaiting_asset",
            "payment_plan","admin_mode",
            "admin_reply_to","message_target"
        ):
            context.user_data.pop(k, None)

        await update.message.reply_text(
            "لغو شد.",
            reply_markup=main_kb(uid)
        )
        return

    handlers = {
        "➕ افزودن دارایی": add_asset_prompt,
        "📋 واچ‌لیست": watchlist_menu,
        "💰 قیمت لحظه‌ای": live_price_menu,
        "📊 تحلیل": analysis_prompt,
        "🚨 سیگنال‌ها": signals_menu,
        "🔎 فرصت‌های رشد": growth_scan_menu,
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
        for k in (
            "support_mode","chat_room","awaiting_asset",
            "payment_plan","admin_reply_to","message_target"
        ):
            context.user_data.pop(k, None)

        await handlers[text](update, context)
        return

    if await admin_text_action(
        update,
        context,
        text
    ):
        return

    if is_admin(uid) and context.user_data.get("admin_reply_to"):
        await send_support_reply(
            update,
            context,
            text
        )
        return

    if context.user_data.get("support_mode"):
        context.user_data.pop("support_mode", None)

        await save_support(
            uid,
            text,
            update.message.message_id
        )

        for aid in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    aid,
                    f"📨 پیام پشتیبانی از {uid}:\n\n"
                    f"{escape(text)}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton(
                            "↩️ پاسخ",
                            callback_data=f"sup:reply:{uid}"
                        )
                    ]])
                )
            except Exception:
                pass

        await update.message.reply_text(
            "✅ پیام برای پشتیبان ارسال شد."
        )
        return

    if await process_chat_message(
        update,
        context
    ):
        return

    awaiting = context.user_data.get(
        "awaiting_asset"
    )

    if awaiting:
        context.user_data.pop(
            "awaiting_asset",
            None
        )
        await process_add_asset(
            update,
            context,
            text
        )
        return

    s = norm_symbol(text)

    if s in COINS or s in ("XAU", "GOLD18"):
        await process_add_asset(
            update,
            context,
            text
        )
        return

    await update.message.reply_text(
        "❓ دستور یا نماد شناخته نشد.\n\n"
        "نمونه: BTC، ZEC، XAU، GOLD18"
    )

async def media_router(update, context):
    ensure_user(update.effective_user)
    uid = update.effective_user.id

    if is_blocked(uid) and not is_admin(uid):
        return

    if context.user_data.get("support_mode"):
        await support_media(update, context)
        return

    if update.message.photo:
        await receipt_photo(update, context)
        return

    await update.message.reply_text(
        "برای ارسال این نوع پیام ابتدا پشتیبانی را انتخاب کنید."
    )

# ============================================================
# LIFECYCLE / MAIN
# ============================================================

async def post_init(app):
    init_db()

    try:
        await app.bot.delete_webhook(
            drop_pending_updates=False
        )
    except Exception as e:
        log.warning(
            "delete webhook: %s",
            e
        )

    app.create_task(
        alert_worker(app)
    )

    app.create_task(
        backup_worker()
    )

    app.create_task(
        market_snapshot_worker()
    )

    try:
        await asyncio.to_thread(
            backup_database,
            "startup"
        )
    except Exception:
        log.exception(
            "startup database backup"
        )

    d = database_diagnostics()

    log.info(
        "FAST Market Analyzer started | "
        "DB=%s | exists=%s | size=%s | persistent=%s | admins=%s",
        d["path"],
        d["exists"],
        d["size"],
        d["persistent_path"],
        sorted(ADMIN_IDS)
    )

    if not d["persistent_path"]:
        log.error(
            "DATABASE IS NOT ON RAILWAY PERSISTENT VOLUME. "
            "Set DB_PATH=/data/crypto_bot.db "
            "and attach a Volume at /data."
        )

async def post_shutdown(app):
    global HTTP_SESSION

    if HTTP_SESSION and not HTTP_SESSION.closed:
        await HTTP_SESSION.close()

def main():
    if not BOT_TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is not set"
        )

    init_db()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(
        CommandHandler("start", start)
    )

    app.add_handler(
        CommandHandler("cancel", text_router)
    )

    app.add_handler(
        CommandHandler("dbstatus", db_status_command)
    )

    app.add_handler(
        CommandHandler("backup", backup_command)
    )

    app.add_handler(
        CallbackQueryHandler(
            plan_callback,
            pattern=r"^plan:"
        )
    )

    app.add_handler(
        CallbackQueryHandler(
            payment_callback,
            pattern=r"^pay:"
        )
    )

    app.add_handler(
        CallbackQueryHandler(
            support_reply_callback,
            pattern=r"^sup:reply:"
        )
    )

    app.add_handler(
        CallbackQueryHandler(
            chat_open_callback,
            pattern=r"^chat:open:"
        )
    )

    app.add_handler(
        CallbackQueryHandler(
            chat_report_callback,
            pattern=r"^chat:report:\d+$"
        )
    )

    app.add_handler(
        CallbackQueryHandler(
            chat_admin_callback,
            pattern=r"^chat:(delete|block):\d+$"
        )
    )

    app.add_handler(
        CallbackQueryHandler(
            alert_callback,
            pattern=r"^alert:"
        )
    )

    app.add_handler(
        CallbackQueryHandler(
            admin_callback,
            pattern=r"^adm:"
        )
    )

    app.add_handler(
        CallbackQueryHandler(
            selected_price_callback,
            pattern=r"^price:"
        )
    )

    app.add_handler(
        CallbackQueryHandler(
            analysis_selector_callback,
            pattern=r"^analysis:"
        )
    )

    app.add_handler(
        CallbackQueryHandler(
            signal_selector_callback,
            pattern=r"^signal:"
        )
    )

    app.add_handler(
        CallbackQueryHandler(
            misc_callback,
            pattern=r"^(pick:|wl:del:)"
        )
    )

    app.add_handler(
        MessageHandler(
            filters.PHOTO |
            filters.Document.ALL |
            filters.VOICE,
            media_router
        )
    )

    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            text_router
        )
    )

    log.info("Starting polling...")

    app.run_polling(
        drop_pending_updates=False,
        allowed_updates=Update.ALL_TYPES
    )

if __name__ == "__main__":
    main()
