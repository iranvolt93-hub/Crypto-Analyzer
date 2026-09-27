# -*- coding: utf-8 -*-
"""
Crypto / Global Gold / Iran 18K Gold Telegram Analyzer
FAST + SAFE Railway production build

Analysis + signals + notifications. No automatic trading.

Required:
    TELEGRAM_BOT_TOKEN

Recommended:
    ADMIN_IDS=123456789,987654321
    PAYMENT_CARD=6037...
    SUPPORT_USERNAME=@username
    DB_PATH=/data/crypto_bot.db

Start:
    python main.py

IMPORTANT:
- Keep ONE running instance for this bot token.
- Never delete /data/crypto_bot.db.
- Database migrations are additive.
- Prices are cached to reduce API load.
- Iran 18K gold is NEVER guessed from arbitrary numbers on TGJU.
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
HTTP_TIMEOUT = max(5, int(os.getenv("HTTP_TIMEOUT", "12")))
CACHE_SECONDS = max(10, int(os.getenv("CACHE_SECONDS", "45")))
PRICE_CACHE_SECONDS = max(10, int(os.getenv("PRICE_CACHE_SECONDS", "20")))
ANALYSIS_CACHE_SECONDS = max(15, int(os.getenv("ANALYSIS_CACHE_SECONDS", "45")))
ALERT_INTERVAL_SECONDS = max(60, int(os.getenv("ALERT_INTERVAL_SECONDS", "300")))
MAX_WATCHLIST = max(1, int(os.getenv("MAX_WATCHLIST", "100")))

PAYMENT_CARD = os.getenv("PAYMENT_CARD", "ثبت نشده").strip()
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "پشتیبان").strip()

# TGJU is used only for the domestic 18K price.
GOLD18_URL = "https://www.tgju.org/profile/geram18"

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
CACHE_LOCK = asyncio.Lock()

MAIN_MENU = [
    ["➕ افزودن دارایی", "📋 واچ‌لیست"],
    ["💰 قیمت لحظه‌ای", "📊 تحلیل"],
    ["🚨 سیگنال‌ها", "💳 خرید اشتراک"],
    ["👤 وضعیت اشتراک", "🔔 هشدارها"],
    ["🪙 ارزهای بیشتر", "💬 چت رمز ارز"],
    ["📨 ارتباط با پشتیبان", "ℹ️ راهنما"],
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
# DATABASE
# ============================================================

def _prepare_persistent_db():
    """
    Railway-safe database bootstrap.
    The real database lives on /data when DB_PATH is not overridden.
    If an older database exists in the application directory and the
    persistent database does not exist yet, migrate it once.
    """
    target = Path(DB_PATH)
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists():
        return

    # Rescue databases from older builds that used a relative path.
    candidates = [
        Path("/app/crypto_bot.db"),
        Path("/app/data/crypto_bot.db"),
        Path("crypto_bot.db"),
    ]
    for src in candidates:
        try:
            if src.exists() and src.resolve() != target.resolve():
                shutil.copy2(src, target)
                log.warning("Migrated old database %s -> %s", src, target)
                return
        except Exception as e:
            log.warning("Database migration candidate failed %s: %s", src, e)

def database_diagnostics():
    try:
        p=Path(DB_PATH)
        size=p.stat().st_size if p.exists() else 0
        return {
            "path": str(p),
            "exists": p.exists(),
            "size": size,
            "persistent_path": str(p).startswith("/data/"),
        }
    except Exception:
        return {"path":str(DB_PATH),"exists":False,"size":0,"persistent_path":False}

def db():
    _prepare_persistent_db()
    folder = os.path.dirname(DB_PATH)
    if folder:
        os.makedirs(folder, exist_ok=True)
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
        log.info("DB migration: %s.%s", table, column)

def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS app_meta(key TEXT PRIMARY KEY,value TEXT);

        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT,
            created_at TEXT NOT NULL, last_seen TEXT NOT NULL,
            blocked INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS subscriptions(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            plan TEXT NOT NULL, days INTEGER NOT NULL, amount INTEGER NOT NULL,
            start_at TEXT NOT NULL, end_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active', source TEXT DEFAULT 'manual',
            payment_request_id INTEGER, created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS payment_requests(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            plan TEXT NOT NULL, days INTEGER NOT NULL, amount INTEGER NOT NULL,
            receipt_file_id TEXT, status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL, reviewed_at TEXT, reviewed_by INTEGER
        );

        CREATE TABLE IF NOT EXISTS watchlist(
            user_id INTEGER NOT NULL, symbol TEXT NOT NULL,
            asset_type TEXT NOT NULL, created_at TEXT NOT NULL,
            PRIMARY KEY(user_id,symbol,asset_type)
        );

        CREATE TABLE IF NOT EXISTS alert_preferences(
            user_id INTEGER PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 1,
            interval_seconds INTEGER NOT NULL DEFAULT 300, last_check_at TEXT
        );

        CREATE TABLE IF NOT EXISTS alert_events(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            symbol TEXT NOT NULL, signal_key TEXT NOT NULL,
            message TEXT NOT NULL, created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS support_messages(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            admin_id INTEGER, direction TEXT NOT NULL, message TEXT,
            telegram_message_id INTEGER, status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT NOT NULL, replied_at TEXT
        );

        CREATE TABLE IF NOT EXISTS chat_messages(
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            asset_type TEXT NOT NULL, symbol TEXT NOT NULL, message TEXT NOT NULL,
            telegram_message_id INTEGER, created_at TEXT NOT NULL,
            deleted INTEGER NOT NULL DEFAULT 0, deleted_by INTEGER, deleted_at TEXT
        );

        CREATE TABLE IF NOT EXISTS chat_reports(
            id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER NOT NULL,
            reporter_id INTEGER NOT NULL, reason TEXT,
            status TEXT NOT NULL DEFAULT 'pending', created_at TEXT NOT NULL,
            reviewed_by INTEGER, reviewed_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_sub_user_end ON subscriptions(user_id,end_at);
        CREATE INDEX IF NOT EXISTS idx_pay_status ON payment_requests(status);
        CREATE INDEX IF NOT EXISTS idx_watch_asset ON watchlist(asset_type,symbol);
        CREATE INDEX IF NOT EXISTS idx_chat_room ON chat_messages(asset_type,symbol,created_at);
        CREATE INDEX IF NOT EXISTS idx_chat_reports ON chat_reports(status);
        CREATE INDEX IF NOT EXISTS idx_alert_events ON alert_events(user_id,symbol,created_at);
        """)
        ensure_column(c, "users", "blocked", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(c, "users", "last_seen", "TEXT")
        ensure_column(c, "alert_events", "signal_key", "TEXT DEFAULT ''")
        ensure_column(c, "support_messages", "replied_at", "TEXT")
        ensure_column(c, "chat_messages", "deleted_by", "INTEGER")
        ensure_column(c, "chat_messages", "deleted_at", "TEXT")
        c.execute("INSERT OR REPLACE INTO app_meta(key,value) VALUES('schema_version','6')")
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
        username=excluded.username, first_name=excluded.first_name,
        last_seen=excluded.last_seen
        """, (user.id,user.username or "",user.first_name or "",n,n))
        c.execute("""
        INSERT OR IGNORE INTO alert_preferences(user_id,enabled,interval_seconds)
        VALUES(?,?,?)
        """, (user.id,1,ALERT_INTERVAL_SECONDS))

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
        """, (uid,n))
        return c.execute("""
        SELECT * FROM subscriptions
        WHERE user_id=? AND status='active' AND end_at>?
        ORDER BY end_at DESC LIMIT 1
        """, (uid,n)).fetchone()

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
        """, (uid,now_iso()))
        c.execute("""
        INSERT INTO subscriptions(
        user_id,plan,days,amount,start_at,end_at,status,source,
        payment_request_id,created_at)
        VALUES(?,?,?,?,?,?,?,?,?,?)
        """, (uid,plan,days,amount,start.isoformat(),end.isoformat(),
              "active",source,payment_request_id,now_iso()))
    return True

def format_dt(v):
    try:
        return datetime.fromisoformat(v).astimezone().strftime("%Y/%m/%d %H:%M")
    except Exception:
        return str(v)

# ============================================================
# HTTP + CACHE
# ============================================================

async def get_session():
    global HTTP_SESSION
    if HTTP_SESSION is None or HTTP_SESSION.closed:
        HTTP_SESSION = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
            connector=aiohttp.TCPConnector(limit=30, ttl_dns_cache=300),
            headers={"User-Agent":"Mozilla/5.0 MarketAnalyzerBot/5.0"},
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
    key = ("TEXT",url)
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
    value = value.replace(" ","").replace("‌","").replace("_","")
    aliases = {
        "GOLD":"XAU","GOLDUSD":"XAU","XAUUSD":"XAU","XAU/USD":"XAU",
        "XAU/USDT":"XAU","XAUUSDT":"XAU","طلا":"XAU","طلایجهانی":"XAU",
        "GERAM18":"GOLD18","GERAM18K":"GOLD18","18K":"GOLD18",
        "GOLD18K":"GOLD18","IRANGOLD":"GOLD18","طلای18":"GOLD18",
        "طلای۱۸":"GOLD18","طلای۱۸عیار":"GOLD18","طلایداخلی":"GOLD18",
    }
    if value in aliases:
        return aliases[value]
    for suffix in ("USDT","USD"):
        if value.endswith(suffix) and len(value) > len(suffix):
            base = value[:-len(suffix)].replace("/","").replace("-","")
            if base in COINS:
                return base
    return value

def asset_type(symbol):
    s = norm_symbol(symbol)
    return "gold" if s=="XAU" else "gold18" if s=="GOLD18" else "crypto"

async def crypto_search(query):
    query = (query or "").strip()
    direct = norm_symbol(query)
    if direct in COINS:
        return [(direct,COINS[direct],direct)]
    data = await http_json(
        "https://api.coingecko.com/api/v3/search",
        {"query":query}, ttl=60
    )
    out=[]; seen=set()
    for coin in (data or {}).get("coins",[])[:15]:
        s=norm_symbol(coin.get("symbol") or "")
        cid=coin.get("id"); name=coin.get("name") or s
        if not s or not cid or (s,cid) in seen: continue
        seen.add((s,cid)); out.append((s,cid,name))
    return out

async def crypto_data(symbol):
    s=norm_symbol(symbol); cid=COINS.get(s)
    if not cid:
        res=await crypto_search(s)
        if not res: return None
        s,cid,_=res[0]
    data=await http_json(
        f"https://api.coingecko.com/api/v3/coins/{cid}/market_chart",
        {"vs_currency":"usd","days":"2","interval":"hourly"},
        ttl=CACHE_SECONDS
    )
    if not data or not data.get("prices"): return None
    prices=pd.Series([float(x[1]) for x in data["prices"]],dtype=float)
    vols=pd.Series([float(x[1]) for x in data.get("total_volumes",[])],dtype=float)
    return s,prices,vols

async def xau_data():
    for ticker in ("XAUUSD=X","GC=F"):
        data=await http_json(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
            {"range":"5d","interval":"1h"}, ttl=CACHE_SECONDS
        )
        try:
            result=data["chart"]["result"][0]
            vals=[float(v) for v in result["indicators"]["quote"][0]["close"] if v is not None]
            if len(vals)>10:
                return "XAU",pd.Series(vals,dtype=float),pd.Series(dtype=float)
        except Exception:
            continue
    return None

# ============================================================
# SAFE TGJU 18K PRICE
# ============================================================

def digits_to_latin(s):
    return (s or "").translate(str.maketrans(
        "۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩",
        "01234567890123456789"
    ))

def number_candidates(text):
    text=digits_to_latin(text or "")
    text=text.replace("٬",",").replace("\u00a0"," ")
    out=[]
    for m in re.finditer(
        r"(?<!\d)(\d{1,3}(?:[,\s]\d{3})+|\d{5,12})(?!\d)", text
    ):
        raw=m.group(1).replace(",","").replace(" ","")
        try:
            v=float(raw)
            if 100_000 <= v <= 500_000_000:
                out.append(v)
        except ValueError:
            pass
    return out

def _gold18_unit_factor(text):
    """
    Return conversion from source value to Toman.
    Explicit unit wins. If no unit is visible, return None.
    We deliberately do NOT assume a divisor.
    """
    t=digits_to_latin(text or "").replace(" ","")
    if "تومان" in t:
        return 1.0
    if "ریال" in t:
        return 0.1
    return None

def _score_gold18_node(tag):
    attrs=" ".join(
        f"{k}={v}" for k,v in tag.attrs.items()
    ).lower()
    text=tag.get_text(" ",strip=True)
    low=text.lower()

    score=0
    if "geram18" in attrs: score += 100
    if "geram18" in low.replace(" ",""): score += 90
    if "geram" in attrs and "18" in attrs: score += 70

    for word in ("طلای 18 عیار","طلای ۱۸ عیار","گرم طلای 18","گرم طلای ۱۸"):
        if word in text: score += 80

    for word in ("قیمت","نرخ","آخرین","ارزش","price","value","current"):
        if word in low: score += 8

    if "ریال" in text: score += 5
    if "تومان" in text: score += 5

    # Penalize large containers. A page-wide div is not a price cell.
    if len(text) > 1500: score -= 100
    if len(text) > 5000: score -= 200
    return score

def _candidate_from_node(tag):
    text=tag.get_text(" ",strip=True)
    vals=number_candidates(text)
    if not vals:
        return None

    factor=_gold18_unit_factor(text)

    # Prefer a value next to explicit price/current labels.
    # If the node contains several values, choose the one nearest a price label.
    if len(vals) == 1:
        return vals[0] * factor if factor is not None else vals[0]

    low=digits_to_latin(text).lower()
    positions=[]
    for kw in ("قیمت","نرخ","آخرین","price","value","current"):
        pos=low.find(kw)
        if pos >= 0:
            positions.append(pos)

    if positions:
        # Re-parse number positions and choose the closest number to a label.
        raw=digits_to_latin(text).replace("٬",",")
        nums=[]
        for m in re.finditer(r"(?<!\d)(\d{1,3}(?:[,\s]\d{3})+|\d{5,12})(?!\d)",raw):
            try:
                nums.append((m.start(),float(m.group(1).replace(",","").replace(" ",""))))
            except Exception:
                pass
        if nums:
            best=min(nums,key=lambda x:min(abs(x[0]-p) for p in positions))
            value=best[1]
            return value*factor if factor is not None else value

    return vals[0] * factor if factor is not None else vals[0]

def find_gold18_value(html):
    """
    Conservative TGJU parser.

    It only accepts a value from a DOM element that is explicitly tied to
    geram18 / 18K gold. It does NOT scan the whole page and pick a random
    six-to-twelve digit number.

    If the source does not expose an identifiable value, return None.
    """
    if not html:
        return None

    soup=BeautifulSoup(html,"html.parser")
    candidates=[]

    # Strong selectors first.
    selectors=[
        '[data-symbol="geram18"]',
        '[data-profile="geram18"]',
        '[data-code="geram18"]',
        '[data-item="geram18"]',
        '#geram18',
        '.geram18',
        '[id*="geram18"]',
        '[class*="geram18"]',
        '[data-field*="geram18"]',
    ]

    seen=set()
    for selector in selectors:
        try:
            nodes=soup.select(selector)
        except Exception:
            nodes=[]
        for tag in nodes:
            if id(tag) in seen:
                continue
            seen.add(id(tag))
            score=_score_gold18_node(tag)
            value=_candidate_from_node(tag)
            if value is not None:
                candidates.append((score,value,len(tag.get_text(" ",strip=True))))

    if candidates:
        # Strong identity score always beats a merely textual match.
        candidates.sort(key=lambda x:(x[0],-x[2]),reverse=True)
        return candidates[0][1]

    # Attribute scan: only attributes whose value explicitly contains geram18.
    for tag in soup.find_all(True):
        attrs=" ".join(str(v).lower() for v in tag.attrs.values())
        if "geram18" not in attrs:
            continue
        score=_score_gold18_node(tag)+40
        value=_candidate_from_node(tag)
        if value is not None:
            candidates.append((score,value,len(tag.get_text(" ",strip=True))))

    if candidates:
        candidates.sort(key=lambda x:(x[0],-x[2]),reverse=True)
        return candidates[0][1]

    # Local text blocks only; never body/html/page-wide text.
    keywords=(
        "geram18","گرم طلای 18","گرم طلای ۱۸",
        "طلای 18 عیار","طلای ۱۸ عیار","طلای18","طلای۱۸"
    )
    for tag in soup.find_all(["tr","li","article","section","td","div"]):
        text=tag.get_text(" ",strip=True)
        compact=text.lower().replace(" ","")
        if len(text)>1000:
            continue
        if not any(k.lower().replace(" ","") in compact for k in keywords):
            continue
        value=_candidate_from_node(tag)
        if value is not None:
            score=_score_gold18_node(tag)
            candidates.append((score,value,len(text)))

    if candidates:
        candidates.sort(key=lambda x:(x[0],-x[2]),reverse=True)
        return candidates[0][1]

    return None

async def gold18_data():
    global GOLD18_CACHE
    now=time.monotonic()
    if GOLD18_CACHE and now-GOLD18_CACHE[0] < PRICE_CACHE_SECONDS:
        return GOLD18_CACHE[1]

    html=await http_text(GOLD18_URL, ttl=PRICE_CACHE_SECONDS)
    if not html:
        log.error("TGJU geram18 page unavailable")
        return None

    price=find_gold18_value(html)

    # Never display an arbitrary number as the gold price.
    if price is None or not (100_000 <= float(price) <= 500_000_000):
        log.error("TGJU geram18 price could not be identified safely")
        return None

    price=float(price)
    result=("GOLD18",pd.Series([price]*30,dtype=float),pd.Series(dtype=float))
    GOLD18_CACHE=(now,result)
    log.info("GOLD18 safe price parsed: %.0f Toman",price)
    return result

async def asset_data(symbol):
    s=norm_symbol(symbol)
    if s=="XAU": return await xau_data()
    if s=="GOLD18": return await gold18_data()
    return await crypto_data(s)

async def current_price(symbol):
    s=norm_symbol(symbol)
    key=s
    item=PRICE_CACHE.get(key)
    if item and time.monotonic()-item[0] < PRICE_CACHE_SECONDS:
        return item[1]

    result=None
    if s=="GOLD18":
        data=await gold18_data()
        if data:
            result={"symbol":"GOLD18","price":float(data[1].iloc[-1]),
                    "unit":"تومان برای هر گرم طلای ۱۸ عیار ایران",
                    "source":"TGJU"}
    elif s=="XAU":
        for ticker in ("XAUUSD=X","GC=F"):
            data=await http_json(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
                {"range":"1d","interval":"5m"}, ttl=PRICE_CACHE_SECONDS
            )
            try:
                r=data["chart"]["result"][0]
                value=r.get("meta",{}).get("regularMarketPrice")
                if value is None:
                    vals=[float(v) for v in r["indicators"]["quote"][0]["close"] if v is not None]
                    value=vals[-1] if vals else None
                if value is not None:
                    result={"symbol":"XAU","price":float(value),
                            "unit":"دلار برای هر اونس"}
                    break
            except Exception:
                pass
    else:
        cid=COINS.get(s)
        if not cid:
            res=await crypto_search(s)
            if res: s,cid,_=res[0]
        if cid:
            data=await http_json(
                "https://api.coingecko.com/api/v3/simple/price",
                {"ids":cid,"vs_currencies":"usd","include_24hr_change":"true"},
                ttl=PRICE_CACHE_SECONDS
            )
            try:
                it=data[cid]
                result={"symbol":s,"price":float(it["usd"]),
                        "change24":float(it.get("usd_24h_change") or 0),
                        "unit":"دلار"}
            except Exception:
                pass

    if result:
        PRICE_CACHE[key]=(time.monotonic(),result)
    return result

def format_live_price(item):
    if not item:
        return "❌ قیمت در حال حاضر در دسترس نیست."
    s=escape(item["symbol"]); p=item["price"]
    if item["symbol"]=="GOLD18":
        value=f"{p:,.0f}"
    elif item["symbol"]=="XAU":
        value=f"{p:,.2f}"
    else:
        value=f"{p:,.8f}".rstrip("0").rstrip(".")
    text=f"💰 <b>{s}</b>\nقیمت فعلی: <b>{value}</b>\nواحد: {escape(item['unit'])}"
    if item.get("source"):
        text+=f"\nمنبع: {escape(item['source'])}"
    if "change24" in item:
        text+=f"\nتغییر ۲۴ ساعت: <b>{item['change24']:+.2f}%</b>"
    return text


# ============================================================
# ANALYSIS
# ============================================================

def analysis_from_series(symbol, prices, volumes=None):
    p=pd.Series(prices,dtype=float).dropna()
    if len(p)<5: return None
    ema9=p.ewm(span=9,adjust=False).mean().iloc[-1]
    ema21=p.ewm(span=21,adjust=False).mean().iloc[-1]
    delta=p.diff()
    gain=delta.clip(lower=0).rolling(14).mean()
    loss=(-delta.clip(upper=0)).rolling(14).mean()
    rs=gain/loss.replace(0,pd.NA)
    try: rsi=float((100-(100/(1+rs))).iloc[-1])
    except Exception: rsi=50.0
    if pd.isna(rsi): rsi=50.0

    r1=((p.iloc[-1]/p.iloc[-2])-1)*100 if len(p)>=2 and p.iloc[-2] else 0
    r6=((p.iloc[-1]/p.iloc[-7])-1)*100 if len(p)>=7 and p.iloc[-7] else 0
    r24=((p.iloc[-1]/p.iloc[-25])-1)*100 if len(p)>=25 and p.iloc[-25] else r6

    score=0
    score+=2 if ema9>ema21 else -2
    score+=2 if rsi>52 else -2 if rsi<48 else 0
    score+=1 if r1>0 else -1 if r1<0 else 0
    score+=1 if r6>0 else -1 if r6<0 else 0
    score=max(-6,min(6,score))
    signal="BUY" if score>=3 else "SELL" if score<=-3 else "WAIT"
    strength=min(99,50+abs(score)*8)
    probability=min(95,max(5,50+score*7))
    return {
        "symbol":symbol,"price":float(p.iloc[-1]),"ema9":float(ema9),
        "ema21":float(ema21),"rsi":float(rsi),"r1":float(r1),
        "r6":float(r6),"r24":float(r24),"score":int(score),
        "signal":signal,"strength":float(strength),
        "probability":float(probability),
    }

async def analyze(symbol):
    s=norm_symbol(symbol)
    cached=ANALYSIS_CACHE.get(s)
    if cached and time.monotonic()-cached[0] < ANALYSIS_CACHE_SECONDS:
        return cached[1]
    data=await asset_data(s)
    result=analysis_from_series(data[0],data[1],data[2]) if data else None
    if result:
        ANALYSIS_CACHE[s]=(time.monotonic(),result)
    return result

def signal_fa(s):
    return {"BUY":"🟢 خرید","SELL":"🔴 فروش","WAIT":"🟡 انتظار"}.get(s,s)

def analysis_text(a):
    if not a: return "❌ اطلاعات بازار در دسترس نیست."
    unit=" تومان" if a["symbol"]=="GOLD18" else ""
    return (
        f"📊 <b>تحلیل {escape(a['symbol'])}</b>\n\n"
        f"💰 قیمت: <b>{a['price']:,.4f}{unit}</b>\n"
        f"📈 EMA9: {a['ema9']:,.4f}\n"
        f"📉 EMA21: {a['ema21']:,.4f}\n"
        f"RSI14: <b>{a['rsi']:.1f}</b>\n"
        f"بازده کوتاه‌مدت: {a['r1']:+.2f}%\n"
        f"بازده ۶ دوره: {a['r6']:+.2f}%\n"
        f"بازده ۲۴ دوره: {a['r24']:+.2f}%\n\n"
        f"🎯 سیگنال: <b>{signal_fa(a['signal'])}</b>\n"
        f"💪 درصد قدرت: <b>{a['strength']:.0f}%</b>\n"
        f"🎲 احتمال سود: <b>{a['probability']:.0f}%</b>\n\n"
        "⚠️ این تحلیل آموزشی است و تضمین سود نیست."
    )

# ============================================================
# WATCHLIST
# ============================================================

def add_watch(uid,symbol,atype):
    s=norm_symbol(symbol)
    with db() as c:
        n=c.execute("SELECT COUNT(*) n FROM watchlist WHERE user_id=?",(uid,)).fetchone()["n"]
        exists=c.execute("""
        SELECT 1 FROM watchlist WHERE user_id=? AND symbol=? AND asset_type=?
        """,(uid,s,atype)).fetchone()
        if n>=MAX_WATCHLIST and not exists: return False
        c.execute("""
        INSERT OR IGNORE INTO watchlist(user_id,symbol,asset_type,created_at)
        VALUES(?,?,?,?)
        """,(uid,s,atype,now_iso()))
    return True

def remove_watch(uid,symbol,atype):
    with db() as c:
        c.execute("""
        DELETE FROM watchlist WHERE user_id=? AND symbol=? AND asset_type=?
        """,(uid,norm_symbol(symbol),atype))

def user_assets(uid,atype=None):
    with db() as c:
        if atype:
            return c.execute("""
            SELECT * FROM watchlist WHERE user_id=? AND asset_type=?
            ORDER BY created_at
            """,(uid,atype)).fetchall()
        return c.execute("""
        SELECT * FROM watchlist WHERE user_id=? ORDER BY created_at
        """,(uid,)).fetchall()

def user_has_asset(uid,symbol,atype):
    with db() as c:
        return c.execute("""
        SELECT 1 FROM watchlist
        WHERE user_id=? AND symbol=? AND asset_type=?
        """,(uid,norm_symbol(symbol),atype)).fetchone() is not None

# ============================================================
# KEYBOARDS / MENUS
# ============================================================

def main_kb(uid):
    rows=[r[:] for r in MAIN_MENU]
    if not is_admin(uid):
        rows=[r for r in rows if r!=["👨‍💼 پنل مدیریت"]]
    return ReplyKeyboardMarkup(rows,resize_keyboard=True)

def admin_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 آمار",callback_data="adm:stats"),
         InlineKeyboardButton("👥 کاربران",callback_data="adm:users:0")],
        [InlineKeyboardButton("📢 پیام به مشترکین",callback_data="adm:broadcast"),
         InlineKeyboardButton("📨 پشتیبانی",callback_data="adm:support")],
        [InlineKeyboardButton("💬 مدیریت چت",callback_data="adm:chat"),
         InlineKeyboardButton("🚨 گزارش‌ها",callback_data="adm:reports")],
        [InlineKeyboardButton("💳 پرداخت‌های در انتظار",callback_data="adm:payments")],
        [InlineKeyboardButton("✉️ پیام به کاربر",callback_data="adm:message"),
         InlineKeyboardButton("🚫 مسدود/رفع",callback_data="adm:block")],
    ])

def watchlist_selector_keyboard(rows,prefix):
    buttons=[]
    for r in rows:
        label=r["symbol"]
        if r["asset_type"]=="gold": label+=" — طلای جهانی"
        elif r["asset_type"]=="gold18": label+=" — طلای ۱۸ عیار"
        buttons.append([InlineKeyboardButton(
            label,callback_data=f"{prefix}:{r['asset_type']}:{r['symbol']}"
        )])
    return InlineKeyboardMarkup(buttons)

# ============================================================
# BASIC
# ============================================================

async def start(update,context):
    ensure_user(update.effective_user)
    uid=update.effective_user.id
    if is_blocked(uid):
        await update.message.reply_text("🚫 دسترسی شما توسط مدیر محدود شده است.")
        return
    await update.message.reply_text(
        "سلام 👋\n\nبه ربات تحلیل بازار خوش آمدید.\n"
        "رمزارزها، طلای جهانی و طلای ۱۸ عیار را جداگانه به واچ‌لیست اضافه کنید.",
        reply_markup=main_kb(uid)
    )

async def help_text(update,context):
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
        "⚠️ ربات معامله خودکار انجام نمی‌دهد."
    )

async def add_asset_prompt(update,context):
    for k in ("support_mode","chat_room","payment_plan","admin_reply_to","admin_mode","message_target"):
        context.user_data.pop(k,None)
    context.user_data["awaiting_asset"]="add"
    await update.message.reply_text(
        "➕ <b>افزودن دارایی</b>\n\n"
        "نمونه: BTC، ZEC، ETH، XAU، GOLD18 یا طلای ۱۸ عیار\n\nبرای لغو /cancel",
        parse_mode=ParseMode.HTML
    )

async def more_coins(update,context):
    context.user_data["awaiting_asset"]="add"
    await update.message.reply_text("🪙 نام یا نماد رمز ارز را ارسال کنید؛ مثال ZEC، BTC، SOL.")

async def process_add_asset(update,context,text):
    uid=update.effective_user.id
    text=(text or "").strip()
    if not text:
        await update.message.reply_text("❌ نماد خالی است."); return
    try:
        s=norm_symbol(text)
        if s in ("XAU","GOLD18"):
            at=asset_type(s)
            if not add_watch(uid,s,at):
                await update.message.reply_text("⚠️ سقف واچ‌لیست پر شده است."); return
            label="طلای جهانی XAU" if s=="XAU" else "طلای ۱۸ عیار ایران"
            await update.message.reply_text(f"✅ <b>{label}</b> به واچ‌لیست اضافه شد.",parse_mode=ParseMode.HTML)
            return
        res=await crypto_search(text)
        if not res:
            await update.message.reply_text(f"❌ دارایی <b>{escape(text)}</b> پیدا نشد.",parse_mode=ParseMode.HTML); return
        if len(res)==1:
            sym,_,name=res[0]
            if not add_watch(uid,sym,"crypto"):
                await update.message.reply_text("⚠️ سقف واچ‌لیست پر شده است."); return
            await update.message.reply_text(
                f"✅ <b>{escape(sym)}</b> به واچ‌لیست اضافه شد.\nنام: {escape(name)}",
                parse_mode=ParseMode.HTML
            ); return
        buttons=[]
        for sym,_,name in res[:10]:
            buttons.append([InlineKeyboardButton(f"{sym} — {name}",callback_data=f"pick:{norm_symbol(sym)}")])
        await update.message.reply_text("🔎 چند دارایی پیدا شد:",reply_markup=InlineKeyboardMarkup(buttons))
    except Exception:
        log.exception("add asset")
        await update.message.reply_text("⚠️ افزودن دارایی انجام نشد.")

async def watchlist_menu(update,context):
    rows=user_assets(update.effective_user.id)
    if not rows:
        await update.message.reply_text("📋 واچ‌لیست خالی است."); return
    lines=[]
    buttons=[]
    for r in rows:
        label=r["symbol"]+" — "+(
            "طلای جهانی" if r["asset_type"]=="gold" else
            "طلای ۱۸ عیار" if r["asset_type"]=="gold18" else "رمز ارز"
        )
        lines.append("• "+label)
        buttons.append([InlineKeyboardButton(f"❌ حذف {r['symbol']}",
            callback_data=f"wl:del:{r['asset_type']}:{r['symbol']}")])
    await update.message.reply_text(
        "📋 <b>واچ‌لیست شما</b>\n\n"+"\n".join(lines),
        parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup(buttons)
    )

async def live_price_menu(update,context):
    rows=user_assets(update.effective_user.id)
    if not rows:
        await update.message.reply_text("📋 واچ‌لیست خالی است؛ ابتدا دارایی اضافه کنید."); return
    buttons=[[InlineKeyboardButton(f"💰 {r['symbol']}",
        callback_data=f"price:{r['asset_type']}:{r['symbol']}")] for r in rows]
    await update.message.reply_text(
        "💰 <b>قیمت لحظه‌ای</b>\nرایگان و بدون نیاز به اشتراک:",
        parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup(buttons)
    )

async def selected_price_callback(update,context):
    q=update.callback_query
    await q.answer("دریافت قیمت...")
    try:
        _,at,s=q.data.split(":",2); s=norm_symbol(s)
        if not user_has_asset(q.from_user.id,s,at):
            await q.message.reply_text("❌ این دارایی در واچ‌لیست شما نیست."); return
        item=await current_price(s)
        await q.message.reply_text(format_live_price(item),parse_mode=ParseMode.HTML)
    except Exception:
        log.exception("price callback")
        await q.message.reply_text("⚠️ دریافت قیمت انجام نشد.")

async def analysis_prompt(update,context):
    uid=update.effective_user.id
    if not has_analysis_access(uid):
        await update.message.reply_text("🔒 تحلیل فقط برای مشترکین فعال است."); return
    rows=user_assets(uid)
    if not rows:
        await update.message.reply_text("📋 واچ‌لیست خالی است."); return
    await update.message.reply_text(
        "📊 <b>انتخاب دارایی برای تحلیل</b>",parse_mode=ParseMode.HTML,
        reply_markup=watchlist_selector_keyboard(rows,"analysis")
    )

async def signals_menu(update,context):
    uid=update.effective_user.id
    if not has_analysis_access(uid):
        await update.message.reply_text("🔒 سیگنال‌ها فقط برای مشترکین فعال است."); return
    rows=user_assets(uid)
    if not rows:
        await update.message.reply_text("📋 واچ‌لیست خالی است."); return
    await update.message.reply_text(
        "🚨 <b>انتخاب دارایی برای سیگنال</b>",parse_mode=ParseMode.HTML,
        reply_markup=watchlist_selector_keyboard(rows,"signal")
    )

# ============================================================
# SUBSCRIPTIONS
# ============================================================

async def buy_menu(update,context):
    buttons=[[InlineKeyboardButton(f"{d} روز — {a:,} تومان",callback_data=f"plan:{p}")]
             for p,(d,a) in PLANS.items()]
    await update.message.reply_text("💳 یکی از پلن‌ها را انتخاب کنید:",
        reply_markup=InlineKeyboardMarkup(buttons))

async def status_menu(update,context):
    s=active_subscription(update.effective_user.id)
    if not s:
        await update.message.reply_text("👤 اشتراک فعال ندارید."); return
    await update.message.reply_text(
        "👤 <b>وضعیت اشتراک</b>\n\n"
        f"پلن: {s['days']} روز\nشروع: {format_dt(s['start_at'])}\n"
        f"پایان: {format_dt(s['end_at'])}\nوضعیت: 🟢 فعال",
        parse_mode=ParseMode.HTML
    )

async def plan_callback(update,context):
    q=update.callback_query; await q.answer()
    plan=q.data.split(":",1)[1]
    if plan not in PLANS:
        await q.message.reply_text("❌ پلن نامعتبر است."); return
    days,amount=PLANS[plan]
    context.user_data["payment_plan"]=plan
    await q.message.reply_text(
        f"💳 <b>پلن {days} روزه</b>\n\nمبلغ: <b>{amount:,} تومان</b>\n\n"
        f"شماره کارت:\n<code>{escape(PAYMENT_CARD)}</code>\n\n"
        "پس از پرداخت تصویر رسید را همین‌جا ارسال کنید.",
        parse_mode=ParseMode.HTML
    )

async def receipt_photo(update,context):
    if context.user_data.get("support_mode"):
        await support_media(update,context); return
    plan=context.user_data.get("payment_plan")
    if plan not in PLANS:
        await update.message.reply_text("ابتدا از «💳 خرید اشتراک» یک پلن انتخاب کنید."); return
    days,amount=PLANS[plan]
    fid=update.message.photo[-1].file_id
    with db() as c:
        cur=c.execute("""
        INSERT INTO payment_requests(user_id,plan,days,amount,receipt_file_id,status,created_at)
        VALUES(?,?,?,?,?,?,?)
        """,(update.effective_user.id,plan,days,amount,fid,"pending",now_iso()))
        pid=cur.lastrowid
    context.user_data.pop("payment_plan",None)
    await update.message.reply_text("✅ رسید دریافت شد؛ پس از بررسی مدیر اشتراک فعال می‌شود.")
    for aid in ADMIN_IDS:
        try:
            await context.bot.send_photo(
                aid,fid,
                caption=f"💳 رسید جدید\nکاربر: {update.effective_user.id}\nپلن: {days} روز\nمبلغ: {amount:,} تومان\nشناسه: {pid}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ تایید",callback_data=f"pay:approve:{pid}"),
                    InlineKeyboardButton("❌ رد",callback_data=f"pay:reject:{pid}")
                ]])
            )
        except Exception: log.exception("payment notify")

# ============================================================
# SUPPORT
# ============================================================

async def support_prompt(update,context):
    context.user_data["support_mode"]=True
    await update.message.reply_text("📨 پیام خود را برای پشتیبان بفرستید. متن، عکس، فایل یا صدا.\n/cancel برای خروج")

async def save_support(uid,message,mid):
    with db() as c:
        c.execute("""
        INSERT INTO support_messages(user_id,direction,message,telegram_message_id,created_at)
        VALUES(?,?,?,?,?)
        """,(uid,"user_to_admin",message,mid,now_iso()))

async def support_media(update,context):
    uid=update.effective_user.id
    if update.message.photo:
        saved="[عکس]"
        for aid in ADMIN_IDS:
            try:
                await context.bot.send_photo(aid,update.message.photo[-1].file_id,
                    caption=f"📨 پیام پشتیبانی\nکاربر: {uid}",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ پاسخ",callback_data=f"sup:reply:{uid}")]]))
            except Exception: pass
    elif update.message.document:
        saved="[فایل]"
        for aid in ADMIN_IDS:
            try:
                await context.bot.send_document(aid,update.message.document.file_id,
                    caption=f"📨 فایل از کاربر {uid}",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ پاسخ",callback_data=f"sup:reply:{uid}")]]))
            except Exception: pass
    elif update.message.voice:
        saved="[صدا]"
        for aid in ADMIN_IDS:
            try:
                await context.bot.send_voice(aid,update.message.voice.file_id,
                    caption=f"📨 صدا از کاربر {uid}",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ پاسخ",callback_data=f"sup:reply:{uid}")]]))
            except Exception: pass
    else: return
    await save_support(uid,saved,update.message.message_id)
    await update.message.reply_text("✅ پیام شما برای پشتیبان ارسال شد.")

async def support_reply_callback(update,context):
    q=update.callback_query; await q.answer()
    if not is_admin(q.from_user.id): return
    uid=int(q.data.split(":")[-1])
    context.user_data["admin_reply_to"]=uid
    await q.message.reply_text(f"✍️ پاسخ به کاربر {uid} را ارسال کنید.")

async def send_support_reply(update,context,text):
    uid=context.user_data.pop("admin_reply_to",None)
    if not uid: return
    try:
        await context.bot.send_message(uid,f"📨 <b>پاسخ پشتیبان</b>\n\n{escape(text)}",parse_mode=ParseMode.HTML)
        with db() as c:
            c.execute("""
            INSERT INTO support_messages(user_id,admin_id,direction,message,status,created_at,replied_at)
            VALUES(?,?,?,?,?,?,?)
            """,(uid,update.effective_user.id,"admin_to_user",text,"closed",now_iso(),now_iso()))
        await update.message.reply_text("✅ پاسخ ارسال شد.")
    except Exception as e:
        await update.message.reply_text(f"❌ ارسال نشد: {escape(str(e))}",parse_mode=ParseMode.HTML)

# ============================================================
# CRYPTO CHAT
# ============================================================

def chat_name(uid):
    with db() as c:
        r=c.execute("SELECT first_name,username FROM users WHERE user_id=?",(uid,)).fetchone()
    if not r: return "کاربر"
    if r["first_name"]: return r["first_name"]
    if r["username"]: return "@"+r["username"]
    return "کاربر"

def chat_members(symbol):
    with db() as c:
        return c.execute("""
        SELECT DISTINCT u.user_id FROM users u
        JOIN watchlist w ON w.user_id=u.user_id
        JOIN subscriptions s ON s.user_id=u.user_id
        WHERE u.blocked=0 AND w.asset_type='crypto' AND w.symbol=?
        AND s.status='active' AND s.end_at>?
        """,(norm_symbol(symbol),now_iso())).fetchall()

async def crypto_chat_menu(update,context):
    uid=update.effective_user.id
    if not has_analysis_access(uid):
        await update.message.reply_text("🔒 چت فقط برای مشترکین فعال است."); return
    rows=user_assets(uid,"crypto")
    if not rows:
        await update.message.reply_text("ابتدا رمز ارز به واچ‌لیست اضافه کنید."); return
    await update.message.reply_text("💬 رمز ارز را انتخاب کنید:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton(f"💬 {r['symbol']}",callback_data=f"chat:open:{r['symbol']}")]
            for r in rows
        ]))

async def chat_open_callback(update,context):
    q=update.callback_query; await q.answer()
    uid=q.from_user.id; sym=norm_symbol(q.data.split(":",2)[2])
    if not has_analysis_access(uid) or not user_has_asset(uid,sym,"crypto"):
        await q.message.reply_text("🔒 شما مجاز نیستید."); return
    context.user_data["chat_room"]=sym
    await q.message.reply_text(
        f"💬 <b>اتاق {escape(sym)}</b>\n👥 اعضای فعال: {len(chat_members(sym))}\n"
        "پیام شما برای مشترکین همین رمز ارز ارسال می‌شود.\n/cancel برای خروج",
        parse_mode=ParseMode.HTML
    )

async def process_chat_message(update,context):
    room=context.user_data.get("chat_room")
    if not room or not update.message or not update.message.text: return False
    uid=update.effective_user.id
    if not has_analysis_access(uid) or not user_has_asset(uid,room,"crypto"):
        context.user_data.pop("chat_room",None); return False
    text=update.message.text.strip()
    if not text: return True
    if len(text)>1500:
        await update.message.reply_text("❌ حداکثر ۱۵۰۰ کاراکتر."); return True
    with db() as c:
        cur=c.execute("""
        INSERT INTO chat_messages(user_id,asset_type,symbol,message,telegram_message_id,created_at)
        VALUES(?,?,?,?,?,?)
        """,(uid,"crypto",room,text,update.message.message_id,now_iso()))
        mid=cur.lastrowid
    sender=escape(chat_name(uid))
    for m in chat_members(room):
        if m["user_id"]==uid: continue
        try:
            await context.bot.send_message(m["user_id"],
                f"💬 <b>{sender}</b> در اتاق {escape(room)}:\n\n{escape(text)}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🚨 گزارش",callback_data=f"chat:report:{mid}")
                ]]))
        except Exception: pass
    return True

async def chat_report_callback(update,context):
    q=update.callback_query; await q.answer("گزارش ثبت شد.")
    mid=int(q.data.split(":")[-1]); rid=q.from_user.id
    with db() as c:
        exists=c.execute("SELECT 1 FROM chat_reports WHERE message_id=? AND reporter_id=? AND status='pending'",
                         (mid,rid)).fetchone()
        if not exists:
            c.execute("""
            INSERT INTO chat_reports(message_id,reporter_id,reason,status,created_at)
            VALUES(?,?,?,?,?)
            """,(mid,rid,"گزارش کاربر","pending",now_iso()))
    for aid in ADMIN_IDS:
        try:
            await context.bot.send_message(aid,f"🚨 گزارش پیام چت #{mid}\nگزارش‌دهنده: {rid}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🗑 حذف پیام",callback_data=f"chat:delete:{mid}")
                ]]))
        except Exception: pass

async def chat_admin_callback(update,context):
    q=update.callback_query; await q.answer()
    if not is_admin(q.from_user.id): return
    parts=q.data.split(":"); action=parts[1]; target=int(parts[2])
    if action=="delete":
        with db() as c:
            row=c.execute("SELECT * FROM chat_messages WHERE id=?",(target,)).fetchone()
            c.execute("UPDATE chat_messages SET deleted=1,deleted_by=?,deleted_at=? WHERE id=?",
                      (q.from_user.id,now_iso(),target))
            c.execute("UPDATE chat_reports SET status='reviewed',reviewed_by=?,reviewed_at=? WHERE message_id=?",
                      (q.from_user.id,now_iso(),target))
        await q.message.reply_text("✅ پیام حذف شد.")
    elif action=="block":
        with db() as c: c.execute("UPDATE users SET blocked=1 WHERE user_id=?",(target,))
        await q.message.reply_text(f"🚫 کاربر {target} مسدود شد.")

# ============================================================
# ALERTS - FAST GLOBAL ANALYSIS CACHE
# ============================================================

async def alerts_menu(update,context):
    uid=update.effective_user.id
    with db() as c:
        r=c.execute("SELECT enabled FROM alert_preferences WHERE user_id=?",(uid,)).fetchone()
    enabled=bool(r["enabled"]) if r else True
    await update.message.reply_text(
        "🔔 هشدار سیگنال: "+("فعال" if enabled else "خاموش"),
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔕 خاموش" if enabled else "🔔 روشن",callback_data="alert:toggle")
        ]])
    )

async def alert_callback(update,context):
    q=update.callback_query; await q.answer()
    uid=q.from_user.id
    with db() as c:
        r=c.execute("SELECT enabled FROM alert_preferences WHERE user_id=?",(uid,)).fetchone()
        new=0 if r and r["enabled"] else 1
        c.execute("""
        INSERT INTO alert_preferences(user_id,enabled,interval_seconds)
        VALUES(?,?,?) ON CONFLICT(user_id) DO UPDATE SET enabled=excluded.enabled
        """,(uid,new,ALERT_INTERVAL_SECONDS))
    await q.message.edit_text("🔔 هشدار سیگنال: "+("فعال" if new else "خاموش"))

async def alert_worker(app):
    while True:
        try:
            # Unique symbols: one API/analysis calculation per symbol,
            # regardless of how many users watch that symbol.
            with db() as c:
                rows=c.execute("""
                SELECT DISTINCT w.symbol
                FROM watchlist w
                JOIN alert_preferences a ON a.user_id=w.user_id
                JOIN subscriptions s ON s.user_id=w.user_id
                WHERE a.enabled=1 AND s.status='active' AND s.end_at>?
                """,(now_iso(),)).fetchall()
            symbols=[r["symbol"] for r in rows]
            analyses={}
            if symbols:
                results=await asyncio.gather(
                    *(analyze(s) for s in symbols),return_exceptions=True
                )
                for s,a in zip(symbols,results):
                    if isinstance(a,Exception): continue
                    if a: analyses[s]=a

            with db() as c:
                users=c.execute("""
                SELECT DISTINCT a.user_id
                FROM alert_preferences a
                JOIN subscriptions s ON s.user_id=a.user_id
                JOIN watchlist w ON w.user_id=a.user_id
                WHERE a.enabled=1 AND s.status='active' AND s.end_at>?
                """,(now_iso(),)).fetchall()

            for ur in users:
                uid=ur["user_id"]
                assets=user_assets(uid)
                for asset in assets:
                    a=analyses.get(asset["symbol"])
                    if not a or a["signal"]=="WAIT": continue
                    key=f"{a['symbol']}:{a['signal']}"
                    with db() as c:
                        prev=c.execute("""
                        SELECT signal_key FROM alert_events
                        WHERE user_id=? AND symbol=? ORDER BY id DESC LIMIT 1
                        """,(uid,a["symbol"])).fetchone()
                        if prev and prev["signal_key"]==key: continue
                        c.execute("""
                        INSERT INTO alert_events(user_id,symbol,signal_key,message,created_at)
                        VALUES(?,?,?,?,?)
                        """,(uid,a["symbol"],key,analysis_text(a),now_iso()))
                    try:
                        await app.bot.send_message(uid,"🔔 <b>سیگنال جدید</b>\n\n"+analysis_text(a),
                                                   parse_mode=ParseMode.HTML)
                    except Exception: pass
                with db() as c:
                    c.execute("UPDATE alert_preferences SET last_check_at=? WHERE user_id=?",(now_iso(),uid))
        except Exception:
            log.exception("alert worker")
        await asyncio.sleep(ALERT_INTERVAL_SECONDS)

# ============================================================
# ADMIN
# ============================================================

async def db_status_command(update,context):
    if not is_admin(update.effective_user.id):
        return
    d=database_diagnostics()
    with db() as c:
        users=c.execute("SELECT COUNT(*) n FROM users").fetchone()["n"]
        subs=c.execute("SELECT COUNT(*) n FROM subscriptions").fetchone()["n"]
        active=c.execute(
            "SELECT COUNT(*) n FROM subscriptions WHERE status='active' AND end_at>?",
            (now_iso(),)
        ).fetchone()["n"]
    await update.message.reply_text(
        "🗄 <b>وضعیت دیتابیس</b>\n\n"
        f"مسیر: <code>{escape(d['path'])}</code>\n"
        f"وجود فایل: {'✅' if d['exists'] else '❌'}\n"
        f"حجم: {d['size']:,} بایت\n"
        f"مسیر پایدار /data: {'✅' if d['persistent_path'] else '❌'}\n"
        f"کاربران: {users}\n"
        f"رکورد اشتراک: {subs}\n"
        f"اشتراک فعال: {active}\n\n"
        "اگر مسیر پایدار ❌ است، در Railway یک Volume با Mount Path = /data بساز "
        "و DB_PATH=/data/crypto_bot.db تنظیم کن.",
        parse_mode=ParseMode.HTML
    )

async def admin_panel(update,context):
    if is_admin(update.effective_user.id):
        await update.message.reply_text("👨‍💼 پنل مدیریت",reply_markup=admin_kb())

async def admin_callback(update,context):
    q=update.callback_query; await q.answer()
    if not is_admin(q.from_user.id): return
    p=q.data.split(":"); action=p[1]
    if action=="stats":
        with db() as c:
            users=c.execute("SELECT COUNT(*) n FROM users").fetchone()["n"]
            active=c.execute("SELECT COUNT(DISTINCT user_id) n FROM subscriptions WHERE status='active' AND end_at>?",(now_iso(),)).fetchone()["n"]
            pending=c.execute("SELECT COUNT(*) n FROM payment_requests WHERE status='pending'").fetchone()["n"]
            assets=c.execute("SELECT COUNT(*) n FROM watchlist").fetchone()["n"]
        await q.message.reply_text(f"📊 <b>آمار</b>\n\n👥 کاربران: {users}\n💳 مشترک فعال: {active}\n⏳ پرداخت: {pending}\n🪙 دارایی: {assets}",parse_mode=ParseMode.HTML)
    elif action=="broadcast":
        context.user_data["admin_mode"]="broadcast"; await q.message.reply_text("📢 متن پیام برای مشترکین فعال را ارسال کنید.")
    elif action=="message":
        context.user_data["admin_mode"]="message_uid"; await q.message.reply_text("شناسه عددی کاربر را ارسال کنید.")
    elif action=="block":
        context.user_data["admin_mode"]="block"; await q.message.reply_text("شناسه کاربر را ارسال کنید.")
    elif action=="payments":
        with db() as c: rows=c.execute("SELECT * FROM payment_requests WHERE status='pending' ORDER BY id DESC LIMIT 20").fetchall()
        if not rows:
            await q.message.reply_text("پرداخت در انتظاری نیست."); return
        for r in rows:
            await q.message.reply_text(
                f"💳 #{r['id']}\nکاربر: {r['user_id']}\nپلن: {r['days']} روز\nمبلغ: {r['amount']:,}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ تایید",callback_data=f"pay:approve:{r['id']}"),
                    InlineKeyboardButton("❌ رد",callback_data=f"pay:reject:{r['id']}")
                ]]))
    elif action=="users":
        page=int(p[2]) if len(p)>2 else 0
        with db() as c: rows=c.execute("SELECT * FROM users ORDER BY created_at DESC LIMIT 20 OFFSET ?",(page*20,)).fetchall()
        txt="👥 <b>کاربران</b>\n\n"+"\n".join(f"{r['user_id']} | {escape(r['first_name'] or '-')} | {'🚫' if r['blocked'] else '✅'}" for r in rows)
        await q.message.reply_text(txt if rows else "کاربری نیست.",parse_mode=ParseMode.HTML)
    elif action=="support":
        with db() as c: rows=c.execute("SELECT * FROM support_messages WHERE direction='user_to_admin' ORDER BY id DESC LIMIT 20").fetchall()
        if not rows: await q.message.reply_text("پیام پشتیبانی نیست."); return
        for r in rows:
            await q.message.reply_text(f"📨 #{r['id']} از {r['user_id']}\n{escape(r['message'] or '[رسانه]')}",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ پاسخ",callback_data=f"sup:reply:{r['user_id']}")]]))
    elif action=="chat":
        with db() as c: rows=c.execute("SELECT symbol,COUNT(*) n FROM chat_messages WHERE deleted=0 GROUP BY symbol ORDER BY n DESC").fetchall()
        await q.message.reply_text("💬 <b>اتاق‌های چت</b>\n\n"+("\n".join(f"• {r['symbol']}: {r['n']} پیام" for r in rows) if rows else "خالی"),parse_mode=ParseMode.HTML)
    elif action=="reports":
        with db() as c: rows=c.execute("SELECT * FROM chat_reports WHERE status='pending' ORDER BY id DESC LIMIT 20").fetchall()
        await q.message.reply_text("🚨 <b>گزارش‌ها</b>\n\n"+("\n".join(f"#{r['id']} پیام #{r['message_id']} توسط {r['reporter_id']}" for r in rows) if rows else "گزارشی نیست."),parse_mode=ParseMode.HTML)

async def payment_callback(update,context):
    q=update.callback_query; await q.answer()
    if not is_admin(q.from_user.id): return
    _,action,pid=q.data.split(":"); pid=int(pid)
    with db() as c: row=c.execute("SELECT * FROM payment_requests WHERE id=?",(pid,)).fetchone()
    if not row or row["status"]!="pending":
        await q.message.reply_text("این درخواست قبلاً بررسی شده است."); return
    if action=="approve":
        add_subscription(row["user_id"],row["plan"],pid,"manual")
        with db() as c: c.execute("UPDATE payment_requests SET status='approved',reviewed_at=?,reviewed_by=? WHERE id=?",(now_iso(),q.from_user.id,pid))
        try: await context.bot.send_message(row["user_id"],f"✅ پرداخت تایید شد.\nاشتراک {row['days']} روزه فعال شد.")
        except Exception: pass
        await q.message.reply_text("✅ اشتراک فعال شد.")
    else:
        with db() as c: c.execute("UPDATE payment_requests SET status='rejected',reviewed_at=?,reviewed_by=? WHERE id=?",(now_iso(),q.from_user.id,pid))
        try: await context.bot.send_message(row["user_id"],"❌ رسید تایید نشد؛ با پشتیبان تماس بگیرید.")
        except Exception: pass
        await q.message.reply_text("❌ درخواست رد شد.")

async def admin_text_action(update,context,text):
    uid=update.effective_user.id; mode=context.user_data.get("admin_mode")
    if not is_admin(uid) or not mode: return False
    if text=="/cancel":
        context.user_data.pop("admin_mode",None); await update.message.reply_text("لغو شد."); return True
    if mode=="broadcast":
        context.user_data.pop("admin_mode",None)
        with db() as c: rows=c.execute("""
        SELECT DISTINCT u.user_id FROM users u JOIN subscriptions s ON s.user_id=u.user_id
        WHERE u.blocked=0 AND s.status='active' AND s.end_at>?
        """,(now_iso(),)).fetchall()
        ok=bad=0
        for r in rows:
            try: await context.bot.send_message(r["user_id"],"📢 پیام مدیر:\n\n"+text); ok+=1
            except Exception: bad+=1
        await update.message.reply_text(f"✅ موفق: {ok}\n❌ ناموفق: {bad}"); return True
    if mode=="message_uid":
        try: target=int(text)
        except ValueError: await update.message.reply_text("شناسه نامعتبر."); return True
        context.user_data["message_target"]=target; context.user_data["admin_mode"]="message_text"
        await update.message.reply_text("متن پیام را ارسال کنید."); return True
    if mode=="message_text":
        target=context.user_data.pop("message_target",None); context.user_data.pop("admin_mode",None)
        try: await context.bot.send_message(target,"📨 پیام مدیر:\n\n"+text); await update.message.reply_text("✅ ارسال شد.")
        except Exception as e: await update.message.reply_text(f"❌ ارسال نشد: {e}")
        return True
    if mode=="block":
        try: target=int(text)
        except ValueError: await update.message.reply_text("شناسه نامعتبر."); return True
        with db() as c:
            r=c.execute("SELECT blocked FROM users WHERE user_id=?",(target,)).fetchone()
            if not r: await update.message.reply_text("کاربر پیدا نشد."); return True
            new=0 if r["blocked"] else 1
            c.execute("UPDATE users SET blocked=? WHERE user_id=?",(new,target))
        context.user_data.pop("admin_mode",None)
        await update.message.reply_text(("🚫 مسدود شد: " if new else "✅ رفع مسدودی شد: ")+str(target)); return True
    return False

# ============================================================
# CALLBACKS
# ============================================================

async def analysis_selector_callback(update,context):
    q=update.callback_query; await q.answer("در حال تحلیل...")
    uid=q.from_user.id
    if not has_analysis_access(uid):
        await q.message.reply_text("🔒 اشتراک فعال لازم است."); return
    try:
        _,at,s=q.data.split(":",2); s=norm_symbol(s)
        if not user_has_asset(uid,s,at): await q.message.reply_text("❌ این دارایی در واچ‌لیست نیست."); return
        a=await analyze(s)
        await q.message.reply_text(analysis_text(a),parse_mode=ParseMode.HTML)
    except Exception:
        log.exception("analysis selector"); await q.message.reply_text("⚠️ تحلیل در دسترس نیست.")

async def signal_selector_callback(update,context):
    q=update.callback_query; await q.answer("در حال بررسی...")
    uid=q.from_user.id
    if not has_analysis_access(uid):
        await q.message.reply_text("🔒 اشتراک فعال لازم است."); return
    try:
        _,at,s=q.data.split(":",2); s=norm_symbol(s)
        if not user_has_asset(uid,s,at): await q.message.reply_text("❌ این دارایی در واچ‌لیست نیست."); return
        a=await analyze(s)
        await q.message.reply_text(analysis_text(a),parse_mode=ParseMode.HTML)
    except Exception:
        log.exception("signal selector"); await q.message.reply_text("⚠️ سیگنال در دسترس نیست.")

async def misc_callback(update,context):
    q=update.callback_query; data=q.data
    if data.startswith("pick:"):
        await q.answer()
        s=norm_symbol(data.split(":",1)[1])
        if s not in COINS and s not in ("XAU","GOLD18"):
            await q.message.reply_text("❌ دارایی نامعتبر."); return
        if not add_watch(q.from_user.id,s,asset_type(s)):
            await q.message.reply_text("⚠️ سقف واچ‌لیست پر شده."); return
        await q.message.reply_text(f"✅ <b>{escape(s)}</b> به واچ‌لیست اضافه شد.",parse_mode=ParseMode.HTML)
    elif data.startswith("wl:del:"):
        await q.answer("حذف شد")
        try:
            _,_,at,s=data.split(":",3); remove_watch(q.from_user.id,s,at)
            await q.message.edit_text(f"✅ {escape(s)} حذف شد.",parse_mode=ParseMode.HTML)
        except Exception:
            await q.message.reply_text("⚠️ حذف انجام نشد.")

# ============================================================
# TEXT / MEDIA ROUTERS
# ============================================================

async def text_router(update,context):
    if not update.message: return
    ensure_user(update.effective_user)
    uid=update.effective_user.id
    text=(update.message.text or "").strip()
    if is_blocked(uid) and not is_admin(uid):
        await update.message.reply_text("🚫 دسترسی شما محدود شده است."); return
    if text=="/cancel":
        for k in ("support_mode","chat_room","awaiting_asset","payment_plan","admin_mode","admin_reply_to","message_target"):
            context.user_data.pop(k,None)
        await update.message.reply_text("لغو شد.",reply_markup=main_kb(uid)); return

    handlers={
        "➕ افزودن دارایی":add_asset_prompt,"📋 واچ‌لیست":watchlist_menu,
        "💰 قیمت لحظه‌ای":live_price_menu,"📊 تحلیل":analysis_prompt,
        "🚨 سیگنال‌ها":signals_menu,"💳 خرید اشتراک":buy_menu,
        "👤 وضعیت اشتراک":status_menu,"🔔 هشدارها":alerts_menu,
        "🪙 ارزهای بیشتر":more_coins,"💬 چت رمز ارز":crypto_chat_menu,
        "📨 ارتباط با پشتیبان":support_prompt,"ℹ️ راهنما":help_text,
        "👨‍💼 پنل مدیریت":admin_panel,
    }
    if text in handlers:
        for k in ("support_mode","chat_room","awaiting_asset","payment_plan","admin_reply_to","message_target"):
            context.user_data.pop(k,None)
        await handlers[text](update,context); return

    if await admin_text_action(update,context,text): return

    if is_admin(uid) and context.user_data.get("admin_reply_to"):
        await send_support_reply(update,context,text); return

    if context.user_data.get("support_mode"):
        context.user_data.pop("support_mode",None)
        await save_support(uid,text,update.message.message_id)
        for aid in ADMIN_IDS:
            try:
                await context.bot.send_message(aid,f"📨 پیام پشتیبانی از {uid}:\n\n{escape(text)}",
                    parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ پاسخ",callback_data=f"sup:reply:{uid}")]]))
            except Exception: pass
        await update.message.reply_text("✅ پیام برای پشتیبان ارسال شد."); return

    if await process_chat_message(update,context): return

    awaiting=context.user_data.get("awaiting_asset")
    if awaiting:
        context.user_data.pop("awaiting_asset",None)
        await process_add_asset(update,context,text); return

    s=norm_symbol(text)
    if s in COINS or s in ("XAU","GOLD18"):
        await process_add_asset(update,context,text); return

    await update.message.reply_text(
        "❓ دستور یا نماد شناخته نشد.\n\n"
        "نمونه: BTC، ZEC، XAU، GOLD18"
    )

async def media_router(update,context):
    ensure_user(update.effective_user)
    uid=update.effective_user.id
    if is_blocked(uid) and not is_admin(uid): return
    if context.user_data.get("support_mode"):
        await support_media(update,context); return
    if update.message.photo:
        await receipt_photo(update,context); return
    await update.message.reply_text("برای ارسال این نوع پیام ابتدا پشتیبانی را انتخاب کنید.")

# ============================================================
# LIFECYCLE / MAIN
# ============================================================

async def post_init(app):
    init_db()
    try:
        await app.bot.delete_webhook(drop_pending_updates=False)
    except Exception as e:
        log.warning("delete webhook: %s",e)
    app.create_task(alert_worker(app))
    d=database_diagnostics()
    log.info(
        "FAST Market Analyzer started | DB=%s | exists=%s | size=%s | persistent=%s | admins=%s",
        d["path"], d["exists"], d["size"], d["persistent_path"], sorted(ADMIN_IDS)
    )
    if not d["persistent_path"]:
        log.error(
            "DATABASE IS NOT ON RAILWAY PERSISTENT VOLUME. "
            "Set DB_PATH=/data/crypto_bot.db and attach a Volume mounted at /data."
        )

async def post_shutdown(app):
    global HTTP_SESSION
    if HTTP_SESSION and not HTTP_SESSION.closed:
        await HTTP_SESSION.close()

def main():
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    init_db()
    app=(Application.builder().token(BOT_TOKEN).post_init(post_init).post_shutdown(post_shutdown).build())

    app.add_handler(CommandHandler("start",start))
    app.add_handler(CommandHandler("cancel",text_router))
    app.add_handler(CommandHandler("dbstatus",db_status_command))

    app.add_handler(CallbackQueryHandler(plan_callback,pattern=r"^plan:"))
    app.add_handler(CallbackQueryHandler(payment_callback,pattern=r"^pay:"))
    app.add_handler(CallbackQueryHandler(support_reply_callback,pattern=r"^sup:reply:"))
    app.add_handler(CallbackQueryHandler(chat_open_callback,pattern=r"^chat:open:"))
    app.add_handler(CallbackQueryHandler(chat_report_callback,pattern=r"^chat:report:\d+$"))
    app.add_handler(CallbackQueryHandler(chat_admin_callback,pattern=r"^chat:(delete|block):\d+$"))
    app.add_handler(CallbackQueryHandler(alert_callback,pattern=r"^alert:"))
    app.add_handler(CallbackQueryHandler(admin_callback,pattern=r"^adm:"))
    app.add_handler(CallbackQueryHandler(selected_price_callback,pattern=r"^price:"))
    app.add_handler(CallbackQueryHandler(analysis_selector_callback,pattern=r"^analysis:"))
    app.add_handler(CallbackQueryHandler(signal_selector_callback,pattern=r"^signal:"))
    app.add_handler(CallbackQueryHandler(misc_callback,pattern=r"^(pick:|wl:del:)"))

    app.add_handler(MessageHandler(filters.PHOTO|filters.Document.ALL|filters.VOICE,media_router))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND,text_router))

    log.info("Starting polling...")
    app.run_polling(drop_pending_updates=False,allowed_updates=Update.ALL_TYPES)

if __name__=="__main__":
    main()
