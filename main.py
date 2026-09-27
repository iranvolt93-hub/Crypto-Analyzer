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
Optional:
    DB_PATH=/data/crypto_bot.db
    HTTP_TIMEOUT=20
    CACHE_SECONDS=60
    ALERT_INTERVAL_SECONDS=300

IMPORTANT:
- Railway Volume should be mounted at /data.
- Start command: python main.py
- Run only ONE instance for this bot token.
"""

import os
import re
import sqlite3
import asyncio
import logging
import time
import math
from datetime import datetime, timedelta, timezone
from html import escape

import aiohttp
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)

# ---------------- CONFIG ----------------
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
ADMIN_IDS = set()
for x in os.getenv("ADMIN_IDS", "").split(","):
    try:
        if x.strip(): ADMIN_IDS.add(int(x.strip()))
    except ValueError:
        pass

DB_PATH = os.getenv("DB_PATH", "/data/crypto_bot.db")
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
CACHE_SECONDS = int(os.getenv("CACHE_SECONDS", "60"))
ALERT_INTERVAL_SECONDS = int(os.getenv("ALERT_INTERVAL_SECONDS", "300"))
PAYMENT_CARD = os.getenv("PAYMENT_CARD", "ثبت نشده")
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "پشتیبان")

PLANS = {
    "30": (30, 200000),
    "90": (90, 350000),
    "180": (180, 500000),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("market_bot")

CACHE = {}
HTTP_SESSION = None

MAIN_MENU = [
    ["➕ افزودن دارایی", "📋 واچ‌لیست"],
    ["📊 تحلیل", "🚨 سیگنال‌ها"],
    ["💳 خرید اشتراک", "👤 وضعیت اشتراک"],
    ["🔔 هشدارها", "🪙 ارزهای بیشتر"],
    ["💬 چت رمز ارز", "📨 ارتباط با پشتیبان"],
    ["ℹ️ راهنما"],
    ["👨‍💼 پنل مدیریت"],
]

# ---------------- DATABASE ----------------
def db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    con.execute("PRAGMA foreign_keys=ON")
    return con


def init_db():
    with db() as c:
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
        CREATE INDEX IF NOT EXISTS idx_sub_user_end ON subscriptions(user_id,end_at);
        CREATE INDEX IF NOT EXISTS idx_pay_status ON payment_requests(status);
        CREATE INDEX IF NOT EXISTS idx_watch_asset ON watchlist(asset_type,symbol);
        CREATE INDEX IF NOT EXISTS idx_chat_room ON chat_messages(asset_type,symbol,created_at);
        CREATE INDEX IF NOT EXISTS idx_chat_reports ON chat_reports(status);
        """)
        # Additive migrations only.
        cols = [r[1] for r in c.execute("PRAGMA table_info(alert_events)").fetchall()]
        if "signal_key" not in cols:
            c.execute("ALTER TABLE alert_events ADD COLUMN signal_key TEXT DEFAULT ''")


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def ensure_user(u):
    if not u:
        return
    now = now_iso()
    with db() as c:
        c.execute("""
        INSERT INTO users(user_id,username,first_name,created_at,last_seen)
        VALUES(?,?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
          username=excluded.username,
          first_name=excluded.first_name,
          last_seen=excluded.last_seen
        """, (u.id, u.username or "", u.first_name or "", now, now))
        c.execute("INSERT OR IGNORE INTO alert_preferences(user_id) VALUES(?)", (u.id,))


def is_admin(uid):
    return uid in ADMIN_IDS


def is_blocked(uid):
    with db() as c:
        r = c.execute("SELECT blocked FROM users WHERE user_id=?", (uid,)).fetchone()
        return bool(r and r["blocked"])


def active_subscription(uid):
    now = now_iso()
    with db() as c:
        return c.execute("""
        SELECT * FROM subscriptions
        WHERE user_id=? AND status='active' AND end_at>?
        ORDER BY end_at DESC LIMIT 1
        """, (uid, now)).fetchone()


def has_analysis_access(uid):
    return is_admin(uid) or active_subscription(uid) is not None


def add_subscription(uid, plan, payment_request_id=None, source="manual"):
    days, amount = PLANS[plan]
    old = active_subscription(uid)
    start = datetime.now(timezone.utc)
    if old:
        try:
            base = datetime.fromisoformat(old["end_at"])
            if base > start:
                start = base
        except Exception:
            pass
    end = start + timedelta(days=days)
    with db() as c:
        c.execute("""
        UPDATE subscriptions SET status='expired'
        WHERE user_id=? AND status='active' AND end_at<=?
        """, (uid, now_iso()))
        c.execute("""
        INSERT INTO subscriptions(user_id,plan,days,amount,start_at,end_at,status,source,payment_request_id,created_at)
        VALUES(?,?,?,?,?,?,?,?,?,?)
        """, (uid,plan,days,amount,start.isoformat(),end.isoformat(),"active",source,payment_request_id,now_iso()))


def format_dt(s):
    try:
        return datetime.fromisoformat(s).astimezone().strftime("%Y/%m/%d %H:%M")
    except Exception:
        return str(s)

# ---------------- HTTP ----------------
async def get_session():
    global HTTP_SESSION
    if HTTP_SESSION is None or HTTP_SESSION.closed:
        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
        HTTP_SESSION = aiohttp.ClientSession(timeout=timeout, headers={"User-Agent":"Mozilla/5.0 MarketAnalyzerBot/1.0"})
    return HTTP_SESSION


async def http_json(url, params=None, retries=3):
    key = (url, tuple(sorted((params or {}).items())))
    cached = CACHE.get(key)
    if cached and time.time() - cached[0] < CACHE_SECONDS:
        return cached[1]
    session = await get_session()
    for attempt in range(retries):
        try:
            async with session.get(url, params=params) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    CACHE[key] = (time.time(), data)
                    return data
                log.warning("HTTP %s: %s", r.status, url)
        except Exception as e:
            log.warning("HTTP error: %s", e)
        await asyncio.sleep(1 + attempt)
    return None


async def http_text(url, params=None):
    session = await get_session()
    try:
        async with session.get(url, params=params) as r:
            if r.status == 200:
                return await r.text()
    except Exception as e:
        log.warning("HTTP text error: %s", e)
    return None

# ---------------- ASSETS ----------------
COINS = {
    "BTC":"bitcoin","ETH":"ethereum","BNB":"binancecoin","SOL":"solana","XRP":"ripple",
    "DOGE":"dogecoin","ADA":"cardano","TRX":"tron","AVAX":"avalanche-2","DOT":"polkadot",
    "LINK":"chainlink","MATIC":"matic-network","POL":"polygon-ecosystem-token","LTC":"litecoin",
    "BCH":"bitcoin-cash","ATOM":"cosmos","ETC":"ethereum-classic","XLM":"stellar",
    "UNI":"uniswap","NEAR":"near","APT":"aptos","ARB":"arbitrum","OP":"optimism",
    "FIL":"filecoin","ICP":"internet-computer","HBAR":"hedera-hashgraph","SUI":"sui",
    "PEPE":"pepe","SHIB":"shiba-inu","TON":"the-open-network","ZEC":"zcash",
    "AAVE":"aave","ALGO":"algorand","VET":"vechain","EOS":"eos","XMR":"monero",
    "TAO":"bittensor","INJ":"injective-protocol","SEI":"sei-network","RUNE":"thorchain",
    "MKR":"maker","CRV":"curve-dao-token","GRT":"the-graph","LDO":"lido-staked-ether",
    "SAND":"the-sandbox","MANA":"decentraland","AXS":"axie-infinity","FTM":"fantom",
    "KAS":"kaspa","WIF":"dogwifcoin","BONK":"bonk","FLOKI":"floki",
}


def norm_symbol(s):
    s = (s or "").strip().upper().replace(" ","")
    aliases = {
        "GOLD":"XAU", "XAUUSD":"XAU", "XAU/USDT":"XAU", "طلا":"XAU", "طلای جهانی":"XAU",
        "GERAM18":"GOLD18", "18K":"GOLD18", "GOLD18K":"GOLD18", "IRANGOLD":"GOLD18",
        "طلای18":"GOLD18", "طلای۱۸":"GOLD18", "طلای۱۸عیار":"GOLD18", "طلای داخلی":"GOLD18",
    }
    return aliases.get(s, s)


def asset_type(symbol):
    symbol = norm_symbol(symbol)
    if symbol == "XAU": return "gold"
    if symbol == "GOLD18": return "gold18"
    return "crypto"


async def crypto_search(query):
    q = query.strip().lower()
    direct = norm_symbol(query)
    if direct in COINS:
        return [(direct, COINS[direct], direct)]
    data = await http_json("https://api.coingecko.com/api/v3/search", {"query": q})
    out=[]
    for x in (data or {}).get("coins",[])[:10]:
        sym=(x.get("symbol") or "").upper()
        cid=x.get("id")
        if sym and cid:
            out.append((sym,cid,x.get("name") or sym))
    return out


async def crypto_data(symbol):
    sym = norm_symbol(symbol)
    cid = COINS.get(sym)
    if not cid:
        results = await crypto_search(sym)
        if not results: return None
        sym,cid,_ = results[0]
    data = await http_json(f"https://api.coingecko.com/api/v3/coins/{cid}/market_chart", {"vs_currency":"usd","days":"2","interval":"hourly"})
    if not data or not data.get("prices"):
        return None
    prices = pd.Series([float(x[1]) for x in data["prices"]])
    vols = pd.Series([float(x[1]) for x in data.get("total_volumes",[])]) if data.get("total_volumes") else pd.Series(dtype=float)
    return sym, prices, vols


async def xau_data():
    for ticker in ("XAUUSD=X", "GC=F"):
        data = await http_json(f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}", {"range":"5d","interval":"1h"})
        try:
            result=data["chart"]["result"][0]
            vals=result["indicators"]["quote"][0]["close"]
            vals=[float(v) for v in vals if v is not None]
            if len(vals)>10: return "XAU",pd.Series(vals),pd.Series(dtype=float)
        except Exception:
            continue
    return None


async def gold18_data():
    html = await http_text("https://www.tgju.org/profile/geram18")
    if not html: return None
    soup=BeautifulSoup(html,"html.parser")
    nums=[]
    # Prefer values around price elements, then fallback to all numeric-looking text.
    for tag in soup.find_all(string=re.compile(r"\d")):
        t=tag.strip().replace(",","").replace("٬","")
        m=re.search(r"(\d{6,})",t)
        if m:
            try:
                n=float(m.group(1))
                if 100000 <= n <= 500000000: nums.append(n)
            except: pass
    if not nums: return None
    # Keep recent candidates and avoid obvious timestamps.
    price=nums[0]
    return "GOLD18",pd.Series([price]*30,dtype=float),pd.Series(dtype=float)


async def asset_data(symbol):
    symbol=norm_symbol(symbol)
    if symbol=="XAU": return await xau_data()
    if symbol=="GOLD18": return await gold18_data()
    return await crypto_data(symbol)

# ---------------- ANALYSIS ----------------
def analysis_from_series(symbol, prices, volumes=None):
    p=pd.Series(prices,dtype=float).dropna()
    if len(p)<5: return None
    ema9=p.ewm(span=9,adjust=False).mean().iloc[-1]
    ema21=p.ewm(span=21,adjust=False).mean().iloc[-1]
    delta=p.diff()
    gain=delta.clip(lower=0).rolling(14).mean()
    loss=(-delta.clip(upper=0)).rolling(14).mean()
    rs=gain/(loss.replace(0,np.nan))
    rsi=float((100-(100/(1+rs))).iloc[-1]) if not pd.isna(rs.iloc[-1]) else 50.0
    r1=(p.iloc[-1]/p.iloc[-2]-1)*100 if len(p)>=2 and p.iloc[-2] else 0
    r6=(p.iloc[-1]/p.iloc[-7]-1)*100 if len(p)>=7 and p.iloc[-7] else 0
    r24=(p.iloc[-1]/p.iloc[-25]-1)*100 if len(p)>=25 and p.iloc[-25] else r6
    score=0
    score += 2 if ema9>ema21 else -2
    score += 2 if rsi>52 else (-2 if rsi<48 else 0)
    score += 1 if r1>0 else (-1 if r1<0 else 0)
    score += 1 if r6>0 else (-1 if r6<0 else 0)
    score=max(-6,min(6,score))
    signal="BUY" if score>=3 else ("SELL" if score<=-3 else "WAIT")
    strength=min(99,50+abs(score)*8)
    probability=min(95,max(5,50+score*7))
    return {
        "symbol":symbol,"price":float(p.iloc[-1]),"ema9":float(ema9),"ema21":float(ema21),
        "rsi":rsi,"r1":r1,"r6":r6,"r24":r24,"score":score,"signal":signal,
        "strength":strength,"probability":probability
    }


async def analyze(symbol):
    d=await asset_data(symbol)
    if not d: return None
    return analysis_from_series(d[0],d[1],d[2])


def signal_fa(s):
    return {"BUY":"🟢 خرید","SELL":"🔴 فروش","WAIT":"🟡 انتظار"}.get(s,s)


def analysis_text(a):
    if not a: return "❌ اطلاعات بازار در دسترس نیست."
    return (
        f"📊 <b>تحلیل {escape(a['symbol'])}</b>\n\n"
        f"💰 قیمت: <b>{a['price']:,.4f}</b>\n"
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

# ---------------- WATCHLIST ----------------
def add_watch(uid,symbol,atype):
    with db() as c:
        c.execute("INSERT OR IGNORE INTO watchlist(user_id,symbol,asset_type,created_at) VALUES(?,?,?,?)", (uid,symbol,atype,now_iso()))


def remove_watch(uid,symbol,atype):
    with db() as c:
        c.execute("DELETE FROM watchlist WHERE user_id=? AND symbol=? AND asset_type=?",(uid,symbol,atype))


def user_assets(uid, atype=None):
    with db() as c:
        if atype:
            return c.execute("SELECT * FROM watchlist WHERE user_id=? AND asset_type=? ORDER BY created_at",(uid,atype)).fetchall()
        return c.execute("SELECT * FROM watchlist WHERE user_id=? ORDER BY created_at",(uid,)).fetchall()


def user_has_asset(uid,symbol,atype="crypto"):
    with db() as c:
        return c.execute("SELECT 1 FROM watchlist WHERE user_id=? AND symbol=? AND asset_type=?",(uid,norm_symbol(symbol),atype)).fetchone() is not None

# ---------------- KEYBOARDS ----------------
def main_kb(uid):
    rows=[r[:] for r in MAIN_MENU]
    if not is_admin(uid): rows=[r for r in rows if r != ["👨‍💼 پنل مدیریت"]]
    return ReplyKeyboardMarkup(rows,resize_keyboard=True)


def admin_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 آمار",callback_data="adm:stats"),InlineKeyboardButton("👥 کاربران",callback_data="adm:users:0")],
        [InlineKeyboardButton("📢 پیام به مشترکین",callback_data="adm:broadcast"),InlineKeyboardButton("📨 پشتیبانی",callback_data="adm:support")],
        [InlineKeyboardButton("💬 مدیریت چت",callback_data="adm:chat"),InlineKeyboardButton("🚨 گزارش‌ها",callback_data="adm:reports")],
        [InlineKeyboardButton("💳 پرداخت‌های در انتظار",callback_data="adm:payments")],
        [InlineKeyboardButton("✉️ پیام به کاربر",callback_data="adm:message"),InlineKeyboardButton("🚫 مسدود/رفع",callback_data="adm:block")],
    ])

# ---------------- BASIC COMMANDS ----------------
async def start(update,context):
    ensure_user(update.effective_user)
    if is_blocked(update.effective_user.id):
        await update.message.reply_text("🚫 دسترسی شما توسط مدیر محدود شده است.")
        return
    await update.message.reply_text(
        "سلام 👋\n\nبه ربات تحلیل بازار خوش آمدید.\nاز منوی زیر استفاده کنید.",
        reply_markup=main_kb(update.effective_user.id)
    )


async def help_text(update,context):
    await update.message.reply_text(
        "ℹ️ راهنما\n\n"
        "➕ افزودن دارایی: افزودن رمز‌ارز، طلای جهانی یا طلای ۱۸ عیار\n"
        "📊 تحلیل و 🚨 سیگنال‌ها: نیازمند اشتراک فعال\n"
        "💬 چت رمز ارز: گفت‌وگوی کاربران مشترک همان رمز‌ارز\n"
        "📨 پشتیبان: ارسال پیام مستقیم به مدیر/پشتیبان\n"
        "💳 خرید اشتراک: پرداخت دستی و ارسال رسید\n\n"
        "⚠️ ربات معامله خودکار انجام نمی‌دهد."
    )

# ---------------- ADD ASSET ----------------
async def add_asset_prompt(update,context):
    context.user_data["awaiting_asset"]="add"
    await update.message.reply_text("نام یا نماد دارایی را بفرستید؛ مثال: BTC ، ZEC ، XAU یا GOLD18")


async def more_coins(update,context):
    await update.message.reply_text(
        "🪙 برای افزودن ارزهای بیشتر، نماد یا نام آن را ارسال کنید.\n"
        "ربات ابتدا فهرست داخلی و سپس CoinGecko را جست‌وجو می‌کند."
    )
    context.user_data["awaiting_asset"]="add"


async def process_add_asset(update,context,text):
    results=[]
    if norm_symbol(text) in ("XAU","GOLD18"):
        results=[(norm_symbol(text),norm_symbol(text),norm_symbol(text))]
    else:
        results=await crypto_search(text)
    if not results:
        await update.message.reply_text("❌ دارایی پیدا نشد.")
        return
    if len(results)==1:
        sym=results[0][0]; at=asset_type(sym); add_watch(update.effective_user.id,sym,at)
        await update.message.reply_text(f"✅ {sym} به واچ‌لیست اضافه شد.")
        return
    buttons=[]
    for sym,cid,name in results[:8]:
        buttons.append([InlineKeyboardButton(f"{sym} — {name}",callback_data=f"pick:{sym}:{cid}")])
    await update.message.reply_text("دارایی موردنظر را انتخاب کنید:",reply_markup=InlineKeyboardMarkup(buttons))

# ---------------- WATCHLIST / ANALYSIS ----------------
async def watchlist_menu(update,context):
    rows=user_assets(update.effective_user.id)
    if not rows:
        await update.message.reply_text("📋 واچ‌لیست خالی است. از «➕ افزودن دارایی» استفاده کنید.")
        return
    buttons=[]
    for r in rows:
        buttons.append([InlineKeyboardButton(f"❌ حذف {r['symbol']}",callback_data=f"wl:del:{r['asset_type']}:{r['symbol']}")])
    txt="📋 <b>واچ‌لیست شما</b>\n\n"+"\n".join(f"• {r['symbol']}" for r in rows)
    await update.message.reply_text(txt,parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup(buttons))


async def analysis_prompt(update,context):
    if not has_analysis_access(update.effective_user.id):
        await update.message.reply_text("🔒 تحلیل فقط برای مشترکین فعال است. از «💳 خرید اشتراک» استفاده کنید.")
        return
    context.user_data["awaiting_asset"]="analysis"
    await update.message.reply_text("نماد دارایی را برای تحلیل بفرستید؛ مثال BTC یا ZEC")


async def signals_menu(update,context):
    uid=update.effective_user.id
    if not has_analysis_access(uid):
        await update.message.reply_text("🔒 سیگنال‌ها فقط برای مشترکین فعال است.")
        return
    rows=user_assets(uid)
    if not rows:
        await update.message.reply_text("واچ‌لیست شما خالی است.")
        return
    await update.message.reply_text("⏳ در حال محاسبه سیگنال‌ها...")
    for r in rows:
        a=await analyze(r["symbol"])
        await update.effective_message.reply_text(analysis_text(a),parse_mode=ParseMode.HTML)

# ---------------- SUBSCRIPTIONS ----------------
async def buy_menu(update,context):
    buttons=[]
    for k,(days,amount) in PLANS.items():
        buttons.append([InlineKeyboardButton(f"{days} روز — {amount:,} تومان",callback_data=f"plan:{k}")])
    await update.message.reply_text("💳 یکی از پلن‌ها را انتخاب کنید:",reply_markup=InlineKeyboardMarkup(buttons))


async def status_menu(update,context):
    s=active_subscription(update.effective_user.id)
    if not s:
        await update.message.reply_text("👤 اشتراک فعال ندارید.")
        return
    await update.message.reply_text(
        f"👤 وضعیت اشتراک\n\nپلن: {s['days']} روز\nشروع: {format_dt(s['start_at'])}\nپایان: {format_dt(s['end_at'])}\nوضعیت: فعال"
    )


async def plan_callback(update,context):
    q=update.callback_query; await q.answer()
    plan=q.data.split(":",1)[1]
    days,amount=PLANS[plan]
    context.user_data["payment_plan"]=plan
    await q.message.reply_text(
        f"💳 پلن {days} روزه\nمبلغ: {amount:,} تومان\n\n"
        f"شماره کارت پرداخت:\n<code>{escape(PAYMENT_CARD)}</code>\n\n"
        "پس از پرداخت، تصویر رسید را همین‌جا ارسال کنید.",parse_mode=ParseMode.HTML
    )


async def receipt_photo(update,context):
    # Support mode has priority over payment receipt.
    if context.user_data.get("support_mode"):
        await support_media(update,context)
        return
    plan=context.user_data.get("payment_plan")
    if not plan:
        await update.message.reply_text("ابتدا از «💳 خرید اشتراک» یک پلن انتخاب کنید.")
        return
    days,amount=PLANS[plan]
    file_id=update.message.photo[-1].file_id
    with db() as c:
        cur=c.execute("""
        INSERT INTO payment_requests(user_id,plan,days,amount,receipt_file_id,status,created_at)
        VALUES(?,?,?,?,?,?,?)
        """,(update.effective_user.id,plan,days,amount,file_id,"pending",now_iso()))
        pid=cur.lastrowid
    context.user_data.pop("payment_plan",None)
    await update.message.reply_text("✅ رسید دریافت شد. پس از بررسی مدیر، اشتراک فعال می‌شود.")
    for aid in ADMIN_IDS:
        try:
            kb=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ تایید",callback_data=f"pay:approve:{pid}"),InlineKeyboardButton("❌ رد",callback_data=f"pay:reject:{pid}")]
            ])
            await context.bot.send_photo(aid,file_id,caption=f"💳 رسید جدید\nکاربر: {update.effective_user.id}\nپلن: {days} روز\nمبلغ: {amount:,} تومان\nشناسه: {pid}",reply_markup=kb)
        except Exception as e: log.warning("payment notify: %s",e)

# ---------------- SUPPORT ----------------
async def support_prompt(update,context):
    context.user_data["support_mode"]=True
    await update.message.reply_text("📨 پیام خود را برای پشتیبان بفرستید. متن، عکس، فایل یا صدا قابل ارسال است. برای خروج /cancel را بزنید.")


async def save_support(uid,message,tid):
    with db() as c:
        c.execute("INSERT INTO support_messages(user_id,direction,message,telegram_message_id,created_at) VALUES(?,?,?,?,?)",(uid,"user_to_admin",message,tid,now_iso()))


async def support_media(update,context):
    uid=update.effective_user.id
    if update.message.photo:
        msg="[عکس]"
        for aid in ADMIN_IDS:
            try:
                kb=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ پاسخ",callback_data=f"sup:reply:{uid}")]])
                await context.bot.send_photo(aid,update.message.photo[-1].file_id,caption=f"📨 پیام پشتیبانی\nکاربر: {uid}",reply_markup=kb)
            except: pass
    elif update.message.document:
        msg="[فایل]"
        for aid in ADMIN_IDS:
            try:
                kb=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ پاسخ",callback_data=f"sup:reply:{uid}")]])
                await context.bot.send_document(aid,update.message.document.file_id,caption=f"📨 فایل از کاربر {uid}",reply_markup=kb)
            except: pass
    elif update.message.voice:
        msg="[صدا]"
        for aid in ADMIN_IDS:
            try:
                kb=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ پاسخ",callback_data=f"sup:reply:{uid}")]])
                await context.bot.send_voice(aid,update.message.voice.file_id,caption=f"📨 صدا از کاربر {uid}",reply_markup=kb)
            except: pass
    else:
        return
    await save_support(uid,msg,update.message.message_id)
    await update.message.reply_text("✅ پیام شما برای پشتیبان ارسال شد.")


async def support_reply_callback(update,context):
    q=update.callback_query; await q.answer()
    uid=int(q.data.split(":")[-1])
    context.user_data["admin_reply_to"]=uid
    await q.message.reply_text(f"✍️ پاسخ خود به کاربر {uid} را ارسال کنید.")


async def send_support_reply(update,context,text):
    uid=context.user_data.pop("admin_reply_to")
    try:
        await context.bot.send_message(uid,f"📨 پاسخ پشتیبان:\n\n{text}")
        with db() as c:
            c.execute("INSERT INTO support_messages(user_id,admin_id,direction,message,status,created_at,replied_at) VALUES(?,?,?,?,?,?,?)",(uid,update.effective_user.id,"admin_to_user",text,"closed",now_iso(),now_iso()))
        await update.message.reply_text("✅ پاسخ ارسال شد.")
    except Exception as e:
        await update.message.reply_text(f"❌ ارسال نشد: {e}")

# ---------------- COMMUNITY CRYPTO CHAT ----------------
def chat_name(uid):
    with db() as c:
        r=c.execute("SELECT first_name,username FROM users WHERE user_id=?",(uid,)).fetchone()
    if not r: return "کاربر"
    return r["first_name"] or ("@"+r["username"] if r["username"] else "کاربر")


def chat_members(symbol):
    with db() as c:
        return c.execute("""
        SELECT DISTINCT u.user_id,u.first_name,u.username
        FROM users u
        JOIN watchlist w ON w.user_id=u.user_id
        JOIN subscriptions s ON s.user_id=u.user_id
        WHERE u.blocked=0 AND w.asset_type='crypto' AND w.symbol=?
          AND s.status='active' AND s.end_at>?
        """,(norm_symbol(symbol),now_iso())).fetchall()


async def crypto_chat_menu(update,context):
    uid=update.effective_user.id
    if not has_analysis_access(uid):
        await update.message.reply_text("🔒 چت رمز ارز فقط برای مشترکین فعال است.")
        return
    rows=user_assets(uid,"crypto")
    if not rows:
        await update.message.reply_text("ابتدا حداقل یک رمز‌ارز به واچ‌لیست اضافه کنید.")
        return
    buttons=[[InlineKeyboardButton(f"💬 {r['symbol']}",callback_data=f"chat:open:{r['symbol']}")] for r in rows]
    await update.message.reply_text("رمز‌ارزی را انتخاب کنید تا وارد اتاق گفت‌وگوی مشترک آن شوید:",reply_markup=InlineKeyboardMarkup(buttons))


async def chat_open_callback(update,context):
    q=update.callback_query; await q.answer()
    uid=q.from_user.id; sym=norm_symbol(q.data.split(":",2)[2])
    if not has_analysis_access(uid) or not user_has_asset(uid,sym,"crypto"):
        await q.message.reply_text("🔒 شما مجاز به ورود به این اتاق نیستید.")
        return
    context.user_data["chat_room"]=sym
    members=chat_members(sym)
    await q.message.reply_text(
        f"💬 <b>اتاق {escape(sym)}</b>\n\n"
        f"👥 اعضای فعال: {len(members)} نفر\n"
        "پیام شما برای مشترکین همین رمز‌ارز ارسال می‌شود.\n"
        "برای خروج /cancel را بزنید.",parse_mode=ParseMode.HTML
    )


async def process_chat_message(update,context):
    room=context.user_data.get("chat_room")
    if not room or not update.message or not update.message.text:
        return False
    uid=update.effective_user.id
    if not has_analysis_access(uid) or not user_has_asset(uid,room,"crypto"):
        context.user_data.pop("chat_room",None)
        return False
    text=update.message.text.strip()
    if not text: return True
    if len(text)>1500:
        await update.message.reply_text("❌ حداکثر طول پیام ۱۵۰۰ کاراکتر است.")
        return True
    with db() as c:
        cur=c.execute("INSERT INTO chat_messages(user_id,asset_type,symbol,message,telegram_message_id,created_at) VALUES(?,?,?,?,?,?)",(uid,"crypto",room,text,update.message.message_id,now_iso()))
        mid=cur.lastrowid
    sender=escape(chat_name(uid))
    members=chat_members(room)
    for m in members:
        if m["user_id"]==uid: continue
        try:
            await context.bot.send_message(m["user_id"],f"💬 <b>{sender}</b> در اتاق {escape(room)}:\n\n{escape(text)}",parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🚨 گزارش",callback_data=f"chat:report:{mid}")]]))
        except Exception as e: log.debug("chat relay %s",e)
    for aid in ADMIN_IDS:
        try:
            await context.bot.send_message(aid,f"👁 <b>چت {escape(room)}</b>\nکاربر: <code>{uid}</code> ({sender})\nپیام #{mid}:\n{escape(text)}",parse_mode=ParseMode.HTML,reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🗑 حذف",callback_data=f"chat:delete:{mid}"),InlineKeyboardButton("🚫 مسدود",callback_data=f"chat:block:{uid}")]]))
        except: pass
    return True


async def chat_report_callback(update,context):
    q=update.callback_query; await q.answer("گزارش ثبت شد.")
    mid=int(q.data.split(":")[-1]); uid=q.from_user.id
    with db() as c:
        exists=c.execute("SELECT 1 FROM chat_reports WHERE message_id=? AND reporter_id=? AND status='pending'",(mid,uid)).fetchone()
        if not exists:
            c.execute("INSERT INTO chat_reports(message_id,reporter_id,reason,status,created_at) VALUES(?,?,?,?,?)",(mid,uid,"گزارش کاربر","pending",now_iso()))
    for aid in ADMIN_IDS:
        try:
            await context.bot.send_message(aid,f"🚨 گزارش جدید برای پیام چت #{mid} توسط کاربر {uid}",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🗑 حذف پیام",callback_data=f"chat:delete:{mid}")]]))
        except: pass


async def chat_admin_callback(update,context):
    q=update.callback_query; await q.answer()
    parts=q.data.split(":")
    action=parts[1]; target=int(parts[2])
    if action=="delete":
        with db() as c:
            r=c.execute("SELECT * FROM chat_messages WHERE id=?",(target,)).fetchone()
            c.execute("UPDATE chat_messages SET deleted=1,deleted_by=?,deleted_at=? WHERE id=?",(q.from_user.id,now_iso(),target))
            c.execute("UPDATE chat_reports SET status='reviewed',reviewed_by=?,reviewed_at=? WHERE message_id=?",(q.from_user.id,now_iso(),target))
        if r:
            members=chat_members(r["symbol"])
            for m in members:
                try: await context.bot.send_message(m["user_id"],f"🗑 پیام #{target} توسط مدیر حذف شد.")
                except: pass
        await q.message.reply_text("✅ پیام حذف شد.")
    elif action=="block":
        with db() as c: c.execute("UPDATE users SET blocked=1 WHERE user_id=?",(target,))
        await q.message.reply_text(f"🚫 کاربر {target} مسدود شد.")

# ---------------- ALERTS ----------------
async def alerts_menu(update,context):
    uid=update.effective_user.id
    with db() as c:
        r=c.execute("SELECT * FROM alert_preferences WHERE user_id=?",(uid,)).fetchone()
        enabled=bool(r["enabled"]) if r else True
    kb=InlineKeyboardMarkup([[InlineKeyboardButton("🔕 خاموش" if enabled else "🔔 روشن",callback_data="alert:toggle")]])
    await update.message.reply_text(f"🔔 هشدار سیگنال: {'فعال' if enabled else 'خاموش'}",reply_markup=kb)


async def alert_callback(update,context):
    q=update.callback_query; await q.answer()
    uid=q.from_user.id
    with db() as c:
        r=c.execute("SELECT enabled FROM alert_preferences WHERE user_id=?",(uid,)).fetchone()
        new=0 if r and r["enabled"] else 1
        c.execute("INSERT INTO alert_preferences(user_id,enabled,interval_seconds) VALUES(?,?,?) ON CONFLICT(user_id) DO UPDATE SET enabled=excluded.enabled",(uid,new,ALERT_INTERVAL_SECONDS))
    await q.message.edit_text(f"🔔 هشدار سیگنال: {'فعال' if new else 'خاموش'}")


async def alert_worker(app):
    while True:
        try:
            with db() as c:
                users=c.execute("SELECT user_id FROM alert_preferences WHERE enabled=1").fetchall()
            for ur in users:
                uid=ur["user_id"]
                if not has_analysis_access(uid): continue
                for wr in user_assets(uid):
                    a=await analyze(wr["symbol"])
                    if not a or a["signal"]=="WAIT": continue
                    key=f"{wr['symbol']}:{a['signal']}"
                    with db() as c:
                        seen=c.execute("SELECT 1 FROM alert_events WHERE user_id=? AND symbol=? AND signal_key=?",(uid,wr["symbol"],key)).fetchone()
                        if seen: continue
                        c.execute("INSERT INTO alert_events(user_id,symbol,signal_key,message,created_at) VALUES(?,?,?,?,?)",(uid,wr["symbol"],key,analysis_text(a),now_iso()))
                    try:
                        await app.bot.send_message(uid,"🔔 <b>سیگنال جدید</b>\n\n"+analysis_text(a),parse_mode=ParseMode.HTML)
                    except: pass
        except Exception as e: log.warning("alert worker: %s",e)
        await asyncio.sleep(ALERT_INTERVAL_SECONDS)

# ---------------- ADMIN ----------------
async def admin_panel(update,context):
    if not is_admin(update.effective_user.id): return
    await update.message.reply_text("👨‍💼 پنل مدیریت",reply_markup=admin_kb())


async def admin_callback(update,context):
    q=update.callback_query; await q.answer()
    if not is_admin(q.from_user.id): return
    parts=q.data.split(":")
    action=parts[1]
    if action=="stats":
        with db() as c:
            users=c.execute("SELECT COUNT(*) n FROM users").fetchone()["n"]
            active=c.execute("SELECT COUNT(DISTINCT user_id) n FROM subscriptions WHERE status='active' AND end_at>?",(now_iso(),)).fetchone()["n"]
            pending=c.execute("SELECT COUNT(*) n FROM payment_requests WHERE status='pending'").fetchone()["n"]
            chats=c.execute("SELECT COUNT(*) n FROM chat_messages WHERE deleted=0").fetchone()["n"]
        await q.message.reply_text(f"📊 آمار\n\n👥 کاربران: {users}\n💳 مشترک فعال: {active}\n⏳ پرداخت در انتظار: {pending}\n💬 پیام‌های چت: {chats}")
    elif action=="broadcast":
        context.user_data["admin_mode"]="broadcast"
        await q.message.reply_text("📢 متن پیام برای تمام مشترکین فعال را ارسال کنید.\nبرای لغو /cancel")
    elif action=="message":
        context.user_data["admin_mode"]="message_uid"
        await q.message.reply_text("شناسه عددی کاربر را ارسال کنید.")
    elif action=="block":
        context.user_data["admin_mode"]="block"
        await q.message.reply_text("شناسه عددی کاربر را ارسال کنید؛ اگر مسدود باشد رفع مسدود می‌شود.")
    elif action=="payments":
        with db() as c: rows=c.execute("SELECT * FROM payment_requests WHERE status='pending' ORDER BY id DESC LIMIT 20").fetchall()
        if not rows: await q.message.reply_text("پرداخت در انتظاری وجود ندارد."); return
        for r in rows:
            await q.message.reply_text(f"💳 #{r['id']}\nکاربر: {r['user_id']}\nپلن: {r['days']} روز\nمبلغ: {r['amount']:,}",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ تایید",callback_data=f"pay:approve:{r['id']}"),InlineKeyboardButton("❌ رد",callback_data=f"pay:reject:{r['id']}")]]))
    elif action=="users":
        page=int(parts[2]) if len(parts)>2 else 0
        with db() as c: rows=c.execute("SELECT * FROM users ORDER BY created_at DESC LIMIT 20 OFFSET ?",(page*20,)).fetchall()
        txt="👥 کاربران\n\n"+"\n".join(f"{r['user_id']} | {r['first_name']} | {'🚫' if r['blocked'] else '✅'}" for r in rows)
        await q.message.reply_text(txt or "کاربری نیست.")
    elif action=="support":
        with db() as c: rows=c.execute("SELECT * FROM support_messages WHERE direction='user_to_admin' ORDER BY id DESC LIMIT 20").fetchall()
        if not rows: await q.message.reply_text("پیام پشتیبانی وجود ندارد."); return
        for r in rows:
            await q.message.reply_text(f"📨 #{r['id']} از {r['user_id']}\n{r['message'] or '[رسانه]'}",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ پاسخ",callback_data=f"sup:reply:{r['user_id']}")]]))
    elif action=="chat":
        with db() as c:
            rooms=c.execute("SELECT symbol,COUNT(*) n FROM chat_messages WHERE deleted=0 GROUP BY symbol ORDER BY n DESC").fetchall()
        await q.message.reply_text("💬 اتاق‌های چت\n\n"+("\n".join(f"• {r['symbol']}: {r['n']} پیام" for r in rooms) if rooms else "هنوز پیامی ثبت نشده است."))
    elif action=="reports":
        with db() as c: rows=c.execute("SELECT * FROM chat_reports WHERE status='pending' ORDER BY id DESC LIMIT 20").fetchall()
        await q.message.reply_text("🚨 گزارش‌ها\n\n"+("\n".join(f"#{r['id']} پیام #{r['message_id']} توسط {r['reporter_id']}" for r in rows) if rows else "گزارش جدیدی نیست."))


async def payment_callback(update,context):
    q=update.callback_query; await q.answer()
    if not is_admin(q.from_user.id): return
    _,action,pid=q.data.split(":"); pid=int(pid)
    with db() as c: r=c.execute("SELECT * FROM payment_requests WHERE id=?",(pid,)).fetchone()
    if not r or r["status"]!="pending":
        await q.message.reply_text("این درخواست قبلاً بررسی شده است."); return
    if action=="approve":
        add_subscription(r["user_id"],r["plan"],pid,"manual")
        with db() as c: c.execute("UPDATE payment_requests SET status='approved',reviewed_at=?,reviewed_by=? WHERE id=?",(now_iso(),q.from_user.id,pid))
        try: await context.bot.send_message(r["user_id"],f"✅ پرداخت شما تایید شد و اشتراک {r['days']} روزه فعال گردید.")
        except: pass
        await q.message.reply_text("✅ اشتراک فعال شد.")
    else:
        with db() as c: c.execute("UPDATE payment_requests SET status='rejected',reviewed_at=?,reviewed_by=? WHERE id=?",(now_iso(),q.from_user.id,pid))
        try: await context.bot.send_message(r["user_id"],"❌ رسید پرداخت شما تایید نشد. برای بررسی با پشتیبان تماس بگیرید.")
        except: pass
        await q.message.reply_text("❌ درخواست رد شد.")

# ---------------- ADMIN TEXT ROUTER ----------------
async def admin_text_action(update,context,text):
    uid=update.effective_user.id; mode=context.user_data.get("admin_mode")
    if not is_admin(uid) or not mode: return False
    if text=="/cancel": context.user_data.pop("admin_mode",None); await update.message.reply_text("لغو شد."); return True
    if mode=="broadcast":
        context.user_data.pop("admin_mode",None)
        with db() as c:
            rows=c.execute("""
            SELECT DISTINCT u.user_id FROM users u JOIN subscriptions s ON s.user_id=u.user_id
            WHERE u.blocked=0 AND s.status='active' AND s.end_at>?
            """,(now_iso(),)).fetchall()
        ok=bad=0
        for r in rows:
            try: await context.bot.send_message(r["user_id"],f"📢 پیام مدیر:\n\n{text}"); ok+=1
            except: bad+=1
        await update.message.reply_text(f"✅ ارسال به {ok} مشترک فعال انجام شد.\n❌ ناموفق: {bad}")
        return True
    if mode=="message_uid":
        try: target=int(text)
        except: await update.message.reply_text("شناسه نامعتبر است."); return True
        context.user_data["message_target"]=target; context.user_data["admin_mode"]="message_text"
        await update.message.reply_text("متن پیام را ارسال کنید."); return True
    if mode=="message_text":
        target=context.user_data.pop("message_target",None); context.user_data.pop("admin_mode",None)
        try: await context.bot.send_message(target,f"📨 پیام مدیر:\n\n{text}"); await update.message.reply_text("✅ ارسال شد.")
        except Exception as e: await update.message.reply_text(f"❌ ارسال نشد: {e}")
        return True
    if mode=="block":
        try: target=int(text)
        except: await update.message.reply_text("شناسه نامعتبر است."); return True
        with db() as c:
            r=c.execute("SELECT blocked FROM users WHERE user_id=?",(target,)).fetchone()
            if not r: await update.message.reply_text("کاربر پیدا نشد."); return True
            new=0 if r["blocked"] else 1; c.execute("UPDATE users SET blocked=? WHERE user_id=?",(new,target))
        context.user_data.pop("admin_mode",None)
        await update.message.reply_text(f"{'🚫 مسدود شد' if new else '✅ رفع مسدودی شد'}: {target}")
        return True
    return False

# ---------------- TEXT ROUTER ----------------
async def text_router(update,context):
    if not update.message: return
    ensure_user(update.effective_user)
    uid=update.effective_user.id; text=(update.message.text or "").strip()
    if is_blocked(uid) and not is_admin(uid):
        await update.message.reply_text("🚫 دسترسی شما توسط مدیر محدود شده است."); return
    if text=="/cancel":
        for k in ("support_mode","chat_room","awaiting_asset","payment_plan","admin_mode","admin_reply_to","message_target"):
            context.user_data.pop(k,None)
        await update.message.reply_text("لغو شد.",reply_markup=main_kb(uid)); return
    # Main-menu buttons always have priority over temporary modes such as
    # support, asset-entry and crypto-chat.  Without this rule, a user who
    # was inside a crypto room could accidentally send buttons such as
    # "🪙 ارزهای بیشتر" as chat messages.
    handlers={
        "➕ افزودن دارایی":add_asset_prompt,"📋 واچ‌لیست":watchlist_menu,
        "📊 تحلیل":analysis_prompt,"🚨 سیگنال‌ها":signals_menu,
        "💳 خرید اشتراک":buy_menu,"👤 وضعیت اشتراک":status_menu,
        "🔔 هشدارها":alerts_menu,"🪙 ارزهای بیشتر":more_coins,
        "💬 چت رمز ارز":crypto_chat_menu,"📨 ارتباط با پشتیبان":support_prompt,
        "ℹ️ راهنما":help_text,"👨‍💼 پنل مدیریت":admin_panel,
    }
    if text in handlers:
        for k in ("support_mode","chat_room","awaiting_asset","payment_plan","admin_reply_to","message_target"):
            context.user_data.pop(k,None)
        await handlers[text](update,context)
        return

    if await admin_text_action(update,context): return
    if is_admin(uid) and context.user_data.get("admin_reply_to"):
        await send_support_reply(update,context,text); return
    if context.user_data.get("support_mode"):
        if text:
            context.user_data.pop("support_mode",None)
            await save_support(uid,text,update.message.message_id)
            for aid in ADMIN_IDS:
                try: await context.bot.send_message(aid,f"📨 پیام پشتیبانی از {uid}:\n\n{text}",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("↩️ پاسخ",callback_data=f"sup:reply:{uid}")]]))
                except: pass
            await update.message.reply_text("✅ پیام شما برای پشتیبان ارسال شد.")
        return
    if await process_chat_message(update,context): return
    if context.user_data.get("awaiting_asset"):
        mode=context.user_data.pop("awaiting_asset")
        if mode=="add": await process_add_asset(update,context,text); return
        if mode=="analysis":
            if not has_analysis_access(uid): await update.message.reply_text("🔒 اشتراک فعال لازم است."); return
            await update.message.reply_text("⏳ در حال تحلیل...")
            a=await analyze(text)
            await update.message.reply_text(analysis_text(a),parse_mode=ParseMode.HTML); return

# ---------------- MEDIA ROUTER ----------------
async def media_router(update,context):
    ensure_user(update.effective_user)
    if is_blocked(update.effective_user.id) and not is_admin(update.effective_user.id): return
    if context.user_data.get("support_mode"):
        await support_media(update,context); return
    if update.message.photo:
        await receipt_photo(update,context); return
    await update.message.reply_text("برای این نوع پیام، ابتدا از بخش پشتیبانی استفاده کنید.")

# ---------------- CALLBACK ROUTER ----------------
async def misc_callback(update,context):
    q=update.callback_query
    data=q.data
    if data.startswith("pick:"):
        await q.answer()
        sym=data.split(":")[1]; at=asset_type(sym); add_watch(q.from_user.id,sym,at)
        await q.message.reply_text(f"✅ {sym} به واچ‌لیست اضافه شد."); return
    if data.startswith("wl:del:"):
        await q.answer("حذف شد")
        _,_,at,sym=data.split(":",3); remove_watch(q.from_user.id,sym,at)
        await q.message.edit_text(f"✅ {sym} حذف شد."); return

# ---------------- MAIN ----------------
async def post_init(app):
    global HTTP_SESSION
    init_db()
    try: await app.bot.delete_webhook(drop_pending_updates=False)
    except Exception as e: log.warning("delete webhook: %s",e)
    app.create_task(alert_worker(app))
    log.info("Market Analyzer started")


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
    app.add_handler(CommandHandler("cancel",lambda u,c: text_router(u,c)))
    app.add_handler(CallbackQueryHandler(plan_callback,pattern=r"^plan:"))
    app.add_handler(CallbackQueryHandler(payment_callback,pattern=r"^pay:"))
    app.add_handler(CallbackQueryHandler(support_reply_callback,pattern=r"^sup:reply:"))
    app.add_handler(CallbackQueryHandler(chat_open_callback,pattern=r"^chat:open:"))
    app.add_handler(CallbackQueryHandler(chat_report_callback,pattern=r"^chat:report:\d+$"))
    app.add_handler(CallbackQueryHandler(chat_admin_callback,pattern=r"^chat:(delete|block):\d+$"))
    app.add_handler(CallbackQueryHandler(alert_callback,pattern=r"^alert:"))
    app.add_handler(CallbackQueryHandler(admin_callback,pattern=r"^adm:"))
    app.add_handler(CallbackQueryHandler(misc_callback,pattern=r"^(pick:|wl:del:)"))
    app.add_handler(MessageHandler(filters.PHOTO | filters.Document.ALL | filters.VOICE,media_router))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND,text_router))
    app.run_polling(drop_pending_updates=False,allowed_updates=Update.ALL_TYPES)


if __name__=="__main__":
    main()
