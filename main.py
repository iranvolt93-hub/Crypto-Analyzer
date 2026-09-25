import os
import re
import sqlite3
import logging
import asyncio
from datetime import datetime, timezone, timedelta

import aiohttp
import numpy as np
import pandas as pd
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)

# ============================================================
# CONFIG — simple Railway variables
# ============================================================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "crypto_bot.db").strip() or "crypto_bot.db"
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.isdigit()}
PAYMENT_CARD = os.getenv("PAYMENT_CARD", "").strip()
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "").strip().lstrip("@")
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
ALERT_SECONDS = int(os.getenv("ALERT_SECONDS", "900"))

COINGECKO = "https://api.coingecko.com/api/v3"
YAHOO_XAU = "https://query1.finance.yahoo.com/v8/finance/chart/XAUUSD=X"
GOLD18_URL = os.getenv("GOLD18_URL", "https://www.tgju.org/profile/geram18")

PLANS = {
    "30": (30, 200000, "۱ ماهه"),
    "90": (90, 350000, "۳ ماهه"),
    "180": (180, 500000, "۶ ماهه"),
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("crypto-analyzer-v5")

# ============================================================
# DATABASE — additive / upgrade safe
# ============================================================
def conn():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")
    return c


def now():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


def init_db():
    c = conn()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INTEGER PRIMARY KEY,
        username TEXT DEFAULT '',
        first_name TEXT DEFAULT '',
        created_at TEXT NOT NULL,
        is_blocked INTEGER DEFAULT 0,
        last_seen_at TEXT
    );
    CREATE TABLE IF NOT EXISTS watchlist(
        user_id INTEGER NOT NULL,
        asset_key TEXT NOT NULL,
        symbol TEXT NOT NULL,
        name TEXT NOT NULL,
        asset_type TEXT NOT NULL DEFAULT 'crypto',
        created_at TEXT NOT NULL,
        PRIMARY KEY(user_id, asset_key)
    );
    CREATE TABLE IF NOT EXISTS settings(
        user_id INTEGER PRIMARY KEY,
        alerts_enabled INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS alert_state(
        user_id INTEGER NOT NULL,
        asset_key TEXT NOT NULL,
        last_signal TEXT DEFAULT '',
        updated_at TEXT,
        PRIMARY KEY(user_id, asset_key)
    );
    CREATE TABLE IF NOT EXISTS subscriptions(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        plan_code TEXT NOT NULL,
        plan_title TEXT NOT NULL,
        days INTEGER NOT NULL,
        amount INTEGER NOT NULL,
        starts_at TEXT NOT NULL,
        ends_at TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        source TEXT NOT NULL DEFAULT 'manual',
        payment_request_id INTEGER,
        created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS payment_requests(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        plan_code TEXT NOT NULL,
        amount INTEGER NOT NULL,
        receipt_file_id TEXT,
        receipt_type TEXT DEFAULT 'photo',
        status TEXT NOT NULL DEFAULT 'pending',
        admin_note TEXT DEFAULT '',
        created_at TEXT NOT NULL,
        reviewed_at TEXT
    );
    CREATE TABLE IF NOT EXISTS admin_log(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        admin_id INTEGER NOT NULL,
        action TEXT NOT NULL,
        target_user_id INTEGER,
        details TEXT DEFAULT '',
        created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_sub_user_end ON subscriptions(user_id, ends_at);
    CREATE INDEX IF NOT EXISTS idx_pay_status ON payment_requests(status);
    """)
    # Safe migration from V4 watchlist schema (user_id, coin_id, symbol, name, asset_type, added_at).
    cols = {r[1] for r in c.execute("PRAGMA table_info(watchlist)")}
    if "coin_id" in cols and "asset_key" not in cols:
        c.execute("ALTER TABLE watchlist RENAME TO watchlist_v4_old")
        c.execute("""CREATE TABLE watchlist(
            user_id INTEGER NOT NULL, asset_key TEXT NOT NULL, symbol TEXT NOT NULL,
            name TEXT NOT NULL, asset_type TEXT NOT NULL DEFAULT 'crypto',
            created_at TEXT NOT NULL, PRIMARY KEY(user_id, asset_key)
        )""")
        oldrows=c.execute("SELECT user_id,coin_id,symbol,name,COALESCE(asset_type,'crypto'),COALESCE(added_at,?) FROM watchlist_v4_old",(iso(now()),)).fetchall()
        c.executemany("INSERT OR IGNORE INTO watchlist(user_id,asset_key,symbol,name,asset_type,created_at) VALUES(?,?,?,?,?,?)", oldrows)
        c.execute("DROP TABLE watchlist_v4_old")
    else:
        cols = {r[1] for r in c.execute("PRAGMA table_info(watchlist)")}
        if "created_at" not in cols:
            c.execute("ALTER TABLE watchlist ADD COLUMN created_at TEXT")
            c.execute("UPDATE watchlist SET created_at=? WHERE created_at IS NULL", (iso(now()),))
    c.commit(); c.close()


def ensure_user(user):
    c = conn(); t = iso(now())
    c.execute("""INSERT INTO users(user_id,username,first_name,created_at,is_blocked,last_seen_at)
                 VALUES(?,?,?,?,0,?)
                 ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,
                 first_name=excluded.first_name,last_seen_at=excluded.last_seen_at""",
              (user.id, user.username or "", user.first_name or "", t, t))
    c.execute("INSERT INTO settings(user_id,alerts_enabled) VALUES(?,0) ON CONFLICT(user_id) DO NOTHING", (user.id,))
    c.commit(); c.close()


def is_admin(uid): return uid in ADMIN_IDS


def is_blocked(uid):
    c=conn(); r=c.execute("SELECT is_blocked FROM users WHERE user_id=?",(uid,)).fetchone(); c.close()
    return bool(r and r[0])


def log_admin(admin_id, action, target=None, details=""):
    c=conn(); c.execute("INSERT INTO admin_log(admin_id,action,target_user_id,details,created_at) VALUES(?,?,?,?,?)",
                       (admin_id,action,target,details,iso(now()))); c.commit(); c.close()

# ============================================================
# SUBSCRIPTIONS / PAYMENTS
# ============================================================
def active_sub(uid):
    c=conn(); r=c.execute("""SELECT * FROM subscriptions WHERE user_id=? AND status='active' AND ends_at>?
                             ORDER BY ends_at DESC LIMIT 1""",(uid,iso(now()))).fetchone(); c.close(); return r


def has_sub(uid): return active_sub(uid) is not None


def sub_text(uid):
    s=active_sub(uid)
    if not s: return "❌ اشتراک فعال ندارید."
    end=datetime.fromisoformat(s["ends_at"])
    days=max(0,(end-now()).days)
    return f"✅ اشتراک {s['plan_title']}\n📅 پایان: {end.strftime('%Y-%m-%d %H:%M')} UTC\n⏳ باقی‌مانده: {days} روز"


def add_subscription(uid, code, payment_id=None, source="manual"):
    days, amount, title=PLANS[code]
    c=conn(); current=active_sub(uid)
    start=max(now(), datetime.fromisoformat(current["ends_at"])) if current else now()
    end=start+timedelta(days=days)
    c.execute("""INSERT INTO subscriptions(user_id,plan_code,plan_title,days,amount,starts_at,ends_at,status,source,payment_request_id,created_at)
                 VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
              (uid,code,title,days,amount,iso(start),iso(end),'active',source,payment_id,iso(now())))
    c.commit(); c.close(); return end

# ============================================================
# KEYBOARDS
# ============================================================
def main_kb(admin=False):
    rows=[
        ["➕ افزودن ارز", "📋 واچ‌لیست"],
        ["📊 تحلیل", "📡 سیگنال‌ها"],
        ["💳 خرید اشتراک", "📅 وضعیت اشتراک"],
        ["🔔 هشدار", "ℹ️ راهنما"],
    ]
    if admin: rows.append(["🛠 پنل مدیریت"])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def admin_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 آمار", callback_data="adm:stats"), InlineKeyboardButton("👥 کاربران", callback_data="adm:users")],
        [InlineKeyboardButton("💳 پرداخت‌های در انتظار", callback_data="adm:payments")],
        [InlineKeyboardButton("➕ تمدید دستی", callback_data="adm:extend")],
        [InlineKeyboardButton("🚫 مسدود/رفع", callback_data="adm:block")],
        [InlineKeyboardButton("📣 پیام همگانی", callback_data="adm:broadcast")],
    ])


def plans_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("۱ ماهه — ۲۰۰,۰۰۰ تومان", callback_data="plan:30")],
        [InlineKeyboardButton("۳ ماهه — ۳۵۰,۰۰۰ تومان", callback_data="plan:90")],
        [InlineKeyboardButton("۶ ماهه — ۵۰۰,۰۰۰ تومان", callback_data="plan:180")],
    ])

# ============================================================
# MARKET DATA
# ============================================================
async def get_json(session, url, params=None):
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT), headers={"User-Agent":"Mozilla/5.0"}) as r:
            if r.status != 200: return None
            return await r.json(content_type=None)
    except Exception as e:
        log.warning("HTTP %s: %s", url, e); return None


async def search_crypto(q):
    q=q.strip().lower().replace("usdt","").replace("/","")
    async with aiohttp.ClientSession() as s:
        data=await get_json(s,COINGECKO+"/search",{"query":q})
    out=[]
    for x in (data or {}).get("coins",[])[:10]:
        out.append({"id":x.get("id",""),"symbol":x.get("symbol","" ).upper(),"name":x.get("name","")})
    return out


async def crypto_history(asset_id):
    async with aiohttp.ClientSession() as s:
        d=await get_json(s,COINGECKO+f"/coins/{asset_id}/market_chart",{"vs_currency":"usd","days":"90","interval":"daily"})
    prices=(d or {}).get("prices",[])
    if len(prices)<35: return None
    return pd.DataFrame(prices,columns=["ts","close"])


async def xau_history():
    async with aiohttp.ClientSession() as s:
        d=await get_json(s,YAHOO_XAU,{"range":"3mo","interval":"1d"})
    try:
        q=d["chart"]["result"][0]; closes=q["indicators"]["quote"][0]["close"]
        vals=[float(x) for x in closes if x is not None]
        if len(vals)<35:return None
        return pd.DataFrame({"close":vals})
    except Exception:return None


async def gold18_price():
    async with aiohttp.ClientSession() as s:
        try:
            async with s.get(GOLD18_URL,timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),headers={"User-Agent":"Mozilla/5.0"}) as r:
                text=await r.text(errors="ignore")
        except Exception as e:
            log.warning("gold18: %s",e); return None
    # TGJU pages commonly contain l-geram18 or data-field values. Pick plausible rial/toman price.
    patterns=[r'id=["\']l-geram18["\'][^>]*>\s*([0-9,]+)',r'data-field=["\']geram18["\'][^>]*>\s*([0-9,]+)',r'id=["\']geram18["\'][^>]*>\s*([0-9,]+)']
    for p in patterns:
        m=re.search(p,text,re.I|re.S)
        if m:
            raw=int(m.group(1).replace(",",""))
            if 100_000_000<=raw<=1_000_000_000: raw//=10
            if 1_000_000<=raw<=2_000_000_000:return raw
    return None


async def gold18_history():
    p=await gold18_price()
    if p is None:return None
    # A single live quote is not enough for a technical signal; deliberately refuse fake history.
    return None


async def analysis_for(asset_key, asset_type, name):
    if asset_type=="crypto":
        df=await crypto_history(asset_key)
    elif asset_type=="xau":
        df=await xau_history()
    else:
        df=await gold18_history()
    if df is None or len(df)<35:
        return {"ok":False,"name":name,"reason":"داده کافی برای تحلیل مطمئن در دسترس نیست."}
    close=df["close"].astype(float)
    ema20=close.ewm(span=20,adjust=False).mean(); ema50=close.ewm(span=50,adjust=False).mean()
    delta=close.diff(); gain=delta.clip(lower=0).rolling(14).mean(); loss=(-delta.clip(upper=0)).rolling(14).mean()
    rs=gain/loss.replace(0,np.nan); rsi=(100-(100/(1+rs))).fillna(50)
    ema12=close.ewm(span=12,adjust=False).mean(); ema26=close.ewm(span=26,adjust=False).mean(); macd=ema12-ema26; sig=macd.ewm(span=9,adjust=False).mean()
    score=50.0
    score += 15 if close.iloc[-1]>ema20.iloc[-1] else -15
    score += 15 if ema20.iloc[-1]>ema50.iloc[-1] else -15
    score += 12 if macd.iloc[-1]>sig.iloc[-1] else -12
    score += max(-8,min(8,(float(rsi.iloc[-1])-50)*0.32))
    score=max(0,min(100,score))
    signal="BUY" if score>=62 else "SELL" if score<=38 else "WAIT"
    strength=round(abs(score-50)*2,1)
    probability=round(50+abs(score-50)*0.55,1)
    ret7=(close.iloc[-1]/close.iloc[-8]-1)*100
    ret30=(close.iloc[-1]/close.iloc[-31]-1)*100
    return {"ok":True,"name":name,"price":float(close.iloc[-1]),"signal":signal,"score":round(score,1),"strength":strength,"probability":probability,"rsi":round(float(rsi.iloc[-1]),1),"ema20":float(ema20.iloc[-1]),"ema50":float(ema50.iloc[-1]),"ret7":ret7,"ret30":ret30}

# ============================================================
# UI HELPERS
# ============================================================
async def send_analysis(update, asset):
    if not has_sub(update.effective_user.id):
        await update.message.reply_text("🔒 تحلیل و سیگنال نیاز به اشتراک فعال دارد.\n\n💳 از «خرید اشتراک» استفاده کنید.", reply_markup=main_kb(is_admin(update.effective_user.id))); return
    a=await analysis_for(asset["asset_key"],asset["asset_type"],asset["name"])
    if not a["ok"]:
        await update.message.reply_text(f"⚠️ {a['name']}\n{a['reason']}",reply_markup=main_kb(is_admin(update.effective_user.id))); return
    sym=asset["symbol"]
    await update.message.reply_text(
        f"📊 تحلیل {a['name']} ({sym})\n\n"
        f"💰 قیمت: {a['price']:,.4f}\n"
        f"🎯 سیگنال: {a['signal']}\n"
        f"💪 قدرت سیگنال: {a['strength']}٪\n"
        f"📈 احتمال مدل: {a['probability']}٪\n\n"
        f"RSI14: {a['rsi']}\nEMA20: {a['ema20']:,.4f}\nEMA50: {a['ema50']:,.4f}\n"
        f"📅 تغییر ۷ روزه: {a['ret7']:+.2f}%\n📅 تغییر ۳۰ روزه: {a['ret30']:+.2f}%\n\n"
        f"ℹ️ این خروجی تحلیل داده است و توصیه مالی شخصی نیست.", reply_markup=main_kb(is_admin(update.effective_user.id)))

# ============================================================
# COMMANDS / MAIN MESSAGES
# ============================================================
async def start(update:Update,context:ContextTypes.DEFAULT_TYPE):
    ensure_user(update.effective_user)
    if is_blocked(update.effective_user.id):
        await update.effective_message.reply_text("🚫 دسترسی شما مسدود است."); return
    await update.effective_message.reply_text("سلام 👋\nربات تحلیل بازار آماده است.\n\nاز منوی زیر انتخاب کنید:",reply_markup=main_kb(is_admin(update.effective_user.id)))


async def message_handler(update:Update,context:ContextTypes.DEFAULT_TYPE):
    u=update.effective_user; ensure_user(u)
    if is_blocked(u.id): return
    text=(update.message.text or "").strip()
    state=context.user_data

    if state.get("mode")=="search":
        state.pop("mode",None)
        if text in ("لغو","❌ لغو"): await update.message.reply_text("لغو شد.",reply_markup=main_kb(is_admin(u.id))); return
        if text in ("XAU","طلا جهانی","طلای جهانی"):
            add_asset(u.id,"xau","XAU","طلای جهانی","xau")
            await update.message.reply_text("✅ طلای جهانی به واچ‌لیست اضافه شد.",reply_markup=main_kb(is_admin(u.id))); return
        if text in ("طلای ۱۸ عیار","طلا ۱۸","GOLD18"):
            add_asset(u.id,"gold18","GOLD18","طلای ۱۸ عیار","gold18")
            await update.message.reply_text("✅ طلای ۱۸ عیار به واچ‌لیست اضافه شد.",reply_markup=main_kb(is_admin(u.id))); return
        await update.message.reply_text("🔎 در حال جستجو...")
        results=await search_crypto(text)
        if not results: await update.message.reply_text("❌ ارز پیدا نشد. نماد دیگری بفرستید.",reply_markup=main_kb(is_admin(u.id))); return
        buttons=[[InlineKeyboardButton(f"{x['symbol']} — {x['name']}",callback_data=f"add:{x['id']}:{x['symbol']}")] for x in results]
        await update.message.reply_text("ارز موردنظر را انتخاب کنید:",reply_markup=InlineKeyboardMarkup(buttons)); return

    if state.get("mode")=="receipt":
        if text=="❌ لغو": state.clear(); await update.message.reply_text("لغو شد.",reply_markup=main_kb(is_admin(u.id))); return
        await update.message.reply_text("لطفاً تصویر رسید را به صورت عکس ارسال کنید."); return

    if state.get("mode") in ("extend","extend_plan","block","broadcast"):
        await admin_state_message(update,context); return

    if text=="➕ افزودن ارز":
        state["mode"]="search"; await update.message.reply_text("نماد ارز را بفرستید.\nمثال: BTC\n\nبرای طلا: XAU یا «طلای ۱۸ عیار»",reply_markup=ReplyKeyboardMarkup([["❌ لغو"]],resize_keyboard=True)); return
    if text=="📋 واچ‌لیست": await show_watchlist(update); return
    if text=="📊 تحلیل": await choose_analysis(update); return
    if text=="📡 سیگنال‌ها": await choose_analysis(update); return
    if text=="💳 خرید اشتراک": await update.message.reply_text("پلن اشتراک را انتخاب کنید:",reply_markup=plans_kb()); return
    if text=="📅 وضعیت اشتراک": await update.message.reply_text(sub_text(u.id),reply_markup=main_kb(is_admin(u.id))); return
    if text=="🔔 هشدار": await toggle_alerts(update); return
    if text=="ℹ️ راهنما": await update.message.reply_text("ℹ️ راهنما\n\n➕ ارز را به واچ‌لیست اضافه کنید.\n📊 برای تحلیل باید اشتراک فعال داشته باشید.\n💳 پرداخت به صورت دستی و با ارسال رسید انجام می‌شود.\n\n⚠️ سیگنال‌ها تضمین سود نیستند.",reply_markup=main_kb(is_admin(u.id))); return
    if text=="🛠 پنل مدیریت":
        if is_admin(u.id): await update.message.reply_text("🛠 پنل مدیریت",reply_markup=admin_kb())
        else: await update.message.reply_text("⛔ دسترسی ندارید.",reply_markup=main_kb(False))
        return
    await update.message.reply_text("از منوی پایین انتخاب کنید.",reply_markup=main_kb(is_admin(u.id)))

# ============================================================
# WATCHLIST
# ============================================================
def add_asset(uid,key,symbol,name,atype):
    c=conn(); c.execute("INSERT OR IGNORE INTO watchlist(user_id,asset_key,symbol,name,asset_type,created_at) VALUES(?,?,?,?,?,?)",
                       (uid,key,symbol,name,atype,iso(now()))); c.commit(); c.close()


def get_assets(uid):
    c=conn(); rows=c.execute("SELECT * FROM watchlist WHERE user_id=? ORDER BY created_at",(uid,)).fetchall(); c.close(); return rows


async def show_watchlist(update):
    rows=get_assets(update.effective_user.id)
    if not rows: await update.message.reply_text("📋 واچ‌لیست خالی است.",reply_markup=main_kb(is_admin(update.effective_user.id))); return
    kb=[]
    for r in rows[:100]: kb.append([InlineKeyboardButton(f"{r['symbol']} — {r['name']}",callback_data=f"asset:{r['asset_key']}")])
    await update.message.reply_text("📋 واچ‌لیست شما:",reply_markup=InlineKeyboardMarkup(kb))


async def choose_analysis(update):
    rows=get_assets(update.effective_user.id)
    if not rows: await update.message.reply_text("اول از «➕ افزودن ارز» یک دارایی اضافه کنید.",reply_markup=main_kb(is_admin(update.effective_user.id))); return
    kb=[[InlineKeyboardButton(f"{r['symbol']} — {r['name']}",callback_data=f"analysis:{r['asset_key']}")] for r in rows[:50]]
    await update.message.reply_text("دارایی برای تحلیل را انتخاب کنید:",reply_markup=InlineKeyboardMarkup(kb))

# ============================================================
# CALLBACKS
# ============================================================
async def callback(update:Update,context:ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; uid=q.from_user.id
    await q.answer()
    ensure_user(q.from_user)
    data=q.data or ""
    try:
        if data.startswith("add:"):
            _,aid,sym=data.split(":",2); add_asset(uid,aid,sym,sym,"crypto")
            await q.edit_message_text(f"✅ {sym} به واچ‌لیست اضافه شد.")
            return
        if data.startswith("asset:"):
            key=data.split(":",1)[1]
            c=conn(); r=c.execute("SELECT * FROM watchlist WHERE user_id=? AND asset_key=?",(uid,key)).fetchone(); c.close()
            if not r: await q.edit_message_text("❌ دارایی پیدا نشد."); return
            kb=InlineKeyboardMarkup([[InlineKeyboardButton("📊 تحلیل",callback_data=f"analysis:{key}")],[InlineKeyboardButton("🗑 حذف",callback_data=f"del:{key}")]])
            await q.edit_message_text(f"{r['symbol']} — {r['name']}",reply_markup=kb); return
        if data.startswith("del:"):
            key=data.split(":",1)[1]; c=conn(); c.execute("DELETE FROM watchlist WHERE user_id=? AND asset_key=?",(uid,key)); c.commit(); c.close(); await q.edit_message_text("🗑 از واچ‌لیست حذف شد."); return
        if data.startswith("analysis:"):
            key=data.split(":",1)[1]
            c=conn(); r=c.execute("SELECT * FROM watchlist WHERE user_id=? AND asset_key=?",(uid,key)).fetchone(); c.close()
            if not r: await q.edit_message_text("❌ دارایی پیدا نشد."); return
            atype,sym,name=r['asset_type'],r['symbol'],r['name']
            if not has_sub(uid): await q.edit_message_text("🔒 برای تحلیل اشتراک فعال لازم است."); return
            a=await analysis_for(key,atype,name)
            if not a["ok"]: await q.edit_message_text("⚠️ "+a["reason"]); return
            await q.edit_message_text(f"📊 {name} ({sym})\n\n💰 قیمت: {a['price']:,.4f}\n🎯 سیگنال: {a['signal']}\n💪 قدرت سیگنال: {a['strength']}٪\n📈 احتمال مدل: {a['probability']}٪\n\nRSI14: {a['rsi']}\nEMA20: {a['ema20']:,.4f}\nEMA50: {a['ema50']:,.4f}\n۷ روز: {a['ret7']:+.2f}%\n۳۰ روز: {a['ret30']:+.2f}%")
            return
        if data.startswith("plan:"):
            code=data.split(":",1)[1]
            if code not in PLANS: return
            days,amount,title=PLANS[code]
            context.user_data["pending_plan"]=code
            card=PAYMENT_CARD or "شماره کارت در Railway تنظیم نشده است"
            await q.edit_message_text(f"💳 پلن {title}\n💰 مبلغ: {amount:,} تومان\n\nشماره کارت:\n{card}\n\nپس از پرداخت، عکس رسید را همین‌جا ارسال کنید.",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📷 ارسال رسید",callback_data=f"receipt:{code}")]])); return
        if data.startswith("receipt:"):
            code=data.split(":",1)[1]
            if code not in PLANS:return
            context.user_data["mode"]="receipt"; context.user_data["pending_plan"]=code
            await q.edit_message_text("📷 حالا عکس رسید را ارسال کنید.\nبرای لغو: «❌ لغو»")
            return
        if data.startswith("payok:") or data.startswith("payno:"):
            if not is_admin(uid): await q.answer("دسترسی ندارید",show_alert=True); return
            ok=data.startswith("payok:"); pid=int(data.split(":",1)[1]); await review_payment(q,pid,ok); return
        if data.startswith("adm:"):
            if not is_admin(uid): await q.answer("دسترسی ندارید ⛔",show_alert=True); return
            action=data.split(":",1)[1]; await admin_action(q,context,action); return
    except Exception as e:
        log.exception("callback failed: %s",data)
        try: await q.edit_message_text("⚠️ خطای داخلی. دوباره تلاش کنید.")
        except Exception: pass

# ============================================================
# RECEIPTS / ADMIN
# ============================================================
async def photo_handler(update,context):
    u=update.effective_user; ensure_user(u)
    if context.user_data.get("mode")!="receipt": return
    code=context.user_data.get("pending_plan")
    if code not in PLANS: await update.message.reply_text("پلن مشخص نیست. دوباره از خرید اشتراک شروع کنید."); context.user_data.clear(); return
    pid=await save_payment(u.id,code,update.message.photo[-1].file_id,"photo")
    context.user_data.clear()
    await update.message.reply_text(f"✅ رسید ثبت شد.\n🧾 شماره درخواست: #{pid}\n\nپس از بررسی مدیر، نتیجه اعلام می‌شود.",reply_markup=main_kb(is_admin(u.id)))
    await notify_admins(context,pid)


async def save_payment(uid,code,file_id,typ):
    amount=PLANS[code][1]; c=conn(); cur=c.execute("INSERT INTO payment_requests(user_id,plan_code,amount,receipt_file_id,receipt_type,status,created_at) VALUES(?,?,?,?,?,?,?)",
                         (uid,code,amount,file_id,typ,"pending",iso(now()))); pid=cur.lastrowid; c.commit(); c.close(); return pid


async def notify_admins(context,pid):
    c=conn(); r=c.execute("""SELECT p.*,u.username,u.first_name FROM payment_requests p LEFT JOIN users u ON u.user_id=p.user_id WHERE p.id=?""",(pid,)).fetchone(); c.close()
    if not r:return
    for aid in ADMIN_IDS:
        try:
            text=f"💳 درخواست پرداخت #{pid}\n\n👤 {r['first_name']}\n🆔 {r['user_id']}\n💰 {r['amount']:,} تومان\n📦 پلن: {PLANS[r['plan_code']][2]}"
            kb=InlineKeyboardMarkup([[InlineKeyboardButton("✅ تأیید",callback_data=f"payok:{pid}"),InlineKeyboardButton("❌ رد",callback_data=f"payno:{pid}")]])
            if r["receipt_type"]=="photo": await context.bot.send_photo(aid,r["receipt_file_id"],caption=text,reply_markup=kb)
        except Exception as e: log.warning("notify admin %s: %s",aid,e)


async def review_payment(q,pid,approve):
    c=conn(); r=c.execute("SELECT * FROM payment_requests WHERE id=?",(pid,)).fetchone()
    if not r: c.close(); await q.edit_message_text("❌ درخواست پیدا نشد."); return
    if r["status"]!="pending": c.close(); await q.edit_message_text(f"ℹ️ این درخواست قبلاً بررسی شده: {r['status']}"); return
    status="approved" if approve else "rejected"
    c.execute("UPDATE payment_requests SET status=?,reviewed_at=? WHERE id=?",(status,iso(now()),pid)); c.commit(); c.close()
    if approve:
        end=add_subscription(r["user_id"],r["plan_code"],pid,"manual")
        msg=f"✅ پرداخت #{pid} تأیید شد.\nاشتراک {PLANS[r['plan_code']][2]} فعال شد.\n📅 پایان: {end.strftime('%Y-%m-%d %H:%M')} UTC"
    else: msg=f"❌ پرداخت #{pid} رد شد.\nبرای بررسی مجدد با پشتیبانی تماس بگیرید."
    try: await q.edit_message_reply_markup(reply_markup=None); await q.message.reply_text(""+msg)
    except Exception: pass
    try: await q.get_bot().send_message(r["user_id"],msg,reply_markup=main_kb(False))
    except Exception as e: log.warning("user notify failed: %s",e)
    log_admin(q.from_user.id,status,r["user_id"],f"payment={pid}")


async def admin_action(q,context,action):
    if action=="stats":
        c=conn(); users=c.execute("SELECT COUNT(*) n FROM users").fetchone()[0]; active=c.execute("SELECT COUNT(*) n FROM subscriptions WHERE status='active' AND ends_at> ?",(iso(now()),)).fetchone()[0]; pending=c.execute("SELECT COUNT(*) n FROM payment_requests WHERE status='pending'").fetchone()[0]; c.close()
        await q.edit_message_text(f"📊 آمار\n\n👥 کاربران: {users}\n💳 اشتراک فعال: {active}\n⏳ پرداخت در انتظار: {pending}",reply_markup=admin_kb()); return
    if action=="users":
        c=conn(); rows=c.execute("SELECT user_id,first_name,username,is_blocked FROM users ORDER BY last_seen_at DESC LIMIT 20").fetchall(); c.close()
        txt="👥 آخرین کاربران:\n\n"+"\n".join(f"{r['user_id']} — {r['first_name']} {'🚫' if r['is_blocked'] else '✅'}" for r in rows) if rows else "کاربری نیست."
        await q.edit_message_text(txt,reply_markup=admin_kb()); return
    if action=="payments":
        c=conn(); rows=c.execute("SELECT p.*,u.first_name FROM payment_requests p LEFT JOIN users u ON u.user_id=p.user_id WHERE p.status='pending' ORDER BY p.id DESC LIMIT 10").fetchall(); c.close()
        if not rows: await q.edit_message_text("✅ پرداخت در انتظاری نیست.",reply_markup=admin_kb()); return
        buttons=[[InlineKeyboardButton(f"#{r['id']} | {r['amount']:,} | {r['first_name']}",callback_data=f"pview:{r['id']}")] for r in rows]
        await q.edit_message_text("💳 پرداخت‌های در انتظار:",reply_markup=InlineKeyboardMarkup(buttons+[[InlineKeyboardButton("🔙 پنل",callback_data="adm:home")]])); return
    if action=="extend":
        context.user_data["mode"]="extend"; await q.edit_message_text("🆔 شناسه کاربر را بفرستید."); return
    if action=="block":
        context.user_data["mode"]="block"; await q.edit_message_text("🆔 شناسه کاربر را بفرستید؛ سپس مسدود/رفع می‌کنیم."); return
    if action=="broadcast":
        context.user_data["mode"]="broadcast"; await q.edit_message_text("متن پیام همگانی را بفرستید."); return
    if action=="home": await q.edit_message_text("🛠 پنل مدیریت",reply_markup=admin_kb()); return


async def admin_state_message(update,context):
    uid=update.effective_user.id
    if not is_admin(uid): context.user_data.clear(); return
    mode=context.user_data.get("mode"); text=update.message.text.strip()
    if mode=="extend":
        if not text.isdigit(): await update.message.reply_text("شناسه عددی بفرستید."); return
        context.user_data["target"] = int(text); context.user_data["mode"]="extend_plan"
        await update.message.reply_text("کد پلن را بفرستید: 30 یا 90 یا 180"); return
    if mode=="extend_plan":
        code=text
        target=context.user_data.get("target")
        if code not in PLANS: await update.message.reply_text("کد اشتباه است."); return
        end=add_subscription(target,code,None,"admin_manual"); log_admin(uid,"manual_extend",target,code); context.user_data.clear()
        await update.message.reply_text(f"✅ تمدید انجام شد.\n📅 پایان: {end.strftime('%Y-%m-%d %H:%M')} UTC",reply_markup=main_kb(True));
        try: await context.bot.send_message(target,"✅ اشتراک شما توسط مدیریت تمدید شد.")
        except Exception: pass
        return
    if mode=="block":
        if not text.isdigit(): await update.message.reply_text("شناسه عددی بفرستید."); return
        target=int(text); c=conn(); r=c.execute("SELECT is_blocked FROM users WHERE user_id=?",(target,)).fetchone()
        if not r: c.close(); await update.message.reply_text("کاربر پیدا نشد."); return
        new=0 if r[0] else 1; c.execute("UPDATE users SET is_blocked=? WHERE user_id=?",(new,target)); c.commit(); c.close(); log_admin(uid,"block_toggle",target,str(new)); context.user_data.clear()
        await update.message.reply_text(("🚫 مسدود شد." if new else "✅ رفع مسدودی شد."),reply_markup=main_kb(True)); return
    if mode=="broadcast":
        context.user_data.clear(); c=conn(); ids=[r[0] for r in c.execute("SELECT user_id FROM users WHERE is_blocked=0").fetchall()]; c.close(); sent=0
        for target in ids:
            try: await context.bot.send_message(target,text); sent+=1
            except Exception: pass
            await asyncio.sleep(.04)
        log_admin(uid,"broadcast",None,f"sent={sent}"); await update.message.reply_text(f"📣 ارسال شد: {sent}",reply_markup=main_kb(True))

# payment pending detail callback
async def payment_view(update,context):
    q=update.callback_query
    if not q.data.startswith("pview:"): return False
    if not is_admin(q.from_user.id): await q.answer("دسترسی ندارید",show_alert=True); return True
    pid=int(q.data.split(":",1)[1]); c=conn(); r=c.execute("SELECT p.*,u.first_name,u.username FROM payment_requests p LEFT JOIN users u ON u.user_id=p.user_id WHERE p.id=?",(pid,)).fetchone(); c.close()
    if not r: await q.answer("پیدا نشد",show_alert=True); return True
    if r["receipt_file_id"]:
        caption=f"💳 درخواست #{pid}\n👤 {r['first_name']}\n🆔 {r['user_id']}\n💰 {r['amount']:,} تومان\n📦 {PLANS[r['plan_code']][2]}"
        kb=InlineKeyboardMarkup([[InlineKeyboardButton("✅ تأیید",callback_data=f"payok:{pid}"),InlineKeyboardButton("❌ رد",callback_data=f"payno:{pid}")]])
        await q.message.reply_photo(r["receipt_file_id"],caption=caption,reply_markup=kb)
    return True

# ============================================================
# ALERTS
# ============================================================
async def toggle_alerts(update):
    uid=update.effective_user.id; c=conn(); r=c.execute("SELECT alerts_enabled FROM settings WHERE user_id=?",(uid,)).fetchone(); new=0 if r and r[0] else 1; c.execute("UPDATE settings SET alerts_enabled=? WHERE user_id=?",(new,uid)); c.commit(); c.close()
    await update.message.reply_text(("🔔 هشدارها فعال شد." if new else "🔕 هشدارها غیرفعال شد."),reply_markup=main_kb(is_admin(uid)))


async def alert_loop(app):
    while True:
        try:
            c=conn(); users=c.execute("SELECT user_id FROM settings WHERE alerts_enabled=1").fetchall(); c.close()
            for row in users:
                uid=row[0]
                if not has_sub(uid): continue
                for asset in get_assets(uid)[:20]:
                    a=await analysis_for(asset["asset_key"],asset["asset_type"],asset["name"])
                    if not a.get("ok") or a["signal"]=="WAIT": continue
                    c=conn(); old=c.execute("SELECT last_signal FROM alert_state WHERE user_id=? AND asset_key=?",(uid,asset["asset_key"])).fetchone(); prev=old[0] if old else ""
                    if prev!=a["signal"]:
                        c.execute("INSERT INTO alert_state(user_id,asset_key,last_signal,updated_at) VALUES(?,?,?,?) ON CONFLICT(user_id,asset_key) DO UPDATE SET last_signal=excluded.last_signal,updated_at=excluded.updated_at",(uid,asset["asset_key"],a["signal"],iso(now()))); c.commit(); c.close()
                        try: await app.bot.send_message(uid,f"🔔 هشدار {asset['symbol']}\n🎯 {a['signal']}\n💪 قدرت: {a['strength']}٪")
                        except Exception: pass
        except Exception as e: log.exception("alert loop: %s",e)
        await asyncio.sleep(ALERT_SECONDS)

# ============================================================
# CALLBACK DISPATCH WRAPPER
# ============================================================
async def callback_router(update,context):
    if await payment_view(update,context): await update.callback_query.answer(); return
    await callback(update,context)

# ============================================================
# APP
# ============================================================
def validate_config():
    if not BOT_TOKEN: raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")
    if not ADMIN_IDS: log.warning("ADMIN_IDS is empty — admin panel will be inaccessible")

async def post_init(app):
    init_db()
    app.create_task(alert_loop(app))
    log.info("V5 started | admins=%s", sorted(ADMIN_IDS))


def main():
    validate_config(); init_db()
    app=Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start",start))
    app.add_handler(CallbackQueryHandler(callback_router))
    app.add_handler(MessageHandler(filters.PHOTO,photo_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND,message_handler))
    app.run_polling(allowed_updates=Update.ALL_TYPES,drop_pending_updates=True)

if __name__=="__main__": main()
