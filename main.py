
import os, sqlite3, logging, asyncio, time
from datetime import datetime, timezone, timedelta
import aiohttp
import numpy as np
import pandas as pd

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    ContextTypes, filters
)

# ============================================================
# CONFIG
# ============================================================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "crypto_bot.db")

CG_BASE = "https://api.coingecko.com/api/v3"
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
MAX_WATCHLIST = int(os.getenv("MAX_WATCHLIST", "100"))
ADMIN_IDS = {
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "").strip().lstrip("@")
PAYMENT_CARD = os.getenv(
    "PAYMENT_CARD",
    ""
).strip()

PLANS = {
    "30": {"days": 30, "price": 200000, "title": "۱ ماهه"},
    "90": {"days": 90, "price": 350000, "title": "۳ ماهه"},
    "180": {"days": 180, "price": 500000, "title": "۶ ماهه"},
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("crypto-watchlist")


# ============================================================
# DATABASE
# IMPORTANT: migrations are ADDITIVE. Existing data is preserved.
# ============================================================
def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")
    return c


def column_exists(c, table, column):
    cols = c.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row["name"] == column for row in cols)


def init_db():
    c = db()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS users(
      user_id INTEGER PRIMARY KEY,
      username TEXT,
      first_name TEXT,
      created_at TEXT
    );

    CREATE TABLE IF NOT EXISTS watchlist(
      user_id INTEGER NOT NULL,
      coin_id TEXT NOT NULL,
      symbol TEXT NOT NULL,
      name TEXT NOT NULL,
      added_at TEXT NOT NULL,
      PRIMARY KEY(user_id,coin_id)
    );

    CREATE TABLE IF NOT EXISTS settings(
      user_id INTEGER PRIMARY KEY,
      interval TEXT DEFAULT '1d',
      alerts_enabled INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS alert_state(
      user_id INTEGER,
      coin_id TEXT,
      last_signal TEXT,
      updated_at TEXT,
      PRIMARY KEY(user_id,coin_id)
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
      receipt_type TEXT,
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

    CREATE INDEX IF NOT EXISTS idx_sub_user_end
      ON subscriptions(user_id, ends_at);

    CREATE INDEX IF NOT EXISTS idx_payment_status
      ON payment_requests(status);

    CREATE INDEX IF NOT EXISTS idx_users_created
      ON users(created_at);
    """)

    # Safe migration for older databases.
    # These ALTER statements only add missing columns; nothing is deleted.
    if not column_exists(c, "users", "is_blocked"):
        c.execute("ALTER TABLE users ADD COLUMN is_blocked INTEGER DEFAULT 0")

    if not column_exists(c, "users", "last_seen_at"):
        c.execute("ALTER TABLE users ADD COLUMN last_seen_at TEXT")

    c.commit()
    c.close()


def now_utc():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


def ensure_user(u):
    c = db()
    now = iso(now_utc())
    c.execute("""
        INSERT INTO users(user_id,username,first_name,created_at,is_blocked,last_seen_at)
        VALUES(?,?,?,?,0,?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            last_seen_at=excluded.last_seen_at
    """, (
        u.id, u.username or "", u.first_name or "", now, now
    ))
    c.execute("""
        INSERT INTO settings(user_id)
        VALUES(?)
        ON CONFLICT(user_id) DO NOTHING
    """, (u.id,))
    c.commit()
    c.close()


def is_admin(uid):
    return uid in ADMIN_IDS


def is_blocked(uid):
    c = db()
    row = c.execute(
        "SELECT is_blocked FROM users WHERE user_id=?", (uid,)
    ).fetchone()
    c.close()
    return bool(row and row["is_blocked"])


def log_admin(admin_id, action, target_user_id=None, details=""):
    c = db()
    c.execute("""
        INSERT INTO admin_log(admin_id,action,target_user_id,details,created_at)
        VALUES(?,?,?,?,?)
    """, (admin_id, action, target_user_id, details, iso(now_utc())))
    c.commit()
    c.close()


# ============================================================
# SUBSCRIPTIONS
# ============================================================
def active_subscription(uid):
    c = db()
    row = c.execute("""
        SELECT *
        FROM subscriptions
        WHERE user_id=?
          AND status='active'
          AND ends_at>?
        ORDER BY ends_at DESC
        LIMIT 1
    """, (uid, iso(now_utc()))).fetchone()
    c.close()
    return row


def has_subscription(uid):
    return active_subscription(uid) is not None


def subscription_text(uid):
    sub = active_subscription(uid)
    if not sub:
        return (
            "📅 <b>وضعیت اشتراک</b>\n\n"
            "❌ اشتراک فعال ندارید.\n"
            "برای استفاده از تحلیل و سیگنال، اشتراک تهیه کنید."
        )

    end = datetime.fromisoformat(sub["ends_at"]).astimezone(timezone.utc)
    remaining = max(0, (end - now_utc()).days)
    return (
        "📅 <b>وضعیت اشتراک</b>\n\n"
        f"✅ پلن: <b>{sub['plan_title']}</b>\n"
        f"📆 شروع: {sub['starts_at'][:10]}\n"
        f"⏳ پایان: {sub['ends_at'][:10]}\n"
        f"🔹 حدود {remaining} روز باقی‌مانده"
    )


async def require_subscription(update, ctx):
    uid = update.effective_user.id
    if has_subscription(uid):
        return True

    text = (
        "🔒 <b>این بخش مخصوص کاربران دارای اشتراک است.</b>\n\n"
        "برای دریافت تحلیل، سیگنال و قابلیت‌های تحلیلی ابتدا اشتراک تهیه کنید."
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 خرید اشتراک", callback_data="buy")],
        [InlineKeyboardButton("📅 وضعیت اشتراک", callback_data="substatus")],
    ])

    if update.callback_query:
        await update.callback_query.edit_message_text(
            text, parse_mode="HTML", reply_markup=kb
        )
    elif update.message:
        await update.message.reply_text(
            text, parse_mode="HTML", reply_markup=kb
        )
    return False


def extend_subscription(uid, plan_code, amount=None, source="manual",
                        payment_request_id=None, admin_id=None):
    plan = PLANS[plan_code]
    c = db()
    now = now_utc()

    current = c.execute("""
        SELECT ends_at FROM subscriptions
        WHERE user_id=? AND status='active' AND ends_at>?
        ORDER BY ends_at DESC LIMIT 1
    """, (uid, iso(now))).fetchone()

    if current:
        start = datetime.fromisoformat(current["ends_at"])
    else:
        start = now

    end = start + timedelta(days=plan["days"])

    c.execute("""
        INSERT INTO subscriptions(
            user_id,plan_code,plan_title,days,amount,starts_at,ends_at,
            status,source,payment_request_id,created_at
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
    """, (
        uid, plan_code, plan["title"], plan["days"],
        amount if amount is not None else plan["price"],
        iso(start), iso(end), "active", source,
        payment_request_id, iso(now)
    ))
    sub_id = c.lastrowid

    if payment_request_id:
        c.execute("""
            UPDATE payment_requests
            SET status='approved', reviewed_at=?
            WHERE id=?
        """, (iso(now), payment_request_id))

    c.commit()
    c.close()

    if admin_id:
        log_admin(
            admin_id, "approve_subscription", uid,
            f"plan={plan_code}, subscription_id={sub_id}"
        )
    return sub_id, end


# ============================================================
# KEYBOARDS / MENUS
# ============================================================
def bottom_menu(uid):
    rows = [
        [KeyboardButton("📋 واچ‌لیست"), KeyboardButton("➕ افزودن ارز")],
        [KeyboardButton("📊 تحلیل"), KeyboardButton("📡 سیگنال‌ها")],
        [KeyboardButton("💳 خرید اشتراک"), KeyboardButton("📅 وضعیت اشتراک")],
        [KeyboardButton("🔔 هشدار"), KeyboardButton("ℹ️ راهنما")],
    ]
    if is_admin(uid):
        rows.append([KeyboardButton("🛠 پنل مدیریت")])
    return ReplyKeyboardMarkup(
        rows, resize_keyboard=True, is_persistent=True
    )


def home_inline():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 واچ‌لیست", callback_data="watch"),
         InlineKeyboardButton("➕ افزودن ارز", callback_data="add")],
        [InlineKeyboardButton("📊 تحلیل", callback_data="analyze"),
         InlineKeyboardButton("📡 سیگنال‌ها", callback_data="signals")],
        [InlineKeyboardButton("💳 خرید اشتراک", callback_data="buy"),
         InlineKeyboardButton("📅 وضعیت اشتراک", callback_data="substatus")],
        [InlineKeyboardButton("🔔 هشدار", callback_data="alerts"),
         InlineKeyboardButton("ℹ️ راهنما", callback_data="help")]
    ])


def admin_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 آمار", callback_data="adm:stats"),
         InlineKeyboardButton("👥 کاربران", callback_data="adm:users")],
        [InlineKeyboardButton("💳 پرداخت‌های در انتظار", callback_data="adm:payments")],
        [InlineKeyboardButton("➕ تمدید دستی کاربر", callback_data="adm:extend")],
        [InlineKeyboardButton("🚫 مسدود/رفع مسدودی", callback_data="adm:block")],
        [InlineKeyboardButton("📣 ارسال پیام همگانی", callback_data="adm:broadcast")],
        [InlineKeyboardButton("🏠 منوی اصلی", callback_data="home")]
    ])


# ============================================================
# MARKET API / ANALYSIS
# ============================================================
def watch(uid):
    c = db()
    r = c.execute(
        "SELECT * FROM watchlist WHERE user_id=? ORDER BY added_at",
        (uid,)
    ).fetchall()
    c.close()
    return r


def normalize(s):
    s = s.strip().lower().replace(" ", "").replace("/", "").replace("-", "")
    if s.endswith("usdt"):
        s = s[:-4]
    return s


async def get_json(path, params=None):
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    async with aiohttp.ClientSession(
        timeout=timeout,
        headers={
            "accept": "application/json",
            "user-agent": "CryptoWatchlistBot/3.0"
        }
    ) as s:
        async with s.get(CG_BASE + path, params=params) as r:
            body = await r.text()
            if r.status != 200:
                raise RuntimeError(f"market api {r.status}: {body[:160]}")
            return await r.json()


async def search_coins(query):
    q = normalize(query)
    data = await get_json("/search", {"query": q})
    coins = data.get("coins", [])
    coins = sorted(
        coins,
        key=lambda x: (
            0 if normalize(x.get("symbol", "")) == q else 1,
            x.get("market_cap_rank") or 999999
        )
    )
    return coins[:8]


async def coin_history(coin_id, days=90):
    data = await get_json(
        f"/coins/{coin_id}/market_chart",
        {"vs_currency": "usd", "days": str(days), "interval": "daily"}
    )
    prices = data.get("prices", [])
    if len(prices) < 40:
        raise RuntimeError("داده تاریخی کافی نیست")
    df = pd.DataFrame(prices, columns=["ts", "close"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna()
    return df


def ema(s, n):
    return s.ewm(span=n, adjust=False).mean()


def rsi(s, n=14):
    d = s.diff()
    up = d.clip(lower=0)
    down = -d.clip(upper=0)
    au = up.ewm(alpha=1/n, adjust=False).mean()
    ad = down.ewm(alpha=1/n, adjust=False).mean()
    rs = au / ad.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def macd(s):
    m = ema(s, 12) - ema(s, 26)
    sig = ema(m, 9)
    return m, sig, m - sig


def analyze(df):
    c = df.close
    price = float(c.iloc[-1])
    e20 = float(ema(c, 20).iloc[-1])
    e50 = float(ema(c, 50).iloc[-1])
    rr = float(rsi(c).iloc[-1])
    ml, ms, mh_series = macd(c)
    mh = float(mh_series.iloc[-1])

    ret7 = float((c.iloc[-1] / c.iloc[-8] - 1) * 100)
    ret30 = float((c.iloc[-1] / c.iloc[-31] - 1) * 100)

    score = 50.0
    reasons = []

    if price > e20:
        score += 10
        reasons.append("قیمت بالای EMA20")
    else:
        score -= 10
        reasons.append("قیمت زیر EMA20")

    if e20 > e50:
        score += 12
        reasons.append("EMA20 بالای EMA50")
    else:
        score -= 12
        reasons.append("EMA20 زیر EMA50")

    if mh > 0:
        score += 12
        reasons.append("MACD مثبت")
    else:
        score -= 12
        reasons.append("MACD منفی")

    if 50 <= rr <= 68:
        score += 8
        reasons.append("RSI مثبت")
    elif rr > 72:
        score -= 5
        reasons.append("RSI داغ")
    elif rr < 30:
        score += 3
        reasons.append("RSI اشباع فروش")
    elif rr < 45:
        score -= 6
        reasons.append("RSI ضعیف")

    if ret7 > 2:
        score += 4
    elif ret7 < -2:
        score -= 4

    if ret30 > 5:
        score += 4
    elif ret30 < -5:
        score -= 4

    score = max(0, min(100, score))
    signal = "BUY" if score >= 62 else "SELL" if score <= 38 else "WAIT"

    agreement = sum([price > e20, e20 > e50, mh > 0, rr >= 50])
    probability = max(
        50,
        min(86, 52 + agreement * 7 + (4 if signal != "WAIT" else 0))
    )
    strength = max(45, min(100, abs(score - 50) * 2 + 45))

    vol = float(c.pct_change().rolling(14).std().iloc[-1] or 0.02)
    move = max(price * vol, price * 0.01)

    if signal == "BUY":
        stop = price - 1.5 * move
        targets = [price + move, price + 2 * move, price + 3 * move]
    elif signal == "SELL":
        stop = price + 1.5 * move
        targets = [price - move, price - 2 * move, price - 3 * move]
    else:
        stop = price
        targets = [price + move, price + 2 * move, price - move]

    return dict(
        price=price, ema20=e20, ema50=e50, rsi=rr, macd=mh,
        ret7=ret7, ret30=ret30, score=score, signal=signal,
        strength=strength, probability=probability, stop=stop,
        targets=targets, reasons=reasons
    )


def fp(x):
    if x >= 1000:
        return f"{x:,.2f}"
    if x >= 1:
        return f"{x:,.4f}"
    if x >= .01:
        return f"{x:,.6f}"
    return f"{x:.8f}"


# ============================================================
# BASIC USER FLOW
# ============================================================
async def start(update, ctx):
    ensure_user(update.effective_user)
    if is_blocked(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی شما توسط مدیریت مسدود شده است.")
        return

    await update.message.reply_text(
        "🚀 <b>ربات تحلیل حرفه‌ای کریپتو</b>\n\n"
        "از منوی پایین می‌توانید ارز اضافه کنید، واچ‌لیست را ببینید، "
        "اشتراک تهیه کنید و تحلیل/سیگنال دریافت کنید.",
        parse_mode="HTML",
        reply_markup=bottom_menu(update.effective_user.id)
    )


async def send_home_message(update, ctx):
    await update.message.reply_text(
        "🏠 منوی اصلی",
        reply_markup=bottom_menu(update.effective_user.id)
    )


async def add_prompt_message(update, ctx):
    ctx.user_data["mode"] = "add"
    await update.message.reply_text(
        "➕ نام یا نماد ارز را بفرست.\n"
        "مثال: ZEC ، Bitcoin ، Solana"
    )


async def search_text(update, ctx):
    ensure_user(update.effective_user)

    if is_blocked(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی شما مسدود است.")
        return

    # Payment receipt mode.
    if ctx.user_data.get("mode") == "payment_receipt":
        await update.message.reply_text(
            "📎 لطفاً رسید پرداخت را به صورت <b>عکس</b> ارسال کن.",
            parse_mode="HTML"
        )
        return

    # Admin modes.
    if ctx.user_data.get("mode") in {
        "admin_extend_user", "admin_extend_plan",
        "admin_block_user", "admin_broadcast"
    }:
        await handle_admin_text(update, ctx)
        return

    if ctx.user_data.get("mode") != "add":
        await update.message.reply_text(
            "از منوی پایین یک گزینه انتخاب کن.",
            reply_markup=bottom_menu(update.effective_user.id)
        )
        return

    q = update.message.text.strip()
    try:
        coins = await search_coins(q)
        if not coins:
            await update.message.reply_text("❌ ارز پیدا نشد. نام یا نماد دیگری بفرست.")
            return

        ctx.user_data["mode"] = "select"
        ctx.user_data["search_results"] = {
            str(i): x for i, x in enumerate(coins)
        }

        rows = []
        for i, x in enumerate(coins):
            rank = x.get("market_cap_rank")
            label = f"{x.get('name','?')} ({x.get('symbol','?').upper()})"
            if rank:
                label += f" #{rank}"
            rows.append([
                InlineKeyboardButton(label[:55], callback_data=f"coin:{i}")
            ])
        rows.append([InlineKeyboardButton("❌ لغو", callback_data="home")])

        await update.message.reply_text(
            "🔎 نتایج جستجو:",
            reply_markup=InlineKeyboardMarkup(rows)
        )
    except Exception:
        log.exception("search failed")
        await update.message.reply_text(
            "⚠️ سرویس جستجوی بازار موقتاً پاسخ نمی‌دهد."
        )


async def select_coin(update, ctx, idx):
    coins = ctx.user_data.get("search_results", {})
    x = coins.get(str(idx))
    if not x:
        await update.callback_query.edit_message_text(
            "نتیجه جستجو منقضی شده؛ دوباره جستجو کن.",
            reply_markup=home_inline()
        )
        return

    uid = update.effective_user.id
    cid = x["id"]
    sym = x.get("symbol", "").upper()
    name = x.get("name", "")

    c = db()
    count = c.execute(
        "SELECT COUNT(*) n FROM watchlist WHERE user_id=?", (uid,)
    ).fetchone()["n"]

    if count >= MAX_WATCHLIST:
        c.close()
        await update.callback_query.edit_message_text(
            f"حداکثر {MAX_WATCHLIST} ارز مجاز است.",
            reply_markup=home_inline()
        )
        return

    try:
        c.execute("""
            INSERT INTO watchlist(
                user_id,coin_id,symbol,name,added_at
            ) VALUES(?,?,?,?,?)
        """, (uid, cid, sym, name, iso(now_utc())))
        c.commit()
        msg = f"✅ {name} ({sym}) به واچ‌لیست اضافه شد."
    except sqlite3.IntegrityError:
        msg = f"ℹ️ {name} ({sym}) قبلاً در واچ‌لیست است."
    finally:
        c.close()

    ctx.user_data.clear()
    await update.callback_query.edit_message_text(
        msg, reply_markup=home_inline()
    )


async def show_watch_message(update, ctx):
    rows = watch(update.effective_user.id)
    if not rows:
        txt = "📋 واچ‌لیست شما خالی است."
    else:
        txt = "📋 واچ‌لیست شما:\n\n" + "\n".join(
            f"• {r['name']} ({r['symbol']})" for r in rows
        )
    await update.message.reply_text(
        txt, reply_markup=bottom_menu(update.effective_user.id)
    )


async def remove_menu_message(update, ctx):
    rows = watch(update.effective_user.id)
    if not rows:
        await update.message.reply_text("واچ‌لیست خالی است.")
        return

    kb = [[
        InlineKeyboardButton(
            f"{r['name']} ({r['symbol']})"[:50],
            callback_data=f"del:{r['coin_id']}"
        )
    ] for r in rows]
    kb.append([InlineKeyboardButton("🏠 منو", callback_data="home")])

    await update.message.reply_text(
        "➖ ارز موردنظر را انتخاب کن:",
        reply_markup=InlineKeyboardMarkup(kb)
    )


async def analyze_menu_message(update, ctx):
    if not await require_subscription(update, ctx):
        return

    rows = watch(update.effective_user.id)
    if not rows:
        await update.message.reply_text("ابتدا یک ارز اضافه کن.")
        return

    kb = [[
        InlineKeyboardButton(
            f"{r['name']} ({r['symbol']})"[:50],
            callback_data=f"an:{r['coin_id']}"
        )
    ] for r in rows]
    kb.append([InlineKeyboardButton("🏠 منو", callback_data="home")])

    await update.message.reply_text(
        "📊 ارز موردنظر را انتخاب کن:",
        reply_markup=InlineKeyboardMarkup(kb)
    )


async def do_an(update, ctx, cid):
    if not has_subscription(update.effective_user.id):
        await update.callback_query.edit_message_text(
            "🔒 برای دریافت تحلیل اشتراک فعال لازم است.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 خرید اشتراک", callback_data="buy")],
                [InlineKeyboardButton("🏠 منو", callback_data="home")]
            ])
        )
        return

    await update.callback_query.edit_message_text(
        "⏳ در حال دریافت داده و تحلیل..."
    )

    try:
        rows = watch(update.effective_user.id)
        row = next((r for r in rows if r["coin_id"] == cid), None)
        if not row:
            raise RuntimeError("not in watchlist")

        df = await coin_history(cid, 90)
        a = analyze(df)

        icon = {
            "BUY": "🟢 خرید",
            "SELL": "🔴 فروش",
            "WAIT": "🟡 انتظار"
        }[a["signal"]]

        tg = "\n".join(
            f"{i+1}) {fp(v)}" for i, v in enumerate(a["targets"])
        )

        text = (
            f"📊 <b>{row['name']} ({row['symbol']})</b>\n\n"
            f"💰 قیمت: <code>{fp(a['price'])} USD</code>\n"
            f"📡 سیگنال: <b>{icon}</b>\n\n"
            f"💪 قدرت روند: <b>{a['strength']:.0f}%</b>\n"
            f"🎯 احتمال مدل: <b>{a['probability']:.0f}%</b>\n\n"
            f"📈 RSI: <code>{a['rsi']:.1f}</code>\n"
            f"📊 MACD: <code>{a['macd']:.6g}</code>\n"
            f"📈 EMA20: <code>{fp(a['ema20'])}</code>\n"
            f"📈 EMA50: <code>{fp(a['ema50'])}</code>\n"
            f"📅 بازده 7روزه: <code>{a['ret7']:.2f}%</code>\n"
            f"📅 بازده 30روزه: <code>{a['ret30']:.2f}%</code>\n\n"
            f"🛑 حد ضرر تحلیلی: <code>{fp(a['stop'])}</code>\n"
            f"🎯 اهداف:\n{tg}\n\n"
            f"🧠 دلایل:\n" +
            "\n".join("• " + x for x in a["reasons"]) +
            "\n\n⚠️ تحلیل آماری است و تضمین سود نیست."
        )

        await update.callback_query.edit_message_text(
            text, parse_mode="HTML", reply_markup=home_inline()
        )
    except Exception:
        log.exception("analysis failed")
        await update.callback_query.edit_message_text(
            "⚠️ داده تحلیل این ارز فعلاً در دسترس نیست.",
            reply_markup=home_inline()
        )


async def signals_message(update, ctx):
    if not await require_subscription(update, ctx):
        return

    rows = watch(update.effective_user.id)
    if not rows:
        await update.message.reply_text("واچ‌لیست خالی است.")
        return

    await update.message.reply_text("⏳ در حال بررسی واچ‌لیست...")
    out = ["📡 <b>سیگنال‌های واچ‌لیست</b>\n"]

    for r in rows:
        try:
            a = analyze(await coin_history(r["coin_id"], 90))
            ic = {"BUY": "🟢", "SELL": "🔴", "WAIT": "🟡"}[a["signal"]]
            out.append(
                f"{ic} <b>{r['symbol']}</b> — {a['signal']} | "
                f"قدرت {a['strength']:.0f}% | احتمال مدل {a['probability']:.0f}%"
            )
        except Exception:
            out.append(f"⚪ <b>{r['symbol']}</b> — داده در دسترس نیست")
        await asyncio.sleep(.15)

    await update.message.reply_text(
        "\n".join(out),
        parse_mode="HTML",
        reply_markup=bottom_menu(update.effective_user.id)
    )


async def alerts_message(update, ctx):
    uid = update.effective_user.id
    c = db()
    s = c.execute(
        "SELECT alerts_enabled FROM settings WHERE user_id=?", (uid,)
    ).fetchone()
    new = 0 if s["alerts_enabled"] else 1
    c.execute(
        "UPDATE settings SET alerts_enabled=? WHERE user_id=?",
        (new, uid)
    )
    c.commit()
    c.close()

    await update.message.reply_text(
        f"🔔 هشدارها: <b>{'فعال' if new else 'خاموش'}</b>",
        parse_mode="HTML",
        reply_markup=bottom_menu(uid)
    )


# ============================================================
# SUBSCRIPTION / PAYMENT USER FLOW
# ============================================================
async def buy_menu(update, ctx):
    kb = []
    for code, p in PLANS.items():
        kb.append([InlineKeyboardButton(
            f"💳 {p['title']} — {p['price']:,} تومان",
            callback_data=f"plan:{code}"
        )])
    kb.append([InlineKeyboardButton("🏠 منو", callback_data="home")])

    await update.message.reply_text(
        "💳 <b>خرید اشتراک</b>\n\n"
        "پلن موردنظر را انتخاب کن:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb)
    )


async def plan_selected(update, ctx, code):
    if code not in PLANS:
        return

    p = PLANS[code]
    ctx.user_data["payment_plan"] = code
    ctx.user_data["mode"] = "payment_receipt"

    text = (
        f"💳 <b>پلن {p['title']}</b>\n"
        f"💰 مبلغ: <b>{p['price']:,} تومان</b>\n\n"
        f"🏦 شماره کارت:\n<code>{PAYMENT_CARD}</code>\n\n"
        "پس از پرداخت، <b>عکس رسید</b> را همین‌جا ارسال کن.\n"
        "درخواست برای مدیریت ارسال می‌شود و بعد از تأیید، اشتراک فعال خواهد شد."
    )

    await update.callback_query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ لغو", callback_data="home")]
        ])
    )


async def receipt_photo(update, ctx):
    ensure_user(update.effective_user)

    if ctx.user_data.get("mode") != "payment_receipt":
        await update.message.reply_text(
            "از منوی پایین یک گزینه انتخاب کن.",
            reply_markup=bottom_menu(update.effective_user.id)
        )
        return

    plan_code = ctx.user_data.get("payment_plan")
    if plan_code not in PLANS:
        ctx.user_data.clear()
        await update.message.reply_text("درخواست پرداخت منقضی شد. دوباره اقدام کن.")
        return

    p = PLANS[plan_code]
    photo = update.message.photo[-1]

    c = db()
    cur = c.execute("""
        INSERT INTO payment_requests(
            user_id,plan_code,amount,receipt_file_id,receipt_type,
            status,created_at
        )
        VALUES(?,?,?,?,?,?,?)
    """, (
        update.effective_user.id, plan_code, p["price"],
        photo.file_id, "photo", "pending", iso(now_utc())
    ))
    request_id = cur.lastrowid
    c.commit()
    c.close()

    ctx.user_data.clear()

    await update.message.reply_text(
        "✅ رسید دریافت شد.\n\n"
        f"شماره درخواست: <code>#{request_id}</code>\n"
        "پس از بررسی مدیریت، نتیجه اعلام می‌شود.",
        parse_mode="HTML",
        reply_markup=bottom_menu(update.effective_user.id)
    )

    # Notify all configured admins.
    for admin_id in ADMIN_IDS:
        try:
            await ctx.bot.send_photo(
                chat_id=admin_id,
                photo=photo.file_id,
                caption=(
                    f"💳 <b>درخواست پرداخت جدید #{request_id}</b>\n\n"
                    f"👤 کاربر: <code>{update.effective_user.id}</code>\n"
                    f"🔹 نام: {update.effective_user.first_name or '-'}\n"
                    f"🔹 Username: @{update.effective_user.username or '-'}\n"
                    f"📦 پلن: {p['title']}\n"
                    f"💰 مبلغ: {p['price']:,} تومان"
                ),
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(
                            "✅ تأیید",
                            callback_data=f"payok:{request_id}"
                        ),
                        InlineKeyboardButton(
                            "❌ رد",
                            callback_data=f"payno:{request_id}"
                        )
                    ]
                ])
            )
        except Exception:
            log.exception("admin receipt notify failed")


async def subscription_status_message(update, ctx):
    await update.message.reply_text(
        subscription_text(update.effective_user.id),
        parse_mode="HTML",
        reply_markup=bottom_menu(update.effective_user.id)
    )


# ============================================================
# ADMIN PANEL
# ============================================================
async def admin_panel_message(update, ctx):
    uid = update.effective_user.id
    if not is_admin(uid):
        await update.message.reply_text("⛔ دسترسی مدیریت ندارید.")
        return

    await update.message.reply_text(
        "🛠 <b>پنل مدیریت</b>\n\n"
        "مدیریت کاربران، پرداخت‌ها و اشتراک‌ها از این بخش انجام می‌شود.",
        parse_mode="HTML",
        reply_markup=admin_menu()
    )


async def admin_stats(update, ctx):
    uid = update.effective_user.id
    if not is_admin(uid):
        return

    c = db()
    users = c.execute("SELECT COUNT(*) n FROM users").fetchone()["n"]
    active = c.execute("""
        SELECT COUNT(DISTINCT user_id) n
        FROM subscriptions
        WHERE status='active' AND ends_at>?
    """, (iso(now_utc()),)).fetchone()["n"]
    pending = c.execute("""
        SELECT COUNT(*) n FROM payment_requests WHERE status='pending'
    """).fetchone()["n"]
    total_sub = c.execute(
        "SELECT COUNT(*) n FROM subscriptions"
    ).fetchone()["n"]
    c.close()

    await update.callback_query.edit_message_text(
        "📊 <b>آمار ربات</b>\n\n"
        f"👥 کل کاربران: <b>{users}</b>\n"
        f"✅ اشتراک فعال: <b>{active}</b>\n"
        f"⏳ پرداخت در انتظار: <b>{pending}</b>\n"
        f"📚 کل سوابق اشتراک: <b>{total_sub}</b>",
        parse_mode="HTML",
        reply_markup=admin_menu()
    )


async def admin_users(update, ctx):
    uid = update.effective_user.id
    if not is_admin(uid):
        return

    c = db()
    rows = c.execute("""
        SELECT u.user_id,u.username,u.first_name,
               MAX(s.ends_at) AS ends_at
        FROM users u
        LEFT JOIN subscriptions s
          ON s.user_id=u.user_id
         AND s.status='active'
        GROUP BY u.user_id
        ORDER BY u.created_at DESC
        LIMIT 30
    """).fetchall()
    c.close()

    if not rows:
        text = "👥 کاربری وجود ندارد."
    else:
        parts = ["👥 <b>آخرین کاربران</b>\n"]
        for r in rows:
            status = "فعال" if r["ends_at"] and r["ends_at"] > iso(now_utc()) else "بدون اشتراک"
            name = r["first_name"] or "-"
            parts.append(
                f"• <code>{r['user_id']}</code> | {name} | {status}"
            )
        text = "\n".join(parts)

    await update.callback_query.edit_message_text(
        text, parse_mode="HTML", reply_markup=admin_menu()
    )


async def admin_payments(update, ctx):
    uid = update.effective_user.id
    if not is_admin(uid):
        return

    c = db()
    rows = c.execute("""
        SELECT p.*,u.username,u.first_name
        FROM payment_requests p
        LEFT JOIN users u ON u.user_id=p.user_id
        WHERE p.status='pending'
        ORDER BY p.created_at DESC
        LIMIT 20
    """).fetchall()
    c.close()

    if not rows:
        await update.callback_query.edit_message_text(
            "✅ پرداخت در انتظاری وجود ندارد.",
            reply_markup=admin_menu()
        )
        return

    buttons = []
    for r in rows:
        buttons.append([InlineKeyboardButton(
            f"#{r['id']} | {r['amount']:,} | {r['first_name'] or r['user_id']}",
            callback_data=f"payview:{r['id']}"
        )])
    buttons.append([InlineKeyboardButton("🔙 مدیریت", callback_data="admin")])

    await update.callback_query.edit_message_text(
        "💳 <b>پرداخت‌های در انتظار</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def admin_payment_view(update, ctx, request_id):
    uid = update.effective_user.id
    if not is_admin(uid):
        return

    c = db()
    r = c.execute("""
        SELECT p.*,u.username,u.first_name
        FROM payment_requests p
        LEFT JOIN users u ON u.user_id=p.user_id
        WHERE p.id=?
    """, (request_id,)).fetchone()
    c.close()

    if not r:
        await update.callback_query.edit_message_text(
            "درخواست پیدا نشد.", reply_markup=admin_menu()
        )
        return

    p = PLANS.get(r["plan_code"], {})
    text = (
        f"💳 <b>درخواست #{r['id']}</b>\n\n"
        f"👤 User ID: <code>{r['user_id']}</code>\n"
        f"👤 نام: {r['first_name'] or '-'}\n"
        f"🔹 Username: @{r['username'] or '-'}\n"
        f"📦 پلن: {p.get('title', r['plan_code'])}\n"
        f"💰 مبلغ: {r['amount']:,} تومان\n"
        f"📅 وضعیت: {r['status']}"
    )

    if r["receipt_file_id"]:
        try:
            await ctx.bot.send_photo(
                chat_id=uid,
                photo=r["receipt_file_id"],
                caption=text,
                parse_mode="HTML"
            )
        except Exception:
            pass

    await update.callback_query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "✅ تأیید و فعال‌سازی",
                    callback_data=f"payok:{r['id']}"
                ),
                InlineKeyboardButton(
                    "❌ رد",
                    callback_data=f"payno:{r['id']}"
                )
            ],
            [InlineKeyboardButton("🔙 پرداخت‌ها", callback_data="adm:payments")]
        ])
    )


async def approve_payment(update, ctx, request_id):
    uid = update.effective_user.id
    if not is_admin(uid):
        return

    c = db()
    r = c.execute(
        "SELECT * FROM payment_requests WHERE id=?", (request_id,)
    ).fetchone()
    c.close()

    if not r:
        await update.callback_query.answer("درخواست پیدا نشد.", show_alert=True)
        return

    if r["status"] != "pending":
        await update.callback_query.answer(
            f"این درخواست قبلاً {r['status']} شده است.",
            show_alert=True
        )
        return

    sub_id, end = extend_subscription(
        r["user_id"], r["plan_code"],
        amount=r["amount"],
        source="manual_payment",
        payment_request_id=request_id,
        admin_id=uid
    )

    await update.callback_query.answer("اشتراک فعال شد.", show_alert=True)
    await update.callback_query.edit_message_text(
        f"✅ درخواست #{request_id} تأیید شد.\n"
        f"اشتراک تا {end.astimezone(timezone.utc).date()} فعال است.",
        reply_markup=admin_menu()
    )

    try:
        await ctx.bot.send_message(
            chat_id=r["user_id"],
            text=(
                "🎉 <b>اشتراک شما فعال شد.</b>\n\n"
                f"📦 پلن: {PLANS[r['plan_code']]['title']}\n"
                f"📅 اعتبار تا: {end.astimezone(timezone.utc).date()}\n\n"
                "اکنون می‌توانید از بخش تحلیل و سیگنال استفاده کنید."
            ),
            parse_mode="HTML",
            reply_markup=bottom_menu(r["user_id"])
        )
    except Exception:
        log.exception("notify user after payment approval failed")


async def reject_payment(update, ctx, request_id):
    uid = update.effective_user.id
    if not is_admin(uid):
        return

    c = db()
    r = c.execute(
        "SELECT * FROM payment_requests WHERE id=?", (request_id,)
    ).fetchone()

    if not r:
        c.close()
        await update.callback_query.answer("درخواست پیدا نشد.", show_alert=True)
        return

    c.execute("""
        UPDATE payment_requests
        SET status='rejected',reviewed_at=?
        WHERE id=? AND status='pending'
    """, (iso(now_utc()), request_id))
    c.commit()
    c.close()

    log_admin(uid, "reject_payment", r["user_id"], f"request={request_id}")

    await update.callback_query.answer("درخواست رد شد.", show_alert=True)
    await update.callback_query.edit_message_text(
        f"❌ درخواست #{request_id} رد شد.",
        reply_markup=admin_menu()
    )

    try:
        await ctx.bot.send_message(
            chat_id=r["user_id"],
            text=(
                "❌ <b>درخواست پرداخت شما تأیید نشد.</b>\n\n"
                f"شماره درخواست: #{request_id}\n"
                "در صورت نیاز، مجدداً پرداخت و رسید معتبر ارسال کنید."
            ),
            parse_mode="HTML",
            reply_markup=bottom_menu(r["user_id"])
        )
    except Exception:
        log.exception("notify user after payment rejection failed")


async def admin_extend_start(update, ctx):
    uid = update.effective_user.id
    if not is_admin(uid):
        return
    ctx.user_data["mode"] = "admin_extend_user"
    await update.callback_query.edit_message_text(
        "➕ User ID کاربر را بفرست:"
    )


async def admin_block_start(update, ctx):
    uid = update.effective_user.id
    if not is_admin(uid):
        return
    ctx.user_data["mode"] = "admin_block_user"
    await update.callback_query.edit_message_text(
        "🚫 User ID کاربر را بفرست:"
    )


async def handle_admin_text(update, ctx):
    uid = update.effective_user.id
    mode = ctx.user_data.get("mode")

    if not is_admin(uid):
        return

    if mode == "admin_broadcast":
        text = update.message.text.strip()
        ctx.user_data.clear()

        c = db()
        users = c.execute(
            "SELECT user_id FROM users WHERE is_blocked=0"
        ).fetchall()
        c.close()

        sent = 0
        for r in users:
            try:
                await ctx.bot.send_message(
                    chat_id=r["user_id"], text=text,
                    reply_markup=bottom_menu(r["user_id"])
                )
                sent += 1
                await asyncio.sleep(.04)
            except Exception:
                pass

        log_admin(uid, "broadcast", None, f"sent={sent}")
        await update.message.reply_text(
            f"📣 پیام همگانی ارسال شد.\nموفق: {sent}",
            reply_markup=bottom_menu(uid)
        )
        return

    if mode == "admin_extend_user":
        value = update.message.text.strip()
        if not value.isdigit():
            await update.message.reply_text("فقط User ID عددی بفرست.")
            return

        target = int(value)
        c = db()
        exists = c.execute(
            "SELECT user_id FROM users WHERE user_id=?", (target,)
        ).fetchone()
        c.close()

        if not exists:
            await update.message.reply_text("کاربر پیدا نشد.")
            return

        ctx.user_data["admin_target_user"] = target
        ctx.user_data["mode"] = "admin_extend_plan"

        buttons = [[
            InlineKeyboardButton(
                f"{p['title']} — {p['price']:,}",
                callback_data=f"admextend:{code}"
            )
        ] for code, p in PLANS.items()]

        await update.message.reply_text(
            f"کاربر <code>{target}</code> انتخاب شد.\nپلن را انتخاب کن:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return

    if mode == "admin_block_user":
        value = update.message.text.strip()
        if not value.isdigit():
            await update.message.reply_text("فقط User ID عددی بفرست.")
            return

        target = int(value)
        c = db()
        r = c.execute(
            "SELECT is_blocked FROM users WHERE user_id=?", (target,)
        ).fetchone()
        if not r:
            c.close()
            await update.message.reply_text("کاربر پیدا نشد.")
            return

        new = 0 if r["is_blocked"] else 1
        c.execute(
            "UPDATE users SET is_blocked=? WHERE user_id=?",
            (new, target)
        )
        c.commit()
        c.close()

        log_admin(uid, "block_toggle", target, f"is_blocked={new}")
        ctx.user_data.clear()

        await update.message.reply_text(
            f"کاربر {target}: {'مسدود شد' if new else 'رفع مسدودی شد'}.",
            reply_markup=bottom_menu(uid)
        )


async def admin_broadcast_start(update, ctx):
    uid = update.effective_user.id
    if not is_admin(uid):
        return
    ctx.user_data["mode"] = "admin_broadcast"
    await update.callback_query.edit_message_text(
        "📣 متن پیام همگانی را بفرست:"
    )


async def admin_extend_plan(update, ctx, code):
    uid = update.effective_user.id
    if not is_admin(uid) or code not in PLANS:
        return

    target = ctx.user_data.get("admin_target_user")
    if not target:
        await update.callback_query.edit_message_text(
            "کاربر مشخص نیست.", reply_markup=admin_menu()
        )
        return

    sub_id, end = extend_subscription(
        target, code, source="admin_manual", admin_id=uid
    )

    ctx.user_data.clear()

    await update.callback_query.edit_message_text(
        f"✅ اشتراک کاربر <code>{target}</code> تمدید شد.\n"
        f"📦 پلن: {PLANS[code]['title']}\n"
        f"📅 پایان: {end.astimezone(timezone.utc).date()}",
        parse_mode="HTML",
        reply_markup=admin_menu()
    )

    try:
        await ctx.bot.send_message(
            chat_id=target,
            text=(
                "🎉 <b>اشتراک شما توسط مدیریت فعال/تمدید شد.</b>\n\n"
                f"📦 پلن: {PLANS[code]['title']}\n"
                f"📅 اعتبار تا: {end.astimezone(timezone.utc).date()}"
            ),
            parse_mode="HTML",
            reply_markup=bottom_menu(target)
        )
    except Exception:
        log.exception("notify manual subscription failed")


# ============================================================
# CALLBACK ROUTER
# ============================================================
async def callback(update, ctx):
    q = update.callback_query
    await q.answer()
    ensure_user(q.from_user)

    if is_blocked(q.from_user.id) and not is_admin(q.from_user.id):
        await q.edit_message_text("⛔ دسترسی شما توسط مدیریت مسدود شده است.")
        return

    d = q.data

    if d == "home":
        ctx.user_data.clear()
        await q.edit_message_text("🏠 منوی اصلی", reply_markup=home_inline())

    elif d == "add":
        ctx.user_data["mode"] = "add"
        await q.edit_message_text(
            "➕ نام یا نماد ارز را بفرست.\nمثال: ZEC، Bitcoin، Solana"
        )

    elif d == "watch":
        rows = watch(q.from_user.id)
        txt = (
            "📋 واچ‌لیست شما خالی است."
            if not rows else
            "📋 <b>واچ‌لیست شما:</b>\n\n" +
            "\n".join(
                f"• {r['name']} ({r['symbol']})" for r in rows
            )
        )
        await q.edit_message_text(
            txt, parse_mode="HTML", reply_markup=home_inline()
        )

    elif d == "remove":
        rows = watch(q.from_user.id)
        if not rows:
            await q.edit_message_text(
                "واچ‌لیست خالی است.", reply_markup=home_inline()
            )
            return
        kb = [[InlineKeyboardButton(
            f"{r['name']} ({r['symbol']})"[:50],
            callback_data=f"del:{r['coin_id']}"
        )] for r in rows]
        kb.append([InlineKeyboardButton("🏠 منو", callback_data="home")])
        await q.edit_message_text(
            "➖ ارز موردنظر را انتخاب کن:",
            reply_markup=InlineKeyboardMarkup(kb)
        )

    elif d == "analyze":
        if not has_subscription(q.from_user.id):
            await q.edit_message_text(
                "🔒 برای دریافت تحلیل اشتراک فعال لازم است.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("💳 خرید اشتراک", callback_data="buy")],
                    [InlineKeyboardButton("🏠 منو", callback_data="home")]
                ])
            )
            return

        rows = watch(q.from_user.id)
        if not rows:
            await q.edit_message_text(
                "ابتدا یک ارز اضافه کن.", reply_markup=home_inline()
            )
            return

        kb = [[InlineKeyboardButton(
            f"{r['name']} ({r['symbol']})"[:50],
            callback_data=f"an:{r['coin_id']}"
        )] for r in rows]
        kb.append([InlineKeyboardButton("🏠 منو", callback_data="home")])
        await q.edit_message_text(
            "📊 ارز موردنظر را انتخاب کن:",
            reply_markup=InlineKeyboardMarkup(kb)
        )

    elif d == "signals":
        # Convert callback flow to a message-like response.
        if not has_subscription(q.from_user.id):
            await q.edit_message_text(
                "🔒 برای دریافت سیگنال اشتراک فعال لازم است.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("💳 خرید اشتراک", callback_data="buy")],
                    [InlineKeyboardButton("🏠 منو", callback_data="home")]
                ])
            )
            return

        rows = watch(q.from_user.id)
        if not rows:
            await q.edit_message_text(
                "واچ‌لیست خالی است.", reply_markup=home_inline()
            )
            return

        await q.edit_message_text("⏳ در حال بررسی واچ‌لیست...")
        out = ["📡 <b>سیگنال‌های واچ‌لیست</b>\n"]
        for r in rows:
            try:
                a = analyze(await coin_history(r["coin_id"], 90))
                ic = {"BUY": "🟢", "SELL": "🔴", "WAIT": "🟡"}[a["signal"]]
                out.append(
                    f"{ic} <b>{r['symbol']}</b> — {a['signal']} | "
                    f"قدرت {a['strength']:.0f}% | احتمال مدل {a['probability']:.0f}%"
                )
            except Exception:
                out.append(f"⚪ <b>{r['symbol']}</b> — داده در دسترس نیست")
        await q.edit_message_text(
            "\n".join(out), parse_mode="HTML", reply_markup=home_inline()
        )

    elif d == "alerts":
        uid = q.from_user.id
        c = db()
        s = c.execute(
            "SELECT alerts_enabled FROM settings WHERE user_id=?", (uid,)
        ).fetchone()
        new = 0 if s["alerts_enabled"] else 1
        c.execute(
            "UPDATE settings SET alerts_enabled=? WHERE user_id=?",
            (new, uid)
        )
        c.commit()
        c.close()
        await q.edit_message_text(
            f"🔔 هشدارها: <b>{'فعال' if new else 'خاموش'}</b>",
            parse_mode="HTML", reply_markup=home_inline()
        )

    elif d == "help":
        await q.edit_message_text(
            "ℹ️ <b>راهنما</b>\n\n"
            "➕ ارزها را به واچ‌لیست اضافه کن.\n"
            "📊 تحلیل و 📡 سیگنال فقط با اشتراک فعال هستند.\n"
            "💳 پرداخت به‌صورت دستی و با ارسال رسید انجام می‌شود.\n"
            "📅 وضعیت اشتراک از منوی پایین قابل مشاهده است.",
            parse_mode="HTML", reply_markup=home_inline()
        )

    elif d == "buy":
        kb = []
        for code, p in PLANS.items():
            kb.append([InlineKeyboardButton(
                f"💳 {p['title']} — {p['price']:,} تومان",
                callback_data=f"plan:{code}"
            )])
        kb.append([InlineKeyboardButton("🏠 منو", callback_data="home")])
        await q.edit_message_text(
            "💳 <b>خرید اشتراک</b>\n\nپلن را انتخاب کن:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(kb)
        )

    elif d == "substatus":
        await q.edit_message_text(
            subscription_text(q.from_user.id),
            parse_mode="HTML", reply_markup=home_inline()
        )

    elif d.startswith("plan:"):
        await plan_selected(update, ctx, d.split(":", 1)[1])

    elif d.startswith("coin:"):
        await select_coin(update, ctx, d.split(":", 1)[1])

    elif d.startswith("an:"):
        await do_an(update, ctx, d.split(":", 1)[1])

    elif d.startswith("del:"):
        c = db()
        c.execute(
            "DELETE FROM watchlist WHERE user_id=? AND coin_id=?",
            (q.from_user.id, d.split(":", 1)[1])
        )
        c.commit()
        c.close()
        await q.edit_message_text(
            "✅ از واچ‌لیست حذف شد.", reply_markup=home_inline()
        )

    # ---------------- ADMIN ----------------
    elif d == "admin":
        if is_admin(q.from_user.id):
            await q.edit_message_text(
                "🛠 <b>پنل مدیریت</b>",
                parse_mode="HTML", reply_markup=admin_menu()
            )

    elif d == "adm:stats":
        await admin_stats(update, ctx)

    elif d == "adm:users":
        await admin_users(update, ctx)

    elif d == "adm:payments":
        await admin_payments(update, ctx)

    elif d == "adm:extend":
        await admin_extend_start(update, ctx)

    elif d == "adm:block":
        await admin_block_start(update, ctx)

    elif d == "adm:broadcast":
        await admin_broadcast_start(update, ctx)

    elif d.startswith("payview:"):
        await admin_payment_view(
            update, ctx, int(d.split(":", 1)[1])
        )

    elif d.startswith("payok:"):
        await approve_payment(
            update, ctx, int(d.split(":", 1)[1])
        )

    elif d.startswith("payno:"):
        await reject_payment(
            update, ctx, int(d.split(":", 1)[1])
        )

    elif d.startswith("admextend:"):
        await admin_extend_plan(
            update, ctx, d.split(":", 1)[1]
        )


# ============================================================
# TEXT ROUTER
# ============================================================
async def text_router(update, ctx):
    ensure_user(update.effective_user)

    if is_blocked(update.effective_user.id) and not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی شما توسط مدیریت مسدود شده است.")
        return

    t = (update.message.text or "").strip()

    if t == "📋 واچ‌لیست":
        await show_watch_message(update, ctx)
    elif t == "➕ افزودن ارز":
        await add_prompt_message(update, ctx)
    elif t == "📊 تحلیل":
        await analyze_menu_message(update, ctx)
    elif t == "📡 سیگنال‌ها":
        await signals_message(update, ctx)
    elif t == "💳 خرید اشتراک":
        await buy_menu(update, ctx)
    elif t == "📅 وضعیت اشتراک":
        await subscription_status_message(update, ctx)
    elif t == "🔔 هشدار":
        await alerts_message(update, ctx)
    elif t == "ℹ️ راهنما":
        await update.message.reply_text(
            "ℹ️ برای افزودن ارز، گزینه «➕ افزودن ارز» را بزن.\n"
            "تحلیل و سیگنال نیاز به اشتراک فعال دارند.",
            reply_markup=bottom_menu(update.effective_user.id)
        )
    elif t == "🛠 پنل مدیریت":
        await admin_panel_message(update, ctx)
    else:
        await search_text(update, ctx)


async def photo_router(update, ctx):
    ensure_user(update.effective_user)

    if is_blocked(update.effective_user.id) and not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی شما مسدود است.")
        return

    await receipt_photo(update, ctx)


# ============================================================
# MAIN
# ============================================================
def main():
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(callback))

    # Photos are handled before general text.
    app.add_handler(MessageHandler(filters.PHOTO, photo_router))
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, text_router
    ))

    log.info("Crypto bot v3 started. DB=%s Admins=%s", DB_PATH, sorted(ADMIN_IDS))
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
