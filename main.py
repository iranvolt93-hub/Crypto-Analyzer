import os, sqlite3, logging, asyncio, re
from datetime import datetime, timezone, timedelta

import aiohttp
import numpy as np
import pandas as pd

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters
)

# ============================================================
# CONFIG
# ============================================================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "crypto_bot.db")

COINGECKO = "https://api.coingecko.com/api/v3"
YAHOO_CHART = "https://query1.finance.yahoo.com/v8/finance/chart/XAUUSD=X"
GOLD18_URL = os.getenv(
    "GOLD18_URL", "https://www.tgju.org/profile/geram18"
)

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "20"))
MAX_WATCHLIST = int(os.getenv("MAX_WATCHLIST", "100"))
ALERT_SECONDS = int(os.getenv("ALERT_SECONDS", "900"))

ADMIN_IDS = {
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}
PAYMENT_CARD = os.getenv("PAYMENT_CARD", "").strip()
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "").strip().lstrip("@")

PLANS = {
    "30": {"days": 30, "price": 200000, "title": "۱ ماهه"},
    "90": {"days": 90, "price": 350000, "title": "۳ ماهه"},
    "180": {"days": 180, "price": 500000, "title": "۶ ماهه"},
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("crypto-analyzer")


# ============================================================
# DATABASE
# ============================================================
def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")
    return c


def col_exists(c, table, col):
    return any(x["name"] == col for x in
               c.execute(f"PRAGMA table_info({table})").fetchall())


def init_db():
    c = db()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        created_at TEXT,
        is_blocked INTEGER DEFAULT 0,
        last_seen_at TEXT
    );

    CREATE TABLE IF NOT EXISTS watchlist(
        user_id INTEGER NOT NULL,
        coin_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        name TEXT NOT NULL,
        asset_type TEXT DEFAULT 'crypto',
        added_at TEXT NOT NULL,
        PRIMARY KEY(user_id, coin_id)
    );

    CREATE TABLE IF NOT EXISTS settings(
        user_id INTEGER PRIMARY KEY,
        alerts_enabled INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS alert_state(
        user_id INTEGER,
        coin_id TEXT,
        last_signal TEXT,
        updated_at TEXT,
        PRIMARY KEY(user_id, coin_id)
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
    """)

    # Additive migrations: never delete old data.
    if not col_exists(c, "watchlist", "asset_type"):
        c.execute(
            "ALTER TABLE watchlist ADD COLUMN asset_type TEXT DEFAULT 'crypto'"
        )
    if not col_exists(c, "users", "is_blocked"):
        c.execute(
            "ALTER TABLE users ADD COLUMN is_blocked INTEGER DEFAULT 0"
        )
    if not col_exists(c, "users", "last_seen_at"):
        c.execute(
            "ALTER TABLE users ADD COLUMN last_seen_at TEXT"
        )

    c.commit()
    c.close()


def now():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


def ensure_user(u):
    c = db()
    n = iso(now())
    c.execute("""
        INSERT INTO users(
            user_id,username,first_name,created_at,is_blocked,last_seen_at
        )
        VALUES(?,?,?,?,0,?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            last_seen_at=excluded.last_seen_at
    """, (u.id, u.username or "", u.first_name or "", n, n))
    c.execute("""
        INSERT INTO settings(user_id,alerts_enabled)
        VALUES(?,0)
        ON CONFLICT(user_id) DO NOTHING
    """, (u.id,))
    c.commit()
    c.close()


def is_admin(uid):
    return uid in ADMIN_IDS


def blocked(uid):
    c = db()
    r = c.execute(
        "SELECT is_blocked FROM users WHERE user_id=?", (uid,)
    ).fetchone()
    c.close()
    return bool(r and r["is_blocked"])


def admin_log(admin, action, target=None, details=""):
    c = db()
    c.execute("""
        INSERT INTO admin_log(
            admin_id,action,target_user_id,details,created_at
        ) VALUES(?,?,?,?,?)
    """, (admin, action, target, details, iso(now())))
    c.commit()
    c.close()


# ============================================================
# SUBSCRIPTIONS
# ============================================================
def active_sub(uid):
    c = db()
    r = c.execute("""
        SELECT * FROM subscriptions
        WHERE user_id=? AND status='active' AND ends_at>?
        ORDER BY ends_at DESC LIMIT 1
    """, (uid, iso(now()))).fetchone()
    c.close()
    return r


def has_sub(uid):
    return active_sub(uid) is not None


def sub_text(uid):
    s = active_sub(uid)
    if not s:
        return (
            "📅 <b>وضعیت اشتراک</b>\n\n"
            "❌ اشتراک فعال ندارید."
        )
    end = datetime.fromisoformat(s["ends_at"])
    days = max(0, (end - now()).days)
    return (
        "📅 <b>وضعیت اشتراک</b>\n\n"
        f"📦 پلن: <b>{s['plan_title']}</b>\n"
        f"📆 شروع: {s['starts_at'][:10]}\n"
        f"⏳ پایان: {s['ends_at'][:10]}\n"
        f"🔹 حدود {days} روز باقی‌مانده"
    )


def extend_sub(uid, code, amount=None, source="manual",
               payment_request_id=None, admin_id=None):
    p = PLANS[code]
    c = db()
    t = now()

    r = c.execute("""
        SELECT ends_at FROM subscriptions
        WHERE user_id=? AND status='active' AND ends_at>?
        ORDER BY ends_at DESC LIMIT 1
    """, (uid, iso(t))).fetchone()

    start = datetime.fromisoformat(r["ends_at"]) if r else t
    end = start + timedelta(days=p["days"])

    c.execute("""
        INSERT INTO subscriptions(
            user_id,plan_code,plan_title,days,amount,
            starts_at,ends_at,status,source,payment_request_id,created_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
    """, (
        uid, code, p["title"], p["days"],
        amount if amount is not None else p["price"],
        iso(start), iso(end), "active", source,
        payment_request_id, iso(t)
    ))
    sid = c.lastrowid

    if payment_request_id:
        c.execute("""
            UPDATE payment_requests
            SET status='approved',reviewed_at=?
            WHERE id=? AND status='pending'
        """, (iso(t), payment_request_id))

    c.commit()
    c.close()

    if admin_id:
        admin_log(
            admin_id, "approve_subscription", uid,
            f"plan={code},subscription_id={sid}"
        )
    return end


# ============================================================
# MENUS
# ============================================================
def bottom(uid):
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


def home():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 واچ‌لیست", callback_data="watch"),
         InlineKeyboardButton("➕ افزودن ارز", callback_data="add")],
        [InlineKeyboardButton("📊 تحلیل", callback_data="analyze"),
         InlineKeyboardButton("📡 سیگنال‌ها", callback_data="signals")],
        [InlineKeyboardButton("💳 خرید اشتراک", callback_data="buy"),
         InlineKeyboardButton("📅 اشتراک", callback_data="substatus")],
        [InlineKeyboardButton("🔔 هشدار", callback_data="alerts"),
         InlineKeyboardButton("ℹ️ راهنما", callback_data="help")]
    ])


def admin_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 آمار", callback_data="adm:stats"),
         InlineKeyboardButton("👥 کاربران", callback_data="adm:users")],
        [InlineKeyboardButton("💳 پرداخت‌ها", callback_data="adm:payments")],
        [InlineKeyboardButton("➕ تمدید دستی", callback_data="adm:extend")],
        [InlineKeyboardButton("🚫 مسدود/رفع", callback_data="adm:block")],
        [InlineKeyboardButton("📣 پیام همگانی", callback_data="adm:broadcast")],
        [InlineKeyboardButton("🏠 منو", callback_data="home")]
    ])


# ============================================================
# HTTP
# ============================================================
async def get_json(url, params=None):
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    headers = {
        "User-Agent": "CryptoAnalyzerBot/4.0",
        "Accept": "application/json"
    }
    async with aiohttp.ClientSession(
        timeout=timeout, headers=headers
    ) as s:
        async with s.get(url, params=params) as r:
            text = await r.text()
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}: {text[:120]}")
            return await r.json()


async def get_text(url):
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    headers = {"User-Agent": "Mozilla/5.0 CryptoAnalyzerBot/4.0"}
    async with aiohttp.ClientSession(
        timeout=timeout, headers=headers
    ) as s:
        async with s.get(url) as r:
            text = await r.text()
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}")
            return text


# ============================================================
# ASSETS
# ============================================================
def normalize(x):
    return (
        x.strip().lower()
        .replace(" ", "")
        .replace("/", "")
        .replace("-", "")
    )


async def search_crypto(q):
    qn = normalize(q)
    if qn in {"xau", "xauusd", "gold", "goldusd", "طلای جهانی"}:
        return [{
            "id": "xauusd",
            "symbol": "XAU",
            "name": "طلای جهانی",
            "asset_type": "xau"
        }]
    if qn in {
        "geram18", "gold18", "18k", "18karat",
        "طلای18", "طلای18عیار", "طلا18"
    }:
        return [{
            "id": "geram18",
            "symbol": "18K",
            "name": "طلای ۱۸ عیار ایران",
            "asset_type": "gold18"
        }]

    data = await get_json(
        COINGECKO + "/search", {"query": q.strip()}
    )
    coins = data.get("coins", [])
    coins = sorted(
        coins,
        key=lambda x: (
            0 if normalize(x.get("symbol", "")) == qn else 1,
            x.get("market_cap_rank") or 999999
        )
    )
    return [{
        "id": x["id"],
        "symbol": x.get("symbol", "").upper(),
        "name": x.get("name", ""),
        "asset_type": "crypto"
    } for x in coins[:8]]


async def crypto_history(cid, days=120):
    data = await get_json(
        f"{COINGECKO}/coins/{cid}/market_chart",
        {"vs_currency": "usd", "days": str(days), "interval": "daily"}
    )
    p = data.get("prices", [])
    if len(p) < 60:
        raise RuntimeError("داده تاریخی کافی نیست")
    df = pd.DataFrame(p, columns=["ts", "close"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df.dropna()[["ts", "close"]]


async def xau_history():
    data = await get_json(
        YAHOO_CHART,
        {"range": "6mo", "interval": "1d"}
    )
    result = data["chart"]["result"][0]
    ts = result["timestamp"]
    close = result["indicators"]["quote"][0]["close"]
    df = pd.DataFrame({"ts": ts, "close": close})
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df.dropna()


def parse_number(s):
    s = (
        s.replace(",", "")
        .replace("٬", "")
        .replace("٫", ".")
        .replace(" ", "")
    )
    persian = "۰۱۲۳۴۵۶۷۸۹"
    arabic = "٠١٢٣٤٥٦٧٨٩"
    for a, b in zip(persian, "0123456789"):
        s = s.replace(a, b)
    for a, b in zip(arabic, "0123456789"):
        s = s.replace(a, b)
    m = re.search(r"\d+(?:\.\d+)?", s)
    return float(m.group()) if m else None


async def gold18_current():
    html = await get_text(GOLD18_URL)

    # Common TGJU current-price patterns.
    patterns = [
        r'id=["\']l-geram18["\'][^>]*>\s*([\d,٬]+)',
        r'id=["\']geram18["\'][^>]*>\s*([\d,٬]+)',
        r'data-field=["\']geram18["\'][^>]*>\s*([\d,٬]+)',
    ]

    value = None
    for pattern in patterns:
        m = re.search(pattern, html, re.I | re.S)
        if m:
            value = parse_number(m.group(1))
            break

    if value is None:
        # Fallback: look around "گرم طلای 18 عیار".
        m = re.search(
            r'(?:طلای ۱۸ عیار|طلای 18 عیار).*?([\d,٬]{7,})',
            html, re.I | re.S
        )
        if m:
            value = parse_number(m.group(1))

    if value is None or value <= 0:
        raise RuntimeError("قیمت طلای ۱۸ عیار پیدا نشد")

    # TGJU may expose the value in rial. Convert to toman when
    # the value is in the usual rial-sized range.
    if value >= 100_000_000:
        value /= 10

    return value


async def gold18_history():
    # TGJU's current page is used only for the current price.
    # We intentionally do not invent historical candles.
    price = await gold18_current()
    return pd.DataFrame({
        "ts": [int(now().timestamp() * 1000)],
        "close": [price]
    })


# ============================================================
# INDICATORS / ANALYSIS
# ============================================================
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


def model_probability(c, signal):
    # Simple historical directional hit-rate.
    # It is deliberately separate from current trend strength.
    if len(c) < 50 or signal == "WAIT":
        return 50.0

    hits = []
    for i in range(35, len(c) - 5):
        e20 = ema(c.iloc[:i], 20).iloc[-1]
        e50 = ema(c.iloc[:i], 50).iloc[-1]
        m, _, h = macd(c.iloc[:i])
        direction = 1 if (
            c.iloc[i-1] > e20 and e20 > e50 and h.iloc[-1] > 0
        ) else -1

        future = float(c.iloc[i+4] / c.iloc[i-1] - 1)
        if direction == 1:
            hits.append(future > 0)
        else:
            hits.append(future < 0)

    if not hits:
        return 50.0

    rate = sum(hits) / len(hits)
    return round(50 + (rate - 0.5) * 100, 1)


def analyze(df):
    c = df["close"].astype(float).reset_index(drop=True)

    if len(c) < 40:
        raise RuntimeError("داده تاریخی کافی نیست")

    price = float(c.iloc[-1])
    e20 = float(ema(c, 20).iloc[-1])
    e50 = float(ema(c, 50).iloc[-1])
    rv = float(rsi(c).iloc[-1])

    ml, ms, hist = macd(c)
    mh = float(hist.iloc[-1])

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

    if 50 <= rv <= 68:
        score += 8
        reasons.append("RSI در محدوده مثبت")
    elif rv > 72:
        score -= 5
        reasons.append("RSI داغ")
    elif rv < 30:
        score += 3
        reasons.append("RSI اشباع فروش")
    elif rv < 45:
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

    # Strength = current technical agreement.
    strength = max(
        45,
        min(100, 50 + abs(score - 50) * 1.7)
    )

    # Probability = historical model hit-rate, not the strength score.
    probability = model_probability(c, signal)

    volatility = float(c.pct_change().rolling(14).std().iloc[-1])
    if not np.isfinite(volatility) or volatility <= 0:
        volatility = 0.02

    move = max(price * volatility, price * 0.01)

    if signal == "BUY":
        stop = price - 1.5 * move
        targets = [
            price + move,
            price + 2 * move,
            price + 3 * move
        ]
    elif signal == "SELL":
        stop = price + 1.5 * move
        targets = [
            price - move,
            price - 2 * move,
            price - 3 * move
        ]
    else:
        stop = price
        targets = [price + move, price - move]

    return {
        "price": price,
        "ema20": e20,
        "ema50": e50,
        "rsi": rv,
        "macd": mh,
        "ret7": ret7,
        "ret30": ret30,
        "score": score,
        "signal": signal,
        "strength": strength,
        "probability": probability,
        "stop": stop,
        "targets": targets,
        "reasons": reasons
    }


def fp(x):
    if x >= 1000:
        return f"{x:,.2f}"
    if x >= 1:
        return f"{x:,.4f}"
    if x >= 0.01:
        return f"{x:,.6f}"
    return f"{x:.8f}"


async def asset_history(asset_type, asset_id):
    if asset_type == "crypto":
        return await crypto_history(asset_id)
    if asset_type == "xau":
        return await xau_history()
    if asset_type == "gold18":
        return await gold18_history()
    raise RuntimeError("دارایی ناشناخته")


async def analyze_asset(asset_type, asset_id):
    df = await asset_history(asset_type, asset_id)
    if asset_type == "gold18":
        # One live price is not enough for a trustworthy trend signal.
        raise RuntimeError(
            "برای طلای ۱۸ عیار فعلاً داده تاریخی کافی برای سیگنال وجود ندارد"
        )
    return analyze(df)


# ============================================================
# WATCHLIST
# ============================================================
def watch(uid):
    c = db()
    rows = c.execute("""
        SELECT * FROM watchlist
        WHERE user_id=? ORDER BY added_at
    """, (uid,)).fetchall()
    c.close()
    return rows


# ============================================================
# USER FLOW
# ============================================================
async def start(update, ctx):
    ensure_user(update.effective_user)

    if blocked(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی شما مسدود است.")
        return

    await update.message.reply_text(
        "🚀 <b>Crypto Analyzer V4</b>\n\n"
        "کریپتو، طلای جهانی و طلای ۱۸ عیار را مدیریت کن.\n"
        "از منوی پایین یک گزینه انتخاب کن.",
        parse_mode="HTML",
        reply_markup=bottom(update.effective_user.id)
    )


async def add_prompt(update, ctx):
    ctx.user_data["mode"] = "add"
    await update.message.reply_text(
        "➕ نماد یا نام دارایی را بفرست.\n\n"
        "مثال:\n"
        "BTC\nZEC\nSolana\nXAU\nطلای ۱۸ عیار"
    )


async def search_text(update, ctx):
    ensure_user(update.effective_user)

    if blocked(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی شما مسدود است.")
        return

    mode = ctx.user_data.get("mode")

    if mode == "payment_receipt":
        await update.message.reply_text(
            "📎 لطفاً عکس رسید را ارسال کن."
        )
        return

    if mode in {
        "admin_extend_user", "admin_block_user", "admin_broadcast"
    }:
        await admin_text(update, ctx)
        return

    if mode != "add":
        await update.message.reply_text(
            "از منوی پایین یک گزینه انتخاب کن.",
            reply_markup=bottom(update.effective_user.id)
        )
        return

    try:
        results = await search_crypto(update.message.text.strip())
        if not results:
            await update.message.reply_text("❌ دارایی پیدا نشد.")
            return

        ctx.user_data["results"] = {
            str(i): x for i, x in enumerate(results)
        }

        buttons = []
        for i, x in enumerate(results):
            buttons.append([InlineKeyboardButton(
                f"{x['name']} ({x['symbol']})"[:55],
                callback_data=f"coin:{i}"
            )])
        buttons.append([InlineKeyboardButton(
            "❌ لغو", callback_data="home"
        )])

        await update.message.reply_text(
            "🔎 دارایی را انتخاب کن:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        ctx.user_data["mode"] = "select"

    except Exception:
        log.exception("search failed")
        await update.message.reply_text(
            "⚠️ سرویس بازار فعلاً پاسخ نمی‌دهد."
        )


async def select_coin(update, ctx, idx):
    x = ctx.user_data.get("results", {}).get(str(idx))
    if not x:
        await update.callback_query.edit_message_text(
            "نتیجه منقضی شده. دوباره جستجو کن.",
            reply_markup=home()
        )
        return

    uid = update.effective_user.id
    c = db()

    count = c.execute(
        "SELECT COUNT(*) n FROM watchlist WHERE user_id=?",
        (uid,)
    ).fetchone()["n"]

    if count >= MAX_WATCHLIST:
        c.close()
        await update.callback_query.edit_message_text(
            f"حداکثر {MAX_WATCHLIST} دارایی مجاز است.",
            reply_markup=home()
        )
        return

    try:
        c.execute("""
            INSERT INTO watchlist(
                user_id,coin_id,symbol,name,asset_type,added_at
            ) VALUES(?,?,?,?,?,?)
        """, (
            uid, x["id"], x["symbol"], x["name"],
            x["asset_type"], iso(now())
        ))
        c.commit()
        msg = f"✅ {x['name']} ({x['symbol']}) اضافه شد."
    except sqlite3.IntegrityError:
        msg = f"ℹ️ {x['name']} قبلاً در واچ‌لیست است."
    finally:
        c.close()

    ctx.user_data.clear()
    await update.callback_query.edit_message_text(
        msg, reply_markup=home()
    )


async def show_watch(update, ctx):
    rows = watch(update.effective_user.id)
    if not rows:
        text = "📋 واچ‌لیست خالی است."
    else:
        lines = ["📋 <b>واچ‌لیست شما</b>\n"]
        for r in rows:
            icon = {
                "crypto": "🪙",
                "xau": "🥇",
                "gold18": "🟡"
            }.get(r["asset_type"], "•")
            lines.append(
                f"{icon} {r['name']} ({r['symbol']})"
            )
        text = "\n".join(lines)

    await update.message.reply_text(
        text, parse_mode="HTML",
        reply_markup=bottom(update.effective_user.id)
    )


async def analysis_menu(update, ctx):
    if not has_sub(update.effective_user.id):
        await update.message.reply_text(
            "🔒 برای تحلیل اشتراک فعال لازم است.",
            reply_markup=bottom(update.effective_user.id)
        )
        return

    rows = watch(update.effective_user.id)
    if not rows:
        await update.message.reply_text("ابتدا یک دارایی اضافه کن.")
        return

    buttons = [[InlineKeyboardButton(
        f"{r['name']} ({r['symbol']})"[:55],
        callback_data=f"an:{r['coin_id']}"
    )] for r in rows]
    buttons.append([InlineKeyboardButton(
        "🏠 منو", callback_data="home"
    )])

    await update.message.reply_text(
        "📊 دارایی را انتخاب کن:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def do_analysis(update, ctx, cid):
    uid = update.effective_user.id

    if not has_sub(uid):
        await update.callback_query.edit_message_text(
            "🔒 اشتراک فعال لازم است.",
            reply_markup=home()
        )
        return

    row = next(
        (r for r in watch(uid) if r["coin_id"] == cid), None
    )
    if not row:
        await update.callback_query.edit_message_text(
            "دارایی در واچ‌لیست نیست.", reply_markup=home()
        )
        return

    await update.callback_query.edit_message_text(
        "⏳ در حال تحلیل..."
    )

    try:
        a = await analyze_asset(row["asset_type"], cid)

        signal_text = {
            "BUY": "🟢 خرید",
            "SELL": "🔴 فروش",
            "WAIT": "🟡 انتظار"
        }[a["signal"]]

        targets = "\n".join(
            f"{i+1}) {fp(v)}"
            for i, v in enumerate(a["targets"])
        )

        text = (
            f"📊 <b>{row['name']} ({row['symbol']})</b>\n\n"
            f"💰 قیمت: <code>{fp(a['price'])} USD</code>\n"
            f"📡 سیگنال: <b>{signal_text}</b>\n\n"
            f"💪 قدرت روند: <b>{a['strength']:.0f}%</b>\n"
            f"🎯 احتمال مدل: <b>{a['probability']:.0f}%</b>\n\n"
            f"RSI: <code>{a['rsi']:.1f}</code>\n"
            f"MACD: <code>{a['macd']:.6g}</code>\n"
            f"EMA20: <code>{fp(a['ema20'])}</code>\n"
            f"EMA50: <code>{fp(a['ema50'])}</code>\n"
            f"بازده 7 روزه: <code>{a['ret7']:.2f}%</code>\n"
            f"بازده 30 روزه: <code>{a['ret30']:.2f}%</code>\n\n"
            f"🛑 حد ضرر تحلیلی: <code>{fp(a['stop'])}</code>\n"
            f"🎯 اهداف:\n{targets}\n\n"
            "🧠 دلایل:\n" +
            "\n".join("• " + x for x in a["reasons"]) +
            "\n\n⚠️ تحلیل آماری است و تضمین سود نیست."
        )

        await update.callback_query.edit_message_text(
            text, parse_mode="HTML", reply_markup=home()
        )

    except Exception as e:
        log.warning("analysis unavailable: %s", e)
        await update.callback_query.edit_message_text(
            "⚪ برای این دارایی فعلاً داده تاریخی کافی برای "
            "صدور سیگنال معتبر وجود ندارد.",
            reply_markup=home()
        )


async def signals(update, ctx):
    uid = update.effective_user.id

    if not has_sub(uid):
        await update.message.reply_text(
            "🔒 برای سیگنال اشتراک فعال لازم است.",
            reply_markup=bottom(uid)
        )
        return

    rows = watch(uid)
    if not rows:
        await update.message.reply_text("واچ‌لیست خالی است.")
        return

    await update.message.reply_text("⏳ در حال بررسی واچ‌لیست...")

    out = ["📡 <b>سیگنال‌های واچ‌لیست</b>\n"]

    for r in rows:
        try:
            a = await analyze_asset(r["asset_type"], r["coin_id"])
            icon = {"BUY": "🟢", "SELL": "🔴",
                    "WAIT": "🟡"}[a["signal"]]
            out.append(
                f"{icon} <b>{r['symbol']}</b> — {a['signal']}\n"
                f"   قدرت {a['strength']:.0f}% | "
                f"احتمال مدل {a['probability']:.0f}%"
            )
        except Exception:
            out.append(
                f"⚪ <b>{r['symbol']}</b> — "
                "داده کافی نیست"
            )

        await asyncio.sleep(0.1)

    await update.message.reply_text(
        "\n".join(out),
        parse_mode="HTML",
        reply_markup=bottom(uid)
    )


# ============================================================
# ALERTS
# ============================================================
async def toggle_alerts(update, ctx):
    uid = update.effective_user.id
    c = db()
    r = c.execute(
        "SELECT alerts_enabled FROM settings WHERE user_id=?",
        (uid,)
    ).fetchone()
    new = 0 if r and r["alerts_enabled"] else 1
    c.execute(
        "UPDATE settings SET alerts_enabled=? WHERE user_id=?",
        (new, uid)
    )
    c.commit()
    c.close()

    await update.message.reply_text(
        f"🔔 هشدارها: <b>{'فعال' if new else 'خاموش'}</b>\n\n"
        "هشدار فقط هنگام تغییر سیگنال ارسال می‌شود.",
        parse_mode="HTML",
        reply_markup=bottom(uid)
    )


async def alert_loop(app):
    while True:
        try:
            c = db()
            users = c.execute("""
                SELECT user_id FROM settings
                WHERE alerts_enabled=1
            """).fetchall()
            c.close()

            for u in users:
                uid = u["user_id"]
                if not has_sub(uid) or blocked(uid):
                    continue

                for r in watch(uid):
                    try:
                        a = await analyze_asset(
                            r["asset_type"], r["coin_id"]
                        )
                    except Exception:
                        continue

                    c = db()
                    old = c.execute("""
                        SELECT last_signal FROM alert_state
                        WHERE user_id=? AND coin_id=?
                    """, (uid, r["coin_id"])).fetchone()

                    old_signal = old["last_signal"] if old else None

                    c.execute("""
                        INSERT INTO alert_state(
                            user_id,coin_id,last_signal,updated_at
                        ) VALUES(?,?,?,?)
                        ON CONFLICT(user_id,coin_id) DO UPDATE SET
                            last_signal=excluded.last_signal,
                            updated_at=excluded.updated_at
                    """, (
                        uid, r["coin_id"], a["signal"], iso(now())
                    ))
                    c.commit()
                    c.close()

                    # Do not spam on first scan.
                    if old_signal and old_signal != a["signal"]:
                        icon = {
                            "BUY": "🟢",
                            "SELL": "🔴",
                            "WAIT": "🟡"
                        }[a["signal"]]

                        await app.bot.send_message(
                            chat_id=uid,
                            text=(
                                f"🔔 <b>تغییر سیگنال</b>\n\n"
                                f"{r['name']} ({r['symbol']})\n"
                                f"{icon} <b>{a['signal']}</b>\n"
                                f"💪 قدرت: {a['strength']:.0f}%\n"
                                f"🎯 احتمال مدل: "
                                f"{a['probability']:.0f}%"
                            ),
                            parse_mode="HTML"
                        )

        except asyncio.CancelledError:
            return
        except Exception:
            log.exception("alert loop error")

        await asyncio.sleep(max(120, ALERT_SECONDS))


# ============================================================
# PAYMENTS
# ============================================================
async def buy(update, ctx):
    buttons = [[InlineKeyboardButton(
        f"💳 {p['title']} — {p['price']:,} تومان",
        callback_data=f"plan:{code}"
    )] for code, p in PLANS.items()]
    buttons.append([InlineKeyboardButton(
        "🏠 منو", callback_data="home"
    )])

    await update.message.reply_text(
        "💳 <b>خرید اشتراک</b>\n\nپلن را انتخاب کن:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def plan_selected(update, ctx, code):
    if code not in PLANS:
        return

    p = PLANS[code]
    ctx.user_data["payment_plan"] = code
    ctx.user_data["mode"] = "payment_receipt"

    await update.callback_query.edit_message_text(
        f"💳 <b>{p['title']}</b>\n"
        f"💰 مبلغ: <b>{p['price']:,} تومان</b>\n\n"
        f"🏦 شماره کارت:\n<code>{PAYMENT_CARD or 'تنظیم نشده'}</code>\n\n"
        "بعد از پرداخت، عکس رسید را همین‌جا ارسال کن.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ لغو", callback_data="home")]
        ])
    )


async def receipt(update, ctx):
    uid = update.effective_user.id

    if ctx.user_data.get("mode") != "payment_receipt":
        await update.message.reply_text(
            "ابتدا یک پلن را انتخاب کن.",
            reply_markup=bottom(uid)
        )
        return

    code = ctx.user_data.get("payment_plan")
    if code not in PLANS:
        ctx.user_data.clear()
        await update.message.reply_text("درخواست منقضی شد.")
        return

    p = PLANS[code]
    photo = update.message.photo[-1]

    c = db()
    cur = c.execute("""
        INSERT INTO payment_requests(
            user_id,plan_code,amount,receipt_file_id,receipt_type,
            status,created_at
        ) VALUES(?,?,?,?,?,?,?)
    """, (
        uid, code, p["price"], photo.file_id, "photo",
        "pending", iso(now())
    ))
    rid = cur.lastrowid
    c.commit()
    c.close()

    ctx.user_data.clear()

    await update.message.reply_text(
        f"✅ رسید دریافت شد.\nشماره درخواست: #{rid}\n"
        "پس از بررسی مدیریت، نتیجه اعلام می‌شود.",
        reply_markup=bottom(uid)
    )

    for admin in ADMIN_IDS:
        try:
            await ctx.bot.send_photo(
                chat_id=admin,
                photo=photo.file_id,
                caption=(
                    f"💳 <b>درخواست #{rid}</b>\n"
                    f"👤 User ID: <code>{uid}</code>\n"
                    f"👤 {update.effective_user.first_name or '-'}\n"
                    f"📦 {p['title']}\n"
                    f"💰 {p['price']:,} تومان"
                ),
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "✅ تأیید", callback_data=f"payok:{rid}"
                    ),
                    InlineKeyboardButton(
                        "❌ رد", callback_data=f"payno:{rid}"
                    )
                ]])
            )
        except Exception:
            log.exception("receipt notify failed")


# ============================================================
# ADMIN
# ============================================================
async def admin_panel(update, ctx):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی ندارید.")
        return
    await update.message.reply_text(
        "🛠 <b>پنل مدیریت</b>",
        parse_mode="HTML",
        reply_markup=admin_menu()
    )


async def admin_stats(update, ctx):
    if not is_admin(update.effective_user.id):
        return
    c = db()
    users = c.execute(
        "SELECT COUNT(*) n FROM users"
    ).fetchone()["n"]
    active = c.execute("""
        SELECT COUNT(DISTINCT user_id) n
        FROM subscriptions
        WHERE status='active' AND ends_at>?
    """, (iso(now()),)).fetchone()["n"]
    pending = c.execute("""
        SELECT COUNT(*) n FROM payment_requests
        WHERE status='pending'
    """).fetchone()["n"]
    c.close()

    await update.callback_query.edit_message_text(
        "📊 <b>آمار</b>\n\n"
        f"👥 کاربران: {users}\n"
        f"✅ اشتراک فعال: {active}\n"
        f"⏳ پرداخت در انتظار: {pending}",
        parse_mode="HTML",
        reply_markup=admin_menu()
    )


async def admin_payments(update, ctx):
    if not is_admin(update.effective_user.id):
        return

    c = db()
    rows = c.execute("""
        SELECT p.*,u.first_name
        FROM payment_requests p
        LEFT JOIN users u ON u.user_id=p.user_id
        WHERE p.status='pending'
        ORDER BY p.created_at DESC
        LIMIT 30
    """).fetchall()
    c.close()

    if not rows:
        await update.callback_query.edit_message_text(
            "✅ پرداخت در انتظاری نیست.",
            reply_markup=admin_menu()
        )
        return

    buttons = [[InlineKeyboardButton(
        f"#{r['id']} | {r['amount']:,} | "
        f"{r['first_name'] or r['user_id']}",
        callback_data=f"payview:{r['id']}"
    )] for r in rows]

    buttons.append([InlineKeyboardButton(
        "🔙 مدیریت", callback_data="admin"
    )])

    await update.callback_query.edit_message_text(
        "💳 <b>پرداخت‌های در انتظار</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def payment_view(update, ctx, rid):
    if not is_admin(update.effective_user.id):
        return

    c = db()
    r = c.execute(
        "SELECT * FROM payment_requests WHERE id=?", (rid,)
    ).fetchone()
    c.close()

    if not r:
        await update.callback_query.answer(
            "درخواست پیدا نشد.", show_alert=True
        )
        return

    p = PLANS.get(r["plan_code"], {})
    text = (
        f"💳 <b>درخواست #{rid}</b>\n\n"
        f"👤 User ID: <code>{r['user_id']}</code>\n"
        f"📦 {p.get('title', r['plan_code'])}\n"
        f"💰 {r['amount']:,} تومان\n"
        f"📅 {r['status']}"
    )

    if r["receipt_file_id"]:
        try:
            await ctx.bot.send_photo(
                chat_id=update.effective_user.id,
                photo=r["receipt_file_id"],
                caption=text,
                parse_mode="HTML"
            )
        except Exception:
            pass

    await update.callback_query.edit_message_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "✅ تأیید", callback_data=f"payok:{rid}"
            ),
            InlineKeyboardButton(
                "❌ رد", callback_data=f"payno:{rid}"
            )
        ], [
            InlineKeyboardButton(
                "🔙 پرداخت‌ها", callback_data="adm:payments"
            )
        ]])
    )


async def approve_payment(update, ctx, rid):
    admin = update.effective_user.id
    if not is_admin(admin):
        return

    c = db()
    r = c.execute(
        "SELECT * FROM payment_requests WHERE id=?", (rid,)
    ).fetchone()
    c.close()

    if not r:
        await update.callback_query.answer(
            "درخواست پیدا نشد.", show_alert=True
        )
        return

    if r["status"] != "pending":
        await update.callback_query.answer(
            "این درخواست قبلاً بررسی شده.", show_alert=True
        )
        return

    end = extend_sub(
        r["user_id"], r["plan_code"],
        amount=r["amount"],
        source="manual_payment",
        payment_request_id=rid,
        admin_id=admin
    )

    await update.callback_query.answer(
        "اشتراک فعال شد.", show_alert=True
    )
    await update.callback_query.edit_message_text(
        f"✅ درخواست #{rid} تأیید شد.\n"
        f"📅 اعتبار تا: {end.date()}",
        reply_markup=admin_menu()
    )

    try:
        await ctx.bot.send_message(
            chat_id=r["user_id"],
            text=(
                "🎉 <b>اشتراک شما فعال شد.</b>\n\n"
                f"📦 {PLANS[r['plan_code']]['title']}\n"
                f"📅 اعتبار تا: {end.date()}"
            ),
            parse_mode="HTML",
            reply_markup=bottom(r["user_id"])
        )
    except Exception:
        log.exception("approval notify failed")


async def reject_payment(update, ctx, rid):
    admin = update.effective_user.id
    if not is_admin(admin):
        return

    c = db()
    r = c.execute(
        "SELECT * FROM payment_requests WHERE id=?", (rid,)
    ).fetchone()

    if not r:
        c.close()
        await update.callback_query.answer(
            "درخواست پیدا نشد.", show_alert=True
        )
        return

    c.execute("""
        UPDATE payment_requests
        SET status='rejected',reviewed_at=?
        WHERE id=? AND status='pending'
    """, (iso(now()), rid))
    c.commit()
    c.close()

    admin_log(admin, "reject_payment", r["user_id"], f"request={rid}")

    await update.callback_query.answer(
        "درخواست رد شد.", show_alert=True
    )
    await update.callback_query.edit_message_text(
        f"❌ درخواست #{rid} رد شد.",
        reply_markup=admin_menu()
    )

    try:
        await ctx.bot.send_message(
            chat_id=r["user_id"],
            text=(
                f"❌ درخواست پرداخت #{rid} تأیید نشد.\n"
                "در صورت نیاز رسید معتبر ارسال کن."
            ),
            reply_markup=bottom(r["user_id"])
        )
    except Exception:
        pass


async def admin_extend_start(update, ctx):
    if not is_admin(update.effective_user.id):
        return
    ctx.user_data["mode"] = "admin_extend_user"
    await update.callback_query.edit_message_text(
        "User ID کاربر را بفرست:"
    )


async def admin_block_start(update, ctx):
    if not is_admin(update.effective_user.id):
        return
    ctx.user_data["mode"] = "admin_block_user"
    await update.callback_query.edit_message_text(
        "User ID کاربر را بفرست:"
    )


async def admin_broadcast_start(update, ctx):
    if not is_admin(update.effective_user.id):
        return
    ctx.user_data["mode"] = "admin_broadcast"
    await update.callback_query.edit_message_text(
        "متن پیام همگانی را بفرست:"
    )


async def admin_text(update, ctx):
    admin = update.effective_user.id
    if not is_admin(admin):
        return

    mode = ctx.user_data.get("mode")
    value = update.message.text.strip()

    if mode == "admin_broadcast":
        ctx.user_data.clear()
        c = db()
        users = c.execute(
            "SELECT user_id FROM users WHERE is_blocked=0"
        ).fetchall()
        c.close()

        sent = 0
        for u in users:
            try:
                await ctx.bot.send_message(
                    chat_id=u["user_id"],
                    text=value,
                    reply_markup=bottom(u["user_id"])
                )
                sent += 1
                await asyncio.sleep(.04)
            except Exception:
                pass

        admin_log(admin, "broadcast", details=f"sent={sent}")
        await update.message.reply_text(
            f"📣 ارسال شد: {sent}",
            reply_markup=bottom(admin)
        )
        return

    if mode == "admin_extend_user":
        if not value.isdigit():
            await update.message.reply_text("User ID عددی بفرست.")
            return

        target = int(value)
        c = db()
        r = c.execute(
            "SELECT user_id FROM users WHERE user_id=?", (target,)
        ).fetchone()
        c.close()

        if not r:
            await update.message.reply_text("کاربر پیدا نشد.")
            return

        ctx.user_data["admin_target"] = target
        ctx.user_data["mode"] = "admin_extend_plan"

        buttons = [[InlineKeyboardButton(
            f"{p['title']} — {p['price']:,}",
            callback_data=f"admextend:{code}"
        )] for code, p in PLANS.items()]

        await update.message.reply_text(
            f"کاربر {target} انتخاب شد:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return

    if mode == "admin_block_user":
        if not value.isdigit():
            await update.message.reply_text("User ID عددی بفرست.")
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

        admin_log(admin, "block_toggle", target, f"is_blocked={new}")
        ctx.user_data.clear()

        await update.message.reply_text(
            f"کاربر {target}: "
            f"{'مسدود شد' if new else 'رفع مسدودی شد'}",
            reply_markup=bottom(admin)
        )


async def admin_extend_plan(update, ctx, code):
    admin = update.effective_user.id
    if not is_admin(admin) or code not in PLANS:
        return

    target = ctx.user_data.get("admin_target")
    if not target:
        await update.callback_query.edit_message_text(
            "کاربر مشخص نیست.", reply_markup=admin_menu()
        )
        return

    end = extend_sub(
        target, code, source="admin_manual", admin_id=admin
    )
    ctx.user_data.clear()

    await update.callback_query.edit_message_text(
        f"✅ اشتراک کاربر {target} تمدید شد.\n"
        f"📦 {PLANS[code]['title']}\n"
        f"📅 تا {end.date()}",
        reply_markup=admin_menu()
    )

    try:
        await ctx.bot.send_message(
            chat_id=target,
            text=(
                "🎉 اشتراک شما توسط مدیریت فعال/تمدید شد.\n"
                f"📦 {PLANS[code]['title']}\n"
                f"📅 تا {end.date()}"
            ),
            reply_markup=bottom(target)
        )
    except Exception:
        pass


# ============================================================
# CALLBACK ROUTER
# ============================================================
async def callback(update, ctx):
    q = update.callback_query
    await q.answer()
    ensure_user(q.from_user)

    if blocked(q.from_user.id) and not is_admin(q.from_user.id):
        await q.edit_message_text("⛔ دسترسی شما مسدود است.")
        return

    d = q.data

    if d == "home":
        ctx.user_data.clear()
        await q.edit_message_text(
            "🏠 منوی اصلی", reply_markup=home()
        )

    elif d == "add":
        ctx.user_data["mode"] = "add"
        await q.edit_message_text(
            "➕ نام یا نماد را بفرست.\n"
            "BTC / ZEC / XAU / طلای ۱۸ عیار"
        )

    elif d == "watch":
        rows = watch(q.from_user.id)
        if not rows:
            text = "📋 واچ‌لیست خالی است."
        else:
            text = "📋 <b>واچ‌لیست</b>\n\n" + "\n".join(
                f"• {r['name']} ({r['symbol']})" for r in rows
            )
        await q.edit_message_text(
            text, parse_mode="HTML", reply_markup=home()
        )

    elif d == "analyze":
        if not has_sub(q.from_user.id):
            await q.edit_message_text(
                "🔒 اشتراک فعال لازم است.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "💳 خرید اشتراک", callback_data="buy"
                    )
                ], [
                    InlineKeyboardButton(
                        "🏠 منو", callback_data="home"
                    )
                ]])
            )
            return

        rows = watch(q.from_user.id)
        if not rows:
            await q.edit_message_text(
                "ابتدا دارایی اضافه کن.", reply_markup=home()
            )
            return

        buttons = [[InlineKeyboardButton(
            f"{r['name']} ({r['symbol']})"[:55],
            callback_data=f"an:{r['coin_id']}"
        )] for r in rows]

        await q.edit_message_text(
            "📊 دارایی را انتخاب کن:",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif d == "signals":
        await q.delete_message()
        fake = type("Obj", (), {})()
        # Easier: use the bot directly without constructing Update.
        uid = q.from_user.id
        if not has_sub(uid):
            await ctx.bot.send_message(
                chat_id=uid, text="🔒 اشتراک فعال لازم است.",
                reply_markup=bottom(uid)
            )
            return

        rows = watch(uid)
        if not rows:
            await ctx.bot.send_message(
                chat_id=uid, text="واچ‌لیست خالی است."
            )
            return

        out = ["📡 <b>سیگنال‌های واچ‌لیست</b>\n"]
        for r in rows:
            try:
                a = await analyze_asset(r["asset_type"], r["coin_id"])
                icon = {"BUY": "🟢", "SELL": "🔴",
                        "WAIT": "🟡"}[a["signal"]]
                out.append(
                    f"{icon} <b>{r['symbol']}</b> — {a['signal']} | "
                    f"قدرت {a['strength']:.0f}% | "
                    f"احتمال {a['probability']:.0f}%"
                )
            except Exception:
                out.append(
                    f"⚪ <b>{r['symbol']}</b> — داده کافی نیست"
                )

        await ctx.bot.send_message(
            chat_id=uid,
            text="\n".join(out),
            parse_mode="HTML",
            reply_markup=bottom(uid)
        )

    elif d == "alerts":
        uid = q.from_user.id
        c = db()
        r = c.execute(
            "SELECT alerts_enabled FROM settings WHERE user_id=?",
            (uid,)
        ).fetchone()
        new = 0 if r["alerts_enabled"] else 1
        c.execute(
            "UPDATE settings SET alerts_enabled=? WHERE user_id=?",
            (new, uid)
        )
        c.commit()
        c.close()

        await q.edit_message_text(
            f"🔔 هشدارها: <b>{'فعال' if new else 'خاموش'}</b>",
            parse_mode="HTML", reply_markup=home()
        )

    elif d == "help":
        support = (
            f"\n📞 پشتیبانی: @{SUPPORT_USERNAME}"
            if SUPPORT_USERNAME else ""
        )
        await q.edit_message_text(
            "ℹ️ <b>راهنما</b>\n\n"
            "🪙 هر ارز را به واچ‌لیست اضافه کن.\n"
            "🥇 XAU = طلای جهانی.\n"
            "🟡 18K = طلای ۱۸ عیار ایران.\n"
            "📊 تحلیل و سیگنال نیاز به اشتراک دارند.\n"
            "🔔 هشدار با تغییر سیگنال ارسال می‌شود."
            + support,
            parse_mode="HTML", reply_markup=home()
        )

    elif d == "buy":
        buttons = [[InlineKeyboardButton(
            f"💳 {p['title']} — {p['price']:,} تومان",
            callback_data=f"plan:{code}"
        )] for code, p in PLANS.items()]
        await q.edit_message_text(
            "💳 <b>خرید اشتراک</b>\n\nپلن را انتخاب کن:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(buttons)
        )

    elif d == "substatus":
        await q.edit_message_text(
            sub_text(q.from_user.id),
            parse_mode="HTML", reply_markup=home()
        )

    elif d.startswith("plan:"):
        await plan_selected(
            update, ctx, d.split(":", 1)[1]
        )

    elif d.startswith("coin:"):
        await select_coin(
            update, ctx, d.split(":", 1)[1]
        )

    elif d.startswith("an:"):
        await do_analysis(
            update, ctx, d.split(":", 1)[1]
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

    elif d == "adm:payments":
        await admin_payments(update, ctx)

    elif d == "adm:extend":
        await admin_extend_start(update, ctx)

    elif d == "adm:block":
        await admin_block_start(update, ctx)

    elif d == "adm:broadcast":
        await admin_broadcast_start(update, ctx)

    elif d.startswith("payview:"):
        await payment_view(
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
# TEXT / PHOTO ROUTERS
# ============================================================
async def text_router(update, ctx):
    ensure_user(update.effective_user)

    if blocked(update.effective_user.id) and not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی شما مسدود است.")
        return

    t = (update.message.text or "").strip()

    if t == "📋 واچ‌لیست":
        await show_watch(update, ctx)
    elif t == "➕ افزودن ارز":
        await add_prompt(update, ctx)
    elif t == "📊 تحلیل":
        await analysis_menu(update, ctx)
    elif t == "📡 سیگنال‌ها":
        await signals(update, ctx)
    elif t == "💳 خرید اشتراک":
        await buy(update, ctx)
    elif t == "📅 وضعیت اشتراک":
        await update.message.reply_text(
            sub_text(update.effective_user.id),
            parse_mode="HTML",
            reply_markup=bottom(update.effective_user.id)
        )
    elif t == "🔔 هشدار":
        await toggle_alerts(update, ctx)
    elif t == "ℹ️ راهنما":
        await update.message.reply_text(
            "ℹ️ BTC/ZEC/SOL یا هر ارز قابل جستجو را اضافه کن.\n"
            "XAU برای طلای جهانی است.\n"
            "طلای ۱۸ عیار را با «طلای ۱۸ عیار» جستجو کن.\n"
            "تحلیل و سیگنال نیاز به اشتراک دارند.",
            reply_markup=bottom(update.effective_user.id)
        )
    elif t == "🛠 پنل مدیریت":
        await admin_panel(update, ctx)
    else:
        await search_text(update, ctx)


async def photo_router(update, ctx):
    ensure_user(update.effective_user)

    if blocked(update.effective_user.id) and not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی شما مسدود است.")
        return

    await receipt(update, ctx)


# ============================================================
# APP LIFECYCLE
# ============================================================
async def post_init(app):
    app.bot_data["alert_task"] = asyncio.create_task(
        alert_loop(app)
    )
    log.info("Alert worker started")


async def post_shutdown(app):
    task = app.bot_data.get("alert_task")
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def main():
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    init_db()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(callback))
    app.add_handler(MessageHandler(
        filters.PHOTO, photo_router
    ))
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, text_router
    ))

    log.info(
        "Crypto Analyzer V4 started | DB=%s | admins=%s",
        DB_PATH, sorted(ADMIN_IDS)
    )
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
