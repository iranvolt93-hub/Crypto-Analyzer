
import os
import time
import math
import sqlite3
import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional

import aiohttp
import pandas as pd
import numpy as np
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    ContextTypes, filters
)

# ============================================================
# LIGHT CRYPTO WATCHLIST BOT
# Telegram + SQLite + Binance public market data
# Analysis only — no automatic trading
# ============================================================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "crypto_bot.db")
SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "300"))
ALERT_SCAN_SECONDS = int(os.getenv("ALERT_SCAN_SECONDS", "300"))
DEFAULT_INTERVAL = os.getenv("DEFAULT_INTERVAL", "1h")
BINANCE_BASE = os.getenv("BINANCE_BASE", "https://api.binance.com")
MAX_WATCHLIST = int(os.getenv("MAX_WATCHLIST", "100"))
MIN_CANDLES = 120
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "15"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("crypto-bot")

# ---------------- DB ----------------

def db():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA foreign_keys=ON")
    return con

def init_db():
    con = db()
    con.executescript("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        created_at TEXT NOT NULL,
        is_active INTEGER DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS watchlist(
        user_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        added_at TEXT NOT NULL,
        PRIMARY KEY(user_id, symbol),
        FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS settings(
        user_id INTEGER PRIMARY KEY,
        interval TEXT DEFAULT '1h',
        alerts_enabled INTEGER DEFAULT 0,
        min_score INTEGER DEFAULT 65,
        FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS alert_state(
        user_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        last_signal TEXT,
        last_price REAL,
        updated_at TEXT,
        PRIMARY KEY(user_id, symbol)
    );

    CREATE TABLE IF NOT EXISTS analysis_log(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        symbol TEXT,
        interval TEXT,
        signal TEXT,
        strength REAL,
        probability REAL,
        price REAL,
        created_at TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_watch_user ON watchlist(user_id);
    CREATE INDEX IF NOT EXISTS idx_log_symbol ON analysis_log(symbol, created_at);
    """)
    con.commit()
    con.close()

def ensure_user(tg_user):
    con = db()
    now = datetime.now(timezone.utc).isoformat()
    con.execute("""
      INSERT INTO users(user_id,username,first_name,created_at)
      VALUES(?,?,?,?)
      ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,
      first_name=excluded.first_name,is_active=1
    """, (tg_user.id, tg_user.username or "", tg_user.first_name or "", now))
    con.execute("""
      INSERT INTO settings(user_id) VALUES(?)
      ON CONFLICT(user_id) DO NOTHING
    """, (tg_user.id,))
    con.commit()
    con.close()

def get_settings(user_id):
    con = db()
    row = con.execute("SELECT * FROM settings WHERE user_id=?", (user_id,)).fetchone()
    con.close()
    return row

def get_watchlist(user_id):
    con = db()
    rows = con.execute(
        "SELECT symbol FROM watchlist WHERE user_id=? ORDER BY added_at",
        (user_id,)
    ).fetchall()
    con.close()
    return [r["symbol"] for r in rows]

def add_watch(user_id, symbol):
    con = db()
    count = con.execute("SELECT COUNT(*) c FROM watchlist WHERE user_id=?", (user_id,)).fetchone()["c"]
    if count >= MAX_WATCHLIST:
        con.close()
        return False, f"حداکثر {MAX_WATCHLIST} ارز در واچ‌لیست مجاز است."
    try:
        con.execute(
            "INSERT INTO watchlist(user_id,symbol,added_at) VALUES(?,?,?)",
            (user_id, symbol, datetime.now(timezone.utc).isoformat())
        )
        con.commit()
        ok = True
    except sqlite3.IntegrityError:
        ok = False
    con.close()
    return ok, ("اضافه شد." if ok else "این ارز قبلاً در واچ‌لیست شماست.")

def remove_watch(user_id, symbol):
    con = db()
    cur = con.execute("DELETE FROM watchlist WHERE user_id=? AND symbol=?", (user_id, symbol))
    con.commit()
    con.close()
    return cur.rowcount > 0

# ---------------- Market API ----------------

async def api_get(session, path, params=None):
    url = BINANCE_BASE + path
    async with session.get(url, params=params, timeout=HTTP_TIMEOUT) as r:
        if r.status != 200:
            body = await r.text()
            raise RuntimeError(f"API {r.status}: {body[:200]}")
        return await r.json()

async def exchange_info():
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        return await api_get(s, "/api/v3/exchangeInfo")

async def get_symbols():
    data = await exchange_info()
    out = []
    for x in data.get("symbols", []):
        if x.get("status") == "TRADING" and x.get("quoteAsset") == "USDT":
            out.append(x["symbol"])
    return sorted(out)

async def get_klines(symbol, interval="1h", limit=250):
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        data = await api_get(
            s, "/api/v3/klines",
            {"symbol": symbol, "interval": interval, "limit": min(limit, 1000)}
        )
    if not data or len(data) < MIN_CANDLES:
        raise RuntimeError("داده کافی برای تحلیل موجود نیست.")
    cols = ["open_time","open","high","low","close","volume",
            "close_time","qav","trades","tbav","tqav","ignore"]
    df = pd.DataFrame(data, columns=cols)
    for c in ["open","high","low","close","volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

async def get_ticker(symbol):
    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as s:
        x = await api_get(s, "/api/v3/ticker/24hr", {"symbol": symbol})
    return {
        "price": float(x["lastPrice"]),
        "change": float(x["priceChangePercent"]),
        "high": float(x["highPrice"]),
        "low": float(x["lowPrice"]),
        "volume": float(x["quoteVolume"]),
    }

# ---------------- Indicators ----------------

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

def atr(df, n=14):
    prev = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev).abs(),
        (df["low"] - prev).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1/n, adjust=False).mean()

def macd(s):
    m12 = ema(s, 12)
    m26 = ema(s, 26)
    line = m12 - m26
    sig = ema(line, 9)
    return line, sig, line - sig

def adx(df, n=14):
    high, low, close = df["high"], df["low"], df["close"]
    up = high.diff()
    down = -low.diff()
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    tr = pd.concat([
        high-low,
        (high-close.shift()).abs(),
        (low-close.shift()).abs()
    ], axis=1).max(axis=1)
    atrv = tr.ewm(alpha=1/n, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1/n, adjust=False).mean() / atrv
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1/n, adjust=False).mean() / atrv
    dx = 100 * (plus_di-minus_di).abs() / (plus_di+minus_di).replace(0, np.nan)
    return dx.ewm(alpha=1/n, adjust=False).mean()

def fmt_price(x):
    if x >= 1000: return f"{x:,.2f}"
    if x >= 1: return f"{x:,.4f}"
    if x >= 0.01: return f"{x:,.6f}"
    return f"{x:.8f}"

def clamp(x, a=0, b=100):
    return max(a, min(b, x))

def analyze(df):
    c = df["close"]
    e20, e50, e200 = ema(c,20), ema(c,50), ema(c,200)
    rr = rsi(c,14)
    ml, ms, mh = macd(c)
    aa = adx(df,14)
    av = atr(df,14)

    price = float(c.iloc[-1])
    vals = {
        "price": price,
        "ema20": float(e20.iloc[-1]),
        "ema50": float(e50.iloc[-1]),
        "ema200": float(e200.iloc[-1]),
        "rsi": float(rr.iloc[-1]),
        "macd": float(ml.iloc[-1]),
        "macd_signal": float(ms.iloc[-1]),
        "macd_hist": float(mh.iloc[-1]),
        "adx": float(aa.iloc[-1]),
        "atr": float(av.iloc[-1]),
    }

    score = 50.0
    reasons = []

    # Trend
    if price > vals["ema20"]:
        score += 7; reasons.append("قیمت بالای EMA20")
    else:
        score -= 7; reasons.append("قیمت زیر EMA20")
    if vals["ema20"] > vals["ema50"]:
        score += 9; reasons.append("EMA20 بالای EMA50")
    else:
        score -= 9; reasons.append("EMA20 زیر EMA50")
    if price > vals["ema200"]:
        score += 8; reasons.append("قیمت بالای EMA200")
    else:
        score -= 8; reasons.append("قیمت زیر EMA200")

    # RSI
    if 52 <= vals["rsi"] <= 68:
        score += 8; reasons.append("RSI در ناحیه مثبت")
    elif vals["rsi"] > 72:
        score -= 5; reasons.append("RSI داغ/اشباع خرید")
    elif vals["rsi"] < 30:
        score += 3; reasons.append("RSI اشباع فروش")
    elif vals["rsi"] < 45:
        score -= 5; reasons.append("RSI ضعیف")

    # MACD
    if vals["macd_hist"] > 0:
        score += 10; reasons.append("MACD مثبت")
    else:
        score -= 10; reasons.append("MACD منفی")

    # ADX confirms trend strength, but not direction
    if vals["adx"] >= 25:
        score += 6 if score >= 50 else -6
        reasons.append(f"ADX={vals['adx']:.1f}؛ روند فعال")
    else:
        reasons.append(f"ADX={vals['adx']:.1f}؛ روند ضعیف")

    # Momentum
    ret5 = (c.iloc[-1] / c.iloc[-6] - 1) * 100
    ret20 = (c.iloc[-1] / c.iloc[-21] - 1) * 100
    vals["ret5"] = float(ret5)
    vals["ret20"] = float(ret20)
    if ret5 > 1: score += 4
    elif ret5 < -1: score -= 4
    if ret20 > 3: score += 4
    elif ret20 < -3: score -= 4

    strength = clamp(abs(score - 50) * 2 + 45)
    if score >= 62:
        signal = "BUY"
    elif score <= 38:
        signal = "SELL"
    else:
        signal = "WAIT"

    # Probability is deliberately separate from strength.
    # It is a model confidence estimate, not a guaranteed win probability.
    agreement = (
        (price > vals["ema20"]) +
        (vals["ema20"] > vals["ema50"]) +
        (vals["macd_hist"] > 0) +
        (vals["rsi"] >= 50) +
        (vals["adx"] >= 20)
    )
    direction_agreement = agreement if signal == "BUY" else (5-agreement if signal=="SELL" else 2.5)
    probability = clamp(52 + direction_agreement * 7 + min(vals["adx"], 40) * 0.15 - (8 if signal=="WAIT" else 0))

    # Levels based on ATR; illustrative analysis levels, not orders.
    atrv = max(vals["atr"], price * 0.002)
    if signal == "BUY":
        entry_low, entry_high = price - 0.35*atrv, price + 0.10*atrv
        stop = price - 1.5*atrv
        targets = [price + 1.0*atrv, price + 2.0*atrv, price + 3.0*atrv]
    elif signal == "SELL":
        entry_low, entry_high = price - 0.10*atrv, price + 0.35*atrv
        stop = price + 1.5*atrv
        targets = [price - 1.0*atrv, price - 2.0*atrv, price - 3.0*atrv]
    else:
        entry_low, entry_high, stop = price-0.5*atrv, price+0.5*atrv, price
        targets = [price+atrv, price+2*atrv, price-atrv]

    return {
        **vals,
        "score": score,
        "strength": strength,
        "probability": probability,
        "signal": signal,
        "reasons": reasons[-6:],
        "entry_low": entry_low,
        "entry_high": entry_high,
        "stop": stop,
        "targets": targets,
    }

# ---------------- Telegram UI ----------------

def menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 واچ‌لیست من", callback_data="watch"),
         InlineKeyboardButton("➕ افزودن ارز", callback_data="add")],
        [InlineKeyboardButton("📊 تحلیل ارز", callback_data="analyze"),
         InlineKeyboardButton("📡 سیگنال‌ها", callback_data="signals")],
        [InlineKeyboardButton("➖ حذف ارز", callback_data="remove"),
         InlineKeyboardButton("🔔 هشدارها", callback_data="alerts")],
        [InlineKeyboardButton("⚙️ تنظیمات", callback_data="settings"),
         InlineKeyboardButton("ℹ️ راهنما", callback_data="help")]
    ])

def symbol_buttons(symbols, prefix, per=2):
    rows = []
    for i in range(0, len(symbols), per):
        rows.append([InlineKeyboardButton(s, callback_data=f"{prefix}:{s}") for s in symbols[i:i+per]])
    return rows

async def start(update, context):
    ensure_user(update.effective_user)
    text = (
        "🚀 *ربات تحلیل بازار کریپتو*\n\n"
        "واچ‌لیست شخصی خودت را بساز و برای هر ارز تحلیل و سیگنال جداگانه بگیر.\n\n"
        "📌 داده بازار: Binance Spot / USDT\n"
        "📌 تحلیل: EMA + RSI + MACD + ADX + ATR + مومنتوم\n"
        "⚠️ تحلیل بازار است و تضمین سود نیست."
    )
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=menu())

async def help_cmd(update, context):
    ensure_user(update.effective_user)
    await update.message.reply_text(
        "ℹ️ *راهنما*\n\n"
        "• افزودن ارز: نمادهایی مثل BTC یا ZEC را وارد کن.\n"
        "• واچ‌لیست: ارزهای ذخیره‌شده را ببین.\n"
        "• تحلیل: برای هر ارز تحلیل کامل بگیر.\n"
        "• سیگنال‌ها: همه ارزهای واچ‌لیست را بررسی می‌کند.\n"
        "• هشدار: در صورت تغییر سیگنال، پیام ارسال می‌شود.\n\n"
        "مثال: `ZEC` یا `ZECUSDT`",
        parse_mode="Markdown",
        reply_markup=menu()
    )

async def show_watch(update, context):
    uid = update.effective_user.id
    ensure_user(update.effective_user)
    syms = get_watchlist(uid)
    if not syms:
        txt = "📋 واچ‌لیست شما خالی است.\nاز «➕ افزودن ارز» استفاده کن."
        kb = menu()
    else:
        txt = "📋 *واچ‌لیست شما:*\n\n" + "\n".join(f"• {s}" for s in syms)
        rows = symbol_buttons(syms, "an")
        rows.append([InlineKeyboardButton("➕ افزودن ارز", callback_data="add")])
        rows.append([InlineKeyboardButton("🏠 منوی اصلی", callback_data="home")])
        kb = InlineKeyboardMarkup(rows)
    if update.callback_query:
        await update.callback_query.edit_message_text(txt, parse_mode="Markdown", reply_markup=kb)
    else:
        await update.message.reply_text(txt, parse_mode="Markdown", reply_markup=kb)

async def prompt_add(update, context):
    context.user_data["mode"] = "add"
    q = update.callback_query
    await q.edit_message_text(
        "➕ نماد ارز را بفرست.\n\nمثال:\n`BTC`\n`ZEC`\n`SOLUSDT`",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("لغو", callback_data="home")]])
    )

async def prompt_remove(update, context):
    syms = get_watchlist(update.effective_user.id)
    if not syms:
        await update.callback_query.edit_message_text("واچ‌لیست خالی است.", reply_markup=menu())
        return
    await update.callback_query.edit_message_text(
        "➖ ارز موردنظر را انتخاب کن:",
        reply_markup=InlineKeyboardMarkup(symbol_buttons(syms, "rm") + [
            [InlineKeyboardButton("🏠 منوی اصلی", callback_data="home")]
        ])
    )

async def prompt_analyze(update, context):
    syms = get_watchlist(update.effective_user.id)
    if not syms:
        await update.callback_query.edit_message_text("ابتدا حداقل یک ارز به واچ‌لیست اضافه کن.", reply_markup=menu())
        return
    await update.callback_query.edit_message_text(
        "📊 ارز موردنظر را انتخاب کن:",
        reply_markup=InlineKeyboardMarkup(symbol_buttons(syms, "an") + [
            [InlineKeyboardButton("🏠 منوی اصلی", callback_data="home")]
        ])
    )

async def do_analysis(update, symbol):
    q = update.callback_query
    await q.edit_message_text(f"⏳ در حال تحلیل {symbol} ...")
    try:
        settings = get_settings(update.effective_user.id)
        interval = settings["interval"] if settings else DEFAULT_INTERVAL
        df = await get_klines(symbol, interval, 250)
        a = analyze(df)
        sig_icon = {"BUY":"🟢 خرید", "SELL":"🔴 فروش", "WAIT":"🟡 انتظار"}[a["signal"]]
        reasons = "\n".join("• "+x for x in a["reasons"])
        targets = "\n".join(f"  {i+1}) {fmt_price(v)}" for i,v in enumerate(a["targets"]))
        text = (
            f"📊 *تحلیل {symbol}*\n"
            f"⏱ تایم‌فریم: `{interval}`\n\n"
            f"💰 قیمت: `{fmt_price(a['price'])}`\n"
            f"📡 سیگنال: *{sig_icon}*\n\n"
            f"💪 قدرت روند: *{a['strength']:.0f}%*\n"
            f"🎯 احتمال موفقیت مدل: *{a['probability']:.0f}%*\n\n"
            f"📈 RSI: `{a['rsi']:.1f}`\n"
            f"📊 MACD Hist: `{a['macd_hist']:.6f}`\n"
            f"📐 ADX: `{a['adx']:.1f}`\n"
            f"📈 EMA20: `{fmt_price(a['ema20'])}`\n"
            f"📈 EMA50: `{fmt_price(a['ema50'])}`\n"
            f"📈 EMA200: `{fmt_price(a['ema200'])}`\n\n"
            f"🎯 محدوده ورود تحلیلی:\n`{fmt_price(a['entry_low'])} - {fmt_price(a['entry_high'])}`\n"
            f"🛑 حد ضرر تحلیلی: `{fmt_price(a['stop'])}`\n"
            f"🎯 اهداف:\n{targets}\n\n"
            f"🧠 *دلایل اصلی:*\n{reasons}\n\n"
            "⚠️ این خروجی ابزار تحلیل است؛ تضمین سود یا توصیه سرمایه‌گذاری نیست."
        )
        con = db()
        con.execute("""
          INSERT INTO analysis_log(user_id,symbol,interval,signal,strength,probability,price,created_at)
          VALUES(?,?,?,?,?,?,?,?)
        """, (update.effective_user.id,symbol,interval,a["signal"],a["strength"],a["probability"],
              a["price"],datetime.now(timezone.utc).isoformat()))
        con.commit(); con.close()
        await q.edit_message_text(text, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 تحلیل مجدد", callback_data=f"an:{symbol}")],
                [InlineKeyboardButton("➖ حذف از واچ‌لیست", callback_data=f"rm:{symbol}")],
                [InlineKeyboardButton("🏠 منوی اصلی", callback_data="home")]
            ]))
    except Exception as e:
        log.exception("analysis error")
        await q.edit_message_text(
            f"❌ تحلیل {symbol} انجام نشد.\n\nممکن است نماد در Binance Spot/USDT وجود نداشته باشد.",
            reply_markup=menu()
        )

async def signals(update, context):
    uid = update.effective_user.id
    syms = get_watchlist(uid)
    if not syms:
        await update.callback_query.edit_message_text("واچ‌لیست خالی است.", reply_markup=menu())
        return
    await update.callback_query.edit_message_text("⏳ در حال بررسی واچ‌لیست...")
    settings = get_settings(uid)
    interval = settings["interval"] if settings else DEFAULT_INTERVAL
    lines = [f"📡 *سیگنال‌های واچ‌لیست — {interval}*\n"]
    for s in syms:
        try:
            df = await get_klines(s, interval, 220)
            a = analyze(df)
            icon = {"BUY":"🟢","SELL":"🔴","WAIT":"🟡"}[a["signal"]]
            lines.append(f"{icon} *{s}* — {a['signal']} | قدرت {a['strength']:.0f}% | احتمال مدل {a['probability']:.0f}%")
        except Exception:
            lines.append(f"⚪ *{s}* — داده در دسترس نیست")
        await asyncio.sleep(0.05)
    lines.append("\n⚠️ سیگنال‌ها قطعی و تضمینی نیستند.")
    await update.callback_query.edit_message_text(
        "\n".join(lines), parse_mode="Markdown", reply_markup=menu()
    )

async def settings_menu(update, context):
    uid = update.effective_user.id
    s = get_settings(uid)
    await update.callback_query.edit_message_text(
        f"⚙️ *تنظیمات*\n\n"
        f"تایم‌فریم فعلی: `{s['interval']}`\n"
        f"هشدار: {'فعال' if s['alerts_enabled'] else 'خاموش'}\n\n"
        "تایم‌فریم را انتخاب کن:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("15m", callback_data="tf:15m"),
             InlineKeyboardButton("1H", callback_data="tf:1h"),
             InlineKeyboardButton("4H", callback_data="tf:4h")],
            [InlineKeyboardButton("1D", callback_data="tf:1d")],
            [InlineKeyboardButton("🔔 روشن/خاموش هشدار", callback_data="toggle_alert")],
            [InlineKeyboardButton("🏠 منوی اصلی", callback_data="home")]
        ])
    )

async def alerts_menu(update, context):
    uid = update.effective_user.id
    s = get_settings(uid)
    await update.callback_query.edit_message_text(
        f"🔔 هشدارها اکنون: *{'فعال' if s['alerts_enabled'] else 'خاموش'}*\n\n"
        "وقتی هشدار فعال باشد، ربات تغییر سیگنال ارزهای واچ‌لیست را بررسی می‌کند.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔔 تغییر وضعیت", callback_data="toggle_alert")],
            [InlineKeyboardButton("⚙️ تنظیمات", callback_data="settings")],
            [InlineKeyboardButton("🏠 منوی اصلی", callback_data="home")]
        ])
    )

async def handle_text(update, context):
    ensure_user(update.effective_user)
    mode = context.user_data.get("mode")
    text = update.message.text.strip().upper().replace("/", "")
    if mode != "add":
        await update.message.reply_text("از منوی اصلی یک گزینه انتخاب کن.", reply_markup=menu())
        return
    if not text.endswith("USDT"):
        text += "USDT"
    context.user_data.pop("mode", None)
    try:
        # Validate symbol via candles; this also avoids storing arbitrary names.
        await get_klines(text, "1h", MIN_CANDLES)
        ok, msg = add_watch(update.effective_user.id, text)
        await update.message.reply_text(
            f"{'✅' if ok else 'ℹ️'} {text}: {msg}",
            reply_markup=menu()
        )
    except Exception:
        await update.message.reply_text(
            f"❌ نماد `{text}` پیدا نشد یا بازار Binance Spot/USDT برای آن فعال نیست.\n"
            "نماد دیگری بفرست.",
            parse_mode="Markdown"
        )
        context.user_data["mode"] = "add"

async def callback(update, context):
    q = update.callback_query
    await q.answer()
    uid = q.from_user.id
    ensure_user(q.from_user)
    data = q.data
    if data == "home":
        await q.edit_message_text("🏠 منوی اصلی", reply_markup=menu())
    elif data == "watch":
        await show_watch(update, context)
    elif data == "add":
        await prompt_add(update, context)
    elif data == "remove":
        await prompt_remove(update, context)
    elif data == "analyze":
        await prompt_analyze(update, context)
    elif data == "signals":
        await signals(update, context)
    elif data == "settings":
        await settings_menu(update, context)
    elif data == "alerts":
        await alerts_menu(update, context)
    elif data == "help":
        await q.edit_message_text(
            "ℹ️ ربات تحلیل سبک کریپتو\n\n"
            "ارزها را به واچ‌لیست اضافه کن و برای هرکدام تحلیل جداگانه بگیر.",
            reply_markup=menu()
        )
    elif data.startswith("an:"):
        await do_analysis(update, data[3:])
    elif data.startswith("rm:"):
        s = data[3:]
        if remove_watch(uid, s):
            await q.edit_message_text(f"✅ {s} از واچ‌لیست حذف شد.", reply_markup=menu())
        else:
            await q.edit_message_text("این ارز در واچ‌لیست نبود.", reply_markup=menu())
    elif data.startswith("tf:"):
        tf = data[3:]
        if tf not in ("15m","1h","4h","1d"):
            return
        con = db()
        con.execute("UPDATE settings SET interval=? WHERE user_id=?", (tf, uid))
        con.commit(); con.close()
        await settings_menu(update, context)
    elif data == "toggle_alert":
        con = db()
        s = con.execute("SELECT alerts_enabled FROM settings WHERE user_id=?", (uid,)).fetchone()
        newv = 0 if s["alerts_enabled"] else 1
        con.execute("UPDATE settings SET alerts_enabled=? WHERE user_id=?", (newv, uid))
        con.commit(); con.close()
        await alerts_menu(update, context)

async def alert_worker(app):
    while True:
        try:
            con = db()
            rows = con.execute("""
                SELECT w.user_id,w.symbol,s.interval,s.min_score
                FROM watchlist w JOIN settings s ON s.user_id=w.user_id
                WHERE s.alerts_enabled=1
            """).fetchall()
            con.close()
            for row in rows:
                try:
                    df = await get_klines(row["symbol"], row["interval"], 220)
                    a = analyze(df)
                    con = db()
                    old = con.execute(
                        "SELECT last_signal FROM alert_state WHERE user_id=? AND symbol=?",
                        (row["user_id"], row["symbol"])
                    ).fetchone()
                    previous = old["last_signal"] if old else None
                    con.execute("""
                      INSERT INTO alert_state(user_id,symbol,last_signal,last_price,updated_at)
                      VALUES(?,?,?,?,?)
                      ON CONFLICT(user_id,symbol) DO UPDATE SET
                      last_signal=excluded.last_signal,last_price=excluded.last_price,
                      updated_at=excluded.updated_at
                    """, (row["user_id"],row["symbol"],a["signal"],a["price"],
                          datetime.now(timezone.utc).isoformat()))
                    con.commit(); con.close()
                    if previous and previous != a["signal"]:
                        icon = {"BUY":"🟢","SELL":"🔴","WAIT":"🟡"}[a["signal"]]
                        msg = (
                            f"🔔 *تغییر سیگنال {row['symbol']}*\n\n"
                            f"{icon} سیگنال جدید: *{a['signal']}*\n"
                            f"💰 قیمت: `{fmt_price(a['price'])}`\n"
                            f"💪 قدرت: `{a['strength']:.0f}%`\n"
                            f"🎯 احتمال مدل: `{a['probability']:.0f}%`\n"
                            f"⏱ {row['interval']}\n\n"
                            "⚠️ تحلیل خودکار؛ تضمین سود نیست."
                        )
                        await app.bot.send_message(row["user_id"], msg, parse_mode="Markdown")
                except Exception:
                    log.exception("alert symbol failed")
                await asyncio.sleep(0.2)
        except Exception:
            log.exception("alert worker failed")
        await asyncio.sleep(ALERT_SCAN_SECONDS)

async def post_init(app):
    init_db()
    asyncio.create_task(alert_worker(app))
    log.info("Bot initialized")

def main():
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CallbackQueryHandler(callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    log.info("Starting bot...")
    application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
