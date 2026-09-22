
import os, sqlite3, logging, asyncio, time
from datetime import datetime, timezone
import aiohttp
import numpy as np
import pandas as pd
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters

BOT_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN","").strip()
DB_PATH=os.getenv("DB_PATH","crypto_bot.db")
CG_BASE="https://api.coingecko.com/api/v3"
HTTP_TIMEOUT=int(os.getenv("HTTP_TIMEOUT","20"))
MAX_WATCHLIST=int(os.getenv("MAX_WATCHLIST","100"))
ALERT_SCAN_SECONDS=int(os.getenv("ALERT_SCAN_SECONDS","600"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log=logging.getLogger("crypto-watchlist")

def db():
    c=sqlite3.connect(DB_PATH,timeout=30)
    c.row_factory=sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    return c

def init_db():
    c=db()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS users(
      user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, created_at TEXT
    );
    CREATE TABLE IF NOT EXISTS watchlist(
      user_id INTEGER NOT NULL, coin_id TEXT NOT NULL, symbol TEXT NOT NULL, name TEXT NOT NULL,
      added_at TEXT NOT NULL, PRIMARY KEY(user_id,coin_id)
    );
    CREATE TABLE IF NOT EXISTS settings(
      user_id INTEGER PRIMARY KEY, interval TEXT DEFAULT '1d', alerts_enabled INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS alert_state(
      user_id INTEGER, coin_id TEXT, last_signal TEXT, updated_at TEXT,
      PRIMARY KEY(user_id,coin_id)
    );
    """)
    c.commit(); c.close()

def ensure_user(u):
    c=db(); now=datetime.now(timezone.utc).isoformat()
    c.execute("""INSERT INTO users(user_id,username,first_name,created_at) VALUES(?,?,?,?)
                 ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,first_name=excluded.first_name""",
              (u.id,u.username or "",u.first_name or "",now))
    c.execute("INSERT INTO settings(user_id) VALUES(?) ON CONFLICT(user_id) DO NOTHING",(u.id,))
    c.commit(); c.close()

def watch(uid):
    c=db(); r=c.execute("SELECT * FROM watchlist WHERE user_id=? ORDER BY added_at",(uid,)).fetchall(); c.close()
    return r

def normalize(s):
    s=s.strip().lower().replace(" ","").replace("/","").replace("-","")
    if s.endswith("usdt"): s=s[:-4]
    return s

async def get_json(path, params=None):
    timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout,headers={"accept":"application/json","user-agent":"CryptoWatchlistBot/2.0"}) as s:
        async with s.get(CG_BASE+path,params=params) as r:
            body=await r.text()
            if r.status!=200:
                raise RuntimeError(f"market api {r.status}: {body[:160]}")
            return await r.json()

async def search_coins(query):
    q=normalize(query)
    data=await get_json("/search",{"query":q})
    coins=data.get("coins",[])
    # Prefer exact symbol/name matches, then market-cap rank.
    coins=sorted(coins,key=lambda x:(0 if normalize(x.get("symbol",""))==q else 1,
                                    x.get("market_cap_rank") or 999999))
    return coins[:8]

async def coin_history(coin_id, days=90):
    data=await get_json(f"/coins/{coin_id}/market_chart",{"vs_currency":"usd","days":str(days),"interval":"daily"})
    prices=data.get("prices",[])
    if len(prices)<40: raise RuntimeError("داده تاریخی کافی نیست")
    df=pd.DataFrame(prices,columns=["ts","close"])
    df["close"]=pd.to_numeric(df["close"],errors="coerce")
    df=df.dropna()
    return df

def ema(s,n): return s.ewm(span=n,adjust=False).mean()

def rsi(s,n=14):
    d=s.diff(); up=d.clip(lower=0); down=-d.clip(upper=0)
    au=up.ewm(alpha=1/n,adjust=False).mean()
    ad=down.ewm(alpha=1/n,adjust=False).mean()
    rs=au/ad.replace(0,np.nan)
    return 100-(100/(1+rs))

def macd(s):
    m=ema(s,12)-ema(s,26); sig=ema(m,9)
    return m,sig,m-sig

def analyze(df):
    c=df.close
    price=float(c.iloc[-1]); e20=float(ema(c,20).iloc[-1]); e50=float(ema(c,50).iloc[-1])
    rr=float(rsi(c).iloc[-1]); ml,ms,mh=macd(c); mh=float(mh.iloc[-1])
    ret7=float((c.iloc[-1]/c.iloc[-8]-1)*100)
    ret30=float((c.iloc[-1]/c.iloc[-31]-1)*100)
    score=50.0; reasons=[]
    if price>e20: score+=10; reasons.append("قیمت بالای EMA20")
    else: score-=10; reasons.append("قیمت زیر EMA20")
    if e20>e50: score+=12; reasons.append("EMA20 بالای EMA50")
    else: score-=12; reasons.append("EMA20 زیر EMA50")
    if mh>0: score+=12; reasons.append("MACD مثبت")
    else: score-=12; reasons.append("MACD منفی")
    if 50<=rr<=68: score+=8; reasons.append("RSI مثبت")
    elif rr>72: score-=5; reasons.append("RSI داغ")
    elif rr<30: score+=3; reasons.append("RSI اشباع فروش")
    elif rr<45: score-=6; reasons.append("RSI ضعیف")
    if ret7>2: score+=4
    elif ret7<-2: score-=4
    if ret30>5: score+=4
    elif ret30<-5: score-=4
    score=max(0,min(100,score))
    signal="BUY" if score>=62 else "SELL" if score<=38 else "WAIT"
    agreement=sum([price>e20,e20>e50,mh>0,rr>=50])
    probability=max(50,min(86,52+agreement*7+(4 if signal!="WAIT" else 0)))
    strength=max(45,min(100,abs(score-50)*2+45))
    # Volatility-based illustrative levels.
    vol=float(c.pct_change().rolling(14).std().iloc[-1] or 0.02)
    move=max(price*vol,price*0.01)
    if signal=="BUY":
        stop=price-1.5*move; targets=[price+move,price+2*move,price+3*move]
    elif signal=="SELL":
        stop=price+1.5*move; targets=[price-move,price-2*move,price-3*move]
    else:
        stop=price; targets=[price+move,price+2*move,price-move]
    return dict(price=price,ema20=e20,ema50=e50,rsi=rr,macd=mh,ret7=ret7,ret30=ret30,
                score=score,signal=signal,strength=strength,probability=probability,
                stop=stop,targets=targets,reasons=reasons)

def fp(x):
    if x>=1000:return f"{x:,.2f}"
    if x>=1:return f"{x:,.4f}"
    if x>=.01:return f"{x:,.6f}"
    return f"{x:.8f}"

def menu():
    return InlineKeyboardMarkup([
      [InlineKeyboardButton("📋 واچ‌لیست من",callback_data="watch"),InlineKeyboardButton("➕ افزودن ارز",callback_data="add")],
      [InlineKeyboardButton("📊 تحلیل",callback_data="analyze"),InlineKeyboardButton("📡 سیگنال‌ها",callback_data="signals")],
      [InlineKeyboardButton("➖ حذف",callback_data="remove"),InlineKeyboardButton("🔔 هشدار",callback_data="alerts")],
      [InlineKeyboardButton("ℹ️ راهنما",callback_data="help")]
    ])

async def start(update,ctx):
    ensure_user(update.effective_user)
    await update.message.reply_text("🚀 ربات تحلیل کریپتو آماده است.\n\nاز جستجوی نام یا نماد برای افزودن ارز استفاده کن.",reply_markup=menu())

async def add_prompt(update,ctx):
    ctx.user_data["mode"]="add"
    await update.callback_query.edit_message_text("➕ نام یا نماد ارز را بفرست.\nمثال: `ZEC`، `Bitcoin`، `Solana`",parse_mode="Markdown",
      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("لغو",callback_data="home")]]))

async def search_text(update,ctx):
    ensure_user(update.effective_user)
    if ctx.user_data.get("mode")!="add":
        await update.message.reply_text("از منوی اصلی یک گزینه انتخاب کن.",reply_markup=menu()); return
    q=update.message.text.strip()
    try:
        coins=await search_coins(q)
        if not coins:
            await update.message.reply_text("❌ ارز پیدا نشد. نام یا نماد دیگری بفرست."); return
        ctx.user_data["mode"]="select"
        ctx.user_data["search_results"]={str(i):x for i,x in enumerate(coins)}
        rows=[]
        for i,x in enumerate(coins):
            rank=x.get("market_cap_rank")
            label=f"{x.get('name','?')} ({x.get('symbol','?').upper()})"
            if rank: label+=f" #{rank}"
            rows.append([InlineKeyboardButton(label[:55],callback_data=f"coin:{i}")])
        rows.append([InlineKeyboardButton("❌ لغو",callback_data="home")])
        await update.message.reply_text("🔎 نتایج جستجو:",reply_markup=InlineKeyboardMarkup(rows))
    except Exception as e:
        log.exception("search failed")
        await update.message.reply_text("⚠️ سرویس جستجوی بازار موقتاً پاسخ نمی‌دهد. چند ثانیه بعد دوباره امتحان کن.")

async def select_coin(update,ctx,idx):
    coins=ctx.user_data.get("search_results",{})
    x=coins.get(str(idx))
    if not x:
        await update.callback_query.edit_message_text("نتیجه جستجو منقضی شده؛ دوباره جستجو کن.",reply_markup=menu()); return
    uid=update.effective_user.id; cid=x["id"]; sym=x.get("symbol","").upper(); name=x.get("name","")
    c=db(); count=c.execute("SELECT COUNT(*) n FROM watchlist WHERE user_id=?",(uid,)).fetchone()["n"]
    if count>=MAX_WATCHLIST:
        c.close(); await update.callback_query.edit_message_text(f"حداکثر {MAX_WATCHLIST} ارز مجاز است.",reply_markup=menu()); return
    try:
        c.execute("INSERT INTO watchlist(user_id,coin_id,symbol,name,added_at) VALUES(?,?,?,?,?)",
                  (uid,cid,sym,name,datetime.now(timezone.utc).isoformat()))
        c.commit(); msg=f"✅ {name} ({sym}) به واچ‌لیست اضافه شد."
    except sqlite3.IntegrityError: msg=f"ℹ️ {name} ({sym}) قبلاً در واچ‌لیست است."
    finally:c.close()
    ctx.user_data.clear()
    await update.callback_query.edit_message_text(msg,reply_markup=menu())

async def show_watch(update,ctx):
    rows=watch(update.effective_user.id)
    if not rows:
        txt="📋 واچ‌لیست شما خالی است."
    else:
        txt="📋 *واچ‌لیست شما:*\n\n"+"\n".join(f"• {r['name']} ({r['symbol']})" for r in rows)
    await update.callback_query.edit_message_text(txt,parse_mode="Markdown",reply_markup=menu())

async def remove_menu(update,ctx):
    rows=watch(update.effective_user.id)
    if not rows:
        await update.callback_query.edit_message_text("واچ‌لیست خالی است.",reply_markup=menu()); return
    kb=[[InlineKeyboardButton(f"{r['name']} ({r['symbol']})"[:50],callback_data=f"del:{r['coin_id']}")] for r in rows]
    kb.append([InlineKeyboardButton("🏠 منو",callback_data="home")])
    await update.callback_query.edit_message_text("➖ ارز موردنظر را انتخاب کن:",reply_markup=InlineKeyboardMarkup(kb))

async def analyze_menu(update,ctx):
    rows=watch(update.effective_user.id)
    if not rows:
        await update.callback_query.edit_message_text("ابتدا یک ارز اضافه کن.",reply_markup=menu()); return
    kb=[[InlineKeyboardButton(f"{r['name']} ({r['symbol']})"[:50],callback_data=f"an:{r['coin_id']}")] for r in rows]
    kb.append([InlineKeyboardButton("🏠 منو",callback_data="home")])
    await update.callback_query.edit_message_text("📊 ارز موردنظر را انتخاب کن:",reply_markup=InlineKeyboardMarkup(kb))

async def do_an(update,ctx,cid):
    await update.callback_query.edit_message_text("⏳ در حال دریافت داده و تحلیل...")
    try:
        rows=watch(update.effective_user.id); row=next((r for r in rows if r["coin_id"]==cid),None)
        if not row: raise RuntimeError("not in watchlist")
        df=await coin_history(cid,90); a=analyze(df)
        icon={"BUY":"🟢 خرید","SELL":"🔴 فروش","WAIT":"🟡 انتظار"}[a["signal"]]
        tg="\n".join(f"{i+1}) {fp(v)}" for i,v in enumerate(a["targets"]))
        text=(f"📊 *{row['name']} ({row['symbol']})*\n\n"
              f"💰 قیمت: `{fp(a['price'])} USD`\n📡 سیگنال: *{icon}*\n\n"
              f"💪 قدرت روند: *{a['strength']:.0f}%*\n🎯 احتمال مدل: *{a['probability']:.0f}%*\n\n"
              f"📈 RSI: `{a['rsi']:.1f}`\n📊 MACD: `{a['macd']:.6g}`\n"
              f"📈 EMA20: `{fp(a['ema20'])}`\n📈 EMA50: `{fp(a['ema50'])}`\n"
              f"📅 بازده 7روزه: `{a['ret7']:.2f}%`\n📅 بازده 30روزه: `{a['ret30']:.2f}%`\n\n"
              f"🛑 حد ضرر تحلیلی: `{fp(a['stop'])}`\n🎯 اهداف:\n{tg}\n\n"
             +"".join([])+f"🧠 دلایل:\n"+"\n".join("• "+x for x in a["reasons"])+
              "\n\n⚠️ تحلیل آماری است و تضمین سود نیست.")
        await update.callback_query.edit_message_text(text,parse_mode="Markdown",reply_markup=menu())
    except Exception:
        log.exception("analysis failed")
        await update.callback_query.edit_message_text("⚠️ داده تحلیل این ارز فعلاً در دسترس نیست. چند لحظه بعد دوباره امتحان کن.",reply_markup=menu())

async def signals(update,ctx):
    rows=watch(update.effective_user.id)
    if not rows:
        await update.callback_query.edit_message_text("واچ‌لیست خالی است.",reply_markup=menu()); return
    await update.callback_query.edit_message_text("⏳ در حال بررسی واچ‌لیست...")
    out=["📡 *سیگنال‌های واچ‌لیست*\n"]
    for r in rows:
        try:
            a=analyze(await coin_history(r["coin_id"],90))
            ic={"BUY":"🟢","SELL":"🔴","WAIT":"🟡"}[a["signal"]]
            out.append(f"{ic} *{r['symbol']}* — {a['signal']} | قدرت {a['strength']:.0f}% | احتمال مدل {a['probability']:.0f}%")
        except Exception: out.append(f"⚪ *{r['symbol']}* — داده در دسترس نیست")
        await asyncio.sleep(.15)
    await update.callback_query.edit_message_text("\n".join(out),parse_mode="Markdown",reply_markup=menu())

async def alerts(update,ctx):
    uid=update.effective_user.id; c=db(); s=c.execute("SELECT alerts_enabled FROM settings WHERE user_id=?",(uid,)).fetchone()
    new=0 if s["alerts_enabled"] else 1
    c.execute("UPDATE settings SET alerts_enabled=? WHERE user_id=?",(new,uid)); c.commit(); c.close()
    await update.callback_query.edit_message_text(f"🔔 هشدارها: *{'فعال' if new else 'خاموش'}*",parse_mode="Markdown",reply_markup=menu())

async def callback(update,ctx):
    q=update.callback_query; await q.answer(); ensure_user(q.from_user); d=q.data
    if d=="home": await q.edit_message_text("🏠 منوی اصلی",reply_markup=menu())
    elif d=="add": await add_prompt(update,ctx)
    elif d=="watch": await show_watch(update,ctx)
    elif d=="remove": await remove_menu(update,ctx)
    elif d=="analyze": await analyze_menu(update,ctx)
    elif d=="signals": await signals(update,ctx)
    elif d=="alerts": await alerts(update,ctx)
    elif d=="help": await q.edit_message_text("ℹ️ نام یا نماد ارز را جستجو کن؛ ربات از CoinGecko برای فهرست گسترده ارزها و داده بازار استفاده می‌کند.",reply_markup=menu())
    elif d.startswith("coin:"): await select_coin(update,ctx,d.split(":",1)[1])
    elif d.startswith("an:"): await do_an(update,ctx,d.split(":",1)[1])
    elif d.startswith("del:"):
        c=db(); c.execute("DELETE FROM watchlist WHERE user_id=? AND coin_id=?",(q.from_user.id,d.split(":",1)[1])); c.commit(); c.close()
        await q.edit_message_text("✅ از واچ‌لیست حذف شد.",reply_markup=menu())

def main():
    if not BOT_TOKEN: raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    init_db()
    app=Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",start))
    app.add_handler(CallbackQueryHandler(callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND,search_text))
    log.info("Starting bot...")
    app.run_polling(drop_pending_updates=True)

if __name__=="__main__": main()
