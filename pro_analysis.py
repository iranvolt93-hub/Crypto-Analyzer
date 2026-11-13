# pro_analysis.py
# -*- coding: utf-8 -*-
"""
MARKET AI Pro — تحلیل حرفه‌ای چندلایه
Candlestick / Chart Patterns / Fibonacci / Pivots / VSA / S&R Clustering
"""
import numpy as np
import pandas as pd


# ============================================================
# 1) الگوهای کندلی
# ============================================================
def detect_candlestick_patterns(df, lookback=5):
    if df is None or len(df) < 10:
        return []
    x = df.tail(10).reset_index(drop=True)
    o, h, l, c = (x[k].astype(float).values for k in ("open", "high", "low", "close"))
    found = []
    def body(i): return abs(c[i] - o[i])
    def range_(i): return max(h[i] - l[i], 1e-9)
    def uw(i): return h[i] - max(o[i], c[i])
    def lw(i): return min(o[i], c[i]) - l[i]
    def bull(i): return c[i] > o[i]
    def bear(i): return c[i] < o[i]
    n = len(x); i = n - 1

    if n >= 2 and bull(i) and bear(i-1):
        if o[i] <= c[i-1] and c[i] >= o[i-1] and body(i) > body(i-1)*1.1:
            found.append({"name":"Bullish Engulfing","name_fa":"پوشای صعودی","bias":"BULLISH","strength":75,"desc":"کندل صعودی قبلی نزولی را دربرگرفته"})
    if n >= 2 and bear(i) and bull(i-1):
        if o[i] >= c[i-1] and c[i] <= o[i-1] and body(i) > body(i-1)*1.1:
            found.append({"name":"Bearish Engulfing","name_fa":"پوشای نزولی","bias":"BEARISH","strength":75,"desc":"کندل نزولی قبلی صعودی را دربرگرفته"})
    if body(i) > 0 and range_(i) > 0:
        if lw(i) >= body(i)*2 and uw(i) <= body(i)*0.5:
            found.append({"name":"Hammer","name_fa":"چکش","bias":"BULLISH","strength":65,"desc":"سایه پایینی بلند؛ فشار خرید"})
        if uw(i) >= body(i)*2 and lw(i) <= body(i)*0.5:
            found.append({"name":"Shooting Star","name_fa":"ستاره ثاقب","bias":"BEARISH","strength":65,"desc":"سایه بالایی بلند؛ فشار فروش"})
    if body(i) <= range_(i)*0.1 and range_(i) > 0:
        found.append({"name":"Doji","name_fa":"دوجی","bias":"NEUTRAL","strength":40,"desc":"بلاتکلیفی بازار"})
    if n >= 3:
        if bear(i-2) and body(i-1) < body(i-2)*0.5 and bull(i) and c[i] > (o[i-2]+c[i-2])/2:
            found.append({"name":"Morning Star","name_fa":"ستاره صبحگاهی","bias":"BULLISH","strength":80,"desc":"برگشتی صعودی سه‌کندلی"})
        if bull(i-2) and body(i-1) < body(i-2)*0.5 and bear(i) and c[i] < (o[i-2]+c[i-2])/2:
            found.append({"name":"Evening Star","name_fa":"ستاره شامگاهی","bias":"BEARISH","strength":80,"desc":"برگشتی نزولی سه‌کندلی"})
    if n >= 4:
        if all(bull(i-k) and c[i-k] > o[i-k-1] for k in (0,1,2)):
            found.append({"name":"Three White Soldiers","name_fa":"سه سرباز سفید","bias":"BULLISH","strength":85,"desc":"سه کندل صعودی متوالی"})
        if all(bear(i-k) and c[i-k] < o[i-k-1] for k in (0,1,2)):
            found.append({"name":"Three Black Crows","name_fa":"سه کلاغ سیاه","bias":"BEARISH","strength":85,"desc":"سه کندل نزولی متوالی"})
    if n >= 2:
        if bear(i-1) and bull(i) and o[i] > c[i-1] and c[i] < o[i-1] and body(i) < body(i-1)*0.6:
            found.append({"name":"Bullish Harami","name_fa":"هارامی صعودی","bias":"BULLISH","strength":60,"desc":"کندل کوچک داخل کندل نزولی"})
        if bull(i-1) and bear(i) and o[i] < c[i-1] and c[i] > o[i-1] and body(i) < body(i-1)*0.6:
            found.append({"name":"Bearish Harami","name_fa":"هارامی نزولی","bias":"BEARISH","strength":60,"desc":"کندل کوچک داخل کندل صعودی"})
    if n >= 3:
        if bear(i-2) and bull(i-1) and bull(i) and c[i] > o[i-2] and o[i-1] > c[i-2]:
            found.append({"name":"Three Inside Up","name_fa":"سه داخلی صعودی","bias":"BULLISH","strength":78,"desc":"تأیید هارامی صعودی"})
        if bull(i-2) and bear(i-1) and bear(i) and c[i] < o[i-2] and o[i-1] < c[i-2]:
            found.append({"name":"Three Inside Down","name_fa":"سه داخلی نزولی","bias":"BEARISH","strength":78,"desc":"تأیید هارامی نزولی"})
    seen, out = set(), []
    for f in found:
        if f["name"] not in seen:
            seen.add(f["name"]); out.append(f)
    return out


# ============================================================
# 2) الگوهای کلاسیک چارت
# ============================================================
def detect_chart_patterns(df, window=60):
    if df is None or len(df) < window:
        return []
    x = df.tail(window).reset_index(drop=True)
    h, l, c = x["high"].astype(float), x["low"].astype(float), x["close"].astype(float)
    n = len(x); radius = 3
    highs, lows = [], []
    for i in range(radius, n - radius):
        if h.iloc[i] >= h.iloc[i-radius:i+radius+1].max():
            highs.append((i, float(h.iloc[i])))
        if l.iloc[i] <= l.iloc[i-radius:i+radius+1].min():
            lows.append((i, float(l.iloc[i])))
    patterns = []; price = float(c.iloc[-1])

    if len(highs) >= 2:
        a, b = highs[-2], highs[-1]
        if abs(a[1]-b[1])/max(a[1],1) < 0.015 and b[0]-a[0] >= 5:
            neckline = min(float(l.iloc[a[0]:b[0]+1].min()), 1e9)
            if neckline < min(a[1],b[1]) and price < b[1]:
                patterns.append({"name":"Double Top","name_fa":"سقف دوقلو","bias":"BEARISH","strength":72,"neckline":neckline,"desc":f"شکست زیر {neckline:.6g}"})
    if len(lows) >= 2:
        a, b = lows[-2], lows[-1]
        if abs(a[1]-b[1])/max(b[1],1) < 0.015 and b[0]-a[0] >= 5:
            neckline = max(float(h.iloc[a[0]:b[0]+1].max()), -1e9)
            if neckline > max(a[1],b[1]) and price > b[1]:
                patterns.append({"name":"Double Bottom","name_fa":"کف دوقلو","bias":"BULLISH","strength":72,"neckline":neckline,"desc":f"شکست بالای {neckline:.6g}"})
    if len(highs) >= 3:
        s1, hh, s2 = highs[-3], highs[-2], highs[-1]
        if hh[1] > s1[1] and hh[1] > s2[1] and abs(s1[1]-s2[1])/max(s1[1],1) < 0.04 and s1[0] < hh[0] < s2[0]:
            neckline = min(float(l.iloc[s1[0]:s2[0]+1].min()), 1e9)
            patterns.append({"name":"Head and Shoulders","name_fa":"سر و شانه","bias":"BEARISH","strength":78,"neckline":neckline,"desc":f"شکست زیر گردن {neckline:.6g}"})
    if len(lows) >= 3:
        s1, hh, s2 = lows[-3], lows[-2], lows[-1]
        if hh[1] < s1[1] and hh[1] < s2[1] and abs(s1[1]-s2[1])/max(s2[1],1) < 0.04 and s1[0] < hh[0] < s2[0]:
            neckline = max(float(h.iloc[s1[0]:s2[0]+1].max()), -1e9)
            patterns.append({"name":"Inverse Head and Shoulders","name_fa":"سر و شانه معکوس","bias":"BULLISH","strength":78,"neckline":neckline,"desc":f"شکست بالای گردن {neckline:.6g}"})
    if len(highs) >= 2 and len(lows) >= 2:
        hs = [x[1] for x in highs[-3:]]; ls = [x[1] for x in lows[-3:]]
        if len(hs) >= 2 and len(ls) >= 2:
            h_slope = (hs[-1]-hs[0])/max(len(hs)-1,1)
            l_slope = (ls[-1]-ls[0])/max(len(ls)-1,1)
            h_range = max(hs)-min(hs); l_range = max(ls)-min(ls)
            if h_range/max(hs[0],1) < 0.02 and l_slope > 0:
                patterns.append({"name":"Ascending Triangle","name_fa":"مثلث صعودی","bias":"BULLISH","strength":68,"desc":"مقاومت افقی + کف بالارونده"})
            if l_range/max(ls[0],1) < 0.02 and h_slope < 0:
                patterns.append({"name":"Descending Triangle","name_fa":"مثلث نزولی","bias":"BEARISH","strength":68,"desc":"حمایت افقی + سقف پایین‌رونده"})
            if h_slope < 0 and l_slope > 0:
                patterns.append({"name":"Symmetrical Triangle","name_fa":"مثلث متقارن","bias":"NEUTRAL","strength":55,"desc":"فشردگی قیمت"})
    if n >= 15:
        prev_move = (float(c.iloc[-1])/float(c.iloc[-15])-1)*100
        recent_range = (float(h.tail(7).max())/float(l.tail(7).min())-1)*100
        if prev_move > 6 and recent_range < 4:
            patterns.append({"name":"Bull Flag","name_fa":"پرچم صعودی","bias":"BULLISH","strength":70,"desc":"ادامه صعودی"})
        if prev_move < -6 and recent_range < 4:
            patterns.append({"name":"Bear Flag","name_fa":"پرچم نزولی","bias":"BEARISH","strength":70,"desc":"ادامه نزولی"})
    seen, out = set(), []
    for p in patterns:
        if p["name"] not in seen:
            seen.add(p["name"]); out.append(p)
    return out[:4]


# ============================================================
# 3) فیبوناچی
# ============================================================
def fibonacci_levels(df, lookback=100):
    if df is None or len(df) < 20:
        return {}
    x = df.tail(lookback)
    swing_low = float(x["low"].min()); swing_high = float(x["high"].max())
    rng = swing_high - swing_low
    if rng <= 0: return {}
    price = float(x["close"].iloc[-1])
    direction = "UP" if price > (swing_low+swing_high)/2 else "DOWN"
    levels = {}
    for ratio in (0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0):
        levels[ratio] = swing_high - rng*ratio if direction == "UP" else swing_low + rng*ratio
    extensions = {}
    for ratio in (1.272, 1.618, 2.0):
        extensions[ratio] = swing_high + rng*(ratio-1) if direction == "UP" else swing_low - rng*(ratio-1)
    nearest_ratio, nearest_val, nearest_dist = None, None, 1e9
    for ratio, val in levels.items():
        d = abs(price-val)/price*100
        if d < nearest_dist:
            nearest_dist, nearest_ratio, nearest_val = d, ratio, val
    return {"swing_high":swing_high,"swing_low":swing_low,"direction":direction,
            "levels":levels,"extensions":extensions,
            "nearest":{"ratio":nearest_ratio,"value":nearest_val,"distance_pct":nearest_dist}}


# ============================================================
# 4) پیوت پوینت
# ============================================================
def pivot_points(df, method="standard"):
    if df is None or len(df) < 2: return {}
    prev = df.iloc[-2]
    H, L, C = float(prev["high"]), float(prev["low"]), float(prev["close"])
    P = (H+L+C)/3
    result = {"P": P}
    if method == "standard":
        result.update({"R1":2*P-L,"S1":2*P-H,"R2":P+(H-L),"S2":P-(H-L),"R3":H+2*(P-L),"S3":L-2*(H-P)})
    elif method == "fibonacci":
        rng = H-L
        result.update({"R1":P+0.382*rng,"S1":P-0.382*rng,"R2":P+0.618*rng,"S2":P-0.618*rng,"R3":P+rng,"S3":P-rng})
    elif method == "camarilla":
        rng = H-L
        result.update({"R1":C+rng*1.1/12,"S1":C-rng*1.1/12,"R2":C+rng*1.1/6,"S2":C-rng*1.1/6,
                       "R3":C+rng*1.1/4,"S3":C-rng*1.1/4,"R4":C+rng*1.1/2,"S4":C-rng*1.1/2})
    elif method == "woodie":
        P = (H+L+2*C)/4; result["P"] = P
        result.update({"R1":2*P-L,"S1":2*P-H,"R2":P+(H-L),"S2":P-(H-L)})
    return result


def pivot_point_multi(df):
    return {"standard":pivot_points(df,"standard"),"fibonacci":pivot_points(df,"fibonacci"),
            "camarilla":pivot_points(df,"camarilla"),"woodie":pivot_points(df,"woodie")}


# ============================================================
# 5) خوشه‌بندی S/R
# ============================================================
def cluster_support_resistance(df, lookback=120, n_clusters=5):
    if df is None or len(df) < 20:
        return {"supports":[],"resistances":[]}
    x = df.tail(lookback)
    price = float(x["close"].iloc[-1])
    highs = x["high"].astype(float).values; lows = x["low"].astype(float).values
    pivots = []; radius = 2
    for i in range(radius, len(highs)-radius):
        if highs[i] == max(highs[i-radius:i+radius+1]): pivots.append(highs[i])
        if lows[i] == min(lows[i-radius:i+radius+1]): pivots.append(lows[i])
    if not pivots: return {"supports":[],"resistances":[]}
    pivots = sorted(pivots); tolerance = price*0.01
    clusters = []; current = [pivots[0]]
    for p in pivots[1:]:
        if p-current[-1] <= tolerance: current.append(p)
        else: clusters.append(current); current = [p]
    clusters.append(current)
    scored = [{"level":sum(c)/len(c),"touches":len(c),"score":len(c)} for c in clusters]
    supports = sorted([s for s in scored if s["level"] < price], key=lambda z:z["level"], reverse=True)[:n_clusters]
    resistances = sorted([s for s in scored if s["level"] > price], key=lambda z:z["level"])[:n_clusters]
    return {"supports":supports,"resistances":resistances,"price":price}


# ============================================================
# 6) تحلیل حجم VSA
# ============================================================
def volume_analysis(df, lookback=50):
    if df is None or len(df) < lookback: return {}
    x = df.tail(lookback).copy()
    c = x["close"].astype(float); v = x["volume"].astype(float)
    h = x["high"].astype(float); l = x["low"].astype(float)
    obv = (np.sign(c.diff().fillna(0))*v).cumsum()
    obv_slope = (obv.iloc[-5:].mean()-obv.iloc[:5].mean()) if len(obv) >= 10 else 0
    clv = ((c-l)-(h-c))/(h-l).replace(0, np.nan)
    ad = (clv.fillna(0)*v).cumsum()
    ad_slope = (ad.iloc[-5:].mean()-ad.iloc[:5].mean()) if len(ad) >= 10 else 0
    vma = v.rolling(20).mean().iloc[-1] if len(v) >= 20 else v.mean()
    v_ratio = float(v.iloc[-1]/vma) if vma > 0 else 1.0
    climax = "NONE"
    if v.iloc[-1] > vma*2.5:
        climax = "SELLING_CLIMAX" if c.iloc[-1] < c.iloc[-2] else "BUYING_CLIMAX"
    if v.iloc[-1] < vma*0.5:
        climax = "NO_DEMAND" if c.iloc[-1] > c.iloc[-2] else "NO_SUPPLY"
    obv_div = "NONE"
    if len(c) >= 20:
        price_up = c.iloc[-1] > c.iloc[-15]; obv_up = obv.iloc[-1] > obv.iloc[-15]
        if price_up and not obv_up: obv_div = "BEARISH"
        elif not price_up and obv_up: obv_div = "BULLISH"
    verdict = ("BULLISH" if (obv_slope > 0 and ad_slope > 0 and v_ratio > 1.2)
               else "BEARISH" if (obv_slope < 0 and ad_slope < 0 and v_ratio > 1.2)
               else "NEUTRAL")
    return {"obv":float(obv.iloc[-1]),"obv_slope":float(obv_slope),
            "ad":float(ad.iloc[-1]),"ad_slope":float(ad_slope),
            "volume_ratio":v_ratio,"vsa":climax,"obv_divergence":obv_div,"verdict":verdict}


# ============================================================
# 7) همبستگی BTC
# ============================================================
def correlation_with_btc(alt_df, btc_df, lookback=100):
    if alt_df is None or btc_df is None: return None
    a = alt_df.tail(lookback)["close"].astype(float).pct_change().dropna()
    b = btc_df.tail(lookback)["close"].astype(float).pct_change().dropna()
    n = min(len(a), len(b))
    if n < 20: return None
    a, b = a.iloc[-n:].reset_index(drop=True), b.iloc[-n:].reset_index(drop=True)
    corr = float(a.corr(b))
    if corr >= 0.85: verdict = "ریسک متراکم بالا"
    elif corr >= 0.6: verdict = "همبستگی بالا"
    elif corr >= 0.3: verdict = "همبستگی متوسط"
    else: verdict = "مستقل"
    return {"correlation":corr,"verdict":verdict,"samples":n}


# ============================================================
# 8) Confluence Score
# ============================================================
def confluence_score(ta, candles, patterns, fib, pivots, vol, sr):
    layers = []; total_w = 0.0; weighted = 0.0
    def add(name, score, weight, reason=""):
        nonlocal total_w, weighted
        total_w += weight; weighted += score*weight
        layers.append({"layer":name,"score":round(score,1),"weight":weight,"reason":reason})

    trend = ta.get("trend","NEUTRAL"); structure = ta.get("structure","RANGE")
    ts = 50.0
    if trend == "BULLISH": ts = 85
    elif trend == "BULLISH_WEAK": ts = 70
    elif trend == "BEARISH": ts = 15
    elif trend == "BEARISH_WEAK": ts = 30
    if structure == "BULLISH": ts = min(100, ts+10)
    elif structure == "BEARISH": ts = max(0, ts-10)
    add("روند", ts, 0.20, f"{trend}/{structure}")

    rsi = ta.get("rsi",50)
    rs = 50.0
    if 45 <= rsi <= 65: rs = 80
    elif 30 <= rsi < 45: rs = 55
    elif 65 < rsi <= 75: rs = 45
    elif rsi > 75: rs = 20
    else: rs = 40
    add("RSI", rs, 0.10, f"RSI={rsi:.1f}")

    mh = ta.get("macd_hist",0); mp = ta.get("macd_hist_prev",mh)
    ms = 50.0
    if mh > 0 and mh > mp: ms = 80
    elif mh > 0: ms = 65
    elif mh < 0 and mh < mp: ms = 20
    else: ms = 35
    add("MACD", ms, 0.10, f"Hist={mh:+.5f}")

    adx = ta.get("adx",0)
    ads = min(100, 30+adx*2) if adx >= 15 else 30
    add("ADX", ads, 0.08, f"ADX={adx:.1f}")

    cb = sum(1 for p in candles if p["bias"]=="BULLISH")
    cs = sum(1 for p in candles if p["bias"]=="BEARISH")
    csc = max(0, min(100, 50+(cb-cs)*15))
    add("کندل", csc, 0.08, f"{cb}↑/{cs}↓")

    pb = sum(1 for p in patterns if p["bias"]=="BULLISH")
    ps = sum(1 for p in patterns if p["bias"]=="BEARISH")
    psc = max(0, min(100, 50+(pb-ps)*20))
    add("الگو", psc, 0.10, f"{pb}↑/{ps}↓")

    price = ta.get("price",0)
    fs = 50.0
    if fib and fib.get("nearest"):
        nr = fib["nearest"]
        if nr["distance_pct"] < 0.5:
            fs = 75 if nr["ratio"] in (0.382,0.618,0.5) else 55
    srs = 50.0
    if sr and price > 0:
        sup = sr.get("supports",[]); res = sr.get("resistances",[])
        near_s = any(abs(s["level"]-price)/price < 0.015 for s in sup[:2])
        near_r = any(abs(r["level"]-price)/price < 0.015 for r in res[:2])
        if near_s and not near_r: srs = 80
        elif near_r and not near_s: srs = 30
        elif near_s and near_r: srs = 50
    add("سطوح", (fs*0.5+srs*0.5), 0.12, f"فیبو{fs:.0f}/S&R{srs:.0f}")

    vs = 50.0
    if vol:
        v = vol.get("verdict","NEUTRAL")
        if v == "BULLISH": vs = 80
        elif v == "BEARISH": vs = 20
        if vol.get("obv_divergence") == "BULLISH": vs += 10
        elif vol.get("obv_divergence") == "BEARISH": vs -= 10
        if vol.get("vsa") in ("BUYING_CLIMAX","NO_SUPPLY"): vs += 5
        elif vol.get("vsa") in ("SELLING_CLIMAX","NO_DEMAND"): vs -= 5
        vs = max(0, min(100, vs))
    add("حجم", vs, 0.12, vol.get("vsa","NONE") if vol else "NONE")

    pvs = 50.0
    if pivots and price > 0:
        p = pivots.get("P", price)
        pvs = 70 if price > p else 30
    add("پیوت", pvs, 0.10, f"P={pivots.get('P',0):.6g}" if pivots else "")

    final = weighted/total_w if total_w > 0 else 50.0
    if final >= 70: bias = "STRONG_BULLISH"
    elif final >= 58: bias = "BULLISH"
    elif final >= 45: bias = "NEUTRAL"
    elif final >= 32: bias = "BEARISH"
    else: bias = "STRONG_BEARISH"
    return {"score":round(final,1),"bias":bias,"layers":layers}


# ============================================================
# 9) مدیریت ریسک
# ============================================================
def position_sizing(entry, stop, account_size=1000.0, risk_pct=1.0, atr=None, leverage_ok=False):
    if entry <= 0 or stop <= 0 or risk_pct <= 0: return None
    risk_per_unit = abs(entry-stop)
    if risk_per_unit <= 0: return None
    risk_amount = account_size*risk_pct/100
    quantity = risk_amount/risk_per_unit
    position_value = quantity*entry
    position_pct = position_value/account_size*100
    if position_pct > 100 and not leverage_ok:
        position_pct = 100; quantity = account_size/entry; risk_amount = quantity*risk_per_unit
    return {"entry":entry,"stop":stop,"risk_per_unit":risk_per_unit,
            "risk_pct_per_unit":risk_per_unit/entry*100,"quantity":quantity,
            "position_value":quantity*entry,"position_pct_of_account":position_pct,
            "max_loss":quantity*risk_per_unit,"atr":atr}


def risk_reward_targets(entry, stop, atr, direction="BUY"):
    risk = abs(entry-stop)
    if risk <= 0: return []
    if direction == "BUY":
        return [{"label":"TP1","price":entry+risk*1.5,"rr":1.5},
                {"label":"TP2","price":entry+risk*2.5,"rr":2.5},
                {"label":"TP3","price":entry+risk*4.0,"rr":4.0}]
    return [{"label":"TP1","price":entry-risk*1.5,"rr":1.5},
            {"label":"TP2","price":entry-risk*2.5,"rr":2.5},
            {"label":"TP3","price":entry-risk*4.0,"rr":4.0}]
