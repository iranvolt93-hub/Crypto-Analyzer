# pro_analysis.py
# -*- coding: utf-8 -*-
"""
MARKET AI Pro — تحلیل حرفه‌ای چندلایه
Candlestick / Chart Patterns / Fibonacci / Pivots / VSA / S&R Clustering
"""
import math
import numpy as np
import pandas as pd

# ============================================================
# 1) الگوهای کندلی — Candlestick Patterns
# ============================================================

def detect_candlestick_patterns(df, lookback=5):
    """آخرین lookback کندل را بررسی می‌کند و الگوهای معتبر را برمی‌گرداند."""
    if df is None or len(df) < 10:
        return []
    x = df.tail(10).reset_index(drop=True)
    o, h, l, c = (x[k].astype(float).values for k in ("open", "high", "low", "close"))
    found = []

    def body(i):  return abs(c[i] - o[i])
    def range_(i): return max(h[i] - l[i], 1e-9)
    def upper_wick(i): return h[i] - max(o[i], c[i])
    def lower_wick(i): return min(o[i], c[i]) - l[i]
    def is_bull(i): return c[i] > o[i]
    def is_bear(i): return c[i] < o[i]

    n = len(x)
    i = n - 1  # آخرین کندل

    # --- Engulfing ---
    if n >= 2 and is_bull(i) and is_bear(i - 1):
        if o[i] <= c[i - 1] and c[i] >= o[i - 1] and body(i) > body(i - 1) * 1.1:
            found.append({"name": "Bullish Engulfing", "name_fa": "پوشای صعودی",
                          "bias": "BULLISH", "strength": 75,
                          "desc": "کندل صعودی قبلی نزولی را کاملاً دربرگرفته"})
    if n >= 2 and is_bear(i) and is_bull(i - 1):
        if o[i] >= c[i - 1] and c[i] <= o[i - 1] and body(i) > body(i - 1) * 1.1:
            found.append({"name": "Bearish Engulfing", "name_fa": "پوشای نزولی",
                          "bias": "BEARISH", "strength": 75,
                          "desc": "کندل نزولی قبلی صعودی را کاملاً دربرگرفته"})

    # --- Hammer / Shooting Star ---
    if body(i) > 0 and range_(i) > 0:
        if lower_wick(i) >= body(i) * 2 and upper_wick(i) <= body(i) * 0.5:
            found.append({"name": "Hammer", "name_fa": "چکش",
                          "bias": "BULLISH", "strength": 65,
                          "desc": "سایه پایینی بلند؛ فشار خرید در انتهای کندل"})
        if upper_wick(i) >= body(i) * 2 and lower_wick(i) <= body(i) * 0.5:
            found.append({"name": "Shooting Star", "name_fa": "ستاره ثاقب",
                          "bias": "BEARISH", "strength": 65,
                          "desc": "سایه بالایی بلند؛ فشار فروش در انتهای کندل"})

    # --- Doji ---
    if body(i) <= range_(i) * 0.1 and range_(i) > 0:
        found.append({"name": "Doji", "name_fa": "دوجی",
                      "bias": "NEUTRAL", "strength": 40,
                      "desc": "بدنه بسیار کوچک؛ بلاتکلیفی بازار"})

    # --- Morning Star / Evening Star (3 کندلی) ---
    if n >= 3:
        if (is_bear(i - 2) and body(i - 1) < body(i - 2) * 0.5
                and is_bull(i) and c[i] > (o[i - 2] + c[i - 2]) / 2):
            found.append({"name": "Morning Star", "name_fa": "ستاره صبحگاهی",
                          "bias": "BULLISH", "strength": 80,
                          "desc": "الگوی سه‌کندلی برگشتی صعودی قوی"})
        if (is_bull(i - 2) and body(i - 1) < body(i - 2) * 0.5
                and is_bear(i) and c[i] < (o[i - 2] + c[i - 2]) / 2):
            found.append({"name": "Evening Star", "name_fa": "ستاره شامگاهی",
                          "bias": "BEARISH", "strength": 80,
                          "desc": "الگوی سه‌کندلی برگشتی نزولی قوی"})

    # --- Three White Soldiers / Three Black Crows ---
    if n >= 3:
        if all(is_bull(i - k) and c[i - k] > o[i - k - 1] for k in (0, 1, 2)):
            found.append({"name": "Three White Soldiers", "name_fa": "سه سرباز سفید",
                          "bias": "BULLISH", "strength": 85,
                          "desc": "سه کندل صعودی متوالی؛ روند صعودی قوی"})
        if n >= 4 and all(is_bear(i - k) and c[i - k] < o[i - k - 1] for k in (0, 1, 2)):
            found.append({"name": "Three Black Crows", "name_fa": "سه کلاغ سیاه",
                          "bias": "BEARISH", "strength": 85,
                          "desc": "سه کندل نزولی متوالی؛ روند نزولی قوی"})

    # --- Harami ---
    if n >= 2:
        if (is_bear(i - 1) and is_bull(i)
                and o[i] > c[i - 1] and c[i] < o[i - 1]
                and body(i) < body(i - 1) * 0.6):
            found.append({"name": "Bullish Harami", "name_fa": "هارامی صعودی",
                          "bias": "BULLISH", "strength": 60,
                          "desc": "کندل کوچک داخل کندل نزولی قبلی"})
        if (is_bull(i - 1) and is_bear(i)
                and o[i] < c[i - 1] and c[i] > o[i - 1]
                and body(i) < body(i - 1) * 0.6):
            found.append({"name": "Bearish Harami", "name_fa": "هارامی نزولی",
                          "bias": "BEARISH", "strength": 60,
                          "desc": "کندل کوچک داخل کندل صعودی قبلی"})

    # --- Three Inside Up / Down ---
    if n >= 3:
        if (is_bear(i - 2) and is_bull(i - 1) and is_bull(i)
                and c[i] > o[i - 2] and o[i - 1] > c[i - 2]):
            found.append({"name": "Three Inside Up", "name_fa": "سه داخلی صعودی",
                          "bias": "BULLISH", "strength": 78,
                          "desc": "تأیید الگوی هارامی صعودی با کندل سوم"})
        if (is_bull(i - 2) and is_bear(i - 1) and is_bear(i)
                and c[i] < o[i - 2] and o[i - 1] < c[i - 2]):
            found.append({"name": "Three Inside Down", "name_fa": "سه داخلی نزولی",
                          "bias": "BEARISH", "strength": 78,
                          "desc": "تأیید الگوی هارامی نزولی با کندل سوم"})

    # dedupe by name
    seen, out = set(), []
    for f in found:
        if f["name"] not in seen:
            seen.add(f["name"])
            out.append(f)
    return out


# ============================================================
# 2) الگوهای کلاسیک چارت — Chart Patterns
# ============================================================

def detect_chart_patterns(df, window=60):
    """الگوهای کلاسیک با تشخیص سقف/کف پیوت."""
    if df is None or len(df) < window:
        return []
    x = df.tail(window).reset_index(drop=True)
    h, l, c = x["high"].astype(float), x["low"].astype(float), x["close"].astype(float)
    n = len(x)
    radius = 3
    highs, lows = [], []
    for i in range(radius, n - radius):
        if h.iloc[i] >= h.iloc[i-radius:i+radius+1].max():
            highs.append((i, float(h.iloc[i])))
        if l.iloc[i] <= l.iloc[i-radius:i+radius+1].min():
            lows.append((i, float(l.iloc[i])))
    patterns = []
    price = float(c.iloc[-1])

    # --- Double Top ---
    if len(highs) >= 2:
        a, b = highs[-2], highs[-1]
        if abs(a[1] - b[1]) / max(a[1], 1) < 0.015 and b[0] - a[0] >= 5:
            neckline = min(float(l.iloc[a[0]:b[0]+1].min()), 1e9)
            if neckline < min(a[1], b[1]) and price < b[1]:
                patterns.append({"name": "Double Top", "name_fa": "سقف دوقلو",
                                 "bias": "BEARISH", "strength": 72,
                                 "neckline": neckline,
                                 "desc": f"شکست زیر {neckline:.6g} تأیید می‌کند"})

    # --- Double Bottom ---
    if len(lows) >= 2:
        a, b = lows[-2], lows[-1]
        if abs(a[1] - b[1]) / max(b[1], 1) < 0.015 and b[0] - a[0] >= 5:
            neckline = max(float(h.iloc[a[0]:b[0]+1].max()), -1e9)
            if neckline > max(a[1], b[1]) and price > b[1]:
                patterns.append({"name": "Double Bottom", "name_fa": "کف دوقلو",
                                 "bias": "BULLISH", "strength": 72,
                                 "neckline": neckline,
                                 "desc": f"شکست بالای {neckline:.6g} تأیید می‌کند"})

    # --- Head & Shoulders ---
    if len(highs) >= 3:
        s1, hh, s2 = highs[-3], highs[-2], highs[-1]
        if (hh[1] > s1[1] and hh[1] > s2[1]
                and abs(s1[1] - s2[1]) / max(s1[1], 1) < 0.04
                and hh[0] > s1[0] and hh[0] < s2[0]):
            neckline = min(float(l.iloc[s1[0]:s2[0]+1].min()), 1e9)
            patterns.append({"name": "Head and Shoulders", "name_fa": "سر و شانه",
                             "bias": "BEARISH", "strength": 78,
                             "neckline": neckline,
                             "desc": f"تأیید با شکست زیر گردن {neckline:.6g}"})

    # --- Inverse Head & Shoulders ---
    if len(lows) >= 3:
        s1, hh, s2 = lows[-3], lows[-2], lows[-1]
        if (hh[1] < s1[1] and hh[1] < s2[1]
                and abs(s1[1] - s2[1]) / max(s2[1], 1) < 0.04
                and hh[0] > s1[0] and hh[0] < s2[0]):
            neckline = max(float(h.iloc[s1[0]:s2[0]+1].max()), -1e9)
            patterns.append({"name": "Inverse Head and Shoulders", "name_fa": "سر و شانه معکوس",
                             "bias": "BULLISH", "strength": 78,
                             "neckline": neckline,
                             "desc": f"تأیید با شکست بالای گردن {neckline:.6g}"})

    # --- Ascending / Descending Triangle ---
    if len(highs) >= 2 and len(lows) >= 2:
        h_series = [x[1] for x in highs[-3:]]
        l_series = [x[1] for x in lows[-3:]]
        if len(h_series) >= 2 and len(l_series) >= 2:
            h_slope = (h_series[-1] - h_series[0]) / max(len(h_series) - 1, 1)
            l_slope = (l_series[-1] - l_series[0]) / max(len(l_series) - 1, 1)
            h_range = max(h_series) - min(h_series)
            l_range = max(l_series) - min(l_series)
            if h_range / max(h_series[0], 1) < 0.02 and l_slope > 0:
                patterns.append({"name": "Ascending Triangle", "name_fa": "مثلث صعودی",
                                 "bias": "BULLISH", "strength": 68,
                                 "desc": "مقاومت افقی + کف‌های بالارونده"})
            if l_range / max(l_series[0], 1) < 0.02 and h_slope < 0:
                patterns.append({"name": "Descending Triangle", "name_fa": "مثلث نزولی",
                                 "bias": "BEARISH", "strength": 68,
                                 "desc": "حمایت افقی + سقف‌های پایین‌رونده"})
            if h_slope < 0 and l_slope > 0:
                patterns.append({"name": "Symmetrical Triangle", "name_fa": "مثلث متقارن",
                                 "bias": "NEUTRAL", "strength": 55,
                                 "desc": "فشردگی قیمت؛ آماده حرکت انفجاری"})

    # --- Bull Flag / Bear Flag ---
    if n >= 15:
        prev_move = (float(c.iloc[-1]) / float(c.iloc[-15]) - 1) * 100
        recent_range = (float(h.tail(7).max()) / float(l.tail(7).min()) - 1) * 100
        if prev_move > 6 and recent_range < 4:
            patterns.append({"name": "Bull Flag", "name_fa": "پرچم صعودی",
                             "bias": "BULLISH", "strength": 70,
                             "desc": "صعود قوی + استراحت فشرده؛ ادامه صعودی"})
        if prev_move < -6 and recent_range < 4:
            patterns.append({"name": "Bear Flag", "name_fa": "پرچم نزولی",
                             "bias": "BEARISH", "strength": 70,
                             "desc": "نزول قوی + استراحت فشرده؛ ادامه نزولی"})

    seen, out = set(), []
    for p in patterns:
        if p["name"] not in seen:
            seen.add(p["name"])
            out.append(p)
    return out[:4]


# ============================================================
# 3) سطوح فیبوناچی — Fibonacci Retracement & Extension
# ============================================================

def fibonacci_levels(df, lookback=100):
    if df is None or len(df) < 20:
        return {}
    x = df.tail(lookback)
    swing_low = float(x["low"].min())
    swing_high = float(x["high"].max())
    rng = swing_high - swing_low
    if rng <= 0:
        return {}
    price = float(x["close"].iloc[-1])
    # تشخیص جهت موج
    direction = "UP" if price > (swing_low + swing_high) / 2 else "DOWN"
    levels = {}
    for ratio in (0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0):
        if direction == "UP":
            levels[ratio] = swing_high - rng * ratio
        else:
            levels[ratio] = swing_low + rng * ratio
    # Extension
    extensions = {}
    for ratio in (1.272, 1.618, 2.0):
        if direction == "UP":
            extensions[ratio] = swing_high + rng * (ratio - 1.0)
        else:
            extensions[ratio] = swing_low - rng * (ratio - 1.0)
    # نزدیک‌ترین سطح
    nearest_ratio, nearest_val, nearest_dist = None, None, 1e9
    for ratio, val in levels.items():
        d = abs(price - val) / price * 100
        if d < nearest_dist:
            nearest_dist, nearest_ratio, nearest_val = d, ratio, val
    return {
        "swing_high": swing_high, "swing_low": swing_low,
        "direction": direction, "levels": levels, "extensions": extensions,
        "nearest": {"ratio": nearest_ratio, "value": nearest_val, "distance_pct": nearest_dist}
    }


# ============================================================
# 4) پیوت پوینت — Pivot Points (4 روش)
# ============================================================

def pivot_points(df, method="standard"):
    if df is None or len(df) < 2:
        return {}
    prev = df.iloc[-2]
    H, L, C = float(prev["high"]), float(prev["low"]), float(prev["close"])
    P = (H + L + C) / 3
    result = {"P": P}
    if method == "standard":
        result.update({"R1": 2*P - L, "S1": 2*P - H,
                       "R2": P + (H - L), "S2": P - (H - L),
                       "R3": H + 2*(P - L), "S3": L - 2*(H - P)})
    elif method == "fibonacci":
        rng = H - L
        result.update({"R1": P + 0.382*rng, "S1": P - 0.382*rng,
                       "R2": P + 0.618*rng, "S2": P - 0.618*rng,
                       "R3": P + 1.000*rng, "S3": P - 1.000*rng})
    elif method == "camarilla":
        rng = H - L
        result.update({"R1": C + rng*1.1/12, "S1": C - rng*1.1/12,
                       "R2": C + rng*1.1/6,  "S2": C - rng*1.1/6,
                       "R3": C + rng*1.1/4,  "S3": C - rng*1.1/4,
                       "R4": C + rng*1.1/2,  "S4": C - rng*1.1/2})
    elif method == "woodie":
        P = (H + L + 2*C) / 4
        result["P"] = P
        result.update({"R1": 2*P - L, "S1": 2*P - H,
                       "R2": P + (H - L), "S2": P - (H - L)})
    return result


# ============================================================
# 5) خوشه‌بندی حمایت/مقاومت — S&R Clustering
# ============================================================

def cluster_support_resistance(df, lookback=120, n_clusters=5):
    """سطوح S/R را با هیستوگرام چگالی قیمت پیدا می‌کند."""
    if df is None or len(df) < 20:
        return {"supports": [], "resistances": []}
    x = df.tail(lookback)
    price = float(x["close"].iloc[-1])
    highs = x["high"].astype(float).values
    lows = x["low"].astype(float).values
    # تمام نقاط چرخش
    pivots = []
    radius = 2
    for i in range(radius, len(highs) - radius):
        if highs[i] == max(highs[i-radius:i+radius+1]):
            pivots.append(highs[i])
        if lows[i] == min(lows[i-radius:i+radius+1]):
            pivots.append(lows[i])
    if not pivots:
        return {"supports": [], "resistances": []}
    # هیستوگرام ساده
    pivots = sorted(pivots)
    tolerance = price * 0.01  # 1%
    clusters = []
    current = [pivots[0]]
    for p in pivots[1:]:
        if p - current[-1] <= tolerance:
            current.append(p)
        else:
            clusters.append(current)
            current = [p]
    clusters.append(current)
    # امتیاز هر خوشه = تعداد + برخورد با قیمت فعلی
    scored = []
    for cl in clusters:
        level = sum(cl) / len(cl)
        score = len(cl)
        scored.append({"level": level, "touches": len(cl), "score": score})
    supports = sorted([s for s in scored if s["level"] < price],
                      key=lambda z: z["level"], reverse=True)[:n_clusters]
    resistances = sorted([s for s in scored if s["level"] > price],
                         key=lambda z: z["level"])[:n_clusters]
    return {"supports": supports, "resistances": resistances, "price": price}


# ============================================================
# 6) تحلیل حجم حرفه‌ای — VSA
# ============================================================

def volume_analysis(df, lookback=50):
    if df is None or len(df) < lookback:
        return {}
    x = df.tail(lookback).copy()
    c = x["close"].astype(float)
    v = x["volume"].astype(float)
    h = x["high"].astype(float)
    l = x["low"].astype(float)

    # OBV
    obv = (np.sign(c.diff().fillna(0)) * v).cumsum()
    obv_slope = (obv.iloc[-5:].mean() - obv.iloc[:5].mean()) if len(obv) >= 10 else 0

    # Accumulation / Distribution
    clv = ((c - l) - (h - c)) / (h - l).replace(0, np.nan)
    ad = (clv.fillna(0) * v).cumsum()
    ad_slope = (ad.iloc[-5:].mean() - ad.iloc[:5].mean()) if len(ad) >= 10 else 0

    # Volume ratio
    vma = v.rolling(20).mean().iloc[-1] if len(v) >= 20 else v.mean()
    v_ratio = float(v.iloc[-1] / vma) if vma > 0 else 1.0

    # VSA: climax detection
    climax = "NONE"
    if v.iloc[-1] > vma * 2.5:
        if c.iloc[-1] < c.iloc[-2]:
            climax = "SELLING_CLIMAX"  # حجم بالا + افت = احتمال پایان ریزش
        else:
            climax = "BUYING_CLIMAX"   # حجم بالا + رشد = احتمال پایان صعود
    # No Demand / No Supply
    if v.iloc[-1] < vma * 0.5:
        if c.iloc[-1] > c.iloc[-2]:
            climax = "NO_DEMAND"
        else:
            climax = "NO_SUPPLY"

    # Divergence با OBV
    obv_divergence = "NONE"
    if len(c) >= 20:
        price_up = c.iloc[-1] > c.iloc[-15]
        obv_up = obv.iloc[-1] > obv.iloc[-15]
        if price_up and not obv_up:
            obv_divergence = "BEARISH"
        elif not price_up and obv_up:
            obv_divergence = "BULLISH"

    return {
        "obv": float(obv.iloc[-1]),
        "obv_slope": float(obv_slope),
        "ad": float(ad.iloc[-1]),
        "ad_slope": float(ad_slope),
        "volume_ratio": v_ratio,
        "vsa": climax,
        "obv_divergence": obv_divergence,
        "verdict": (
            "BULLISH" if (obv_slope > 0 and ad_slope > 0 and v_ratio > 1.2)
            else "BEARISH" if (obv_slope < 0 and ad_slope < 0 and v_ratio > 1.2)
            else "NEUTRAL"
        )
    }


# ============================================================
# 7) همبستگی با BTC — Correlation
# ============================================================

def correlation_with_btc(alt_df, btc_df, lookback=100):
    if alt_df is None or btc_df is None:
        return None
    a = alt_df.tail(lookback)["close"].astype(float).pct_change().dropna()
    b = btc_df.tail(lookback)["close"].astype(float).pct_change().dropna()
    n = min(len(a), len(b))
    if n < 20:
        return None
    a, b = a.iloc[-n:].reset_index(drop=True), b.iloc[-n:].reset_index(drop=True)
    corr = float(a.corr(b))
    if corr >= 0.85:
        verdict = "ریسک متراکم بالا؛ حرکت هم‌جهت با BTC"
    elif corr >= 0.6:
        verdict = "همبستگی بالا با BTC"
    elif corr >= 0.3:
        verdict = "همبستگی متوسط"
    else:
        verdict = "مستقل از BTC"
    return {"correlation": corr, "verdict": verdict, "samples": n}


# ============================================================
# 8) Confluence Scoring — قلب سیستم حرفه‌ای
# ============================================================

def confluence_score(ta, candles, patterns, fib, pivots, vol, sr):
    """
    امتیاز هم‌گرایی از ۰ تا ۱۰۰.
    هر لایه امتیاز جدا می‌دهد و با وزن مشخص جمع می‌شود.
    """
    layers = []
    total_weight = 0.0
    weighted_sum = 0.0

    def add(name, score, weight, reason=""):
        nonlocal total_weight, weighted_sum
        total_weight += weight
        weighted_sum += score * weight
        layers.append({"layer": name, "score": round(score, 1),
                       "weight": weight, "reason": reason})

    # --- 1. روند چند تایم‌فریمی ---
    trend = ta.get("trend", "NEUTRAL")
    structure = ta.get("structure", "RANGE")
    trend_score = 50.0
    if trend == "BULLISH": trend_score = 85
    elif trend == "BULLISH_WEAK": trend_score = 70
    elif trend == "BEARISH": trend_score = 15
    elif trend == "BEARISH_WEAK": trend_score = 30
    if structure == "BULLISH": trend_score = min(100, trend_score + 10)
    elif structure == "BEARISH": trend_score = max(0, trend_score - 10)
    add("روند", trend_score, 0.20, f"{trend} / {structure}")

    # --- 2. RSI و مومنتوم ---
    rsi = ta.get("rsi", 50)
    rsi_score = 50.0
    if 45 <= rsi <= 65: rsi_score = 80
    elif 30 <= rsi < 45: rsi_score = 55
    elif 65 < rsi <= 75: rsi_score = 45
    elif rsi > 75: rsi_score = 20
    else: rsi_score = 40
    add("RSI", rsi_score, 0.10, f"RSI={rsi:.1f}")

    # --- 3. MACD ---
    macd_hist = ta.get("macd_hist", 0)
    macd_prev = ta.get("macd_hist_prev", macd_hist)
    macd_score = 50.0
    if macd_hist > 0 and macd_hist > macd_prev: macd_score = 80
    elif macd_hist > 0: macd_score = 65
    elif macd_hist < 0 and macd_hist < macd_prev: macd_score = 20
    else: macd_score = 35
    add("MACD", macd_score, 0.10, f"Hist={macd_hist:+.5f}")

    # --- 4. ADX (قدرت روند) ---
    adx = ta.get("adx", 0)
    adx_score = min(100, 30 + adx * 2) if adx >= 15 else 30
    add("ADX", adx_score, 0.08, f"ADX={adx:.1f}")

    # --- 5. الگوهای کندلی ---
    cs_bull = sum(1 for p in candles if p["bias"] == "BULLISH")
    cs_bear = sum(1 for p in candles if p["bias"] == "BEARISH")
    cs_score = 50 + (cs_bull - cs_bear) * 15
    cs_score = max(0, min(100, cs_score))
    add("کندل", cs_score, 0.08,
        f"{cs_bull} صعودی / {cs_bear} نزولی")

    # --- 6. الگوهای کلاسیک ---
    cp_bull = sum(1 for p in patterns if p["bias"] == "BULLISH")
    cp_bear = sum(1 for p in patterns if p["bias"] == "BEARISH")
    cp_score = 50 + (cp_bull - cp_bear) * 20
    cp_score = max(0, min(100, cp_score))
    add("الگو", cp_score, 0.10,
        f"{cp_bull} صعودی / {cp_bear} نزولی")

    # --- 7. فیبوناچی + S&R ---
    price = ta.get("price", 0)
    fib_score = 50.0
    if fib and fib.get("nearest"):
        nr = fib["nearest"]
        if nr["distance_pct"] < 0.5:
            # نزدیک سطح کلیدی → واکنش احتمالی
            fib_score = 75 if nr["ratio"] in (0.382, 0.618, 0.5) else 55
    # Confluence با S/R
    sr_score = 50.0
    if sr and price > 0:
        supports = sr.get("supports", [])
        resistances = sr.get("resistances", [])
        near_support = any(abs(s["level"] - price) / price < 0.015 for s in supports[:2])
        near_resistance = any(abs(r["level"] - price) / price < 0.015 for r in resistances[:2])
        if near_support and not near_resistance: sr_score = 80
        elif near_resistance and not near_support: sr_score = 30
        elif near_support and near_resistance: sr_score = 50
    # ترکیب فیبو + S&R به عنوان یک لایه
    level_score = (fib_score * 0.5 + sr_score * 0.5)
    add("سطوح", level_score, 0.12,
        f"فیبو {fib_score:.0f} / S&R {sr_score:.0f}")

    # --- 8. حجم ---
    vol_score = 50.0
    if vol:
        v = vol.get("verdict", "NEUTRAL")
        if v == "BULLISH": vol_score = 80
        elif v == "BEARISH": vol_score = 20
        if vol.get("obv_divergence") == "BULLISH": vol_score += 10
        elif vol.get("obv_divergence") == "BEARISH": vol_score -= 10
        if vol.get("vsa") in ("BUYING_CLIMAX", "NO_SUPPLY"): vol_score += 5
        elif vol.get("vsa") in ("SELLING_CLIMAX", "NO_DEMAND"): vol_score -= 5
        vol_score = max(0, min(100, vol_score))
    add("حجم", vol_score, 0.12, vol.get("vsa", "NONE") if vol else "NONE")

    # --- 9. پیوت ---
    pivot_score = 50.0
    if pivots and price > 0:
        p = pivots.get("P", price)
        if price > p: pivot_score = 70
        else: pivot_score = 30
    add("پیوت", pivot_score, 0.10, f"P={pivots.get('P', 0):.6g}" if pivots else "")

    final = weighted_sum / total_weight if total_weight > 0 else 50.0
    # Bias نهایی
    if final >= 70: bias = "STRONG_BULLISH"
    elif final >= 58: bias = "BULLISH"
    elif final >= 45: bias = "NEUTRAL"
    elif final >= 32: bias = "BEARISH"
    else: bias = "STRONG_BEARISH"
    return {
        "score": round(final, 1),
        "bias": bias,
        "layers": layers,
    }


# ============================================================
# 9) مدیریت ریسک حرفه‌ای — Position Sizing
# ============================================================

def position_sizing(entry, stop, account_size=1000.0, risk_pct=1.0,
                    atr=None, leverage_ok=False):
    """
    محاسبه اندازه پوزیشن بر اساس ریسک.
    account_size: سرمایه کل
    risk_pct: درصد ریسک مجاز (معمولا 0.5 تا 2)
    """
    if entry <= 0 or stop <= 0 or risk_pct <= 0:
        return None
    risk_per_unit = abs(entry - stop)
    if risk_per_unit <= 0:
        return None
    risk_amount = account_size * risk_pct / 100
    quantity = risk_amount / risk_per_unit
    position_value = quantity * entry
    position_pct = position_value / account_size * 100
    if position_pct > 100 and not leverage_ok:
        position_pct = 100
        quantity = account_size / entry
        risk_amount = quantity * risk_per_unit
    return {
        "entry": entry, "stop": stop,
        "risk_per_unit": risk_per_unit,
        "risk_pct_per_unit": risk_per_unit / entry * 100,
        "quantity": quantity,
        "position_value": quantity * entry,
        "position_pct_of_account": position_pct,
        "max_loss": quantity * risk_per_unit,
        "atr": atr,
    }


def risk_reward_targets(entry, stop, atr, direction="BUY"):
    """اهداف بر اساس ATR و ضریب R/R."""
    risk = abs(entry - stop)
    if risk <= 0:
        return []
    if direction == "BUY":
        return [
            {"label": "TP1", "price": entry + risk * 1.5, "rr": 1.5},
            {"label": "TP2", "price": entry + risk * 2.5, "rr": 2.5},
            {"label": "TP3", "price": entry + risk * 4.0, "rr": 4.0},
        ]
    else:
        return [
            {"label": "TP1", "price": entry - risk * 1.5, "rr": 1.5},
            {"label": "TP2", "price": entry - risk * 2.5, "rr": 2.5},
            {"label": "TP3", "price": entry - risk * 4.0, "rr": 4.0},
        ]


# ============================================================
# 10) تحلیل جامع — orchestrator
# ============================================================

def full_pro_analysis(df, btc_df=None, account_size=1000.0):
    """همه تحلیل‌های حرفه‌ای را اجرا و یک دیکشنری جامع برمی‌گرداند."""
    if df is None or len(df) < 50:
        return None
    # پایه: ta از ماژول موجود شما (پاس داده می‌شود) — اینجا فقط لایه‌های جدید
    candles = detect_candlestick_patterns(df)
    patterns = detect_chart_patterns(df)
    fib = fibonacci_levels(df)
    pivots = pivot_point_multi(df)
    vol = volume_analysis(df)
    sr = cluster_support_resistance(df)
    corr = correlation_with_btc(df, btc_df) if btc_df is not None else None
    return {
        "candles": candles,
        "patterns": patterns,
        "fib": fib,
        "pivots": pivots,
        "volume": vol,
        "sr": sr,
        "correlation_btc": corr,
        "position_sizing": position_sizing,
        "risk_reward": risk_reward_targets,
    }


def pivot_point_multi(df):
    """چهار متد پیوت را برمی‌گرداند."""
    return {
        "standard":   pivot_points(df, "standard"),
        "fibonacci":  pivot_points(df, "fibonacci"),
        "camarilla":  pivot_points(df, "camarilla"),
        "woodie":     pivot_points(df, "woodie"),
    }
