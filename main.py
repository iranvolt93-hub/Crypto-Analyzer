def analysis_text(a):
    if not a:
        return "❌ اطلاعات بازار در دسترس نیست."

    unit = {
        "GOLD18": " تومان/گرم",
        "XAU":    " دلار/اونس",
    }.get(a["symbol"], " دلار")

    body = [
        f"📊 <b>تحلیل {escape(a['symbol'])}</b>",
        "",
        f"💰 قیمت: <b>{a['price']:,.4f}{unit}</b>",
    ]

    if a.get("mode") == "snapshot":
        body += [
            f"📈 تغییر روز: {a['r1']:+.2f}%",
        ]
        if a.get("high") and a.get("low"):
            body.append(
                f"🔺 سقف: {a['high']:,.0f}  |  🔻 کف: {a['low']:,.0f}"
            )
        body += [
            "",
            "ℹ️ تحلیل این دارایی بر پایه snapshot لحظه‌ای TGJU است "
            "(سری تاریخی معتبر در دسترس نیست).",
        ]
    else:
        body += [
            f"📈 EMA9: {a['ema9']:,.4f}",
            f"📉 EMA21: {a['ema21']:,.4f}",
            f"RSI14: <b>{a['rsi']:.1f}</b>",
            f"بازده کوتاه‌مدت: {a['r1']:+.2f}%",
            f"بازده ۶ دوره: {a['r6']:+.2f}%",
            f"بازده ۲۴ دوره: {a['r24']:+.2f}%",
        ]

    body += [
        "",
        f"🎯 سیگنال: <b>{signal_fa(a['signal'])}</b>",
        f"💪 قدرت: <b>{a['strength']:.0f}%</b>",
        f"🎲 احتمال سود: <b>{a['probability']:.0f}%</b>",
        "",
        "⚠️ تحلیل آموزشی است و تضمین سود نیست.",
    ]

    return "\n".join(body)
