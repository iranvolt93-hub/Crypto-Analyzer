# Crypto Analysis Bot v3

این نسخه شامل:
- واچ‌لیست قبلی
- منوی پایین Telegram (Reply Keyboard)
- سیستم اشتراک ۳۰/۹۰/۱۸۰ روزه
- پرداخت دستی با شماره کارت + ارسال رسید
- پنل مدیریت
- تأیید/رد رسید توسط ادمین
- تمدید دستی اشتراک توسط ادمین
- مسدود/رفع مسدودی کاربر
- آمار کاربران و اشتراک‌ها
- پیام همگانی
- مهاجرت امن SQLite بدون حذف اطلاعات قبلی

## Railway Variables

Required:
TELEGRAM_BOT_TOKEN=توکن ربات
ADMIN_IDS=123456789

Recommended:
DB_PATH=/data/crypto_bot.db
PAYMENT_CARD=6037xxxxxxxxxxxx
SUPPORT_USERNAME=your_support
HTTP_TIMEOUT=20
MAX_WATCHLIST=100

ADMIN_IDS را با User ID ادمین/ادمین‌ها وارد کنید؛ اگر چند ادمین دارید با کاما جدا کنید:
ADMIN_IDS=123456789,987654321

## Railway Persistence

حتماً برای سرویس Railway یک Volume بسازید و آن را مثلاً روی `/data` Mount کنید.
سپس:
DB_PATH=/data/crypto_bot.db

با این کار فایل SQLite خارج از کد نسخه جدید نگهداری می‌شود و Deployهای بعدی اطلاعات کاربران، واچ‌لیست، اشتراک‌ها و سوابق پرداخت را پاک نمی‌کنند.

## Important

این کد جدول‌های جدید را با CREATE TABLE IF NOT EXISTS می‌سازد و فقط ستون‌های جدیدِ موجود نبودن را با ALTER TABLE اضافه می‌کند.
هیچ DROP TABLE یا حذف اطلاعات قدیمی انجام نمی‌دهد.

## Existing Database

اگر `crypto_bot.db` فعلی را دارید، قبل از Deploy جدید از آن یک Backup بگیرید.
در صورت استفاده از Railway Volume، فایل دیتابیس قبلی را باید در همان Volume قرار دهید/منتقل کنید.
