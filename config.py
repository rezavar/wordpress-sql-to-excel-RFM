from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

DUMP_DIR = BASE_DIR / "dump"
OUTPUT_DIR = BASE_DIR / "output"
DB_DIR = BASE_DIR / "db"

SQLITE_DB_PATH = DB_DIR / "converted.db"

# encoding پیش‌فرض
DEFAULT_ENCODING = "utf-8"

# پسوندهای مجاز فایل دامپ
DUMP_EXTENSIONS = (".sql", ".gz", ".sql.gz")

EXCEL_SETTINGS = {
    "engine": "openpyxl",
    "max_rows_per_sheet": 500000,
}

# حداکثر ردیف در هر فایل Excel (برای همه خروجی‌های اکسل)
# اگر تعداد ردیف‌ها بیشتر شد، فایل‌های بعدی با شماره (۱، ۲، ۳، ...) ایجاد می‌شوند
EXCEL_MAX_ROWS_PER_FILE = 500000

# تعداد باندهای Quantile برای تحلیل RFM (پیش‌فرض: quintile=5)
RFM_QUANTILE_BANDS = 5

# گروه‌های جدول: نام گروه -> لیست جداول مورد انتظار (بدون پیشوند)
# در فایل دامپ چک می‌شود کدام گروه‌ها به طور کامل وجود دارند
TABLE_GROUPS = {
    "wp": ["users", "wc_order_stats", "usermeta", "wc_customer_lookup"],
    "avanse": ["avans_log_score", "avans_log_refs"],
}
