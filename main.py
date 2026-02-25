import os
import sys

from bidi.algorithm import get_display

from config import DUMP_DIR, OUTPUT_DIR, SQLITE_DB_PATH

# رفع خطای Unicode در ویندوز
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from flows import run_import_new_data, run_use_existing_data


def rtl(text: str) -> str:
    """تبدیل متن فارسی/عربی برای نمایش درست در کنسول."""
    return get_display(text)


def _clear_screen():
    os.system("cls" if sys.platform == "win32" else "clear")


def main():
    while True:
        print(rtl("=== SQL to Excel Tool ==="))
        print(rtl(f"پوشه دامپ: {DUMP_DIR}"))
        print(rtl(f"خروجی: {OUTPUT_DIR}"))
        print(rtl(f"دیتابیس SQLite: {SQLITE_DB_PATH}"))

        print(rtl("\nیک گزینه را انتخاب کنید:"))
        print(rtl("  ۱) وارد کردن داده‌های جدید"))
        print(rtl("  ۲) استفاده از داده‌های وارد شده"))
        print(rtl("  ۰) خروج"))
        try:
            choice = input(rtl("\nانتخاب:  ")).strip()
        except (KeyboardInterrupt, EOFError):
            print(rtl("\nلغو شد."))
            return

        if choice == "1":
            run_import_new_data()
            return
        if choice == "2":
            run_use_existing_data()
            return
        if choice == "0":
            print(rtl("خروج."))
            return
        # غیر از ۰، ۱، ۲: پاک کردن صفحه و پرسیدن دوباره
        _clear_screen()


if __name__ == "__main__":
    main()
