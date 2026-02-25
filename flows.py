"""جریان‌های کاری: وارد کردن داده جدید و استفاده از دادهٔ موجود."""
import shutil
from pathlib import Path

import jdatetime
from bidi.algorithm import get_display

from config import DUMP_DIR, OUTPUT_DIR, SQLITE_DB_PATH, TABLE_GROUPS
from core.customer_purchases import (
    CUSTOMER_PURCHASES_VIEW,
    create_customer_purchases_view,
    get_customer_purchases_row_count,
)
from core.db_manager import SQLiteManager
from core.dump_reader import DumpReader
from core.excel_exporter import ExcelExporter
from core.importer import DumpImporter
from core.rfm_constants import create_rfm_constant_excel
from core.rfm_data import RFM_DATA_TABLE, create_rfm_data_table, get_rfm_data_row_count
from core.user_full_data import (
    USER_FULL_DATA_TABLE,
    create_user_full_data_table,
    get_user_full_data_row_count,
)
from utils.helpers import create_output_folder, write_output_readme


def rtl(text: str) -> str:
    """تبدیل متن فارسی/عربی برای نمایش درست در کنسول."""
    return get_display(text)


def select_dump_file() -> str | None:
    """نمایش لیست فایل‌ها و انتخاب توسط کاربر."""
    reader = DumpReader(DUMP_DIR)
    files = reader.list_files()

    if not files:
        print(rtl(f"هیچ فایل دامپی در پوشه {DUMP_DIR} یافت نشد."))
        print(rtl("فایل‌های مجاز: .sql, .gz, .sql.gz"))
        return None

    print(rtl("\nفایل‌های دامپ موجود:"))
    print("-" * 50)
    for i, f in enumerate(files):
        comp = rtl(" [فشرده]") if f["compressed"] else ""
        print(f"  {i + 1}. {f['name']} ({f['size_mb']} MB){comp}")
    print("-" * 50)

    while True:
        try:
            choice = input(rtl("شماره فایل را وارد کنید (یا Enter برای اولین فایل، 0 برای خروج):  ")).strip()
            if not choice:
                idx = 0
            else:
                idx = int(choice)
                if idx == 0:
                    return None
                idx -= 1
            path = reader.select_file(idx)
            if path:
                return str(path)
            print(rtl("شماره نامعتبر است."))
        except ValueError:
            print(rtl("لطفاً یک عدد وارد کنید."))
        except (KeyboardInterrupt, EOFError):
            print(rtl("\nلغو شد."))
            return None


def _ask_rfm_base_date() -> str:
    """از کاربر مبنای محاسبه RFM را می‌گیرد: از ابتدا (۰) یا تاریخ شمسی (۱)."""
    print(rtl("\nمبنای محاسبات RFM:"))
    print(rtl("  ۰) از ابتدای تراکنش‌ها"))
    print(rtl("  ۱) می‌خوام تاریخ انتخاب کنم"))
    while True:
        try:
            choice = input(rtl("\nانتخاب:  ")).strip()
            if choice == "0":
                return "0"
            if choice == "1":
                date_val = input(rtl("تاریخ شمسی (مثال 1404/01/20):  ")).strip()
                return date_val if date_val else "0"
            print(rtl("لطفاً ۰ یا ۱ وارد کنید."))
        except (KeyboardInterrupt, EOFError):
            return "0"


def run_import_new_data() -> None:
    """وارد کردن دامپ جدید، ساخت viewها، خروجی Excel و کپی دیتابیس به پوشه خروجی."""
    rfm_from_shamsi_date = _ask_rfm_base_date()
    if str(rfm_from_shamsi_date).strip() and str(rfm_from_shamsi_date).strip() != "0":
        print(rtl(f"مبنای محاسبات RFM (شمسی): {rfm_from_shamsi_date}"))
    else:
        print(rtl("مبنای محاسبات RFM (شمسی): از ابتدا"))

    # ۱. خالی کردن دیتابیس موقت
    with SQLiteManager(SQLITE_DB_PATH) as db:
        dropped = db.clear_all_tables()
    print(rtl(f"\nدیتابیس موقت خالی شد ({dropped} جدول حذف شد)."))

    dump_path = select_dump_file()
    if not dump_path:
        return

    reader = DumpReader()
    info = reader.get_info(dump_path)
    print(rtl(f"\nفایل انتخاب شده: {info['name']}"))
    print(rtl(f"حجم: {info['size_mb']} MB"))
    print(rtl(f"فشرده: {'بله' if info['compressed'] else 'خیر'}"))

    prefix = reader.detect_prefix(dump_path)
    if prefix:
        print(rtl(f"پیشوند تشخیص داده شده: '{prefix}'"))
    else:
        print(rtl("پیشوندی تشخیص داده نشد."))

    complete_groups = reader.get_complete_groups(dump_path, prefix) if TABLE_GROUPS else []
    if TABLE_GROUPS:
        print(rtl("\nبررسی لیست‌ها:"))
        for group_name in TABLE_GROUPS:
            status = "detect" if group_name in complete_groups else "not found"
            print(rtl(f"{group_name}: {status}"))

    if complete_groups:
        print(rtl("\nدر حال وارد کردن جداول به دیتابیس موقت..."))
        importer = DumpImporter(SQLITE_DB_PATH)
        result = importer.import_complete_groups(dump_path, complete_groups, prefix)
        print(rtl(f"  جداول ایجاد شده: {result['tables_created']}"))
        print(rtl(f"  دستورات INSERT اجرا شده: {result['inserts_count']}"))
        if result["errors"]:
            print(rtl("  خطاها:"))
            for err in result["errors"][:5]:
                print(rtl(f"    - {err}"))
            if len(result["errors"]) > 5:
                print(rtl(f"    ... و {len(result['errors']) - 5} خطای دیگر"))

    table_row_counts: dict[str, int] = {}
    if complete_groups:
        with SQLiteManager(SQLITE_DB_PATH) as db:
            idx_result = db.ensure_recommended_indexes()
            if idx_result["created"] > 0:
                print(rtl(f"  ایندکس‌های پیشنهادی ایجاد شد ({idx_result['created']} مورد)."))
            table_row_counts = db.get_table_row_counts()

            if "wp" in complete_groups:
                if create_customer_purchases_view(db):
                    count = get_customer_purchases_row_count(db)
                    table_row_counts[CUSTOMER_PURCHASES_VIEW] = count
                    print(rtl(f"  جدول اطلاعات خرید مشتری ایجاد شد ({count} رکورد)."))
                else:
                    print(rtl("  خطا در ایجاد جدول اطلاعات خرید مشتری."))

                if create_user_full_data_table(db):
                    count = get_user_full_data_row_count(db)
                    table_row_counts[USER_FULL_DATA_TABLE] = count
                    print(rtl(f"  جدول user_full_data ایجاد شد ({count} رکورد)."))
                else:
                    print(rtl("  خطا در ایجاد جدول user_full_data."))

                if create_rfm_data_table(db, from_shamsi_date=rfm_from_shamsi_date):
                    count = get_rfm_data_row_count(db)
                    table_row_counts[RFM_DATA_TABLE] = count
                    if str(rfm_from_shamsi_date).strip() and str(rfm_from_shamsi_date).strip() != "0":
                        print(rtl(f"  جدول rfm_data ایجاد شد ({count} رکورد) - از تاریخ شمسی {rfm_from_shamsi_date}."))
                    else:
                        print(rtl(f"  جدول rfm_data ایجاد شد ({count} رکورد) - بدون فیلتر تاریخ."))
                else:
                    print(rtl("  خطا در ایجاد جدول rfm_data."))

    folder_name = prefix.rstrip("_") if prefix else "output"
    output_folder = create_output_folder(OUTPUT_DIR, folder_name)
    write_output_readme(
        output_folder,
        info["name"],
        info["size_mb"],
        complete_groups=complete_groups,
        table_groups=TABLE_GROUPS,
        table_row_counts=table_row_counts,
        rfm_from_shamsi_date=rfm_from_shamsi_date,
    )
    print(rtl(f"\nپوشه خروجی: {output_folder}"))
    print(rtl("فایل README.txt ایجاد شد."))

    if CUSTOMER_PURCHASES_VIEW in table_row_counts:
        headers = [
            "شناسه کاربر",
            "نام کاربر",
            "ایمیل",
            "شماره موبایل",
            "شناسه سفارش",
            "تاریخ خرید",
            "مبلغ خرید",
            "وضعیت سفارش",
        ]
        with SQLiteManager(SQLITE_DB_PATH) as db:
            exporter = ExcelExporter(db, output_folder)
            paths = exporter.export_view_chunked(
                CUSTOMER_PURCHASES_VIEW,
                output_base_name="user_orders",
                column_headers=headers,
            )
            for p in paths:
                print(rtl(f"فایل Excel: {p.name}"))

    if USER_FULL_DATA_TABLE in table_row_counts:
        with SQLiteManager(SQLITE_DB_PATH) as db:
            exporter = ExcelExporter(db, output_folder)
            paths = exporter.export_view_chunked(
                USER_FULL_DATA_TABLE,
                output_base_name="user_full_data",
            )
            for p in paths:
                print(rtl(f"فایل Excel: {p.name}"))

    if RFM_DATA_TABLE in table_row_counts:
        with SQLiteManager(SQLITE_DB_PATH) as db:
            exporter = ExcelExporter(db, output_folder)
            paths = exporter.export_view_chunked(
                RFM_DATA_TABLE,
                output_base_name="rfm_data",
                column_formats={
                    "total_spent": "#,##0",
                    "last_order_amount": "#,##0",
                },
            )
            for p in paths:
                print(rtl(f"فایل Excel: {p.name}"))

            # ساخت فایل ثابت‌ها/لیبل‌های پیشنهادی RFM برای استفاده کاربر و مراحل بعد
            const_path = create_rfm_constant_excel(db, output_folder)
            print(rtl(f"فایل Excel: {const_path.name}"))

    # کپی دیتابیس موقت به پوشه خروجی
    dest_db = output_folder / "converted.db"
    shutil.copy2(SQLITE_DB_PATH, dest_db)
    print(rtl(f"کپی دیتابیس به پوشه خروجی: {dest_db.name}"))


def run_use_existing_data() -> Path | None:
    """لیست پوشه‌های خروجی، انتخاب یکی توسط کاربر؛ برمی‌گرداند Path پوشه یا None."""
    output_path = Path(OUTPUT_DIR)
    if not output_path.exists():
        output_path.mkdir(parents=True, exist_ok=True)

    subdirs = [p for p in output_path.iterdir() if p.is_dir()]
    # مرتب‌سازی بر اساس تاریخ ایجاد؛ جدیدترین بالا
    subdirs.sort(key=lambda p: p.stat().st_ctime, reverse=True)

    if not subdirs:
        print(rtl(f"هیچ پوشه‌ای در {OUTPUT_DIR} یافت نشد."))
        return None

    print(rtl("\nپوشه‌های خروجی موجود:"))
    print("-" * 50)
    for i, p in enumerate(subdirs):
        try:
            ctime = p.stat().st_ctime
            date_str = jdatetime.datetime.fromtimestamp(ctime).strftime("%Y/%m/%d %H:%M")
        except Exception:
            date_str = "-"
        file_count = sum(1 for x in p.iterdir() if x.is_file())
        print(rtl(f"  {i + 1}. {p.name}  {date_str}  ({file_count})"))
    print("-" * 50)

    while True:
        try:
            choice = input(rtl("شماره پوشه را وارد کنید (۰ برای خروج):  ")).strip()
            if choice == "0":
                return None
            idx = int(choice)
            if 1 <= idx <= len(subdirs):
                chosen = subdirs[idx - 1]
                print(rtl(f"انتخاب شد: {chosen.name}"))
                return chosen
            print(rtl("شماره نامعتبر است."))
        except ValueError:
            print(rtl("لطفاً یک عدد وارد کنید."))
        except (KeyboardInterrupt, EOFError):
            print(rtl("\nلغو شد."))
            return None
