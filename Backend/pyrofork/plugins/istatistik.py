from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pymongo import MongoClient
from psutil import virtual_memory, cpu_percent, disk_usage
from time import time
import os
import importlib.util
from Backend.helper.custom_filter import CustomFilters  # CustomFilters gerekli

CONFIG_PATH = "/home/debian/dfbot/config.env"
DOWNLOAD_DIR = "/"
bot_start_time = time()
PAGE_SIZE = 10  # 30 günlük detayda sayfa başına gün sayısı

# ---------------- Config Database Okuma ----------------
def read_database_from_config():
    if not os.path.exists(CONFIG_PATH):
        return None
    spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    return getattr(config, "DATABASE", None)

def get_db_urls():
    db_raw = read_database_from_config() or os.getenv("DATABASE") or ""
    return [u.strip() for u in db_raw.split(",") if u.strip()]

# ---------------- Sistem Durumu ----------------
def get_system_status():
    cpu = round(cpu_percent(interval=1), 1)
    ram = round(virtual_memory().percent, 1)
    disk = disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free / (1024 ** 3), 2)  # GB
    free_percent = round((disk.free / disk.total) * 100, 1)
    uptime_sec = int(time() - bot_start_time)
    h, r = divmod(uptime_sec, 3600)
    m, s = divmod(r, 60)
    uptime = f"{h} saat {m} dakika {s} saniye"
    return cpu, ram, free_disk, free_percent, uptime

# ---------------- Upload/Download İstatistikleri ----------------
def get_traffic_stats(db_url):
    client = MongoClient(db_url)
    db_name_list = client.list_database_names()
    if not db_name_list:
        return 0, 0, 0, 0, []
    db = client[db_name_list[0]]

    # Günlük ve 30 günlük veriler
    today = time()
    daily = db["traffic"].find({"date": {"$gte": today - 86400}})  # Son 1 gün
    thirty = db["traffic"].find({"date": {"$gte": today - 2592000}})  # Son 30 gün

    daily_up = sum(d.get("upload", 0) for d in daily)
    daily_down = sum(d.get("download", 0) for d in daily)
    thirty_up = sum(d.get("upload", 0) for d in thirty)
    thirty_down = sum(d.get("download", 0) for d in thirty)

    # 30 günlük detay listesi
    thirty_days = [{"date": d["date_str"], "upload": d.get("upload", 0), "download": d.get("download", 0)} for d in thirty]

    return daily_up, daily_down, thirty_up, thirty_down, thirty_days

def format_size(size_bytes):
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024**2:
        return f"{round(size_bytes/1024,2)} KB"
    elif size_bytes < 1024**3:
        return f"{round(size_bytes/(1024**2),2)} MB"
    else:
        return f"{round(size_bytes/(1024**3),2)} GB"

# ---------------- /istatistik Komutu ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    try:
        db_urls = get_db_urls()
        if not db_urls:
            await message.reply_text("⚠️ Veritabanı bulunamadı!")
            return

        cpu, ram, free_disk, free_percent, uptime = get_system_status()
        daily_up, daily_down, thirty_up, thirty_down, thirty_days = get_traffic_stats(db_urls[0])

        text = (
            f"⌬ <b>İstatistik</b>\n"
            f"│\n"
            f"┟ <b>CPU</b> → {cpu}% | <b>Boş</b> → {free_disk}GB [{free_percent}%]\n"
            f"┖ <b>RAM</b> → {ram}% | <b>Süre</b> → {uptime}\n\n"
            f"📊 <b>Yüklenen / İndirilen</b>\n"
            f"┠ Bugün → Yüklenen: {format_size(daily_up)} | İndirilen: {format_size(daily_down)}\n"
            f"┖ Son 30 Gün → Yüklenen: {format_size(thirty_up)} | İndirilen: {format_size(thirty_down)}"
        )
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("📄 30 Gün Detay", callback_data="30gün_detay:0")]])
        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, reply_markup=keyboard)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("istatistik hata:", e)

# ---------------- Callback Query ----------------
@Client.on_callback_query(filters.regex(r"^(istatistik|30gün_detay)(:\d+)?$") & CustomFilters.owner)
async def handle_stats_callback(client: Client, query: CallbackQuery):
    try:
        db_urls = get_db_urls()
        cpu, ram, free_disk, free_percent, uptime = get_system_status()
        daily_up, daily_down, thirty_up, thirty_down, thirty_days = get_traffic_stats(db_urls[0])

        data_split = query.data.split(":")
        page_type = data_split[0]
        page_num = int(data_split[1]) if len(data_split) > 1 else 0

        if page_type == "istatistik":
            text = (
                f"⌬ <b>İstatistik</b>\n"
                f"│\n"
                f"┟ <b>CPU</b> → {cpu}% | <b>Boş</b> → {free_disk}GB [{free_percent}%]\n"
                f"┖ <b>RAM</b> → {ram}% | <b>Süre</b> → {uptime}\n\n"
                f"📊 <b>Yüklenen / İndirilen</b>\n"
                f"┠ Bugün → Yüklenen: {format_size(daily_up)} | İndirilen: {format_size(daily_down)}\n"
                f"┖ Son 30 Gün → Yüklenen: {format_size(thirty_up)} | İndirilen: {format_size(thirty_down)}\n\n"
                f"Detay için butona basın ⬇️"
            )
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("📄 30 Gün Detay", callback_data="30gün_detay:0")]])

        elif page_type == "30gün_detay":
            total_pages = (len(thirty_days) + PAGE_SIZE - 1) // PAGE_SIZE
            start = page_num * PAGE_SIZE
            end = start + PAGE_SIZE
            page_items = thirty_days[start:end]

            text = f"<b>📄 Son 30 Gün Detay</b> - Sayfa {page_num+1}/{total_pages}\n\n"
            for day in page_items:
                text += f"{day['date']} → Yüklenen: {format_size(day['upload'])} | İndirilen: {format_size(day['download'])}\n"

            buttons = []
            if page_num > 0:
                buttons.append(InlineKeyboardButton("⬅️", callback_data=f"30gün_detay:{page_num-1}"))
            else:
                buttons.append(InlineKeyboardButton("⬅️ Ana Ekran", callback_data="istatistik"))

            if page_num < total_pages - 1:
                buttons.append(InlineKeyboardButton("➡️", callback_data=f"30gün_detay:{page_num+1}"))

            keyboard = InlineKeyboardMarkup([buttons]) if buttons else None

        await query.message.edit_text(text, parse_mode=enums.ParseMode.HTML, reply_markup=keyboard)
        await query.answer()  # Callback yanıtı gönder

    except Exception as e:
        await query.message.edit_text(f"⚠️ Hata: {e}")
        print("istatistik detay hata:", e)
