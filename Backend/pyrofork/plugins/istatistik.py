from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
from psutil import virtual_memory, cpu_percent, disk_usage
from time import time
import os
import importlib.util
from datetime import datetime, timedelta

CONFIG_PATH = "/home/debian/dfbot/config.env"
DOWNLOAD_DIR = "/"
bot_start_time = time()

PAGE_SIZE = 10  # Her sayfada gösterilecek gün sayısı

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
    uptime = f"{h}s{m}d{s}s"
    return cpu, ram, free_disk, free_percent, uptime

# ---------------- Ağ Trafiği ----------------
def format_size(size):
    tb = 1024 ** 4
    gb = 1024 ** 3
    if size >= tb:
        return f"{size / tb:.2f}TB"
    elif size >= gb:
        return f"{size / gb:.2f}GB"
    else:
        return f"{size / (1024 ** 2):.2f}MB"

def get_network_usage():
    import psutil
    counters = psutil.net_io_counters()
    return counters.bytes_sent, counters.bytes_recv

# ---------------- Günlük / 30 Günlük Trafik ----------------
def get_traffic_stats(db_url):
    client = MongoClient(db_url)
    db = client["TrafficStats"]
    col = db["daily_usage"]

    today = datetime.utcnow().strftime("%Y-%m-%d")
    daily_doc = col.find_one({"date": today})
    daily_upload = daily_doc.get("upload", 0) if daily_doc else 0
    daily_download = daily_doc.get("download", 0) if daily_doc else 0

    # Son 30 gün
    thirty_days_list = []
    for i in range(30):
        day = (datetime.utcnow() - timedelta(days=i)).strftime("%Y-%m-%d")
        doc = col.find_one({"date": day})
        thirty_days_list.append({
            "date": day,
            "upload": doc.get("upload",0) if doc else 0,
            "download": doc.get("download",0) if doc else 0
        })

    thirty_days_list.sort(key=lambda x: x["date"], reverse=True)
    thirty_upload_total = sum(d["upload"] for d in thirty_days_list)
    thirty_download_total = sum(d["download"] for d in thirty_days_list)

    return daily_upload, daily_download, thirty_upload_total, thirty_download_total, thirty_days_list

# ---------------- /istatistik Komutu ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    try:
        db_urls = get_db_urls()
        cpu, ram, free_disk, free_percent, uptime = get_system_status()
        daily_up, daily_down, thirty_up, thirty_down, thirty_days = get_traffic_stats(db_urls[0])

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
        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, reply_markup=keyboard)

        # Mesaj detaylarını callback ile gösteriyoruz

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("istatistik hata:", e)

# ---------------- Callback Query - Sayfalama ----------------
@Client.on_callback_query(filters.regex(r"^30gün_detay(:\d+)?$") & CustomFilters.owner)
async def show_30day_detail(client, query):
    try:
        db_urls = get_db_urls()
        _, _, _, _, thirty_days = get_traffic_stats(db_urls[0])

        page = 0
        if ":" in query.data:
            page = int(query.data.split(":")[1])

        total_pages = (len(thirty_days) + PAGE_SIZE - 1) // PAGE_SIZE
        start = page * PAGE_SIZE
        end = start + PAGE_SIZE
        page_items = thirty_days[start:end]

        text = f"<b>📄 Son 30 Gün Detay</b> - Sayfa {page+1}/{total_pages}\n\n"
        for day in page_items:
            text += f"{day['date']} → Yüklenen: {format_size(day['upload'])} | İndirilen: {format_size(day['download'])}\n"

        buttons = []
        if page > 0:
            buttons.append(InlineKeyboardButton("⬅️", callback_data=f"30gün_detay:{page-1}"))
        if page < total_pages - 1:
            buttons.append(InlineKeyboardButton("➡️", callback_data=f"30gün_detay:{page+1}"))

        keyboard = InlineKeyboardMarkup([buttons]) if buttons else None
        await query.message.edit_text(text, parse_mode=enums.ParseMode.HTML, reply_markup=keyboard)

    except Exception as e:
        await query.message.edit_text(f"⚠️ Hata: {e}")
        print("30gün_detay hata:", e)
