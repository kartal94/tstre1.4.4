from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
from psutil import virtual_memory, cpu_percent, disk_usage
from time import time
import os
import importlib.util
import json
from datetime import datetime, timedelta

CONFIG_PATH = "/home/debian/dfbot/config.env"
DOWNLOAD_DIR = "/"
bot_start_time = time()
TRAFFIC_FILE = "/tmp/traffic.json"  # Docker içinde geçici dosya

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

# ---------------- Database İstatistikleri ----------------
def get_db_stats(url):
    client = MongoClient(url)
    db_name_list = client.list_database_names()
    if not db_name_list:
        return 0, 0, 0.0
    db = client[db_name_list[0]]
    movies = db["movie"].count_documents({})
    series = db["tv"].count_documents({})
    stats = db.command("dbstats")
    storage_mb = round(stats.get("storageSize", 0) / (1024 * 1024), 2)
    return movies, series, storage_mb

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

# ---------------- Traffic Dosyası ----------------
def read_traffic():
    if not os.path.exists(TRAFFIC_FILE):
        return {}
    with open(TRAFFIC_FILE, "r") as f:
        return json.load(f)

def write_traffic(data):
    with open(TRAFFIC_FILE, "w") as f:
        json.dump(data, f)

def add_traffic(upload_bytes: int, download_bytes: int):
    data = read_traffic()
    today = datetime.now().strftime("%Y-%m-%d")
    if today not in data:
        data[today] = {"upload": 0, "download": 0}
    data[today]["upload"] += upload_bytes
    data[today]["download"] += download_bytes
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    data = {k: v for k, v in data.items() if k >= thirty_days_ago}
    write_traffic(data)

def get_traffic_stats():
    data = read_traffic()
    today = datetime.now().strftime("%Y-%m-%d")
    daily = data.get(today, {"upload": 0, "download": 0})
    thirty_up = sum(v["upload"] for v in data.values())
    thirty_down = sum(v["download"] for v in data.values())
    return daily["upload"], daily["download"], thirty_up, thirty_down

# ---------------- Mesaj Metinleri ----------------
def main_stats_text(cpu, ram, free_disk, free_percent, uptime, movies, series, storage_mb):
    daily_up, daily_down, thirty_up, thirty_down = get_traffic_stats()
    return (
        f"⌬ <b>İstatistik</b>\n"
        f"│\n"
        f"┠ <b>Filmler:</b> {movies}\n"
        f"┠ <b>Diziler:</b> {series}\n"
        f"┖ <b>Depolama:</b> {storage_mb} MB\n\n"
        f"┟ <b>CPU</b> → {cpu}% | <b>Boş</b> → {free_disk}GB [{free_percent}%]\n"
        f"┖ <b>RAM</b> → {ram}% | <b>Süre</b> → {uptime}\n\n"
        f"┠ <b>Bugün Yüklenen:</b> {round(daily_up/1024/1024,2)} MB\n"
        f"┖ <b>Bugün İndirilen:</b> {round(daily_down/1024/1024,2)} MB\n"
        f"┠ <b>Son 30 Gün Yüklenen:</b> {round(thirty_up/1024/1024,2)} MB\n"
        f"┖ <b>Son 30 Gün İndirilen:</b> {round(thirty_down/1024/1024,2)} MB"
    )

def traffic_detail_text():
    data = read_traffic()
    lines = []
    for date, val in sorted(data.items()):
        lines.append(f"{date} → Yüklenen: {round(val['upload']/1024/1024,2)} MB | İndirilen: {round(val['download']/1024/1024,2)} MB")
    return "⌬ <b>Son 30 Gün Detay</b>\n" + "\n".join(lines)

# ---------------- /istatistik Komutu ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    try:
        db_urls = get_db_urls()
        movies = series = storage_mb = 0
        if len(db_urls) >= 2:
            movies, series, storage_mb = get_db_stats(db_urls[1])
        cpu, ram, free_disk, free_percent, uptime = get_system_status()
        text = main_stats_text(cpu, ram, free_disk, free_percent, uptime, movies, series, storage_mb)
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("30 Gün Detay", callback_data="show_30_days")]]
        )
        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, reply_markup=keyboard)
    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("istatistik hata:", e)

# ---------------- Callback Handler ----------------
@Client.on_callback_query()
async def callback_handler(client: Client, callback_query: CallbackQuery):
    try:
        if callback_query.data == "show_30_days":
            text = traffic_detail_text()
            keyboard = InlineKeyboardMarkup(
                [[InlineKeyboardButton("Geri", callback_data="show_main")]]
            )
            await callback_query.message.edit_text(text, parse_mode=enums.ParseMode.HTML, reply_markup=keyboard)
        elif callback_query.data == "show_main":
            db_urls = get_db_urls()
            movies = series = storage_mb = 0
            if len(db_urls) >= 2:
                movies, series, storage_mb = get_db_stats(db_urls[1])
            cpu, ram, free_disk, free_percent, uptime = get_system_status()
            text = main_stats_text(cpu, ram, free_disk, free_percent, uptime, movies, series, storage_mb)
            keyboard = InlineKeyboardMarkup(
                [[InlineKeyboardButton("30 Gün Detay", callback_data="show_30_days")]]
            )
            await callback_query.message.edit_text(text, parse_mode=enums.ParseMode.HTML, reply_markup=keyboard)
    except Exception as e:
        await callback_query.message.edit_text(f"⚠️ Hata: {e}")
        print("callback hata:", e)
