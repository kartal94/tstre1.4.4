from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
from psutil import virtual_memory, cpu_percent, disk_usage, net_io_counters
from time import time
from datetime import datetime, timedelta
import os
import json
import importlib.util

CONFIG_PATH = "/home/debian/dfbot/config.env"
DOWNLOAD_DIR = "/"
bot_start_time = time()
TRAFFIC_FILE = "/tmp/traffic_stats.json"
MAX_STORAGE_MB = 512  # MongoDB limit olarak kullanacağız

# ---------------- MongoDB Config ----------------
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

# ---------------- MongoDB Film/Dizi İstatistik ----------------
def get_db_stats(url):
    client = MongoClient(url)
    db_name_list = client.list_database_names()
    if not db_name_list:
        return 0, 0, 0.0
    db = client[db_name_list[0]]
    movies = db["movie"].count_documents({})
    series = db["tv"].count_documents({})
    stats = db.command("dbstats")
    storage_mb = round(stats.get("storageSize", 0) / (1024*1024), 2)
    return movies, series, storage_mb

# ---------------- Sistem Durumu ----------------
def get_system_status():
    cpu = round(cpu_percent(interval=0), 1)
    ram = round(virtual_memory().percent, 1)
    disk = disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free / (1024 ** 3), 2)
    free_percent = round((disk.free / disk.total) * 100, 1)

    uptime_sec = int(time() - bot_start_time)
    h, r = divmod(uptime_sec, 3600)
    m, s = divmod(r, 60)
    uptime = f"{h}s {m}d {s}s"

    return cpu, ram, free_disk, free_percent, uptime

# ---------------- Ağ Trafiği ----------------
def format_size(size):
    if size >= 1024**3:
        return f"{size/(1024**3):.2f}GB"
    elif size >= 1024**2:
        return f"{size/(1024**2):.2f}MB"
    elif size >= 1024:
        return f"{size/1024:.2f}KB"
    else:
        return f"{size}B"

def load_traffic():
    if not os.path.exists(TRAFFIC_FILE):
        data = {"daily": {}, "monthly": {}, "last": {"sent": 0, "recv": 0}}
        save_traffic(data)
        return data
    with open(TRAFFIC_FILE, "r") as f:
        try:
            return json.load(f)
        except:
            data = {"daily": {}, "monthly": {}, "last": {"sent": 0, "recv": 0}}
            save_traffic(data)
            return data

def save_traffic(data):
    with open(TRAFFIC_FILE, "w") as f:
        json.dump(data, f, indent=4)

def get_network_usage():
    counters = net_io_counters()
    return counters.bytes_sent, counters.bytes_recv

def update_traffic_stats():
    data = load_traffic()

    today = datetime.utcnow().strftime("%Y-%m-%d")
    month = datetime.utcnow().strftime("%Y-%m")

    sent, recv = get_network_usage()

    last_sent = data.get("last", {}).get("sent", sent)
    last_recv = data.get("last", {}).get("recv", recv)

    diff_sent = max(sent - last_sent, 0)
    diff_recv = max(recv - last_recv, 0)
    diff_total = diff_sent + diff_recv

    data.setdefault("daily", {})
    data["daily"][today] = data["daily"].get(today, 0) + diff_total

    data.setdefault("monthly", {})
    data["monthly"][month] = data["monthly"].get(month, 0) + diff_total

    data["last"] = {"sent": sent, "recv": recv}

    save_traffic(data)

    last_15_days = []
    for i in range(15):
        day = (datetime.utcnow() - timedelta(days=i)).strftime("%Y-%m-%d")
        if day in data["daily"] and data["daily"][day] > 0:
            last_15_days.append((day, format_size(data["daily"][day])))

    total_traffic = sent + recv

    return {
        "diff_sent": format_size(diff_sent),
        "diff_recv": format_size(diff_recv),
        "today_total": format_size(data["daily"].get(today, 0)),
        "month_total": format_size(data["monthly"].get(month, 0)),
        "total_sent": format_size(sent),
        "total_recv": format_size(recv),
        "all_time_total": format_size(total_traffic),
        "last_15_days": last_15_days
    }

# ---------------- /istatistik Komutu ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    try:
        db_urls = get_db_urls()
        movies = series = storage_mb = 0
        storage_percent = 0

        if len(db_urls) >= 2:
            movies, series, storage_mb = get_db_stats(db_urls[1])
        elif len(db_urls) == 1:
            movies, series, storage_mb = get_db_stats(db_urls[0])

        storage_percent = round((storage_mb / MAX_STORAGE_MB) * 100, 1)
        storage_text = f"{storage_mb} MB (%{storage_percent})"

        cpu, ram, free_disk, free_percent, uptime = get_system_status()
        stats = update_traffic_stats()
        last_7_text = "\n".join([f"{day}: {total}" for day, total in stats["last_15_days"]]) or "Veri yok"

        text = (
            f"⌬ <b>İstatistik</b>\n\n"
            f"┠ <b>Filmler:</b> {movies}\n"
            f"┠ <b>Diziler:</b> {series}\n"
            f"┖ <b>Depolama:</b> {storage_text}\n\n"
            f"┠ <b>Bugün:</b> {stats['today_total']}\n"
            f"┖ <b>Aylık:</b> {stats['month_total']}\n\n"
            f"┟ <b>CPU</b> → {cpu}% | <b>Boş</b> → {free_disk}GB [{free_percent}%]\n"
            f"┖ <b>RAM</b> → {ram}% | <b>Süre</b> → {uptime}\n\n"
            f"⌬ <b>Son 15 Gün:</b>\n{last_7_text}"
        )

        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, quote=True)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("istatistik hata:", e)
