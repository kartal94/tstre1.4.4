from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
from psutil import virtual_memory, cpu_percent, disk_usage, disk_partitions
from time import time
import os
import importlib.util


CONFIG_PATH = "/home/debian/tstre1.4.4/config.py"
bot_start_time = time()


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


# ---------------- Docker-Aware Gerçek Disk Ölçümü ----------------
def get_real_disk_usage():
    mounts = disk_partitions(all=False)

    ignore = ("overlay", "squashfs", "tmpfs", "devtmpfs", "shm")

    real_devices = [m for m in mounts if not any(fs in m.fstype for fs in ignore)]

    if not real_devices:
        disk = disk_usage("/")
        return disk.total, disk.used, disk.free

    best = max(real_devices, key=lambda m: disk_usage(m.mountpoint).total)
    disk = disk_usage(best.mountpoint)

    return disk.total, disk.used, disk.free


# ---------------- Database İstatistikleri ----------------
def get_db_stats(url):
    client = MongoClient(url)

    db_name_list = client.list_database_names()
    if not db_name_list:
        return 0, 0, 0.0, 0.0

    db = client[db_name_list[0]]

    movies = db["movie"].count_documents({})
    series = db["tv"].count_documents({})

    stats = db.command("dbstats")
    storage_mb = round(stats.get("storageSize", 0) / (1024 * 1024), 2)

    max_storage_mb = 512
    storage_percent = round((storage_mb / max_storage_mb) * 100, 1)

    return movies, series, storage_mb, storage_percent


# ---------------- Sistem Durumu ----------------
def get_system_status():
    cpu = round(cpu_percent(interval=1), 1)
    ram = round(virtual_memory().percent, 1)

    total, used, free = get_real_disk_usage()

    free_disk = round(free / (1024 ** 3), 2)
    free_percent = round((free / total) * 100, 1)

    uptime_sec = int(time() - bot_start_time)
    h, r = divmod(uptime_sec, 3600)
    m, s = divmod(r, 60)
    uptime = f"{h}h{m}m{s}s"

    return cpu, ram, free_disk, free_percent, uptime


# ---------------- /istatistik Komutu ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    try:
        db_urls = get_db_urls()

        movies = series = storage_mb = storage_percent = 0

        if len(db_urls) >= 2:
            movies, series, storage_mb, storage_percent = get_db_stats(db_urls[1])

        cpu, ram, free_disk, free_percent, uptime = get_system_status()

        text = (
            f"⌬ <b>İstatistik</b>\n"
            f"│\n"
            f"┠ <b>Filmler:</b> {movies}\n"
            f"┠ <b>Diziler:</b> {series}\n"
            f"┖ <b>Depolama:</b> {storage_mb}MB / 512MB ({storage_percent}%)\n\n"
            f"┟ <b>CPU</b> → {cpu}% | <b>Boş Alan</b> → {free_disk}GB [{free_percent}%]\n"
            f"┖ <b>RAM</b> → {ram}% | <b>UP</b> → {uptime}"
        )

        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, quote=True)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("istatistik hata:", e)
