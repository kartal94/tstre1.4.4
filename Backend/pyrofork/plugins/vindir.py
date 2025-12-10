from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
from psutil import virtual_memory, cpu_percent, disk_usage
from time import time
import os
import importlib.util
from collections import defaultdict

CONFIG_PATH = "/home/debian/dfbot/config.py"
DOWNLOAD_DIR = "/"
bot_start_time = time()
flood_wait = 10  # saniye cinsinden flood kontrolü
last_command_time = {}  # kullanıcı_id : zaman

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

# ---------------- Database İstatistikleri ve Tür Bazlı ----------------
def get_db_stats_and_genres(url):
    client = MongoClient(url)
    db_name_list = client.list_database_names()
    if not db_name_list:
        return 0, 0, 0.0, 0.0, {}

    db = client[db_name_list[0]]

    total_movies = db["movie"].count_documents({})
    total_series = db["tv"].count_documents({})

    stats = db.command("dbstats")
    storage_mb = round(stats.get("storageSize", 0) / (1024 * 1024), 2)
    max_storage_mb = 512
    storage_percent = round((storage_mb / max_storage_mb) * 100, 1)

    genre_stats = defaultdict(lambda: {"film": 0, "dizi": 0})

    for doc in db["movie"].aggregate([{"$unwind": "$genres"}, {"$group": {"_id": "$genres", "count": {"$sum": 1}}}] ):
        genre_stats[doc["_id"]]["film"] = doc["count"]

    for doc in db["tv"].aggregate([{"$unwind": "$genres"}, {"$group": {"_id": "$genres", "count": {"$sum": 1}}}] ):
        genre_stats[doc["_id"]]["dizi"] = doc["count"]

    return total_movies, total_series, storage_mb, storage_percent, genre_stats

# ---------------- Sistem Durumu ----------------
def get_system_status():
    cpu = round(cpu_percent(interval=1), 1)
    ram = round(virtual_memory().percent, 1)

    disk = disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free / (1024 ** 3), 2)  # GB
    free_percent = round((disk.free / disk.total) * 100, 1)

    uptime_sec = int(time() - bot_start_time)
    h, rem = divmod(uptime_sec, 3600)
    m, s = divmod(rem, 60)
    uptime = f"{h}h {m}m {s}s"

    return cpu, ram, free_disk, free_percent, uptime

# ---------------- /istatistik Komutu ----------------
@Client.on_message(filters.command("vtindir") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    user_id = message.from_user.id
    now = time()

    # Flood kontrolü
    if user_id in last_command_time and now - last_command_time[user_id] < flood_wait:
        await message.reply_text(f"⚠️ Lütfen {flood_wait} saniye bekleyin.", quote=True)
        return
    last_command_time[user_id] = now

    try:
        db_urls = get_db_urls()

        if not db_urls or len(db_urls) < 2:
            await message.reply_text("⚠️ İkinci veritabanı bulunamadı.")
            return

        total_movies, total_series, storage_mb, storage_percent, genre_stats = get_db_stats_and_genres(db_urls[1])
        cpu, ram, free_disk, free_percent, uptime = get_system_status()

        # Tür istatistikleri tablo formatında
        genre_lines = []
        for genre, counts in sorted(genre_stats.items(), key=lambda x: x[0]):
            genre_lines.append(f"{genre:<12} | Film: {counts['film']:<3} | Dizi: {counts['dizi']:<3}")

        genre_text = "\n".join(genre_lines)

        text = (
            f"⌬ İstatistik\n\n"
            f"Filmler: {total_movies}\n"
            f"Diziler: {total_series}\n"
            f"Depolama: {storage_mb} MB ({storage_percent}%)\n\n"
            f"Tür Bazlı:\n{genre_text}\n\n"
            f"CPU → {cpu}% | Boş Disk → {free_disk}GB [{free_percent}%]\n"
            f"RAM → {ram}% | Süre → {uptime}"
        )

        # Txt dosyasına yaz
        file_path = "/tmp/istatistik.txt"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(text)

        # Telegram'a gönder
        await client.send_document(
            chat_id=message.chat.id,
            document=file_path,
            caption="📊 İstatistik Dosyası"
        )

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("istatistik hata:", e)
