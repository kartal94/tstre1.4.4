import os
import asyncio
import json
from time import time
from collections import defaultdict
import importlib.util
from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient

CONFIG_PATH = "/home/debian/dfbot/config.env"
flood_wait = 5
confirmation_wait = 120

# ---------------- Database Setup ----------------
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

db_urls = get_db_urls()
if len(db_urls) < 2:
    raise Exception("İkinci DATABASE bulunamadı!")

# Async Mongo
MONGO_URL = db_urls[1]
client_async = AsyncIOMotorClient(MONGO_URL)
db = None
movie_col = None
series_col = None

async def init_db():
    global db, movie_col, series_col
    db_names = await client_async.list_database_names()
    db = client_async[db_names[0]]
    movie_col = db["movie"]
    series_col = db["tv"]

# ---------------- Onay Bekleyen ----------------
awaiting_confirmation = {}  # user_id -> {"type": "sil"|"vsil", "data": ..., "task": ...}
last_command_time = {}      # user_id -> zaman

# ---------------- /sil Komutu ----------------
@Client.on_message(filters.command("sil") & filters.private & CustomFilters.owner)
async def request_delete_all(client, message: Message):
    user_id = message.from_user.id
    await message.reply_text(
        "⚠️ Tüm veriler silinecek!\n"
        "Onaylamak için **evet**, iptal için **hayır** yazın.\n"
        f"⏱ {confirmation_wait} saniye içinde cevap vermezsen işlem iptal edilir."
    )
    if user_id in awaiting_confirmation:
        awaiting_confirmation[user_id]["task"].cancel()

    async def timeout():
        await asyncio.sleep(confirmation_wait)
        if user_id in awaiting_confirmation:
            awaiting_confirmation.pop(user_id, None)
            await message.reply_text("⏰ Zaman doldu, silme işlemi iptal edildi.")

    task = asyncio.create_task(timeout())
    awaiting_confirmation[user_id] = {"type": "sil", "task": task}

# ---------------- /vsil Komutu ----------------
@Client.on_message(filters.command("vsil") & filters.private & CustomFilters.owner)
async def request_delete_specific(client, message: Message):
    user_id = message.from_user.id
    now = time()
    if user_id in last_command_time and now - last_command_time[user_id] < flood_wait:
        await message.reply_text(f"⚠️ Lütfen {flood_wait} saniye bekleyin.", quote=True)
        return
    last_command_time[user_id] = now

    if len(message.command) < 2:
        await message.reply_text(
            "⚠️ Lütfen silinecek dosya adını, telegram ID, tmdb veya imdb ID girin:\n"
            "/vsil <telegram_id veya dosya_adı>\n"
            "/vsil <tmdb_id>\n"
            "/vsil tt<imdb_id>", quote=True)
        return

    arg = message.command[1]
    deleted_files = []

    try:
        client_sync = MongoClient(MONGO_URL)
        db_name = client_sync.list_database_names()[0]
        db_sync = client_sync[db_name]

        # ---- Silinecek dosyaları bul ----
        if arg.isdigit():
            tmdb_id = int(arg)
            movie_docs = list(db_sync["movie"].find({"tmdb_id": tmdb_id}))
            for doc in movie_docs:
                deleted_files += [t.get("name") for t in doc.get("telegram", [])]
            tv_docs = list(db_sync["tv"].find({"tmdb_id": tmdb_id}))
            for doc in tv_docs:
                for season in doc.get("seasons", []):
                    for ep in season.get("episodes", []):
                        deleted_files += [t.get("name") for t in ep.get("telegram", [])]

        elif arg.lower().startswith("tt"):
            imdb_id = arg
            movie_docs = list(db_sync["movie"].find({"imdb_id": imdb_id}))
            for doc in movie_docs:
                deleted_files += [t.get("name") for t in doc.get("telegram", [])]
            tv_docs = list(db_sync["tv"].find({"imdb_id": imdb_id}))
            for doc in tv_docs:
                for season in doc.get("seasons", []):
                    for ep in season.get("episodes", []):
                        deleted_files += [t.get("name") for t in ep.get("telegram", [])]

        else:  # telegram id/name
            target = arg
            movie_docs = db_sync["movie"].find({"$or":[{"telegram.id": target},{"telegram.name": target}]})
            for doc in movie_docs:
                telegram_list = doc.get("telegram", [])
                match = [t for t in telegram_list if t.get("id") == target or t.get("name") == target]
                deleted_files += [t.get("name") for t in match]

            tv_docs = db_sync["tv"].find({})
            for doc in tv_docs:
                for season in doc.get("seasons", []):
                    for ep in season.get("episodes", []):
                        telegram_list = ep.get("telegram", [])
                        match = [t for t in telegram_list if t.get("id") == target or t.get("name") == target]
                        deleted_files += [t.get("name") for t in match]

        if not deleted_files:
            await message.reply_text("⚠️ Hiçbir eşleşme bulunamadı.", quote=True)
            return

        # ---- Onay mekanizması ----
        awaiting_confirmation[user_id] = {"type": "vsil", "arg": arg, "files": deleted_files, "task": asyncio.create_task(asyncio.sleep(confirmation_wait))}
        file_list_text = "\n".join(deleted_files)
        await message.reply_text(
            f"⚠️ Aşağıdaki {len(deleted_files)} dosya silinecek:\n\n{file_list_text}\n\n"
            f"Silmek için **evet**, iptal için **hayır** yazın.\n"
            f"⏱ {confirmation_wait} saniye içinde cevap vermezsen işlem iptal edilir."
        )

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}", quote=True)
        print("vsil hata:", e)

# ---------------- /vindir Komutu ----------------
@Client.on_message(filters.command("vindir") & filters.private & CustomFilters.owner)
async def download_collections(client: Client, message: Message):
    user_id = message.from_user.id
    now = time()
    if user_id in last_command_time and now - last_command_time[user_id] < flood_wait:
        await message.reply_text(f"⚠️ Lütfen {flood_wait} saniye bekleyin.", quote=True)
        return
    last_command_time[user_id] = now

    try:
        client_sync = MongoClient(MONGO_URL)
        db_name = client_sync.list_database_names()[0]
        db_sync = client_sync[db_name]

        movie_data = list(db_sync["movie"].find({}, {"_id": 0}))
        tv_data = list(db_sync["tv"].find({}, {"_id": 0}))
        combined_data = {"movie": movie_data, "tv": tv_data}

        file_path = f"/tmp/dizi_ve_film_veritabanı.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(combined_data, f, ensure_ascii=False, indent=2, default=str)

        await client.send_document(chat_id=message.chat.id,
                                   document=file_path,
                                   caption="📁 Film ve Dizi Koleksiyonları")

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("vtindir hata:", e)

# ---------------- /istatistik Komutu ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    await init_db()
    total_movies = await movie_col.count_documents({})
    total_series = await series_col.count_documents({})

    genre_stats = defaultdict(lambda: {"film": 0, "dizi": 0})
    async for doc in movie_col.aggregate([{"$unwind": "$genres"}, {"$group": {"_id": "$genres", "count": {"$sum": 1}}}]):
        genre_stats[doc["_id"]]["film"] = doc["count"]
    async for doc in series_col.aggregate([{"$unwind": "$genres"}, {"$group": {"_id": "$genres", "count": {"$sum": 1}}}]):
        genre_stats[doc["_id"]]["dizi"] = doc["count"]

    genre_lines = [f"{g:<12} | Film: {c['film']:<3} | Dizi: {c['dizi']:<3}" for g, c in sorted(genre_stats.items())]
    genre_text = "\n".join(genre_lines)

    text = (
        f"⌬ <b>İstatistik</b>\n\n"
        f"┠ Filmler: {total_movies}\n"
        f"┠ Diziler: {total_series}\n\n"
        f"<b>Tür Bazlı:</b>\n<pre>{genre_text}</pre>"
    )
    await message.reply_text(text, parse_mode=enums.ParseMode.HTML)

# ---------------- /tur Komutu ----------------
@Client.on_message(filters.command("tur") & filters.private & CustomFilters.owner)
async def tur_ve_platform_duzelt(client: Client, message: Message):
    await message.reply_text("🔄 Tür ve platform güncellemesi başlatıldı…")
    # Mevcut bulk update kodunu buraya ekle

# ---------------- Onay Mesajları ----------------
@Client.on_message(filters.private & CustomFilters.owner)
async def handle_confirmation(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in awaiting_confirmation:
        return
    text = message.text.strip().lower()
    info = awaiting_confirmation[user_id]
    info["task"].cancel()
    awaiting_confirmation.pop(user_id, None)

    if text != "evet":
        await message.reply_text("❌ İşlem iptal edildi.")
        return

    # Silme türüne göre işlem
    await init_db()
    if info["type"] == "sil":
        movie_count = await movie_col.count_documents({})
        series_count = await series_col.count_documents({})
        await movie_col.delete_many({})
        await series_col.delete_many({})
        await message.reply_text(f"✅ Tüm veriler silindi.\n📌 Filmler: {movie_count}\n📌 Diziler: {series_count}")
    elif info["type"] == "vsil":
        arg = info["arg"]
        deleted_files = info["files"]
        client_sync = MongoClient(MONGO_URL)
        db_name = client_sync.list_database_names()[0]
        db_sync = client_sync[db_name]

        # Basit silme mantığı (tmdb, imdb veya telegram id/name)
        if arg.isdigit():
            tmdb_id = int(arg)
            await db_sync["movie"].delete_many({"tmdb_id": tmdb_id})
            await db_sync["tv"].delete_many({"tmdb_id": tmdb_id})
        elif arg.lower().startswith("tt"):
            await db_sync["movie"].delete_many({"imdb_id": arg})
            await db_sync["tv"].delete_many({"imdb_id": arg})
        else:
            # telegram id/name için basit örnek
            for col_name in ["movie", "tv"]:
                docs = db_sync[col_name].find({})
                for doc in docs:
                    telegram_list = doc.get("telegram", [])
                    new_list = [t for t in telegram_list if t.get("id") != arg and t.get("name") != arg]
                    if new_list != telegram_list:
                        doc["telegram"] = new_list
                        db_sync[col_name].replace_one({"_id": doc["_id"]}, doc)
        await message.reply_text(f"✅ Dosyalar başarıyla silindi: {len(deleted_files)}")
