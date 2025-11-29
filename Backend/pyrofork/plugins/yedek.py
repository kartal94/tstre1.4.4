from pyrogram import Client, filters, enums
from Backend.helper.custom_filter import CustomFilters
from pyrogram.types import Message
from pymongo import MongoClient
import os

def get_db_urls():
    db_raw = os.getenv("DATABASE", "")
    db_urls = [u.strip() for u in db_raw.split(",") if u.strip()]
    return db_urls

def get_db_stats(url):
    client = MongoClient(url)

    # Database adı URL içinde olmadığı için default db seçiyoruz
    db_name = client.list_database_names()[0] if client.list_database_names() else None
    if not db_name:
        return None

    db = client[db_name]

    # Koleksiyon istatistikleri
    movies_count = db["movie"].count_documents({})
    series_count = db["tv"].count_documents({})

    # Depolama bilgisi
    stats = db.command("dbstats")
    storage_mb = round(stats["storageSize"] / (1024 * 1024), 2)

    return movies_count, series_count, storage_mb


@Client.on_message(filters.command("yedek") & filters.private & CustomFilters.owner)
async def database_status(_, message: Message):
    try:
        db_urls = get_db_urls()

        if not db_urls:
            return await message.reply_text("⚠️ DATABASE ortam değişkeni bulunamadı.")

        # Sadece Database 2 istendiği için db_urls[1]
        if len(db_urls) < 2:
            return await message.reply_text("⚠️ İki adet DATABASE URL bulunamadı.")

        stats = get_db_stats(db_urls[1])

        if not stats:
            return await message.reply_text("⚠️ Database bilgisi alınamadı.")

        movies_count, series_count, storage_mb = stats

        text = (
            f"🎬 Filmler ...... {movies_count:,}\n"
            f"📺 Diziler ...... {series_count:,}\n"
            f"💾 Depolama ..... {storage_mb} MB"
        )

        await message.reply_text(text)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print(e)
