from pyrogram import filters, Client
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
import os
from pymongo import MongoClient

# config.py varsa import etmeye çalış
try:
    from Backend.config import DATABASE as CONFIG_DATABASE
except ImportError:
    CONFIG_DATABASE = None

@Client.on_message(filters.command('yedek') & filters.private & CustomFilters.owner, group=10)
async def show_db_usage(client: Client, message: Message):
    """
    /yedek komutu ile virgülle ayrılmış birden fazla MongoDB database'in depolama kullanımını gösterir.
    Öncelik: config.py -> environment değişkenleri
    """
    try:
        # DATABASE URL’lerini al
        databases = CONFIG_DATABASE or os.environ.get("DATABASE") or os.environ.get("DATABASE_URL")
        if not databases:
            await message.reply_text("⚠️ MongoDB bağlantısı config dosyasında veya environment değişkenlerinde bulunamadı.")
            return

        # Virgülle ayır ve boş olanları filtrele
        mongo_urls = [url.strip() for url in databases.split(",") if url.strip()]

        messages = []
        for i, url in enumerate(mongo_urls, 1):
            try:
                mongo_client = MongoClient(url)
                db_name = mongo_client.get_default_database().name
                db_stats = mongo_client[db_name].command("dbstats")
                used_storage_mb = db_stats.get("storageSize", 0) / (1024 * 1024)  # byte -> MB
                messages.append(f"💾 Database {i} ('{db_name}') depolama kullanımı: {used_storage_mb:.2f} MB")
            except Exception as db_err:
                messages.append(f"⚠️ Database {i} bağlantı hatası: {db_err}")

        await message.reply_text("\n".join(messages), quote=True)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print(f"Error in /yedek handler: {e}")
