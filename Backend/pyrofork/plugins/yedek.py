from pyrogram import filters, Client
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
import os
from pymongo import MongoClient

# config.py varsa DATABASE değişkenini import etmeye çalış
try:
    from Backend.config import DATABASE as CONFIG_DATABASE
except ImportError:
    CONFIG_DATABASE = None

@Client.on_message(filters.command('yedek') & filters.private & CustomFilters.owner, group=10)
async def show_db_storage(client: Client, message: Message):
    """
    /yedek komutu ile DATABASE değişkenindeki her MongoDB URL’inin
    kullandığı toplam depolama alanını yazdırır.
    Database adı URL’de yoksa ilk database otomatik seçilir.
    """
    try:
        # DATABASE URL’lerini al
        databases = CONFIG_DATABASE or os.environ.get("DATABASE") or os.environ.get("DATABASE_URL")
        if not databases:
            await message.reply_text("⚠️ MongoDB bağlantısı config/env değişkenlerinde bulunamadı.")
            return

        # Virgülle ayrılmış URL’leri listele
        mongo_urls = [url.strip() for url in databases.split(",") if url.strip()]

        if not mongo_urls:
            await message.reply_text("⚠️ Database URL bulunamadı.")
            return

        messages = []
        for i, url in enumerate(mongo_urls, 1):
            try:
                mongo_client = MongoClient(url)
                
                # URL’de default database yoksa ilk DB’yi al
                db_names = mongo_client.list_database_names()
                if not db_names:
                    messages.append(f"⚠️ Database {i} bağlantı başarılı ama database bulunamadı.")
                    continue
                db_name = db_names[0]  # ilk database
                db = mongo_client[db_name]

                db_stats = db.command("dbstats")
                used_storage_mb = db_stats.get("storageSize", 0) / (1024 * 1024)

                messages.append(f"💾 Database {i} ('{db_name}') kullanımı: {used_storage_mb:.2f} MB")

            except Exception as e:
                messages.append(f"⚠️ Database {i} bağlantı hatası: {e}")

        await message.reply_text("\n".join(messages), quote=True)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print(f"Error in /yedek handler: {e}")
