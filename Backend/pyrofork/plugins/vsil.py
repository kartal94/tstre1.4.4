from pyrogram import Client, filters
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from time import time

CONFIG_PATH = "/home/debian/dfbot/config.env"
flood_wait = 5  # saniye aralık
last_command_time = {}  # kullanıcı_id : zaman

# ---------------- .env Yükleme ----------------
if os.path.exists(CONFIG_PATH):
    load_dotenv(CONFIG_PATH)

DATABASE_URLS = os.getenv("DATABASE", "")
db_urls = [u.strip() for u in DATABASE_URLS.split(",") if u.strip()]

# ---------------- /vsil Komutu ----------------
@Client.on_message(filters.command("vsil") & filters.private & CustomFilters.owner)
async def delete_file(client: Client, message: Message):
    user_id = message.from_user.id
    now = time()

    # Flood kontrolü
    if user_id in last_command_time and now - last_command_time[user_id] < flood_wait:
        await message.reply_text(f"⚠️ Lütfen {flood_wait} saniye bekleyin.", quote=True)
        return
    last_command_time[user_id] = now

    # Komut argümanı kontrolü
    if len(message.command) < 2:
        await message.reply_text("⚠️ Lütfen silinecek dosya adını yazın:\n/vsil <dosya_adı>", quote=True)
        return

    file_name = " ".join(message.command[1:])

    try:
        if not db_urls or len(db_urls) < 2:
            await message.reply_text("⚠️ İkinci veritabanı bulunamadı.")
            return

        client_db = MongoClient(db_urls[1])
        db_name_list = client_db.list_database_names()
        db = client_db[db_name_list[0]]

        # movie ve tv koleksiyonlarında arayıp sil
        deleted_count = 0
        for col_name in ["movie", "tv"]:
            collection = db[col_name]
            result = collection.delete_many({"name": file_name})
            deleted_count += result.deleted_count

        if deleted_count == 0:
            await message.reply_text(f"⚠️ '{file_name}' bulunamadı.", quote=True)
        else:
            await message.reply_text(f"✅ '{file_name}' başarıyla silindi. Toplam silinen doküman: {deleted_count}", quote=True)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}", quote=True)
        print("vsil hata:", e)
