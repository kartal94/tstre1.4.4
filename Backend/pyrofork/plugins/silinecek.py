import asyncio
import os
import time
from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient

# ----------------------- GLOBAL -----------------------
stop_event = asyncio.Event()
CONFIG_PATH = "/home/debian/dfbot/config.env"

def read_database_from_config():
    if not os.path.exists(CONFIG_PATH):
        return None
    import importlib.util
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

MONGO_URL = db_urls[1]
client_db = MongoClient(MONGO_URL)
db_name = client_db.list_database_names()[0]
db = client_db[db_name]

movie_col = db["movie"]
series_col = db["tv"]

# ----------------------- /silinecek -----------------------
@Client.on_message(filters.command("silinecek") & filters.private & CustomFilters.owner)
async def silinecek(client: Client, message: Message):
    stop_event.clear()
    try:
        args = message.text.split()[1:]
        if not args:
            await message.reply_text("❗ Lütfen `tmdb` veya `imdb` ile bir ID girin.\nÖrn: `/silinecek tmdb 12345`")
            return

        key = args[0].lower()
        value = args[1] if len(args) > 1 else None

        if not value:
            await message.reply_text("❗ Geçerli bir değer girin.")
            return

        await message.reply_text(
            f"⚠️ Onay için 60 saniye içinde 'evet' yazınız.\nSilinecek: {key} → {value}"
        )

        def check(m: Message):
            return m.from_user.id == message.from_user.id and m.text.lower() == "evet"

        try:
            confirm_msg = await client.listen(message.chat.id, timeout=60, filter=check)
        except asyncio.TimeoutError:
            await message.reply_text("⏰ 60 saniye geçti, işlem iptal edildi.")
            return

        deleted_count = 0
        if key == "tmdb":
            # Film veya dizi ayrımı
            movie_result = movie_col.delete_many({"tmdb_id": int(value)})
            deleted_count += movie_result.deleted_count

            series_result = series_col.delete_many({"tmdb_id": int(value)})
            deleted_count += series_result.deleted_count

        elif key == "imdb":
            movie_result = movie_col.delete_many({"imdb_id": value})
            deleted_count += movie_result.deleted_count

            series_result = series_col.delete_many({"imdb_id": value})
            deleted_count += series_result.deleted_count

        else:
            await message.reply_text("❗ Desteklenmeyen anahtar. Sadece `tmdb` veya `imdb` kullanılabilir.")
            return

        await message.reply_text(f"✅ Silme işlemi tamamlandı. Toplam silinen kayıt: {deleted_count}")

    except Exception as e:
        await message.reply_text(f"❌ Hata: {e}")
