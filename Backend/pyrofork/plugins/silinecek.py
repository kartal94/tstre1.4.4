from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pymongo import MongoClient
import os, importlib.util, asyncio
from Backend.helper.custom_filter import CustomFilters

CONFIG_PATH = "/home/debian/dfbot/config.env"
stop_event = asyncio.Event()

def read_database_from_config():
    if not os.path.exists(CONFIG_PATH):
        return None
    spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    return getattr(config, "DATABASE", None)

db_urls = [u.strip() for u in (read_database_from_config() or os.getenv("DATABASE", "")).split(",") if u.strip()]
MONGO_URL = db_urls[1]
client_db = MongoClient(MONGO_URL)
db = client_db[client_db.list_database_names()[0]]
movie_col = db["movie"]
series_col = db["tv"]

# ----------------- STOP CALLBACK -----------------
@Client.on_callback_query(filters.regex("stop"))
async def stop_callback(client, callback_query):
    stop_event.set()
    await callback_query.answer("İşlem iptal edildi!")

# ----------------- SILINECEK KOMUTU -----------------
@Client.on_message(filters.command("silinecek") & filters.private & CustomFilters.owner)
async def silinecek_cmd(client, message):
    args = message.text.split()
    if len(args) != 3:
        await message.reply_text("❌ Kullanım: /silinecek tmdb 69315 veya /silinecek imdb tt0991178")
        return

    cmd_type, value = args[1].lower(), args[2]
    stop_event.clear()

    confirm_msg = await message.reply_text(
        f"⚠️ {cmd_type.upper()} {value} ile eşleşen kayıtları silmek istediğinize emin misiniz?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Evet", callback_data=f"confirm|{cmd_type}|{value}")],
            [InlineKeyboardButton("❌ Hayır", callback_data="stop")]
        ])
    )

# ----------------- ONAY CALLBACK -----------------
@Client.on_callback_query(filters.regex(r"confirm\|"))
async def confirm_delete(client, callback_query):
    _, cmd_type, value = callback_query.data.split("|")
    collections = [movie_col, series_col]
    deleted_count = 0

    for col in collections:
        if cmd_type == "tmdb":
            try:
                tmdb_id = int(value)
                result = col.delete_many({"tmdb_id": tmdb_id})
                deleted_count += result.deleted_count
            except ValueError:
                await callback_query.message.edit_text("❌ Geçersiz tmdb_id!")
                return
        elif cmd_type == "imdb":
            result = col.delete_many({"imdb_id": value})
            deleted_count += result.deleted_count

        # Telegram alt öğelerini sil
        result = col.update_many(
            {},
            {"$pull": {"telegram": {"name": {"$regex": value, "$options": "i"}}}}
        )
        deleted_count += result.modified_count

        result = col.update_many(
            {},
            {"$pull": {"seasons.$[].episodes.$[].telegram": {"name": {"$regex": value, "$options": "i"}}}}
        )
        deleted_count += result.modified_count

    await callback_query.message.edit_text(f"✅ Silme işlemi tamamlandı. Toplam silinen/temizlenen kayıt: {deleted_count}")
