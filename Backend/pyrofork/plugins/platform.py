import asyncio
import time
from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
import os
import importlib.util

# -----------------------
# stop_event ve MongoDB koleksiyonları
stop_event = asyncio.Event()

CONFIG_PATH = "/home/debian/dfbot/config.env"

def read_database_from_config():
    if not os.path.exists(CONFIG_PATH):
        return None
    spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    return getattr(config, "DATABASE", None)

def get_db_urls():
    db_raw = read_database_from_config()
    if not db_raw:
        db_raw = os.getenv("DATABASE", "")
    return [u.strip() for u in db_raw.split(",") if u.strip()]

db_urls = get_db_urls()
if len(db_urls) < 2:
    raise Exception("İkinci DATABASE bulunamadı!")

MONGO_URL = db_urls[1]
client_db = MongoClient(MONGO_URL)
db_name = client_db.list_database_names()[0]
db = client_db[db_name]

movie_col = db["movie"]
tv_col = db["tv"]
# -----------------------

PLATFORMS = {
    "dsnp": "Disney",
    "nf": "Netflix",
    "amzn": "Amazon",
    "tod": "Tod",
    "tv+": "TV+",
    "tvplus": "TV+",
    "tabii": "Tabii",
    "exxen": "Exxen",
    "gain": "Gain",

    # Max grubu
    "hbo": "Max",
    "hbomax": "Max",
    "max": "Max",
    "blutv": "Max"
}

@Client.on_message(filters.command("platform") & filters.private & CustomFilters.owner)
async def platform_duzelt(client: Client, message):
    stop_event.clear()

    start_msg = await message.reply_text(
        "🎬 Platform taraması başlıyor…",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
    )

    collections = [
        (movie_col, "Filmler"),
        (tv_col, "Diziler")
    ]

    total = 0
    fixed = 0
    last_update = 0

    # Her iki koleksiyon için döngü
    for col, label in collections:

        ids_cursor = col.find({}, {"_id": 1, "name": 1, "genres": 1})
        ids = list(ids_cursor)

        for doc in ids:
            if stop_event.is_set():
                break

            total += 1
            name = doc.get("name", "").lower()
            genres = doc.get("genres", [])
            updated = False

            for key, platform in PLATFORMS.items():
                if key in name:
                    if platform not in genres:
                        genres.append(platform)
                        updated = True
                    break

            if updated:
                col.update_one({"_id": doc["_id"]}, {"$set": {"genres": genres}})
                fixed += 1

            if time.time() - last_update > 4:
                try:
                    await start_msg.edit_text(
                        f"{label} taranıyor...\n\n"
                        f"🔍 Toplam taranan: {total}\n"
                        f"✅ Eklenen platform etiketi: {fixed}",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
                    )
                except:
                    pass
                last_update = time.time()

    # Bitiş mesajı
    try:
        await start_msg.edit_text(
            f"🎉 **Platform güncellemesi tamamlandı!**\n\n"
            f"📌 Toplam taranan: {total}\n"
            f"🏷 Eklenen platform etiketi: {fixed}",
            parse_mode=enums.ParseMode.MARKDOWN
        )
    except:
        pass
