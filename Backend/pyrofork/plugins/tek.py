import asyncio
import time
import os
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict

from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pymongo import MongoClient, UpdateOne
from deep_translator import GoogleTranslator
import psutil

from Backend.helper.custom_filter import CustomFilters

# =====================================================
OWNER_ID = int(os.getenv("OWNER_ID", 12345))
bot_start_time = time.time()
stop_event = asyncio.Event()

# ================= DATABASE ==========================
db_raw = os.getenv("DATABASE", "")
if not db_raw:
    raise Exception("DATABASE ortam değişkeni bulunamadı!")

db_urls = [u.strip() for u in db_raw.split(",") if u.strip()]
if len(db_urls) < 2:
    raise Exception("İkinci DATABASE bulunamadı!")

MONGO_URL = db_urls[1]
mongo = MongoClient(MONGO_URL)
db = mongo[mongo.list_database_names()[0]]

movie_col = db["movie"]
series_col = db["tv"]

# ================= UTILS =============================
def dynamic_config():
    cpu = multiprocessing.cpu_count()
    ram = psutil.virtual_memory().percent
    workers = max(1, min(cpu, 4))
    batch = 50 if ram < 50 else 25 if ram < 75 else 10
    return workers, batch

def translate_safe(text, cache):
    if not text or not text.strip():
        return ""
    if text in cache:
        return cache[text]
    try:
        tr = GoogleTranslator(source="en", target="tr").translate(text)
    except:
        tr = text
    cache[text] = tr
    return tr

# ================= STOP CALLBACK (SADECE /cevir) ====
@Client.on_callback_query(filters.regex("^stop$"))
async def stop_callback(_, q: CallbackQuery):
    stop_event.set()
    await q.answer("İşlem iptal edildi")

# ================= /CEVIR ===========================
@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def cevir(client: Client, message: Message):
    if stop_event.is_set():
        await message.reply_text("⛔ Devam eden işlem var")
        return

    stop_event.clear()
    msg = await message.reply_text(
        "🇹🇷 Çeviri başlatıldı...",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]]
        )
    )

    workers, batch = dynamic_config()
    pool = ProcessPoolExecutor(max_workers=workers)
    start = time.time()

    for col in (movie_col, series_col):
        docs = list(col.find({"cevrildi": {"$ne": True}}))
        cache = {}

        for doc in docs:
            if stop_event.is_set():
                break

            upd = {}
            if doc.get("description"):
                upd["description"] = translate_safe(doc["description"], cache)

            seasons = doc.get("seasons")
            if seasons:
                for s in seasons:
                    for ep in s.get("episodes", []):
                        if ep.get("title"):
                            ep["title"] = translate_safe(ep["title"], cache)
                        if ep.get("overview"):
                            ep["overview"] = translate_safe(ep["overview"], cache)
                upd["seasons"] = seasons

            upd["cevrildi"] = True
            col.update_one({"_id": doc["_id"]}, {"$set": upd})

    await msg.edit_text("✅ Çeviri tamamlandı")

# ================= /TUR (İPTALSİZ) ==================
@Client.on_message(filters.command("tur") & filters.private & CustomFilters.owner)
async def tur_ve_platform_duzelt(client: Client, message: Message):
    start_msg = await message.reply_text("🔄 Tür ve platform güncellemesi başlatıldı…")

    genre_map = {
        "Action": "Aksiyon", "Film-Noir": "Kara Film", "Game-Show": "Oyun Gösterisi",
        "Short": "Kısa", "Sci-Fi": "Bilim Kurgu", "Sport": "Spor",
        "Adventure": "Macera", "Animation": "Animasyon",
        "Biography": "Biyografi", "Comedy": "Komedi", "Crime": "Suç",
        "Documentary": "Belgesel", "Drama": "Dram", "Family": "Aile",
        "Fantasy": "Fantastik", "History": "Tarih", "Horror": "Korku",
        "Music": "Müzik", "Mystery": "Gizem", "Romance": "Romantik",
        "Thriller": "Gerilim", "War": "Savaş", "Western": "Vahşi Batı",
        "Action & Adventure": "Aksiyon ve Macera",
        "Sci-Fi & Fantasy": "Bilim Kurgu ve Fantazi"
    }

    platform_map = {
        "NF": "Netflix", "DSNP": "Disney", "AMZN": "Amazon",
        "HBOMAX": "Max", "HBO": "Max", "BLUTV": "Max",
        "EXXEN": "Exxen", "GAIN": "Gain", "TABII": "Tabii",
        "TOD": "Tod"
    }

    total = 0
    for col in (movie_col, series_col):
        bulk = []

        for doc in col.find({}, {"genres": 1, "telegram": 1, "seasons": 1}):
            genres = doc.get("genres", []).copy()
            updated = False

            genres = [genre_map.get(g, g) for g in genres]

            for t in doc.get("telegram", []):
                name = t.get("name", "").lower()
                for k, v in platform_map.items():
                    if k.lower() in name and v not in genres:
                        genres.append(v)
                        updated = True

            for s in doc.get("seasons", []):
                for ep in s.get("episodes", []):
                    for t in ep.get("telegram", []):
                        name = t.get("name", "").lower()
                        for k, v in platform_map.items():
                            if k.lower() in name and v not in genres:
                                genres.append(v)
                                updated = True

            if updated:
                bulk.append(UpdateOne({"_id": doc["_id"]}, {"$set": {"genres": genres}}))
                total += 1

        if bulk:
            col.bulk_write(bulk)

    await start_msg.edit_text(f"✅ Tür güncellemesi tamamlandı\nToplam: {total}")

# ================= /ISTATISTIK ======================
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def istatistik(_, m: Message):
    movies = movie_col.count_documents({})
    series = series_col.count_documents({})
    await m.reply_text(
        f"📊 **İstatistik**\n\n"
        f"🎬 Filmler: `{movies}`\n"
        f"📺 Diziler: `{series}`\n"
        f"⏱ Çalışma süresi: `{int(time.time() - bot_start_time)} sn`",
        parse_mode=enums.ParseMode.MARKDOWN
    )
