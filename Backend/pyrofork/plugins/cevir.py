import asyncio
import time
import os
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pymongo import MongoClient
from deep_translator import GoogleTranslator
import psutil

OWNER_ID = int(os.getenv("OWNER_ID", 12345))
stop_event = asyncio.Event()

# ================= DATABASE =================
MONGO_URL = os.getenv("DATABASE")
client_db = MongoClient(MONGO_URL)
db = client_db[client_db.list_database_names()[0]]
movie_col = db["movie"]
series_col = db["tv"]

# ================= DIL ALGILAMA =================
def is_turkish(text: str) -> bool:
    if not text or len(text.strip()) < 20:
        return False
    try:
        return GoogleTranslator(source="auto", target="en").detect(text) == "tr"
    except:
        return False

def translate_text_safe(text, cache):
    if not text:
        return ""
    if text in cache:
        return cache[text]
    try:
        tr = GoogleTranslator(source="en", target="tr").translate(text)
    except:
        tr = text
    cache[text] = tr
    return tr

# ================= WORKER =================
def translate_batch_worker(batch_data):
    docs = batch_data["docs"]
    CACHE = {}
    results = []

    for doc in docs:
        _id = doc["_id"]
        upd = {}
        media_type = doc.get("media_type")

        # ---------- MOVIE ----------
        if media_type == "movie":
            if doc.get("translated"):
                continue

            desc = doc.get("description")
            if desc and not is_turkish(desc):
                upd["description"] = translate_text_safe(desc, CACHE)

            upd["translated"] = True
            results.append((_id, upd))

        # ---------- TV ----------
        elif media_type == "tv":
            modified = False

            if doc.get("description") and not doc.get("description_translated"):
                if not is_turkish(doc["description"]):
                    upd["description"] = translate_text_safe(doc["description"], CACHE)
                upd["description_translated"] = True
                modified = True

            seasons = doc.get("seasons", [])
            for season in seasons:
                for ep in season.get("episodes", []):
                    if ep.get("translated"):
                        continue

                    if ep.get("title") and not is_turkish(ep["title"]):
                        ep["title"] = translate_text_safe(ep["title"], CACHE)
                    if ep.get("overview") and not is_turkish(ep["overview"]):
                        ep["overview"] = translate_text_safe(ep["overview"], CACHE)

                    ep["translated"] = True
                    modified = True

            if modified:
                upd["seasons"] = seasons
                results.append((_id, upd))

    return results

# ================= CALLBACK =================
@Client.on_callback_query()
async def stop_cb(client, query: CallbackQuery):
    if query.data == "stop":
        stop_event.set()
        await query.answer("İptal edildi")

# ================= /CEVIR =================
@Client.on_message(filters.command("cevir") & filters.private & filters.user(OWNER_ID))
async def cevir(client: Client, message: Message):
    if len(message.command) > 1:
        cmd = message.command[1].lower()

        # ======= SAYI =======
        if cmd == "sayi":
            movie_count = 0
            ep_count = 0

            for m in movie_col.find({"translated": {"$ne": True}}):
                if m.get("description") and not is_turkish(m["description"]):
                    movie_count += 1

            for s in series_col.find({}):
                for season in s.get("seasons", []):
                    for ep in season.get("episodes", []):
                        if ep.get("translated"):
                            continue
                        if (
                            (ep.get("title") and not is_turkish(ep["title"])) or
                            (ep.get("overview") and not is_turkish(ep["overview"]))
                        ):
                            ep_count += 1

            await message.reply_text(
                f"🎬 Filmler: `{movie_count}`\n📺 Bölümler: `{ep_count}`\n\n🔢 Toplam: `{movie_count + ep_count}`",
                parse_mode=enums.ParseMode.MARKDOWN
            )
            return

        # ======= EKLE =======
        if cmd == "ekle":
            movie_col.update_many({}, {"$set": {"translated": True}})
            series_col.update_many({}, {"$set": {"description_translated": True}})

            for doc in series_col.find({}):
                seasons = doc.get("seasons", [])
                for season in seasons:
                    for ep in season.get("episodes", []):
                        ep["translated"] = True
                series_col.update_one({"_id": doc["_id"]}, {"$set": {"seasons": seasons}})

            await message.reply_text("✅ Tüm içerikler çevrildi olarak işaretlendi")
            return

        # ======= KALDIR =======
        if cmd == "kaldir":
            movie_col.update_many({}, {"$unset": {"translated": ""}})
            series_col.update_many({}, {"$unset": {"description_translated": ""}})

            for doc in series_col.find({}):
                seasons = doc.get("seasons", [])
                for season in seasons:
                    for ep in season.get("episodes", []):
                        ep.pop("translated", None)
                series_col.update_one({"_id": doc["_id"]}, {"$set": {"seasons": seasons}})

            await message.reply_text("🧹 Tüm çeviri işaretleri kaldırıldı")
            return

    # ======= ÇEVIRI BAŞLAT =======
    stop_event.clear()
    msg = await message.reply_text(
        "🇹🇷 Çeviri başlatıldı",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal", callback_data="stop")]])
    )

    pool = ProcessPoolExecutor(max_workers=min(4, multiprocessing.cpu_count()))

    try:
        for col in [movie_col, series_col]:
            ids = list(col.find({}, {"_id": 1}))
            for i in range(0, len(ids), 20):
                if stop_event.is_set():
                    break

                docs = list(col.find({"_id": {"$in": ids[i:i+20]}}))
                loop = asyncio.get_event_loop()
                res = await loop.run_in_executor(pool, translate_batch_worker, {"docs": docs})

                for _id, upd in res:
                    if upd:
                        col.update_one({"_id": _id}, {"$set": upd})

    finally:
        pool.shutdown(wait=False)

    await msg.edit_text("🎉 Çeviri tamamlandı")
