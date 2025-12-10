import asyncio
import time
import math
import os
import importlib.util
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pymongo import MongoClient, UpdateOne
from deep_translator import GoogleTranslator
import multiprocessing
import psutil

from Backend.helper.custom_filter import CustomFilters  # Owner filtresi için

# ---------------- GLOBAL ----------------
stop_event = asyncio.Event()
awaiting_confirmation = {}  # vsil / sil onay bekleyen kullanıcılar için dict

# ---------------- DATABASE ----------------
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
series_col = db["tv"]

translator = GoogleTranslator(source='en', target='tr')

# ---------------- DYNAMIC WORKER/BATCH ----------------
def dynamic_config():
    cpu_count = multiprocessing.cpu_count()
    ram_percent = psutil.virtual_memory().percent
    cpu_percent = psutil.cpu_percent(interval=0.5)

    if cpu_percent < 30:
        workers = min(cpu_count * 2, 16)
    elif cpu_percent < 60:
        workers = max(1, cpu_count)
    else:
        workers = 1

    if ram_percent < 40:
        batch = 80
    elif ram_percent < 60:
        batch = 40
    elif ram_percent < 75:
        batch = 20
    else:
        batch = 10

    return workers, batch

# ---------------- HELPER ----------------
def translate_text_safe(text, cache):
    if not text or str(text).strip() == "":
        return ""
    if text in cache:
        return cache[text]
    try:
        tr = translator.translate(text)
    except Exception:
        tr = text
    cache[text] = tr
    return tr

def progress_bar(current, total, bar_length=12):
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {percent:.2f}%"

# ---------------- VSIL / SIL ONAY ----------------
@Client.on_message(filters.command("sil") & filters.private & CustomFilters.owner)
async def request_delete(client, message: Message):
    user_id = message.from_user.id
    await message.reply_text(
        "⚠️ Tüm veriler silinecek!\n"
        "Onaylamak için **Evet**, iptal etmek için **Hayır** yazın.\n"
        "⏱ 60 saniye içinde cevap vermezsen işlem otomatik iptal edilir."
    )

    if user_id in awaiting_confirmation:
        awaiting_confirmation[user_id].cancel()

    async def timeout():
        await asyncio.sleep(60)
        if user_id in awaiting_confirmation:
            awaiting_confirmation.pop(user_id, None)
            await message.reply_text("⏰ Zaman doldu, silme işlemi iptal edildi.")

    task = asyncio.create_task(timeout())
    awaiting_confirmation[user_id] = task

@Client.on_message(filters.private & CustomFilters.owner & filters.text)
async def handle_confirmation(client, message: Message):
    user_id = message.from_user.id
    if user_id not in awaiting_confirmation:
        return  # sadece onay bekleyenleri yakala

    text = message.text.strip().lower()
    awaiting_confirmation[user_id].cancel()
    awaiting_confirmation.pop(user_id, None)

    if text == "evet":
        await message.reply_text("🗑️ Silme işlemi başlatılıyor...")
        movie_count = movie_col.count_documents({})
        series_count = series_col.count_documents({})
        movie_col.delete_many({})
        series_col.delete_many({})
        await message.reply_text(
            f"✅ Silme tamamlandı.\nFilmler: {movie_count}\nDiziler: {series_count}"
        )
    elif text == "hayır":
        await message.reply_text("❌ Silme işlemi iptal edildi.")

# ---------------- CEVIR KOMUTU ----------------
def translate_batch_worker(batch, stop_flag):
    CACHE = {}
    results = []
    for doc in batch:
        if stop_flag.is_set():
            break
        _id = doc.get("_id")
        upd = {}

        # Açıklama çevirisi
        desc = doc.get("description")
        if desc:
            upd["description"] = translate_text_safe(desc, CACHE)

        # Sezon / bölüm çevirisi
        seasons = doc.get("seasons", [])
        for season in seasons:
            eps = season.get("episodes", []) or []
            for ep in eps:
                if stop_flag.is_set():
                    break
                if "title" in ep and ep["title"]:
                    ep["title"] = translate_text_safe(ep["title"], CACHE)
                if "overview" in ep and ep["overview"]:
                    ep["overview"] = translate_text_safe(ep["overview"], CACHE)
        if seasons:
            upd["seasons"] = seasons
        results.append((_id, upd))
    return results

async def process_collection_parallel(collection, name, message):
    total = collection.count_documents({})
    done = 0
    errors = 0
    last_update = 0
    ids = [d["_id"] for d in collection.find({}, {"_id": 1})]
    idx = 0
    workers, batch_size = dynamic_config()
    loop = asyncio.get_event_loop()
    while idx < len(ids):
        if stop_event.is_set():
            break
        batch_ids = ids[idx: idx+batch_size]
        batch_docs = list(collection.find({"_id": {"$in": batch_ids}}))
        try:
            results = await loop.run_in_executor(None, translate_batch_worker, batch_docs, stop_event)
        except:
            errors += len(batch_docs)
            idx += len(batch_ids)
            continue
        for _id, upd in results:
            if stop_event.is_set():
                break
            if upd:
                collection.update_one({"_id": _id}, {"$set": upd})
            done += 1
        idx += len(batch_ids)
        if time.time() - last_update > 10 or idx >= len(ids):
            try:
                await message.edit_text(
                    f"{name}: {done}/{total}\n{progress_bar(done, total)}",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
                )
            except:
                pass
            last_update = time.time()
    return total, done, errors

@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def turkce_icerik(client, message: Message):
    stop_event.clear()
    start_msg = await message.reply_text(
        "🇹🇷 Çeviri başlatıldı…",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
    )
    movie_total, movie_done, movie_errors = await process_collection_parallel(movie_col, "Filmler", start_msg)
    series_total, series_done, series_errors = await process_collection_parallel(series_col, "Diziler", start_msg)

    total_all = movie_total + series_total
    done_all = movie_done + series_done
    errors_all = movie_errors + series_errors
    summary = (
        f"🎉 Türkçe Çeviri Sonuçları\n\n"
        f"📌 Filmler: {movie_done}/{movie_total}\n"
        f"📌 Diziler: {series_done}/{series_total}\n"
        f"📊 Genel Toplam: {done_all-errors_all}/{total_all}, Hatalar: {errors_all}"
    )
    await start_msg.edit_text(summary)

# ---------------- STOP CALLBACK ----------------
@Client.on_callback_query()
async def handle_stop(query: CallbackQuery):
    if query.data == "stop":
        stop_event.set()
        try:
            await query.message.edit_text("⛔ İşlem iptal edildi!")
        except:
            pass
        try:
            await query.answer("Durdurma talimatı alındı.")
        except:
            pass

# ---------------- ISTATISTIK ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def istatistik(client, message: Message):
    movie_count = movie_col.count_documents({})
    series_count = series_col.count_documents({})
    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent()
    await message.reply_text(
        f"📊 İstatistik\n"
        f"Filmler: {movie_count}\n"
        f"Diziler: {series_count}\n"
        f"CPU: {cpu}% | RAM: %{mem.percent}"
    )

# ---------------- TUR / PLATFORM GUNCELLE ----------------
@Client.on_message(filters.command("tur") & filters.private & CustomFilters.owner)
async def tur_ve_platform_duzelt(client, message: Message):
    stop_event.clear()
    start_msg = await message.reply_text("🔄 Tür ve platform güncellemesi başlatıldı…", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]]))

    genre_map = {
        "Action": "Aksiyon", "Comedy": "Komedi", "Drama": "Dram",
        "Horror": "Korku", "Sci-Fi": "Bilim Kurgu", "Romance": "Romantik"
    }
    platform_map = {"Netflix": "Netflix", "Max": "Max", "Disney": "Disney"}

    collections = [(movie_col, "Filmler"), (series_col, "Diziler")]
    total_fixed = 0
    for col, name in collections:
        for doc in col.find({}):
            if stop_event.is_set():
                break
            genres = doc.get("genres", [])
            updated = False
            new_genres = [genre_map.get(g, g) for g in genres]
            if new_genres != genres:
                updated = True
                genres = new_genres

            for t in doc.get("telegram", []):
                platform_name = t.get("name", "")
                if platform_name in platform_map and platform_map[platform_name] not in genres:
                    genres.append(platform_map[platform_name])
                    updated = True

            if updated:
                col.update_one({"_id": doc["_id"]}, {"$set": {"genres": genres}})
                total_fixed += 1
            # İlerleme göstergesi
            try:
                await start_msg.edit_text(f"{name}: Güncellenen kayıtlar: {total_fixed}")
            except:
                pass
    await start_msg.edit_text(f"✅ Tür ve platform güncellemesi tamamlandı. Toplam değiştirilen kayıt: {total_fixed}")
