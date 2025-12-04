import asyncio
import time
import math
import os
import importlib.util
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pymongo import MongoClient
from deep_translator import GoogleTranslator
import psutil

from Backend.helper.custom_filter import CustomFilters

# GLOBAL STOP EVENT
stop_event = asyncio.Event()

# ----------------------- DATABASE -----------------------
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

# =======================================================
#                  DİNAMİK CPU AYARI
# =======================================================

def dynamic_config():
    cpu_count = multiprocessing.cpu_count()
    ram_percent = psutil.virtual_memory().percent
    cpu_percent = psutil.cpu_percent(interval=0.5)

    # Worker ayarı
    if cpu_percent < 30:
        workers = min(cpu_count * 2, 16)
    elif cpu_percent < 60:
        workers = max(1, cpu_count)
    else:
        workers = 1

    # Batch boyutu
    if ram_percent < 40:
        batch = 80
    elif ram_percent < 60:
        batch = 40
    elif ram_percent < 75:
        batch = 20
    else:
        batch = 10

    return workers, batch


# =======================================================
#             GÜVENLİ ÇEVİRİ FONKSİYONU
# =======================================================

def translate_text_safe(text, cache):
    if not text or str(text).strip() == "":
        return ""
    if text in cache:
        return cache[text]
    try:
        tr = GoogleTranslator(source='en', target='tr').translate(text)
    except Exception:
        tr = text
    cache[text] = tr
    return tr


# =======================================================
#                   PROGRESS BAR
# =======================================================

def progress_bar(current, total, bar_length=12):
    if total == 0:
        return "[⬡" * bar_length + "] 0%"
    percent = (current / total) * 100
    filled = int(bar_length * current // total)
    bar = "⬢" * filled + "⬡" * (bar_length - filled)
    return f"[{bar}] {percent:.2f}%"


# =======================================================
#                BATCH ÇEVİRİ WORKER
# =======================================================

def translate_batch_worker(batch, stop_flag):
    CACHE = {}
    results = []

    for doc in batch:
        if stop_flag.is_set():
            break

        _id = doc["_id"]
        upd = {}

        # Açıklama
        desc = doc.get("description")
        if desc:
            upd["description"] = translate_text_safe(desc, CACHE)

        # Sezon / bölüm
        seasons = doc.get("seasons")
        if seasons:
            modified = False
            for season in seasons:
                eps = season.get("episodes", [])
                for ep in eps:
                    if stop_flag.is_set():
                        break
                    if ep.get("title"):
                        ep["title"] = translate_text_safe(ep["title"], CACHE)
                        modified = True
                    if ep.get("overview"):
                        ep["overview"] = translate_text_safe(ep["overview"], CACHE)
                        modified = True

            if modified:
                upd["seasons"] = seasons

        results.append((_id, upd))

    return results


# =======================================================
#           PARALEL KOLEKSİYON ÇEVİRME
# =======================================================

async def process_collection_parallel(collection, name, message):
    loop = asyncio.get_event_loop()
    total = collection.count_documents({})
    done = 0
    errors = 0
    start_time = time.time()
    last_update = 0

    ids = [d["_id"] for d in collection.find({}, {"_id": 1})]
    idx = 0

    workers, batch_size = dynamic_config()
    pool = ProcessPoolExecutor(max_workers=workers)

    while idx < len(ids):
        if stop_event.is_set():
            break

        batch_ids = ids[idx: idx + batch_size]
        batch_docs = list(collection.find({"_id": {"$in": batch_ids}}))

        try:
            future = loop.run_in_executor(pool, translate_batch_worker, batch_docs, stop_event)
            results = await future
        except Exception:
            errors += len(batch_docs)
            idx += batch_size
            continue

        for _id, upd in results:
            if stop_event.is_set():
                break
            try:
                if upd:
                    collection.update_one({"_id": _id}, {"$set": upd})
                done += 1
            except:
                errors += 1

        idx += batch_size

        # İlerleme mesajı
        if time.time() - last_update > 5:
            try:
                await message.edit_text(
                    f"{name}: {done}/{total}\n"
                    f"{progress_bar(done, total)}\n\n"
                    f"Hata: {errors} | Kalan: {total - done}\n",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal", callback_data="stop")]])
                )
            except:
                pass
            last_update = time.time()

    pool.shutdown(wait=False)
    return total, done, errors, round(time.time() - start_time)


# =======================================================
#                İPTAL BUTONU CALLBACK
# =======================================================

@Client.on_callback_query()
async def _cb(client, query: CallbackQuery):
    if query.data == "stop":
        stop_event.set()
        try:
            await query.message.edit_text("⛔ İşlem iptal edildi!")
        except:
            pass
        try:
            await query.answer("Durduruldu.")
        except:
            pass


# =======================================================
#                 TEKLEŞMİŞ /cevir KOMUTU
# =======================================================

@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def full_process(client: Client, message: Message):
    global stop_event
    stop_event.clear()

    main_msg = await message.reply_text(
        "🔄 *İşlem Başladı*\n\n"
        "1) Tür düzeltme\n2) Açıklama + Bölüm çevirisi\n",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal", callback_data="stop")]])
    )

    # -----------------------------------------------------
    #                1) TÜR DÜZENLEME
    # -----------------------------------------------------
    genre_map = {
        "Action": "Aksiyon",
        "Game-Show": "Oyun Gösterisi",
        "Short": "Kısa",
        "Sci-Fi": "Bilim Kurgu",
        "Sport": "Spor",
        "Adventure": "Macera",
        "Animation": "Animasyon",
        "Biography": "Biyografi",
        "Comedy": "Komedi",
        "Crime": "Suç",
        "Documentary": "Belgesel",
        "Drama": "Dram",
        "Family": "Aile",
        "Fantasy": "Fantastik",
        "History": "Tarih",
        "Horror": "Korku",
        "Music": "Müzik",
        "Mystery": "Gizem",
        "Romance": "Romantik",
        "Science Fiction": "Bilim Kurgu",
        "TV Movie": "TV Filmi",
        "Thriller": "Gerilim",
        "War": "Savaş",
        "Western": "Vahşi Batı",
        "Action & Adventure": "Aksiyon ve Macera",
        "Kids": "Çocuklar",
        "Reality": "Gerçeklik",
        "Reality-TV": "Gerçeklik",
        "Sci-Fi & Fantasy": "Bilim Kurgu ve Fantazi",
        "Soap": "Pembe Dizi",
        "War & Politics": "Savaş ve Politika",
        "Talk": "Talk-Show"
    }

    collections = [
        (movie_col, "Filmler"),
        (series_col, "Diziler"),
    ]

    total_fixed = 0
    last_upd = 0

    for col, name in collections:
        ids = [d["_id"] for d in col.find({"genres": {"$in": list(genre_map.keys())}}, {"_id": 1})]
        idx = 0

        while idx < len(ids):
            if stop_event.is_set():
                return

            doc = col.find_one({"_id": ids[idx]})
            genres = doc.get("genres", [])
            new_genres = []
            updated = False

            for g in genres:
                if g in genre_map:
                    new_genres.append(genre_map[g])
                    updated = True
                else:
                    new_genres.append(g)

            if updated:
                col.update_one({"_id": ids[idx]}, {"$set": {"genres": new_genres}})
                total_fixed += 1

            idx += 1

            if time.time() - last_upd > 4:
                await main_msg.edit_text(
                    f"🔧 *Tür Düzenleme*\n{name}: {total_fixed} değişiklik\n",
                    parse_mode=enums.ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal", callback_data="stop")]])
                )
                last_upd = time.time()

    # -----------------------------------------------------
    #                     2) ÇEVİRİ
    # -----------------------------------------------------

    await main_msg.edit_text(
        "🇹🇷 *Tür düzenleme tamamlandı!*\nÇeviri başlıyor…",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal", callback_data="stop")]])
    )

    movie_total, movie_done, movie_errors, movie_time = await process_collection_parallel(
        movie_col, "Filmler", main_msg
    )
    if stop_event.is_set():
        return

    series_total, series_done, series_errors, series_time = await process_collection_parallel(
        series_col, "Diziler", main_msg
    )

    total_all = movie_total + series_total
    done_all = movie_done + series_done
    errors_all = movie_errors + series_errors

    total_time = movie_time + series_time
    h = int(total_time // 3600)
    m = int((total_time % 3600) // 60)
    s = int(total_time % 60)

    await main_msg.edit_text(
        "🎉 *Tamamlandı!*\n\n"
        f"🔧 Tür değişikliği: {total_fixed}\n\n"
        f"🎬 Filmler: {movie_done}/{movie_total} (Hata: {movie_errors})\n"
        f"📺 Diziler: {series_done}/{series_total} (Hata: {series_errors})\n\n"
        f"📊 Toplam: {done_all}/{total_all} • Hata: {errors_all}\n"
        f"⏳ Süre: {h}s {m}d {s}s",
        parse_mode=enums.ParseMode.MARKDOWN
    )
