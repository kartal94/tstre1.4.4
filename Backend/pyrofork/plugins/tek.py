import asyncio
import os
import time
import math
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict

from pyrogram import Client, filters, enums
from pyrogram.types import Message
from pymongo import MongoClient, UpdateOne
from deep_translator import GoogleTranslator
import psutil

from Backend.helper.custom_filter import CustomFilters

DOWNLOAD_DIR = "/"
bot_start_time = time.time()
translator = GoogleTranslator(source='en', target='tr')
STOP_TRANSLATION = False

# ---------------- DATABASE ----------------
db_raw = os.getenv("DATABASE", "")
if not db_raw:
    raise Exception("DATABASE ortam değişkeni bulunamadı!")

db_urls = [u.strip() for u in db_raw.split(",") if u.strip()]
if len(db_urls) < 2:
    raise Exception("İkinci DATABASE bulunamadı!")

MONGO_URL = db_urls[1]
client_db = MongoClient(MONGO_URL)
db_name = client_db.list_database_names()[0]
db = client_db[db_name]

movie_col = db["movie"]
series_col = db["tv"]

# ---------------- Dinamik Worker & Batch ----------------
def dynamic_config():
    cpu_count = multiprocessing.cpu_count()
    ram_percent = psutil.virtual_memory().percent
    cpu_percent_val = psutil.cpu_percent(interval=0.5)

    if cpu_percent_val < 30:
        workers = min(cpu_count * 2, 16)
    elif cpu_percent_val < 60:
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

def translate_batch_worker(batch):
    CACHE = {}
    results = []

    for doc in batch:
        _id = doc.get("_id")
        upd = {}
        if doc.get("cevrildi"):
            results.append((_id, None))
            continue

        desc = doc.get("description")
        if desc:
            upd["description"] = translate_text_safe(desc, CACHE)

        seasons = doc.get("seasons")
        if seasons and isinstance(seasons, list):
            modified = False
            for season in seasons:
                eps = season.get("episodes", []) or []
                for ep in eps:
                    if ep.get("cevrildi"):
                        continue
                    if "title" in ep and ep["title"]:
                        ep["title"] = translate_text_safe(ep["title"], CACHE)
                        modified = True
                    if "overview" in ep and ep["overview"]:
                        ep["overview"] = translate_text_safe(ep["overview"], CACHE)
                        modified = True
                    ep["cevrildi"] = True
            if modified:
                upd["seasons"] = seasons

        upd["cevrildi"] = True
        results.append((_id, upd))

    return results

async def process_collection_parallel(collection, name, message):
    global STOP_TRANSLATION
    loop = asyncio.get_event_loop()
    total = collection.count_documents({"cevrildi": {"$ne": True}})
    if total == 0:
        return 0, 0, 0, 0

    done = 0
    errors = 0
    start_time = time.time()
    last_update = 0

    ids_cursor = collection.find({"cevrildi": {"$ne": True}}, {"_id": 1})
    ids = [d["_id"] for d in ids_cursor]

    idx = 0
    workers, batch_size = dynamic_config()
    pool = ProcessPoolExecutor(max_workers=workers)

    while idx < len(ids):
        if STOP_TRANSLATION:
            break

        batch_ids = ids[idx: idx + batch_size]
        batch_docs = list(collection.find({"_id": {"$in": batch_ids}}))
        if not batch_docs:
            break

        try:
            future = loop.run_in_executor(pool, translate_batch_worker, batch_docs)
            results = await future
        except Exception:
            errors += len(batch_docs)
            idx += len(batch_ids)
            await asyncio.sleep(1)
            continue

        for _id, upd in results:
            try:
                if upd:
                    collection.update_one({"_id": _id}, {"$set": upd})
                done += 1
            except Exception:
                errors += 1

        idx += len(batch_ids)

        elapsed = time.time() - start_time
        speed = done / elapsed if elapsed > 0 else 0
        remaining = total - done
        eta = remaining / speed if speed > 0 else float("inf")
        eta_str = time.strftime("%H:%M:%S", time.gmtime(eta)) if math.isfinite(eta) else "∞"

        cpu = psutil.cpu_percent(interval=None)
        ram_percent = psutil.virtual_memory().percent
        sys_info = f"CPU: {cpu}% | RAM: {ram_percent}%"

        if time.time() - last_update > 10 or idx >= len(ids):
            text = (
                f"{name}: {done}/{total}\n"
                f"{progress_bar(done, total)}\n"
                f"Kalan: {remaining}, Hatalar: {errors}\n"
                f"Süre: {eta_str}\n"
                f"{sys_info}\n\n"
                f"/iptal ile durdurabilirsiniz."
            )
            try:
                await message.edit_text(text)
            except:
                pass
            last_update = time.time()

    pool.shutdown(wait=False)
    elapsed_time = round(time.time() - start_time, 2)
    return total, done, errors, elapsed_time

# ---------------- /CEVIR KOMUTU ----------------
@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def cevir_command(client: Client, message: Message):
    global STOP_TRANSLATION
    STOP_TRANSLATION = False
    start_msg = await message.reply_text("🇹🇷 Türkçe çeviri hazırlanıyor.\nİlerleme tek mesajda gösterilecektir.")

    movie_total, movie_done, movie_errors, movie_time = await process_collection_parallel(movie_col, "Filmler", start_msg)
    series_total, series_done, series_errors, series_time = await process_collection_parallel(series_col, "Diziler", start_msg)

    total_all = movie_total + series_total
    done_all = movie_done + series_done
    errors_all = movie_errors + series_errors
    remaining_all = total_all - done_all
    total_time = round(movie_time + series_time, 2)
    h, rem = divmod(total_time, 3600)
    m, s = divmod(rem, 60)
    eta_str = f"{int(h)}s {int(m)}d {int(s)}s"

    if total_all == 0:
        summary = "✅ Bütün içerikler zaten çevrilmiş."
    else:
        summary = (
            "🎉 Türkçe Çeviri Sonuçları\n\n"
            f"📌 Filmler: {movie_done}/{movie_total}\n{progress_bar(movie_done, movie_total)}\nKalan: {movie_total - movie_done}, Hatalar: {movie_errors}\n\n"
            f"📌 Diziler: {series_done}/{series_total}\n{progress_bar(series_done, series_total)}\nKalan: {series_total - series_done}, Hatalar: {series_errors}\n\n"
            f"📊 Genel Özet\nToplam içerik : {total_all}\nBaşarılı     : {done_all - errors_all}\nHatalı       : {errors_all}\nKalan        : {remaining_all}\nToplam süre  : {eta_str}"
        )

    try:
        await start_msg.edit_text(summary)
    except:
        pass

# ---------------- /IPTAL KOMUTU ----------------
@Client.on_message(filters.command("iptal") & filters.private & CustomFilters.owner)
async def iptal_command(client: Client, message: Message):
    global STOP_TRANSLATION
    STOP_TRANSLATION = True
    await message.reply_text("⏹ Çeviri işlemi iptal edildi.")

# ---------------- /CEVIR EKLE/KALDIR ----------------
@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def cevir_ekle_kaldir(client: Client, message: Message):
    if len(message.command) < 2:
        return
    action = message.command[1].lower()
    if action == "ekle":
        movie_result = movie_col.update_many({}, {"$set": {"cevrildi": True}})
        series_result = series_col.update_many({}, {"$set": {"cevrildi": True}})
        text = f"✅ Çevirildi alanı eklendi.\n📌 Filmler: {movie_result.modified_count}\n📌 Diziler: {series_result.modified_count}"
        await message.reply_text(text)
    elif action == "kaldır":
        movie_result = movie_col.update_many({}, {"$unset": {"cevrildi": ""}})
        series_result = series_col.update_many({}, {"$unset": {"cevrildi": ""}})
        text = f"✅ Çevirildi alanı kaldırıldı.\n📌 Filmler: {movie_result.modified_count}\n📌 Diziler: {series_result.modified_count}"
        await message.reply_text(text)

# ---------------- /ISTATISTIK KOMUTU ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    try:
        if not db_urls or len(db_urls) < 2:
            await message.reply_text("⚠️ İkinci veritabanı bulunamadı.")
            return

        total_movies = movie_col.count_documents({})
        total_series = series_col.count_documents({})

        stats = db.command("dbstats")
        storage_mb = round(stats.get("storageSize", 0) / (1024 * 1024), 2)
        max_storage_mb = 512
        storage_percent = round((storage_mb / max_storage_mb) * 100, 1)

        genre_stats = defaultdict(lambda: {"film": 0, "dizi": 0})
        for doc in movie_col.aggregate([{"$unwind": "$genres"}, {"$group": {"_id": "$genres", "count": {"$sum": 1}}}]):
            genre_stats[doc["_id"]]["film"] = doc["count"]
        for doc in series_col.aggregate([{"$unwind": "$genres"}, {"$group": {"_id": "$genres", "count": {"$sum": 1}}}]):
            genre_stats[doc["_id"]]["dizi"] = doc["count"]

        genre_lines = []
        for genre, counts in sorted(genre_stats.items(), key=lambda x: x[0]):
            genre_lines.append(f"{genre:<12} | Film: {counts['film']:<3} | Dizi: {counts['dizi']:<3}")
        genre_text = "\n".join(genre_lines)

        cpu = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory().percent
        disk = psutil.disk_usage(DOWNLOAD_DIR)
        free_disk = round(disk.free / (1024 ** 3), 2)
        free_percent = round((disk.free / disk.total) * 100, 1)
        uptime_sec = int(time.time() - bot_start_time)
        h, rem = divmod(uptime_sec, 3600)
        m, s = divmod(rem, 60)
        uptime = f"{h}s {m}d {s}s"

        text = (
            f"⌬ <b>İstatistik</b>\n\n"
            f"┠ Filmler: {total_movies}\n"
            f"┠ Diziler: {total_series}\n"
            f"┖ Depolama: {storage_mb} MB ({storage_percent}%)\n\n"
            f"<b>Tür Bazlı:</b>\n<pre>{genre_text}</pre>\n\n"
            f"┟ CPU → {cpu}% | Boş → {free_disk}GB [{free_percent}%]\n"
            f"┖ RAM → {ram}% | Süre → {uptime}"
        )
        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, quote=True)
    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
