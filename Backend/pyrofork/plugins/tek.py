import asyncio
import time
import os
import math
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pymongo import MongoClient, UpdateOne
from deep_translator import GoogleTranslator
import psutil

# ---------- Ayarlar ----------
OWNER_ID = int(os.getenv("OWNER_ID", 12345))  # Sahip ID
stop_event = asyncio.Event()

# ---------- Database Bağlantısı ----------
db_raw = os.getenv("DATABASE", "")
if not db_raw:
    raise Exception("DATABASE ortam değişkeni bulunamadı!")
db_urls = [u.strip() for u in db_raw.split(",") if u.strip()]
if len(db_urls) < 2:
    MONGO_URL = db_urls[0]
else:
    MONGO_URL = db_urls[1]

client_db = MongoClient(MONGO_URL)
db_name = client_db.list_database_names()[0]
db = client_db[db_name]
movie_col = db["movie"]
series_col = db["tv"]

# ---------- Yardımcı Fonksiyonlar ----------
def dynamic_config():
    cpu_count = multiprocessing.cpu_count()
    ram_percent = psutil.virtual_memory().percent
    workers = max(1, min(cpu_count, 4))
    batch = 50 if ram_percent < 50 else 25 if ram_percent < 75 else 10
    return workers, batch

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

def progress_bar(current, total, bar_length=12):
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    percent_display = min(percent, 100.00)
    return f"[{bar}] {percent_display:.2f}%"

def format_time_custom(total_seconds):
    if total_seconds is None or total_seconds < 0:
        return "0s0d00s"
    total_seconds = int(total_seconds)
    h, rem = divmod(total_seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}s{m}d{s:02}s"

def translate_batch_worker(batch_data):
    batch_docs = batch_data["docs"]
    stop_flag_set = batch_data["stop_flag_set"]
    if stop_flag_set:
        return []

    CACHE = {}
    results = []

    for doc in batch_docs:
        if stop_flag_set:
            break
        _id = doc.get("_id")
        upd = {}
        desc = doc.get("description")
        if desc:
            upd["description"] = translate_text_safe(desc, CACHE)
        seasons = doc.get("seasons")
        if seasons and isinstance(seasons, list):
            modified = False
            for season in seasons:
                eps = season.get("episodes", []) or []
                for ep in eps:
                    if stop_flag_set:
                        break
                    if "title" in ep and ep["title"]:
                        ep["title"] = translate_text_safe(ep["title"], CACHE)
                        modified = True
                    if "overview" in ep and ep["overview"]:
                        ep["overview"] = translate_text_safe(ep["overview"], CACHE)
                        modified = True
            if modified:
                upd["seasons"] = seasons
        results.append((_id, upd))
    return results

async def handle_stop(callback_query: CallbackQuery):
    stop_event.set()
    try:
        await callback_query.message.edit_text(
            "⛔ İşlem **iptal edildi**! Lütfen yeni bir komut başlatmadan önce bekleyin.",
            parse_mode=enums.ParseMode.MARKDOWN
        )
        await callback_query.answer("Durdurma talimatı alındı.")
    except:
        pass

# ---------- /cevir Komutu ----------
@Client.on_message(filters.command("cevir") & filters.private & filters.user(OWNER_ID))
async def turkce_icerik(client: Client, message: Message):
    global stop_event
    if stop_event.is_set():
        await message.reply_text("⛔ Devam eden işlem var, bekleyin veya iptal edin.")
        return
    stop_event.clear()

    start_msg = await message.reply_text(
        "🇹🇷 Türkçe çeviri hazırlanıyor...\nİlerleme tek mesajda gösterilecektir.",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
    )

    collections = [
        {"col": movie_col, "name": "Filmler", "total": movie_col.count_documents({}), "done":0, "errors":0},
        {"col": series_col, "name": "Diziler", "total": series_col.count_documents({}), "done":0, "errors":0}
    ]

    start_time = time.time()
    last_update = 0
    workers, batch_size = dynamic_config()
    pool = ProcessPoolExecutor(max_workers=workers)

    try:
        for c in collections:
            col = c["col"]
            total = c["total"]
            if total == 0:
                c["done"] = 0
                continue
            ids = [d["_id"] for d in col.find({}, {"_id":1})]
            idx = 0
            while idx < len(ids):
                if stop_event.is_set():
                    break
                batch_ids = ids[idx: idx + batch_size]
                batch_docs = list(col.find({"_id": {"$in": batch_ids}}))
                worker_data = {"docs": batch_docs, "stop_flag_set": stop_event.is_set()}
                loop = asyncio.get_event_loop()
                results = await loop.run_in_executor(pool, translate_batch_worker, worker_data)
                for _id, upd in results:
                    if stop_event.is_set():
                        break
                    if upd:
                        col.update_one({"_id":_id},{"$set":upd})
                    c["done"] += 1
                idx += len(batch_ids)

                # İlerleme güncellemesi
                if time.time() - last_update > 4 or idx >= len(ids):
                    text = ""
                    for col_summary in collections:
                        remaining = col_summary["total"] - col_summary["done"]
                        text += f"📌 **{col_summary['name']}**: {col_summary['done']}/{col_summary['total']}\n"
                        text += f"{progress_bar(col_summary['done'], col_summary['total'])}\n"
                        text += f"Kalan: {remaining}\n\n"
                    try:
                        await start_msg.edit_text(
                            text,
                            parse_mode=enums.ParseMode.MARKDOWN,
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
                        )
                    except:
                        pass
                    last_update = time.time()
    finally:
        pool.shutdown(wait=False)

    # Sonuç ekranı
    total_all = sum(c["total"] for c in collections)
    done_all = sum(c["done"] for c in collections)
    final_text = "🎉 **Türkçe Çeviri Sonuçları**\n\n"
    for col_summary in collections:
        final_text += f"📌 **{col_summary['name']}**: {col_summary['done']}/{col_summary['total']}\n"
        final_text += f"{progress_bar(col_summary['done'], col_summary['total'])}\n"
        final_text += f"Hatalar: `{col_summary['errors']}`\n\n"
    final_text += f"📊 **Genel Özet**\nToplam içerik: `{total_all}`\nBaşarılı: `{done_all}`\nHatalı: `0`\nKalan: `{total_all - done_all}`"
    try:
        await start_msg.edit_text(final_text, parse_mode=enums.ParseMode.MARKDOWN)
    except:
        pass

# ---------- /istatistik Komutu ----------
@Client.on_message(filters.command("istatistik") & filters.private & filters.user(OWNER_ID))
async def send_statistics(client: Client, message: Message):
    total_movies = movie_col.count_documents({})
    total_series = series_col.count_documents({})
    cpu = psutil.cpu_percent()
    ram = psutil.virtual_memory().percent
    disk = psutil.disk_usage("/")
    free_disk = round(disk.free/1024**3,2)
    free_percent = round(disk.free/disk.total*100,1)
    text = (
        f"⌬ **İstatistik**\n\n"
        f"┠ Filmler: {total_movies}\n"
        f"┠ Diziler: {total_series}\n"
        f"┖ Disk Boş: {free_disk}GB [{free_percent}%]\n"
        f"┟ CPU: {cpu}% | RAM: {ram}%"
    )
    await message.reply_text(text, parse_mode=enums.ParseMode.MARKDOWN)

# ---------- /tur Komutu ----------
@Client.on_message(filters.command("tur") & filters.private & filters.user(OWNER_ID))
async def tur_ve_platform(client: Client, message: Message):
    genre_map = {"Action":"Aksiyon","Adventure":"Macera","Drama":"Dram","Comedy":"Komedi","Horror":"Korku","Fantasy":"Fantastik"}
    platform_map = {"MAX":"Max","NF":"Netflix","DSNP":"Disney","AMZN":"Amazon"}
    total_updated = 0
    for col in [movie_col, series_col]:
        docs = col.find({})
        for doc in docs:
            genres = doc.get("genres",[])
            updated = False
            new_genres = [genre_map.get(g,g) for g in genres]
            if new_genres != genres:
                updated = True
                genres = new_genres
            for t in doc.get("telegram",[]):
                name = t.get("name","")
                for k,v in platform_map.items():
                    if k.lower() in name.lower() and v not in genres:
                        genres.append(v)
                        updated = True
            if updated:
                col.update_one({"_id":doc["_id"]},{"$set":{"genres":genres}})
                total_updated +=1
    await message.reply_text(f"✅ Tür ve platform güncellemesi tamamlandı. Toplam değiştirilen kayıt: {total_updated}")
    
# ---------- Callback Query ----------
@Client.on_callback_query()
async def callback(client: Client, query: CallbackQuery):
    if query.data=="stop":
        await handle_stop(query)
