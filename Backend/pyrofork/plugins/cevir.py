import asyncio
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pymongo import MongoClient
from deep_translator import GoogleTranslator
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import time
import psutil
import os

# ----------------- DATABASE -----------------
db_raw = os.getenv("DATABASE", "")
db_urls = [u.strip() for u in db_raw.split(",") if u.strip()]
MONGO_URL = db_urls[1]
client_db = MongoClient(MONGO_URL)
db_name = client_db.list_database_names()[0]
db = client_db[db_name]
movie_col = db["movie"]
series_col = db["tv"]

translator = GoogleTranslator(source='en', target='tr')

# ----------------- STOP EVENT -----------------
stop_event = asyncio.Event()
mp_stop_event = multiprocessing.Event()  # multiprocessing uyumlu

# ----------------- Progress Bar -----------------
def progress_bar(current, total, bar_length=12):
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {percent:.2f}%"

# ----------------- Güvenli Çeviri -----------------
def translate_text_safe(text, cache):
    if not text: return ""
    if text in cache: return cache[text]
    try:
        tr = translator.translate(text)
    except Exception:
        tr = text
    cache[text] = tr
    return tr

# ----------------- Worker -----------------
def translate_batch_worker(batch, stop_flag):
    CACHE = {}
    results = []
    for doc in batch:
        if stop_flag.is_set():
            break
        _id = doc.get("_id")
        upd = {}

        # Film description
        desc = doc.get("description")
        if desc:
            upd["description"] = translate_text_safe(desc, CACHE)

        # Dizi seasons & episodes
        seasons = doc.get("seasons")
        if seasons and isinstance(seasons, list):
            for season in seasons:
                eps = season.get("episodes") or []
                for ep in eps:
                    if ep.get("title"):
                        ep["title"] = translate_text_safe(ep["title"], CACHE)
                    if ep.get("overview"):
                        ep["overview"] = translate_text_safe(ep["overview"], CACHE)
            upd["seasons"] = seasons  # her zaman ekle

        # Debug print
        print(f"[DEBUG] İşlenen _id: {_id}, upd keys: {list(upd.keys())}")

        results.append((_id, upd))
    return results

# ----------------- Stop Handler -----------------
async def handle_stop(callback_query):
    stop_event.set()
    mp_stop_event.set()
    try:
        await callback_query.message.edit_text("⛔ İşlem iptal edildi!")
    except: pass
    try:
        await callback_query.answer("Durdurma talimatı alındı.")
    except: pass

# ----------------- /cevir Komutu -----------------
@Client.on_message(filters.command("cevir") & filters.private)
async def turkce_icerik(client, message: Message):
    stop_event.clear()
    mp_stop_event.clear()

    start_msg = await message.reply_text(
        "🇹🇷 Türkçe çeviri hazırlanıyor...\nİlerleme tek mesajda gösterilecektir.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
    )

    collections = [
        {"col": movie_col, "name": "Filmler", "done": 0, "errors": 0},
        {"col": series_col, "name": "Diziler", "done": 0, "errors": 0}
    ]

    for c in collections:
        c["total"] = c["col"].count_documents({})

    start_time = time.time()
    last_update = 0

    for c in collections:
        col = c["col"]
        name = c["name"]
        total = c["total"]
        done = 0
        errors = 0

        ids = [d["_id"] for d in col.find({}, {"_id":1})]
        idx = 0
        pool = ProcessPoolExecutor(max_workers=4)
        batch_size = 5  # küçük test batch

        while idx < len(ids):
            if stop_event.is_set():
                break
            batch_ids = ids[idx: idx+batch_size]
            batch_docs = list(col.find({"_id":{"$in":batch_ids}}))

            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(pool, translate_batch_worker, batch_docs, mp_stop_event)

            for _id, upd in results:
                try:
                    if upd:
                        col.update_one({"_id": _id}, {"$set": upd})
                    done += 1
                except:
                    errors += 1

            idx += len(batch_ids)
            c["done"] = done
            c["errors"] = errors

            # İlerleme güncelle
            if time.time() - last_update > 2 or idx >= len(ids):
                text = ""
                for col_summary in collections:
                    text += (
                        f"📌 {col_summary['name']}: {col_summary['done']}/{col_summary['total']}\n"
                        f"{progress_bar(col_summary['done'], col_summary['total'])}\n"
                        f"Kalan: {col_summary['total'] - col_summary['done']}\n\n"
                    )
                try:
                    await start_msg.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
                    )
                except: pass
                last_update = time.time()

        pool.shutdown(wait=False)

    # Sonuç ekranı
    final_text = "🎉 Türkçe Çeviri Sonuçları\n\n"
    for col_summary in collections:
        final_text += (
            f"📌 {col_summary['name']}: {col_summary['done']}/{col_summary['total']}\n"
            f"{progress_bar(col_summary['done'], col_summary['total'])}\n"
            f"Kalan: {col_summary['total'] - col_summary['done']}, Hatalar: {col_summary['errors']}\n\n"
        )
    total_time = round(time.time() - start_time)
    final_text += f"⏱ Toplam süre: {total_time} sn"
    try:
        await start_msg.edit_text(final_text)
    except: pass

# ----------------- Callback -----------------
@Client.on_callback_query()
async def _cb(client, query):
    if query.data == "stop":
        await handle_stop(query)
