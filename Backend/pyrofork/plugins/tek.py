import asyncio
import os
import time
from concurrent.futures import ProcessPoolExecutor
from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
from pymongo import MongoClient, UpdateOne
from deep_translator import GoogleTranslator
import psutil

OWNER_ID = int(os.getenv("OWNER_ID", 12345))
DOWNLOAD_DIR = "/"
stop_event = asyncio.Event()
bot_start_time = time.time()

# ---------------- DATABASE ----------------
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

# ---------------- Yardımcı Fonksiyonlar ----------------
def progress_bar(current, total, bar_length=12):
    percent = (current / total) * 100 if total else 0
    filled_length = int(bar_length * current // total) if total else 0
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {min(percent,100):.2f}%"

def format_time_custom(seconds):
    if seconds < 0: seconds = 0
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h}s{m}d{s:02}s"

def is_turkish(text):
    """Basit Türkçe kontrolü (ç)"""
    return any(c in text for c in "çğıöşüÇĞİÖŞÜ")

def translate_safe(text, cache):
    if not text: return ""
    if text in cache: return cache[text]
    try:
        tr = GoogleTranslator(source='en', target='tr').translate(text)
    except: 
        tr = text
    cache[text] = tr
    return tr

def dynamic_config():
    cpu_count = psutil.cpu_count()
    ram_percent = psutil.virtual_memory().percent
    workers = max(1, min(cpu_count, 4))
    batch = 50 if ram_percent < 50 else 25 if ram_percent < 75 else 10
    return workers, batch

# ---------------- Batch Worker ----------------
def translate_batch_worker(batch_docs):
    CACHE = {}
    results = []
    for doc in batch_docs:
        _id = doc["_id"]
        upd = {}
        desc = doc.get("description", "")
        if desc and not is_turkish(desc):
            upd["description"] = translate_safe(desc, CACHE)
        seasons = doc.get("seasons", [])
        modified = False
        for season in seasons:
            for ep in season.get("episodes", []):
                if "title" in ep and ep["title"] and not is_turkish(ep["title"]):
                    ep["title"] = translate_safe(ep["title"], CACHE)
                    modified = True
                if "overview" in ep and ep["overview"] and not is_turkish(ep["overview"]):
                    ep["overview"] = translate_safe(ep["overview"], CACHE)
                    modified = True
        if modified:
            upd["seasons"] = seasons
        if upd:
            results.append((_id, upd))
    return results

# ---------------- /cevirekle ----------------
@Client.on_message(filters.command("cevirekle") & filters.private & filters.user(OWNER_ID))
async def add_cevrildi(client: Client, message: Message):
    movie_col.update_many({}, {"$set": {"cevrildi": True}})
    series_col.update_many({}, {"$set": {"cevrildi": True}})
    await message.reply_text("✅ Tüm içeriklere 'cevrildi': true eklendi.")

# ---------------- /cevirkaldir ----------------
@Client.on_message(filters.command("cevirkaldir") & filters.private & filters.user(OWNER_ID))
async def remove_cevrildi(client: Client, message: Message):
    movie_col.update_many({}, {"$unset": {"cevrildi": ""}})
    series_col.update_many({}, {"$unset": {"cevrildi": ""}})
    await message.reply_text("✅ Tüm içeriklerden 'cevrildi' kaldırıldı.")

# ---------------- /iptal ----------------
@Client.on_message(filters.command("iptal") & filters.private & filters.user(OWNER_ID))
async def iptal(client: Client, message: Message):
    stop_event.set()
    await message.reply_text("⛔ Çeviri işlemi iptal edildi.")

# ---------------- /cevir ----------------
@Client.on_message(filters.command("cevir") & filters.private & filters.user(OWNER_ID))
async def cevir(client: Client, message: Message):
    global stop_event
    if stop_event.is_set():
        await message.reply_text("⛔ Devam eden bir işlem var. Önce iptal edin.")
        return
    stop_event.clear()
    
    start_msg = await message.reply_text(
        "🇹🇷 Çeviri başlatıldı...",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("/iptal", callback_data="iptal")]])
    )

    collections = [
        {"col": movie_col, "name": "Filmler"},
        {"col": series_col, "name": "Diziler"}
    ]

    workers, batch_size = dynamic_config()
    pool = ProcessPoolExecutor(max_workers=workers)

    try:
        total_to_translate = 0
        # Çevrilecek toplam içerik sayısı
        for c in collections:
            col = c["col"]
            count = col.count_documents({"$or":[{"cevrildi":{"$exists":False}},{"cevrildi":False}]})
            if c["name"] == "Diziler":
                # Bölüm sayısını say
                total = 0
                for doc in col.find({"$or":[{"cevrildi":{"$exists":False}},{"cevrildi":False}]}, {"seasons.episodes":1}):
                    for s in doc.get("seasons", []):
                        total += len(s.get("episodes", []))
                total_to_translate += total
                c["total"] = total
            else:
                total_to_translate += count
                c["total"] = count
            c["done"] = 0
            c["errors"] = 0

        start_time = time.time()
        for c in collections:
            col = c["col"]
            name = c["name"]
            ids_cursor = col.find({"$or":[{"cevrildi":{"$exists":False}},{"cevrildi":False}]}, {"_id":1})
            ids = [d["_id"] for d in ids_cursor]
            idx = 0
            while idx < len(ids):
                if stop_event.is_set(): break
                batch_ids = ids[idx:idx+batch_size]
                batch_docs = list(col.find({"_id":{"$in":batch_ids}}))
                future = asyncio.get_event_loop().run_in_executor(pool, translate_batch_worker, batch_docs)
                results = await future
                for _id, upd in results:
                    try:
                        col.update_one({"_id":_id},{"$set":upd})
                        col.update_one({"_id":_id},{"$set":{"cevrildi":True}})
                        c["done"] += 1
                    except:
                        c["errors"] += 1
                idx += len(batch_ids)
                elapsed = time.time()-start_time
                text = f"📌 **{name}**: {c['done']}/{c['total']}\n{progress_bar(c['done'],c['total'])}\nSüre: `{format_time_custom(elapsed)}`"
                await start_msg.edit_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("/iptal", callback_data="iptal")]]))
    finally:
        pool.shutdown(wait=False)
    await start_msg.edit_text("✅ Çeviri tamamlandı.")
