import asyncio
import os
import time
from concurrent.futures import ProcessPoolExecutor
from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from pymongo import MongoClient, UpdateOne
from deep_translator import GoogleTranslator
import psutil
from collections import defaultdict

OWNER_ID = int(os.getenv("OWNER_ID", 12345))
DOWNLOAD_DIR = "/"
stop_event = asyncio.Event()
bot_start_time = time.time()

# ---------------- DATABASE ----------------
db_raw = os.getenv("DATABASE", "")
if not db_raw:
    raise Exception("DATABASE ortam değişkeni bulunamadı!")

db_urls = [u.strip() for u in db_raw.split(",") if u.strip()]
MONGO_URL = db_urls[1] if len(db_urls) > 1 else db_urls[0]

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
        "🇹🇷 Çeviri başlatıldı...\nİlerleme 15 saniyede bir güncellenecek.",
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
        for c in collections:
            col = c["col"]
            docs_cursor = col.find({"$or":[{"cevrildi":{"$exists":False}},{"cevrildi":False}]})
            if c["name"] == "Diziler":
                total = sum(len(s.get("episodes", [])) for doc in docs_cursor for s in doc.get("seasons", []))
            else:
                total = docs_cursor.count()
            c["total"] = total
            c["done"] = 0
            total_to_translate += total

        start_time = time.time()
        last_update = 0

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
                        pass
                idx += len(batch_ids)

                if time.time() - last_update > 15 or idx>=len(ids):
                    elapsed = time.time() - start_time
                    total_done = sum(x["done"] for x in collections)
                    remaining = total_to_translate - total_done
                    speed = total_done/elapsed if elapsed>0 else 0
                    eta = remaining/speed if speed>0 else -1

                    text = ""
                    for col_summary in collections:
                        text += f"📌 **{col_summary['name']}**: {col_summary['done']}/{col_summary['total']}\n{progress_bar(col_summary['done'],col_summary['total'])}\n"
                    text += f"Süre: `{format_time_custom(elapsed)}` (`{format_time_custom(eta)}`)"
                    await start_msg.edit_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("/iptal", callback_data="iptal")]]))
                    last_update = time.time()
    finally:
        pool.shutdown(wait=False)
    await start_msg.edit_text("✅ Çeviri tamamlandı.")

# ---------------- /tur ----------------
@Client.on_message(filters.command("tur") & filters.private & filters.user(OWNER_ID))
async def tur_ve_platform_duzelt(client: Client, message: Message):
    start_msg = await message.reply_text("🔄 Tür ve platform güncellemesi başlatıldı…")
    
    genre_map = {
        "Action": "Aksiyon", "Film-Noir": "Kara Film", "Game-Show": "Oyun Gösterisi", "Short": "Kısa",
        "Sci-Fi": "Bilim Kurgu", "Sport": "Spor", "Adventure": "Macera", "Animation": "Animasyon",
        "Biography": "Biyografi", "Comedy": "Komedi", "Crime": "Suç", "Documentary": "Belgesel",
        "Drama": "Dram", "Family": "Aile", "News": "Haberler", "Fantasy": "Fantastik",
        "History": "Tarih", "Horror": "Korku", "Music": "Müzik", "Musical": "Müzikal",
        "Mystery": "Gizem", "Romance": "Romantik", "Science Fiction": "Bilim Kurgu",
        "TV Movie": "TV Filmi", "Thriller": "Gerilim", "War": "Savaş", "Western": "Vahşi Batı",
        "Action & Adventure": "Aksiyon ve Macera", "Kids": "Çocuklar", "Reality": "Gerçeklik",
        "Reality-TV": "Gerçeklik", "Sci-Fi & Fantasy": "Bilim Kurgu ve Fantazi", "Soap": "Pembe Dizi",
        "War & Politics": "Savaş ve Politika", "Bilim-Kurgu": "Bilim Kurgu",
        "Aksiyon & Macera": "Aksiyon ve Macera", "Savaş & Politik": "Savaş ve Politika",
        "Bilim Kurgu & Fantazi": "Bilim Kurgu ve Fantazi", "Talk": "Talk-Show"
    }

    platform_genre_map = {
        "MAX": "Max", "Hbomax": "Max", "TABİİ": "Tabii", "NF": "Netflix", "DSNP": "Disney",
        "Tod": "Tod", "Blutv": "Max", "Tv+": "Tv+", "Exxen": "Exxen",
        "Gain": "Gain", "HBO": "Max", "Tabii": "Tabii", "AMZN": "Amazon",
    }

    collections = [
        (movie_col, "Filmler"),
        (series_col, "Diziler")
    ]

    total_fixed = 0
    last_update = 0

    for col, name in collections:
        docs_cursor = col.find({}, {"_id": 1, "genres": 1, "telegram": 1, "seasons": 1})
        bulk_ops = []

        for doc in docs_cursor:
            doc_id = doc["_id"]
            genres = doc.get("genres", [])
            updated = False

            new_genres = [genre_map.get(g,g) for g in genres]
            if new_genres != genres: updated = True
            genres = new_genres

            for t in doc.get("telegram", []):
                name_field = t.get("name", "").lower()
                for key, genre_name in platform_genre_map.items():
                    if key.lower() in name_field and genre_name not in genres:
                        genres.append(genre_name)
                        updated = True

            for season in doc.get("seasons", []):
                for ep in season.get("episodes", []):
                    for t in ep.get("telegram", []):
                        name_field = t.get("name", "").lower()
                        for key, genre_name in platform_genre_map.items():
                            if key.lower() in name_field and genre_name not in genres:
                                genres.append(genre_name)
                                updated = True

            if updated:
                bulk_ops.append(UpdateOne({"_id": doc_id}, {"$set": {"genres": genres}}))
                total_fixed += 1

            if time.time() - last_update > 15:
                try:
                    await start_msg.edit_text(f"{name}: Güncellenen kayıtlar: {total_fixed}")
                    last_update = time.time()
                except:
                    pass

        if bulk_ops: col.bulk_write(bulk_ops)

    await start_msg.edit_text(f"✅ Tür ve platform güncellemesi tamamlandı.\nToplam değiştirilen kayıt: {total_fixed}")

# ---------------- /istatistik ----------------
@Client.on_message(filters.command("istatistik") & filters.private & filters.user(OWNER_ID))
async def send_statistics(client: Client, message: Message):
    try:
        total_movies = movie_col.count_documents({})
        total_series = series_col.count_documents({})

        stats = movie_col.database.command("dbstats")
        storage_mb = round(stats.get("storageSize", 0)/(1024*1024),2)

        genre_stats = defaultdict(lambda: {"film":0,"dizi":0})
        for doc in movie_col.aggregate([{"$unwind":"$genres"},{"$group":{"_id":"$genres","count":{"$sum":1}}}):
            genre_stats[doc["_id"]]["film"]=doc["count"]
        for doc in series_col.aggregate([{"$unwind":"$genres"},{"$group":{"_id":"$genres","count":{"$sum":1}}}):
            genre_stats[doc["_id"]]["dizi"]=doc["count"]

        cpu = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory().percent
        disk = psutil.disk_usage(DOWNLOAD_DIR)
        free_disk = round(disk.free/1024**3,2)
        free_percent = round(disk.free/disk.total*100,1)
        uptime_sec=int(time.time()-bot_start_time)
        h, rem = divmod(uptime_sec,3600)
        m,s = divmod(rem,60)
        uptime=f"{h}s {m}d {s}s"

        genre_lines = [f"{g:<12} | Film: {c['film']:<3} | Dizi: {c['dizi']:<3}" for g,c in sorted(genre_stats.items())]
        genre_text="\n".join(genre_lines)
        text=f"⌬ <b>İstatistik</b>\n\n┠ Filmler: {total_movies}\n┠ Diziler: {total_series}\n┖ Depolama: {storage_mb} MB\n\n<b>Tür Bazlı:</b>\n<pre>{genre_text}</pre>\n\n┟ CPU → {cpu}% | Boş → {free_disk}GB [{free_percent}%]\n┖ RAM → {ram}% | Süre → {uptime}"
        await message.reply_text(text,parse_mode=enums.ParseMode.HTML)
    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
