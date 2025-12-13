import asyncio
import time
import os
from pyrogram import Client, filters, enums
from pymongo import MongoClient, UpdateOne
from collections import defaultdict
from deep_translator import GoogleTranslator
import psutil

# ---------- GLOBAL ----------
OWNER_ID = int(os.getenv("OWNER_ID", 12345))
bot_start_time = time.time()
stop_event = asyncio.Event()

# ---------- DATABASE ----------
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

# ---------- HELPER FUNCTIONS ----------
def progress_bar(current, total, bar_length=12):
    if total == 0: return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = min((current/total)*100, 100.0)
    filled = int(bar_length * current // total)
    return "[" + "⬢"*filled + "⬡"*(bar_length-filled) + f"] {percent:.2f}%"

def format_time(seconds):
    if seconds < 0: seconds = 0
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h}s{m}d{s:02}s"

def translate_text_safe(text, cache):
    if not text or str(text).strip() == "": return ""
    if text in cache: return cache[text]
    try:
        tr = GoogleTranslator(source='en', target='tr').translate(text)
    except:
        tr = text
    cache[text] = tr
    return tr

# ---------- /istatistik ----------
@Client.on_message(filters.command("istatistik") & filters.private & filters.user(OWNER_ID))
async def send_statistics(client, message):
    try:
        total_movies = movie_col.count_documents({})
        total_series = series_col.count_documents({})

        stats = db.command("dbstats")
        storage_mb = round(stats.get("storageSize",0)/(1024*1024),2)
        storage_percent = round(storage_mb/512*100,1)

        genre_stats = defaultdict(lambda: {"film":0,"dizi":0})
        for doc in movie_col.aggregate([{"$unwind":"$genres"},{"$group":{"_id":"$genres","count":{"$sum":1}}}]):
            genre_stats[doc["_id"]]["film"]=doc["count"]
        for doc in series_col.aggregate([{"$unwind":"$genres"},{"$group":{"_id":"$genres","count":{"$sum":1}}}]):
            genre_stats[doc["_id"]]["dizi"]=doc["count"]

        cpu = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory().percent
        disk = psutil.disk_usage("/")
        free_disk = round(disk.free/(1024**3),2)
        free_percent = round(disk.free/disk.total*100,1)
        uptime_sec = int(time.time()-bot_start_time)
        h,m,s = divmod(uptime_sec,3600),divmod(_,60); uptime=f"{h[0]}s {h[1]}d {s}s"

        genre_text = "\n".join([f"{k:<12} | Film: {v['film']:<3} | Dizi: {v['dizi']:<3}" for k,v in sorted(genre_stats.items())])

        text = (
            f"⌬ <b>İstatistik</b>\n\n"
            f"┠ Filmler: {total_movies}\n"
            f"┠ Diziler: {total_series}\n"
            f"┖ Depolama: {storage_mb} MB ({storage_percent}%)\n\n"
            f"<b>Tür Bazlı:</b>\n<pre>{genre_text}</pre>\n\n"
            f"┟ CPU → {cpu}% | Boş → {free_disk}GB [{free_percent}%]\n"
            f"┖ RAM → {ram}% | Süre → {uptime}"
        )
        await message.reply_text(text, parse_mode=enums.ParseMode.HTML)
    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")

# ---------- /tur ----------
@Client.on_message(filters.command("tur") & filters.private & filters.user(OWNER_ID))
async def tur_update(client, message):
    genre_map = {
        "Action":"Aksiyon","Drama":"Dram","Comedy":"Komedi",
        # diğer türler...
    }
    platform_map = {"MAX":"Max","NF":"Netflix"} # diğer platformlar
    collections = [(movie_col,"Filmler"),(series_col,"Diziler")]
    total_fixed=0
    start_msg = await message.reply_text("🔄 Tür ve platform güncellemesi başlatıldı…")
    for col,name in collections:
        bulk_ops=[]
        docs=col.find({})
        for doc in docs:
            doc_id=doc["_id"]
            genres=doc.get("genres",[])
            updated=False
            new_genres=[genre_map.get(g,g) for g in genres]
            if new_genres!=genres: updated=True; genres=new_genres
            for t in doc.get("telegram",[]):
                name_field=t.get("name","").lower()
                for k,v in platform_map.items():
                    if k.lower() in name_field and v not in genres: genres.append(v); updated=True
            if updated: bulk_ops.append(UpdateOne({"_id":doc_id},{"$set":{"genres":genres}})); total_fixed+=1
        if bulk_ops: col.bulk_write(bulk_ops)
    await start_msg.edit_text(f"✅ Tür ve platform güncellemesi tamamlandı.\nToplam değiştirilen kayıt: {total_fixed}")

# ---------- /cevir ----------
@Client.on_message(filters.command("cevir") & filters.private & filters.user(OWNER_ID))
async def cevir(client,message):
    cache={}
    start=time.time()
    start_msg=await message.reply_text("🇹🇷 Türkçe çeviri başlatıldı…")
    collections=[(movie_col,"Filmler"),(series_col,"Diziler")]
    for col,name in collections:
        docs=col.find({"cevrildi":{"$ne":True}})
        done=0
        total=docs.count()
        for doc in docs:
            upd={}
            if "description" in doc: upd["description"]=translate_text_safe(doc["description"],cache)
            # Dizi bölümleri
            if "seasons" in doc:
                modified=False
                for season in doc["seasons"]:
                    for ep in season.get("episodes",[]):
                        if "title" in ep: ep["title"]=translate_text_safe(ep["title"],cache); modified=True
                        if "overview" in ep: ep["overview"]=translate_text_safe(ep["overview"],cache); modified=True
                if modified: upd["seasons"]=doc["seasons"]
            upd["cevrildi"]=True
            col.update_one({"_id":doc["_id"]},{"$set":upd})
            done+=1
            if done%5==0: await start_msg.edit_text(f"{name}: {done}/{total}")
    elapsed=format_time(time.time()-start)
    await start_msg.edit_text(f"✅ Çeviri tamamlandı. Toplam içerik: {total}, Süre: {elapsed}")

# ---------- /cevirekle ----------
@Client.on_message(filters.command("cevirekle") & filters.private & filters.user(OWNER_ID))
async def cevirekle(client,message):
    total=0
    for col in [movie_col,series_col]:
        res=col.update_many({},{"$set":{"cevrildi":True}})
        total+=res.modified_count
    await message.reply_text(f"✅ Tüm içeriklere 'cevrildi': true eklendi. Toplam: {total}")

# ---------- /cevirkaldir ----------
@Client.on_message(filters.command("cevirkaldir") & filters.private & filters.user(OWNER_ID))
async def cevirkaldir(client,message):
    total=0
    for col in [movie_col,series_col]:
        res=col.update_many({},{"$unset":{"cevrildi":""}})
        total+=res.modified_count
    await message.reply_text(f"✅ Tüm içeriklerden 'cevrildi' kaldırıldı. Toplam: {total}")
