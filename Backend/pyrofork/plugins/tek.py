import asyncio
import time
import math
import os
from collections import defaultdict

from pyrogram import Client, filters, enums
from pyrogram.types import Message
from pymongo import MongoClient, UpdateOne
from psutil import virtual_memory, cpu_percent, disk_usage
from deep_translator import GoogleTranslator

from Backend.helper.custom_filter import CustomFilters

DOWNLOAD_DIR = "/"
bot_start_time = time.time()
stop_event = asyncio.Event()

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

translator = GoogleTranslator(source="en", target="tr")

# ---------------- Helper Functions ----------------
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

def get_system_status():
    cpu = round(cpu_percent(interval=0.5), 1)
    ram = round(virtual_memory().percent, 1)
    disk = disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free / (1024 ** 3), 2)
    free_percent = round((disk.free / disk.total) * 100, 1)
    uptime_sec = int(time.time() - bot_start_time)
    h, rem = divmod(uptime_sec, 3600)
    m, s = divmod(rem, 60)
    uptime = f"{h}s {m}d {s}s"
    return cpu, ram, free_disk, free_percent, uptime

def get_db_stats_and_genres(url):
    client = MongoClient(url)
    db_name_list = client.list_database_names()
    if not db_name_list:
        return 0, 0, 0.0, 0.0, {}
    db = client[db_name_list[0]]
    total_movies = db["movie"].count_documents({})
    total_series = db["tv"].count_documents({})
    stats = db.command("dbstats")
    storage_mb = round(stats.get("storageSize", 0)/(1024*1024), 2)
    max_storage_mb = 512
    storage_percent = round((storage_mb/max_storage_mb)*100,1)
    genre_stats = defaultdict(lambda: {"film":0,"dizi":0})
    for doc in db["movie"].aggregate([{"$unwind":"$genres"},{"$group":{"_id":"$genres","count":{"$sum":1}}}]):
        genre_stats[doc["_id"]]["film"] = doc["count"]
    for doc in db["tv"].aggregate([{"$unwind":"$genres"},{"$group":{"_id":"$genres","count":{"$sum":1}}}]):
        genre_stats[doc["_id"]]["dizi"] = doc["count"]
    return total_movies,total_series,storage_mb,storage_percent,genre_stats

# ---------------- /tur Komutu ----------------
@Client.on_message(filters.command("tur") & filters.private & CustomFilters.owner)
async def tur_ve_platform_duzelt(client: Client, message: Message):
    start_msg = await message.reply_text("🔄 Tür ve platform güncellemesi başlatıldı…")
    
    genre_map = {
        "Action":"Aksiyon","Film-Noir":"Kara Film","Game-Show":"Oyun Gösterisi","Short":"Kısa",
        "Sci-Fi":"Bilim Kurgu","Sport":"Spor","Adventure":"Macera","Animation":"Animasyon",
        "Biography":"Biyografi","Comedy":"Komedi","Crime":"Suç","Documentary":"Belgesel",
        "Drama":"Dram","Family":"Aile","News":"Haberler","Fantasy":"Fantastik",
        "History":"Tarih","Horror":"Korku","Music":"Müzik","Musical":"Müzikal",
        "Mystery":"Gizem","Romance":"Romantik","Science Fiction":"Bilim Kurgu",
        "TV Movie":"TV Filmi","Thriller":"Gerilim","War":"Savaş","Western":"Vahşi Batı",
        "Action & Adventure":"Aksiyon ve Macera","Kids":"Çocuklar","Reality":"Gerçeklik",
        "Reality-TV":"Gerçeklik","Sci-Fi & Fantasy":"Bilim Kurgu ve Fantazi","Soap":"Pembe Dizi",
        "War & Politics":"Savaş ve Politika","Bilim-Kurgu":"Bilim Kurgu",
        "Aksiyon & Macera":"Aksiyon ve Macera","Savaş & Politik":"Savaş ve Politika",
        "Bilim Kurgu & Fantazi":"Bilim Kurgu ve Fantazi","Talk":"Talk-Show"
    }
    platform_genre_map = {
        "MAX":"Max","Hbomax":"Max","TABİİ":"Tabii","NF":"Netflix","DSNP":"Disney",
        "Tod":"Tod","Blutv":"Max","Tv+":"Tv+","Exxen":"Exxen",
        "Gain":"Gain","HBO":"Max","Tabii":"Tabii","AMZN":"Amazon"
    }
    collections = [(movie_col,"Filmler"),(series_col,"Diziler")]
    total_fixed=0
    last_update=0
    for col,name in collections:
        docs_cursor = col.find({},{"_id":1,"genres":1,"telegram":1,"seasons":1})
        bulk_ops=[]
        for doc in docs_cursor:
            doc_id = doc["_id"]
            genres = doc.get("genres",[])
            updated=False
            # Tür güncelleme
            new_genres = [genre_map.get(g,g) for g in genres]
            if new_genres != genres:
                updated=True
                genres=new_genres
            # Platformdan tür ekleme
            for t in doc.get("telegram",[]):
                name_field = t.get("name","").lower()
                for key,genre_name in platform_genre_map.items():
                    if key.lower() in name_field and genre_name not in genres:
                        genres.append(genre_name)
                        updated=True
            # Dizilerde sezon ve bölüm
            for season in doc.get("seasons",[]):
                for ep in season.get("episodes",[]):
                    for t in ep.get("telegram",[]):
                        name_field = t.get("name","").lower()
                        for key,genre_name in platform_genre_map.items():
                            if key.lower() in name_field and genre_name not in genres:
                                genres.append(genre_name)
                                updated=True
            if updated:
                bulk_ops.append(UpdateOne({"_id":doc_id},{"$set":{"genres":genres}}))
                total_fixed+=1
            if time.time()-last_update>5:
                try:
                    await start_msg.edit_text(f"{name}: Güncellenen kayıtlar: {total_fixed}")
                except: pass
                last_update=time.time()
        if bulk_ops:
            col.bulk_write(bulk_ops)
    try:
        await start_msg.edit_text(f"✅ Tür ve platform güncellemesi tamamlandı.\nToplam değiştirilen kayıt: {total_fixed}",parse_mode=enums.ParseMode.MARKDOWN)
    except: pass

# ---------------- /cevir Komutu ----------------
@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def turkce_icerik(client: Client, message: Message):
    start_msg = await message.reply_text("🇹🇷 Türkçe çeviri hazırlanıyor.\nİlerleme tek mesajda gösterilecektir.")
    CACHE={}
    total_done=0
    total_errors=0
    total_movies = movie_col.count_documents({})
    total_series = series_col.count_documents({})
    
    # ---- Filmler ----
    movie_docs = list(movie_col.find({"cevrildi":{"$ne":True}}))
    done_movies=0
    for doc in movie_docs:
        upd={}
        desc = doc.get("description")
        if desc:
            upd["description"]=translate_text_safe(desc,CACHE)
        if upd:
            upd["cevrildi"]=True
            movie_col.update_one({"_id":doc["_id"]},{"$set":upd})
            done_movies+=1
        await start_msg.edit_text(f"Filmler: {done_movies}/{len(movie_docs)}\nHatalar: {total_errors}")
    total_done+=done_movies
    
    # ---- Diziler ----
    series_docs = list(series_col.find({}))
    done_series=0
    for doc in series_docs:
        updated=False
        seasons = doc.get("seasons",[])
        for season in seasons:
            for ep in season.get("episodes",[]):
                if ep.get("cevrildi"):
                    continue
                upd={}
                if "title" in ep and ep["title"]:
                    upd["title"]=translate_text_safe(ep["title"],CACHE)
                if "overview" in ep and ep["overview"]:
                    upd["overview"]=translate_text_safe(ep["overview"],CACHE)
                if upd:
                    upd["cevrildi"]=True
                    ep.update(upd)
                    updated=True
        if updated:
            series_col.update_one({"_id":doc["_id"]},{"$set":{"seasons":seasons}})
            done_series+=1
        await start_msg.edit_text(f"Diziler: {done_series}/{len(series_docs)}\nHatalar: {total_errors}")
    total_done+=done_series
    
    total_all = total_movies+total_series
    summary=f"🎉 Türkçe Çeviri Sonuçları\n\n📌 Filmler: {done_movies}/{total_movies}\n📌 Diziler: {done_series}/{total_series}\n\n📊 Genel Özet\nToplam içerik : {total_all}\nBaşarılı     : {total_done}\nHatalı       : {total_errors}\nKalan        : {total_all-total_done}"
    await start_msg.edit_text(summary)

# ---------------- /cevir ekle ----------------
@Client.on_message(filters.command("cevir") & filters.private & filters.regex("ekle") & CustomFilters.owner)
async def cevir_ekle(client: Client, message: Message):
    movie_col.update_many({}, {"$set":{"cevrildi":True}})
    series_col.update_many({}, {"$set":{"cevrildi":True}})
    await message.reply_text("Tüm içeriklere 'cevrildi: true' alanı eklendi.")

# ---------------- /cevir kaldır ----------------
@Client.on_message(filters.command("cevir") & filters.private & filters.regex("kaldır") & CustomFilters.owner)
async def cevir_kaldir(client: Client, message: Message):
    movie_col.update_many({}, {"$unset":{"cevrildi":""}})
    series_col.update_many({}, {"$unset":{"cevrildi":""}})
    await message.reply_text("'cevrildi: true' alanı tüm içeriklerden kaldırıldı.")

# ---------------- /istatistik ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    try:
        total_movies,total_series,storage_mb,storage_percent,genre_stats=get_db_stats_and_genres(MONGO_URL)
        cpu,ram,free_disk,free_percent,uptime=get_system_status()
        genre_lines=[]
        for genre,counts in sorted(genre_stats.items(),key=lambda x:x[0]):
            genre_lines.append(f"{genre:<12} | Film: {counts['film']:<3} | Dizi: {counts['dizi']:<3}")
        genre_text="\n".join(genre_lines)
        text=(
            f"⌬ <b>İstatistik</b>\n\n"
            f"┠ Filmler: {total_movies}\n"
            f"┠ Diziler: {total_series}\n"
            f"┖ Depolama: {storage_mb} MB ({storage_percent}%)\n\n"
            f"<b>Tür Bazlı:</b>\n<pre>{genre_text}</pre>\n\n"
            f"┟ CPU → {cpu}% | Boş → {free_disk}GB [{free_percent}%]\n"
            f"┖ RAM → {ram}% | Süre → {uptime}"
        )
        await message.reply_text(text,parse_mode=enums.ParseMode.HTML,quote=True)
    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
