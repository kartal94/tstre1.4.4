import asyncio
import time
import re
import os
import importlib.util
import json
import math
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import psutil

# Harici bağımlılıkları yüklemeye çalış
try:
    from deep_translator import GoogleTranslator
except ImportError:
    print("⚠️ Deep Translator kütüphanesi bulunamadı. /cevir komutu çalışmayacaktır.")
    GoogleTranslator = None
    
# Pyrogram
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# Motor (Asenkron MongoDB İstemcisi)
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from dotenv import load_dotenv

# **DİKKAT:** Bu kısım sizin ortamınıza göre düzeltilmelidir.
# Eğer 'Backend.helper.custom_filter' mevcut değilse, aşağıdaki varsayılan filtreyi kullanın.
try:
    from Backend.helper.custom_filter import CustomFilters
except ImportError:
    class CustomFilters:
        @staticmethod
        def owner(flt, client):
            async def func(message):
                # Lütfen OWNER_ID'nizi buraya doğru şekilde ekleyin veya env'den okuyun
                OWNER_ID = int(os.getenv("OWNER_ID", "12345")) 
                return message.from_user.id == OWNER_ID
            return func
    
# ------------ 1. YAPILANDIRMA VE VERİTABANI BAĞLANTISI ------------

CONFIG_PATH = "/home/debian/dfbot/config.env"
DOWNLOAD_DIR = "/"
bot_start_time = time.time()
flood_wait = 30
confirmation_wait = 120

# Global Durumlar
last_command_time = {}  
pending_deletes = {}    
awaiting_confirmation = {} 
stop_event = asyncio.Event() 

# ---------------- Config/Env Okuma ----------------
if os.path.exists(CONFIG_PATH):
    load_dotenv(CONFIG_PATH)

def get_db_urls():
    """DATABASE URL'lerini config/env'den alır."""
    db_raw = os.getenv("DATABASE", "")
    return [u.strip() for u in db_raw.split(",") if u.strip()]

db_urls = get_db_urls()
MONGO_URL = db_urls[1] if len(db_urls) >= 2 else None
BASE_URL = os.getenv("BASE_URL", "")

# Asenkron MongoDB İstemcisi (Motor)
motor_client = AsyncIOMotorClient(MONGO_URL) if MONGO_URL else None
db = None
movie_col = None
series_col = None


async def init_db_collections():
    """Veritabanı bağlantısını asenkron olarak başlatır ve koleksiyonları ayarlar."""
    global db, movie_col, series_col
    
    # Motor istemcisi yoksa veya db zaten ayarlanmışsa
    if not motor_client or db is not None: 
        return True
    
    try:
        # Bağlantıyı test et ve veritabanı adını al
        db_names = await motor_client.list_database_names()
        if not db_names:
            print("Veritabanı bulunamadı.")
            return False
            
        # İlk veritabanını kullan
        db = motor_client[db_names[0]] 
        movie_col = db["movie"]
        series_col = db["tv"]
        print("MongoDB bağlantısı başarılı.")
        return True
    except Exception as e:
        print(f"MongoDB bağlantı hatası: {e}", file=sys.stderr)
        return False

# ------------ 2. YARDIMCI FONKSİYONLAR ------------

# --- Çeviri için İşlem Havuzu Fonksiyonları (Senkron) ---
# (Bu kısım, önceki yanıtta verilen ve çalışan /cevir komutunun parçasıdır)
def translate_text_safe(text, cache):
    """Deep Translator ile güvenli çeviri."""
    if not text or str(text).strip() == "" or not GoogleTranslator:
        return text
    if text in cache:
        return cache[text]
    try:
        tr = GoogleTranslator(source='en', target='tr').translate(text)
    except Exception:
        tr = text
    cache[text] = tr
    return tr

def translate_batch_worker(batch, stop_flag_value):
    """Batch çevirisi yapan işçi (Process Pool için)."""
    CACHE = {}
    results = []
    
    class StopFlagEmulator:
        def __init__(self, value):
            self._value = value
        def is_set(self):
            return self._value
            
    stop_flag = StopFlagEmulator(stop_flag_value)

    for doc in batch:
        if stop_flag.is_set():
            break

        _id = doc.get("_id")
        upd = {}
        
        # Açıklama çevirisi
        desc = doc.get("description")
        if desc and desc.strip() and desc.strip().lower() not in ["null", "none"]:
            upd["description"] = translate_text_safe(desc, CACHE)

        # Sezon / bölüm çevirisi
        seasons = doc.get("seasons")
        if seasons and isinstance(seasons, list):
            modified = False
            for season in seasons:
                eps = season.get("episodes", []) or []
                # Burada orjinal listeyi kopyalayarak ProcessPool'un değiştirmesine izin verilir.
                for ep in eps: 
                    if stop_flag.is_set():
                        break
                    # Başlık çevirisi
                    if "title" in ep and ep["title"] and ep["title"].strip() and ep["title"].strip().lower() not in ["null", "none"]:
                        ep["title"] = translate_text_safe(ep["title"], CACHE)
                        modified = True
                    # Özet çevirisi
                    if "overview" in ep and ep["overview"] and ep["overview"].strip() and ep["overview"].strip().lower() not in ["null", "none"]:
                        ep["overview"] = translate_text_safe(ep["overview"], CACHE)
                        modified = True
            if modified:
                upd["seasons"] = seasons

        results.append((_id, upd))

    return results

def progress_bar(current, total, bar_length=12):
    """İlerleme çubuğu metni."""
    if total == 0:
        return "[⬡" + "⬡"*(bar_length-1) + "] 0.00%"
    percent = (current / total) * 100
    filled_length = int(bar_length * current // total)
    bar = "⬢" * filled_length + "⬡" * (bar_length - filled_length)
    return f"[{bar}] {percent:.2f}%"

# --- Blocking Veri Çekme Fonksiyonları (asyncio.to_thread için) ---
def get_db_stats_and_genres_sync(url):
    """Senkron MongoClient kullanarak istatistik ve tür verilerini çeker."""
    from pymongo import MongoClient 
    client = MongoClient(url)
    db_name_list = client.list_database_names()
    if not db_name_list:
        client.close()
        return 0, 0, 0.0, 0.0, {}

    db_sync = client[db_name_list[0]]
    total_movies = db_sync["movie"].count_documents({})
    total_series = db_sync["tv"].count_documents({})

    stats = db_sync.command("dbstats")
    storage_mb = round(stats.get("storageSize", 0) / (1024 * 1024), 2)
    max_storage_mb = 512 
    storage_percent = round((storage_mb / max_storage_mb) * 100, 1)

    genre_stats = defaultdict(lambda: {"film": 0, "dizi": 0})
    for doc in db_sync["movie"].aggregate([{"$unwind": "$genres"}, {"$group": {"_id": "$genres", "count": {"$sum": 1}}}]):
        genre_stats[doc["_id"]]["film"] = doc["count"]

    for doc in db_sync["tv"].aggregate([{"$unwind": "$genres"}, {"$group": {"_id": "$genres", "count": {"$sum": 1}}}]):
        genre_stats[doc["_id"]]["dizi"] = doc["count"]
        
    client.close()
    return total_movies, total_series, storage_mb, storage_percent, genre_stats

def get_system_status():
    """Sistem durumunu (CPU, RAM, Disk, Uptime) çeker."""
    cpu = round(psutil.cpu_percent(interval=1), 1)
    ram = round(psutil.virtual_memory().percent, 1)

    disk = psutil.disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free / (1024 ** 3), 2)  # GB
    free_percent = round((disk.free / disk.total) * 100, 1)

    uptime_sec = int(time.time() - bot_start_time)
    h, rem = divmod(uptime_sec, 3600)
    m, s = divmod(rem, 60)
    uptime = f"{h}s {m}d {s}s"

    return cpu, ram, free_disk, free_percent, uptime

def export_collections_to_json_sync(url):
    """Senkron MongoClient ile koleksiyonları JSON'a çeker."""
    from pymongo import MongoClient
    client = MongoClient(url)
    db_name_list = client.list_database_names()
    if not db_name_list:
        client.close()
        return None

    db_sync = client[db_name_list[0]]
    movie_data = list(db_sync["movie"].find({}, {"_id": 0}))
    tv_data = list(db_sync["tv"].find({}, {"_id": 0}))
    
    client.close()
    return {"movie": movie_data, "tv": tv_data}

# ------------ 3. KOMUT HANDLER'LARI ------------

# --- /m3uindir Komutu (Çalışıyor) ---
@Client.on_message(filters.command("m3uindir") & filters.private & CustomFilters.owner)
async def send_m3u_file(client, message: Message):
    # Kodu korumak için burası atlandı, önceki yanıtta mevcut ve çalıştığı bildirildi.
    pass

# --- /istatistik Komutu (Çalışıyor) ---
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    # Kodu korumak için burası atlandı, önceki yanıtta mevcut ve çalıştığı bildirildi.
    pass

# --- /vindir Komutu (Çalışıyor) ---
@Client.on_message(filters.command("vindir") & filters.private & CustomFilters.owner)
async def download_collections(client: Client, message: Message):
    # Kodu korumak için burası atlandı, önceki yanıtta mevcut ve çalıştığı bildirildi.
    pass

# --- /sil Komutu (Çalışıyor) ---
@Client.on_message(filters.command("sil") & filters.private & CustomFilters.owner)
async def request_delete(client, message):
    # Kodu korumak için burası atlandı, önceki yanıtta mevcut ve çalıştığı bildirildi.
    pass

@Client.on_message(filters.private & CustomFilters.owner & filters.text & ~filters.command(["sil", "vsil", "tur", "cevir", "m3uindir", "vindir", "istatistik"]))
async def handle_confirmation(client, message):
    # Kodu korumak için burası atlandı, önceki yanıtta mevcut ve çalıştığı bildirildi.
    pass

# --- /tur Komutu (Çalışıyor) ---
@Client.on_message(filters.command("tur") & filters.private & CustomFilters.owner)
async def tur_ve_platform_duzelt(client: Client, message):
    # Kodu korumak için burası atlandı, önceki yanıtta mevcut ve çalıştığı bildirildi.
    pass

# --- /cevir Komutu (Çalışıyor) ---
async def process_collection_parallel(collection, name, message):
    # Kodu korumak için burası atlandı, önceki yanıtta mevcut ve çalıştığı bildirildi.
    pass

@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def turkce_icerik(client: Client, message: Message):
    # Kodu korumak için burası atlandı, önceki yanıtta mevcut ve çalıştığı bildirildi.
    pass


# --- /vsil Komutu (OPTIMİZE EDİLDİ) ---

# *find_files_to_delete* fonksiyonu sadece onay için çalıştığı ve silme işlemi yapmadığı için korunmuştur.
async def find_files_to_delete(arg):
    deleted_files = []
    
    if movie_col is None or series_col is None: return [] # Güvenlik kontrolü

    if arg.isdigit():
        tmdb_id = int(arg)
        movie_docs = [doc async for doc in movie_col.find({"tmdb_id": tmdb_id})]
        for doc in movie_docs:
            deleted_files += [t.get("name") for t in doc.get("telegram", [])]

        tv_docs = [doc async for doc in series_col.find({"tmdb_id": tmdb_id})]
        for doc in tv_docs:
            for season in doc.get("seasons", []):
                for episode in season.get("episodes", []):
                    deleted_files += [t.get("name") for t in episode.get("telegram", [])]

    elif arg.lower().startswith("tt"):
        imdb_id = arg
        movie_docs = [doc async for doc in movie_col.find({"imdb_id": imdb_id})]
        for doc in movie_docs:
            deleted_files += [t.get("name") for t in doc.get("telegram", [])]

        tv_docs = [doc async for doc in series_col.find({"imdb_id": imdb_id})]
        for doc in tv_docs:
            for season in doc.get("seasons", []):
                for episode in season.get("episodes", []):
                    deleted_files += [t.get("name") for t in episode.get("telegram", [])]

    else:
        target = arg
        
        # Filmler
        movie_docs = [doc async for doc in movie_col.find({"$or":[{"telegram.id": target},{"telegram.name": target}]})]
        for doc in movie_docs:
            telegram_list = doc.get("telegram", [])
            match = [t for t in telegram_list if t.get("id") == target or t.get("name") == target]
            deleted_files += [t.get("name") for t in match]

        # Diziler: Sadece ilgili dokümanları çekmek için `$or` kullan
        tv_docs = [doc async for doc in series_col.find({"seasons.episodes.telegram.name": target} or {"seasons.episodes.telegram.id": target})]
        for doc in tv_docs:
            for season in doc.get("seasons", []):
                for episode in season.get("episodes", []):
                    telegram_list = episode.get("telegram", [])
                    match = [t for t in telegram_list if t.get("id") == target or t.get("name") == target]
                    deleted_files += [t.get("name") for t in match]
                    
    return deleted_files

@Client.on_message(filters.command("vsil") & filters.private & CustomFilters.owner)
async def delete_file_request(client: Client, message: Message):
    user_id = message.from_user.id
    now = time.time()

    if user_id in last_command_time and now - last_command_time[user_id] < flood_wait:
        await message.reply_text(f"⚠️ Lütfen {flood_wait} saniye bekleyin.", quote=True)
        return
    last_command_time[user_id] = now

    if user_id in pending_deletes:
        await message.reply_text("⚠️ Bir silme işlemi zaten onay bekliyor. Lütfen 'evet' veya 'hayır' yazın.")
        return

    if len(message.command) < 2:
        await message.reply_text(
            "⚠️ Lütfen silinecek dosya adını, telegram ID, tmdb veya imdb ID girin:\n"
            "/vsil <telegram_id veya dosya_adı>\n"
            "/vsil <tmdb_id>\n"
            "/vsil tt<imdb_id>", quote=True)
        return

    arg = message.command[1]
    
    if not MONGO_URL or not await init_db_collections():
        await message.reply_text("⚠️ İkinci veritabanı bulunamadı veya başlatılamadı.")
        return
    
    try:
        deleted_files = await find_files_to_delete(arg)

        if not deleted_files:
            await message.reply_text("⚠️ Hiçbir eşleşme bulunamadı.", quote=True)
            return

        # --- ONAY MEKANİZMASI ---
        pending_deletes[user_id] = {
            "files": deleted_files,
            "arg": arg,
            "time": now
        }

        # Büyük dosya listelerini TXT olarak gönder
        if len(deleted_files) > 10:
            file_path = f"/tmp/silinen_dosyalar_{int(time.time())}.txt"
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("\n".join(deleted_files))
            await client.send_document(chat_id=message.chat.id, document=file_path,
                                       caption=f"⚠️ {len(deleted_files)} dosya silinecek.\nSilmek için 'evet', iptal için 'hayır' yazın. ⏳ {confirmation_wait} sn.")
        else:
            text = "\n".join(deleted_files)
            await message.reply_text(
                f"⚠️ Aşağıdaki {len(deleted_files)} dosya silinecek:\n\n"
                f"{text}\n\n"
                f"Silmek için **evet** yazın.\n"
                f"İptal için **hayır** yazın.\n"
                f"⏳ {confirmation_wait} saniye içinde cevap vermezseniz işlem iptal edilir.",
                quote=True
            )

    except Exception as e:
        print(f"/vsil isteği hatası: {e}", file=sys.stderr)
        await message.reply_text(f"⚠️ Hata: {e}", quote=True)


# --- Onay Mesajlarını Dinleme (vsil için - OPTİMİZE EDİLMİŞ BÖLÜM) ---
@Client.on_message(filters.private & CustomFilters.owner & ~filters.command(["sil", "vsil", "tur", "cevir", "m3uindir", "vindir", "istatistik"]))
async def confirm_delete_vsil(client: Client, message: Message):
    user_id = message.from_user.id
    now = time.time()

    if user_id not in pending_deletes:
        return

    data = pending_deletes[user_id]

    if now - data["time"] > confirmation_wait:
        del pending_deletes[user_id]
        await message.reply_text(f"⏳ Süre doldu, silme işlemi iptal edildi.")
        return

    text = message.text.lower()

    if text == "hayır":
        del pending_deletes[user_id]
        await message.reply_text("❌ Silme işlemi iptal edildi.")
        return

    if text != "evet":
        await message.reply_text("⚠️ Lütfen 'evet' veya 'hayır' yazın.")
        return

    arg = data["arg"]
    # Onay geldikten sonra pending_deletes'i sil
    del pending_deletes[user_id] 
    
    if db is None or movie_col is None or series_col is None:
        await message.reply_text("⚠️ Veritabanı nesnesi bulunamıyor, silme iptal edildi.")
        return

    try:
        if arg.isdigit():
            tmdb_id = int(arg)
            # TMDB ID ile direkt silme
            await movie_col.delete_many({"tmdb_id": tmdb_id})
            await series_col.delete_many({"tmdb_id": tmdb_id})

        elif arg.lower().startswith("tt"):
            imdb_id = arg
            # IMDB ID ile direkt silme
            await movie_col.delete_many({"imdb_id": imdb_id})
            await series_col.delete_many({"imdb_id": imdb_id})

        else:
            # --- Performans için optimize edilmiş kısım: Dosya Adı / Telegram ID ile Silme ---
            target = arg
            
            # --- 1. Filmler (Movies) ---
            # a) Telegram linkini çek ($pull)
            await movie_col.update_many(
                {"$or":[{"telegram.id": target},{"telegram.name": target}]},
                {"$pull": {"telegram": {"$or": [{"id": target}, {"name": target}]}}}
            )
            
            # b) Tüm telegram linkleri silinmiş olan filmi sil
            await movie_col.delete_many(
                {"telegram": {"$exists": True, "$size": 0}}
            )

            # --- 2. Diziler (Series) ---
            # MongoDB'nin `$pull` ve `$[]` operatörleri ile nested array'lerdeki öğeleri verimli silme

            # a) Telegram linkini çek ($pull). `arrayFilters` kullanmadan array içindeki tüm eşleşmeleri çeker.
            await series_col.update_many(
                {"seasons.episodes.telegram": {"$elemMatch": {"$or": [{"id": target}, {"name": target}]}}},
                {"$pull": {"seasons.$[].episodes.$[].telegram": {"$or": [{"id": target}, {"name": target}]}}}
            )
            
            # b) Telegram listesi boş kalan bölümleri sil ($pull episodes with empty telegram array)
            await series_col.update_many(
                {"seasons.episodes.telegram": {"$size": 0}},
                {"$pull": {"seasons.$[].episodes": {"telegram": {"$size": 0}}}}
            )
            
            # c) Bölüm listesi boş kalan sezonları sil ($pull seasons with empty episodes array)
            await series_col.update_many(
                {"seasons.episodes": {"$size": 0}},
                {"$pull": {"seasons": {"episodes": {"$size": 0}}}}
            )
            
            # d) Tüm sezonları silinmiş olan diziyi sil
            await series_col.delete_many(
                {"seasons": {"$exists": True, "$size": 0}}
            )
                        
        await message.reply_text("✅ Dosyalar başarıyla silindi.")

    except Exception as e:
        print(f"/vsil onay silme hatası: {e}", file=sys.stderr)
        await message.reply_text(f"⚠️ Hata: {e}")

# --- Callback Handler (Ortak) ---
@Client.on_callback_query()
async def _cb(client: Client, query: CallbackQuery):
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
