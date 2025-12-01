from pyrogram import Client, filters, enums
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from psutil import virtual_memory, cpu_percent, disk_usage, net_io_counters
from pymongo import MongoClient
from time import time
from datetime import datetime, date, timedelta
import os
import importlib.util
from typing import Dict, Any, Tuple
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# !!! DIKKAT: Aşağıdaki satırın çalışması için 'Backend/helper/custom_filter.py' dosyanızın var olması gerekir.
# Eğer bu dosyaya sahip değilseniz, bu satırı yorumlayın ve 'CustomFilters.owner' yerine kendi filtre mekanizmanızı kullanın.
try:
    from Backend.helper.custom_filter import CustomFilters
except ImportError:
    class CustomFilters:
        @staticmethod
        async def owner(client, query):
            # Owner filtresi tanımlı değilse, varsayılan olarak True döndürerek test edilmesini sağlar.
            # Gerçek uygulamada buraya sahibin ID'sini kontrol eden bir mantık eklemelisiniz.
            return True 
    print("UYARI: CustomFilters import edilemedi. Lütfen elle tanımlayın.")

# ----------------- Global Ayarlar ve Değişkenler -----------------
CONFIG_PATH = "/home/debian/dfbot/config.env"
DOWNLOAD_DIR = "/"
bot_start_time = time()
CALLBACK_PREFIX = "STATS"

# --- Bellek Tabanlı Ağ İstatistikleri Depolama ---
# 1. Botun başlangıcındaki kümülatif ağ trafiği sayacı
global initial_net_io
initial_net_io = net_io_counters()

# 2. Bellekte tutulan günlük veri kullanımı sözlüğü
global data_usage
data_usage: Dict[str, Dict[str, int]] = {}


# ----------------- Yardımcı Fonksiyonlar -----------------

def format_bytes(bytes_value: int) -> str:
    """Byte değerini daha okunabilir bir formatta (GB, MB, TB) döndürür."""
    if bytes_value is None or bytes_value == 0:
        return "0 Bytes"
    
    units = ["Bytes", "KB", "MB", "GB", "TB"]
    unit_index = 0
    size = bytes_value
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
        
    return f"{size:.2f} {units[unit_index]}"

def read_database_from_config():
    """config.env dosyasından DATABASE değişkenini okur."""
    if not os.path.exists(CONFIG_PATH):
        return None
    spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    return getattr(config, "DATABASE", None)

def get_db_urls():
    """Ortam değişkenlerinden veya config dosyasından DB URL'lerini alır."""
    db_raw = read_database_from_config() or os.getenv("DATABASE") or ""
    return [u.strip() for u in db_raw.split(",") if u.strip()]

# ----------------- Database İstatistikleri (Orijinal Kod) -----------------
def get_db_stats(url: str) -> Tuple[int, int, float]:
    """MongoDB'den film/dizi sayılarını ve depolama boyutunu alır."""
    try:
        client = MongoClient(url)
        db_name_list = client.list_database_names()
        if not db_name_list:
            return 0, 0, 0.0

        db = client[db_name_list[0]]
        movies = db["movie"].count_documents({})
        series = db["tv"].count_documents({})

        stats = db.command("dbstats")
        storage_mb = round(stats.get("storageSize", 0) / (1024 * 1024), 2)

        return movies, series, storage_mb
    except Exception as e:
        print(f"MongoDB istatistik hatası: {e}")
        return 0, 0, 0.0

# ----------------- Sistem Durumu (Orijinal Kod) -----------------
def get_system_status() -> Tuple[float, float, float, float, str]:
    """CPU, RAM, Disk ve Uptime bilgilerini döndürür."""
    cpu = round(cpu_percent(interval=1), 1)
    ram = round(virtual_memory().percent, 1)

    disk = disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free / (1024 ** 3), 2)  # GB
    free_percent = round((disk.free / disk.total) * 100, 1)

    uptime_sec = int(time() - bot_start_time)
    h, r = divmod(uptime_sec, 3600)
    m, s = divmod(r, 60)
    # Sizin orijinal formatınızı korudum: {h}s{m}d{s}s
    uptime = f"{h}s{m}d{s}s" 

    return cpu, ram, free_disk, free_percent, uptime

# ----------------- Sunucu Veri Güncelleme İşlemi (Bellek) -----------------
def get_net_usage_and_update_memory():
    """Sunucudan ağ istatistiklerini alır ve bellekteki günlük kaydı günceller."""
    global initial_net_io
    global data_usage
    
    today_date_str = date.today().strftime("%Y-%m-%d")

    current_io = net_io_counters()
    session_download = current_io.bytes_recv - initial_net_io.bytes_recv
    session_upload = current_io.bytes_sent - initial_net_io.bytes_sent
    
    if today_date_str not in data_usage:
        data_usage[today_date_str] = {"download_bytes": 0, "upload_bytes": 0}

    data_usage[today_date_str]["download_bytes"] += session_download
    data_usage[today_date_str]["upload_bytes"] += session_upload
    
    initial_net_io = net_io_counters()
    
# ----------------- Aylık İstatistik Görüntüleme Fonksiyonları -----------------

async def get_usage_text_and_keyboard_memory(year: int, month: int):
    """Verilen yıl ve ay için metin ve klavye oluşturur (Bellek Verisiyle)."""
    global data_usage
    
    try:
        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = datetime(year, month + 1, 1) - timedelta(days=1)

        total_download = 0
        total_upload = 0
        daily_stats_lines = []
        current_day = start_date
        
        while current_day <= end_date:
            date_str = current_day.strftime("%Y-%m-%d")
            daily_data = data_usage.get(date_str)
            
            if daily_data:
                dl = daily_data['download_bytes']
                ul = daily_data['upload_bytes']
                total_download += dl
                total_upload += ul
                
                if dl > 0 or ul > 0:
                     daily_stats_lines.append(
                        f"  - **{current_day.day}. Gün:** 📥 {format_bytes(dl)} | 📤 {format_bytes(ul)}"
                    )
            
            current_day += timedelta(days=1)

        prev_month_date = start_date - timedelta(days=1)
        next_month_date = end_date + timedelta(days=2) 
        month_name_tr = start_date.strftime("%B").capitalize()
        
        # Metin Oluşturma
        text = (
            f"📊 <b>Aylık Sunucu Veri Kullanımı (Bellek)</b> ({month_name_tr} {year})\n"
            f"⚠️ Bot yeniden başlatılırsa bu veriler kaybolur.\n"
            f"────────────────────────\n"
            f"**📥 Toplam İndirme:** {format_bytes(total_download)}\n"
            f"**📤 Toplam Yükleme:** {format_bytes(total_upload)}\n"
            f"**🗄️ Detaylı Günlük Veriler:**\n"
        )
        text += "\n".join(daily_stats_lines) if daily_stats_lines else "  - Bu ay için veri yok."
        
        # Klavye Oluşturma
        keyboard = [
            [
                InlineKeyboardButton(
                    f"⬅️ {prev_month_date.strftime('%B').capitalize()}",
                    callback_data=f"{CALLBACK_PREFIX}_{prev_month_date.year}_{prev_month_date.month}"
                ),
                InlineKeyboardButton(
                    f"{next_month_date.strftime('%B').capitalize()} ➡️",
                    callback_data=f"{CALLBACK_PREFIX}_{next_month_date.year}_{next_month_date.month}"
                )
            ],
            [
                InlineKeyboardButton("❌ Kapat", callback_data=f"{CALLBACK_PREFIX}_CLOSE")
            ]
        ]

        return text, InlineKeyboardMarkup(keyboard)

    except Exception as e:
        print(f"İstatistik oluşturma hatası (Bellek): {e}")
        return f"⚠️ Hata: İstatistikler alınamadı. {e}", None


async def send_usage_page(client: Client, message: Message, year: int, month: int, is_new_message: bool = True):
    """İstatistik sayfasını gönderir veya düzenler (Bellek versiyonu)."""
    
    text, reply_markup = await get_usage_text_and_keyboard_memory(year, month)
    
    if is_new_message:
        # Yeni mesaj olarak gönderiyoruz, mevcut mesajı değil
        await message.reply_text(text, parse_mode=enums.ParseMode.MARKDOWN, reply_markup=reply_markup, quote=True)
    else:
        # Mevcut mesajı düzenliyoruz
        await message.edit_text(text, parse_mode=enums.ParseMode.MARKDOWN, reply_markup=reply_markup)


# ----------------- Pyrogram Komut İşleyicileri -----------------

@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def handle_statistics(client: Client, message: Message):
    """/istatistik komutu ile genel ve aylık istatistikleri gönderir."""
    # 1. Genel Sistem ve DB istatistiklerini gönder (Orijinal Kodun çıktısı)
    try:
        db_urls = get_db_urls()
        movies = series = storage_mb = 0
        if len(db_urls) >= 2:
            movies, series, storage_mb = get_db_stats(db_urls[1])

        cpu, ram, free_disk, free_percent, uptime = get_system_status()

        sys_text = (
            f"⌬ <b>İstatistik (Genel Durum)</b>\n"
            f"│\n"
            f"┠ <b>Filmler:</b> {movies}\n"
            f"┠ <b>Diziler:</b> {series}\n"
            f"┖ <b>Depolama:</b> {storage_mb} MB\n\n"
            f"┟ <b>CPU</b> → {cpu}% | <b>Boş</b> → {free_disk}GB [{free_percent}%]\n"
            f"┖ <b>RAM</b> → {ram}% | <b>Süre</b> → {uptime}"
        )
        await message.reply_text(sys_text, parse_mode=enums.ParseMode.HTML, quote=True)
        
    except Exception as e:
        await message.reply_text(f"⚠️ Genel İstatistik Hatası: {e}")
        print("istatistik genel hata:", e)

    # 2. Aylık/Günlük Veri Kullanımı menüsünü gönder (Yeni işlevsellik)
    current_date = datetime.now()
    # İlk mesajın üzerine cevap olarak gönderilir
    await send_usage_page(client, message, current_date.year, current_date.month, is_new_message=True)


@Client.on_callback_query(filters.regex(f"^{CALLBACK_PREFIX}"))
async def stats_callback_handler(client: Client, query: CallbackQuery):
    """Etkileşimli klavye (callback) tıklamalarını işler."""
    data = query.data
    
    # Kullanıcının yetkili olup olmadığını kontrol et
    if not await CustomFilters.owner(client, query):
        await query.answer("Bu komutu kullanmaya yetkiniz yok.", show_alert=True)
        return
        
    try:
        parts = data.split("_")
        action = parts[1]

        if action == "CLOSE":
            await query.message.delete()
            await query.answer("Kapatıldı.")
        
        elif action.isdigit(): 
            year = int(parts[1])
            month = int(parts[2])
            
            # Yeni sayfayı düzenle (is_new_message=False)
            await send_usage_page(client, query.message, year, month, is_new_message=False)
            await query.answer()

    except Exception as e:
        print("Callback hatası:", e)
        await query.answer(f"⚠️ Hata: İşlem gerçekleştirilemedi. {e}", show_alert=True)


# ----------------- Arka Plan Görevlisi Başlatma -----------------
def start_scheduler(client: Client):
    """Bellek güncelleme görevini başlatan fonksiyon."""
    scheduler = AsyncIOScheduler()
    
    # Veriyi her 60 dakikada bir güncelle
    scheduler.add_job(
        get_net_usage_and_update_memory, 
        'interval', 
        minutes=60, 
        id='data_recorder'
    )
    
    scheduler.start()
    print("Arka plan veri kayıt görevlisi başlatıldı (Her 60 dakikada bir güncellenir).")
