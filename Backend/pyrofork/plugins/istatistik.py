from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from psutil import virtual_memory, cpu_percent, disk_usage, net_io_counters
from time import time
import os
from datetime import datetime
import json

CONFIG_PATH = "/home/debian/dfbot/config.env"
DOWNLOAD_DIR = "/"
bot_start_time = time()
TRAFFIC_FILE = "/tmp/traffic_stats.json"  # Docker konteynerinde geçici trafik dosyası

# ---------------- Sistem Durumu ----------------
def get_system_status():
    cpu = round(cpu_percent(interval=0), 1)  # interval=0 anlık değer
    ram = round(virtual_memory().percent, 1)

    disk = disk_usage(DOWNLOAD_DIR)
    free_disk = round(disk.free / (1024 ** 3), 2)  # GB
    free_percent = round((disk.free / disk.total) * 100, 1)

    uptime_sec = int(time() - bot_start_time)
    h, r = divmod(uptime_sec, 3600)
    m, s = divmod(r, 60)
    uptime = f"{h} saat {m} dakika {s} saniye"

    return cpu, ram, free_disk, free_percent, uptime

# ---------------- Ağ Trafiği ----------------
def get_network_usage():
    counters = net_io_counters()
    return counters.bytes_sent, counters.bytes_recv

def format_size(size):
    tb = 1024 ** 4
    gb = 1024 ** 3
    if size >= tb:
        return f"{size / tb:.2f}TB"
    elif size >= gb:
        return f"{size / gb:.2f}GB"
    else:
        return f"{size / (1024 ** 2):.2f}MB"

# ---------------- Dosya Tabanlı Günlük/Aylık Trafik ----------------
def load_traffic_data():
    if not os.path.exists(TRAFFIC_FILE):
        return {}
    with open(TRAFFIC_FILE, "r") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {}

def save_traffic_data(data):
    with open(TRAFFIC_FILE, "w") as f:
        json.dump(data, f)

def update_traffic_stats():
    data = load_traffic_data()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    month = datetime.utcnow().strftime("%Y-%m")

    upload, download = get_network_usage()

    # Günlük veri
    data.setdefault("daily", {})
    data["daily"][today] = {"upload": upload, "download": download}

    # Aylık veri
    data.setdefault("monthly", {})
    data["monthly"][month] = {"upload": upload, "download": download}

    save_traffic_data(data)

    # Formatlı değerleri döndür
    daily_up = format_size(data["daily"][today]["upload"])
    daily_down = format_size(data["daily"][today]["download"])
    month_up = format_size(data["monthly"][month]["upload"])
    month_down = format_size(data["monthly"][month]["download"])

    return daily_up, daily_down, month_up, month_down

# ---------------- /istatistik Komutu ----------------
@Client.on_message(filters.command("istatistik") & filters.private & CustomFilters.owner)
async def send_statistics(client: Client, message: Message):
    try:
        cpu, ram, free_disk, free_percent, uptime = get_system_status()

        # Günlük / Aylık trafik istatistikleri (dosya tabanlı)
        daily_up, daily_down, month_up, month_down = update_traffic_stats()

        # Gerçek zamanlı trafik
        sent, recv = get_network_usage()
        realtime_up = format_size(sent)
        realtime_down = format_size(recv)

        text = (
            f"⌬ <b>İstatistik</b>\n"
            f"│\n"
            f"┟ <b>Upload:</b> {realtime_up}\n"
            f"┠ <b>Download:</b> {realtime_down}\n"
            f"┠ <b>Bugün İndirilen:</b> {daily_down}\n"
            f"┠ <b>Bugün Yüklenen:</b> {daily_up}\n"
            f"┠ <b>Aylık İndirilen:</b> {month_down}\n"
            f"┖ <b>Aylık Yüklenen:</b> {month_up}\n\n"
            f"┟ <b>CPU</b> → {cpu}% | <b>Boş Disk</b> → {free_disk}GB [{free_percent}%]\n"
            f"┖ <b>RAM</b> → {ram}% | <b>Süre</b> → {uptime}"
        )

        await message.reply_text(text, parse_mode=enums.ParseMode.HTML, quote=True)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("istatistik hata:", e)
