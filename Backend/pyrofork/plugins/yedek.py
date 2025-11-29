from pyrogram import Client, filters
from pyrogram.types import Message
from psutil import virtual_memory, cpu_percent, disk_usage
from time import time

# Bot başlama zamanı
bot_start_time = time()

# Disk kullanımını kontrol etmek için dizin
DOWNLOAD_DIR = "/"

@Client.on_message(filters.command("yedek") & filters.private)
async def system_status(client: Client, message: Message):
    try:
        # Sistem bilgilerini al
        cpu = cpu_percent(interval=1)  # CPU kullanımını ölç
        ram = virtual_memory().percent
        disk = disk_usage(DOWNLOAD_DIR)
        free_disk = round(disk.free / (1024 ** 3), 2)
        total_disk = round(disk.total / (1024 ** 3), 2)
        disk_percent = round((disk.free / disk.total) * 100, 1)

        # Uptime hesapla
        uptime_sec = int(time() - bot_start_time)
        hours, remainder = divmod(uptime_sec, 3600)
        minutes, seconds = divmod(remainder, 60)
        uptime = f"{hours}h{minutes}m{seconds}s"

        # Mesajı hazırla (görsel format)
        text = (
            "⌬ Bot Stats\n"
            f"┟ CPU → {cpu}% | F → {free_disk}GB [{disk_percent}%]\n"
            f"┖ RAM → {ram}% | UP → {uptime}"
        )

        # Cevap gönder
        await message.reply_text(text)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print(e)
