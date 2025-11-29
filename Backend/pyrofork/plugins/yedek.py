from pyrofork import Plugin
from psutil import virtual_memory, cpu_percent, disk_usage
import datetime

yedek = Plugin("yedek")

def format_bytes(size):
    # Baytları insan tarafından okunabilir şekilde dönüştür
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"

@yedek.on_cmd("/yedek")
async def yedek_status(client, message):
    # CPU
    cpu = cpu_percent(interval=1)
    
    # RAM
    ram = virtual_memory()
    
    # Disk (root)
    disk = disk_usage("/")
    
    # Uptime
    now = datetime.datetime.now()
    
    # Mesaj formatı
    status_msg = (
        f"📊 **System Status**\n\n"
        f"**CPU:** {cpu}%\n"
        f"**RAM:** {ram.percent}% ({format_bytes(ram.used)} / {format_bytes(ram.total)})\n"
        f"**Disk:** {disk.percent}% ({format_bytes(disk.used)} / {format_bytes(disk.total)})\n"
        f"**Time:** {now.strftime('%d-%b-%y %I:%M:%S %p')}"
    )
    
    await message.reply(status_msg)
