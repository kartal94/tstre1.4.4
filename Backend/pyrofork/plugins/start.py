from pyrogram import filters, Client, enums
from Backend.helper.custom_filter import CustomFilters
from pyrogram.types import Message
from Backend.config import Telegram
import psutil

@Client.on_message(filters.command('start') & filters.private & CustomFilters.owner, group=10)
async def send_start_message(client: Client, message: Message):
    try:
        base_url = Telegram.BASE_URL
        addon_url = f"{base_url}/stremio/manifest.json"

        # CPU, RAM ve Disk bilgilerini al
        cpu_percent = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory()
        ram_percent = ram.percent
        ram_total_gb = ram.total / (1024 ** 3)
        ram_used_gb = ram.used / (1024 ** 3)
        disk = psutil.disk_usage('/')
        disk_percent = disk.percent
        disk_free_gb = disk.free / (1024 ** 3)

        system_info = (
            f"💻 <b>CPU Kullanımı:</b> {cpu_percent}%\n"
            f"🧠 <b>RAM:</b> {ram_used_gb:.2f}GB / {ram_total_gb:.2f}GB ({ram_percent}%)\n"
            f"💾 <b>Boş Disk Alanı:</b> {disk_free_gb:.2f}GB ({disk_percent}% kullanıldı)"
        )

        await message.reply_text(
            'Eklentiyi Stremio’ya yüklemek için aşağıdaki adresi kopyalayın ve Eklentiler bölümüne ekleyin.\n\n'
            f'<b>Eklenti adresin:</b>\n<code>{addon_url}</code>\n\n'
            f'{system_info}\n',
            quote=True,
            parse_mode=enums.ParseMode.HTML
        )

    except Exception as e:
        await message.reply_text(f"⚠️ Error: {e}")
        print(f"Error in /start handler: {e}")
