# yayin.py
import os
from pyrogram import Client, filters, enums
from pyrogram.types import Message
from dotenv import load_dotenv

# ---------------- Load Config ----------------
load_dotenv()  # .env veya config.env dosyasından yükler

API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", 0))
BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")

# ----------------- Telegram Bot -----------------
bot = Client("bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ----------------- Owner Filter -----------------
from pyrogram.filters import Filter
class OwnerFilter(Filter):
    async def __call__(self, client, message: Message):
        return message.from_user.id == OWNER_ID

OWNER_ONLY = OwnerFilter()

# ----------------- /start -----------------
@bot.on_message(filters.command("start") & filters.private & OWNER_ONLY)
async def start(client: Client, message: Message):
    await message.reply_text(
        f"Merhaba! Bana dosya gönder, sana **Telegram linki** oluşturayım.\n"
        f"Örnek kullanım: Video, film veya dizi dosyası gönderebilirsin.\n",
        parse_mode=enums.ParseMode.MARKDOWN
    )

# ----------------- Dosya Alma -----------------
@bot.on_message(
    (filters.document | filters.video | filters.audio) & filters.private & OWNER_ONLY
)
async def handle_file(client: Client, message: Message):
    file = message.document or message.video or message.audio
    file_name = file.file_name or "file"

    try:
        # Telegram API üzerinden file_path al
        file_info = await client.get_file(file.file_id)
        file_link = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_info.file_path}"

        await message.reply_text(
            f"Dosya linkin hazır:\n\n"
            f"<b>{file_name}</b>\n"
            f"<code>{file_link}</code>\n\n"
            f"Bu linki Stremio veya tarayıcıda kullanabilirsin.",
            parse_mode=enums.ParseMode.HTML,
            quote=True
        )
    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}", quote=True)
        print(f"Dosya link hatası: {e}")

# ----------------- Bot Başlat -----------------
if __name__ == "__main__":
    bot.run()
