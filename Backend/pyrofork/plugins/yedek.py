
from pyrogram import filters, Client
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
import os

@Client.on_message(filters.command('yedek') & filters.private & CustomFilters.owner, group=10)
async def send_backup(client: Client, message: Message):
    """
    /yedek komutu ile config.env dosyasını Telegram'a gönderir
    """
    try:
        config_path = "Backend/config.env"  # Dosyanın gerçek yolu
        if not os.path.exists(config_path):
            await message.reply_text("⚠️ Config dosyası bulunamadı.")
            return

        await message.reply_document(
            document=config_path,
            caption="📄 İşte config.env dosyanız:",
            quote=True
        )

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print(f"Error in /yedek handler: {e}")
