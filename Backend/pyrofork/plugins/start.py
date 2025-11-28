from pyrogram import Client, filters, enums
from pyrogram.types import Message
from Backend.config import Telegram

def hex_bar(percent: int, size: int = 12):
    """
    Altıgen bar üretir:
    Dolu: ⬢
    Boş: ⬡
    """
    filled = int((percent / 100) * size)
    empty = size - filled

    return "⬢" * filled + "⬡" * empty


@Client.on_message(filters.command("start") & filters.private)
async def send_start_message(client: Client, message: Message):
    try:
        base_url = Telegram.BASE_URL
        addon_url = f"{base_url}/stremio/manifest.json"

        # Örnek depolama verileri — gerçek DB değerlerini buraya koyabilirsin
        used_mb = 320
        total_mb = 500
        percent = round((used_mb / total_mb) * 100)

        # Altıgen bar oluştur
        bar = hex_bar(percent)

        text = (
            "Eklentiyi Stremio’ya eklemek için aşağıdaki adresi kopyalayın:\n\n"
            f"<b>Eklenti adresiniz:</b>\n<code>{addon_url}</code>\n\n"
            "<b>💾 Depolama Kullanımı</b>\n"
            f"{used_mb}MB / {total_mb}MB ({percent}%)\n\n"
            f"[{bar}]"
        )

        await message.reply_text(
            text,
            parse_mode=enums.ParseMode.HTML
        )

    except Exception as e:
        await message.reply_text(f"⚠️ Hata oluştu: {e}")
        print("Start Hata:", e)
