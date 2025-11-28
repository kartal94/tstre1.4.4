from pyrogram import filters, Client, enums
from pyrogram.types import Message
from Backend.config import Telegram
from Backend.helper.database import Database

db = Database()  # global db objesi
# bot startup sırasında: await db.connect()

@Client.on_message(filters.command("start") & filters.private, group=10)
async def send_start_message(client: Client, message: Message):
    try:
        addon_url = f"{Telegram.BASE_URL}/deneme/stremio/manifest.json"

        # Database istatistiklerini al
        db_stats_list = await db.get_database_stats()
        # Örnek olarak aktif storage DB = 1
        db_stat = db_stats_list[0]

        # Film / Dizi sayıları
        movie_count = f"{db_stat['movie_count']:,}"
        tv_count = f"{db_stat['tv_count']:,}"

        # Depolama bilgisi ve bar
        used_mb = db_stat['storageSize'] / 1024 / 1024
        total_mb = 500  # toplam depolama MB cinsinden (örnek)
        percent = round((used_mb / total_mb) * 100)
        bar_size = 12
        filled = int((percent / 100) * bar_size)
        empty = bar_size - filled
        bar = "⬢" * filled + "⬡" * empty

        # Telegram mesajı
        text = (
            "Eklentiyi Stremio’ya yüklemek için aşağıdaki adresi kopyalayın ve Eklentiler bölümüne ekleyin.\n\n"
            f"<b>Eklenti adresin:</b>\n<code>{addon_url}</code>\n\n"
            f"🎬 <b>Filmler:</b> {movie_count}\n"
            f"📺 <b>Diziler:</b> {tv_count}\n\n"
            f"💾 <b>Depolama:</b>\n"
            f"{used_mb:.1f}MB / {total_mb}MB ({percent}%)\n"
            f"[{bar}]"
        )

        await message.reply_text(text, quote=True, parse_mode=enums.ParseMode.HTML)

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("Error in /start handler:", e)
