from pyrogram import filters, Client, enums
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters

@Client.on_message(filters.command('stats') & filters.private & CustomFilters.owner, group=10)
async def stats_greeting(client: Client, message: Message):
    """
    /stats komutuna sadece selam mesajı gönderir
    """
    await message.reply_text(
        "Selam! Stats komutu alındı 😊",
        quote=True,
        parse_mode=enums.ParseMode.HTML
    )
