import os
from urllib.parse import quote
from pyrogram import filters
from pyrogram.types import Message

# ================================================================
# CONFIG YÜKLEME (DFbot ile uyumlu)
# ================================================================
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
BASE_URL = os.getenv("BASE_URL", "")

AUTO_MODE = False  # /yayin açık/kapalı


# ================================================================
# /yayin KOMUTU
# ================================================================
@Client.on_message(filters.command("yayin") & filters.private)
async def yayin_toggle(client, msg: Message):

    global AUTO_MODE

    if msg.from_user.id != OWNER_ID:
        return await msg.reply("⛔ Yetkin yok.")

    AUTO_MODE = not AUTO_MODE

    if AUTO_MODE:
        return await msg.reply("✅ Yayın modu açıldı.\nDosya gönder, link vereyim.")
    else:
        return await msg.reply("⛔ Yayın modu kapatıldı.")


# ================================================================
# DOSYA GELİNCE OTOMATİK LINK ÜRET
# ================================================================
@Client.on_message((filters.video | filters.document | filters.audio) & filters.private)
async def auto_stream(client, msg: Message):

    if msg.from_user.id != OWNER_ID:
        return await msg.reply("⛔ Yetkin yok.")

    if not AUTO_MODE:
        return await msg.reply("ℹ️ Yayın modu kapalı. Açmak için: /yayin")

    file = msg.document or msg.video or msg.audio
    if not file:
        return await msg.reply("❌ Dosya alınamadı.")

    file_id = file.file_id
    safe_name = quote(file.file_name or "video.mp4")

    url = f"{BASE_URL}/dl/{file_id}/{safe_name}"

    await msg.reply(f"🔗 **Link Hazır:**\n{url}", disable_web_page_preview=True)
