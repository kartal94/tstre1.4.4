# auto_stremio_bot.py
import os
import secrets
from urllib.parse import quote
from threading import Thread

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from pyrogram import Client, filters
from pyrogram.types import Message
from dotenv import load_dotenv

# -------------------- CONFIG --------------------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH")
BASE_URL = os.getenv("BASE_URL")  # Örn: https://yourdomain.com
OWNER_ID = int(os.getenv("OWNER_ID", "0"))  # Sadece bu kullanıcı ID dosya alabilir

if not BOT_TOKEN or not API_ID or not API_HASH or not BASE_URL or not OWNER_ID:
    raise Exception("BOT_TOKEN, API_ID, API_HASH, BASE_URL ve OWNER_ID ayarlanmalı!")

# -------------------- TELEGRAM BOT --------------------
bot = Client("stremio_bot", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH)

# Owner kontrolü
def is_owner(user_id: int):
    return user_id == OWNER_ID

# Otomatik dosya yakalama
@bot.on_message(filters.private & (filters.document | filters.video))
async def auto_file_handler(client: Client, message: Message):
    if not is_owner(message.from_user.id):
        return  # Owner değilse işlem yapma

    file = message.document or message.video
    file_id = file.file_id
    file_name = file.file_name or f"{secrets.token_hex(4)}.mkv"

    stremio_url = f"{BASE_URL}/dl/{quote(file_id)}/{quote(file_name)}"
    await message.reply_text(f"✅ Stremio uyumlu link hazır:\n{stremio_url}")

# -------------------- FASTAPI --------------------
app = FastAPI(title="Telegram Stremio Streaming")

@app.get("/dl/{file_id}/{file_name}")
async def stream_file(file_id: str, file_name: str, request: Request):
    try:
        # Telegram'dan dosya bilgisi al
        message = await bot.get_messages(chat_id="@me", message_ids=file_id)
        file = message.document or message.video
        file_size = file.file_size

        # Range header işlemleri
        range_header = request.headers.get("Range", "")
        from_bytes, until_bytes = 0, file_size - 1
        if range_header.startswith("bytes="):
            try:
                from_str, until_str = range_header[6:].split("-")
                from_bytes = int(from_str) if from_str else 0
                until_bytes = int(until_str) if until_str else file_size - 1
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid Range header")

        req_length = until_bytes - from_bytes + 1

        # Telegram'dan byte aralığı oku
        async def file_generator():
            offset = from_bytes
            chunk_size = 1024 * 1024
            while offset <= until_bytes:
                chunk = await bot.download_media(
                    file, file_name=None, file_offset=offset,
                    file_size=min(chunk_size, until_bytes - offset + 1)
                )
                yield chunk
                offset += len(chunk)

        headers = {
            "Content-Type": file.mime_type or "application/octet-stream",
            "Content-Length": str(req_length),
            "Content-Disposition": f'inline; filename="{file_name}"',
            "Accept-Ranges": "bytes",
            "Cache-Control": "public, max-age=3600, immutable",
        }
        if range_header:
            headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"
            status_code = 206
        else:
            status_code = 200

        return StreamingResponse(
            content=file_generator(),
            status_code=status_code,
            headers=headers,
            media_type=file.mime_type or "application/octet-stream"
        )

    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

# -------------------- BOT START --------------------
if __name__ == "__main__":
    import uvicorn

    # Pyrogram bot'u ayrı thread'de çalıştır
    def run_bot():
        bot.run()

    Thread(target=run_bot).start()

    # FastAPI server başlat
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
