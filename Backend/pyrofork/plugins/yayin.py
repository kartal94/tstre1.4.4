# auto_stremio_bot_config_fallback.py
import os
import secrets
import importlib.util
from urllib.parse import quote
from threading import Thread

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from pyrogram import Client, filters
import asyncio

# -------------------- CONFIG --------------------
CONFIG_PATH = "/home/debian/dfbot/config.env"

def load_config():
    """Config dosyasını oku, yoksa env'den al"""
    if os.path.exists(CONFIG_PATH):
        spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
        config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config)
        return config
    else:
        # Geçici env fallback
        class EnvConfig:
            BOT_TOKEN = os.environ.get("BOT_TOKEN")
            API_ID = int(os.environ.get("API_ID", "0"))
            API_HASH = os.environ.get("API_HASH")
            BASE_URL = os.environ.get("BASE_URL")
            OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
            PORT = int(os.environ.get("PORT", "8000"))
        return EnvConfig

config = load_config()

BOT_TOKEN = getattr(config, "BOT_TOKEN", None)
API_ID = getattr(config, "API_ID", None)
API_HASH = getattr(config, "API_HASH", None)
BASE_URL = getattr(config, "BASE_URL", None)
OWNER_ID = getattr(config, "OWNER_ID", None)
PORT = getattr(config, "PORT", 8000)

if not BOT_TOKEN or not API_ID or not API_HASH or not BASE_URL or not OWNER_ID:
    raise Exception("BOT_TOKEN, API_ID, API_HASH, BASE_URL ve OWNER_ID tanımlanmalı!")

# -------------------- TELEGRAM BOT --------------------
bot = Client("stremio_bot", bot_token=BOT_TOKEN, api_id=API_ID, api_hash=API_HASH)

# Owner kontrolü
def is_owner(user_id: int):
    return user_id == OWNER_ID

# Otomatik dosya yakalama
@bot.on_message(filters.private & (filters.document | filters.video))
async def auto_file_handler(client: Client, message):
    if not is_owner(message.from_user.id):
        return

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
        # Telegram üzerinden file_id ile dosya bilgisi al
        message = await bot.get_messages(chat_id="@me", message_ids=file_id)
        file = message.document or message.video
        file_size = file.file_size

        # Range header
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

        # Telegram'dan byte aralığını async olarak al
        async def file_generator():
            offset = from_bytes
            chunk_size = 1024 * 1024
            while offset <= until_bytes:
                chunk = await bot.download_media(
                    file, file_name=None,
                    file_offset=offset,
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
def start_bot():
    asyncio.run(bot.start())
    bot.idle()

if __name__ == "__main__":
    import uvicorn

    # Pyrogram bot'u ayrı thread'de çalıştır
    Thread(target=start_bot).start()

    # FastAPI server başlat
    uvicorn.run(app, host="0.0.0.0", port=PORT)
