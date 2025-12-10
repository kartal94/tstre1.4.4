import os
import json
import time
import datetime
import tempfile
import asyncio
from pyrogram import Client, filters
from pyrogram.types import Message
from pymongo import MongoClient
from Backend.helper.custom_filter import CustomFilters

CONFIG_PATH = "/home/debian/dfbot/config.env"

# ---------------- Database URL ----------------
def read_database_from_config():
    if not os.path.exists(CONFIG_PATH):
        return None
    import importlib.util
    spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    return getattr(config, "DATABASE", None)

def get_db_urls():
    db_raw = read_database_from_config() or os.getenv("DATABASE") or ""
    return [u.strip() for u in db_raw.split(",") if u.strip()]

db_urls = get_db_urls()
if len(db_urls) < 2:
    raise Exception("İkinci DATABASE bulunamadı!")

MONGO_URL = db_urls[1]
client_db = MongoClient(MONGO_URL)
db_name = client_db.list_database_names()[0]
db = client_db[db_name]

# ---------------- Global Cancel Event ----------------
cancel_event = asyncio.Event()

# ---------------- Pyrogram Client ----------------
app = Client("dfbot")  # Buraya api_id, api_hash veya bot_token ekle

# ---------------- /vtindir Komutu ----------------
@app.on_message(filters.command("vindir") & filters.private & CustomFilters.owner)
async def download_database(client: Client, message: Message):
    cancel_event.clear()
    start_msg = await message.reply_text("💾 Database hazırlanıyor, lütfen bekleyin...")

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"veritabani_{timestamp}.json"

    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    tmp_file_path = tmp_file.name
    tmp_file.close()

    try:
        collections = db.list_collection_names()
        total_docs = sum(db[col].count_documents({}) for col in collections)
        processed_docs = 0
        start_time = time.time()

        with open(tmp_file_path, "w", encoding="utf-8") as f:
            f.write("{")
            for i, col_name in enumerate(collections):
                if cancel_event.is_set():
                    await start_msg.edit_text("❌ İşlem kullanıcı tarafından iptal edildi.")
                    return

                if i != 0:
                    f.write(",")

                f.write(f'"{col_name}": [')
                col_cursor = db[col_name].find({})
                first_doc = True
                async for doc in col_cursor if hasattr(col_cursor, "__aiter__") else col_cursor:
                    if cancel_event.is_set():
                        await start_msg.edit_text("❌ İşlem kullanıcı tarafından iptal edildi.")
                        return

                    if not first_doc:
                        f.write(",")
                    else:
                        first_doc = False

                    f.write(json.dumps(doc, default=str, ensure_ascii=False))
                    processed_docs += 1

                    # Her 50 belge için progress güncelle
                    if processed_docs % 50 == 0 or processed_docs == total_docs:
                        elapsed = time.time() - start_time
                        remaining = (elapsed / processed_docs) * (total_docs - processed_docs) if processed_docs else 0
                        await start_msg.edit_text(
                            f"💾 Database hazırlanıyor...\n"
                            f"İlerleme: {processed_docs}/{total_docs} belgeler\n"
                            f"Tahmini kalan süre: {int(remaining)} saniye"
                        )
                        await asyncio.sleep(0.05)  # Floodwait önleme

                f.write("]")
            f.write("}")

        await client.send_document(
            chat_id=message.chat.id,
            document=tmp_file_path,
            filename=file_name,
            caption=f"📂 Veritabanı: {db_name} ({timestamp})"
        )
        await start_msg.delete()

    except Exception as e:
        await start_msg.edit_text(f"❌ Database indirilemedi.\nHata: {e}")

    finally:
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)

# ---------------- /iptal Komutu ----------------
@app.on_message(filters.command("iptal") & filters.private & CustomFilters.owner)
async def cancel_database_export(client: Client, message: Message):
    cancel_event.set()
    await message.reply_text("❌ Database indirme işlemi iptal ediliyor...")

# ---------------- RUN ----------------
if __name__ == "__main__":
    app.run()
