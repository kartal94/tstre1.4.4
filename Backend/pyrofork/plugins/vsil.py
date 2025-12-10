from pyrogram import Client, filters
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
import os
from time import time

CONFIG_PATH = "/home/debian/dfbot/config.env"
confirmation_wait = 120  # saniye
pending_deletes = {}     # user_id: { "files": [...], "arg": ..., "time": ... }

# ---------------- Database ----------------
DATABASE_URLS = os.getenv("DATABASE", "")
db_urls = [u.strip() for u in DATABASE_URLS.split(",") if u.strip()]

# ---------------- /vsil Komutu ----------------
@Client.on_message(filters.command("vsil") & filters.private & CustomFilters.owner)
async def delete_file(client: Client, message):
    user_id = message.from_user.id

    if len(message.command) < 2:
        await message.reply_text(
            "⚠️ Lütfen silinecek dosya adını, telegram ID, tmdb veya imdb ID girin:\n"
            "/vsil <telegram_id veya dosya_adı>\n"
            "/vsil <tmdb_id>\n"
            "/vsil tt<imdb_id>"
        )
        return

    arg = message.command[1]
    deleted_files = []

    # Veritabanı kontrolü
    if not db_urls or len(db_urls) < 2:
        await message.reply_text("⚠️ İkinci veritabanı bulunamadı.")
        return

    client_db = MongoClient(db_urls[1])
    db_name = client_db.list_database_names()[0]
    db = client_db[db_name]

    # Silinecek dosyaları bul
    if arg.isdigit():
        tmdb_id = int(arg)
        movie_docs = list(db["movie"].find({"tmdb_id": tmdb_id}))
        tv_docs = list(db["tv"].find({"tmdb_id": tmdb_id}))
    elif arg.lower().startswith("tt"):
        imdb_id = arg
        movie_docs = list(db["movie"].find({"imdb_id": imdb_id}))
        tv_docs = list(db["tv"].find({"imdb_id": imdb_id}))
    else:
        target = arg
        movie_docs = list(db["movie"].find({"$or":[{"telegram.id": target},{"telegram.name": target}]}))
        tv_docs = list(db["tv"].find({}))

    for doc in movie_docs:
        deleted_files += [t.get("name") for t in doc.get("telegram", [])]

    for doc in tv_docs:
        for season in doc.get("seasons", []):
            for episode in season.get("episodes", []):
                deleted_files += [t.get("name") for t in episode.get("telegram", [])]

    if not deleted_files:
        await message.reply_text("⚠️ Hiçbir eşleşme bulunamadı.")
        return

    # Onay mekanizması
    pending_deletes[user_id] = {
        "files": deleted_files,
        "arg": arg,
        "time": time()
    }

    if len(deleted_files) > 10:
        file_path = f"/tmp/silinen_dosyalar_{int(time())}.txt"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(deleted_files))
        await client.send_document(chat_id=message.chat.id, document=file_path,
                                   caption=f"⚠️ {len(deleted_files)} dosya silinecek.\nSilmek için 'evet', iptal için 'hayır' yazın. ⏳ {confirmation_wait} sn.")
    else:
        text = "\n".join(deleted_files)
        await message.reply_text(
            f"⚠️ Aşağıdaki {len(deleted_files)} dosya silinecek:\n\n{text}\n\n"
            f"Silmek için **evet** yazın.\nİptal için **hayır** yazın.\n"
            f"⏳ {confirmation_wait} saniye içinde cevap vermezseniz işlem iptal edilir."
        )

# ---------------- Onay Mesajlarını Dinleme ----------------
@Client.on_message(filters.private & CustomFilters.owner & ~filters.command)
async def confirm_delete(client: Client, message):
    user_id = message.from_user.id
    if user_id not in pending_deletes:
        return

    data = pending_deletes[user_id]

    # Süre kontrolü
    from time import time
    if time() - data["time"] > confirmation_wait:
        del pending_deletes[user_id]
        await message.reply_text("⏳ Süre doldu, silme işlemi iptal edildi.")
        return

    text = message.text.lower()
    if text == "hayır":
        del pending_deletes[user_id]
        await message.reply_text("❌ Silme işlemi iptal edildi.")
        return
    if text != "evet":
        await message.reply_text("⚠️ Lütfen 'evet' veya 'hayır' yazın.")
        return

    # Silme işlemi
    arg = data["arg"]
    client_db = MongoClient(db_urls[1])
    db_name = client_db.list_database_names()[0]
    db = client_db[db_name]

    try:
        if arg.isdigit():
            tmdb_id = int(arg)
            db["movie"].delete_many({"tmdb_id": tmdb_id})
            db["tv"].delete_many({"tmdb_id": tmdb_id})
        elif arg.lower().startswith("tt"):
            imdb_id = arg
            db["movie"].delete_many({"imdb_id": imdb_id})
            db["tv"].delete_many({"imdb_id": imdb_id})
        else:
            target = arg
            # Film ve TV için telegram listeleri temizle
            for col_name in ["movie", "tv"]:
                for doc in db[col_name].find({}):
                    modified = False
                    if col_name == "movie":
                        telegram_list = doc.get("telegram", [])
                        new_telegram = [t for t in telegram_list if t.get("id") != target and t.get("name") != target]
                        if new_telegram != telegram_list:
                            modified = True
                            doc["telegram"] = new_telegram
                    else:
                        for season in doc.get("seasons", []):
                            for ep in season.get("episodes", []):
                                telegram_list = ep.get("telegram", [])
                                ep["telegram"] = [t for t in telegram_list if t.get("id") != target and t.get("name") != target]
                    if modified or col_name=="tv":
                        db[col_name].replace_one({"_id": doc["_id"]}, doc)

        del pending_deletes[user_id]
        await message.reply_text("✅ Dosyalar başarıyla silindi.")
    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}")
        print("onay silme hata:", e)
