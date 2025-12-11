from pyrogram import Client, filters
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
from dotenv import load_dotenv
import os, re
from time import time

CONFIG_PATH = "/home/debian/dfbot/config.env"

if os.path.exists(CONFIG_PATH):
    load_dotenv(CONFIG_PATH)

DATABASE_URLS = os.getenv("DATABASE", "")
db_urls = [u.strip() for u in DATABASE_URLS.split(",") if u.strip()]

flood_wait = 5
last_command_time = {}

# ---------------------------------------------------
#  UNIVERSAL ID + STREMIO -> TMDB -> IMDb fallback
# ---------------------------------------------------

def extract_id(raw):
    raw = raw.strip()

    stremio = re.search(r"/detail/(movie|series)/(\d+)-", raw)
    if stremio:
        tmdb_id = stremio.group(2)
        imdb_guess = f"tt{tmdb_id}"
        return ("tmdb", tmdb_id, imdb_guess)

    if raw.isdigit():
        return ("tmdb", raw, f"tt{raw}")

    if raw.lower().startswith("tt"):
        return ("imdb", raw, None)

    tg = re.search(r"/dl/([A-Za-z0-9]+)", raw)
    if tg:
        return ("telegram", tg.group(1), None)

    if len(raw) > 30 and raw.isalnum():
        return ("telegram", raw, None)

    return ("filename", raw, None)


# ---------------------------------------------------
#  SİLME MOTORU (kategori seçilebilir)
#  category: "movie", "tv", "all"
# ---------------------------------------------------

def process_delete(db, id_type, val, imdb_fallback=None, test_mode=False, category="all"):
    deleted_files = []

    def match_category(cat):
        return category == "all" or category == cat

    # ------------------- TMDB ---------------------
    if id_type == "tmdb":
        tmdb_id = int(val)

        movie_docs = list(db["movie"].find({"tmdb_id": tmdb_id})) if match_category("movie") else []
        tv_docs = list(db["tv"].find({"tmdb_id": tmdb_id})) if match_category("tv") else []

        if not movie_docs and not tv_docs and imdb_fallback:
            return process_delete(db, "imdb", imdb_fallback, None, test_mode, category)

        for doc in movie_docs:
            for t in doc.get("telegram", []):
                deleted_files.append(t.get("name"))
            if not test_mode:
                db["movie"].delete_one({"_id": doc["_id"]})

        for doc in tv_docs:
            for s in doc.get("seasons", []):
                for e in s.get("episodes", []):
                    for t in e.get("telegram", []):
                        deleted_files.append(t.get("name"))
            if not test_mode:
                db["tv"].delete_one({"_id": doc["_id"]})

        return deleted_files

    # ------------------- IMDb ---------------------
    if id_type == "imdb":
        imdb_id = val

        movie_docs = list(db["movie"].find({"imdb_id": imdb_id})) if match_category("movie") else []
        tv_docs = list(db["tv"].find({"imdb_id": imdb_id})) if match_category("tv") else []

        for doc in movie_docs:
            for t in doc.get("telegram", []):
                deleted_files.append(t.get("name"))
            if not test_mode:
                db["movie"].delete_one({"_id": doc["_id"]})

        for doc in tv_docs:
            for s in doc.get("seasons", []):
                for e in s.get("episodes", []):
                    for t in e.get("telegram", []):
                        deleted_files.append(t.get("name"))
            if not test_mode:
                db["tv"].delete_one({"_id": doc["_id"]})

        return deleted_files

    # ------------- Telegram / Filename -------------
    target = val

    # MOVIE
    if match_category("movie"):
        movie_docs = list(db["movie"].find({}))
        for doc in movie_docs:
            tlist = doc.get("telegram", [])
            newlist = [t for t in tlist if t.get("id") != target and t.get("name") != target]
            removed = [t.get("name") for t in tlist if t not in newlist]
            deleted_files.extend(removed)

            if removed and not test_mode:
                if not newlist:
                    db["movie"].delete_one({"_id": doc["_id"]})
                else:
                    doc["telegram"] = newlist
                    db["movie"].replace_one({"_id": doc["_id"]}, doc)

    # TV
    if match_category("tv"):
        tv_docs = list(db["tv"].find({}))
        for doc in tv_docs:
            changed = False
            remove_seasons = []

            for season in doc.get("seasons", []):
                remove_eps = []

                for ep in season.get("episodes", []):
                    tlist = ep.get("telegram", [])
                    newlist = [t for t in tlist if t.get("id") != target and t.get("name") != target]

                    removed = [t.get("name") for t in tlist if t not in newlist]
                    deleted_files.extend(removed)

                    if removed:
                        changed = True

                    if newlist:
                        ep["telegram"] = newlist
                    else:
                        remove_eps.append(ep)

                for e in remove_eps:
                    season["episodes"].remove(e)

                if not season["episodes"]:
                    remove_seasons.append(season)

            for s in remove_seasons:
                doc["seasons"].remove(s)

            if changed and not test_mode:
                if not doc["seasons"]:
                    db["tv"].delete_one({"_id": doc["_id"]})
                else:
                    db["tv"].replace_one({"_id": doc["_id"]}, doc)

    return deleted_files


# ---------------------------------------------------
#  YARDIMCI: uzun listeyi TXT olarak gönderme
# ---------------------------------------------------

async def send_result(message, deleted_files, prefix):
    if not deleted_files:
        await message.reply_text("⚠️ Dosya bulunamadı.")
        return

    if len(deleted_files) > 10:
        file_path = f"/tmp/{prefix}_{int(time())}.txt"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(deleted_files))
        await message.reply_document(file_path, caption=f"{len(deleted_files)} dosya listelendi.")
    else:
        await message.reply_text("\n".join(deleted_files))


# ---------------------------------------------------
#  /vsil – tüm kategoriler
# ---------------------------------------------------

@Client.on_message(filters.command("vsil") & filters.private & CustomFilters.owner)
async def vsil(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vsil id/link")

    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]

    id_type, val, fallback = extract_id(message.command[1])

    deleted_files = process_delete(db, id_type, val, fallback, test_mode=False, category="all")
    await send_result(message, deleted_files, "vsil")


# ---------------------------------------------------
#  /vtest – tüm kategoriler test
# ---------------------------------------------------

@Client.on_message(filters.command("vtest") & filters.private & CustomFilters.owner)
async def vtest(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vtest id/link")

    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]

    id_type, val, fallback = extract_id(message.command[1])

    deleted_files = process_delete(db, id_type, val, fallback, test_mode=True, category="all")
    await send_result(message, deleted_files, "vtest")


# ---------------------------------------------------
#  /vsild – sadece DİZİ kategorisi
# ---------------------------------------------------

@Client.on_message(filters.command("vsild") & filters.private & CustomFilters.owner)
async def vsild(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vsild id/link (Sadece DİZİ siler)")

    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]

    id_type, val, fallback = extract_id(message.command[1])

    deleted_files = process_delete(db, id_type, val, fallback, test_mode=False, category="tv")
    await send_result(message, deleted_files, "vsild")


# ---------------------------------------------------
#  /vsilf – sadece FİLM kategorisi
# ---------------------------------------------------

@Client.on_message(filters.command("vsilf") & filters.private & CustomFilters.owner)
async def vsilf(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vsilf id/link (Sadece FİLM siler)")

    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]

    id_type, val, fallback = extract_id(message.command[1])

    deleted_files = process_delete(db, id_type, val, fallback, test_mode=False, category="movie")
    await send_result(message, deleted_files, "vsilf")


# ---------------------------------------------------
#  /vsildtest – dizi test
# ---------------------------------------------------

@Client.on_message(filters.command("vsildtest") & filters.private & CustomFilters.owner)
async def vsildtest(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vsildtest id/link (Dizi testi)")

    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]

    id_type, val, fallback = extract_id(message.command[1])

    deleted_files = process_delete(db, id_type, val, fallback, test_mode=True, category="tv")
    await send_result(message, deleted_files, "vsildtest")


# ---------------------------------------------------
#  /vsilftest – film test
# ---------------------------------------------------

@Client.on_message(filters.command("vsilftest") & filters.private & CustomFilters.owner)
async def vsilftest(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vsilftest id/link (Film testi)")

    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]

    id_type, val, fallback = extract_id(message.command[1])

    deleted_files = process_delete(db, id_type, val, fallback, test_mode=True, category="movie")
    await send_result(message, deleted_files, "vsilftest")
