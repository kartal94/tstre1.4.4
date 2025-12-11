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

# ------------------------------------------------------------------
#  UNIVERSAL ID PARSE + STREMIO → TMDB → IMDb fallback
# ------------------------------------------------------------------

def extract_id(raw):
    raw = raw.strip()
    stremio = re.search(r"/detail/(movie|series)/(\d+)-", raw)
    if stremio:
        tmdb = stremio.group(2)
        return ("tmdb", tmdb, f"tt{tmdb}")

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


# ------------------------------------------------------------------
#  SİLME MOTORU
# ------------------------------------------------------------------

def process_delete(db, id_type, val, imdb_fallback=None, test=False, category="all", season=None, episodes=None):
    deleted = []

    def allow(cat):
        return category == "all" or category == cat

    # ---------------- TMDB ----------------
    if id_type == "tmdb":
        tmdb_id = int(val)
        movie_docs = list(db["movie"].find({"tmdb_id": tmdb_id})) if allow("movie") else []
        tv_docs = list(db["tv"].find({"tmdb_id": tmdb_id})) if allow("tv") else []

        if not movie_docs and not tv_docs and imdb_fallback:
            return process_delete(db, "imdb", imdb_fallback, None, test, category, season, episodes)

        for doc in movie_docs:
            for t in doc.get("telegram", []):
                deleted.append(t.get("name"))
            if not test:
                db["movie"].delete_one({"_id": doc["_id"]})

        for doc in tv_docs:
            if season:  # sezon bazlı silme
                for s in doc.get("seasons", []):
                    if s.get("season_number") == season:
                        eps_to_remove = []
                        for ep in s.get("episodes", []):
                            if episodes:
                                if ep.get("episode_number") in episodes:
                                    for t in ep.get("telegram", []):
                                        deleted.append(t.get("name"))
                                    if not test:
                                        s["episodes"].remove(ep)
                            else:
                                for t in ep.get("telegram", []):
                                    deleted.append(t.get("name"))
                                if not test:
                                    eps_to_remove.append(ep)
                        for ep in eps_to_remove:
                            s["episodes"].remove(ep)
                if not test:
                    doc_seasons = [s for s in doc.get("seasons", []) if s.get("episodes")]
                    if not doc_seasons:
                        db["tv"].delete_one({"_id": doc["_id"]})
                    else:
                        doc["seasons"] = doc_seasons
                        db["tv"].replace_one({"_id": doc["_id"]}, doc)
            else:
                for s in doc.get("seasons", []):
                    for e in s.get("episodes", []):
                        for t in e.get("telegram", []):
                            deleted.append(t.get("name"))
                if not test:
                    db["tv"].delete_one({"_id": doc["_id"]})

        return deleted

    # ---------------- IMDb ----------------
    if id_type == "imdb":
        imdb_id = val
        movie_docs = list(db["movie"].find({"imdb_id": imdb_id})) if allow("movie") else []
        tv_docs = list(db["tv"].find({"imdb_id": imdb_id})) if allow("tv") else []

        for doc in movie_docs:
            for t in doc.get("telegram", []):
                deleted.append(t.get("name"))
            if not test:
                db["movie"].delete_one({"_id": doc["_id"]})

        for doc in tv_docs:
            for s in doc.get("seasons", []):
                for e in s.get("episodes", []):
                    for t in e.get("telegram", []):
                        deleted.append(t.get("name"))
            if not test:
                db["tv"].delete_one({"_id": doc["_id"]})

        return deleted

    # ------------- TELEGRAM / FILENAME -------------
    target = val

    if allow("movie"):
        for doc in list(db["movie"].find({})):
            old = doc.get("telegram", [])
            new = [t for t in old if t.get("id") != target and t.get("name") != target]
            removed = [t.get("name") for t in old if t not in new]
            deleted.extend(removed)
            if removed and not test:
                if not new:
                    db["movie"].delete_one({"_id": doc["_id"]})
                else:
                    doc["telegram"] = new
                    db["movie"].replace_one({"_id": doc["_id"]}, doc)

    if allow("tv"):
        for doc in list(db["tv"].find({})):
            changed = False
            remove_seasons = []

            for s in doc.get("seasons", []):
                if season and s.get("season_number") != season:
                    continue
                remove_eps = []
                for ep in s.get("episodes", []):
                    if episodes and ep.get("episode_number") not in episodes:
                        continue
                    for t in ep.get("telegram", []):
                        deleted.append(t.get("name"))
                    if not test:
                        remove_eps.append(ep)
                        changed = True
                for e in remove_eps:
                    s["episodes"].remove(e)
                if not s["episodes"]:
                    remove_seasons.append(s)

            for s in remove_seasons:
                doc["seasons"].remove(s)

            if changed and not test:
                if not doc["seasons"]:
                    db["tv"].delete_one({"_id": doc["_id"]})
                else:
                    db["tv"].replace_one({"_id": doc["_id"]}, doc)

    return deleted


# ------------------------------------------------------------------
#  TXT / MESAJ GÖNDERME
# ------------------------------------------------------------------

async def send_output(message, data, prefix):
    if not data:
        return await message.reply_text("⚠️ Dosya bulunamadı.")

    if len(data) > 10:
        path = f"/tmp/{prefix}_{int(time())}.txt"
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(data))
        await message.reply_document(path, caption=f"{len(data)} dosya listelendi.")
    else:
        await message.reply_text("\n".join(data))


# ------------------------------------------------------------------
#  Standart komutlar
# ------------------------------------------------------------------

@Client.on_message(filters.command("vbilgi") & filters.private & CustomFilters.owner)
async def vbilgi(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vbilgi id/link")
    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]
    idt, val, fb = extract_id(message.command[1])
    data = process_delete(db, idt, val, fb, test=True, category="all")
    await send_output(message, data, "vbilgi")


@Client.on_message(filters.command("vsil") & filters.private & CustomFilters.owner)
async def vsil(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vsil id/link")
    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]
    idt, val, fb = extract_id(message.command[1])
    data = process_delete(db, idt, val, fb, test=False, category="all")
    await send_output(message, data, "vsil")


@Client.on_message(filters.command("vtest") & filters.private & CustomFilters.owner)
async def vtest(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vtest id/link")
    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]
    idt, val, fb = extract_id(message.command[1])
    data = process_delete(db, idt, val, fb, test=True, category="all")
    await send_output(message, data, "vtest")


@Client.on_message(filters.command("vsild") & filters.private & CustomFilters.owner)
async def vsild(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vsild id/link")
    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]
    idt, val, fb = extract_id(message.command[1])
    data = process_delete(db, idt, val, fb, test=False, category="tv")
    await send_output(message, data, "vsild")


@Client.on_message(filters.command("vsildtest") & filters.private & CustomFilters.owner)
async def vsildtest(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vsildtest id/link")
    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]
    idt, val, fb = extract_id(message.command[1])
    data = process_delete(db, idt, val, fb, test=True, category="tv")
    await send_output(message, data, "vsildtest")


@Client.on_message(filters.command("vsilf") & filters.private & CustomFilters.owner)
async def vsilf(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vsilf id/link")
    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]
    idt, val, fb = extract_id(message.command[1])
    data = process_delete(db, idt, val, fb, test=False, category="movie")
    await send_output(message, data, "vsilf")


@Client.on_message(filters.command("vsilftest") & filters.private & CustomFilters.owner)
async def vsilftest(client, message):
    if len(message.command) < 2:
        return await message.reply_text("Kullanım: /vsilftest id/link")
    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]
    idt, val, fb = extract_id(message.command[1])
    data = process_delete(db, idt, val, fb, test=True, category="movie")
    await send_output(message, data, "vsilftest")


# ------------------------------------------------------------------
#  /vsilsezon – Sezon silme
# ------------------------------------------------------------------

@Client.on_message(filters.command("vsilsezon") & filters.private & CustomFilters.owner)
async def vsilsezon(client, message):
    if len(message.command) < 3:
        return await message.reply_text("Kullanım: /vsilsezon id s3")
    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]
    idt, val, fb = extract_id(message.command[1])
    season_match = re.match(r"s(\d+)", message.command[2].lower())
    if not season_match:
        return await message.reply_text("Sezon formatı hatalı. Örn: s3")
    season_num = int(season_match.group(1))
    data = process_delete(db, idt, val, fb, test=False, category="tv", season=season_num)
    await send_output(message, data, "vsilsezon")


@Client.on_message(filters.command("vsilsezontest") & filters.private & CustomFilters.owner)
async def vsilsezontest(client, message):
    if len(message.command) < 3:
        return await message.reply_text("Kullanım: /vsilsezontest id s3")
    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]
    idt, val, fb = extract_id(message.command[1])
    season_match = re.match(r"s(\d+)", message.command[2].lower())
    if not season_match:
        return await message.reply_text("Sezon formatı hatalı. Örn: s3")
    season_num = int(season_match.group(1))
    data = process_delete(db, idt, val, fb, test=True, category="tv", season=season_num)
    await send_output(message, data, "vsilsezontest")


# ------------------------------------------------------------------
#  /vsilbolum – Bölüm silme (s3e6e7e8)
# ------------------------------------------------------------------

@Client.on_message(filters.command("vsilbolum") & filters.private & CustomFilters.owner)
async def vsilbolum(client, message):
    if len(message.command) < 3:
        return await message.reply_text("Kullanım: /vsilbolum id s3e5e6...")
    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]
    idt, val, fb = extract_id(message.command[1])
    season_match = re.match(r"s(\d+)((?:e\d+)+)", message.command[2].lower())
    if not season_match:
        return await message.reply_text("Format hatalı. Örn: s3e6e7e8")
    season_num = int(season_match.group(1))
    eps = [int(x[1:]) for x in re.findall(r"e\d+", season_match.group(2))]
    data = process_delete(db, idt, val, fb, test=False, category="tv", season=season_num, episodes=eps)
    await send_output(message, data, "vsilbolum")


@Client.on_message(filters.command("vsilbolumtest") & filters.private & CustomFilters.owner)
async def vsilbolumtest(client, message):
    if len(message.command) < 3:
        return await message.reply_text("Kullanım: /vsilbolumtest id s3e4e5")
    mongo = MongoClient(db_urls[1])
    db = mongo[mongo.list_database_names()[0]]
    idt, val, fb = extract_id(message.command[1])
    season_match = re.match(r"s(\d+)((?:e\d+)+)", message.command[2].lower())
    if not season_match:
        return await message.reply_text("Format hatalı. Örn: s3e6e7e8")
    season_num = int(season_match.group(1))
    eps = [int(x[1:]) for x in re.findall(r"e\d+", season_match.group(2))]
    data = process_delete(db, idt, val, fb, test=True, category="tv", season=season_num, episodes=eps)
    await send_output(message, data, "vsilbolumtest")
