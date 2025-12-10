# vt_vs_vsil.py  -- /vsil komutu
import os
import re
import unicodedata
from time import time
from dotenv import load_dotenv

from pyrogram import Client, filters
from pyrogram.types import Message
from pymongo import MongoClient

# kendi projende bu filtreyi zaten kullanıyorsun:
from Backend.helper.custom_filter import CustomFilters

# ---------- Ayarlar ----------
CONFIG_PATH = "/home/debian/dfbot/config.env"  # varsa burayı kullanır
FLOOD_WAIT = 5  # saniye
TXT_THRESHOLD_COUNT = 50
TXT_THRESHOLD_CHARS = 4000

last_command_time = {}  # user_id : timestamp

# .env yükle (önce CONFIG_PATH, yoksa default)
if os.path.exists(CONFIG_PATH):
    load_dotenv(CONFIG_PATH)
else:
    load_dotenv()

DATABASE_URLS_RAW = os.getenv("DATABASE", "") or ""
DB_URLS = [u.strip() for u in DATABASE_URLS_RAW.split(",") if u.strip()]

# ---------- Yardımcı fonksiyonlar ----------
def normalize_text(s: str) -> str:
    """Unicode normalizasyonu, nokta/altçizgi/çizgi -> boşluk, çoklu boşluk -> tek,
       küçük harfe çevir, trim."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    # Replace punctuation commonly used in filenames with space
    s = re.sub(r"[._\-]+", " ", s)
    # Replace any sequence of whitespace with single space
    s = re.sub(r"\s+", " ", s)
    return s.lower().strip()

def is_file_match(query: str, filename: str) -> bool:
    """Güvenli, tolerant dosya eşleştirme:
       1) normalize edilmiş tam eşleşme
       2) normalize edilmiş içeriyor mu
       3) orijinal query küçük/küçük karşılaştırması (fallback)
    """
    if not query or not filename:
        return False

    qn = normalize_text(query)
    fn = normalize_text(filename)

    # 1) Tam normalize eşleşme
    if qn == fn:
        return True

    # 2) normalize edilmiş içeriyor mu
    if qn in fn:
        return True

    # 3) Fallback: raw küçük harf içerik kontrolü
    if query.lower().strip() in filename.lower():
        return True

    return False

def dump_deleted_list_and_send(client: Client, chat_id: int, deleted_files: list):
    """Eğer çok fazla öğe varsa txt dosyası oluşturup gönder. Aksi halde mesajla gönder."""
    if not deleted_files:
        return

    total_chars = sum(len(x) for x in deleted_files)
    if len(deleted_files) > TXT_THRESHOLD_COUNT or total_chars > TXT_THRESHOLD_CHARS:
        file_path = f"/tmp/vsil_deleted_{int(time())}.txt"
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                for name in deleted_files:
                    f.write(name + "\n")
            client.send_document(chat_id=chat_id, document=file_path, caption="✅ Silinen dosyalar")
            try:
                os.remove(file_path)
            except Exception:
                pass
        except Exception as e:
            client.send_message(chat_id=chat_id, text=f"⚠️ TXT yazma/gönderme hatası: {e}")
    else:
        text = "✅ Silinen dosyalar:\n" + "\n".join(deleted_files)
        client.send_message(chat_id=chat_id, text=text)

# ---------- /vsil Komutu ----------
@Client.on_message(filters.command("vsil") & filters.private & CustomFilters.owner)
async def vsil_command(client: Client, message: Message):
    user_id = message.from_user.id
    now_ts = time()

    # flood
    last = last_command_time.get(user_id)
    if last and now_ts - last < FLOOD_WAIT:
        await message.reply_text(f"⚠️ Lütfen {FLOOD_WAIT} saniye bekleyin.", quote=True)
        return
    last_command_time[user_id] = now_ts

    # arg kontrolü
    if len(message.command) < 2:
        await message.reply_text(
            "⚠️ Kullanım:\n"
            "/vsil <tmdb_id>           (ör. /vsil 223300)\n"
            "/vsil tt<imdb_id>         (ör. /vsil tt16280546)\n"
            "/vsil <telegram_id>       (ör. id: FWL...)\n"
            "/vsil <dosya_adı veya kısmi>", quote=True)
        return

    # DB hazır mı?
    if not DB_URLS or len(DB_URLS) < 2:
        await message.reply_text("⚠️ İkinci veritabanı bulunamadı.", quote=True)
        return

    arg0 = message.command[1].strip()
    # support cases: "/vsil tmdb 223300" or "/vsil 223300" -> handle both
    if arg0.lower() == "tmdb" and len(message.command) > 2:
        mode = "tmdb"
        identifier = message.command[2].strip()
    else:
        # otherwise single arg
        mode = None
        identifier = arg0

    deleted_files = []

    try:
        # connect to second DB
        client_db = MongoClient(DB_URLS[1])
        db_list = client_db.list_database_names()
        if not db_list:
            await message.reply_text("⚠️ Hedef veritabanı bulunamadı.", quote=True)
            return
        db = client_db[db_list[0]]
        movie_col = db["movie"]
        tv_col = db["tv"]

        # ---------- tmdb id (numeric) => tam doküman sil ----------
        if (mode == "tmdb") or (identifier.isdigit() and not identifier.startswith("0")):
            # If mode == tmdb we used identifier from message[2], else identifier is numeric
            try:
                tmdb_id = int(identifier)
            except ValueError:
                await message.reply_text("⚠️ Geçersiz tmdb id.", quote=True)
                return

            # Movie'leri sil
            movie_docs = list(movie_col.find({"tmdb_id": tmdb_id}))
            for doc in movie_docs:
                for t in doc.get("telegram", []):
                    if t.get("name"):
                        deleted_files.append(t.get("name"))
                movie_col.delete_one({"_id": doc["_id"]})

            # TV'leri sil
            tv_docs = list(tv_col.find({"tmdb_id": tmdb_id}))
            for doc in tv_docs:
                for season in doc.get("seasons", []):
                    for ep in season.get("episodes", []):
                        for t in ep.get("telegram", []):
                            if t.get("name"):
                                deleted_files.append(t.get("name"))
                tv_col.delete_one({"_id": doc["_id"]})

            if not deleted_files:
                await message.reply_text("⚠️ Hiçbir eşleşme bulunamadı.", quote=True)
                return

            # sonuç gönder
            dump_deleted_list_and_send(client, message.chat.id, deleted_files)
            return

        # ---------- imdb id (tt...) => tam doküman sil ----------
        if identifier.lower().startswith("tt"):
            imdb_id = identifier.lower()

            movie_docs = list(movie_col.find({"imdb_id": imdb_id}))
            for doc in movie_docs:
                for t in doc.get("telegram", []):
                    if t.get("name"):
                        deleted_files.append(t.get("name"))
                movie_col.delete_one({"_id": doc["_id"]})

            tv_docs = list(tv_col.find({"imdb_id": imdb_id}))
            for doc in tv_docs:
                for season in doc.get("seasons", []):
                    for ep in season.get("episodes", []):
                        for t in ep.get("telegram", []):
                            if t.get("name"):
                                deleted_files.append(t.get("name"))
                tv_col.delete_one({"_id": doc["_id"]})

            if not deleted_files:
                await message.reply_text("⚠️ Hiçbir eşleşme bulunamadı.", quote=True)
                return

            dump_deleted_list_and_send(client, message.chat.id, deleted_files)
            return

        # ---------- telegram id veya dosya adı / kısmi isim => tek dosya sil ----------
        target = identifier  # raw user input

        # 1) Öncelik: telegram.id birebir eşleşmesi
        # movie koleksiyonunda telegram.id ile arama
        movie_docs = list(movie_col.find({"telegram.id": target}))
        for doc in movie_docs:
            telegram_list = doc.get("telegram", [])
            # hangi öğe silinecek
            removed = [t for t in telegram_list if t.get("id") == target]
            kept = [t for t in telegram_list if t.get("id") != target]
            for t in removed:
                if t.get("name"):
                    deleted_files.append(t.get("name"))
            if not kept:
                movie_col.delete_one({"_id": doc["_id"]})
            else:
                doc["telegram"] = kept
                movie_col.replace_one({"_id": doc["_id"]}, doc)

        # tv koleksiyonunda telegram.id ile arama (bölüm seviyesinde)
        tv_docs = list(tv_col.find({}))
        for doc in tv_docs:
            doc_modified = False
            seasons_to_remove = []
            for season in list(doc.get("seasons", [])):
                episodes_to_remove = []
                for episode in list(season.get("episodes", [])):
                    telegram_list = episode.get("telegram", [])
                    removed = [t for t in telegram_list if t.get("id") == target]
                    if removed:
                        for t in removed:
                            if t.get("name"):
                                deleted_files.append(t.get("name"))
                        new_telegram = [t for t in telegram_list if t.get("id") != target]
                        if new_telegram:
                            episode["telegram"] = new_telegram
                        else:
                            episodes_to_remove.append(episode)
                        doc_modified = True
                # remove episodes that became empty
                for ep in episodes_to_remove:
                    if ep in season["episodes"]:
                        season["episodes"].remove(ep)
                # if season has no episodes now, mark for removal
                if not season.get("episodes"):
                    seasons_to_remove.append(season)
            # remove empty seasons
            for s in seasons_to_remove:
                if s in doc.get("seasons", []):
                    doc["seasons"].remove(s)
            if doc_modified:
                # if no seasons left -> delete doc
                if not doc.get("seasons"):
                    tv_col.delete_one({"_id": doc["_id"]})
                else:
                    tv_col.replace_one({"_id": doc["_id"]}, doc)

        # If found by exact telegram.id, return results
        if deleted_files:
            dump_deleted_list_and_send(client, message.chat.id, deleted_files)
            return

        # 2) Eğer telegram.id ile bulunmadıysa -> dosya adı / kısmi isim ile arama (normalize edilmiş)
        # movie: iterate and find first matching file (single-file delete mode)
        movie_cursor = movie_col.find({})
        movie_found = False
        for doc in movie_cursor:
            telegram_list = doc.get("telegram", [])
            for t in telegram_list:
                name = t.get("name", "")
                if is_file_match(target, name):
                    # remove this one file only
                    deleted_files.append(name)
                    new_telegram = [x for x in telegram_list if not (x.get("id") == t.get("id") and x.get("name") == t.get("name"))]
                    if not new_telegram:
                        movie_col.delete_one({"_id": doc["_id"]})
                    else:
                        doc["telegram"] = new_telegram
                        movie_col.replace_one({"_id": doc["_id"]}, doc)
                    movie_found = True
                    break
            if movie_found:
                break

        if movie_found:
            dump_deleted_list_and_send(client, message.chat.id, deleted_files)
            return

        # tv: iterate and find first matching file (single-file delete mode)
        tv_cursor = tv_col.find({})
        tv_found = False
        for doc in tv_cursor:
            doc_modified = False
            seasons_to_remove = []
            for season in list(doc.get("seasons", [])):
                episodes_to_remove = []
                for episode in list(season.get("episodes", [])):
                    telegram_list = episode.get("telegram", [])
                    remove_here = None
                    for t in telegram_list:
                        name = t.get("name", "")
                        if is_file_match(target, name):
                            # mark to remove this telegram entry from this episode
                            deleted_files.append(name)
                            new_telegram = [x for x in telegram_list if not (x.get("id") == t.get("id") and x.get("name") == t.get("name"))]
                            if new_telegram:
                                episode["telegram"] = new_telegram
                            else:
                                episodes_to_remove.append(episode)
                            doc_modified = True
                            # as we're in single-file mode, stop search after first found overall
                            break
                    if doc_modified:
                        # stop iterating further episodes/seasons for this doc
                        pass
                # remove episodes flagged
                for ep in episodes_to_remove:
                    if ep in season.get("episodes", []):
                        season["episodes"].remove(ep)
                if doc_modified and not season.get("episodes"):
                    seasons_to_remove.append(season)
                if doc_modified:
                    break  # found and handled in this doc, no need to check more seasons
            # remove empty seasons
            for s in seasons_to_remove:
                if s in doc.get("seasons", []):
                    doc["seasons"].remove(s)
            if doc_modified:
                # if no seasons left -> delete doc
                if not doc.get("seasons"):
                    tv_col.delete_one({"_id": doc["_id"]})
                else:
                    tv_col.replace_one({"_id": doc["_id"]}, doc)
                tv_found = True
                break
        if tv_found:
            dump_deleted_list_and_send(client, message.chat.id, deleted_files)
            return

        # Son: hiçbir eşleşme yok
        await message.reply_text("⚠️ Hiçbir eşleşme bulunamadı.", quote=True)
        return

    except Exception as e:
        await message.reply_text(f"⚠️ Hata: {e}", quote=True)
        print("vsil hata:", e)
        return
