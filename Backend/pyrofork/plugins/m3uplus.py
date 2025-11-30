from pyrogram import Client, filters
from pyrogram.types import Message
from Backend.helper.custom_filter import CustomFilters
from pymongo import MongoClient
import os
import importlib.util
import datetime
import tempfile
import json

# ------------ CONFIG / DATABASE ------------

CONFIG_PATH = "/home/debian/dfbot/config.env"

def read_database_from_config():
    if not os.path.exists(CONFIG_PATH):
        return None
    spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    return getattr(config, "DATABASE", None)

def get_db_urls():
    db_raw = read_database_from_config()
    if not db_raw:
        db_raw = os.getenv("DATABASE", "")
    return [u.strip() for u in db_raw.split(",") if u.strip()]

db_urls = get_db_urls()
if len(db_urls) < 2:
    raise Exception("İkinci DATABASE bulunamadı!")

MONGO_URL = db_urls[1]
client_db = MongoClient(MONGO_URL)
db_name = client_db.list_database_names()[0]
db = client_db[db_name]

# BASE_URL
spec = importlib.util.spec_from_file_location("config", CONFIG_PATH)
cfg_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cfg_module)
BASE_URL = getattr(cfg_module, "BASE_URL", "")
if not BASE_URL:
    BASE_URL = os.getenv("BASE_URL", "")

# ------------ /m3uplus Komutu ------------

@Client.on_message(filters.command("m3uplus") & filters.private & CustomFilters.owner)
async def generate_m3u(client, message: Message):
    start_msg = await message.reply_text("🎬 M3U hazırlanıyor, lütfen bekleyin...")

    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".m3u")
    tmp_file_path = tmp_file.name
    tmp_file.close()

    try:
        m3u_lines = ["#EXTM3U"]

        # --- Filmler ---
        if "movie" in db.list_collection_names():
            movies = list(db["movie"].find({}))
            if movies:
                m3u_lines.append("# --- Filmler ---")
            for movie in movies:
                if "telegram" not in movie or not movie["telegram"]:
                    continue
                for tg in movie["telegram"]:
                    url = f"{BASE_URL}/dl/{tg['id']}/video.mkv"
                    title = f"{movie['title']} [{tg.get('quality', '1080p')}]"
                    logo = movie.get("poster", "")
                    m3u_lines.append(f'#EXTINF:-1 tvg-id="" tvg-name="{title}" tvg-logo="{logo}" group-title="Filmler",{title}')
                    m3u_lines.append(url)

        # --- Diziler ---
        if "tv" in db.list_collection_names():
            tvshows = list(db["tv"].find({}))
            if tvshows:
                m3u_lines.append("# --- Diziler ---")
            for show in tvshows:
                if "seasons" not in show:
                    continue
                for season in show["seasons"]:
                    for ep in season.get("episodes", []):
                        if "telegram" not in ep or not ep["telegram"]:
                            continue
                        for tg in ep["telegram"]:
                            url = f"{BASE_URL}/dl/{tg['id']}/video.mkv"
                            ep_title = ep.get("title", f"E{ep.get('episode_number', '?')}")
                            title = f"{show['title']} S{season['season_number']:02d}E{ep.get('episode_number', '?'):02d} [{tg.get('quality', '1080p')}]"
                            logo = ep.get("episode_backdrop") or show.get("poster","")
                            m3u_lines.append(f'#EXTINF:-1 tvg-id="" tvg-name="{title}" tvg-logo="{logo}" group-title="Diziler",{title}')
                            m3u_lines.append(url)

        # Dosyaya yaz
        with open(tmp_file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(m3u_lines))

        # Telegram'a gönder
        await client.send_document(
            chat_id=message.chat.id,
            document=tmp_file_path,
            caption=f"🎬 M3U hazır: {db_name}"
        )

        # İndirilebilir link
        filename = f"{db_name}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.m3u"
        download_link = f"{BASE_URL}/{filename}"  # Sunucunda BASE_URL altında bu dosyayı sunmalısın
        await message.reply_text(f"📥 M3U İndirilebilir Link: {download_link}")

        await start_msg.delete()

    except Exception as e:
        await start_msg.edit_text(f"❌ M3U oluşturulamadı.\nHata: {e}")

    finally:
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
