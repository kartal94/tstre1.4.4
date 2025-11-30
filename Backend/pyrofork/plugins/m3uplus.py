# ------------ /m3uplus KOMUTU ------------  
@Client.on_message(filters.command("m3uplus") & filters.private & CustomFilters.owner)
async def send_m3u(client, message: Message):
    start_msg = await message.reply_text("📝 M3U dosyası hazırlanıyor, lütfen bekleyin...")

    tmp_file_path = "filmlervediziler.m3u"  # Sabit dosya ismi

    try:
        with open(tmp_file_path, "w", encoding="utf-8") as m3u:
            m3u.write("#EXTM3U\n")

            # --- Filmler: Türlerine göre ---
            for movie in db["movie"].find({}):
                title = movie.get("title", "Unknown Movie")
                logo = movie.get("poster", "")
                telegram_files = movie.get("telegram", [])
                genres = movie.get("genres", ["Diğer"])  # genres yoksa "Diğer"

                for tg in telegram_files:
                    quality = tg.get("quality", "Unknown")
                    file_id = tg.get("id")
                    if not file_id:
                        continue
                    url = f"{BASE_URL}/dl/{file_id}/video.mkv"
                    
                    for genre in genres:
                        group_name = f"{genre} filmleri"  # tür + filmleri
                        name = f"{title} [{quality}]"
                        m3u.write(f'#EXTINF:-1 tvg-id="" tvg-name="{name}" tvg-logo="{logo}" group-title="{group_name}",{name}\n')
                        m3u.write(f"{url}\n")

            # --- Diziler: Tek kategori "Diziler" ---
            for tv in db["tv"].find({}):
                title = tv.get("title", "Unknown TV")
                group_name = "Diziler"
                seasons = tv.get("seasons", [])
                for season in seasons:
                    season_number = season.get("season_number", 1)
                    episodes = season.get("episodes", [])
                    for ep in episodes:
                        ep_number = ep.get("episode_number", 1)
                        ep_title = ep.get("title", f"{ep_number}")
                        logo = ep.get("episode_backdrop") or tv.get("poster", "")
                        telegram_files = ep.get("telegram", [])
                        for tg in telegram_files:
                            quality = tg.get("quality", "Unknown")
                            file_id = tg.get("id")
                            if not file_id:
                                continue
                            url = f"{BASE_URL}/dl/{file_id}/video.mkv"
                            name = f"{title} S{season_number:02d}E{ep_number:02d} [{quality}]"
                            m3u.write(f'#EXTINF:-1 tvg-id="" tvg-name="{name}" tvg-logo="{logo}" group-title="{group_name}",{name}\n')
                            m3u.write(f"{url}\n")

        await client.send_document(
            chat_id=message.chat.id,
            document=tmp_file_path,
            caption="📂 M3U dosyanız hazır!"
        )
        await start_msg.delete()

    except Exception as e:
        await start_msg.edit_text(f"❌ M3U dosyası oluşturulamadı.\nHata: {e}")

    finally:
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
