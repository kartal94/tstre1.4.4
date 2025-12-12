import multiprocessing

# GLOBAL STOP EVENT (multiprocessing uyumlu)
stop_event = multiprocessing.Event()

# ------------ Worker: batch çevirici ------------
def translate_batch_worker(batch, stop_flag):
    CACHE = {}
    results = []

    for doc in batch:
        if stop_flag.is_set():
            break

        _id = doc.get("_id")
        upd = {}

        desc = doc.get("description")
        if desc:
            upd["description"] = translate_text_safe(desc, CACHE)

        seasons = doc.get("seasons")
        if seasons and isinstance(seasons, list):
            modified = False
            for season in seasons:
                eps = season.get("episodes", []) or []
                for ep in eps:
                    if stop_flag.is_set():
                        break
                    if "title" in ep and ep["title"]:
                        ep["title"] = translate_text_safe(ep["title"], CACHE)
                        modified = True
                    if "overview" in ep and ep["overview"]:
                        ep["overview"] = translate_text_safe(ep["overview"], CACHE)
                        modified = True
            # Sezonları her durumda ekle (boş olsa da) ki update çalışsın
            upd["seasons"] = seasons

        results.append((_id, upd))

    return results

# ------------ /cevir Komutu (Sadece owner) ------------
@Client.on_message(filters.command("cevir") & filters.private & CustomFilters.owner)
async def turkce_icerik(client: Client, message: Message):
    global stop_event
    stop_event.clear()

    start_msg = await message.reply_text(
        "🇹🇷 Türkçe çeviri hazırlanıyor...\nİlerleme tek mesajda gösterilecektir.",
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
    )

    collections = [
        {"col": movie_col, "name": "Filmler", "total": 0, "done": 0, "errors": 0},
        {"col": series_col, "name": "Diziler", "total": 0, "done": 0, "errors": 0}
    ]

    for c in collections:
        c["total"] = c["col"].count_documents({})

    start_time = time.time()
    last_update = 0
    update_interval = 3  # daha sık güncelleme

    for c in collections:
        col = c["col"]
        name = c["name"]
        total = c["total"]
        done = 0
        errors = 0

        ids_cursor = col.find({}, {"_id": 1})
        ids = [d["_id"] for d in ids_cursor]

        idx = 0
        workers, batch_size = dynamic_config()
        batch_size = min(batch_size, 20)  # güvenli batch boyutu
        pool = multiprocessing.get_context("spawn").Pool(workers)

        while idx < len(ids):
            if stop_event.is_set():
                break

            batch_ids = ids[idx: idx + batch_size]
            batch_docs = list(col.find({"_id": {"$in": batch_ids}}))

            try:
                results = pool.apply(translate_batch_worker, args=(batch_docs, stop_event))
            except Exception:
                errors += len(batch_docs)
                idx += len(batch_ids)
                await asyncio.sleep(1)
                continue

            for _id, upd in results:
                try:
                    if upd:
                        col.update_one({"_id": _id}, {"$set": upd})
                    done += 1  # her doküman için done artır
                except:
                    errors += 1

            idx += len(batch_ids)
            c["done"] = done
            c["errors"] = errors

            # Tek Mesaj Güncellemesi
            if time.time() - last_update > update_interval or idx >= len(ids):
                text = ""
                total_done = 0
                total_all = 0

                cpu = psutil.cpu_percent(interval=None)
                ram_percent = psutil.virtual_memory().percent

                for col_summary in collections:
                    text += (
                        f"📌 {col_summary['name']}: {col_summary['done']}/{col_summary['total']}\n"
                        f"{progress_bar(col_summary['done'], col_summary['total'])}\n"
                        f"Kalan: {col_summary['total'] - col_summary['done']}\n\n"
                    )
                    total_done += col_summary['done']
                    total_all += col_summary['total']

                remaining_all = total_all - total_done
                elapsed_time = time.time() - start_time

                text += (
                    f"⏱ Süre: {round(elapsed_time,2)} sn | Kalan toplam: {remaining_all}\n"
                    f"💻 CPU: {cpu}% | RAM: {ram_percent}%"
                )

                try:
                    await start_msg.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ İptal Et", callback_data="stop")]])
                    )
                except:
                    pass
                last_update = time.time()

        pool.close()
        pool.join()

    # ------------ SONUÇ EKRANI ------------
    final_text = "🎉 Türkçe Çeviri Sonuçları\n\n"

    for col_summary in collections:
        final_text += (
            f"📌 {col_summary['name']}: {col_summary['done']}/{col_summary['total']}\n"
            f"{progress_bar(col_summary['done'], col_summary['total'])}\n"
            f"Kalan: {col_summary['total'] - col_summary['done']}, Hatalar: {col_summary['errors']}\n\n"
        )

    total_all = sum(c["total"] for c in collections)
    done_all = sum(c["done"] for c in collections)
    errors_all = sum(c["errors"] for c in collections)
    remaining_all = total_all - done_all

    total_time = round(time.time() - start_time)
    hours, rem = divmod(total_time, 3600)
    minutes, seconds = divmod(rem, 60)
    eta_str = f"{int(hours)}s{int(minutes)}d{int(seconds)}s"

    final_text += (
        f"📊 Genel Özet\n"
        f"Toplam içerik : {total_all}\n"
        f"Başarılı     : {done_all - errors_all}\n"
        f"Hatalı       : {errors_all}\n"
        f"Kalan        : {remaining_all}\n"
        f"Toplam süre  : {eta_str}"
    )

    try:
        await start_msg.edit_text(final_text)
    except:
        pass
