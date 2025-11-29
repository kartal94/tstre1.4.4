import asyncio
from datetime import datetime, timedelta
from Backend.logger import LOGGER
from Backend.helper.database import DBManager
from typing import List, Dict, Any, Tuple

# Verilerin cache'lenmesi
_STATS_CACHE: Dict[str, Any] = {}
_CACHE_EXPIRY: datetime = datetime.min


async def get_db_stats() -> Dict[str, Any]:
    """
    Tüm aktif depolama veritabanlarından genel medya istatistiklerini toplar ve döndürür.
    Verileri 1 saat boyunca cache'ler.
    """
    global _STATS_CACHE, _CACHE_EXPIRY
    
    # Cache kontrolü: Eğer veriler güncelse, cache'i döndür.
    if datetime.now() < _CACHE_EXPIRY:
        LOGGER.info("Returning DB stats from cache.")
        return _STATS_CACHE

    LOGGER.info("Starting new DB stats calculation.")
    
    # DBManager'dan tüm storage DB proxy'lerini al
    db_proxies = DBManager.get_all_instances()
    
    if not db_proxies:
        LOGGER.warning("No active storage databases found via DBManager.")
        return {
            "movie_count": 0,
            "tv_show_count": 0,
            "total_media_count": 0,
            "database_count": 0,
            "latest_update": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "db_details": []
        }

    # Tüm DB'ler için asenkron görevler oluştur
    tasks = [collect_stats_for_db(db_proxy, i + 1) for i, db_proxy in enumerate(db_proxies)]
    
    # Görevleri paralel olarak çalıştır ve sonuçları topla
    results: List[Tuple[int, Dict[str, Any]]] = await asyncio.gather(*tasks)

    # Toplanan istatistikleri birleştir
    total_movie_count = 0
    total_tv_show_count = 0
    db_details = []

    for db_index, stats in results:
        total_movie_count += stats['movie_count']
        total_tv_show_count += stats['tv_show_count']
        db_details.append({
            "db_index": db_index,
            "movie_count": stats['movie_count'],
            "tv_show_count": stats['tv_show_count'],
            "total_in_db": stats['movie_count'] + stats['tv_show_count'],
            "db_name": stats['db_name']
        })

    # Nihai sonucu oluştur
    final_stats = {
        "movie_count": total_movie_count,
        "tv_show_count": total_tv_show_count,
        "total_media_count": total_movie_count + total_tv_show_count,
        "database_count": len(db_proxies),
        "latest_update": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "db_details": db_details
    }

    # Cache'i güncelle
    _STATS_CACHE = final_stats
    _CACHE_EXPIRY = datetime.now() + timedelta(hours=1)
    
    LOGGER.info("DB stats calculation completed and cached.")
    return final_stats


async def collect_stats_for_db(db_proxy: Any, db_index: int) -> Tuple[int, Dict[str, Any]]:
    """
    Belirli bir DB proxy'sinden (storage DB) film ve dizi sayısını sayar.
    DBManager'daki DBProxy sınıfı, Movie ve TVShow koleksiyonlarına erişim sağlar.
    """
    movie_count = 0
    tv_show_count = 0
    db_name = f"storage_{db_index} ({db_proxy.name})"

    try:
        # DBManager.get_all_instances() tarafından sağlanan DBProxy üzerindeki koleksiyonları kullan
        movie_count = await db_proxy.Movie.count_documents({})
        tv_show_count = await db_proxy.TVShow.count_documents({})
        
        LOGGER.info(f"Stats for {db_name}: Movies={movie_count}, TVShows={tv_show_count}")
        
    except Exception as e:
        LOGGER.error(f"Error collecting stats for {db_name}: {e}")
        # Hata durumunda bile diğer DB'lerin istatistiklerini engellememek için 0 döndür
        pass

    return db_index, {
        "movie_count": movie_count,
        "tv_show_count": tv_show_count,
        "db_name": db_name
    }

# Örnek kullanım (Bu dosyanın doğrudan çalıştırılması amaçlanmamıştır, ancak test amaçlı olabilir)
if __name__ == '__main__':
    LOGGER.warning("stats_utils.py'nin bağımsız olarak çalıştırılması beklenmez.")
