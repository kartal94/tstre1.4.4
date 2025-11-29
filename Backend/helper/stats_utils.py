# Eski (Hata Veren) Satır:
# from Backend.db.stats import get_db_stats

# Yeni Satır:
from Backend.helper.stats_utils import get_db_stats

# master/Backend/helper/stats_utils.py

import locale
from typing import Dict
# Lütfen aşağıdaki satırdaki içe aktarma yolunu (import path) kendi projenizin 
# MongoDB bağlantı sınıfına göre kontrol edin ve gerekirse düzeltin!
from Backend.db.database import Database 

# Türkçe formatlama için locale ayarı (Sisteminiz desteklemiyorsa bu satırı silebilirsiniz)
# locale.setlocale(locale.LC_ALL, 'tr_TR.UTF-8') 

async def get_db_stats() -> Dict[str, str]:
    """
    Tüm aktif depolama veritabanlarından (Storage DB) film ve dizi sayılarını toplar,
    toplam kullanılan depolama alanını hesaplar ve formatlanmış bir sözlük döndürür.
    """
    stats = {
        'total_movies': 0,
        'total_tv_shows': 0,
        'total_storage_mb': 0.0
    }
    
    # Veritabanı bağlantılarından (Database instances) oluşan listeyi almaya çalışın
    try:
        # Bu metodun tüm aktif depolama (storage) DB örneklerini döndürdüğü varsayılır
        db_instances = Database.get_all_instances()
    except AttributeError:
        # Eğer get_all_instances yoksa veya hata verirse, sadece ana DB'yi deneyin.
        print("Database.get_all_instances() bulunamadı. Sadece varsayılan DB denenecek.")
        db_instances = [Database.get_instance()]
    except Exception as e:
        print(f"Veritabanı örnekleri alınırken beklenmedik hata: {e}")
        db_instances = []

    for db_instance in db_instances:
        try:
            # 1. Koleksiyon Sayılarını Çekme
            # Koleksiyon isimleri (Movie, TVShow) projenizin şemasına uygun olmalıdır
            movie_count = await db_instance.Movie.count_documents({})
            tv_count = await db_instance.TVShow.count_documents({})
            
            stats['total_movies'] += movie_count
            stats['total_tv_shows'] += tv_count
            
            # 2. Depolama Boyutunu Çekme (MongoDB'den dbstats komutu ile)
            # Bu, admin komutlarını çalıştırma yetkisi gerektirebilir.
            db_stats = await db_instance.client.admin.command('dbstats', db=db_instance.name)
            
            # storageSize veya fileSize alanını kullanın
            storage_size_bytes = db_stats.get('storageSize', 0)
            
            # Byte'tan Megabyte'a çevirme ve toplama
            stats['total_storage_mb'] += storage_size_bytes / (1024 * 1024)
            
        except Exception as e:
            # Bağlantı kopukluğu, yetki hatası vb. durumlar için loglama
            print(f"'{db_instance.name}' veritabanı istatistikleri çekilirken hata: {e}")
            continue

    # 3. Verileri Telegram için formatlama

    # Depolama formatı: 123.45 MB
    stats['formatted_storage'] = f"{stats['total_storage_mb']:.2f} MB"
    
    # Sayı formatı: Binlik ayırıcı (Örn: 10.250)
    # Python'da f-string ile binlik ayırıcı: {sayı:,} kullanılır. Türkçede nokta(.) ile ayrılması için:
    formatted_movies = f"{stats['total_movies']:,}".replace(",", ".")
    formatted_tv = f"{stats['total_tv_shows']:,}".replace(",", ".")
    
    return {
        'formatted_movies': formatted_movies,
        'formatted_tv': formatted_tv,
        'formatted_storage': stats['formatted_storage']
    }

# Not: Bu fonksiyonu 'start.py' dosyasında doğru şekilde içe aktarmıştınız:
# from Backend.helper.stats_utils import get_db_stats
