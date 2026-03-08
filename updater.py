import schedule
import time
import logging
import psycopg2
from datetime import datetime

from src.config import DB_CONFIG
from collectors.lis_skins.lis import collect as collect_lis
from collectors.cs_market.market import collect as collect_market

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def cleanup_db():
    logging.info("Очистка устаревших цен (старше 6 часов)")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("DELETE FROM prices WHERE updated_at < NOW() - INTERVAL '6 hours'")
        conn.commit()
        count = cur.rowcount
        cur.close()
        conn.close()
        logging.info(f"Очистка завершена. Удалено устаревших записей: {count}")
    except Exception as e:
        logging.error(f"Ошибка при очистке БД: {e}")


def update_prices():
    logging.info("--- ЗАПУСК ПОЛНОГО ЦИКЛА ОБНОВЛЕНИЯ ---")

    cleanup_db()

    try:
        logging.info("1/2: Сбор данных с Lis-Skins...")
        collect_lis()
        logging.info("Данные Lis-Skins успешно актуализированы")
    except Exception as e:
        logging.error(f"Ошибка во время обновления Lis-Skins: {e}")

    try:
        logging.info("2/2: Сбор данных с CS Market...")
        collect_market()
        logging.info("Данные CS Market успешно актуализированы")
    except Exception as e:
        logging.error(f"Ошибка во время обновления CS Market: {e}")

    logging.info("--- ВСЕ ОПЕРАЦИИ ЗАВЕРШЕНЫ. ОЖИДАНИЕ. ---")


schedule.every(2).hours.do(update_prices)

if __name__ == "__main__":
    logging.info("Планировщик VKR запущен.")

    update_prices()

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Планировщик остановлен пользователем.")