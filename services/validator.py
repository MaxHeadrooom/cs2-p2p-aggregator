import time
import requests
import psycopg2
import logging
import random
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.config import DB_CONFIG, CS_MARKET_API_KEY, get_usd_rate

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')


def get_stale_items(limit=40):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT i.market_hash_name, i.id
        FROM items i
        JOIN prices p_cs ON i.id = p_cs.item_id AND p_cs.source = 'cs_market'
        JOIN prices p_lis ON i.id = p_lis.item_id AND p_lis.source = 'lis_skins'
        WHERE (p_cs.last_live_check IS NULL OR p_cs.last_live_check < NOW() - INTERVAL '45 minutes')
        ORDER BY p_cs.last_live_check ASC NULLS FIRST
        LIMIT %s
    """, (limit,))

    rows = cur.fetchall()
    conn.close()
    return rows


def update_bulk_prices(items_chunk):
    id_map = {item[0]: item[1] for item in items_chunk}
    url = "https://market.csgo.com/api/v2/search-list-items-by-hash-name-all"

    params = [('key', CS_MARKET_API_KEY)]
    for name, _ in items_chunk:
        params.append(('list_hash_name[]', name))

    try:
        r = requests.get(url, params=params, timeout=20)
        response = r.json()

        if response.get('success') and response.get('data'):
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()

            processed_names = set()
            for name, offers in response['data'].items():
                item_id = id_map.get(name)
                if not item_id: continue

                processed_names.add(name)

                if offers and len(offers) > 0:
                    min_price_rub = float(offers[0]['price']) / 100
                    cur.execute("""
                        UPDATE prices SET live_price_cache = %s, last_live_check = NOW()
                        WHERE item_id = %s AND source = 'cs_market'
                    """, (min_price_rub, item_id))
                else:
                    cur.execute("""
                        UPDATE prices SET live_price_cache = NULL, last_live_check = NOW()
                        WHERE item_id = %s AND source = 'cs_market'
                    """, (item_id,))

            for name, item_id in id_map.items():
                if name not in processed_names:
                    cur.execute("""
                        UPDATE prices SET live_price_cache = NULL, last_live_check = NOW()
                        WHERE item_id = %s AND source = 'cs_market'
                    """, (item_id,))

            conn.commit()
            conn.close()
            return True
        else:
            logging.error(f"API Error: {response.get('error')}")
            return False
    except Exception as e:
        logging.error(f"Network Error: {e}")
        return False


if __name__ == "__main__":
    logging.info("Валидатор запущен в циклическом режиме")
    while True:
        chunk = get_stale_items(40)

        if chunk:
            success = update_bulk_prices(chunk)
            if success:
                logging.info(f"Обработано: {len(chunk)} предметов")
                time.sleep(random.uniform(4.0, 6.0))
            else:
                logging.warning("Ошибка API")
                time.sleep(60)
        else:
            logging.info("Все актуально. Ждем 2 минуты")
            time.sleep(120)