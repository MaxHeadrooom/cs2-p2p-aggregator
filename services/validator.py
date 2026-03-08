import time
import requests
import psycopg2
import sys
import os
import logging
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.config import DB_CONFIG, CS_MARKET_API_KEY

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)


def get_stale_items(limit=100):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT i.market_hash_name, i.id
        FROM items i
        JOIN prices p_lis ON i.id = p_lis.item_id AND p_lis.source = 'lis_skins'
        JOIN prices p_cs ON i.id = p_cs.item_id AND p_cs.source = 'cs_market'
        ORDER BY 
            (p_cs.last_live_check IS NULL) DESC, -- Сначала те, что вообще не проверялись
            ((p_cs.price_buy / NULLIF(p_lis.price_buy, 0)) - 1) DESC, -- Потом самые профитные
            p_cs.last_live_check ASC -- Потом самые "старые"
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    conn.close()
    return rows

def update_live_price(item_id, market_name):
    url = "https://market.csgo.com/api/v2/search-item-by-hash-name"
    params = {'key': CS_MARKET_API_KEY, 'hash_name': market_name}
    try:
        r = requests.get(url, params=params, timeout=5)
        data = r.json()

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        if data.get('success') and data.get('data'):
            price_rub = float(data['data'][0]['price']) / 100
            cur.execute("""
                UPDATE prices 
                SET live_price_cache = %s, last_live_check = NOW()
                WHERE item_id = %s AND source = 'cs_market'
            """, (price_rub, item_id))
            status = True
        else:
            cur.execute("""
                UPDATE prices SET last_live_check = NOW() 
                WHERE item_id = %s AND source = 'cs_market'
            """, (item_id,))
            status = False

        conn.commit()
        conn.close()
        return status
    except Exception as e:
        logging.error(f"Ошибка API для {market_name}: {e}")
        return False


if __name__ == "__main__":
    logging.info("Фоновый валидатор цен запущен...")

    while True:
        items = get_stale_items(50)
        if not items:
            logging.info("Нет предметов для проверки. Жду 60 сек...")
            time.sleep(60)
            continue

        for name, item_id in items:
            success = update_live_price(item_id, name)
            if success:
                logging.info(f"Обновлен: {name}")
            else:
                logging.info(f"Проверен (пусто): {name}")

            time.sleep(0.25)