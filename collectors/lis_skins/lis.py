import requests
import psycopg2
from psycopg2.extras import execute_values
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import LIS_SKINS_API_KEY, URL_LIS_SKINS, DB_CONFIG
from src.models import normalize_item_name


def collect():
    headers = {"Authorization": f"Bearer {LIS_SKINS_API_KEY}"}
    try:
        response = requests.get(URL_LIS_SKINS, headers=headers, timeout=60)
        data = response.json()
        items = data.get('items', [])
    except Exception as e:
        print(f"Error: {e}")
        return

    min_prices = {}
    for entry in items:
        name = entry['name']
        price = float(entry['price'] or 0)
        if price <= 0: continue

        if name not in min_prices or price < min_prices[name]['price']:
            min_prices[name] = {
                'price': price,
                'float': entry.get('item_float'),
                'ext_id': entry.get('id'),
            }

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    items_to_insert = []
    for name in min_prices.keys():
        norm = normalize_item_name(name)
        items_to_insert.append(
            (name, norm.clean_name, norm.gun, norm.skin, norm.quality, norm.category, norm.is_stattrack))

    execute_values(cur, """
        INSERT INTO items (market_hash_name, clean_name, gun, skin, quality, category, is_stattrack)
        VALUES %s ON CONFLICT (market_hash_name) DO NOTHING
    """, items_to_insert, page_size=10000)
    conn.commit()

    cur.execute("SELECT market_hash_name, id FROM items")
    name_to_id = dict(cur.fetchall())

    prices_to_insert = []
    for name, d in min_prices.items():
        if name in name_to_id:
            prices_to_insert.append((name_to_id[name], 'lis_skins', d['ext_id'], d['price'], d['float']))

    execute_values(cur, """
        INSERT INTO prices (item_id, source, external_id, price_buy, float_value)
        VALUES %s
        ON CONFLICT (item_id, source) 
        DO UPDATE SET 
            price_buy = EXCLUDED.price_buy,
            float_value = EXCLUDED.float_value,
            external_id = EXCLUDED.external_id,
            updated_at = NOW()
    """, prices_to_insert, page_size=10000)

    conn.commit()
    cur.close()
    conn.close()
    print(" Lis-Skins")