import requests
import psycopg2
from psycopg2.extras import execute_values
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import URL_CS_MARKET, DB_CONFIG
from src.models import normalize_item_name


def collect():
    try:
        r = requests.get(URL_CS_MARKET, timeout=30)
        r.raise_for_status()
        data = r.json()
        items = data.get('items', {})
    except Exception as e:
        print(f"Error: {e}")
        return

    print(f"Got {len(items)} items from Market. Filtering duplicates...")

    best_prices = {}
    for key, val in items.items():
        if not isinstance(val, dict): continue
        name = val.get('market_hash_name')
        if not name: continue

        price = float(val.get('price') or 0)
        if price <= 0: continue

        if name not in best_prices or price < best_prices[name]['price']:
            best_prices[name] = {
                'price': price,
                'avg': float(val.get('avg_price') or 0),
                'order': float(val.get('buy_order') or 0),
                'vol': int(val.get('popularity_7d') or 0)
            }

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    items_to_insert = []
    for name in best_prices.keys():
        norm = normalize_item_name(name)
        items_to_insert.append((
            name, norm.clean_name, norm.gun, norm.skin,
            norm.quality, norm.category, norm.is_stattrack
        ))

    execute_values(cur, """
        INSERT INTO items (market_hash_name, clean_name, gun, skin, quality, category, is_stattrack)
        VALUES %s ON CONFLICT (market_hash_name) DO NOTHING
    """, items_to_insert, page_size=10000)
    conn.commit()

    cur.execute("SELECT market_hash_name, id FROM items")
    name_to_id = dict(cur.fetchall())

    prices_to_insert = []
    for name, p_data in best_prices.items():
        if name in name_to_id:
            prices_to_insert.append((
                name_to_id[name], 'cs_market', p_data['price'],
                p_data['avg'], p_data['order'], p_data['vol']
            ))

    print(f"Upserting {len(prices_to_insert)} unique prices...")
    execute_values(cur, """
        INSERT INTO prices (item_id, source, price_buy, avg_price, buy_order, volume)
        VALUES %s
        ON CONFLICT (item_id, source) 
        DO UPDATE SET 
            price_buy = EXCLUDED.price_buy,
            avg_price = EXCLUDED.avg_price,
            buy_order = EXCLUDED.buy_order,
            volume = EXCLUDED.volume,
            updated_at = NOW()
    """, prices_to_insert, page_size=10000)

    conn.commit()
    cur.close()
    conn.close()
    print(" CS Market")