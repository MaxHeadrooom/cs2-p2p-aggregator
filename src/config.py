import os
import requests
import re
from dotenv import load_dotenv

load_dotenv()

def clean(key):
    val = os.getenv(key)
    if not val: return ""
    return re.sub(r'[^\x20-\x7E]', '', val).strip()

DB_CONFIG = {
    "dbname": clean("DB_NAME"),
    "user": clean("DB_USER"),
    "password": clean("DB_PASSWORD"),
    "host": clean("DB_HOST"),
    "port": clean("DB_PORT")
}

LIS_SKINS_API_KEY = clean("LIS_SKINS_API_KEY")
CS_MARKET_API_KEY = clean("CS_MARKET_API_KEY")

URL_LIS_SKINS = "https://lis-skins.com/market_export_json/api_csgo_full.json"
URL_CS_MARKET = "https://market.csgo.com/api/v2/prices/class_instance/USD.json"

def get_usd_rate():
    try:
        r = requests.get("https://open.er-api.com/v6/latest/USD", timeout=5)
        return round(float(r.json()['rates']['RUB']), 2)
    except:
        return 80