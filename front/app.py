import streamlit as st
import pandas as pd
import psycopg2
import requests
import sys
import os
import urllib.parse
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.config import DB_CONFIG, get_usd_rate, CS_MARKET_API_KEY

st.set_page_config(page_title="P2P Terminal v2.0", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
    <style>
    .main { background-color: #0E1117; }
    .stMetric { background-color: #161B22; padding: 15px; border-radius: 10px; border: 1px solid #30363D; }
    </style>
    """, unsafe_allow_html=True)

if 'usd_rate' not in st.session_state:
    st.session_state.usd_rate = 77.42


def check_live_price(hash_name):
    encoded_name = urllib.parse.quote(hash_name.replace('™', '').strip())
    url = f"https://market.csgo.com/api/v2/search-item-by-hash-name?key={CS_MARKET_API_KEY}&hash_name={encoded_name}"
    try:
        r = requests.get(url, timeout=5)
        res = r.json()
        if res.get('success') and res.get('data'):
            return float(res['data'][0]['price']) / 100
        return "Лот не найден"
    except:
        return "Ошибка"


def calculate_net_profit(row, rate):
    cost = (row['price_lis'] * rate) * 1.03


    if row['live_p_rub'] and row['live_p_rub'] > 0:
        income_raw = row['live_p_rub']
    else:
        income_raw = (row['avg_m_usd'] if row['avg_m_usd'] > 0 else row['price_market_usd']) * rate

    income_after_sale = income_raw * 0.95

    if income_after_sale > 4411:
        final_money = income_after_sale * 0.95
    else:
        final_money = income_after_sale - (income_after_sale * 0.016 + 50)

    net_profit = final_money - cost
    roi = (net_profit / cost * 100) if cost > 0 else 0
    return pd.Series([round(net_profit, 2), round(roi, 2)])


@st.cache_data(ttl=30)
def load_data():
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
    WITH latest_prices AS (
        SELECT DISTINCT ON (item_id, source) 
            item_id, source, price_buy, avg_price, volume, updated_at, 
            live_price_cache, last_live_check
        FROM prices
        WHERE updated_at > NOW() - INTERVAL '24 hours'
        ORDER BY item_id, source, updated_at DESC
    )
    SELECT 
        i.clean_name as "Название",
        i.quality as "Качество",
        i.category as "Кат.",
        p_lis.price_buy as price_lis,
        p_cs.price_buy as price_market_usd,
        p_cs.avg_price as avg_m_usd,
        p_cs.live_price_cache as live_p_rub,
        p_cs.volume as "Vol",
        p_cs.last_live_check as "check_time",
        i.market_hash_name as "full_name"
    FROM items i
    JOIN latest_prices p_lis ON i.id = p_lis.item_id AND p_lis.source = 'lis_skins'
    JOIN latest_prices p_cs ON i.id = p_cs.item_id AND p_cs.source = 'cs_market'
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

st.title("P2P Trading Terminal")

raw_df = load_data()
if not raw_df.empty:
    df = raw_df.copy()
    df[['Прибыль', 'ROI']] = df.apply(calculate_net_profit, axis=1, args=(st.session_state.usd_rate,))

    df = df[(df['ROI'] > -15) & (df['ROI'] < 40)]

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Связок в базе", len(df))
    m2.metric("Макс. профит", f"{df['Прибыль'].max():.0f} ₽")
    m3.metric("Курс USD", f"{st.session_state.usd_rate} ₽")
    m4.metric("Проверено Валидатором", df['check_time'].notnull().sum())

    st.sidebar.header("🕹 Управление")
    search = st.sidebar.text_input("🔍 Поиск скина")
    min_roi = st.sidebar.slider("Мин. ROI (%)", -5.0, 20.0, 2.0)
    min_v = st.sidebar.number_input("Мин. продаж (Vol)", value=5)

    if st.sidebar.button("🗑 Очистить кэш"):
        st.cache_data.clear()
        st.rerun()

    col_table, col_panel = st.columns([2.5, 1])

    with col_table:
        f_df = df[(df['ROI'] >= min_roi) & (df['Vol'] >= min_v)].sort_values("ROI", ascending=False)
        if search:
            f_df = f_df[f_df['Название'].str.contains(search, case=False)]


        def format_status(row):
            if pd.isnull(row['check_time']): return "⌛ Ожидание"
            return "⚡ Live"


        f_df['Статус'] = f_df.apply(format_status, axis=1)

        selection = st.dataframe(
            f_df.drop(columns=['full_name', 'check_time', 'live_p_rub', 'avg_m_usd', 'price_market_usd']),
            column_config={
                "ROI": st.column_config.NumberColumn("ROI", format="%.2f%%"),
                "Прибыль": st.column_config.NumberColumn("Профит", format="%.2f ₽"),
                "price_lis": st.column_config.NumberColumn("Lis ($)", format="%.2f$"),
                "Vol": st.column_config.ProgressColumn("Ликвидность", min_value=0, max_value=100),
            },
            use_container_width=True, height=700, on_select="rerun", selection_mode="single-row"
        )

    with col_panel:
        st.markdown("### 🎯 Детали лота")
        if len(selection.selection.rows) > 0:
            row_idx = f_df.index[selection.selection.rows[0]]
            sel_item = f_df.loc[row_idx]

            st.info(f"**{sel_item['Название']}**")
            st.write(f"Качество: `{sel_item['Качество']}`")
            st.write(f"Закуп (с комиссией): **{sel_item['price_lis'] * st.session_state.usd_rate * 1.03:.2f} ₽**")

            if st.button("🚀 ПРОВЕРИТЬ СЕЙЧАС", use_container_width=True):
                with st.spinner('Запрос к Маркету...'):
                    live = check_live_price(sel_item['full_name'])
                    if isinstance(live, float):
                        st.balloons()
                        st.metric("Живая цена (Маркет)", f"{live:.2f} ₽")
                        cost = (sel_item['price_lis'] * st.session_state.usd_rate) * 1.03
                        inc = live * 0.95
                        final = inc * 0.95 if inc > 4411 else inc - (inc * 0.016 + 50)
                        st.metric("Чистый профит", f"{final - cost:.2f} ₽", delta=f"{(final - cost) / cost * 100:.2f}%")
                    else:
                        st.error(live)
        else:
            st.write("Выберите строку в таблице слева.")