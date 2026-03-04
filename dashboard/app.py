import sys
sys.path.append('.')
import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
from datetime import datetime
from config import REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD

def get_connection():
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )

def get_recent_trades():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT product_id, price, size, side, trade_value_usd, 
               size_category, trade_timestamp
        FROM trades.raw_events
        ORDER BY trade_timestamp DESC
        LIMIT 100
    """, conn)
    conn.close()
    return df

def get_trade_summary():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT product_id, window_start, window_end,
               total_trades, total_volume, avg_price,
               buy_count, sell_count, high_price, low_price
        FROM trades.trade_summary
        ORDER BY window_end DESC
        LIMIT 20
    """, conn)
    conn.close()
    return df

def get_metrics():
    conn = get_connection()
    df = pd.read_sql("""
        SELECT 
            COUNT(*)                    AS total_trades,
            SUM(trade_value_usd)        AS total_volume_usd,
            SUM(CASE WHEN size_category = 'WHALE' THEN 1 ELSE 0 END) AS whale_trades,
            SUM(CASE WHEN side = 'buy' THEN 1 ELSE 0 END)  AS total_buys,
            SUM(CASE WHEN side = 'sell' THEN 1 ELSE 0 END) AS total_sells
        FROM trades.raw_events
    """, conn)
    conn.close()
    return df.iloc[0]

# Page config
st.set_page_config(
    page_title="Real-Time Trade Dashboard",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Real-Time Financial Transaction Pipeline")
st.caption(f"Live data from Coinbase WebSocket | Last refreshed: {datetime.now().strftime('%H:%M:%S')}")

# Auto refresh every 10 seconds
st.markdown("""
    <meta http-equiv="refresh" content="10">
""", unsafe_allow_html=True)

# Top metrics
metrics = get_metrics()
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total Trades", f"{int(metrics['total_trades']):,}")
col2.metric("Total Volume", f"${metrics['total_volume_usd']:,.0f}")
col3.metric("Whale Trades", f"{int(metrics['whale_trades']):,}")
col4.metric("Total Buys", f"{int(metrics['total_buys']):,}")
col5.metric("Total Sells", f"{int(metrics['total_sells']):,}")

st.divider()

# Charts
col1, col2 = st.columns(2)

with col1:
    st.subheader("Trade Volume by Product")
    df = get_recent_trades()
    fig = px.bar(
        df.groupby('product_id')['trade_value_usd'].sum().reset_index(),
        x='product_id', y='trade_value_usd',
        color='product_id',
        title="Total Trade Value (Last 100 Trades)"
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Buy vs Sell Distribution")
    fig = px.pie(
        df, names='side',
        title="Buy vs Sell Ratio",
        color_discrete_map={'buy': 'green', 'sell': 'red'}
    )
    st.plotly_chart(fig, use_container_width=True)

# Trade size distribution
st.subheader("Trade Size Categories")
fig = px.histogram(
    df, x='size_category',
    color='product_id',
    title="Trade Size Distribution",
    barmode='group'
)
st.plotly_chart(fig, use_container_width=True)

# Recent trades table
st.subheader("Recent Trades")
st.dataframe(df, use_container_width=True)

# Trade summary
st.subheader("1-Minute Aggregation Windows")
summary_df = get_trade_summary()
st.dataframe(summary_df, use_container_width=True)