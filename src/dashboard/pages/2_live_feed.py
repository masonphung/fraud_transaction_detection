import sys
import time

import streamlit as st

sys.path.insert(0, '/app')

from components.fraud_table import fraud_table
from data.db import get_recent_predictions
from config import LIVE_FEED_LIMIT, REFRESH_INTERVAL_SEC

st.set_page_config(page_title='Live Feed', page_icon='🚨', layout='wide')
st.title('🚨 Live Fraud Feed')

# ---------------------------------------------------------------------------
# Controls
# ---------------------------------------------------------------------------
col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    st.caption('Polling the fraud_predictions PostgreSQL table — refreshes automatically.')
with col2:
    limit = st.selectbox('Show last N records', [50, 100, 250, 500], index=1)
with col3:
    refresh_sec = st.selectbox('Refresh interval (s)', [5, 10, 30, 60], index=1)

st.divider()

# ---------------------------------------------------------------------------
# Fraud alert table
# ---------------------------------------------------------------------------
df = get_recent_predictions(limit=int(limit))

col_left, col_right = st.columns([3, 1])
with col_left:
    st.subheader(f'Latest {len(df)} Fraud Alerts')
with col_right:
    if not df.empty:
        st.metric('Avg Confidence', f"{df['fraud_probability'].mean():.1%}")

fraud_table(df, key='live_feed_table')

# ---------------------------------------------------------------------------
# Pattern quick-count sidebar
# ---------------------------------------------------------------------------
if not df.empty and 'fraud_pattern' in df.columns:
    st.divider()
    st.subheader('Pattern Breakdown (Visible Window)')
    pattern_counts = df['fraud_pattern'].value_counts().reset_index()
    pattern_counts.columns = ['Pattern', 'Count']
    st.dataframe(pattern_counts, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Auto-refresh
# ---------------------------------------------------------------------------
time.sleep(refresh_sec)
st.rerun()
