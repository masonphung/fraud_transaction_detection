import sys

import streamlit as st

sys.path.insert(0, '/app')

from components.charts import pattern_donut, pattern_timeseries, country_bar
from data.db import (
    get_pattern_breakdown, get_pattern_timeseries,
    get_top_merchants, get_country_breakdown,
)

st.set_page_config(page_title='Pattern Analytics', page_icon='🔍', layout='wide')
st.title('🔍 Pattern Analytics')

# ---------------------------------------------------------------------------
# Time window selector
# ---------------------------------------------------------------------------
window = st.radio(
    'Time window',
    ['Last 24 h', 'Last 7 days', 'Last 30 days'],
    horizontal=True,
)
days_map = {'Last 24 h': 1, 'Last 7 days': 7, 'Last 30 days': 30}
days = days_map[window]

st.divider()

# ---------------------------------------------------------------------------
# Row 1 — Donut + pattern breakdown table
# ---------------------------------------------------------------------------
breakdown_df = get_pattern_breakdown(days=days)

col_donut, col_table = st.columns([1, 1])
with col_donut:
    st.subheader('Fraud Pattern Distribution')
    st.plotly_chart(pattern_donut(breakdown_df), use_container_width=True)

with col_table:
    st.subheader('Pattern Summary')
    if not breakdown_df.empty:
        display = breakdown_df.copy()
        display['total_amount'] = display['total_amount'].map('${:,.2f}'.format)
        display.columns = ['Pattern', 'Events', 'Total Amount']
        st.dataframe(display, use_container_width=True, hide_index=True)
    else:
        st.info('No data for the selected window.')

st.divider()

# ---------------------------------------------------------------------------
# Row 2 — Time series
# ---------------------------------------------------------------------------
st.subheader(f'Pattern Trend — {window}')
ts_df = get_pattern_timeseries(days=days)
st.plotly_chart(pattern_timeseries(ts_df), use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Row 3 — Top merchants + country breakdown
# ---------------------------------------------------------------------------
col_merch, col_country = st.columns([1, 1])

with col_merch:
    st.subheader('Top Flagged Merchants (Last 7 Days)')
    merch_df = get_top_merchants(limit=10)
    if not merch_df.empty:
        display = merch_df.copy()
        display['total_amount']  = display['total_amount'].map('${:,.2f}'.format)
        display['avg_confidence'] = display['avg_confidence'].map('{:.1%}'.format)
        display.columns = ['Merchant', 'Fraud Events', 'Total Amount', 'Avg Confidence']
        st.dataframe(display, use_container_width=True, hide_index=True)
    else:
        st.info('No merchant data yet.')

with col_country:
    st.subheader('Geographic Distribution (Last 7 Days)')
    country_df = get_country_breakdown()
    if not country_df.empty:
        st.plotly_chart(country_bar(country_df), use_container_width=True)
    else:
        st.info('No location data yet.')
