import sys
import time

import requests
import streamlit as st

sys.path.insert(0, '/app')

from components.metrics_cards import kpi_row, system_health_row
from components.charts import hourly_bar
from data.db import get_overview_stats, get_hourly_counts
from data.mlflow_client import get_metrics_history
from config import MLFLOW_TRACKING_URI, REFRESH_INTERVAL_SEC

st.set_page_config(page_title='Overview', page_icon='📊', layout='wide')
st.title('📊 Overview')
st.caption('Fraud manager\'s morning view — refreshes every 10 seconds.')

# ---------------------------------------------------------------------------
# Auto-refresh
# ---------------------------------------------------------------------------
if 'overview_last_refresh' not in st.session_state:
    st.session_state.overview_last_refresh = 0

elapsed = time.time() - st.session_state.overview_last_refresh
if elapsed >= REFRESH_INTERVAL_SEC:
    st.session_state.overview_last_refresh = time.time()

# ---------------------------------------------------------------------------
# KPI tiles
# ---------------------------------------------------------------------------
stats = get_overview_stats()

total        = int(stats.get('total_today', 0))
intercepted  = float(stats.get('value_intercepted', 0))
rate_today   = float(stats.get('fraud_rate_today', 0) or 0)
rate_yest    = float(stats.get('fraud_rate_yesterday', 0) or 0)
rate_delta   = rate_today - rate_yest

ml_df    = get_metrics_history(n_runs=1)
f1_score = ml_df['f1_score'].iloc[0] if not ml_df.empty and 'f1_score' in ml_df.columns else None
precision = ml_df['precision'].iloc[0] if not ml_df.empty else None
recall    = ml_df['recall'].iloc[0] if not ml_df.empty else None

kpi_row([
    {'label': 'Fraud Events Today',        'value': f'{total:,}'},
    {'label': 'Value Intercepted Today',   'value': f'${intercepted:,.2f}'},
    {'label': 'Fraud Rate Today',
     'value': f'{rate_today:.2f}%',
     'delta': f'{rate_delta:+.2f}% vs yesterday',
     'help':  'Fraud events / total events through the Kafka pipeline'},
    {'label': 'Model F1 Score',
     'value': f'{f1_score:.3f}' if f1_score is not None else 'N/A',
     'help':  'F1 from the most recent Airflow training run'},
    {'label': 'Precision',
     'value': f'{precision:.3f}' if precision is not None else 'N/A'},
    {'label': 'Recall',
     'value': f'{recall:.3f}' if recall is not None else 'N/A'},
])

st.divider()

# ---------------------------------------------------------------------------
# Hourly fraud volume
# ---------------------------------------------------------------------------
st.subheader('Hourly Fraud Volume (Last 24 h)')
hourly_df = get_hourly_counts(days=1)
st.plotly_chart(hourly_bar(hourly_df), use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# System health
# ---------------------------------------------------------------------------
st.subheader('System Health')


def _ping(url: str) -> bool:
    try:
        r = requests.get(url, timeout=3)
        return r.status_code < 500
    except Exception:
        return False


services = [
    {'name': 'MLflow',   'healthy': _ping(f'{MLFLOW_TRACKING_URI}/health')},
    {'name': 'Airflow',  'healthy': _ping('http://airflow-apiserver:8080/api/v2/monitor/health')},
    {'name': 'MinIO',    'healthy': _ping('http://minio:9000/minio/health/live')},
    {'name': 'PostgreSQL', 'healthy': bool(stats.get('total_today') is not None)},
]
system_health_row(services)

# rerun to enable auto-refresh
time.sleep(REFRESH_INTERVAL_SEC)
st.rerun()
