import streamlit as st

st.set_page_config(
    page_title='Fraud Detection Dashboard',
    page_icon='🛡️',
    layout='wide',
    initial_sidebar_state='expanded',
)

st.title('Fraud Detection System')
st.markdown(
    'Real-time fraud monitoring — XGBoost + Spark Structured Streaming + Kafka'
)
st.divider()

st.markdown(
    """
    **Navigate using the sidebar to access:**

    | Page | Description |
    |---|---|
    | 📊 Overview | Daily KPI tiles, fraud rate delta, model F1, system health |
    | 🚨 Live Feed | Auto-refreshing fraud alert table with confidence scores |
    | 🔍 Pattern Analytics | Pattern breakdown, time series, top merchants, country heatmap |
    | 🤖 Model Health | MLflow metric trends, PR curve, confusion matrix |
    """
)
