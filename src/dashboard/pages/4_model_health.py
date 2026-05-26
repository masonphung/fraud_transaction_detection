import sys

import streamlit as st

sys.path.insert(0, '/app')

from components.charts import metrics_trend
from components.metrics_cards import kpi_row
from data.mlflow_client import (
    get_latest_model_version,
    get_metrics_history,
    get_confusion_matrix_image,
    get_pr_curve_image,
    get_latest_run_id,
)
from config import FRAUD_THRESHOLD

st.set_page_config(page_title='Model Health', page_icon='🤖', layout='wide')
st.title('🤖 Model Health')
st.caption('Pulled from the MLflow tracking server — updated after every Airflow training run (03:00 UTC).')

# ---------------------------------------------------------------------------
# Current model version
# ---------------------------------------------------------------------------
model_info = get_latest_model_version()

if model_info:
    kpi_row([
        {'label': 'Registered Model',   'value': 'fraud_detection_xgboost'},
        {'label': 'Version',            'value': f"v{model_info.get('version', 'N/A')}"},
        {'label': 'Stage',              'value': model_info.get('stage', 'N/A')},
        {'label': 'Last Trained',       'value': str(model_info.get('created_at', 'N/A'))[:19]},
        {'label': 'Decision Threshold', 'value': f'{FRAUD_THRESHOLD:.0%}',
         'help': 'Fixed inference threshold applied in the Spark streaming UDF'},
    ])
else:
    st.warning('No registered model found in MLflow. Run the Airflow training DAG first.')

st.divider()

# ---------------------------------------------------------------------------
# Metric trend across training runs
# ---------------------------------------------------------------------------
st.subheader('Metric Trends Across Training Runs')

n_runs = st.slider('Number of runs to display', min_value=3, max_value=30, value=10)
history_df = get_metrics_history(n_runs=n_runs)

if not history_df.empty:
    latest = history_df.iloc[0]
    kpi_row([
        {'label': 'AUC-PR',    'value': f"{latest.get('auc_pr', 0):.3f}",
         'help': 'Area under the precision-recall curve — primary metric for imbalanced data'},
        {'label': 'Precision', 'value': f"{latest.get('precision', 0):.3f}",
         'help': 'Of flagged transactions, fraction that are truly fraudulent'},
        {'label': 'Recall',    'value': f"{latest.get('recall', 0):.3f}",
         'help': 'Of all fraud cases, fraction that were caught'},
        {'label': 'F1 Score',  'value': f"{latest.get('f1_score', 0):.3f}"},
    ])
    st.plotly_chart(metrics_trend(history_df), use_container_width=True)
else:
    st.info('No completed training runs found in MLflow.')

st.divider()

# ---------------------------------------------------------------------------
# Artifacts — confusion matrix + PR curve
# ---------------------------------------------------------------------------
run_id = get_latest_run_id()

if run_id:
    col_cm, col_pr = st.columns(2)

    with col_cm:
        st.subheader('Confusion Matrix')
        cm_bytes = get_confusion_matrix_image(run_id)
        if cm_bytes:
            st.image(cm_bytes, use_container_width=True)
        else:
            st.info('Confusion matrix artifact not found for this run.')

    with col_pr:
        st.subheader('Precision-Recall Curve')
        pr_bytes = get_pr_curve_image(run_id)
        if pr_bytes:
            st.image(pr_bytes, use_container_width=True)
        else:
            st.info('PR curve artifact not found for this run.')

    with st.expander('Run ID'):
        st.code(run_id)
else:
    st.info('No run ID available — train a model via the Airflow DAG first.')
