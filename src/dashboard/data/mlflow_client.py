import io
import os

import mlflow
import pandas as pd
import requests

from config import MLFLOW_TRACKING_URI, MLFLOW_MODEL_NAME, MLFLOW_EXPERIMENT, MLFLOW_S3_ENDPOINT

os.environ.setdefault('MLFLOW_TRACKING_URI', MLFLOW_TRACKING_URI)
os.environ.setdefault('MLFLOW_S3_ENDPOINT_URL', MLFLOW_S3_ENDPOINT)

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# ---------------------------------------------------------------------------
# Registered model
# ---------------------------------------------------------------------------

def get_latest_model_version() -> dict:
    try:
        client = mlflow.tracking.MlflowClient()
        versions = client.get_registered_model(MLFLOW_MODEL_NAME).latest_versions
        if not versions:
            return {}
        v = versions[0]
        return {
            'version':      v.version,
            'stage':        v.current_stage,
            'run_id':       v.run_id,
            'created_at':   pd.Timestamp(v.creation_timestamp, unit='ms'),
        }
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Run metrics history
# ---------------------------------------------------------------------------

def get_metrics_history(n_runs: int = 10) -> pd.DataFrame:
    try:
        client = mlflow.tracking.MlflowClient()
        experiment = client.get_experiment_by_name(MLFLOW_EXPERIMENT)
        if experiment is None:
            return pd.DataFrame()
        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=['start_time DESC'],
            max_results=n_runs,
        )
        rows = []
        for r in runs:
            m = r.data.metrics
            rows.append({
                'run_id':    r.info.run_id,
                'started':   pd.Timestamp(r.info.start_time, unit='ms'),
                'auc_pr':    m.get('auc_pr'),
                'precision': m.get('precision'),
                'recall':    m.get('recall'),
                'f1_score':  m.get('f1_score'),
            })
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Artifact images
# ---------------------------------------------------------------------------

def _download_artifact_bytes(run_id: str, artifact_name: str) -> bytes | None:
    try:
        client = mlflow.tracking.MlflowClient()
        local_path = client.download_artifacts(run_id, artifact_name, dst_path='/tmp')
        with open(local_path, 'rb') as f:
            return f.read()
    except Exception:
        return None


def get_confusion_matrix_image(run_id: str) -> bytes | None:
    return _download_artifact_bytes(run_id, 'confusion_matrix.png')


def get_pr_curve_image(run_id: str) -> bytes | None:
    return _download_artifact_bytes(run_id, 'precision_recall_curve.png')


# ---------------------------------------------------------------------------
# Latest run shortcut
# ---------------------------------------------------------------------------

def get_latest_run_id() -> str | None:
    info = get_latest_model_version()
    return info.get('run_id')
