import os
from dotenv import load_dotenv

load_dotenv('/app/.env')

# PostgreSQL — fraud_detection database
POSTGRES_HOST     = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT     = int(os.getenv('POSTGRES_PORT', '5432'))
POSTGRES_DB       = os.getenv('POSTGRES_DB', 'fraud_detection')
POSTGRES_USER     = os.getenv('POSTGRES_USER', 'fraud_detection')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'fraud_detection')
POSTGRES_URL      = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '')
KAFKA_USERNAME          = os.getenv('KAFKA_USERNAME', '')
KAFKA_PASSWORD          = os.getenv('KAFKA_PASSWORD', '')
KAFKA_FRAUD_TOPIC       = 'fraud_predictions'

# MLflow
MLFLOW_TRACKING_URI   = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow-server:5500')
MLFLOW_S3_ENDPOINT    = os.getenv('MLFLOW_S3_ENDPOINT_URL', 'http://minio:9000')
MLFLOW_EXPERIMENT     = 'fraud_detection'
MLFLOW_MODEL_NAME     = 'fraud_detection_xgboost'

# Dashboard behaviour
REFRESH_INTERVAL_SEC  = int(os.getenv('DASHBOARD_REFRESH_INTERVAL', '10'))
FRAUD_THRESHOLD       = float(os.getenv('FRAUD_THRESHOLD', '0.60'))
LIVE_FEED_LIMIT       = 100

# Domain knowledge — must stay in sync with producer and inference
HIGH_RISK_MERCHANTS = ['QuickCash', 'GlobalDigital', 'FastMoneyX']
HIGH_RISK_COUNTRIES = ['RU', 'CN', 'NG', 'GB']

PATTERN_COLORS = {
    'Account Takeover':    '#EF4444',
    'Card Testing':        '#F97316',
    'Merchant Collusion':  '#8B5CF6',
    'Geographic Anomaly':  '#06B6D4',
    'Other':               '#6B7280',
}
