import json
import os
import logging
from dotenv import load_dotenv
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, confusion_matrix, f1_score, fbeta_score, make_scorer, precision_recall_curve, precision_score, recall_score
import yaml
import boto3
from kafka import KafkaConsumer
import mlflow
from mlflow.models import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import RandomizedSearchCV, StratifiedKFold, train_test_split
from sklearn.preprocessing import OrdinalEncoder
from xgboost import XGBClassifier
from imblearn.pipeline import Pipeline as ImbPipeline
from imblearn.over_sampling import SMOTE
import matplotlib.pyplot as plt
import joblib



logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler('./fraud_detection_model.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FraudDetectionTraining:
    def __init__(self, config_path='/app/config.yaml'):
        os.environ['GIT_PYTHON_REFRESH'] = 'quiet'
        os.environ['GIT_PYTHON_GIT_EXECUTABLE'] = '/usr/bin/git'
        
        load_dotenv(dotenv_path='/app/.env')
        
        self.config = self._load_config(config_path)
        
        os.environ.update({
            'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID'),
            'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'MLFLOW_S3_ENDPOINT_URL': self.config['mlflow']['s3_endpoint_url'],
        })
        
        self._validate_environment()
        
        mlflow.set_tracking_uri(self.config['mlflow']['tracking_uri'])
        mlflow.set_experiment(self.config['mlflow']['experiment_name'])
    
    def _load_config(self, config_path: str) -> dict:
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                logger.info(f'Configuration loaded successfully from {config_path}')
                return config
        except Exception as e:
            logger.error(f'Failed to load configuration: {str(e)}')
            raise
        
    def _validate_environment(self):
        required_vars = ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_USERNAME', 'KAFKA_PASSWORD']
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f'Missing required environment variables: {missing}')
            
        self._check_minio_connection()
        
    def _check_minio_connection(self):
        try:
            s3 = boto3.client(
                's3', 
                endpoint_url= self.config['mlflow']['s3_endpoint_url'],
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
            )

            buckets = s3.list_buckets()
            bucket_names = [b['Name'] for b in buckets.get('Buckets', [])]
            logger.info('Minio connection successful. Available buckets: %s', bucket_names)
            
            mlflow_bucket = self.config['mlflow'].get('bucket', 'mlflow')
            
            if mlflow_bucket not in bucket_names:
                s3.create_bucket(Bucket=mlflow_bucket)
                logger.info('Created missing mlflow bucket: %s', mlflow_bucket)

        except Exception as e:
            logger.error('Minio connection failed: %s', str(e))
            
    def read_from_kafka(self) -> pd.DataFrame:
        try:
            topic = self.config['kafka']['topic']
            logger.info(f'Connecting to Kafka topic: {topic}')
            
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers = self.config['kafka']['bootstrap_servers'].split(','),
                security_protocol = 'SASL_SSL',
                sasl_mechanism = 'PLAIN',
                sasl_plain_username = self.config['kafka']['username'],
                sasl_plain_password = self.config['kafka']['password'],
                value_deserializer = lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset = 'earliest',
                consumer_timeout_ms = self.config['kafka'].get('timeout', 10000)
            )
            
            messages = [msg.value for msg in consumer]
            consumer.close()
            
            df = pd.DataFrame(messages)
            # Raise noti if no data was read
            if df.empty:
                raise ValueError('No messages received from Kafka topic')
            
            # Ensure timestamp is in datetime format and fraud label is present
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601', utc=True)
            if 'is_fraud' not in df.columns:
                raise ValueError('Fraud label column "is_fraud" is missing from Kafka data')
            
            # Calculate and log the fraud rate (% of transactions that are fraudulent)
            fraud_rate = df['is_fraud'].mean() * 100
            logger.info(f'Kafka data read successfully with fraud rate: {fraud_rate:.2f}%')
            logger.info(f'Read {len(df)} records from Kafka')
            
            return df
        
        except Exception as e:
            logger.error(f'Failed to read from Kafka: {str(e)}', exc_info=True)
            raise
    
    def create_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(['user_id', 'timestamp']).copy()
        
        # ---- Temporal features ----
        df['transaction_hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['transaction_day'] = df['timestamp'].dt.day
        df['transaction_month'] = df['timestamp'].dt.month
        # Flag transactions happening at night (10 PM - 5 AM)
        df['is_night'] = ((df['transaction_hour'] >= 22) | (df['transaction_hour'] < 5)).astype(int)
        # Flag transactions happening on weekends (Saturday=5, Sunday=6)
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
        
        # ---- Behavioral features ----
        # Count of transactions in the last 24 hours for each user
        df['transaction_count_last_24h'] = df.groupby('user_id', group_keys=False).apply(
            lambda x: x.rolling('24h', on='timestamp', closed='left')['amount'].count().fillna(0)
        )
        # Time since last transaction for each user
        df['time_since_last_transaction'] = df.groupby('user_id')['timestamp'].diff().dt.total_seconds().fillna(0)
        # Count of transactions in the last 7 days for each user
        df['transaction_count_last_7d'] = df.groupby('user_id', group_keys=False).apply(
            lambda x: x.rolling('7d', on='timestamp', closed='left')['amount'].count().fillna(0)
        )
    
        # ---- Monetary features ----
        # Transaction amount compared to user's average transaction amount in the last 14 days
        df['amount_to_avg_ratio'] = df.groupby('user_id', group_keys=False).apply(
            lambda x: (x['amount'] / x.rolling('14d', on='timestamp', min_periods=1)['amount'].mean()).fillna(1.0)
        )
        
        # ---- Merchant features ----
        # Flag if the merchant is in a known high-risk list
        high_risk_merchants = self.config.get('high_risk_merchants', ['QuickCash', 'GlobalDigital', 'FastMoneyX'])
        df['is_high_risk_merchant'] = df['merchant'].isin(high_risk_merchants).astype(int)
        
        features_col = [
            'amount', 'transaction_hour', 'day_of_week', "transaction_day", "transaction_month", 'is_night', 'is_weekend',
            'transaction_count_last_24h', 'time_since_last_transaction', 'transaction_count_last_7d', 
            'amount_to_avg_ratio', 'is_high_risk_merchant'
        ]
        
        if 'is_fraud' not in df.columns:
            raise ValueError('Fraud label column "is_fraud" is missing from DataFrame')
        
        return df[features_col + ['is_fraud']]
    
    def train_model(self):
        """
        Main training logic:
        - Load data from Kafka
        - Preprocess and feature engineering
        - Train XGBoost model with hyperparameter tuning
        - Log results to MLFlow
        """
        # Placeholder for actual training code
        logger.info('Starting model training...')
        
        try:
            # Read raw transaction data from Kafka
            df = self.read_from_kafka()
            
            # Start feature engineering from the raw data
            data = self.create_features(df)
            
            # Train-test split
            X = data.drop('is_fraud', axis=1)
            y = data['is_fraud']
            
            if y.sum() == 0:
                raise ValueError('No fraudulent transactions in the dataset, cannot train model')
            
            if y.sum() < 10:
                logger.warning('Very few fraudulent transactions in the dataset, model performance may be poor', y.sum())
            
            X_train, X_test, y_train, y_test = train_test_split(
                X, y,
                test_size=self.config['model'].get('test_size', 0.2),
                random_state=self.config['model'].get('random_state', 282),
                stratify=y
            )

            with mlflow.start_run():
                mlflow.log_metrics({
                    'train_samples': len(X_train),
                    'test_samples': len(X_test),
                    'positive_samples': int(y_train.sum()),
                    'fraud_rate': float(y_train.mean())
                })
                
                preprocessor = ColumnTransformer([
                    (   
                        'merchant_encoder', OrdinalEncoder(
                            handle_unknown='use_encoded_value', unknown_value=-1, dtype=np.float32
                            ), ['is_high_risk_merchant']
                    ),
                ], remainder='passthrough')
                
                model_params = self.config['model'].get('params') or {}
                # Build classifier with parameters from config
                xgb = XGBClassifier(
                    eval_metric='aucpr',
                    random_state=self.config['model'].get('random_state', 282),
                    n_jobs=-1,
                    reg_lambda=1.0,
                    max_depth=model_params.get('max_depth', 6),
                    n_estimators=model_params.get('n_estimators', 100),
                    learning_rate=model_params.get('learning_rate', 0.1),
                    subsample=model_params.get('subsample', 0.8),
                    colsample_bytree=model_params.get('colsample_bytree', 0.8),
                    tree_method=model_params.get('tree_method', 'hist')
                )
                
                # ---- Training pipeline ----
                # Preprocessing
                pipeline = ImbPipeline([
                    ('preprocessor', preprocessor),
                    ('smote', SMOTE(random_state=self.config['model'].get('random_state', 282))),
                    ('classifier', xgb)
                ], memory='./cache')
                
                # Define hyperparameter search space for tuning
                params_distribution = {
                    'classifier__max_depth': [3, 5, 7],
                    'classifier__learning_rate': [0.01, 0.05, 0.1],
                    'classifier__subsample': [0.6, 0.8, 1.0],
                    'classifier__colsample_bytree': [0.6, 0.8, 1.0],
                    'classifier__gamma': [0, 0.1, 0.3],
                    'classifier__reg_lambda': [0, 0.1, 0.5]
                }
                # Hyperparameter tuning
                searcher = RandomizedSearchCV(
                    estimator=pipeline,
                    param_distributions=params_distribution,
                    n_iter=20,
                    scoring=make_scorer(fbeta_score, beta=2, zero_division=0),  # Focus on recall for fraud detection
                    cv=StratifiedKFold(n_splits=3, shuffle=True),
                    n_jobs=-1,
                    refit=True,
                    error_score='raise',
                    random_state=self.config['model'].get('random_state', 282)
                )
                
                logger.info('Starting hyperparameter tuning with RandomizedSearchCV...')
                searcher.fit(X_train, y_train)
                
                # Get the best model and parameters
                best_model = searcher.best_estimator_
                best_params = searcher.best_params_
                logger.info(f'Best hyperparameters found: {best_params}')
                
                # Evaluate on test set
                train_proba = best_model.predict_proba(X_train)[:, 1]
                precision_arr, recall_arr, threshold_arr = precision_recall_curve(y_train, train_proba)
                f1_scores = 2 * (precision_arr * recall_arr) / (precision_arr + recall_arr + 1e-6)
                best_threshold = threshold_arr[np.argmax(f1_scores)]
                logger.info(f'Best threshold based on F1 score: {best_threshold:.4f}')
                
                # Apply the best threshold to get binary predictions
                X_test_processed = best_model.named_steps['preprocessor'].transform(X_test)
                test_proba = best_model.named_steps['classifier'].predict_proba(X_test_processed)[:, 1]
                y_pred = (test_proba >= best_threshold).astype(int)
                
                metrics = {
                    'auc_pr': float(average_precision_score(y_test, test_proba)),
                    'precision': float(precision_score(y_test, y_pred, zero_division=0)),
                    'recall': float(recall_score(y_test, y_pred, zero_division=0)),
                    'f1_score': float(f1_score(y_test, y_pred, zero_division=0))
                }
                
                # Log metrics to MLFlow
                mlflow.log_metrics(metrics)
                mlflow.log_params(best_params)
                
                # Plot confusion matrix and log it as an artifact
                cm = confusion_matrix(y_test, y_pred)
                plt.figure(figsize=(6, 4))
                plt.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
                plt.title('Confusion Matrix')
                plt.colorbar()
                tick_marks = np.arange(2)
                plt.xticks(tick_marks, ['Not Fraud', 'Fraud'], rotation=45)
                plt.yticks(tick_marks, ['Not Fraud', 'Fraud'])
                plt.tight_layout()
                plt.ylabel('True label')
                plt.xlabel('Predicted label')
                
                for i in range(2):
                    for j in range(2):
                        plt.text(
                            j, i, format(cm[i, j], 'd'), 
                            horizontalalignment='center', 
                            color='white' if cm[i, j] > cm.max() / 2 else 'black'
                        )
                plt.tight_layout()
                cm_file_name = 'confusion_matrix.png'
                plt.savefig(cm_file_name)
                mlflow.log_artifact(cm_file_name)
                plt.close()
                
                # Plot precision-recall curve and log it as an artifact
                plt.figure(figsize=(10, 4))
                plt.plot(recall_arr, precision_arr, marker='.', label='Precision-Recall curve')
                plt.xlabel('Recall')
                plt.ylabel('Precision')
                plt.title('Precision-Recall Curve')
                plt.legend()
                pr_file_name = 'precision_recall_curve.png'
                plt.savefig(pr_file_name)
                mlflow.log_artifact(pr_file_name)
                plt.close()
                
                signature = infer_signature(X_train, y_pred)
                mlflow.sklearn.log_model(
                    sk_model=best_model,
                    name='model',
                    signature=signature,
                    registered_model_name=self.config['mlflow'].get('registered_model_name', 'fraud_detection_model')
                )
                
                logger.info('Model training successfully completed with metrics: %s', metrics)
                
                # Save the best model locally as a pickle file
                model_path = model_params.get('path', '/app/models/fraud_detection_xgb_model.pkl')
                os.makedirs('/app/models', exist_ok=True)
                joblib.dump(best_model, model_path)
                logger.info('Best model saved locally at %s', model_path    )
                
                return best_model, metrics
        
        except Exception as e:
            logger.error(f'Model training failed: {str(e)}', exc_info=True)
            raise