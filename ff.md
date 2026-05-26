Fraud Transaction Detection — System Workflow Summary
1. Project Overview
This project implements a production-grade, end-to-end real-time fraud detection system built on an event-driven architecture. The system ingests a continuous stream of financial transactions, applies machine learning inference in real-time to flag fraudulent activity, and runs a scheduled retraining pipeline to keep the model current. All components are containerized and orchestrated via Docker Compose across 14 services.

2. Technology Stack
Layer	Technology
Event Streaming	Apache Kafka (Confluent Cloud)
Stream Processing	Apache Spark 3.5.4 (Structured Streaming)
ML Framework	XGBoost + scikit-learn + imbalanced-learn
Training Orchestration	Apache Airflow 3.0.2 (CeleryExecutor)
Experiment Tracking	MLflow + MinIO (S3-compatible artifact store)
Task Broker	Redis
Metadata Store	PostgreSQL 16
Containerization	Docker / Docker Compose
3. System Architecture & Data Flow
The system is composed of three independent, loosely coupled pipelines that share Kafka as the central data bus:


┌─────────────────────────────────────────────────────────────────────┐
│                         Kafka (Confluent)                           │
│                       Topic: "transactions"                         │
└───────────────┬──────────────────────────────┬──────────────────────┘
                │                              │
                ▼                              ▼
   ┌────────────────────┐         ┌────────────────────────┐
   │   Producer Service  │         │   Training Pipeline     │
   │   (Data Generator)  │         │   (Airflow DAG – 3AM)  │
   └────────────────────┘         └────────────┬───────────┘
                                               │
                                               ▼
                                   ┌──────────────────────┐
                                   │     MLflow / MinIO    │
                                   │  (Model Registry +   │
                                   │   Artifact Store)    │
                                   └──────────┬───────────┘
                                              │
                                              ▼
                               ┌──────────────────────────┐
                               │   Inference Service       │
                               │   (Spark Structured       │
                               │    Streaming)             │
                               └──────────────┬───────────┘
                                              │
                                              ▼
                              Kafka Topic: "fraud_predictions"
4. Pipeline Descriptions
4.1 Data Production — Synthetic Transaction Generator
Service: producer (2 replicas)
Source: src/producer/main.py

The producer continuously generates synthetic financial transactions and publishes them to the Kafka topic transactions. Each message represents a single transaction with the following schema:

Field	Type	Description
transaction_id	UUID	Unique transaction identifier
user_id	Integer (1000–9999)	Simulated cardholder
amount	Float (0.01–10,000)	Transaction value
currency	String	3-letter currency code
merchant	String	Merchant name
timestamp	ISO 8601 UTC	Event time
location	String	2-letter country code
is_fraud	Binary	Ground-truth label
The producer simulates four realistic fraud patterns weighted by real-world frequency:

Pattern	Weight	Behaviour
Account Takeover	40%	Compromised users with transactions > $500, 30% fraud rate
Card Testing	30%	Micro-transactions < $2 across specific user IDs, 25% fraud rate
Merchant Collusion	20%	High-value transactions > $3,000 at high-risk merchants, 15% fraud rate
Geographic Anomaly	10%	Transactions from high-risk countries (RU, CN, NG, GB), 10% fraud rate
A baseline random fraud rate of 0.2% is applied across all users, maintaining a realistic overall fraud prevalence of 1–2% — consistent with industry-observed rates.

4.2 Model Training Pipeline — Airflow DAG
Service: Airflow (CeleryExecutor, 2 workers)
Source: src/dags/fraud_detection_training.py | src/dags/fraud_detection_training_dag.py
Schedule: Daily at 03:00 UTC (0 3 * * *)

The training DAG has three sequential tasks:


validate_environment >> execute_training >> cleanup
Task 1 — validate_environment (BashOperator)
Verifies that config.yaml and .env are present and readable before any compute resources are consumed.

Task 2 — execute_training (PythonOperator)
This is the core ML task. It executes the following sequence:

Data Ingestion — Reads from the transactions Kafka topic using a KafkaConsumer (earliest offset, SASL_SSL). Timestamps are parsed as ISO 8601 UTC. Validates that the is_fraud label column is present.

Feature Engineering — Derives 12 features across four groups:

Group	Features
Temporal	transaction_hour, day_of_week, transaction_day, transaction_month, is_night, is_weekend
Behavioural	transaction_count_last_24h (rolling 24h window), time_since_last_transaction (seconds), transaction_count_last_7d (rolling 7d window)
Monetary	amount_to_avg_ratio (current amount ÷ 14-day rolling mean per user)
Merchant	is_high_risk_merchant (binary flag against config list)
Preprocessing & Modelling — An ImbPipeline chains three steps: OrdinalEncoder (ColumnTransformer) → SMOTE (oversampling to address class imbalance) → XGBClassifier.

Hyperparameter Tuning — RandomizedSearchCV with 20 iterations, 3-fold StratifiedKFold, optimising the F2 score (recall-weighted, β=2) to minimise missed fraud cases. The search space covers max_depth, learning_rate, subsample, colsample_bytree, gamma, and reg_lambda.

Threshold Optimisation — Rather than using the default 0.5 decision threshold, the best F1-maximising threshold is derived from the training set's precision-recall curve. This decouples the classification threshold from the model fitting step.

Evaluation — The best estimator is evaluated on the held-out test set (20% stratified split): AUC-PR, Precision, Recall, and F1.

MLflow Logging — All parameters, metrics, a confusion matrix PNG, a precision-recall curve PNG, and the serialised model are logged to the MLflow tracking server backed by PostgreSQL. The model is registered in the MLflow Model Registry.

Model Persistence — The best pipeline is serialised with joblib to /app/models/fraud_detection_xgb_model.pkl for consumption by the inference service.

Task 3 — cleanup (BashOperator, trigger_rule=all_done)
Removes temporary .pkl files regardless of the training outcome to keep container storage clean.

4.3 Real-Time Inference Pipeline — Spark Structured Streaming
Service: inference
Source: src/inference/main.py

The inference service runs as a persistent Spark Structured Streaming job. It reads from the same transactions Kafka topic in real time and writes confirmed fraud predictions to fraud_predictions.

Execution Flow:

Stream Ingestion — spark.readStream subscribes to the transactions Kafka topic from the latest offset. The raw binary Kafka value is cast to a string and parsed against a predefined StructType JSON schema using from_json.

Watermarking — A 24-hour watermark on the timestamp column is applied to handle late-arriving events and enable stateful operations safely.

Feature Engineering — The same 12 features engineered during training are derived using Spark SQL functions (hour, dayofweek, dayofmonth, month, when, lit, coalesce). Behavioural features such as rolling transaction counts use placeholder values in the current implementation; in production these would be served from a feature store or computed via Spark window functions.

Distributed Inference — A @pandas_udf("int") is used for vectorised, Arrow-optimised batch prediction across Spark partitions. The trained XGBoost pipeline is broadcast to all executors via SparkContext.broadcast to avoid repeated serialisation overhead. The UDF reconstructs a pandas.DataFrame from the 12 feature series, calls predict_proba, and applies a 0.60 probability threshold to produce binary predictions.

Fraud Filtering & Output — Only records with prediction == 1 are selected. The result set is serialised as JSON (to_json(struct(*))) and written back to Kafka topic fraud_predictions using writeStream in update mode with a checkpoint directory for fault-tolerant exactly-once delivery.

5. Supporting Infrastructure
Service	Role	Port
PostgreSQL 16	Airflow metadata DB + MLflow backend store	5432
Redis 7.2	Celery task broker	6379
MinIO	S3-compatible artifact store for MLflow model registry	9000 / 9001
MLflow Server	Experiment tracking UI and model registry	5500
Airflow API Server	DAG management and monitoring UI	8080
Flower	Celery worker monitoring	5555
All services communicate over a dedicated Docker bridge network (fraud-detection). Health checks are defined for every critical service. The MinIO client container initialises the mlflow bucket on startup via wait-for-it.sh.

6. Key Design Decisions
F2 Score for Tuning: The hyperparameter search optimises recall over precision (β=2). In fraud detection, the cost of a false negative (missed fraud) materially exceeds the cost of a false positive (false alert), making this the correct objective.
SMOTE after split: Oversampling is applied inside the pipeline after the train/test split, ensuring the test set reflects the true class distribution and evaluation metrics are not inflated.
Broadcasted model in UDF: Broadcasting avoids deserialising the model once per task; it is deserialised once per executor, which is the correct pattern for Spark UDF inference.
Threshold decoupled from model: The decision threshold is derived post-hoc from the precision-recall curve rather than hardcoded, allowing it to be re-optimised without retraining.
Kafka as the integration layer: Both the training and inference services consume from the same topic independently. This decouples their lifecycles — training does not block inference, and the inference service does not need to be restarted when a new model is trained.