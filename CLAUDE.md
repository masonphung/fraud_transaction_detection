# Project Overview & Context

This is an end-to-end fraud detection project focus on predicting fraudulent credit card transactions.

This project implements a production-grade, end-to-end real-time fraud detection system built on an event-driven architecture. The system ingests a continuous stream of financial transactions, applies machine learning inference in real-time to flag fraudulent activity, and runs a scheduled retraining pipeline to keep the model current. All components are containerized and orchestrated via Docker Compose across 14 services.

# Who are you
You are a professional full-stack Data Scientist who help me building the pipeline, troubleshooting and working to improve the pipeline cycle + model performance.

# Data
- The data for the project is synthetized to match a credit card transaction data of a bank.
- The dataset is synthetized using the producer at `src/producer` and later enriched at `src/dags/fraud_detection_training.py`.
- The data dictionary at `/docs/data_dictionary.html`.

# Tech stack

| Layer | Technology |
|---|---|
|Event Streaming	| Apache Kafka (Confluent Cloud) |
|Stream Processing	| Apache Spark 3.5.4 (Structured Streaming) |
|ML Framework	| XGBoost + scikit-learn + imbalanced-learn |
|Training Orchestration | Apache Airflow 3.0.2 (CeleryExecutor) |
|Experiment Tracking | MLflow + MinIO (S3-compatible artifact store) |
|Task Broker | Redis |
|Metadata Store	| PostgreSQL 16 |
|Containerization | Docker / Docker Compose |

# End-to-end pipeline

- The pipeline architecture diagram can be founded at `docs/architecture.png`.
- Pipeline description:
    - **Data Production** (`/src/producer/`): Continuously generate synthetic credit card transactions and publishes them to Kafka topic `transactions`. The producer simulates 4 realistic fraud patterns including account takeover, card testing, merchant collusion and geographic anomaly with different generational weights.
    - **Model Training** (`/src/dags/`): At 3:00 UTC do sequential tasks: `validate_environment >> execute_training >> cleanup`.
        - Task 1 - validate_environment: Verifies that `config.yaml` and `.env` are present and readable before any compute resources are consumed.
        - Task 2 - execute_training: Ingest data from Kafka `transactions` topic, then do feature engineering, preprocessing, modelling with SMOTE and hyperparameter tuning. After that evaluate the model, log to mlflow and save the model for inference.
        - Task 3 - cleanup: Remove temporary .pkl to keep container storage clean.
    - **Real-time inference**: The inference service runs as a persistent Spark Structured Streaming job. It reads from the same transactions Kafka topic in real time and writes confirmed fraud predictions to fraud_predictions.
- Key Design Decisions:
    - F2 Score for Tuning: The hyperparameter search optimises recall over precision (β=2). In fraud detection, the cost of a false negative (missed fraud) materially exceeds the cost of a false positive (false alert), making this the correct objective.
    - SMOTE after split: Oversampling is applied inside the pipeline after the train/test split, ensuring the test set reflects the true class distribution and evaluation metrics are not inflated.
    - Broadcasted model in UDF: Broadcasting avoids deserialising the model once per task; it is deserialised once per executor, which is the correct pattern for Spark UDF inference.
    - Threshold decoupled from model: The decision threshold is derived post-hoc from the precision-recall curve rather than hardcoded, allowing it to be re-optimised without retraining.
    - Kafka as the integration layer: Both the training and inference services consume from the same topic independently. This decouples their lifecycles — training does not block inference, and the inference service does not need to be restarted when a new model is trained.

# Coding rules
- Always use random seed = 282 for reproducibility (Machine Learning only).