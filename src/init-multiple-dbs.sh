#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER mlflow WITH PASSWORD 'mlflow';
    CREATE DATABASE mlflow;
    GRANT ALL PRIVILEGES ON DATABASE mlflow TO mlflow;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "mlflow" <<-EOSQL
    GRANT USAGE, CREATE ON SCHEMA public TO mlflow;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER fraud_detection WITH PASSWORD 'fraud_detection';
    CREATE DATABASE fraud_detection;
    GRANT ALL PRIVILEGES ON DATABASE fraud_detection TO fraud_detection;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "fraud_detection" <<-EOSQL
    GRANT USAGE, CREATE ON SCHEMA public TO fraud_detection;
    CREATE TABLE IF NOT EXISTS fraud_predictions (
        id                          SERIAL PRIMARY KEY,
        transaction_id              VARCHAR(255) UNIQUE,
        user_id                     INTEGER,
        amount                      DOUBLE PRECISION,
        currency                    VARCHAR(10),
        merchant                    VARCHAR(255),
        timestamp                   TIMESTAMPTZ,
        location                    VARCHAR(10),
        transaction_hour            INTEGER,
        day_of_week                 INTEGER,
        transaction_day             INTEGER,
        transaction_month           INTEGER,
        is_night                    INTEGER,
        is_weekend                  INTEGER,
        transaction_count_last_24h  DOUBLE PRECISION,
        time_since_last_transaction DOUBLE PRECISION,
        transaction_count_last_7d   DOUBLE PRECISION,
        amount_to_avg_ratio         DOUBLE PRECISION,
        is_high_risk_merchant       INTEGER,
        fraud_probability           DOUBLE PRECISION,
        prediction                  INTEGER,
        detected_at                 TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_fp_detected_at   ON fraud_predictions (detected_at DESC);
    CREATE INDEX IF NOT EXISTS idx_fp_user_id       ON fraud_predictions (user_id);
    CREATE INDEX IF NOT EXISTS idx_fp_location      ON fraud_predictions (location);
    CREATE INDEX IF NOT EXISTS idx_fp_merchant      ON fraud_predictions (merchant);
EOSQL