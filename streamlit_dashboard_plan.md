# Streamlit Fraud Detection Dashboard — Planning Guide

> Based on: Fraud Transaction Detection system with Kafka, Spark, XGBoost, Airflow, MLflow, PostgreSQL

---

## 1. Core Pages / Sections

### 🏠 Page 1 — Overview (Landing Page)
The "fraud manager's morning view." First thing opened every day.

- Total transactions processed today
- Total fraud detected today + total $ value intercepted
- Current fraud rate % vs yesterday (delta indicator)
- Model's current Precision / Recall / F1 (pulled from MLflow)
- System health indicators — Kafka, Spark, Airflow shown as green/red status badges

---

### 🚨 Page 2 — Live Fraud Feed
Directly consuming from your `fraud_predictions` Kafka topic.

- Auto-refreshing table of flagged transactions (every N seconds)
- Columns: Transaction ID, User ID, Amount, Merchant, Location, Fraud Pattern, Confidence Score, Timestamp
- Rows color-coded by fraud pattern type
- Clicking a row opens the User Case View for that user

---

### 📊 Page 3 — Pattern Analytics
Your four fraud patterns visualised over time.

- Donut chart: breakdown of Account Takeover vs Card Testing vs Merchant Collusion vs Geographic Anomaly today
- Time series line chart: how each pattern trends across the day / week
- Top flagged merchants table (for Merchant Collusion pattern)
- Geographic heatmap by country code (for Geographic Anomaly pattern — RU, CN, NG, GB)

---

### 🤖 Page 4 — Model Health
Pulled directly from your MLflow backend at port 5500.

- Current model version + last trained timestamp (your 03:00 UTC Airflow DAG)
- AUC-PR, Precision, Recall, F1 trend across last N training runs (line chart)
- Current decision threshold — 0.60 inference threshold vs optimal threshold from training
- Confusion matrix image (logged to MLflow as PNG)
- Precision-Recall curve image (logged to MLflow as PNG)

---

### 👤 Page 5 — User Case View
Opened when an analyst clicks a flagged user. The investigation screen.

- That user's full transaction history table
- `amount_to_avg_ratio` trend over time (line chart)
- Rolling 24h and 7d transaction counts
- Total number of times this user has been flagged previously
- This mirrors what a real fraud analyst reviews before blocking a card

---

### ⚙️ Page 6 — Pipeline Monitoring
Operational health of all your backend services.

- Last Airflow DAG run: success/fail status + duration
- Kafka consumer lag on `fraud_predictions` topic
- Spark Structured Streaming throughput (records/sec)
- MinIO storage usage for MLflow artifacts

---

## 2. Directory Structure

```
your-project/
├── src/
│   ├── producer/
│   ├── inference/
│   ├── dags/
│   └── dashboard/                    ← new folder
│       ├── app.py                        # main entry point, sidebar navigation
│       ├── pages/
│       │   ├── 1_overview.py             # KPI tiles, system health
│       │   ├── 2_live_feed.py            # real-time fraud table
│       │   ├── 3_pattern_analytics.py    # charts, heatmap, pattern breakdown
│       │   ├── 4_model_health.py         # MLflow metrics, curves
│       │   └── 5_user_case_view.py       # per-user investigation view
│       ├── components/
│       │   ├── metrics_cards.py          # reusable KPI tile widgets
│       │   ├── fraud_table.py            # reusable styled fraud table
│       │   └── charts.py                 # reusable Plotly chart functions
│       ├── data/
│       │   ├── db.py                     # PostgreSQL connection + all queries
│       │   ├── kafka_consumer.py         # reads fraud_predictions topic
│       │   └── mlflow_client.py          # pulls metrics + artifacts from MLflow
│       ├── config.py                 # DB URLs, Kafka config, thresholds — reads .env
│       └── requirements.txt          # all Python dependencies
├── docker-compose.yml            ← add dashboard service here
└── .env                          ← already exists, reused by config.py
```

---

## 3. What Each File Does

### Entry & Navigation

| File | Purpose |
|---|---|
| `app.py` | Streamlit entry point. Sets page config, sidebar navigation, app title and theme. Streamlit's multi-page routing is built-in — files inside `pages/` become nav items automatically by their filename. |
| `config.py` | Single source of truth for all connection strings — Postgres URL, Kafka broker address, MLflow tracking URI, refresh intervals. Reads from your existing `.env` file so nothing is duplicated. |

---

### Pages

| File | Pulls Data From | What It Shows |
|---|---|---|
| `1_overview.py` | `db.py` + `mlflow_client.py` | KPI metric tiles, fraud rate delta, model F1, service health badges |
| `2_live_feed.py` | `kafka_consumer.py` + `db.py` | Auto-refreshing fraud alert table with confidence scores and pattern labels |
| `3_pattern_analytics.py` | `db.py` | Donut chart, time series per pattern, merchant table, country heatmap |
| `4_model_health.py` | `mlflow_client.py` | Training run history chart, PR curve PNG, confusion matrix PNG, threshold comparison |
| `5_user_case_view.py` | `db.py` | User transaction history, amount ratio trend, rolling counts, flag history |

---

### Data Layer

| File | Purpose |
|---|---|
| `db.py` | All PostgreSQL queries in one place — fraud counts, user history, pattern aggregations, pipeline run logs. Uses `psycopg2` or `SQLAlchemy`. Your Airflow metadata and MLflow backend store already live in this same Postgres instance. |
| `kafka_consumer.py` | Polls the `fraud_predictions` Kafka topic for the live feed page. Uses the same `kafka-python` library already used in your producer service — no new dependencies. |
| `mlflow_client.py` | Calls MLflow's REST API (port 5500 in your stack) to fetch the latest run metrics, registered model version, and artifact URLs for the PR curve and confusion matrix PNGs. |

---

### Components (Reusable Widgets)

| File | Purpose |
|---|---|
| `metrics_cards.py` | Wraps `st.metric()` into a consistent function so every page renders KPI tiles the same way. Changing the style here updates all pages at once. |
| `fraud_table.py` | The color-coded styled dataframe with fraud pattern labels. Used in both the Live Feed page and the User Case View so they stay consistent. |
| `charts.py` | All Plotly chart functions centralised — donut, time series, geographic heatmap, bar charts. Changing a chart style here updates everywhere it appears. |

---

### Infrastructure

| File | Purpose |
|---|---|
| `requirements.txt` | Python dependencies: `streamlit`, `psycopg2-binary`, `kafka-python`, `mlflow`, `plotly`, `pandas`, `sqlalchemy` |
| `docker-compose.yml` addition | Add a `dashboard` service pointing at `src/dashboard/`, expose port `8501`, connect to your existing `fraud-detection` Docker bridge network so it can reach Postgres (5432), Kafka, and MLflow (5500) without any extra configuration. |

---

## 4. Build Order

Build in this sequence — each step produces something visible or unblocks the next step.

| Step | File | Why This Order |
|---|---|---|
| 1 | `config.py` | Foundation — everything else imports from here |
| 2 | `data/db.py` | Most pages depend on Postgres queries |
| 3 | `app.py` | Gets the app running in the browser immediately |
| 4 | `pages/1_overview.py` | First visible result — confirms DB connection works |
| 5 | `data/kafka_consumer.py` | Required before live feed can work |
| 6 | `pages/2_live_feed.py` | Highest business value — real-time fraud alerts |
| 7 | `data/mlflow_client.py` | Required before model health page |
| 8 | `pages/4_model_health.py` | Pulls from MLflow, no new DB queries needed |
| 9 | `pages/3_pattern_analytics.py` | More complex DB aggregations, build after basics work |
| 10 | `components/` (all three) | Refactor repeated code from pages 1–4 into reusable components |
| 11 | `pages/5_user_case_view.py` | Needs fraud table component and DB queries from earlier steps |
| 12 | `docker-compose.yml` addition | Containerise last, once all code is verified working locally |

---

## 5. Quick Reference — Data Sources per Feature

| Dashboard Feature | Data Source | Table / Endpoint |
|---|---|---|
| Transaction counts, fraud rate | PostgreSQL | Airflow metadata or a predictions sink table |
| Fraud pattern breakdown | PostgreSQL | Aggregated from `fraud_predictions` sink |
| Live fraud alerts | Kafka | `fraud_predictions` topic |
| User transaction history | PostgreSQL | Predictions sink, filtered by `user_id` |
| Model F1 / AUC-PR / Precision / Recall | MLflow REST API | `/api/2.0/mlflow/runs/search` |
| PR curve + confusion matrix images | MLflow REST API | `/api/2.0/mlflow/artifacts/list` |
| Airflow DAG run status | PostgreSQL | `dag_run` table in Airflow metadata DB |
| Kafka consumer lag | Kafka Admin API | `kafka-python` AdminClient |

---

## 6. One Extra Recommendation — Kafka → Postgres Sink

Your `fraud_predictions` Kafka topic is the richest data source but neither Streamlit nor any BI tool reads Kafka directly in a query-friendly way.

**Add a Kafka Connect JDBC Sink** to write every prediction into a Postgres table:

```
fraud_predictions (Kafka topic)
        ↓
  Kafka Connect JDBC Sink
        ↓
  fraud_predictions (Postgres table)
        ↓
  db.py queries everything from here
```

This is a small addition to `docker-compose.yml` and unlocks clean SQL queries for every page in the dashboard — and also makes Power BI / Tableau connections trivial if needed later.
