import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

from config import (
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    POSTGRES_USER, POSTGRES_PASSWORD,
)

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def _conn():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
        connect_timeout=5,
    )


def _query(sql: str, params=None) -> pd.DataFrame:
    try:
        with _conn() as conn:
            return pd.read_sql(sql, conn, params=params)
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Pattern classification (mirrors producer fraud patterns)
# ---------------------------------------------------------------------------

PATTERN_SQL = """
    CASE
        WHEN amount < 2                                      THEN 'Card Testing'
        WHEN is_high_risk_merchant = 1 AND amount > 3000     THEN 'Merchant Collusion'
        WHEN location IN ('RU','CN','NG','GB')               THEN 'Geographic Anomaly'
        WHEN amount > 500                                    THEN 'Account Takeover'
        ELSE 'Other'
    END
"""

# ---------------------------------------------------------------------------
# Overview page
# ---------------------------------------------------------------------------

def get_overview_stats() -> dict:
    sql = f"""
        SELECT
            COUNT(*)                                            AS total_today,
            COALESCE(SUM(amount), 0)                           AS value_intercepted,
            ROUND(100.0 * COUNT(*) /
                NULLIF((
                    SELECT COUNT(*) FROM fraud_predictions
                    WHERE detected_at >= CURRENT_DATE
                ), 0), 2)                                      AS fraud_rate_today,
            ROUND(100.0 * COUNT(*) /
                NULLIF((
                    SELECT COUNT(*) FROM fraud_predictions
                    WHERE detected_at >= CURRENT_DATE - INTERVAL '1 day'
                      AND detected_at <  CURRENT_DATE
                ), 0), 2)                                      AS fraud_rate_yesterday
        FROM fraud_predictions
        WHERE detected_at >= CURRENT_DATE
    """
    row = _query(sql)
    if row.empty:
        return {'total_today': 0, 'value_intercepted': 0.0,
                'fraud_rate_today': 0.0, 'fraud_rate_yesterday': 0.0}
    return row.iloc[0].to_dict()


def get_hourly_counts(days: int = 1) -> pd.DataFrame:
    sql = """
        SELECT
            DATE_TRUNC('hour', detected_at) AS hour,
            COUNT(*)                        AS fraud_count
        FROM fraud_predictions
        WHERE detected_at >= NOW() - INTERVAL '1 day' * %(days)s
        GROUP BY 1
        ORDER BY 1
    """
    return _query(sql, {'days': days})


# ---------------------------------------------------------------------------
# Live feed page
# ---------------------------------------------------------------------------

def get_recent_predictions(limit: int = 100) -> pd.DataFrame:
    sql = f"""
        SELECT
            transaction_id,
            user_id,
            amount,
            currency,
            merchant,
            location,
            fraud_probability,
            {PATTERN_SQL} AS fraud_pattern,
            detected_at
        FROM fraud_predictions
        ORDER BY detected_at DESC
        LIMIT %(limit)s
    """
    return _query(sql, {'limit': limit})


# ---------------------------------------------------------------------------
# Pattern analytics page
# ---------------------------------------------------------------------------

def get_pattern_breakdown(days: int = 1) -> pd.DataFrame:
    sql = f"""
        SELECT
            {PATTERN_SQL} AS fraud_pattern,
            COUNT(*)       AS count,
            SUM(amount)    AS total_amount
        FROM fraud_predictions
        WHERE detected_at >= NOW() - INTERVAL '1 day' * %(days)s
        GROUP BY 1
        ORDER BY 2 DESC
    """
    return _query(sql, {'days': days})


def get_pattern_timeseries(days: int = 7) -> pd.DataFrame:
    sql = f"""
        SELECT
            DATE_TRUNC('hour', detected_at) AS hour,
            {PATTERN_SQL}                   AS fraud_pattern,
            COUNT(*)                        AS count
        FROM fraud_predictions
        WHERE detected_at >= NOW() - INTERVAL '1 day' * %(days)s
        GROUP BY 1, 2
        ORDER BY 1
    """
    return _query(sql, {'days': days})


def get_top_merchants(limit: int = 10) -> pd.DataFrame:
    sql = """
        SELECT
            merchant,
            COUNT(*)    AS fraud_count,
            SUM(amount) AS total_amount,
            ROUND(AVG(fraud_probability)::numeric, 3) AS avg_confidence
        FROM fraud_predictions
        WHERE detected_at >= NOW() - INTERVAL '7 days'
        GROUP BY merchant
        ORDER BY fraud_count DESC
        LIMIT %(limit)s
    """
    return _query(sql, {'limit': limit})


def get_country_breakdown() -> pd.DataFrame:
    sql = """
        SELECT
            location,
            COUNT(*)    AS fraud_count,
            SUM(amount) AS total_amount
        FROM fraud_predictions
        WHERE detected_at >= NOW() - INTERVAL '7 days'
        GROUP BY location
        ORDER BY fraud_count DESC
    """
    return _query(sql)


# ---------------------------------------------------------------------------
# User case view page
# ---------------------------------------------------------------------------

def get_user_history(user_id: int) -> pd.DataFrame:
    sql = f"""
        SELECT
            transaction_id,
            amount,
            currency,
            merchant,
            location,
            fraud_probability,
            {PATTERN_SQL} AS fraud_pattern,
            amount_to_avg_ratio,
            transaction_count_last_24h,
            transaction_count_last_7d,
            detected_at
        FROM fraud_predictions
        WHERE user_id = %(user_id)s
        ORDER BY detected_at DESC
    """
    return _query(sql, {'user_id': user_id})


def get_user_flag_counts(top_n: int = 20) -> pd.DataFrame:
    sql = """
        SELECT user_id, COUNT(*) AS times_flagged, SUM(amount) AS total_amount
        FROM fraud_predictions
        GROUP BY user_id
        ORDER BY times_flagged DESC
        LIMIT %(top_n)s
    """
    return _query(sql, {'top_n': top_n})


# ---------------------------------------------------------------------------
# Pipeline monitoring
# ---------------------------------------------------------------------------

def get_airflow_last_run() -> pd.DataFrame:
    """Reads from the Airflow metadata database (same Postgres instance, different DB)."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT, dbname='airflow',
            user='airflow', password='airflow', connect_timeout=5,
        )
        sql = """
            SELECT dag_id, state, start_date, end_date,
                   EXTRACT(EPOCH FROM (end_date - start_date)) AS duration_sec
            FROM dag_run
            WHERE dag_id = 'fraud_detection_training'
            ORDER BY start_date DESC
            LIMIT 10
        """
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()
