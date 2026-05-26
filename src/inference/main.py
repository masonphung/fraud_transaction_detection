# Standard library imports
import logging
import os

# Third-party imports
import joblib
import psycopg2
from psycopg2.extras import execute_values
import yaml
from dotenv import load_dotenv

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (from_json, col, hour, dayofmonth,
                                  dayofweek, month, when, lit, coalesce)
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import (StructType, StructField, StringType,
                              IntegerType, DoubleType, TimestampType)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger= logging.getLogger(__name__)

class FraudDetectionInference:
    """
    Fraud detection inference pipeline class that handles:
    - Configuration loading
    - Spark session management
    - Kafka stream processing
    - Feature engineering
    - Model inference
    - Results publishing

    Attributes:
        config (dict): Pipeline configuration parameters
        spark (SparkSession): Spark session instance
        model: Loaded ML model for fraud detection
        broadcast_model: Model broadcast to Spark workers for distributed inference
    """
    bootstrap_servers = None
    topic = None
    security_protocol = None
    sasl_mechanism = None
    username = None
    password = None
    sasl_jaa_config = None

    def __init__(self, config_path='/app/config.yaml'):
        """Initialize pipeline with configuration and dependencies

        Args:
            config_path (str): Path to YAML configuration file
        """
        load_dotenv(dotenv_path='/app/.env')
        self.config = self._load_config(config_path)
        self.spark = self._init_spark_session()
        self.model = self._load_model(self.config['model']['path'])
        self.broadcast_model = self.spark.sparkContext.broadcast(self.model)

        pg = self.config.get('postgres', {})
        self.pg_host = pg.get('host', 'postgres')
        self.pg_port = pg.get('port', 5432)
        self.pg_db = pg.get('database', 'fraud_detection')
        self.pg_user = pg.get('user', 'fraud_detection')
        self.pg_password = pg.get('password', 'fraud_detection')

        self._create_predictions_table()

        # Debug: Log loaded environment variables for verification (avoid logging sensitive info in production)
        logger.debug('Environment variables loaded: %s', dict(os.environ))
    
    @staticmethod
    def _load_config(config_path):
        """
        Loads config
        """
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f'Error loading config: {str(e)}')
            
    def _init_spark_session(self):
        """
        Initialize Spark session with Kafka dependencies
        
        Returns:
            Configurated Spark session
            
        Raises:
            Exception: If Spark initialization fails
        """
        try:
            packages = self.config.get('spark', {}).get('packages', '')
            builder = SparkSession.builder.appName(self.config.get('spark').get('app_name', 'FraudDetectionInference'))
            if packages:
                builder = builder.config('spark.jars.packages', packages)
            spark = builder.getOrCreate()
            logger.info('Spark Session Initialized')
            return spark
            
        except Exception as e:
            logger.error('Error initializing spark session: %s', str(e))
            
    def _load_model(self, model_path):
        """Load pre-trained fraud detection model from disk

        Args:
            model_path (str): Path to serialized model file

        Returns:
            model: Loaded ML model

        Raises:
            Exception: If model loading fails
        """
        try:
            model = joblib.load(model_path)
            logger.info("Model loaded from %s", model_path)
            return model
        except Exception as e:
            logger.error("Error loading model: %s", str(e))
            raise
        
    def _pg_conn(self):
        return psycopg2.connect(
            host=self.pg_host, port=self.pg_port, dbname=self.pg_db,
            user=self.pg_user, password=self.pg_password
        )

    def _create_predictions_table(self):
        ddl = """
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
        """
        try:
            conn = self._pg_conn()
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()
            conn.close()
            logger.info('fraud_predictions table ready')
        except Exception as e:
            logger.error('Failed to create predictions table: %s', str(e))

    def _write_to_postgres(self, df):
        rows = df.select(
            'transaction_id', 'user_id', 'amount', 'currency', 'merchant',
            'timestamp', 'location', 'transaction_hour', 'day_of_week',
            'transaction_day', 'transaction_month', 'is_night', 'is_weekend',
            'transaction_count_last_24h', 'time_since_last_transaction',
            'transaction_count_last_7d', 'amount_to_avg_ratio',
            'is_high_risk_merchant', 'fraud_probability', 'prediction'
        ).collect()
        if not rows:
            return
        try:
            conn = self._pg_conn()
            with conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO fraud_predictions (
                        transaction_id, user_id, amount, currency, merchant,
                        timestamp, location, transaction_hour, day_of_week,
                        transaction_day, transaction_month, is_night, is_weekend,
                        transaction_count_last_24h, time_since_last_transaction,
                        transaction_count_last_7d, amount_to_avg_ratio,
                        is_high_risk_merchant, fraud_probability, prediction
                    ) VALUES %s
                    ON CONFLICT (transaction_id) DO NOTHING
                """, [tuple(r) for r in rows])
            conn.commit()
            conn.close()
            logger.info('Wrote %d fraud predictions to PostgreSQL', len(rows))
        except Exception as e:
            logger.error('PostgreSQL write failed: %s', str(e))

    def read_from_kafka(self):
        logger.info('Reading data from kafka topic %s', self.config['kafka']['topic'])
        kafka_bootstrap_server = self.config['kafka']['bootstrap_servers']
        kafka_topic = self.config['kafka']['topic']
        kafka_security_protocol = self.config['kafka'].get('security_protocol', 'SASL_SSL')
        kafka_sasl_mechanism = self.config['kafka'].get('sasl_mechanism', 'PLAIN')
        kafka_username = self.config['kafka'].get('username')
        kafka_password = self.config['kafka'].get('password')
        kafka_sasl_jaas_config = (
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_username}" password="{kafka_password}";'
        )
        
        self.bootstrap_servers = kafka_bootstrap_server
        self.topic = kafka_topic
        self.security_protocol = kafka_security_protocol
        self.sasl_mechanism = kafka_sasl_mechanism
        self.username = kafka_username
        self.password = kafka_password
        self.sasl_jaa_config = kafka_sasl_jaas_config
            
        df = (self.spark.readStream
              .format('kafka')
              .option('kafka.bootstrap.servers', kafka_bootstrap_server)
              .option('subscribe', kafka_topic)
              .option('startingOffsets', 'latest')
              .option('kafka.security.protocol', kafka_security_protocol)
              .option('kafka.sasl.mechanism', kafka_sasl_mechanism)
              .option('kafka.sasl.jaas.config', kafka_sasl_jaas_config)
              .load()
              )
        
        # Define schema for incoming JSON transaction data
        json_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("merchant", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
        ])

        # Create streaming DataFrame from Kafka source
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
            .option("subscribe", kafka_topic) \
            .option("startingOffsets", "latest") \
            .option("kafka.security.protocol", kafka_security_protocol) \
            .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
            .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) \
            .load()

        # Parse JSON payload using defined schema
        parsed_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), json_schema).alias("data")) \
            .select("data.*")

        return parsed_df
    
    def add_features(self, df):
        """
        Add engineered features to the DataFrame for model inference
        
        Params:
            df (DataFrame): Input DataFrame with raw transaction data
            
        Returns:
            DataFrame with added features ready for model inference
        """
        # ---- Temporal features ----
        df = df.withColumn("transaction_hour", hour(col("timestamp")))
        df = df.withColumn("day_of_week", dayofweek(col("timestamp")))
        df = df.withColumn("transaction_day", dayofmonth(col("timestamp")))
        df = df.withColumn("transaction_month", month(col("timestamp")))
        df = df.withColumn("is_night",
                           when((col("transaction_hour") >= 22) | (col("transaction_hour") < 5), 1).otherwise(0))
        df = df.withColumn("is_weekend",
                           when((dayofweek(col("timestamp")) == 1) | (dayofweek(col("timestamp")) == 7),
                                1).otherwise(0))
        
        # ---- Behavioral features ----
        # Transaction pattern features (placeholders - would normally come from historical data)
        # In production, these would be calculated using window functions or join with historical data
        df = df.withColumn("transaction_count_last_24h", lit(5))  # Placeholder value
        df = df.withColumn("time_since_last_transaction", lit(0.0))  # Placeholder value
        df = df.withColumn("transaction_count_last_7d", lit(1000.0))  # Placeholder value

        # ---- Monetary features ----
        # Ratio features to capture transaction amount patterns
        df = df.withColumn("amount_to_avg_ratio", col("amount") / col("transaction_count_last_7d"))
        df = df.withColumn("amount_to_avg_ratio", coalesce(col("amount_to_avg_ratio"), lit(1.0)))

        # ---- Merchant risk features ----
        # Merchant risk features from configurable list of high-risk merchants
        high_risk_merchants = self.config.get('high_risk_merchants', ['QuickCash', 'GlobalDigital', 'FastMoneyX'])
        df = df.withColumn("is_high_risk_merchant", col("merchant").isin(high_risk_merchants).cast("int"))

        # Debug: Output schema of processed data for verification
        df.printSchema()
        return df

    def run_inference(self):
        """
        Main pipeline execution flow: process stream and run predictions"""
        # Local import for Spark executor compatibility
        import pandas as pd
        
        # Process streaming data from Kafka
        df = self.read_from_kafka()
        
        # Define watermark to handle late-arriving data (24 hour tolerance)
        df = df.withWatermark("timestamp", "24 hours")
        
        # Add engineered features to raw data
        feature_df = self.add_features(df)
        
        # Get broadcasted model reference for use in UDF
        broadcast_model = self.broadcast_model

        inference_schema = StructType([
            StructField("fraud_probability", DoubleType()),
            StructField("prediction", IntegerType()),
        ])

        @pandas_udf(inference_schema)
        def predict_udf(
                amount: pd.Series,
                transaction_hour: pd.Series,
                day_of_week: pd.Series,
                transaction_day: pd.Series,
                transaction_month: pd.Series,
                is_night: pd.Series,
                is_weekend: pd.Series,
                transaction_count_last_24h: pd.Series,
                time_since_last_transaction: pd.Series,
                transaction_count_last_7d: pd.Series,
                amount_to_avg_ratio: pd.Series,
                is_high_risk_merchant: pd.Series,
        ) -> pd.DataFrame:
            input_df = pd.DataFrame({
                "amount": amount,
                "transaction_hour": transaction_hour,
                "day_of_week": day_of_week,
                "transaction_day": transaction_day,
                "transaction_month": transaction_month,
                "is_night": is_night,
                "is_weekend": is_weekend,
                "transaction_count_last_24h": transaction_count_last_24h,
                "time_since_last_transaction": time_since_last_transaction,
                "transaction_count_last_7d": transaction_count_last_7d,
                "amount_to_avg_ratio": amount_to_avg_ratio,
                "is_high_risk_merchant": is_high_risk_merchant,
            })
            probabilities = broadcast_model.value.predict_proba(input_df)[:, 1]
            return pd.DataFrame({
                "fraud_probability": probabilities,
                "prediction": (probabilities >= 0.60).astype(int),
            })

        feature_cols = [
            "amount", "transaction_hour", "day_of_week", "transaction_day", "transaction_month",
            "is_night", "is_weekend", "transaction_count_last_24h", "time_since_last_transaction",
            "transaction_count_last_7d", "amount_to_avg_ratio", "is_high_risk_merchant"
        ]
        result = feature_df.withColumn("_inf", predict_udf(*[col(f) for f in feature_cols]))
        prediction_df = (result
                         .withColumn("fraud_probability", col("_inf.fraud_probability"))
                         .withColumn("prediction", col("_inf.prediction"))
                         .drop("_inf"))

        bootstrap_servers = self.bootstrap_servers
        security_protocol = self.security_protocol
        sasl_mechanism = self.sasl_mechanism
        sasl_jaas_config = self.sasl_jaa_config
        write_to_postgres = self._write_to_postgres

        def write_batch(batch_df, _epoch_id):
            fraud_df = batch_df.filter(col("prediction") == 1)
            if fraud_df.isEmpty():
                return
            # Sink 1 — Kafka
            (fraud_df.selectExpr("CAST(transaction_id AS STRING) AS key", "to_json(struct(*)) AS value")
             .write.format("kafka")
             .option("kafka.bootstrap.servers", bootstrap_servers)
             .option("topic", "fraud_predictions")
             .option("kafka.security.protocol", security_protocol)
             .option("kafka.sasl.mechanism", sasl_mechanism)
             .option("kafka.sasl.jaas.config", sasl_jaas_config)
             .save())
            # Sink 2 — PostgreSQL
            write_to_postgres(fraud_df)

        (prediction_df.writeStream
         .foreachBatch(write_batch)
         .option("checkpointLocation", "checkpoints/checkpoint")
         .start()
         .awaitTermination())
        
if __name__ == "__main__":
    # Initialize pipeline with configuration
    inference = FraudDetectionInference('/app/config.yaml')
    inference.run_inference()