"""
Spark Batch Consumer - Air Quality Pipeline
Triggered by Airflow SparkSubmitOperator after each fetch
Reads from Kafka: raw_data (latest batch)
Writes to:
  - PostgreSQL: raw_readings, processed_metrics, anomalies
  - Kafka:      processed_data, anomalies
Then EXITS (not streaming forever)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, IntegerType
)

# ─── CONFIG ────────────────────────────────────────────────────────────────────
KAFKA_BROKER    = "broker:9092"
RAW_TOPIC       = "raw_data"
PROCESSED_TOPIC = "processed_data"
ANOMALY_TOPIC   = "anomalies"

PG_URL   = "jdbc:postgresql://postgres:5432/airflow"
PG_PROPS = {
    "user":     "airflow",
    "password": "airflow",
    "driver":   "org.postgresql.Driver"
}

THRESHOLDS = {
    "pm25": {"warning": 15.0,    "danger": 35.0},
    "pm10": {"warning": 45.0,    "danger": 75.0},
    "no2":  {"warning": 25.0,    "danger": 50.0},
    "o3":   {"warning": 60.0,    "danger": 100.0},
    "co":   {"warning": 4000.0,  "danger": 10000.0},
    "so2":  {"warning": 40.0,    "danger": 100.0},
}

# ─── SPARK SESSION ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("AirQuality_BatchProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ─── SCHEMA ────────────────────────────────────────────────────────────────────
SCHEMA = StructType([
    StructField("sensor_id",    IntegerType(), True),
    StructField("location_id",  IntegerType(), True),
    StructField("station_name", StringType(),  True),
    StructField("parameter",    StringType(),  True),
    StructField("display_name", StringType(),  True),
    StructField("units",        StringType(),  True),
    StructField("value",        FloatType(),   True),
    StructField("fetched_at",   StringType(),  True),
])

# ─── READ FROM KAFKA (batch, not streaming) ────────────────────────────────────
raw_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", RAW_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# ─── PARSE + CLEAN ─────────────────────────────────────────────────────────────
parsed = raw_df \
    .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("d")) \
    .select("d.*") \
    .withColumn("fetched_at", F.to_timestamp("fetched_at")) \
    .filter(F.col("value").isNotNull()) \
    .filter(F.col("value") >= 0) \
    .filter(F.col("parameter").isNotNull())

total = parsed.count()
print(f"✓ Read and cleaned {total} records from Kafka")

if total == 0:
    print("No data to process, exiting.")
    spark.stop()
    exit(0)

# ─── 1. STORE RAW READINGS ─────────────────────────────────────────────────────
parsed.select(
    "sensor_id", "location_id", "station_name",
    "parameter", "units", "value",
    F.current_timestamp().alias("ingested_at")
).write.jdbc(url=PG_URL, table="raw_readings", mode="append", properties=PG_PROPS)

print(f"✓ Stored {total} raw readings to PostgreSQL")

# ─── 2. COMPUTE METRICS ────────────────────────────────────────────────────────
metrics = parsed.groupBy("sensor_id", "location_id", "parameter") \
    .agg(
        F.round(F.avg("value"), 4).alias("avg_value"),
        F.round(F.min("value"), 4).alias("min_value"),
        F.round(F.max("value"), 4).alias("max_value"),
        F.count("value").alias("reading_count"),
        F.min("fetched_at").alias("window_start"),
        F.max("fetched_at").alias("window_end"),
    ) \
    .withColumn("processed_at", F.current_timestamp())

# Write to PostgreSQL
metrics.write.jdbc(url=PG_URL, table="processed_metrics", mode="append", properties=PG_PROPS)

# Publish to Kafka processed_data topic
metrics.select(F.to_json(F.struct("*")).alias("value")) \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", PROCESSED_TOPIC) \
    .save()

print(f"✓ Metrics computed and stored for {metrics.count()} sensors")

# ─── 3. DETECT ANOMALIES ───────────────────────────────────────────────────────
anomaly_type_expr = F.lit(None).cast(StringType())
threshold_expr    = F.lit(None).cast(FloatType())

for param, levels in THRESHOLDS.items():
    anomaly_type_expr = F.when(
        (F.lower(F.col("parameter")) == param) & (F.col("value") >= levels["danger"]),
        F.lit(f"DANGER_{param.upper()}")
    ).when(
        (F.lower(F.col("parameter")) == param) & (F.col("value") >= levels["warning"]),
        F.lit(f"WARNING_{param.upper()}")
    ).otherwise(anomaly_type_expr)

    threshold_expr = F.when(
        (F.lower(F.col("parameter")) == param) & (F.col("value") >= levels["danger"]),
        F.lit(levels["danger"])
    ).when(
        (F.lower(F.col("parameter")) == param) & (F.col("value") >= levels["warning"]),
        F.lit(levels["warning"])
    ).otherwise(threshold_expr)

anomalies = parsed \
    .withColumn("anomaly_type", anomaly_type_expr) \
    .withColumn("threshold",    threshold_expr) \
    .filter(F.col("anomaly_type").isNotNull()) \
    .select(
        "sensor_id", "location_id", "parameter",
        "value", "threshold", "anomaly_type",
        F.current_timestamp().alias("detected_at")
    )

anomaly_count = anomalies.count()
if anomaly_count > 0:
    anomalies.write.jdbc(url=PG_URL, table="anomalies", mode="append", properties=PG_PROPS)
    anomalies.select(F.to_json(F.struct("*")).alias("value")) \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", ANOMALY_TOPIC) \
        .save()
    print(f"⚠ {anomaly_count} anomalies detected and stored!")
else:
    print("✓ No anomalies detected")

print("✓ Spark job completed successfully - exiting")
spark.stop()