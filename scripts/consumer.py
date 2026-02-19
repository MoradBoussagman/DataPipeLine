from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

KAFKA_BROKER    = "broker:9092"
RAW_TOPIC       = "raw_data"
PROCESSED_TOPIC = "processed_data"
ANOMALY_TOPIC   = "anomalies"

THRESHOLDS = {
    "pm1":         25.0,
    "pm10":        30.0,
    "pm25":        40.0,
    "rh":          95.0,
    "temperature": 50.0,   # Marrakech
    "pm003":       5000.0,
}

SCHEMA = StructType([
    StructField("measured_at", StringType()),
    StructField("pm1",         FloatType()),
    StructField("pm10",        FloatType()),
    StructField("pm25",        FloatType()),
    StructField("rh",          FloatType()),
    StructField("temperature",FloatType()),
    StructField("pm003",       FloatType()),
])

spark = SparkSession.builder.appName("AirQuality_Consumer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── SAFE KAFKA READ ───────────────────────────────────────
raw_df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", RAW_TOPIC)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

parsed = (
    raw_df
    .selectExpr("CAST(value AS STRING)")
    .select(F.from_json("value", SCHEMA).alias("d"))
    .select("d.*")
)

if parsed.rdd.isEmpty():
    print("✓ No Kafka data")
    spark.stop()
    exit(0)

# ✅ TAKE ONLY THE LAST MESSAGE
latest = parsed.orderBy(F.col("measured_at").desc()).limit(1)

# ── anomaly condition ─────────────────────────────────────
cond = (
    (F.col("pm1")         >= THRESHOLDS["pm1"]) |
    (F.col("pm10")        >= THRESHOLDS["pm10"]) |
    (F.col("pm25")        >= THRESHOLDS["pm25"]) |
    (F.col("rh")          >= THRESHOLDS["rh"]) |
    (F.col("temperature") >= THRESHOLDS["temperature"]) |
    (F.col("pm003")       >= THRESHOLDS["pm003"])
)

good = latest.filter(~cond)
bad  = latest.filter(cond)

# ── write outputs ─────────────────────────────────────────
if not good.rdd.isEmpty():
    good.select(F.to_json(F.struct("*")).alias("value")) \
        .write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", PROCESSED_TOPIC) \
        .save()

if not bad.rdd.isEmpty():
    bad.select(F.to_json(F.struct("*")).alias("value")) \
        .write.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", ANOMALY_TOPIC) \
        .save()

print("✓ 1 row processed")
spark.stop()
