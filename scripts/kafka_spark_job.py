from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
    # Don't configure packages here - let spark-submit handle it via --packages
    spark = SparkSession.builder \
        .appName("kafka_spark_job") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    print("=" * 60)
    print("KAFKA + SPARK INTEGRATION JOB STARTED")
    print("=" * 60)

    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True)
    ])

    # Read from Kafka
    kafka_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:9092") \
        .option("subscribe", "input-topic") \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), schema).alias("data")) \
        .select("data.*")

    transformed_df = parsed_df.filter(col("age") >= 18) \
        .withColumn("message", col("name") + " from " + col("city") + " is an adult")

    kafka_output = transformed_df.select(
        to_json(struct("name", "age", "city", "message")).alias("value")
    )

    kafka_output.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:9092") \
        .option("topic", "output-topic") \
        .save()

    input_count = kafka_df.count()
    output_count = transformed_df.count()
    
    print("=" * 60)
    print(f"STATISTICS: input={input_count}, output={output_count}, filtered={input_count - output_count}")
    print("=" * 60)

    spark.stop()
    print("✓ Kafka-Spark job completed successfully!")

if __name__ == "__main__":
    main()