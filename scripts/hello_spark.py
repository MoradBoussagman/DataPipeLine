from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("HelloSpark").getOrCreate()

    data = [("Hello", 1), ("World", 2), ("from", 3), ("Spark", 4)]
    df = spark.createDataFrame(data, ["word", "count"])

    print("=" * 50)
    print("HELLO WORLD FROM SPARK CLUSTER")
    print("=" * 50)

    df.show()

    spark.stop()
    print("Spark job completed successfully!")

if __name__ == "__main__":
    main()
