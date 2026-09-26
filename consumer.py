from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    count,
    struct,
    to_json,
    cast
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark = SparkSession.builder \
    .appName("AntiFraudKafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

kafka_raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "streams") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = kafka_raw_df \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

windowed_counts = parsed_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "30 seconds", "10 seconds"),
        col("track_id")
    ) \
    .agg(count("user_id").alias("stream_count"))

anomalies = windowed_counts \
    .filter(col("stream_count") > 10) \
    .select(
        col("track_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("stream_count")
    )

output_kafka_df = anomalies.select(
    col("track_id").cast("string").alias("key"),
    to_json(struct(
        col("track_id"),
        col("window_start").cast("string").alias("window_start"),
        col("window_end").cast("string").alias("window_end"),
        col("stream_count")
    )).alias("value")
)

query = output_kafka_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "alerts") \
    .option("checkpointLocation", "./chk_point") \
    .outputMode("update") \
    .start()

query.awaitTermination()