"""Spark Structured Streaming job that detects stream-farm attacks.

For every track and every 30-second window it computes:
  * stream_count       - how many plays the track got
  * unique_users       - approximate number of distinct listeners
  * short_play_ratio   - share of plays that barely passed the 30 s mark

A window is flagged when a track gets enough plays AND most of them are
suspiciously short. Volume alone is not enough: a real hit also gets a lot
of plays, but people listen to it for longer.
"""
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    approx_count_distinct,
    avg,
    col,
    count,
    from_json,
    lit,
    round as spark_round,
    struct,
    to_json,
    when,
    window,
)
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

import config

# Keep the Kafka connector on the same version as the installed PySpark
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}"

spark = (
    SparkSession.builder.appName("FarmbusterDetector")
    .config("spark.jars.packages", KAFKA_PACKAGE)
    .config("spark.sql.shuffle.partitions", "4")  # local run, no need for 200
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

event_schema = StructType(
    [
        StructField("event_id", StringType()),
        StructField("user_id", StringType()),
        StructField("track_id", StringType()),
        StructField("artist_id", StringType()),
        StructField("genre", StringType()),
        StructField("duration_played_sec", IntegerType()),
        StructField("device_type", StringType()),
        # parsed as a string first, then cast: tolerant to ISO strings with
        # microseconds and a +00:00 offset that Python's isoformat() produces
        StructField("timestamp", StringType()),
    ]
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", config.STREAMS_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

plays = (
    raw.select(from_json(col("value").cast("string"), event_schema).alias("e"))
    .select("e.*")
    .withColumn("event_time", col("timestamp").cast("timestamp"))
    .filter(col("event_time").isNotNull() & col("track_id").isNotNull())
)

is_short_play = col("duration_played_sec").between(
    config.SHORT_PLAY_MIN_SEC, config.SHORT_PLAY_MAX_SEC
)

stats = (
    plays.withWatermark("event_time", config.WATERMARK_DELAY)
    # tumbling window: each play belongs to exactly one window,
    # so one attack produces one alert per window instead of a pile of duplicates
    .groupBy(window(col("event_time"), config.WINDOW_DURATION), col("track_id"))
    .agg(
        count(lit(1)).alias("stream_count"),
        approx_count_distinct("user_id").alias("unique_users"),
        avg(when(is_short_play, 1.0).otherwise(0.0)).alias("short_play_ratio"),
    )
)

alerts = stats.filter(
    (col("stream_count") >= config.MIN_STREAMS_PER_WINDOW)
    & (col("short_play_ratio") >= config.MIN_SHORT_PLAY_RATIO)
).select(
    col("track_id").alias("key"),
    to_json(
        struct(
            col("track_id"),
            col("window.start").cast("string").alias("window_start"),
            col("window.end").cast("string").alias("window_end"),
            col("stream_count"),
            col("unique_users"),
            spark_round(col("short_play_ratio"), 2).alias("short_play_ratio"),
        )
    ).alias("value"),
)

query = (
    alerts.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
    .option("topic", config.ALERTS_TOPIC)
    .option("checkpointLocation", config.CHECKPOINT_DIR)
    # append: a window is emitted once, after the watermark closes it
    .outputMode("append")
    .start()
)

print(f"Reading '{config.STREAMS_TOPIC}', writing alerts to '{config.ALERTS_TOPIC}'.")
query.awaitTermination()
