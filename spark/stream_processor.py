"""Structured streaming pipeline: Kafka -> Bronze/Silver/Gold."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    from_json,
    sum as spark_sum,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("user_id", IntegerType()),
        StructField("product_id", IntegerType()),
        StructField("action", StringType()),
        StructField("price", DoubleType()),
        StructField("quantity", IntegerType()),
        StructField("session_id", StringType()),
        StructField("country", StringType()),
        StructField("timestamp", StringType()),
    ]
)


def main() -> None:
    spark = (
        SparkSession.builder.appName("ecommerce-stream-processor")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "user_activity")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = kafka_df.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e")).select(
        "e.*"
    )

    silver = parsed.withColumn("event_ts", to_timestamp("timestamp")).withColumn(
        "order_revenue", when(col("action") == "purchase", col("price") * col("quantity")).otherwise(0)
    )

    gold = silver.withWatermark("event_ts", "10 minutes").groupBy(
        window("event_ts", "5 minutes", "1 minute")
    ).agg(
        countDistinct("user_id").alias("active_users"),
        spark_sum(when(col("action") == "purchase", 1).otherwise(0)).alias("orders"),
        spark_sum("order_revenue").alias("revenue"),
    )

    (
        parsed.writeStream.outputMode("append")
        .format("parquet")
        .option("path", "data/bronze/events")
        .option("checkpointLocation", "data/checkpoints/bronze")
        .start()
    )

    (
        silver.writeStream.outputMode("append")
        .format("parquet")
        .option("path", "data/silver/events")
        .option("checkpointLocation", "data/checkpoints/silver")
        .start()
    )

    query = (
        gold.writeStream.outputMode("append")
        .format("parquet")
        .option("path", "data/gold/kpis")
        .option("checkpointLocation", "data/checkpoints/gold")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
