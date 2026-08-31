import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    date_format,
    round as spark_round,
    sum as spark_sum,
    window,
)


SILVER_PATH = os.getenv("SILVER_PATH", "data/silver/transactions")
GOLD_PATH = os.getenv("GOLD_PATH", "data/gold/transaction_metrics")
CHECKPOINT_PATH = os.getenv(
    "GOLD_CHECKPOINT_PATH",
    "checkpoints/gold_transactions"
)


def create_spark_session():
    """Create Spark session with Delta Lake support."""

    return (
        SparkSession.builder
        .appName("GoldTransactionAnalytics")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    # Read validated transactions from Silver layer
    silver_stream = (
        spark.readStream
        .format("delta")
        .load(SILVER_PATH)
    )

    # Create event-time windows for real-time analytics
    gold_metrics = (
        silver_stream
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            window(
                col("event_timestamp"),
                "5 minutes"
            ),
            col("merchant_category"),
            col("transaction_type")
        )
        .agg(
            count("*").alias("transaction_count"),
            spark_round(
                spark_sum("amount"),
                2
            ).alias("total_transaction_amount"),
            spark_round(
                avg("amount"),
                2
            ).alias("average_transaction_amount")
        )
        .withColumn(
            "window_start",
            col("window.start")
        )
        .withColumn(
            "window_end",
            col("window.end")
        )
        .withColumn(
            "metric_date",
            date_format(
                col("window.start"),
                "yyyy-MM-dd"
            )
        )
        .withColumn(
            "processed_at",
            current_timestamp()
        )
        .drop("window")
    )

    # Write analytics-ready aggregates to Gold Delta layer
    query = (
        gold_metrics.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            CHECKPOINT_PATH
        )
        .partitionBy("metric_date")
        .trigger(processingTime="30 seconds")
        .start(GOLD_PATH)
    )

    print("Gold streaming pipeline started.")
    print(f"Silver source: {SILVER_PATH}")
    print(f"Gold destination: {GOLD_PATH}")

    query.awaitTermination()


if __name__ == "__main__":
    main()
