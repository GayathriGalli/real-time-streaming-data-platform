import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_timestamp,
    trim,
)


BRONZE_PATH = os.getenv(
    "BRONZE_PATH",
    "data/bronze/transactions",
)

SILVER_PATH = os.getenv(
    "SILVER_PATH",
    "data/silver/transactions",
)

CHECKPOINT_PATH = os.getenv(
    "SILVER_CHECKPOINT_PATH",
    "checkpoints/silver_transactions",
)


def create_spark_session():
    """Create Spark session with Delta Lake support."""

    return (
        SparkSession.builder
        .appName("SilverTransactionStreaming")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def read_bronze_stream(spark):
    """Continuously read transactions from the Bronze Delta layer."""

    return (
        spark.readStream
        .format("delta")
        .load(BRONZE_PATH)
    )


def clean_and_validate(bronze_df):
    """
    Apply Silver-layer data quality rules, normalize fields,
    parse event time, and prepare records for deduplication.
    """

    cleaned_df = (
        bronze_df
        .withColumn(
            "event_time",
            to_timestamp(col("event_timestamp")),
        )
        .withColumn(
            "product_name",
            trim(col("product_name")),
        )
        .withColumn(
            "category",
            trim(col("category")),
        )
        .withColumn(
            "payment_method",
            trim(col("payment_method")),
        )
        .withColumn(
            "country",
            trim(col("country")),
        )
    )

    valid_df = cleaned_df.filter(
        col("transaction_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("product_id").isNotNull()
        & col("event_time").isNotNull()
        & (col("quantity") > 0)
        & (col("unit_price") >= 0)
        & (col("total_amount") >= 0)
    )

    return valid_df


def deduplicate_transactions(valid_df):
    """
    Use event-time watermarking to control streaming state and
    remove duplicate transaction IDs.
    """

    return (
        valid_df
        .withWatermark(
            "event_time",
            "10 minutes",
        )
        .dropDuplicates(
            ["transaction_id"]
        )
    )


def prepare_silver_output(deduplicated_df):
    """Select curated Silver fields and add processing metadata."""

    return (
        deduplicated_df
        .select(
            "transaction_id",
            "customer_id",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "unit_price",
            "total_amount",
            "payment_method",
            "country",
            "event_time",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingestion_timestamp",
        )
        .withColumn(
            "silver_processed_timestamp",
            current_timestamp(),
        )
    )


def write_silver_stream(silver_df):
    """Write validated transactions to the Silver Delta layer."""

    return (
        silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            CHECKPOINT_PATH,
        )
        .option(
            "path",
            SILVER_PATH,
        )
        .trigger(processingTime="10 seconds")
        .start()
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    print("Starting Silver streaming pipeline...")
    print(f"Bronze path: {BRONZE_PATH}")
    print(f"Silver path: {SILVER_PATH}")
    print(f"Checkpoint: {CHECKPOINT_PATH}")

    bronze_df = read_bronze_stream(spark)

    valid_df = clean_and_validate(bronze_df)

    deduplicated_df = deduplicate_transactions(valid_df)

    silver_df = prepare_silver_output(deduplicated_df)

    query = write_silver_stream(silver_df)

    query.awaitTermination()


if __name__ == "__main__":
    main()
