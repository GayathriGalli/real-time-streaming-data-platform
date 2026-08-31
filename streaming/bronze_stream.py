import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "transactions",
)

BRONZE_PATH = os.getenv(
    "BRONZE_PATH",
    "data/bronze/transactions",
)

CHECKPOINT_PATH = os.getenv(
    "BRONZE_CHECKPOINT_PATH",
    "checkpoints/bronze_transactions",
)


transaction_schema = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("payment_method", StringType(), True),
        StructField("event_timestamp", StringType(), False),
        StructField("country", StringType(), True),
    ]
)


def create_spark_session():
    """Create the Spark session used by the streaming pipeline."""

    return (
        SparkSession.builder
        .appName("BronzeTransactionStreaming")
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


def read_kafka_stream(spark):
    """Read transaction events continuously from Kafka."""

    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def transform_to_bronze(kafka_df):
    """
    Parse Kafka JSON events and preserve ingestion metadata
    required for traceability and downstream processing.
    """

    parsed_df = (
        kafka_df
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_payload"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn(
            "transaction",
            from_json(
                col("raw_payload"),
                transaction_schema,
            ),
        )
    )

    bronze_df = (
        parsed_df
        .select(
            "kafka_key",
            "raw_payload",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            col("transaction.*"),
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp(),
        )
    )

    return bronze_df


def write_bronze_stream(bronze_df):
    """Write the Bronze stream to Delta Lake with checkpointing."""

    return (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            CHECKPOINT_PATH,
        )
        .option(
            "path",
            BRONZE_PATH,
        )
        .trigger(processingTime="10 seconds")
        .start()
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    print("Starting Bronze streaming pipeline...")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Bronze path: {BRONZE_PATH}")
    print(f"Checkpoint: {CHECKPOINT_PATH}")

    kafka_df = read_kafka_stream(spark)

    bronze_df = transform_to_bronze(kafka_df)

    query = write_bronze_stream(bronze_df)

    query.awaitTermination()


if __name__ == "__main__":
    main()
