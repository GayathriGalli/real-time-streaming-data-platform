import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    lit,
    sum as spark_sum,
    when,
)


SILVER_PATH = os.getenv(
    "SILVER_PATH",
    "data/silver/transactions"
)

QUALITY_METRICS_PATH = os.getenv(
    "QUALITY_METRICS_PATH",
    "data/quality/metrics"
)


def create_spark_session():
    return (
        SparkSession.builder
        .appName("StreamingDataQualityChecks")
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


def run_quality_checks(df):
    """
    Calculate data-quality metrics for validated transactions.
    """

    total_records = df.count()

    null_transaction_ids = (
        df.filter(col("transaction_id").isNull()).count()
    )

    null_customer_ids = (
        df.filter(col("customer_id").isNull()).count()
    )

    invalid_amounts = (
        df.filter(
            col("amount").isNull() |
            (col("amount") <= 0)
        ).count()
    )

    invalid_timestamps = (
        df.filter(
            col("event_timestamp").isNull()
        ).count()
    )

    duplicate_transactions = (
        df.groupBy("transaction_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    valid_records = (
        total_records
        - null_transaction_ids
        - null_customer_ids
        - invalid_amounts
        - invalid_timestamps
    )

    quality_score = (
        (valid_records / total_records) * 100
        if total_records > 0
        else 100.0
    )

    metrics = {
        "total_records": total_records,
        "null_transaction_ids": null_transaction_ids,
        "null_customer_ids": null_customer_ids,
        "invalid_amounts": invalid_amounts,
        "invalid_timestamps": invalid_timestamps,
        "duplicate_transactions": duplicate_transactions,
        "quality_score": round(quality_score, 2),
    }

    return metrics


def save_quality_metrics(spark, metrics):
    """
    Persist quality metrics to Delta Lake for monitoring.
    """

    metrics_df = spark.createDataFrame(
        [
            (
                metrics["total_records"],
                metrics["null_transaction_ids"],
                metrics["null_customer_ids"],
                metrics["invalid_amounts"],
                metrics["invalid_timestamps"],
                metrics["duplicate_transactions"],
                metrics["quality_score"],
            )
        ],
        [
            "total_records",
            "null_transaction_ids",
            "null_customer_ids",
            "invalid_amounts",
            "invalid_timestamps",
            "duplicate_transactions",
            "quality_score",
        ],
    ).withColumn(
        "checked_at",
        current_timestamp()
    )

    (
        metrics_df.write
        .format("delta")
        .mode("append")
        .save(QUALITY_METRICS_PATH)
    )


def main():
    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    silver_df = (
        spark.read
        .format("delta")
        .load(SILVER_PATH)
    )

    metrics = run_quality_checks(silver_df)

    print("\nDATA QUALITY REPORT")
    print("-" * 40)

    for metric, value in metrics.items():
        print(f"{metric}: {value}")

    save_quality_metrics(
        spark,
        metrics
    )

    minimum_quality_score = 95.0

    if metrics["quality_score"] < minimum_quality_score:
        raise ValueError(
            "Data quality validation failed. "
            f"Score: {metrics['quality_score']}%"
        )

    print(
        f"\nData quality validation passed: "
        f"{metrics['quality_score']}%"
    )

    spark.stop()


if __name__ == "__main__":
    main()
