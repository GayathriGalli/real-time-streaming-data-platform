import pytest
from datetime import datetime

from pyspark.sql import SparkSession

from quality.data_quality_checks import run_quality_checks


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[2]")
        .appName("DataQualityTests")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


def test_valid_transactions(spark):
    data = [
        ("txn-001", "customer-001", 125.50, datetime(2026, 8, 31, 10, 0)),
        ("txn-002", "customer-002", 75.00, datetime(2026, 8, 31, 10, 1)),
    ]

    columns = [
        "transaction_id",
        "customer_id",
        "amount",
        "event_timestamp",
    ]

    dataframe = spark.createDataFrame(data, columns)

    metrics = run_quality_checks(dataframe)

    assert metrics["total_records"] == 2
    assert metrics["null_transaction_ids"] == 0
    assert metrics["null_customer_ids"] == 0
    assert metrics["invalid_amounts"] == 0
    assert metrics["invalid_timestamps"] == 0
    assert metrics["duplicate_transactions"] == 0
    assert metrics["quality_score"] == 100.0


def test_duplicate_transactions_detected(spark):
    data = [
        ("txn-001", "customer-001", 125.50, datetime(2026, 8, 31, 10, 0)),
        ("txn-001", "customer-001", 125.50, datetime(2026, 8, 31, 10, 1)),
    ]

    columns = [
        "transaction_id",
        "customer_id",
        "amount",
        "event_timestamp",
    ]

    dataframe = spark.createDataFrame(data, columns)

    metrics = run_quality_checks(dataframe)

    assert metrics["total_records"] == 2
    assert metrics["duplicate_transactions"] == 1


def test_invalid_amount_detected(spark):
    data = [
        ("txn-001", "customer-001", 100.00, datetime(2026, 8, 31, 10, 0)),
        ("txn-002", "customer-002", -50.00, datetime(2026, 8, 31, 10, 1)),
    ]

    columns = [
        "transaction_id",
        "customer_id",
        "amount",
        "event_timestamp",
    ]

    dataframe = spark.createDataFrame(data, columns)

    metrics = run_quality_checks(dataframe)

    assert metrics["total_records"] == 2
    assert metrics["invalid_amounts"] == 1
    assert metrics["quality_score"] == 50.0
