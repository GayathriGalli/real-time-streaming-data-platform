from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="real_time_streaming_data_pipeline",
    description="Orchestrates Bronze, Silver, and Gold streaming data pipelines",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "kafka",
        "spark",
        "delta-lake",
        "streaming",
        "data-engineering",
    ],
) as dag:

    start_bronze_pipeline = BashOperator(
        task_id="start_bronze_stream",
        bash_command=(
            "spark-submit "
            "--packages io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 "
            "streaming/bronze_stream.py"
        ),
    )

    start_silver_pipeline = BashOperator(
        task_id="start_silver_stream",
        bash_command=(
            "spark-submit "
            "--packages io.delta:delta-spark_2.12:3.2.0 "
            "streaming/silver_stream.py"
        ),
    )

    start_gold_pipeline = BashOperator(
        task_id="start_gold_stream",
        bash_command=(
            "spark-submit "
            "--packages io.delta:delta-spark_2.12:3.2.0 "
            "streaming/gold_stream.py"
        ),
    )

    start_bronze_pipeline >> start_silver_pipeline >> start_gold_pipeline
