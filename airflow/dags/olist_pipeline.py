from datetime import timedelta

import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

SPARK = "docker exec spark spark-submit"
SRC = "/opt/spark/work-dir/src"
KAFKA_OPTS = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 "
    "--conf spark.jars.ivy=/tmp/.ivy2"
)

# Réglages communs : posés explicitement sur chaque tâche (plus sûr que default_args).
COMMON = dict(
    retries=2,
    retry_delay=timedelta(minutes=1),
    execution_timeout=timedelta(minutes=20),
)

with DAG(
    dag_id="olist_pipeline",
    schedule=None,                # déclenchement manuel pour l'instant
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,            # un seul run à la fois : Delta/S3 = un seul écrivain par table
    tags=["olist"],
) as dag:
    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command=f"{SPARK} {SRC}/batch/bronze_ingest.py", **COMMON)

    silver_quarantine = BashOperator(
        task_id="silver_quarantine",
        bash_command=f"{SPARK} {SRC}/batch/silver_quarantine.py", **COMMON)

    stream_to_bronze = BashOperator(
        task_id="stream_to_bronze",
        bash_command=f"{SPARK} {KAFKA_OPTS} {SRC}/streaming/orders_stream_to_bronze.py", **COMMON)

    silver_orders_from_stream = BashOperator(
        task_id="silver_orders_from_stream",
        bash_command=f"{SPARK} {SRC}/streaming/silver_orders_from_stream.py", **COMMON)

    gold_star_schema = BashOperator(
        task_id="gold_star_schema",
        bash_command=f"{SPARK} {SRC}/batch/gold_star_schema.py", **COMMON)

    bronze_ingest >> silver_quarantine
    [silver_quarantine, stream_to_bronze] >> silver_orders_from_stream >> gold_star_schema