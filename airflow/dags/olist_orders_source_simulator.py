import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="olist_orders_source_simulator",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={"skip": 0, "limit": 100},  # modifiables au déclenchement
    tags=["olist", "simulation"],
) as dag:
    BashOperator(
        task_id="produce_orders",
        bash_command=(
            "docker exec spark python3 /opt/spark/work-dir/src/producer/orders_producer.py "
            "--skip {{ params.skip }} --limit {{ params.limit }} --rate 50"
        ),
        retries=0,  # un retry renverrait les mêmes commandes : doublons
    )
