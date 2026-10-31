import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="hello_pipeline",
    schedule=None,                                   # déclenchement manuel uniquement
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,                                   # ne rattrape pas les dates passées
    tags=["demo"],
) as dag:
    step_1 = BashOperator(task_id="step_1", bash_command='echo "étape 1 : démarrage"')
    step_2 = BashOperator(task_id="step_2", bash_command='echo "étape 2 : fin"')

    step_1 >> step_2                                 # step_2 attend la réussite de step_1