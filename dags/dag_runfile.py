from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(dag_id="generate_runfile", start_date=datetime(2022, 1, 31), schedule_interval='@daily', catchup=False) as dag:
    generating_file = BashOperator(
        task_id="generate_file",
        bash_command="touch run"
    )