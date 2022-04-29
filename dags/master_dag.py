from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils import SmartFileSensor, alert_to_slack, define_taskgroup


triggeredDagId = "dag_id_1"
filename = "run"
dag_id = "master_dag"
with DAG(dag_id=dag_id, start_date=datetime(2022, 1, 1), schedule_interval=None, catchup=False) as dag:

    run_sensor = SmartFileSensor(
        filepath=filename,
        task_id="sensing_file",
        fs_conn_id="FileSensorRunPath",
        poke_interval=10
    )
    trigger_dag = TriggerDagRunOperator(
        task_id="triggering_dag",
        trigger_dag_id=triggeredDagId,
    )
    alert_to_slack = PythonOperator(
        task_id="alert_to_slack",
        python_callable=alert_to_slack,
        op_kwargs={'execution_date': '{{ ts_nodash }}',
                   'dag_id': dag_id,
                   'channel': 'general'}
    )
    processResults = define_taskgroup(triggeredDagId)
    run_sensor >> trigger_dag >> processResults >> alert_to_slack

