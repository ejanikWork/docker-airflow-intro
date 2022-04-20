from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import DagRun
import pendulum
from slack_writechat import alert_to_slack


def _process():
    print("result")


def get_most_recent_dag_run(dag_id):
    dag_runs = DagRun.find(dag_id=dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    return dag_runs[0] if dag_runs else None


def execution_date_fn(execution_date, dag, task, **kwargs):
    # Return the most recent execution date for the dependent DAG.
    dag_run = get_most_recent_dag_run(task.external_dag_id)
    dag_execution_date = dag.start_date if dag_run is None else dag_run.execution_date
    return pendulum.instance(dag_execution_date)


def define_taskgroup():
    with TaskGroup(group_id="process_results") as tg:
        sensor_triggered_dag = ExternalTaskSensor(
            task_id="subdag_sensor_triggered_dag",
            external_dag_id=triggeredDagId,
            poke_interval=10,
            execution_date_fn=execution_date_fn
        )
        print_result = PythonOperator(
            task_id="subdag_print_result",
            python_callable=_process,
        )
        create_timestamp = BashOperator(
            task_id="creating_timestamp",
            bash_command="touch ../finished#$({{ ts_nodash }})"
        )
        sensor_triggered_dag >> print_result >> create_timestamp
    return tg


triggeredDagId = "dag_id_1"
filename = "run"
dag_id = "master_dag"


class SmartFileSensor(FileSensor):
    poke_context_fields = ('filepath', 'fs_conn_id')

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def is_smart_sensor_compatible(self):
        result = not self.soft_fail and super().is_smart_sensor_compatible()
        return result


with DAG(dag_id=dag_id, start_date=now, schedule_interval=None, catchup=False) as dag:

    run_sensor = SmartFileSensor(
        filepath=filename,
        task_id="sensing_file",
        dag=dag,
        fs_conn_id="FileSensorRunPath"
    )
    trigger_dag = TriggerDagRunOperator(
        task_id="triggering_dag",
        trigger_dag_id=triggeredDagId,
        dag=dag,
    )
    alert_to_slack = PythonOperator(
        task_id="alert_to_slack",
        dag=dag,
        python_callable=alert_to_slack,
        op_kwargs={'execution_date': '{{ ts_nodash }}',
                   'dag_id': dag_id,
                   'channel': 'general'}
    )
    processResults = define_taskgroup()
    run_sensor >> trigger_dag >> processResults >> alert_to_slack

