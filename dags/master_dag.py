from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import DagRun
import pendulum
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from airflow.models import Variable


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


def send_message_to_slack(slack_client, text, channel):
    try:
        response = slack_client.chat_postMessage(channel=channel, text=text)
        assert response["message"]["text"] == text
    except SlackApiError as e:
        # You will get a SlackApiError if "ok" is False
        assert e.response["ok"] is False
        assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
        print(f"Got an error kek: {e.response['error']}")


def alert_to_slack(**kwargs):
    slack_token = Variable.get_variable_from_secrets("slack_token")

    slack_client = WebClient(token=slack_token)

    dag_id = kwargs['dag_id']
    exec_date = kwargs['execution_date']
    channel = kwargs['channel']
    text = "Dag: " + dag_id + " finished at " + exec_date
    send_message_to_slack(slack_client, text, channel)


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
            bash_command="touch finished#$({{ ts_nodash }})"
        )
        sensor_triggered_dag >> print_result >> create_timestamp
    return tg


class SmartFileSensor(FileSensor):
    poke_context_fields = ('filepath', 'fs_conn_id')

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def is_smart_sensor_compatible(self):
        result = not self.soft_fail and super().is_smart_sensor_compatible()
        return result


triggeredDagId = "dag_id_1"
filename = "run"
dag_id = "master_dag"
with DAG(dag_id=dag_id, start_date=datetime(2022, 1, 1), schedule_interval=None, catchup=False) as dag:

    run_sensor = SmartFileSensor(
        filepath=filename,
        task_id="sensing_file",
        dag=dag,
        fs_conn_id="FileSensorRunPath",
        poke_interval=10
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

