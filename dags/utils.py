from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import DagRun
import pendulum
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from airflow.models import Variable
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor


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


def define_taskgroup(triggeredDagId):
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

class PostgreSQLCountRowsOperator(BaseOperator):
    def __init__(self, postgres_conn_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = '''SELECT COUNT(*) FROM TABLE_NAME;'''
        self.hook = None

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.hook.run(self.sql)
        for output in self.hook.conn.notices:
            self.log.info(output)


def get_schema(hook, sql_to_get_schema):
    query = hook.get_records(sql=sql_to_get_schema)
    for result in query:
        if 'airflow' in result:
            schema = result[0]
            return schema


def check_table_exist(sql_to_get_schema, sql_to_check_table_exist,
                      table_name):
    """ callable function to get schema name and after that check if table exist """
    hook = PostgresHook("my_conn")
    # get schema name
    schema = get_schema(hook, sql_to_get_schema)

    # check table exist
    query = hook.get_first(sql=sql_to_check_table_exist.format(schema, table_name))
    print(query)
    if query:
        return "dummy_task" #True
    else:
        return "create_table"


def log_operator(dag_id, table_name):
    logging.info(f"{dag_id} start processing tables in database: {table_name}")