import uuid
from datetime import datetime
from airflow import DAG
import logging
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


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


def get_dags(config):
    dag_list = []
    for key in config:
        # https://github.com/apache/airflow/issues/19902
        with DAG(dag_id=key,
                 start_date=config[key]["start_date"],
                 schedule_interval=config[key]["schedule_interval"],
                 catchup=False) as dag:

            connect = PythonOperator(
                task_id=f"connecting_to_database",
                op_kwargs={'dag_id': key,
                           'table_name': config[key]["table_name"]},
                python_callable=log_operator,
                dag=dag
            )
            get_current_user = BashOperator(
                task_id=f'identify_user',
                bash_command='echo $(whoami)',
                do_xcom_push=True,
                dag=dag
            )
            table_name = "table_name"
            check_table = BranchPythonOperator(
                task_id=f"check_if_table_exists",
                python_callable=check_table_exist,
                trigger_rule='one_success',
                op_args=["SELECT * FROM pg_tables;",
                         "SELECT * FROM information_schema.tables "
                         "WHERE table_schema = '{}'"
                         "AND table_name = '{}';", table_name],
                dag=dag
            )
            dummy_task = DummyOperator(
                task_id='dummy_task',
                dag=dag
            )
            create_table = PostgresOperator(
                task_id=f'create_table',
                postgres_conn_id='my_conn',
                sql='''CREATE TABLE table_name(custom_id integer NOT NULL, 
                    user_name VARCHAR (50) NOT NULL, timestamp TIMESTAMP NOT NULL);
                    ''',
                dag=dag
            )
            insert_row = PostgresOperator(
                trigger_rule="one_success",
                task_id=f"insert_row",
                postgres_conn_id='my_conn',
                parameters={"custom_id_value": uuid.uuid4().int % 123456789,
                            "timestamp_value": datetime.now()},
                sql='''INSERT INTO table_name VALUES
                    (%(custom_id_value)s, '{{ ti.xcom_pull(task_ids='identify_user', key='return_value') }}', %(timestamp_value)s);
                    ''',
                dag=dag
            )
            query = PostgreSQLCountRowsOperator(
                task_id="query_the_table",
                postgres_conn_id='my_conn',
                do_xcom_push=True,
                dag=dag
            )
            connect >> get_current_user >> check_table >> [create_table, dummy_task] >> insert_row >> query
        dag_list.append(dag)
    return dag_list


config = {
    'dag_id_1': {'schedule_interval': None, "start_date": datetime(2022, 2, 1),
                 'table_name': "table name 1"},
    'dag_id_2': {'schedule_interval': None, "start_date": datetime(2022, 2, 1),
                 'table_name': "table name 2"},
    'dag_id_3': {'schedule_interval': None, "start_date": datetime(2022, 2, 1),
                 'table_name': "table name 3"},
}

(d1, d2, d3) = get_dags(config)
