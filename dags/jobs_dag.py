import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils import log_operator, check_table_exist, PostgreSQLCountRowsOperator


config = {
    'dag_id_1': {'schedule_interval': None, "start_date": datetime(2022, 2, 1),
                 'table_name': "table name 1"},
    'dag_id_2': {'schedule_interval': None, "start_date": datetime(2022, 2, 1),
                 'table_name': "table name 2"},
    'dag_id_3': {'schedule_interval': None, "start_date": datetime(2022, 2, 1),
                 'table_name': "table name 3"},
}

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
            python_callable=log_operator
        )
        get_current_user = BashOperator(
            task_id=f'identify_user',
            bash_command='echo $(whoami)',
            do_xcom_push=True,
        )
        table_name = "table_name"
        check_table = BranchPythonOperator(
            task_id=f"check_if_table_exists",
            python_callable=check_table_exist,
            trigger_rule='one_success',
            op_args=["SELECT * FROM pg_tables;",
                     "SELECT * FROM information_schema.tables "
                     "WHERE table_schema = '{}'"
                     "AND table_name = '{}';", table_name]
        )
        dummy_task = DummyOperator(
            task_id='dummy_task'
        )
        create_table = PostgresOperator(
            task_id=f'create_table',
            postgres_conn_id='my_conn',
            sql='''CREATE TABLE table_name(custom_id integer NOT NULL, 
                user_name VARCHAR (50) NOT NULL, timestamp TIMESTAMP NOT NULL);
                '''
        )
        insert_row = PostgresOperator(
            trigger_rule="one_success",
            task_id=f"insert_row",
            postgres_conn_id='my_conn',
            parameters={"custom_id_value": uuid.uuid4().int % 123456789,
                        "timestamp_value": datetime.now()},
            sql='''INSERT INTO table_name VALUES
                (%(custom_id_value)s, '{{ ti.xcom_pull(task_ids='identify_user', key='return_value') }}', %(timestamp_value)s);
                '''
        )
        query = PostgreSQLCountRowsOperator(
            task_id="query_the_table",
            postgres_conn_id='my_conn',
            do_xcom_push=True
        )
        connect >> get_current_user >> check_table >> [create_table, dummy_task] >> insert_row >> query
