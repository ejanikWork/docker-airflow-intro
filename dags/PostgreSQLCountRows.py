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
