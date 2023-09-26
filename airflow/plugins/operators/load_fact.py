from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 destination_table = "",
                 sql = "",
                 schema = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql = sql
        self.schema = schema

    def execute(self, context):
        self.log.info('Beginning the LoadFactOperator...')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql = f"\nINSERT INTO {self.destination_table} ({self.sql})"
        self.log.info(f"Loading in our data to the {self.schema}.{self.destination_table} table")
        redshift.run(sql)
        self.log.info("LoadFactOperator has now completed.")
