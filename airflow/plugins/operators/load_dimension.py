from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 destination_table = "",
                 sql = "",
                 schema = "",
                 truncate = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql = sql
        self.schema = schema
        self.truncate = truncate

    def execute(self, context):
        self.log.info('Beginning the LoadDimensionOperator...')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.destination_table}")
        sql = f"\nINSERT INTO {self.destination_table} ({self.sql})"
        self.log.info(f"Loading in our data to the {self.schema}.{self.destination_table} table")
        redshift.run(sql)
        self.log.info("LoadDimensionOperator has now completed.")
