from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 destination_table = "",
                 json_location = "",
                 s3_bucket = "",
                 s3_key = "",
                 aws_region = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.destination_table = destination_table
        self.json_location = json_location
        self.s3_source_location = f"s3://{s3_bucket}/{s3_key}"
        self.aws_region = aws_region
        
    def cmd_builder(self, credentials):
        copy_command = f"""
        COPY {self.destination_table}
        FROM '{self.s3_source_location}'
        ACCESS_KEY_ID '{credentials.access_key}'
        SECRET_ACCESS_KEY '{credentials.secret_key}'
        REGION AS '{self.aws_region}'
        JSON '{self.json_location}';
        """
        self.log.info(f"Copy command listing: {copy_command}")
        return copy_command
    
    def execute(self, context):
        self.log.info("Beginning the StageToRedshiftOperator...")
        self.log.info("Starting to process data from s3 bucket to redshift")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Copying data from s3 bucket to redshift: {self.s3_source_path} to {self.destination}")
        redshift.run(self.cmd_builder(credentials))
        self.log.info("StageToRedshiftOperator has now completed.")





