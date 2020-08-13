from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

from helpers import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    """
    Copies JSON data from S3 to staging tables in Redshift 
    
    :param redshift_conn_id: Redshift connection id
    :param aws_credentials_id: AWS credentials id
    :param table: Target staging table in Redshift
    :param s3_path: S3 path where JSON data resides
    :param json_option: defines the mapping for data elements in the JSON source to the columns in the target table
    :param region: AWS Region where the source data is located
    """
    
    ui_color = '#358140'
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        {}
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_path="",
                 copy_json_option="auto",
                 region="",
                 *args, **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_path = s3_path
        self.json_option = json_option
        self.region = region
    
    def execute(self, context):
        self.log.info("Getting credentials")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Deleting data from Redshift staging table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_option
        )
        redshift.run(formatted_sql)