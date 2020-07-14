from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
 
 
# This class is used to stage the data in the tables on Redshift 
class StageToRedshiftOperator(BaseOperator):
     
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        csv
        IGNOREHEADER 1 
        emptyasnull
        blanksasnull
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.aws_credentials_id = aws_credentials_id

      
    
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run( "DELETE FROM {}".format(self.table) )

        self.log.info("Copying data from S3 to staging")
         
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
             self.table,
             self.s3_bucket,
             credentials.access_key,
             credentials.secret_key
           )
        redshift.run(formatted_sql)
      





    
    