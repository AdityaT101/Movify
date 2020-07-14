from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 sql_insert="",
                 sql_delete="",
                 append_data="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql_insert=sql_insert
        self.sql_delete=sql_delete
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.append_data = append_data

        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Implementing LoadFactOperator')
        
        if self.append_data == True:
            sql  = self.sql_insert
            redshift.run(sql)
        else:
            sql  = self.sql_delete
            redshift.run(sql)
            sql  = self.sql_insert
            redshift.run(sql)

        
        
        
        
        
        
        
        
        
        