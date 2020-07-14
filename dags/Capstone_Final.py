from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from plugins.operators import ( StageToRedshiftOperator, LoadFactOperator, LoadGenreStagingOperator, LoadCastStagingOperator , DataQualityOperator , LoadDimensionTablesOperator )      
from plugins.helpers import SqlQueries
from airflow.operators import PostgresOperator
import numpy as np
import pandas as pd
import psycopg2
from airflow.operators.python_operator import PythonOperator
 


#defining the default arhguments for the Dag
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 7, 13),
    'Depends_on_past': False,
    'Retries': 1,
    'Retry_delay': timedelta(minutes=5),
    'Catchup': True,
    'email_on_retry': False
}



#defining the Dag
dag = DAG(
          'Capstone_Final',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@once',
          max_active_runs=1
        )


#defining the start operator, acts like an entry point operator for the DAG.
start_operator = DummyOperator( task_id ='Begin_execution',  dag=dag )



''' 
create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables_new.sql'
)
''' 

 
 
  
#STAGING - Movies Metadata
#operator to COPY data from S3 into movies_metadata_staging
stage_movies_metadata_to_redshift = StageToRedshiftOperator(
    task_id='load_movies_staging',
    dag=dag,
    table="movies_staging",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="s3://movies-bucket-new/movies_metadata_107.csv"
)
 

 
 
  

#STAGING - Genre
#operator to COPY from S3 into movie_genre_staging table 

stage_genres_to_redshift = LoadGenreStagingOperator(
        
        task_id="load_Genres_Staging",
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="movie_genre_staging",
        s3_bucket="s3://movies-bucket-new/genre_dimension.csv"
)
  

#STAGING - Cast
#operator to COPY from S3 into cast_staging5 table 
load_Cast_Staging = LoadCastStagingOperator(
        task_id="load_Cast_Staging",
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="cast_staging5",
        s3_bucket="s3://movies-bucket-new/cast_dimension.csv"
)


  

#STAGING - ratings
#operator to COPY from S3 into ratings_staging table 
stage_ratings_to_redshift = StageToRedshiftOperator(
        task_id="load_ratings_Staging",
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="ratings_staging",
        s3_bucket="s3://movies-bucket-new/ratings.csv"
)
 
 

#STAGING - CPI 
#operator to COPY from S3 into CPI_staging table 
stage_CPI_to_redshift = StageToRedshiftOperator(
        task_id="load_CPI_Staging",
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="CPI_Staging",
        s3_bucket="s3://movies-bucket-new/CPI.csv"
) 
 


#===========================  
 


#DIMENSION - CPI 
#Operator to load data from "CPI_staging" into "CPI_dimension" table
load_CPI_Dimension = LoadDimensionTablesOperator(
    task_id='load_CPI_data_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_insert=SqlQueries.Insert_CPI_Data_Dimension,
    sql_delete=SqlQueries.Remove_CPI_Data_Dimension,
    append_data = False
)




 

#DIMENSION - ratings
#operator to load data from "ratings_staging" into "ratings_dimension" table
load_ratings_Dimension = LoadDimensionTablesOperator(
    task_id='load_ratings_data_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_insert=SqlQueries.Insert_Ratings_Data_Dimension,
    sql_delete=SqlQueries.Remove_Ratings_Data_Dimension,
    append_data = False
)

 
 

#FACT  - Movies Fact
#operator to load data from "movies_staging" into "movies_fact" table
load_movie_data_fact_table = LoadFactOperator(
    task_id='load_movie_data_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_insert=SqlQueries.Insert_movie_data_fact,
    sql_delete=SqlQueries.remove_movie_data_fact,
    append_data = True
)

 
 

#DIMENSION - time
#operator to load just the 'time' data from "movies_staging" into "Time_Dimension table"
load_Time_Dimension_table = LoadDimensionTablesOperator(
    task_id='load_time_data_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_insert=SqlQueries.Insert_time_data_Dimension,
    sql_delete=SqlQueries.Remove_time_data_Dimension,
    append_data = False
)

 



#DIMENSION - Genre
#operator to load  data from "Movie_Genre_Staging" into "Movie_Genre_Dimension"
load_genre_dimension_table = LoadDimensionTablesOperator(
    task_id='Load_genre_data_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_insert=SqlQueries.Genre_table_insert,
    sql_delete=SqlQueries.Genre_table_delete,
    append_data = False
)




#DIMENSION - cast
#operator to load  data from "Cast_Staging5" into "Cast_Dimension"
load_Cast_dimension_table = LoadDimensionTablesOperator(
    task_id='Load_Cast_data_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_insert=SqlQueries.Insert_Cast_Data_Dimension,
    sql_delete=SqlQueries.Remove_Cast_Data_Dimension,
    append_data = False
)




#defining operator for doing a quality check on all the dimension tables and the fact table.
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    
    dq_checks=[
       {'check_sql': 'select count(*) from public."Movies_Fact";' , 'expected_result': 0 },
       {'check_sql': 'select count(*) from public."Movie_Genre_Dimension";' , 'expected_result': 0 },
       {'check_sql': 'select count(*) from public."Ratings_Dimension";' , 'expected_result': 0 },
       {'check_sql': 'select count(*) from public."Time_Dimension";' , 'expected_result': 0 },
       {'check_sql': 'select count(*) from public."Cast_Dimension";' , 'expected_result': 0 },
       {'check_sql': 'select count(*) from public."CPI_Dimension";' , 'expected_result': 0 }
    ]
)
 
 
#=========================== 

end_operator = DummyOperator( task_id='Stop_execution',  dag=dag )




 
start_operator >> stage_movies_metadata_to_redshift
stage_movies_metadata_to_redshift >>  [ load_movie_data_fact_table , load_Time_Dimension_table ]
[ load_movie_data_fact_table , load_Time_Dimension_table ] >> stage_CPI_to_redshift
stage_CPI_to_redshift >> load_CPI_Dimension
load_CPI_Dimension >> run_quality_checks
 


start_operator >> stage_genres_to_redshift
stage_genres_to_redshift >> load_genre_dimension_table
load_genre_dimension_table >> run_quality_checks
 


start_operator >> stage_ratings_to_redshift
stage_ratings_to_redshift >> load_ratings_Dimension
load_ratings_Dimension >> run_quality_checks
 


start_operator >>  load_Cast_Staging
load_Cast_Staging  >> load_Cast_dimension_table
load_Cast_dimension_table >> run_quality_checks



run_quality_checks >> end_operator
 



 
 
 



