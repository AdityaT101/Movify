from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import pandas as pd
import numpy as np
import logging
import boto3
import os
import sys
from io import StringIO
import math 


'''
The LoadGenreStagingOperator first ports in the Genre data from the S3 bucket (in the file named 'movies_metadata_107.csv ').
Then it filters on the Genre_id and Genre_name and stores it in a dataframe object. This is then converted into a csv object
This csv object is further processed to enter the Genre_Data into the Staging table.
This entire processing was done cause the Genre data was in form of comma separated JSON ARRAY. 
eg - [{"id": 27, "name": "Horror"}, {"id": 878, "name": "Science Fiction"}]

'''

class LoadGenreStagingOperator(BaseOperator):
    
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

        super(LoadGenreStagingOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        
        
        
    def execute(self, context):
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook( postgres_conn_id=self.redshift_conn_id )
        
         
        
        #importing the data from S3 bucket
        s3client = boto3.client(
                   's3',
                   region_name="us-west-2",
                   aws_access_key_id=' ',
                   aws_secret_access_key=' '
            )
        
        
        fileobj = s3client.get_object(
                
                     Bucket='movies-bucket-new',
                     Key='movies_metadata_107.csv'
                
            ) 
            
        
        filedata = fileobj['Body'].read()
        contents = filedata.decode('utf-8')
        
        df2 = pd.DataFrame(columns =  ["movie_id", "genre_id", "genre_name"])
        
        data = pd.read_csv( StringIO(contents), sep=',', engine='python' )
        
        itr=0
     
        #unnesting the json object and filtering on certain values.
        for i, row in data.iterrows():
            
            data1 = pd.DataFrame( eval( row['genres'] )  , columns=['id' , 'name']  )
             
            for index, rows in data1.iterrows():
                
                #Adding data to the Dataframe Object
                df2.loc[itr] =  [ row['id'] , rows['id'], rows['name'] ] 
                itr=itr+1
    
        
        csv_buf = StringIO()
        
        #converting this dataframe object into a csv object.
        df2.to_csv(csv_buf, header=True, index=False)
        csv_buf.seek(0)
        s3client.put_object(Bucket='movies-bucket-new', Body=csv_buf.getvalue(), Key='genre_dimension.csv')
            
        self.log.info("Clearing data from destination Redshift table")
        redshift.run( "Delete from Movie_Genre_Staging" )

        self.log.info("Copying data from S3 to staging - 'movies_genre_rating' data")
         
        
        formatted_sql = LoadGenreStagingOperator.copy_sql.format(
             self.table,
             self.s3_bucket,
             credentials.access_key,
             credentials.secret_key
           )
        redshift.run(formatted_sql)












'''
The LoadGenreStagingOperator first ports in the Cast data from the S3 bucket(file named credits74).
Then it filters on the Cast_id and cast_name and stores it in a dataframe object.This is then converted into a csv object
This csv object is further processed to enter the Cast Data into the Cast_Staging table.
This entire processing was done cause originally the Cast_data was in form of comma separated JSON ARRAY. 
eg - [{'cast_id': 14, 'character': 'Woody (voice)', 'credit_id': '52fe4284c3a36847f8024f95', 'gender': 2, 'id': 31, 'name': 'Tom Hanks', 'order': 0, 'profile_path': '/pQFoyx7rp09CJTAb932F2g8Nlho.jpg'}, 
      {'cast_id': 15, 'character': 'Buzz Lightyear (voice)', 'credit_id': '52fe4284c3a36847f8024f99', 'gender': 2, 'id': 12898, 'name': 'Tim Allen', 'order': 1, 'profile_path': '/uX2xVf6pMmPepxnvFWyBtjexzgY.jpg'} ]                           

'''

class LoadCastStagingOperator(BaseOperator):
    
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

        super(LoadCastStagingOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        
        
        
    def execute(self, context):
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook( postgres_conn_id=self.redshift_conn_id )
        
         
        
        #importing the data from S3 bucket
        s3client = boto3.client(
                   's3',
                   region_name="us-west-2",
                   aws_access_key_id=' ',
                   aws_secret_access_key=' '
            )
        
        
        fileobj = s3client.get_object(
                
                   Bucket='movies-bucket-new',
                   Key='credits74.csv'
                
            ) 
            
        self.log.info("adone")
        
        filedata = fileobj['Body'].read()
        contents = filedata.decode('utf-8')
        
        df2 = pd.DataFrame(columns =  ["movie_id", "cast_id", "cast_name"])
        
        data = pd.read_csv( StringIO(contents), sep=',', engine='python' )
        
        itr=0
     
        
        #unnesting the json object and filtering on certain values.
        for i, row in data.iterrows():
            
            data1 = pd.DataFrame( eval( row['cast'] )  , columns=['id' , 'name']  )
             
            for index, rows in data1.iterrows():
                #Adding data to the Dataframe Object
                df2.loc[itr] =  [ row['id'] , rows['id'], rows['name'] ] 
                itr=itr+1
                self.log.info(i)
    
    
        csv_buf = StringIO()
        
        #converting this dataframe object into a csv object.
        df2.to_csv(csv_buf, header=True, index=False)
        
        self.log.info("credits74.csv")
        csv_buf.seek(0)
        
        s3client.put_object(Bucket='movies-bucket-new', Body=csv_buf.getvalue(), Key='cast_dimension.csv')
            
        self.log.info("Clearing data from destination Redshift table")
        redshift.run( "Delete from cast_staging5" )

        self.log.info("Copying data from S3 to staging - 'cast_staging' data")
         
        
        
        formatted_sql = LoadCastStagingOperator.copy_sql.format(
             self.table,
             self.s3_bucket,
             credentials.access_key,
             credentials.secret_key
           )
        redshift.run(formatted_sql)




#redshift - explore

 
