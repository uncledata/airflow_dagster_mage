#write a dag that would extract a table from Postgres DB and load it into a csv file to S3

from airflow import DAG
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
import pandas as pd

#default arguments
default_args = {

    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#create a dag with decorators. Dag should extract the data from postgres and load it into a csv file to S3

@dag(
    default_args=default_args,
    schedule_interval='@daily', 
    start_date=days_ago(1),
)
def example_dag():
    @task
    def extract_data():
        import psycopg2
        import pandas as pd
        import os
        #get the connection string from the .env file
        conn_string = os.getenv('CONN_STRING')
        #create the connection
        conn = psycopg2.connect(conn_string)
        #create the cursor
        cur = conn.cursor()
        #execute the query
        cur.execute('SELECT * FROM table')
        #fetch the data
        data = cur.fetchall()
        #create a dataframe
        df = pd.DataFrame(data)
        #close the connection
        conn.close()
        #return the dataframe
        return df
    
    @task
    def load_data(df):
        #load the dataframe to S3
        #create the connection to S3
        s3 = boto3.client('s3')
        #write the dataframe to S3
        df.to_csv('s3://bucket_name/filename.csv')
        #return the dataframe
        return df
    
    @task
    def check_data(df):
        #check if the file exists in S3
        #create the connection to S3
        s3 = boto3.client('s3')
        #check if the file exists
        s3.head_object(Bucket='bucket_name', Key='filename.csv')
        #return the dataframe
        return df

    extract_data = extract_data()
    load_data = load_data(extract_data)
    check_data = check_data(load_data)

