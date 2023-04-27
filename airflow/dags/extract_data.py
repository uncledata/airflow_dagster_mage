#write a dag that would download a file from api and save it to s3

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import BaseHook

from airflow import Dataset


from datetime import datetime, timedelta
import requests


#default args
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

#dag
@dag('extract_data', default_args=default_args, schedule_interval='@monthly', start_date=datetime(2022, 11, 30), end_date=datetime(2023,1,1), catchup=True)
def extract_data():

    bucket = 'tomas-data-lake'
    prefix_raw = 'yellow_taxi/raw'
    prefix_clean = 'yellow_taxi/clean'
    prefix_dirty = 'yellow_taxi/dirty'

    download_dir = Dataset(f"s3://{bucket}/{prefix_raw}/")
    clean_dir = Dataset(f"s3://{bucket}/{prefix_clean}/")
    dirty_dir = Dataset(f"s3://{bucket}/{prefix_dirty}/")
    @task(outlets=[download_dir])
    def download_save_file(bucket_name, prefix_raw, **kwargs):
        #download file from api
        dt = kwargs['data_interval_start'].format('Y-MM')
        print(dt)
        key_name = f'{prefix_raw}/{dt}/yellow_taxi_{dt}.parquet'
        url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{dt}.parquet'.format(dt=dt)
        print(url)
        r = requests.get(url)
        print(r)
        data = r.content
        hook = S3Hook('aws_default')
        file_name = f's3://{bucket_name}/{key_name}'

        hook.load_bytes(data, key=key_name, bucket_name=bucket_name)
        return file_name
   


    @task(outlets=[clean_dir, dirty_dir])
    def clean_and_split_data(url_to_parquet_file, bucket, prefix_clean, prefix_dirty, **kwargs):
        import duckdb
        con = duckdb.connect(database=":memory:", read_only=False)

        print (url_to_parquet_file)
        dt = kwargs['data_interval_start'].format('Y-MM')
        s3_details = BaseHook.get_connection(conn_id="aws_default")
        print(f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_access_key_id='{s3_details.login}';
            SET s3_secret_access_key='{s3_details.password}';
            SET s3_use_ssl=false;
            CREATE VIEW raw_events AS
            SELECT *
            FROM read_parquet('{url_to_parquet_file}');
        """)

        con.sql(f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_access_key_id='{s3_details.login}';
            SET s3_secret_access_key='{s3_details.password}';
            SET s3_region='eu-central-1';
            SET s3_use_ssl=false;
            CREATE VIEW raw_events AS
            SELECT *
            FROM read_parquet('{url_to_parquet_file}');
        """)
        print("exec done")
        con.sql("""create view clean_data as select * from raw_events""" )
        print("clean done")
        con.sql("""create view dirty_data as select * from raw_events""" )
        print("dirty done")
        con.sql(f"""
            COPY clean_data TO 's3://{bucket}/{prefix_clean}/{dt}/yellow_taxi_{dt}.parquet' (FORMAT PARQUET, CODEC SNAPPY);
        """)

        con.sql(f"""
            COPY dirty_data TO 's3://{bucket}/{prefix_dirty}/{dt}/yellow_taxi_{dt}.parquet' (FORMAT PARQUET, CODEC SNAPPY);
        """)


    clean_and_split_data(url_to_parquet_file=download_save_file(bucket_name=bucket, prefix_raw=prefix_raw),
                         bucket=bucket, 
                         prefix_clean=prefix_clean, 
                         prefix_dirty=prefix_dirty)

extract_data()
