#write a dag that would download a file from api and save it to s3

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.base_aws import BaseHook

from airflow import Dataset

import os

from datetime import datetime, timedelta
import requests


#default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#dag
@dag('extract_data', default_args=default_args, schedule_interval='@monthly', start_date=datetime(2022, 11, 30), end_date=datetime(2022,12,31), catchup=True)
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

    def duckdb_query_export(table_name, prefix, bucket, is_dirty, url_to_parquet_file, dt):
        import duckdb
        con = duckdb.connect(database=":memory:", read_only=False)
        s3_details = BaseHook.get_connection(conn_id="aws_default")

        columns_names_mapping = {
            'VendorID': 'vendor_id',
            'tpep_pickup_datetime': 'tpep_pickup_datetime',
            'tpep_dropoff_datetime': 'tpep_dropoff_datetime',
            'passenger_count': 'passenger_count',
            'trip_distance': 'trip_distance',
            'RateCodeID': 'rate_code_id',
            'store_and_fwd_flag': 'store_and_fwd_flag',
            'PULocationID': 'pu_location_id',
            'DOLocationID': 'do_location_id',
            'payment_type': 'payment_type',
            'fare_amount': 'fare_amount',
            'extra': 'extra',
            'mta_tax': 'mta_tax',
            'tip_amount': 'tip_amount',
            'tolls_amount': 'tolls_amount',
            'improvement_surcharge': 'improvement_surcharge',
            'total_amount': 'total_amount',
            'congestion_surcharge': 'congestion_surcharge',
            'airport_fee': 'airport_fee',
        }

        # Why not OS and env vars - Airflow hides passwords from logs, you can't print them to view!
        con.sql(f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_access_key_id='{s3_details.login}';
            SET s3_secret_access_key='{s3_details.password}';
            SET s3_region='eu-central-1';
            SET s3_use_ssl=true;
            CREATE VIEW raw_events AS
            SELECT *
            FROM read_parquet('{url_to_parquet_file}');
        """)
        columns_list_to_extract = ",".join([key +' as ' + columns_names_mapping[key] for key in columns_names_mapping])
        clean_data_condition = """VendorID in (1,2) and RateCodeID in (1,2,3,4,5,6) 
        and Store_and_fwd_flag in ('Y', 'N') and Payment_type in (1,2,3,4,5,6) and Trip_distance > 0 and Fare_amount > 0
        and tpep_pickup_datetime is not null and tpep_dropoff_datetime is not null and passenger_count > 0"""

        con.sql(f"""create view {table_name} as 
            select 
                {columns_list_to_extract},
                {dt} as row_belongs_to_period 
            from
              raw_events
            where
                {'' if not is_dirty else 'not'} ({clean_data_condition});""" )
        con.sql(f"""
            COPY {table_name} TO 's3://{bucket}/{prefix}/yellow_taxi_{dt}.parquet';
        """)

    @task(outlets=[dirty_dir])
    def dirty(url_to_parquet_file, bucket, prefix_dirty, **kwargs):
       duckdb_query_export(table_name='dirty_data', bucket=bucket, prefix=prefix_dirty, is_dirty=True, url_to_parquet_file=url_to_parquet_file, dt=kwargs['data_interval_start'].format('Y-MM'))
    
    @task(outlets=[clean_dir])
    def clean(url_to_parquet_file, bucket, prefix_clean, **kwargs):
        duckdb_query_export(table_name='clean_data', bucket=bucket, prefix=prefix_clean, is_dirty=False, url_to_parquet_file=url_to_parquet_file, dt=kwargs['data_interval_start'].format('Y-MM'))

    url_to_parquet_file=download_save_file(bucket_name=bucket, prefix_raw=prefix_raw)
    dirty(url_to_parquet_file=url_to_parquet_file,
                         bucket=bucket, 
                         prefix_dirty=prefix_dirty)
    clean(url_to_parquet_file=url_to_parquet_file,
                         bucket=bucket, 
                         prefix_clean=prefix_clean)

extract_data()
