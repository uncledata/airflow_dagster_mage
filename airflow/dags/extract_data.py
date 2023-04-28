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

        dt = kwargs['data_interval_start'].format('Y-MM')
        s3_details = BaseHook.get_connection(conn_id="aws_default")

        columns_types = {
            'VendorID': 'int',
            'tpep_pickup_datetime': 'timestamp',
            'tpep_dropoff_datetime': 'timestamp',
            'passenger_count': 'int',
            'trip_distance': 'float',
            'RateCodeID': 'int',
            'store_and_fwd_flag': 'string',
            'PULocationID': 'int',
            'DOLocationID': 'int',
            'payment_type': 'int',
            'fare_amount': 'float',
            'extra': 'float',
            'mta_tax': 'float',
            'tip_amount': 'float',
            'tolls_amount': 'float',
            'improvement_surcharge': 'float',
            'total_amount': 'float',
            'congestion_surcharge': 'float',
            'airport_fee': 'float'
        }

        columns_names_mapping = {
            'VendorID': 'vendor_id',
            'tpep_pickup_datetime': 'tpep_pickup_datetime',
            'tpep_dropoff_datetime': 'tpep_dropoff_datetime',
            'passenger_count': 'passenger_count',
            'trip_distance': 'trip_distance',
            'RateCodeID': 'rate_code_id',
            'store_and_fwd_flag': 'store_and_fwd_flag',
            'PULocationID': 'pu_location_id',
            'DOLocationID': 'do_loation_id',
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
        columns_list_to_extract = ",".join([key +' as ' + columns_names_mapping[key] for key in columns_names_mapping])

        clean_data_condition = """VendorID in (1,2) and RateCodeID in (1,2,3,4,5,6) 
        and Store_and_fwd_flag in ('Y', 'N') and Payment_type in (1,2,3,4,5,6) and Trip_distance > 0 and Fare_amount > 0
        and tpep_pickup_datetime is not null and tpep_dropoff_datetime is not null and passenger_count > 0"""

        con.sql(f"""create view clean_data as 
            select 
                {columns_list_to_extract},
                {dt} as row_belongs_to_period 
            from 
                raw_events 
            where 
                {clean_data_condition};""" )

        con.sql(f"""create view dirty_data as 
            select 
                {columns_list_to_extract},
                {dt} as row_belongs_to_period 
            from
              raw_events
            where
                not ({clean_data_condition});""" )

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
