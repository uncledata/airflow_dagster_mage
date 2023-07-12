import requests
import asyncio
from prefect import flow, task

from prefect_aws import AwsCredentials
from prefect_dask.task_runners import DaskTaskRunner
import os

@task
def yellow_tripdata_raw(month: str = "2023-01"):
    bucket_name = "tomas-data-lake"
    prefix_raw = "yellow_taxi/raw"

    partition_date_str = month
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{partition_date_str}.parquet"
    key_name = (
        f"{prefix_raw}/{partition_date_str}/yellow_taxi_{partition_date_str}.parquet"
    )
    r = requests.get(url)
    data = r.content

    aws_credentials_block = AwsCredentials.load("s3")

    s3 = aws_credentials_block.get_s3_client()

    s3_path = s3.put_object(Bucket=bucket_name, Key=key_name, Body=data)
    return f's3://{bucket_name}/{key_name}'
    
def duckdb_query_export(table_name, prefix, bucket, is_dirty, url_to_parquet_file, dt):
        import duckdb
        con = duckdb.connect(database=":memory:", read_only=False)

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

        con.sql(f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}';
            SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}';
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

@task()
def dirty(url_to_parquet_file, bucket, prefix_dirty, **kwargs):
    duckdb_query_export(table_name='dirty_data', bucket=bucket, prefix=prefix_dirty, is_dirty=True, url_to_parquet_file=url_to_parquet_file, dt="2023-01")
    
@task()
def clean(url_to_parquet_file, bucket, prefix_clean, **kwargs):
    duckdb_query_export(table_name='clean_data', bucket=bucket, prefix=prefix_clean, is_dirty=False, url_to_parquet_file=url_to_parquet_file, dt="2023-01")



@task
def dbt_seed():
    os.system("dbt seed --profiles-dir /opt/prefect/dwh --project-dir /opt/prefect/dwh")

@task
def dbt_fct_trips():
    os.system("dbt run --select fct_trips --profiles-dir /opt/prefect/dwh --project-dir /opt/prefect/dwh")


@flow(task_runner=DaskTaskRunner(), name="extract_and_load_to_s3")
def extract_and_load_to_s3():
    extract = yellow_tripdata_raw()
    dirty_ds = dirty(url_to_parquet_file=extract, bucket="tomas-data-lake", prefix_dirty="yellow_taxi/dirty", wait_for=[extract])
    clean_ds = clean(url_to_parquet_file=extract, bucket="tomas-data-lake", prefix_clean="yellow_taxi/clean", wait_for=[extract])
    seeds = dbt_seed(wait_for=[extract])
    fct = dbt_fct_trips(wait_for=[clean_ds, seeds])


if __name__ == "__main__":
    extract_and_load_to_s3()
 