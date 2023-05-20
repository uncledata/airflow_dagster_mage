from .commons import partitions_def
from dagster import asset
import os

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

        # Why not OS and env vars - Airflow hides passwords from logs, you can't print them to view!
        con.sql(f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_access_key_id='{os.environ['S3_ACCESS_KEY_ID']}';
            SET s3_secret_access_key='{os.environ['S3_ACCESS_KEY_SECRET']}';
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

@asset(partitions_def=partitions_def, non_argument_deps={"yellow_tripdata_raw"}, key_prefix=["yellow_taxi"],)
def clean(
    context
):
    partition_date_str = context.asset_partition_key_for_output()
    bucket = "tomas-data-lake"
    prefix_raw = "yellow_taxi/raw"
    prefix_clean = "yellow_taxi/clean"
    url_to_parquet_file = f"s3://{bucket}/{prefix_raw}/{partition_date_str}/yellow_taxi_{partition_date_str}.parquet"
    duckdb_query_export(table_name='dirty_data', bucket=bucket, prefix=prefix_clean, is_dirty=False, url_to_parquet_file=url_to_parquet_file, dt=partition_date_str)


@asset(partitions_def=partitions_def, non_argument_deps={"yellow_tripdata_raw"},)
def dirty(
    context
):
    partition_date_str = context.asset_partition_key_for_output()
    bucket = "tomas-data-lake"
    prefix_raw = "yellow_taxi/raw"
    prefix_dirty = "yellow_taxi/dirty"
    url_to_parquet_file = f"s3://{bucket}/{prefix_raw}/{partition_date_str}/yellow_taxi_{partition_date_str}.parquet"
    duckdb_query_export(table_name='dirty_data', bucket=bucket, prefix=prefix_dirty, is_dirty=True, url_to_parquet_file=url_to_parquet_file, dt=partition_date_str)

