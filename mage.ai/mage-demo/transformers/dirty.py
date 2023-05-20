if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import os
from mage_ai.data_preparation.shared.secrets import get_secret_value

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


@transformer
def transform(*args, **kwargs):
    bucket = "tomas-data-lake"
    prefix_raw = "yellow_taxi/raw"
    prefix_dirty = "yellow_taxi/dirty"
    dt = kwargs['execution_date'].strftime('%Y-%m')
    url_to_parquet_file = f"s3://{bucket}/{prefix_raw}/{dt}/yellow_taxi_{dt}.parquet"
    duckdb_query_export(table_name='dirty_data', bucket=bucket, prefix=prefix_dirty, is_dirty=True, url_to_parquet_file=url_to_parquet_file, dt=dt)
