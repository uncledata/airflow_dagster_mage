from dagster import (
    asset,
    Definitions,
    MonthlyPartitionsDefinition,
    define_asset_job,
    ScheduleDefinition,
)

from dagster_dbt import dbt_cli_resource
from dagster_dbt import load_assets_from_dbt_project
from dagster_aws.s3.resources import s3_resource
from dagster import file_relative_path
import requests
import os

partitions_def = MonthlyPartitionsDefinition(start_date="2022-12", fmt="%Y-%m")


@asset(partitions_def=partitions_def, required_resource_keys={"s3"})
def yellow_tripdata_raw(context):
    bucket = "tomas-data-lake"
    prefix_raw = "yellow_taxi/raw"

    partition_date_str = context.asset_partition_key_for_output()
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{partition_date_str}.parquet"
    key_name = (
        f"{prefix_raw}/{partition_date_str}/yellow_taxi_{partition_date_str}.parquet"
    )
    r = requests.get(url)
    data = r.content
    s3_client = context.resources.s3
    s3_client.put_object(Bucket=bucket, Key=key_name, Body=data)
    

@asset(partitions_def=partitions_def, non_argument_deps={"yellow_tripdata_raw"}, key_prefix=["yellow_taxi"],)
def clean(
    context
):
    bucket = "tomas-data-lake"
    prefix_raw = "yellow_taxi/raw"
    prefix_clean = "yellow_taxi/clean"
    prefix_dirty = "yellow_taxi/dirty"

    import duckdb
    partition_date_str = context.asset_partition_key_for_output()
    con = duckdb.connect(database=":memory:", read_only=False)
    url_to_parquet_file = f"s3://{bucket}/{prefix_raw}/{partition_date_str}/yellow_taxi_{partition_date_str}.parquet"
    columns_names_mapping = {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "tpep_pickup_datetime",
        "tpep_dropoff_datetime": "tpep_dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "RateCodeID": "rate_code_id",
        "store_and_fwd_flag": "store_and_fwd_flag",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        "payment_type": "payment_type",
        "fare_amount": "fare_amount",
        "extra": "extra",
        "mta_tax": "mta_tax",
        "tip_amount": "tip_amount",
        "tolls_amount": "tolls_amount",
        "improvement_surcharge": "improvement_surcharge",
        "total_amount": "total_amount",
        "congestion_surcharge": "congestion_surcharge",
        "airport_fee": "airport_fee",
    }
    # Why not OS and env vars - Airflow hides passwords from logs, you can't print them to view!
    con.sql(
        f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_access_key_id='{os.environ['S3_ACCESS_KEY_ID']}';
        SET s3_secret_access_key='{os.environ['S3_ACCESS_KEY_SECRET']}';
        SET s3_region='eu-central-1';
        SET s3_use_ssl=false;
        CREATE VIEW raw_events AS
        SELECT *
        FROM read_parquet('{url_to_parquet_file}');
    """
    )
    columns_list_to_extract = ",".join(
        [key + " as " + columns_names_mapping[key] for key in columns_names_mapping]
    )
    clean_data_condition = """VendorID in (1,2) and RateCodeID in (1,2,3,4,5,6) 
    and Store_and_fwd_flag in ('Y', 'N') and Payment_type in (1,2,3,4,5,6) and Trip_distance > 0 and Fare_amount > 0
    and tpep_pickup_datetime is not null and tpep_dropoff_datetime is not null and passenger_count > 0"""
    con.sql(
        f"""create view clean_data as 
        select 
            {columns_list_to_extract},
            {partition_date_str} as row_belongs_to_period 
        from 
            raw_events 
        where 
            {clean_data_condition};"""
    )

    con.sql(
        f"""
        COPY clean_data TO 's3://{bucket}/{prefix_clean}/yellow_taxi_{partition_date_str}.parquet' (FORMAT PARQUET, CODEC SNAPPY);
    """
    )


@asset(partitions_def=partitions_def, non_argument_deps={"yellow_tripdata_raw"},)
def dirty(
    context
):
    bucket = "tomas-data-lake"
    prefix_raw = "yellow_taxi/raw"
    prefix_clean = "yellow_taxi/clean"
    prefix_dirty = "yellow_taxi/dirty"

    import duckdb
    partition_date_str = context.asset_partition_key_for_output()
    con = duckdb.connect(database=":memory:", read_only=False)
    url_to_parquet_file = f"s3://{bucket}/{prefix_raw}/{partition_date_str}/yellow_taxi_{partition_date_str}.parquet"
    columns_names_mapping = {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "tpep_pickup_datetime",
        "tpep_dropoff_datetime": "tpep_dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "RateCodeID": "rate_code_id",
        "store_and_fwd_flag": "store_and_fwd_flag",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        "payment_type": "payment_type",
        "fare_amount": "fare_amount",
        "extra": "extra",
        "mta_tax": "mta_tax",
        "tip_amount": "tip_amount",
        "tolls_amount": "tolls_amount",
        "improvement_surcharge": "improvement_surcharge",
        "total_amount": "total_amount",
        "congestion_surcharge": "congestion_surcharge",
        "airport_fee": "airport_fee",
    }
    # Why not OS and env vars - Airflow hides passwords from logs, you can't print them to view!
    con.sql(
        f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_access_key_id='{os.environ['S3_ACCESS_KEY_ID']}';
        SET s3_secret_access_key='{os.environ['S3_ACCESS_KEY_SECRET']}';
        SET s3_region='eu-central-1';
        SET s3_use_ssl=false;
        CREATE VIEW raw_events AS
        SELECT *
        FROM read_parquet('{url_to_parquet_file}');
    """
    )
    columns_list_to_extract = ",".join(
        [key + " as " + columns_names_mapping[key] for key in columns_names_mapping]
    )
    clean_data_condition = """VendorID in (1,2) and RateCodeID in (1,2,3,4,5,6) 
    and Store_and_fwd_flag in ('Y', 'N') and Payment_type in (1,2,3,4,5,6) and Trip_distance > 0 and Fare_amount > 0
    and tpep_pickup_datetime is not null and tpep_dropoff_datetime is not null and passenger_count > 0"""

    con.sql(
        f"""create view dirty_data as 
        select 
            {columns_list_to_extract},
            {partition_date_str} as row_belongs_to_period 
        from
          raw_events
        where
            not ({clean_data_condition});"""
    )

    con.sql(
        f"""
        COPY dirty_data TO 's3://{bucket}/{prefix_dirty}/yellow_taxi_{partition_date_str}.parquet' (FORMAT PARQUET, CODEC SNAPPY);
    """
    )


monthly_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="all_assets_job"),
    cron_schedule="0 0 5 * *",
)

s3 = s3_resource.configured(
    {
        "aws_access_key_id": {"env": "S3_ACCESS_KEY_ID"},
        "aws_secret_access_key": {"env": "S3_ACCESS_KEY_SECRET"},
    },
)

DBT_PROJECT_PATH = file_relative_path(__file__, "/opt/dagster/dwh")
DBT_PROFILES = file_relative_path(__file__, "/opt/dagster/")



dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH, profiles_dir=DBT_PROFILES, key_prefix=["yellow_taxi"] # If you want seeds: , use_build_command=True
)

defs = Definitions(
    assets=[yellow_tripdata_raw, clean, dirty]+dbt_assets,
    resources={"s3": s3, "dbt": dbt_cli_resource.configured(
        {
            "project_dir":DBT_PROJECT_PATH,
            "profiles_dir": DBT_PROFILES,
        },
    ),},
    schedules=[monthly_refresh_schedule],
)


