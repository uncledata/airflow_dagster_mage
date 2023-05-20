from dagster import asset
from .commons import partitions_def

import requests


@asset(partitions_def=partitions_def, required_resource_keys={"s3"}, non_argument_deps={"dbt_seeds"})
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