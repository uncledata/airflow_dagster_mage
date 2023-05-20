from dagster import (
    Definitions,
    define_asset_job,
    ScheduleDefinition,
)

from dagster_dbt import dbt_cli_resource
from dagster_dbt import load_assets_from_dbt_project
from dagster_aws.s3.resources import s3_resource
from dagster import file_relative_path
import requests
import os

from .assets.taxi.extract import yellow_tripdata_raw
from .assets.taxi.transform import clean, dirty
from .assets.taxi.dbt import dbt_assets, dbt_seeds


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
    assets=[yellow_tripdata_raw, clean, dirty, dbt_seeds]+dbt_assets,
    resources={"s3": s3, "dbt": dbt_cli_resource.configured(
        {
            "project_dir":DBT_PROJECT_PATH,
            "profiles_dir": DBT_PROFILES,
        },
    ),},
    schedules=[monthly_refresh_schedule],
)


