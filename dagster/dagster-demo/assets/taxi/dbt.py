from dagster_dbt import load_assets_from_dbt_project
from dagster import file_relative_path
from dagster import asset

from .commons import DBT_PROJECT_PATH, DBT_PROFILES

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH, profiles_dir=DBT_PROFILES, key_prefix=["yellow_taxi"] # If you want seeds: , use_build_command=True
)
import os
@asset()
def dbt_seeds():
    os.system(f"dbt seed --profiles-dir {DBT_PROFILES} --project-dir {DBT_PROJECT_PATH}")
    