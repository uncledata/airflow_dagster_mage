from dagster import MonthlyPartitionsDefinition
from dagster import file_relative_path

DBT_PROJECT_PATH = file_relative_path(__file__, "/opt/dagster/dwh")
DBT_PROFILES = file_relative_path(__file__, "/opt/dagster/")

partitions_def = MonthlyPartitionsDefinition(start_date="2022-12", fmt="%Y-%m")