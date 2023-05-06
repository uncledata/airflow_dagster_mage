from airflow import DAG
from airflow.decorators import dag

from airflow.operators.bash import BashOperator


from airflow import Dataset

from datetime import datetime, timedelta


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
bucket = 'tomas-data-lake'
prefix_clean = 'yellow_taxi/clean'
clean_dir = Dataset(f"s3://{bucket}/{prefix_clean}/")
#dag
@dag('dbt_dag', default_args=default_args, schedule=[clean_dir])
def dbt_dag():
    # bash operator to run dbt seeds
    fct_trips = BashOperator(
        task_id='fct_trips',
        bash_command='cd /opt/airflow/dwh && dbt run --models fct_trips --profiles-dir /opt/airflow',
        outlets = [Dataset('duckdb://fct_trips')]
        )
dbt_dag()