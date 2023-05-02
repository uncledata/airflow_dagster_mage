import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import boto3
from mage_ai.data_preparation.shared.secrets import get_secret_value

@data_loader
def load_data_from_api(*args, **kwargs):
    bucket = "tomas-data-lake"
    prefix_raw = "yellow_taxi/raw"
    dt = '2022-12' #kwargs['execution_date'].strftime('%Y-%m')
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{dt}.parquet"
    key_name = (
        f"{prefix_raw}/{dt}/yellow_taxi_{dt}.parquet"
    )
    r = requests.get(url)
    data = r.content
    
    boto3.client('s3',
        aws_access_key_id=get_secret_value('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=get_secret_value('AWS_SECRET_ACCESS_KEY')
        ).put_object(Bucket=bucket, Key=key_name, Body=data)
