#rewrite airflow dag to dagster that is located in ./airflow/dags/extract_data.py
#
#dagster equivalent
#write a dagster pipeline that would download a file from api and save it to s3
#
#dagster equivalent
#write a dagster pipeline that would download a file from api and save it to s3

from dagster import pipeline, solid

import requests
import json
import os
import boto3
import botocore

@solid
def download_file(context):
    #download file from api
    dt = context.solid_config['data_interval_start'].format('%Y-%m')
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{dt}.parquet'.format(dt=dt)
    r = requests.get(url)
    data = json.loads(r.text)
    return data

@solid
def save_to_s3(context, data):
    #save file to s3
    dt = context.solid_config['data_interval_start'].format('%Y-%m')
    s3 = boto3.resource('s3')
    s3.Bucket('bucket_name').put_object(Key='yellow_taxi_{dt}'.format(dt=dt), Body=data)

@pipeline
def extract_data():
    save_to_s3(download_file())
