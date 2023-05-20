FROM apache/airflow:2.5.3-python3.10
USER root
COPY requirements.txt requirements.txt
RUN apt-get update -q && apt-get install gcc openssl git -y

USER airflow
RUN pip install --no-cache-dir -r requirements.txt
