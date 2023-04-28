FROM apache/airflow:2.5.3-python3.10
USER root
COPY requirements.txt requirements.txt
RUN apt-get update -q && apt-get install gcc openssl git -y
COPY .env /opt/airflow/.env
RUN chmod 777 /opt/airflow/.env
RUN . /opt/airflow/.env
USER airflow
RUN pip install --no-cache-dir -r requirements.txt
