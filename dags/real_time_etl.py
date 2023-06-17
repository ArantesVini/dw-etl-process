import csv
import airflow
import time
import pandas as pd
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def extarct_csv_files():
    values = []
    with open('data/D_CUSTOMER.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            values.append(tuple(row))


with DAG(
        dag_id='real_time_etl',
        default_args=default_args,
        schedule_interval='0 0 * * *',
        dagrun_timeout=timedelta(minutes=60),
        description='ETL Job to load data in a DW with Airflow',
        start_date=airflow.utils.dates.days_ago(1),) as dag:

    read_csv = PythonOperator(
        task_id='read_csv',
        python_callable=extarct_csv_files,
    )

    load_data = PostgresOperator(
        task_id='load_data',
        sql='''INSERT INTO dbs.D_CUSTOMER VALUES 
            (customer_id,
            customer_name,
            customer_surname,)
            VALUES (%s, %s, %s)''',
        postgres_conn_id='postgres_default',
        params=read_csv.python_callable(),
    )

    read_csv >> load_data
