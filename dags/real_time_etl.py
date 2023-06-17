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


def load_data_fun(**kwargs):

    csv_file_path = kwargs['params']['csv_file_path']
    i = 0
    with open(csv_file_path, 'r') as f:
        reader = csv.DictReader(f)
        for item in reader:
            i += 1
            data = dict(item)
            sql_query = "INSERT INTO dbs.D_CUSTOMER (%s) VALUES (%s)" % (
                ','.join(data.keys()), ','.join([item for item in data.values()]))
            postgres_operator = PostgresOperator(task_id='load_data_' + str(i),
                                                 sql=sql_query,
                                                 params=(data),
                                                 postgres_conn_id='postgres_default',
                                                 dag=dag)
            postgres_operator.execute(context=kwargs)


with DAG(
        dag_id='real_time_etl',
        default_args=default_args,
        schedule_interval='0 0 * * *',
        dagrun_timeout=timedelta(minutes=60),
        description='ETL Job to load data in a DW with Airflow',
        start_date=airflow.utils.dates.days_ago(1),) as dag:

    load_data_task = PythonOperator(
        task_id='load_data_task',
        python_callable=load_data_fun,
        provide_context=True,
        op_kwargs={'params': {
            'csv_file_path': '/opt/airflow/dags/data/D_CUSTOMER.csv'}},
    )

    load_data_task
