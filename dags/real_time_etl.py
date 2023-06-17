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
import os


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def load_data_fun_standard(table_name, **kwargs):
    csv_file_path = os.path.join('/opt/airflow/dags/data', f'{table_name}.csv')
    i = 0
    with open(csv_file_path, 'r') as f:
        reader = csv.DictReader(f)
        for item in reader:
            i += 1
            data = dict(item)
            sql_query = f'INSERT INTO dbs.{table_name} ({",".join(data.keys())}) VALUES ({",".join(data.values())})'
            postgres_operator = PostgresOperator(task_id=f'{table_name}_load_data_' + str(i),
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

    table_name = 'D_CUSTOMER'
    task_load_data_customer = PythonOperator(
        task_id='task_load_data_customer',
        python_callable=load_data_fun_standard,
        op_args=[table_name],
        provide_context=True,
        op_kwargs={'params': {
            f'csv_file_path': '/opt/airflow/dags/data/{table_name}.csv'}},
    )

    table_name = 'D_WAREHOUSE'
    task_load_data_warehouse = PythonOperator(
        task_id='task_load_data_warehouse',
        python_callable=load_data_fun_standard,
        op_args=[table_name],
        provide_context=True,
        op_kwargs={'params': {
            f'csv_file_path': '/opt/airflow/dags/data/{table_name}.csv'}},
    )

    table_name = 'D_DELIVERY'
    task_load_data_delivery = PythonOperator(
        task_id='task_load_data_delivery',
        python_callable=load_data_fun_standard,
        op_args=[table_name],
        provide_context=True,
        op_kwargs={'params': {
            f'csv_file_path': '/opt/airflow/dags/data/{table_name}.csv'}},
    )

    table_name = 'D_PAYMENT'
    task_load_data_payment = PythonOperator(
        task_id='task_load_data_payment',
        python_callable=load_data_fun_standard,
        op_args=[table_name],
        provide_context=True,
        op_kwargs={'params': {
            f'csv_file_path': '/opt/airflow/dags/data/{table_name}.csv'}},
    )

    table_name = 'D_DATE'
    task_load_data_date = PythonOperator(
        task_id='task_load_data_date',
        python_callable=load_data_fun_standard,
        op_args=[table_name],
        provide_context=True,
        op_kwargs={'params': {
            f'csv_file_path': '/opt/airflow/dags/data/{table_name}.csv'}},
    )

    table_name = 'D_SHIPP_COMPANY'
    task_load_data_shipp_company = PythonOperator(
        task_id='task_load_data_shipp_company',
        python_callable=load_data_fun_standard,
        op_args=[table_name],
        provide_context=True,
        op_kwargs={'params': {
            f'csv_file_path': '/opt/airflow/dags/data/{table_name}.csv'}},
    )

    table_name = 'F_LOGISTIC'
    task_load_data_fact_logistic = PythonOperator(
        task_id='task_load_data_fact_logistic',
        python_callable=load_data_fun_standard,
        op_args=[table_name],
        provide_context=True,
        op_kwargs={'params': {
            f'csv_file_path': '/opt/airflow/dags/data/{table_name}.csv'}},
    )

    truncat_f_logistic = PostgresOperator(
        task_id='truncat_f_logistic',
        postgres_conn_id='postgres_default',
        sql="TRUNCATE TABLE dbs.F_LOGISTIC CASCADE",
    )
    truncat_d_customer = PostgresOperator(
        task_id='truncat_d_customer',
        postgres_conn_id='postgres_default',
        sql="TRUNCATE TABLE dbs.D_CUSTOMER CASCADE",
    )
    truncat_d_payment = PostgresOperator(
        task_id='truncat_d_payment',
        postgres_conn_id='postgres_default',
        sql="TRUNCATE TABLE dbs.D_PAYMENT CASCADE",
    )
    truncat_d_date = PostgresOperator(
        task_id='truncat_d_date',
        postgres_conn_id='postgres_default',
        sql="TRUNCATE TABLE dbs.D_DATE CASCADE",
    )
    truncat_d_shippcom = PostgresOperator(
        task_id='truncat_d_shippcom',
        postgres_conn_id='postgres_default',
        sql="TRUNCATE TABLE dbs.D_SHIPP_COMPANY CASCADE",
    )
    truncat_d_delivery = PostgresOperator(
        task_id='truncat_d_delivery',
        postgres_conn_id='postgres_default',
        sql="TRUNCATE TABLE dbs.D_DELIVERY CASCADE",
    )
    truncat_d_warehouse = PostgresOperator(
        task_id='truncat_d_warehouse',
        postgres_conn_id='postgres_default',
        sql="TRUNCATE TABLE dbs.D_WAREHOUSE CASCADE",
    )

    truncat_f_logistic >> truncat_d_customer >> truncat_d_payment >> \
        truncat_d_date >> truncat_d_shippcom >> truncat_d_delivery >> truncat_d_warehouse >> \
        task_load_data_customer >> task_load_data_warehouse >> task_load_data_delivery >> \
        task_load_data_payment >> task_load_data_shipp_company >> task_load_data_date >> \
        task_load_data_fact_logistic
