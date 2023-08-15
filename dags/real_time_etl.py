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
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def load_data_fun_standard(table_name, **kwargs):
    csv_file_path = os.path.join("/opt/airflow/dags/data", f"{table_name}.csv")
    i = 0
    with open(csv_file_path, "r") as f:
        reader = csv.DictReader(f)
        for item in reader:
            i += 1
            data = dict(item)
            sql_query = f'INSERT INTO dbs.{table_name} ({",".join(data.keys())}) VALUES ({",".join(data.values())})'
            postgres_operator = PostgresOperator(
                task_id=f"{table_name}_load_data_" + str(i),
                sql=sql_query,
                params=(data),
                postgres_conn_id="postgres_default",
                dag=dag,
            )
            postgres_operator.execute(context=kwargs)


with DAG(
    dag_id="real_time_etl",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    dagrun_timeout=timedelta(minutes=60),
    description="ETL Job to load data in a DW with Airflow",
    start_date=airflow.utils.dates.days_ago(1),
) as dag:
    table_names = [
        "D_CUSTOMER",
        "D_WAREHOUSE",
        "D_DELIVERY",
        "D_PAYMENT",
        "D_DATE",
        "D_SHIPP_COMPANY",
        "F_LOGISTIC",
    ]

    load_data_tasks = []
    for table_name in table_names:
        csv_file_path = f"/opt/airflow/dags/data/{table_name}.csv"
        task_id = f"task_load_data_{table_name.lower()}"

        load_data_task = PythonOperator(
            task_id=task_id,
            python_callable=load_data_fun_standard,
            op_args=[table_name],
            provide_context=True,
            op_kwargs={"params": {"csv_file_path": csv_file_path}},
            dag=dag,
        )
        load_data_tasks.append(load_data_task)

    truncate_tasks = []
    for table_name in table_names:
        truncate_task = PostgresOperator(
            task_id=f"truncate_{table_name.lower()}",
            postgres_conn_id="postgres_default",
            sql=f"TRUNCATE TABLE dbs.{table_name} CASCADE",
            dag=dag,
        )
        truncate_tasks.append(truncate_task)

    truncate_tasks[-1].set_downstream(load_data_tasks[0])
    for i in range(len(truncate_tasks) - 1):
        truncate_tasks[i].set_downstream(truncate_tasks[i + 1])

    load_data_tasks[0].set_upstream(truncate_tasks[-1])
    for i in range(len(load_data_tasks) - 1):
        load_data_tasks[i].set_downstream(load_data_tasks[i + 1])
