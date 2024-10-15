
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from common_functions import load_csv_to_mysql

with DAG('temperature_data_load_dag', start_date=datetime.now(), schedule_interval=None, template_searchpath='/opt/airflow/csv_files') as dag:
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_csv_to_mysql,
        op_kwargs={'table_name': 'temperature_data', 'file_path': '/opt/airflow/csv_files/temperature_data_{{ ds_nodash }}.csv', 'mode': 'replace'},
    )
