
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from common_functions import append_data_to_mysql

with DAG('temperature_data_append_dag', start_date=datetime.now(), schedule_interval=None, template_searchpath='/opt/airflow/csv_files') as dag:
    append_data = PythonOperator(
        task_id='append_data',
        python_callable=append_data_to_mysql,
        op_kwargs={'table_name': 'temperature_data', 'file_path': '/opt/airflow/csv_files/temperature_data_{{ ds_nodash }}.csv', 'mode': 'append'},
    )
