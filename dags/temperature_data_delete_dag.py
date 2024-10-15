
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import logging
from common_functions import delete_table_from_mysql

with DAG('temperature_data_delete_dag', start_date=datetime.now(), schedule_interval=None, template_searchpath='/opt/airflow/csv_files') as dag:
    delete_data = PythonOperator(
        task_id='delete_data',
        python_callable=delete_table_from_mysql,
        op_kwargs={'table_name': 'temperature_data'},
    )
