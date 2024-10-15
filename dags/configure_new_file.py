import re
import os
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.models import Variable
import importlib

# Import common functions module
common_functions = importlib.import_module('common_functions')

# Set up default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def sanitize_column_name(column_name):
    sanitized_name = column_name.replace(' ', '_').replace('(', '_').replace(')', '_').replace('°', '')
    return sanitized_name

def parse_csv_and_determine_ddl(csv_prefix, **kwargs):
    csv_folder = Variable.get("csv_folder", default_var="/opt/airflow/csv_files")
    
    # Get all CSV files in the folder
    files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]
    matching_files = [f for f in files if f.startswith(csv_prefix)]
    if not matching_files:
        raise ValueError("No matching CSV files found")
    
    # Find the latest file by date pattern
    latest_file = max(matching_files, key=lambda x: datetime.strptime(x, f"{csv_prefix}_%Y%m%d.csv"))
    file_path = os.path.join(csv_folder, latest_file)
    
    # Extract the table name by removing date part (e.g., _20240101) using regex
    match = re.match(r'^(.*?)(?:_\d{4}[-_]?\d{2}[-_]?\d{2})?\.csv$', latest_file)
    if match:
        raw_table_name = match.group(1)
        table_name = raw_table_name.lower()  # Convert to lowercase
    else:
        raise ValueError(f"Could not parse table name from file: {latest_file}")
    
    # Load the CSV to determine column types
    df = pd.read_csv(file_path)
    column_types = df.dtypes
    
    # Create the DDL statement with lowercase column names
    ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    for column, dtype in column_types.items():
        column_name = sanitize_column_name(column.lower())  # Lowercase column names
        if dtype == 'object':
            ddl += f"    {column_name} VARCHAR(255),\n"
        elif dtype == 'int64':
            ddl += f"    {column_name} INT,\n"
        elif dtype == 'float64':
            ddl += f"    {column_name} FLOAT,\n"
        else:
            ddl += f"    {column_name} VARCHAR(255),\n"
    ddl = ddl.rstrip(',\n') + "\n);"
    
    return {
        'ddl': ddl,
        'table_name': table_name,
        'file_path': file_path
    }

def create_dynamic_dags(**kwargs):
    ti = kwargs['ti']
    ddl_info = ti.xcom_pull(task_ids='parse_csv_task')
    
    table_name = ddl_info['table_name']
    file_template = os.path.join('/opt/airflow/csv_files', f"{table_name}_{{{{ ds_nodash }}}}.csv")
    
    # Create dynamic Load DAG
    load_dag_id = f"{table_name}_load_dag"
    load_dag_code = f"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from common_functions import load_csv_to_mysql

with DAG('{load_dag_id}', start_date=datetime.now(), schedule_interval=None, template_searchpath='/opt/airflow/csv_files') as dag:
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_csv_to_mysql,
        op_kwargs={{'table_name': '{table_name}', 'file_path': '{file_template}', 'mode': 'replace'}},
    )
"""
    # Save the Load DAG to a file
    load_dag_file_path = os.path.join(Variable.get("airflow_dags_folder"), f"{load_dag_id}.py")
    with open(load_dag_file_path, 'w') as f:
        f.write(load_dag_code)
    
    # Create dynamic Append DAG
    append_dag_id = f"{table_name}_append_dag"
    append_dag_code = f"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from common_functions import append_data_to_mysql

with DAG('{append_dag_id}', start_date=datetime.now(), schedule_interval=None, template_searchpath='/opt/airflow/csv_files') as dag:
    append_data = PythonOperator(
        task_id='append_data',
        python_callable=append_data_to_mysql,
        op_kwargs={{'table_name': '{table_name}', 'file_path': '{file_template}', 'mode': 'append'}},
    )
"""
    # Save the Append DAG to a file
    append_dag_file_path = os.path.join(Variable.get("airflow_dags_folder"), f"{append_dag_id}.py")
    with open(append_dag_file_path, 'w') as f:
        f.write(append_dag_code)
    
    # Create dynamic Delete DAG
    delete_dag_id = f"{table_name}_delete_dag"
    delete_dag_code = f"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import logging
from common_functions import delete_table_from_mysql

with DAG('{delete_dag_id}', start_date=datetime.now(), schedule_interval=None, template_searchpath='/opt/airflow/csv_files/'{table_name}'') as dag:
    delete_data = PythonOperator(
        task_id='delete_data',
        python_callable=delete_table_from_mysql,
        op_kwargs={{'table_name': '{table_name}'}},
    )
"""
    # Save the Delete DAG to a file
    delete_dag_file_path = os.path.join(Variable.get("airflow_dags_folder"), f"{delete_dag_id}.py")
    with open(delete_dag_file_path, 'w') as f:
        f.write(delete_dag_code)

# Create the master DAG
with DAG(
    'Configure New Input File',
    default_args=default_args,
    description='A master DAG to process CSV files and create dynamic DAGs',
    schedule_interval=None,  # Run only when triggered manually
) as dag:

    csv_prefix = Variable.get("csv_prefix", default_var="Temperature_Data")

    parse_csv_task = PythonOperator(
        task_id='parse_csv_task',
        python_callable=parse_csv_and_determine_ddl,
        op_kwargs={'csv_prefix': csv_prefix},
        provide_context=True,
    )

    create_table_task = MySqlOperator(
        task_id='create_table_task',
        mysql_conn_id='local_mysql',
        sql="{{ ti.xcom_pull(task_ids='parse_csv_task')['ddl'] }}",
    )

    create_dynamic_dag_task = PythonOperator(
        task_id='create_dynamic_dags_task',
        python_callable=create_dynamic_dags,
        provide_context=True,
    )

    parse_csv_task >> create_table_task >> create_dynamic_dag_task
