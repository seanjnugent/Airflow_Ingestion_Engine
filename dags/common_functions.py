from airflow.hooks.mysql_hook import MySqlHook
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect, create_engine
import logging

def load_csv_to_mysql(table_name, file_path):
    # Get the MySQL connection from Airflow
    mysql_hook = MySqlHook(mysql_conn_id='local_mysql')
    engine = mysql_hook.get_sqlalchemy_engine()  # Get SQLAlchemy engine from the hook

    try:
        # Check if the table exists (case-insensitive check)
        inspector = inspect(engine)
        table_names = [t.lower() for t in inspector.get_table_names()]  # Normalize to lowercase
        if table_name.lower() not in table_names:
            raise ValueError(f"Table {table_name} does not exist in the database.")

        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Write DataFrame to MySQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data successfully loaded into {table_name} table.")

    except SQLAlchemyError as e:
        print(f"An error occurred: {e}")
        raise

def delete_table_from_mysql(table_name, mysql_conn_id='local_mysql'):
    try:
        # Get the MySQL connection from Airflow
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        engine = mysql_hook.get_sqlalchemy_engine()  # Get SQLAlchemy engine from the hook

        # Form the SQL delete query
        delete_query = f"DELETE FROM {table_name};"
        
        # Connect to the database and execute the query
        with engine.connect() as connection:
            connection.execute(delete_query)
            logging.info(f"Successfully deleted data from table: {table_name}")

    except SQLAlchemyError as e:
        logging.error(f"An error occurred while deleting data from table: {table_name}")
        logging.error(str(e))
        raise


def append_data_to_mysql(table_name, file_path):
    # Get the MySQL connection from Airflow
    mysql_hook = MySqlHook(mysql_conn_id='local_mysql')
    engine = mysql_hook.get_sqlalchemy_engine()  # Get SQLAlchemy engine from the hook

    try:
        # Check if the table exists (case-insensitive check)
        inspector = inspect(engine)
        table_names = [t.lower() for t in inspector.get_table_names()]  # Normalize to lowercase
        if table_name.lower() not in table_names:
            raise ValueError(f"Table {table_name} does not exist in the database.")

        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Append DataFrame to MySQL
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Data successfully appended to {table_name} table.")

    except SQLAlchemyError as e:
        print(f"An error occurred: {e}")
        raise