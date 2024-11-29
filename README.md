# Airflow Dynamic CSV Processing Pipeline

## Overview
Automates creation and management of MySQL database tables from CSV files using Apache Airflow.

## Features
- Dynamically generate database tables from CSV files
- Automatically sanitize column names
- Create Load/Append/Delete DAGs for each discovered dataset
- Support for multiple input file formats
- Configurable via Airflow Variables

## Prerequisites
- Apache Airflow
- MySQL Database
- Python libraries:
  - pandas
  - SQLAlchemy
  - airflow providers

## Key Components
- `parse_csv_and_determine_ddl()`: Extracts table schema from CSV
- `create_dynamic_dags()`: Generates DAGs for data operations
- `load_csv_to_mysql()`: Load CSV data into MySQL
- `append_data_to_mysql()`: Append new data to existing table
- `delete_table_from_mysql()`: Delete data from table

## Configuration
Set Airflow Variables:
- `csv_folder`: Path to CSV files
- `csv_prefix`: Prefix for input files
- `airflow_dags_folder`: Location for generated DAGs

## Usage
1. Place CSV files in specified folder
2. Trigger master DAG
3. Automatically generates table and operational DAGs

## Workflow
1. Parse latest CSV file
2. Create MySQL table
3. Generate dynamic DAGs for:
   - Loading data
   - Appending data
   - Deleting data

## Limitations
- Assumes consistent CSV file naming
- Limited to MySQL
- Requires predefined MySQL connection
