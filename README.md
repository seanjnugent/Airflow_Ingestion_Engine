This Airflow project is a data ingestion tool which does the following:
- Parses a CSV to define DDL, creating that table in the database (this is the master/define new table DAG)
- Dynamically create three DAGs specifically for that input file, load data, append data and delete data.

When triggering the master dag, the file prefix is required followed by the current date in the format YYYYMMDD.
Similarly, for each of the newly created DAGs, the pattern should be the same with the file with the current date being ingested. This can be scheduled or triggered automatically via a file sensor (to do).

*currently uses MySQL.


To Do - Transformation Pipelines
a) Form to submit DML
b) DML to be PR'd 
c) DAG based on the DML
