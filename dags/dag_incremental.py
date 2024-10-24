# Airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Libs
import os, sys
from datetime import datetime, date, timedelta
from dateutil.relativedelta import *

# Dependencies
from dependencies.scripts import getIncrementalStrategy, readFile

default_args = {
    "owner": "Marius",  # Owner of the DAG
    "depends_on_past": False,  # Does this task depend on past runs?
    "retries": 0,  # Number of retries for the tasks
    "email_on_failure": False,  # Email notifications on failure
    "email_on_retry": False,  # Email notifications on retry
}

with DAG(
    "dag_incremental",
    start_date=datetime(2022, 5, 30),  # Start date of the DAG
    max_active_runs=10,  # Maximum number of active runs
    schedule_interval="*/5 * * * *",  # Schedule interval: every 5 minutes
    default_args=default_args,  # Default arguments for the DAG
    tags=["POSTGRES", "INCREMENTAL"],  # Tags to categorize the DAG
    catchup=False,  # Disable automatic backfill
    params={"full": "False"},  # Parameters for the DAG
    is_paused_upon_creation=False,  # Do not pause the DAG upon creation
) as dag:

    init = DummyOperator(task_id="init")  # Initialize a dummy operator
    task_date = BashOperator(
        task_id="task_date",
        bash_command=f"echo '{getIncrementalStrategy()}'",  # Print the incremental strategy
        retries=3,  # Number of retries for this task
    )

    task_incremental = PostgresOperator(
        task_id="task_incremental",
        postgres_conn_id="POSTGRESCONN",  # PostgreSQL connection ID
        sql=readFile(path="/opt/airflow/dags/queries/raw_biler.sql").replace(
            "${DATE}", getIncrementalStrategy()
        ),  # Read and replace the SQL query with the incremental strategy
    )

    final = DummyOperator(task_id="final")  # Final dummy operator

# Define task dependencies
init >> task_date >> task_incremental >> final
