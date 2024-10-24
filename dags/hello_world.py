from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


# Define a simple Python function that prints "Hello, World!"
def hello_world():
    print("Hello, World!")


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 10, 22),
}

# Instantiate the DAG object
with DAG(
    "hello_world_dag",
    default_args=default_args,
    schedule_interval="@daily",  # Runs daily
    catchup=False,
) as dag:

    # Define a PythonOperator that calls the hello_world function
    hello_task = PythonOperator(
        task_id="say_hello",  # Unique ID for the task
        python_callable=hello_world,  # Function to execute
    )

# No dependencies here, it's a single task DAG
