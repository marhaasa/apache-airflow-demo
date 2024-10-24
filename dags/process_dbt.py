from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    "owner": "Marius",
    "start_date": datetime(2023, 1, 1),
}

dag = DAG(
    "process_dbt", default_args=default_args, schedule_interval="@daily", catchup=False
)

task1 = BashOperator(
    task_id="run_dbt",
    bash_command="source /opt/airflow/dbt/bin/activate && cd /opt/airflow/dbt/airflow_demo && /home/airflow/.local/bin/dbt run --profiles-dir /opt/airflow/dbt/airflow_demo && /home/airflow/.local/bin/dbt docs generate --profiles-dir /opt/airflow/dbt/airflow_demo",
    dag=dag,
)
