import datetime
import pendulum
import os
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id="process_oslokommune",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessOslokommune():

    create_oslokommune_table = PostgresOperator(
        task_id="create_oslokommune_table",
        postgres_conn_id="POSTGRESCONN",
        sql="""
            CREATE TABLE IF NOT EXISTS oslokommune (
               "Avdelingskode" TEXT,
               "Avdelingsnavn" TEXT,
               "Kapittelkode" TEXT,
               "Kapittelnavn" TEXT,
               "Artgruppekode" INTEGER,
               "Artsgruppenavn" TEXT,
               "Kostrafunksjonskode" INTEGER,
               "Kostrafunksjonsnavn" TEXT,
               "Årsregnskap" FLOAT,
               "Regulert budsjett" FLOAT,
               "Opprinnelig budsjett" FLOAT
            );""",
    )

    @task
    def get_data():
        # NOTE: configure this as appropriate for your airflow environment
        data_path = "/opt/airflow/dags/files/oslokommune.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        url = "https://www.oslo.kommune.no/getfile.php/13516840-1720167839/Tjenester%20og%20tilbud/Politikk%20og%20administrasjon/Budsjett%2C%20regnskap%20og%20rapportering/%C3%85pne%20datasett/2023%20A%CC%8Arsregnskap%20drift.csv"

        response = requests.request("GET", url)

        with open(data_path, "w") as file:
            file.write(response.text)

        postgres_hook = PostgresHook(postgres_conn_id="POSTGRESCONN")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY oslokommune FROM STDIN WITH CSV HEADER DELIMITER AS ';' QUOTE '\"'",
                file,
            )
        conn.commit()

    [create_oslokommune_table] >> get_data()


dag = ProcessOslokommune()
