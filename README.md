# Setup

## Prerequisites

Make sure you have cloned the repo and installed `docker` on your machine.

## Start the demo

1. Navigate to the root of apache-airflow-demo folder via your terminal.
2. Run the following command: `docker compose up airflow-init && docker compose up`
3. To access the most relevant services follow the following links or use the following connection string to access the postgres database:

- [Airflow UI](http://localhost:8080)
- [Adminer](http://localhost:33380)
- postgres://airflow:airflow@postgres/airflow?sslmode=disable

If you want to connect via **Azure Data Studio** use the following and make sure you have the **PostgreSQL** extension for **Azure Data Studio**:\
Server name: **localhost**\
User name: **airflow**\
Password: **airflow**\
Database name: **airflow**\
