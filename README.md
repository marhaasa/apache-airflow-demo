# Setup

## Prerequisites

Make sure you have cloned the repo and installed `docker` on your machine.

## Start the demo

1. Navigate to the root of apache-airflow-demo folder via your terminal.
2. Run the following command: `docker compose build && docker compose up airflow-init && docker compose up`
3. To access the most relevant services follow the following links or use the following connection string to access the postgres database:

- [Airflow UI](http://localhost:8080)
- [Adminer](http://localhost:33380)
- [Superset UI](http://localhost:8088)
- postgres://airflow:airflow@postgres/airflow?sslmode=disable

If you want to connect via **Azure Data Studio** use the following and make sure you have the **PostgreSQL** extension for **Azure Data Studio**:

Server name: **localhost**\
User name: **airflow**\
Password: **airflow**\
Database name: **airflow**

4. To stop all services and remove all containers run the following command inside the root of apache-airflow-demo folder via your terminal: `docker compose down --volumes --remove-orphans`

## Simple overview of the different containers and services

```mermaid
graph TD
    subgraph Airflow containere
        direction LR

        Scheduler[Scheduler] --> Webserver[Webserver]
        Scheduler --> Worker1[Worker]

        Scheduler --> Database[Metadata database]
        Worker1 --> Database
        Webserver --> Database

        subgraph Broker queue
            direction TB
            Redis[Redis] --> Worker1

        end
    end
    subgraph Uavhengige containere
		direction TB
	    fake_data_generator[Fake Data Generator] --> DataSource1[postgres]
	    adminer[Adminer]
	    DataSource1[postgres] <--> Worker1[Worker]
      Supserset[Superset] --> DataSource1[postgres]
	end

```
