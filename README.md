# Data Engineering Workshop 2

## Overview
This project involves automating an ETL process using Airflow within Docker containers. The data is processed and stored in both PostgreSQL and Google Drive, followed by visualization using Power BI. Google Drive API is employed for data transfer operations.

## Table of Contents
- [Setup Instructions](#setup-instructions)
- [Data Preparation](#data-preparation)
- [Airflow Automation](#airflow-automation)
- [Dashboard Visualization](#dashboard-visualization)

## Setup Instructions <a name="setup-instructions"></a>
Create a project directory with the necessary subdirectories (`logs`, `plugins`) and clone the repository. Ensure the following software is installed:
- **Python**
- **PostgreSQL**
- **PowerBI**
- **VS Code** or **Jupyter**
- **Docker**

### Google Drive API Setup
Enable the Google Drive API via Google Cloud Platform:
1. Access "APIs & Services" > "Library", search for "Google Drive API" and enable it.
2. Navigate to "config", create credentials for a service account, and download the JSON file as "service_account.json".

Place `config.json` and `service_account.json` in the **config** directory:
- `config.json`: Contains details for PostgreSQL connection.
- `service_account.json`: Includes Google service account information.

## Data Preparation <a name="data-preparation"></a>
Prepare the database and pre-load Grammy data using scripts in the **dags** directory. Perform an exploratory data analysis with notebooks provided in the **notebook** directory to better understand the datasets.

## Airflow Automation <a name="airflow-automation"></a>
Automate ETL processes using Airflow managed through:
- **Dags Connection**: Manages the DAG connections in Airflow.
- **ETL**: Contains scripts for individual ETL tasks:
  - `grammy_etl`
  - `spotify_etl`
  - `merge_load_data`

### Running Airflow
1. Ensure Docker is running.
2. Initialize Airflow with: `docker-compose up airflow-init`.
3. Start Airflow with: `docker-compose up`.
4. Open the Airflow web interface at [http://localhost:8080/](http://localhost:8080/) using credentials (user: airflow, password: airflow) and trigger the `etl_dag`.

## Dashboard Visualization <a name="dashboard-visualization"></a>
After executing the DAG, access the visualizations through the provided [Power BI Dashboard](https://github.com/VanessaSuare/workshop2/blob/main/docs/DASHBOARD.pdf) for a graphic analysis of the data.