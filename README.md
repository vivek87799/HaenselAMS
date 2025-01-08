# Attribution Pipeline Orchestration
This document outlines the code setup for an attribution pipeline orchestration solution, developed as part of a Data Engineer position test challenge. The pipeline aims to process customer journey data, obtain attribution results from an external API, and generate reports for analysis.

# Task
 Each step should run as separate task in Apache Airflow. 
    - Query Data from a database 
    - Transform data as is necessary
    - Send transformed data to an API which returns you attribution results
    - Write attribution results to the database
    - Query and export data from the database
    
# Setup Airflow
- Download the docker-compose from official Airflow page
- Update the airflow image to "docker pull apache/airflow:2.10.4-python3.9" 
- Change the CeleryExecutor to local executor and remove the unnecessary configs for Celery
- Redis is needed for Celery so it will be no longer need. Remove the Redis service

- Create the dag and log files
```
mkdir -p ./sources/dags ./sources/logs ./sources/plugins ./sources/config ./sources/data 
```
- set the env UID and GID 
```
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```
- Add the .db and .sql file into the data folder that was created before
- init the airflow setup by running the below command
```
docker-compose up airflow-init
```
- Now start the Airflow container
```
docker-compose up
```
- The Airflow webserver will be exposed on http://0.0.0.0:8080 with the default username and pwd as airflow

 ### *A detailed Design Document on pipeline architecture has been attached to the repo as a pdf*
 
# Pipeline Overview
 ```markdown
  The pipeline takes a time range as input that can be assigned in env variables inside the docker-compose.yml file
 ``` 

The pipeline will perform the following steps (Takes the date range as input that is configured as airflow’s env variable. Configurable in environments in the docker-compose.yml provided): See README.md to run the pipeline
- Init Database: Initialize the tables on SQLite database
- Data Extraction: Extract session and conversion data from a provided SQLite database (challenge.db) which is preprocess and then persisted as a JSON File.
- Customer Journey Construction (Transform data): Combine session and conversion data to build customer journeys for each conversion.
- API Interaction: Send customer journey data to the IHC Attribution API (https://ihc-attribution.com/marketing-attribution-api/) in asynchronous chunks to receive attribution results.

- Attribution Data Storage: Store the received attribution results in the attribution_customer_journey table within the database.
- Report Generation: Aggregate data and generate the channel_reporting table, summarizing key metrics. Export the channel_reporting data to a CSV file, including calculated CPO (Cost Per Order) and ROAS (Return on Ad Spend) metrics.

