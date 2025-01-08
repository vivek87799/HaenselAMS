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

# Design Document

A detail insights on the design and the project is attached as a pdf in this repo






