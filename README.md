# recruitment_challenge
recruitment challenge for job at HAMS

# Understand the IHC attribution model
- Familiarize with the IHC attribution model 
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
mkdir -p ./dags ./logs ./plugins ./config
```
- set the env UID and GID 
```
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```
- init the airflow setup by running the below command
```
docker-compose up airflow-init
```
- Now start the Airflow container
```
docker-compose up
```
- The Airflow webserver will be exposed on http://0.0.0.0:8080 with the default username and pwd as airflow





