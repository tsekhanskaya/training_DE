# Task 8. Snowflake

## Description

Build an ELT pipeline based on Snowflake using Airflow (local). The overall design of the architecture should look something like the illustration below.

![image.png](image.png)

## Preparing

If you want to run the individual parts of Airflow manually rather than using the all-in-one standalone command, you can instead run:
```
airflow db init

airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org

airflow webserver --port 8080

airflow scheduler
```
