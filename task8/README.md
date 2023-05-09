# Task 8. Snowflake

## Description

Build an ELT pipeline based on Snowflake using Airflow (local). The overall design of the architecture should look something like the illustration below.

![image.png](images/image.png)

## Result

Dag_data_migration consist of:

![img.png](images/img.png)

The structure in the Data warehouse:  

![img_2.png](images/img_2.png)

IOS_APPS/PUBLIC/MASTER_TABLE:

![img_3.png](images/img_3.png)

## Preparing

```pip install -r requirements.txt```

### 1. Setting up a PostgreSQL Database

```sql
CREATE DATABASE airflow_db;
CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
USE airflow_db;
GRANT ALL ON SCHEMA public TO airflow_user;
```

### 2. Setting up an ```airflow.ctg```

```
load_examples = False
sql_alchemy_conn = postgresql+psycopg2://airflow_user:airflow_pass@127.0.0.1:5432/airflow_db
executor = CeleryExecutor
```

### 3. Run

The Standalone command will initialise the database, make a user,
and start all components for you.

```
airflow standalone
```
As you grow and deploy Airflow to production, you will also want to move away from the standalone command we use here to running the components separately.

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

### 4. Setting up a Snowflake connection

1. ```pip install apache-airflow-providers-snowflake```
2. Sign in to your snowflake account. Create a database (example: IOS_APPS).
3. In the airflow panel use ```Admin->Connection``` and enter the following:
```
Connection Id: snowflake_default
Connection Type: Snowflake
Schema: YOUR_SCHEMA
Login: YOUR_LOGIN
Password: YOUR_PASSWORD
Account: YOUR_ACCOUNT_NAME
Database: YOUR_DATABASE
Region: YOUR_REGION
Role: YOUR_ROLE
```

### 5. Setting up a Snowflake Variables

Upload ```config.json``` with your values.

![img_1.png](images/img_1.png)
