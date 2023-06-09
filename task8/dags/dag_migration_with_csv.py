import pandas as pd

from airflow.models import Variable
from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
RAW_DATA_FILE = Variable.get("RAW_DATA_FILE")
OUTPUT_FILE = Variable.get("OUTPUT_FILE")
DATA_PATH = Variable.get("DATA_PATH")
DDL_PATH = Variable.get("DDL_PATH")
DML_PATH = Variable.get("DML_PATH")

SNOWFLAKE_CONN_ID = Variable.get("SNOWFLAKE_CONN_ID")
SNOWFLAKE_SCHEMA = Variable.get("SNOWFLAKE_SCHEMA")
SNOWFLAKE_DATABASE = Variable.get("SNOWFLAKE_DATABASE")
SNOWFLAKE_ROLE = Variable.get("SNOWFLAKE_ROLE")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 5, 4),
    "retries": 1,
    "snowflake_conn_id": SNOWFLAKE_CONN_ID
}


@dag(start_date=datetime(2023, 5, 4), schedule="@once", default_args=default_args)
def dag_data_migration_csv():
    def query_string(filename: str) -> str:
        """
        Return string of sql query.
        :param filename: str
        :return: str
        """
        if filename.lower().startswith("create"):
            file_path = f"{DDL_PATH}/{filename}"
        elif filename.lower().startswith("insert"):
            file_path = f"{DML_PATH}/{filename}"
        with open(file_path, 'r') as f:
            query = f.read()
        return query

    with TaskGroup(group_id='preparing') as preparing:
        create_raw_table = SnowflakeOperator(
            task_id="create_raw_table",
            sql=query_string("create_raw_table.sql"),
        )

        create_stage_table = SnowflakeOperator(
            task_id="create_stage_table",
            sql=query_string("create_stage_table.sql"),
        )

        create_master_table = SnowflakeOperator(
            task_id="create_master_table",
            sql=query_string("create_master_table.sql")
        )

        create_raw_table_stream = SnowflakeOperator(
            task_id="create_raw_table_stream",
            sql=query_string("create_raw_stream.sql")
        )

        create_stage_table_stream = SnowflakeOperator(
            task_id="create_stage_table_stream",
            sql=query_string("create_stage_stream.sql")
        )

        create_format_csv = SnowflakeOperator (
            task_id = 'create_format_csv',
            sql=query_string('create_file_format_updated_csv.sql')
        )

        create_raw_table >> create_raw_table_stream
        create_stage_table >> create_stage_table_stream
        create_master_table
        create_format_csv

    with TaskGroup(group_id='migration') as migration:
        @task
        def csv_change_format():
            """
            Change format file in source file
            :return: None
            """
            df = pd.read_csv(f"{DATA_PATH}/{RAW_DATA_FILE}", index_col=False)
            df.to_csv(f"{DATA_PATH}/{OUTPUT_FILE}", index=False, sep='^', lineterminator='\t')

        csv_to_stage = SnowflakeOperator(
            task_id="csv_to_stage",
            sql=f"PUT file://{DATA_PATH}/{OUTPUT_FILE} @%RAW_TABLE OVERWRITE = TRUE;"
        )

        to_raw_table = SnowflakeOperator(
            task_id="to_raw_table",
            sql=f"COPY INTO RAW_TABLE FROM @%RAW_TABLE/{OUTPUT_FILE}.gz FILE_FORMAT = ( FORMAT_NAME = UPDATED_FORMAT_CSV') ON_ERROR = CONTINUE;"
        )

        to_stage_table = SnowflakeOperator(
            task_id="to_stage_table",
            sql=query_string("insert_to_stage_table.sql"),
        )

        to_master_table = SnowflakeOperator(
            task_id="to_master_table",
            sql=query_string("insert_to_master_table.sql"),
        )

        csv_change_format() >> csv_to_stage >> to_raw_table >> to_stage_table >> to_master_table

    preparing >> migration


dag_data_migration_csv()
