import pandas as pd
import emoji

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.providers.mongo.hooks.mongo import MongoHook
from settings import FILENAME, DATA_PATH


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
        'data_processing_dag',
        default_args=default_args,
        description='A simple data processing DAG',
        schedule_interval='@once',
        catchup=False
) as dag:

    start_task = EmptyOperator(task_id='start_task', dag=dag)

    sensor_task = FileSensor(
        task_id='sensor_task',
        filepath=f'{DATA_PATH}/{FILENAME}',
        fs_conn_id="fs_default",
        poke_interval=60,
        dag=dag
    )

    with TaskGroup(group_id='data_processing_tasks') as data_processing_tasks:
        @task(task_id='clean_data')
        def clean_data(file: str):
            """
            Clears the dataframe of empty slots. The result is written to a file (clean_data.csv)
            :param file: string
            :return: string
            """
            data = pd.read_csv(file)
            data.dropna(how='all', inplace=True)
            output = "clean_data.csv"
            data.to_csv(output, index=False)
            return output

        @task(task_id='replace_null')
        def replace_null(file: str):
            """
            Replaces all "null" values with "-". The result is written to a file (replace_null.csv).
            :param file: string
            :return: string
            """
            data = pd.read_csv(file)
            data.fillna(value='-', inplace=True)
            output = f"{DATA_PATH}/replace_null.csv"
            data.to_csv(output, index=False)
            return output

        @task(task_id='sort_data')
        def sort_data(file: str):
            """
            Sorts data by created_date. The result is written to a file (sort_data.scv).
            :param file: string
            :return: string
            """
            data = pd.read_csv(file)
            data.sort_values(by='at', inplace=True)
            output = f"{DATA_PATH}/sort_data.csv"
            data.to_csv(output, index=False)
            return output

        @task(task_id='clean_content')
        def clean_content(file: str):
            """
            removes all smiley characters from the content column, leaving only the text and punctuation marks.
            The result is written to a file (clean_content.csv).
            :param file: string
            :return: string
            """
            data = pd.read_csv(file)
            data['content'] = data['content'].apply(lambda s: emoji.replace_emoji(s, ''))
            output = f"{DATA_PATH}/clean_content.csv"
            data.to_csv(output, index=False)
            return output

        clean_content(sort_data(replace_null(clean_data(f'{DATA_PATH}/{FILENAME}'))))

    with TaskGroup(group_id='mongo') as mongo:
        @task
        def mongo_task(file: str):
            """
            Loading processed data into MongoDB.
            :param file: string
            :return: None
            """
            hook = MongoHook(mongo_conn_id="mongo_default")
            client = hook.get_conn()
            db = client.Tiktok
            currency_collection = db.tiktok_google_play_reviews
            data = pd.read_csv(file)
            data.reset_index(inplace=True)
            data_dict = data.to_dict(orient="records")
            currency_collection.insert_many(data_dict)

        mongo_task(f'{DATA_PATH}/clean_content.csv')

    end_task = EmptyOperator(task_id='end_task', dag=dag)

start_task >> sensor_task >> data_processing_tasks >> mongo >> end_task
