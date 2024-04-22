''' Workflow dags decorators '''

from datetime import timedelta, datetime
from airflow.decorators import dag, task
from etl import extract_spotify_data, transform_spotify_data, load_spotify_data, load_grammys_data, merge



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 18),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(
        default_args=default_args,
        schedule_interval='@daily',
        tags=['spotify', 'grammys']
)
def etl_dag():

    @task
    def extract_spotify_task():
        return extract_spotify_data()

    @task
    def transform_spotify_task(spotify_data):
        return transform_spotify_data(spotify_data)

    @task
    def load_spotify_task(spotify_data):
        return load_spotify_data(spotify_data)

    @task
    def load_grammys_task():
        return load_grammys_data()
    
    @task
    def merge_task(spotify_data, grammys_data):
        return merge(spotify_data, grammys_data)

    spotify_data = extract_spotify_task()
    spotify_transform = transform_spotify_task(spotify_data)
    load_spotify = load_spotify_task(spotify_transform)
    grammys_load = load_grammys_task()
    final_merge = merge_task(load_spotify, grammys_load)

etl_workflow = etl_dag()