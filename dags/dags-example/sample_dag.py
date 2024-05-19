from airflow.decorators import dag, task
from datetime import datetime


@dag(dag_id='dag_setting_v2',
     start_date=datetime(2021, 10, 26),
     catchup=False,
     schedule_interval='@daily') # or https://crontab.guru
def this_dag():
    @task()
    def my_task():
        print("Task content go here")

    my_task()

this_dag = this_dag()

