from airflow.decorators import dag, task
from datetime import datetime
from helper import module_1 #https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/modules_management.html#built-in-pythonpath-entries-in-airflow

import os
import time

@dag(dag_id='dag_setting_v2',
     start_date=datetime(2021, 10, 26),
     catchup=False,
     schedule_interval='@daily')
def dag_setting():
    @task()
    def get_info():
        print(module_1.check())
        time.sleep(5)

    @task()
    def get_info2():
        print(module_1.check2())

    get_info() >>get_info2()

greet_dag = dag_setting()

