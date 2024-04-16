from airflow.decorators import dag, task
from datetime import datetime
from helper import module_1 #https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/modules_management.html#built-in-pythonpath-entries-in-airflow

import os

@dag(dag_id='dag_setting_v1',
     start_date=datetime(2021, 10, 26),
     catchup=False,
     schedule_interval='@daily')
def dag_setting():
    @task()
    def get_info():
        module_1.check()
        dag_path = os.path.abspath(os.path.dirname(__file__))
        airflow_home = os.environ['AIRFLOW_HOME']
        print(f"DAG path: {dag_path}, AIRFLOW_HOME: {airflow_home}")

    get_info()

greet_dag = dag_setting()
