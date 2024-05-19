from airflow.decorators import dag, task
from datetime import datetime

import time

@dag(dag_id='dag_setting_v2',
     start_date=datetime(2021, 10, 26),
     catchup=False,
     schedule_interval='@daily')
def this_dag():
    @task()
    def get_info():
        print(module_1.check())
        time.sleep(5)

    @task()
    def get_info2():
        print(module_1.check2())

    get_info() >>get_info2()

this_dag = this_dag()

