from airflow.decorators import dag, task
from datetime import datetime
from sqlalchemy import create_engine

@dag(dag_id='db_scd2',
     start_date=datetime(2021, 10, 26),
     catchup=False,
     schedule_interval='@daily')
def dag_etl():
    @task()
    def run_sql():
        db_url = 'postgresql://my_user:my_password@pg-tiki:5432/dw_tiki'
        engine = create_engine(db_url)
        with open('dags/sql/dim_product_scd2.sql', 'r') as file:
            sql_query = file.read()
            print('sqlquery', sql_query)
        
        with engine.begin() as connection:
            connection.execute(sql_query)

    run_sql()

greet_dag = dag_etl()
