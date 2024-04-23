from airflow.decorators import dag, task
from datetime import datetime
from helper import util_minio

import pandas as pd
from helper import config

from sqlalchemy import create_engine

@dag(dag_id='db_scd2',
     start_date=datetime(2021, 10, 26),
     catchup=False,
     schedule_interval='@daily')
def dag_etl():
    @task()
    def run_sql():
        postgres_username = config.DATABASE_CONFIG['username']
        postgres_password = config.DATABASE_CONFIG['password']
        postgres_host = config.DATABASE_CONFIG['host']
        postgres_port = config.DATABASE_CONFIG['port']
        postgres_db = config.DATABASE_CONFIG['db_name']

        connection_string = f'postgresql://{postgres_username}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}'
        engine = create_engine(connection_string)
        connection = engine.connect()

        sql_query = """
merge into dim_product_scd2
using (
  select
    dim_product.tiki_pid as join_key,
    dim_product.* from dim_product

  union all

  select
	null,
	dim_product.*
from
	dim_product
join dim_product_scd2 on
	dim_product.tiki_pid = dim_product_scd2.tiki_pid
where
	(( dim_product.name <> dim_product_scd2.name )
		or ( dim_product.origin <> dim_product_scd2.origin )
			or ( dim_product.brand_name <> dim_product_scd2.brand_name ) )
	and dim_product_scd2.valid_to is null
	and dim_product_scd2.is_current = true
) sub
on
	sub.join_key = dim_product_scd2.tiki_pid
	when matched
	and (sub.name <> dim_product_scd2.name
		or sub.origin <> dim_product_scd2.origin
		or sub.brand_name <> dim_product_scd2.brand_name )  
  
  then
update
set
	valid_to = sub.ingestion_dt_unix,
	is_current = false
	
	when not matched
  then
insert
values (default,
sub.tiki_pid,
sub.name,
sub.brand_name,
sub.origin,
sub.ingestion_dt_unix,
sub.ingestion_dt_unix,
null,
true);
"""
        connection.execute(sql_query)
        
        #connection.commit()

    run_sql()

greet_dag = dag_etl()


