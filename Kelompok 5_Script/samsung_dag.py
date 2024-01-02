import datetime as dt
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine

from airflow.utils.dates import days_ago
from samsung_etl import samsung_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2023,6,13),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}

dag = DAG(
    'samsung_dag',
    default_args=default_args,
    description='Shopee etl',
    schedule_interval=dt.timedelta(minutes=50),
)

def ETL():
    print("started")
    df= samsung_etl()
    #print(df)
    conn = BaseHook.get_connection('postgre_sql')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    df.to_sql('toko_shopee', engine, if_exists='replace')

with dag:    
    create_table= PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgre_sql',
        sql="""
            CREATE TABLE IF NOT EXISTS toko_shopee(
            nama VARCHAR(200),
            harga VARCHAR(200),
            currency VARCHAR(200),
            stock VARCHAR(200),
            rating VARCHAR(200)

        )
        """
    )

    run_etl = PythonOperator(
        task_id='samsung_etl_final',
        python_callable=ETL,
        dag=dag,
    )

    create_table >> run_etl