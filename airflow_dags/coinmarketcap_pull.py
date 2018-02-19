import os
import sys
sys.path.append("/usr/local/lib/python2.7/site-packages")
from secrets import *  # where all passwords are located
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
import requests
import psycopg2

email_s = email('project_email')


def get_coin_market_cap_data():
    """making a API call to coinmarketcap to get data for the top 600 coins
    returns list of json objects"""
    r = requests.get('https://api.coinmarketcap.com/v1/ticker/?limit=600')
    rj = r.json()
    return rj

def coin_dict_to_dataframe(dict_date):
    """converts json data into a dataframe & cleans up fields"""
    coin_df = pd.DataFrame(dict_date)
    coin_df = coin_df[['id','name','symbol','price_usd','last_updated','percent_change_1h','percent_change_24h','percent_change_7d','24h_volume_usd','market_cap_usd']]
    coin_df = coin_df.apply(pd.to_numeric, errors='ignore')
    coin_df['last_updated'] = pd.to_datetime(coin_df['last_updated'],unit = 's')
    return coin_df

def insert_to_postgres(coin_df):
    """create temp table in postgres of coin data"""
    hook = PostgresHook(postgres_conn_id = 'main_pg_db')
    hook.run(" drop table if exists coin.stg_coin_data")
    hook.run("""Create table coin.stg_coin_data
            (id varchar,
            name varchar,
            symbol varchar,
            price_usd float,
            last_update timestamp,
            percent_change_1h float,
            percent_change_24h float,
            percent_change_7d float,
            volume_usd_24h float,
            market_cap_usd float) """)
    vals = [tuple(x) for x in coin_df.values]
    hook.insert_rows(
        table = 'coin.stg_coin_data',
        rows = vals,
        commit_every = 0
    )

def create_temp_table():
    coin_dict = get_coin_market_cap_data()
    coin_df = coin_dict_to_dataframe(coin_dict)
    insert_to_postgres(coin_df)


default_args = {
    'owner': 'Keerthan', 
    'depends_on_past': False,  # default to false, this is set at the task level
    'start_date': datetime(2017, 12, 31),  # always put todays date
    'email': email_s,  # your email
    # if you want to a email when a dag/task fails.. I would recommend you put to false
    'email_on_failure': False,
    'email_on_retry': False,  # if you want a email for every retry
    'retries': 2,  # the number of retries. Set default to 1 and explictyly state it within the task
    # the amount of time you want to wait between retries.
    'retry_delay': timedelta(minutes=5),
    # the max amount of time tasks should run
    'execution_timeout': timedelta(minutes=20)
}

dag = DAG('coinmarketcap_refresh',  # name of dag
          default_args=default_args,  # assigning the default args stated above to this
          schedule_interval='*/30 * * * *')


t1 = PythonOperator(
    task_id='create_stg_coin_data',
    python_callable=create_temp_table,
    dag=dag,
    execution_timeout=timedelta(minutes=60)
    )

##if a new coin is in the top 600, we have to do this first
t2 = PostgresOperator(
        task_id='insert_into_dim_coin',
        postgres_conn_id='main_pg_db',
        sql="""Select coin.dim_coin_insert();""",
        execution_timeout=timedelta(minutes=15),
        retry_exponential_backoff=True,
        dag=dag)


t3 = PostgresOperator(
        task_id='insert_into_24hprice_a',
        postgres_conn_id='main_pg_db',
        sql= """Select  coin.price_24h_insert();""",
        execution_timeout=timedelta(minutes=15),
       dag=dag)

t1 >> t2 >> t3 
