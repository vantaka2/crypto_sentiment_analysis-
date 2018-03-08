import os
import sys
sys.path.append("/usr/local/lib/python2.7/site-packages")
from secrets import * 
from reddit_kv import get_reddit_dict,get_reddit_trends_dict,assign_coin_to_posts
from sentiment_analysis import get_sentiment
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


email = email('project_email')

def create_stg_table():
    df = get_reddit_df()
    insert_stg_tbl(df)

def trends_data_insert():  
    list_id = get_post_id()
    trend_df = get_trends_df(list_id)
    trends_stg_tbl(trend_df)

def get_reddit_df():
    red_dict = get_reddit_dict()
    df = pd.DataFrame(red_dict)
    df = df.apply(pd.to_numeric, errors='ignore')
    df['created_utc'] = pd.to_datetime(df['created_utc'],unit = 's')
    df['author'] = df['author'].astype('str')
    df['subreddit'] = df['subreddit'].astype('str')
    return df    

def get_post_id():
    hook = PostgresHook(postgres_conn_id = 'main_pg_db')
    kl = hook.get_records("""SELECT array_agg(post_id) from coin.dim_reddit_post
    where created >= current_timestamp - interval '2 days' """)
    list_id = kl[0][0]
    return list_id

def get_trends_df(list_id):
    red_trend_dict = get_reddit_trends_dict(list_id)
    trends_df = pd.DataFrame(red_trend_dict)
    trends_df['update_time'] = datetime.now()
    return trends_df

def assign_sentiment():
    hook = PostgresHook(postgres_conn_id = 'main_pg_db')
    hook.run(" drop table if exists coin.sentiment_stg")
    hook.run(""" Create table coin.sentiment_stg 
    (confidence float,
    post_id text,
    sentiment text,
    subreddit text,
    title text)""")
    data = hook.get_records()
    fields = ['post_id','subreddit','title']
    dicts = [dict(zip(fields, d)) for d in data]
    for i in dicts:
        sentiment = get_sentiment(i['title'])
        i.update(sentiment)
    df1 = pd.DataFrame(dicts)
    vals = df1.values
    hook.insert_rows(
            table = 'coin.sentiment_stg',
            rows = vals,
            commit_every = 0
        )


def trends_stg_tbl(df):
    hook = PostgresHook(postgres_conn_id = 'main_pg_db')
    hook.run(" drop table if exists coin.stg_trends")
    print('done dropping stageing table')
    hook.run(""" Create table coin.stg_trends 
            (post_id varchar,
            num_comments bigint,
            score bigint,
            update_time timestamp)
              """)
    print('done creating shell table')
    val_trends = [tuple(x) for x in df.values]
    print(val_trends[2])
    hook.insert_rows(
            table = 'coin.stg_trends',
            rows = val_trends,
            commit_every = 0
        )

def insert_stg_tbl(df):
    """create temp table in postgres of coin data"""
    hook = PostgresHook(postgres_conn_id = 'main_pg_db')
    hook.run(" drop table if exists coin.stg_reddit_post")
    hook.run("""Create table coin.stg_reddit_post
            (author varchar,
            created_utc timestamp,
            id varchar,
            num_comments bigint,
            score bigint,
            selftext varchar,
            subreddit varchar,
            title varchar
            ) """)
    vals = [tuple(x) for x in df.values]
    hook.insert_rows(
        table = 'coin.stg_reddit_post',
        rows = vals,
        commit_every = 0
    )


default_args = {
    'owner': 'Keerthan', 
    'depends_on_past': False,  # default to false, this is set at the task level
    'start_date': datetime(2017, 12, 31),  # always put todays date
    'email': email,  # your email
    # if you want to a email when a dag/task fails.. I would recommend you put to false
    'email_on_failure': False,
    'email_on_retry': False,  # if you want a email for every retry
    'retries': 2,  # the number of retries. Set default to 1 and explictyly state it within the task
    # the amount of time you want to wait between retries.
    'retry_delay': timedelta(minutes=5),
    # the max amount of time tasks should run
    'execution_timeout': timedelta(minutes=20)
        }

dag = DAG('reddit_refresh',  # name of dag
          default_args=default_args,  # assigning the default args stated above to this
          schedule_interval='*/30 * * * *')


t1 = PythonOperator(
    task_id='create_stg_reddit_data',
    python_callable=create_stg_table,
    dag=dag,
    execution_timeout=timedelta(minutes=60)
    )

t2 = PostgresOperator(
        task_id='insert_into_dim_reddit_post',
        postgres_conn_id='main_pg_db',
        sql="""Select coin.dim_reddit_post_insert();""",
        execution_timeout=timedelta(minutes=15),
        retry_exponential_backoff=True,
        dag=dag)

t3 = PythonOperator(
    task_id='create_stg_trends_table',
    python_callable=trends_data_insert,
    dag=dag,
    execution_timeout=timedelta(minutes=15)
    )


t4 = PostgresOperator(
        task_id='insert_into_reddit_post_trends',
        postgres_conn_id='main_pg_db',
        sql="""Select coin.reddit_trends_insert();""",
        execution_timeout=timedelta(minutes=15),
        retry_exponential_backoff=True,
        dag=dag)

t5 = PostgresOperator(
        task_id='drop_stg_tables',
        postgres_conn_id='main_pg_db',
        sql="""Drop table if exists coin.stg_reddit_post;
               Drop table if exists coin.stg_trends;
               Drop table if exists coin.stg_xref_post_to_coin""",
        execution_timeout=timedelta(minutes=15),
        retry_exponential_backoff=True,
        dag=dag)

t6 = PythonOperator(
    task_id='assign_coin_to_posts_stg',
    python_callable=assign_coin_to_posts,
    dag=dag,
    execution_timeout=timedelta(minutes=15)
    )

t7 = PostgresOperator(
        task_id='insert_coin_to_post',
        postgres_conn_id='main_pg_db',
        sql="""insert into coin.xref_post_to_coin
                (post_id, coin_id, keyword,match_type) 
                Select reddit_id as post_id,  coin_id, array_agg(keyword), match_type from 
                coin.stg_xref_post_to_coin
                group by 1,2,4;""",
        execution_timeout=timedelta(minutes=15),
        retry_exponential_backoff=True,
        dag=dag)

t8 = PythonOperator(
    task_id='assign_sentiment',
    python_callable=assign_sentiment,
    dag=dag,
    execution_timeout=timedelta(minutes=15)
    )

t9 = PostgresOperator(
        task_id='insert_to_sentiment',
        postgres_conn_id='main_pg_db',
        sql="""insert into coin.sentiment
                (source, source_category, source_id, sentiment, confidence) 
                Select 'reddit' as source, subreddit, post_id,  sentiment, confidence 
                from coin.sentiment_stg a
                left join coin.sentiment b
                on a.post_id = b.source_id
                where b.source_id is null; 
                """,
        execution_timeout=timedelta(minutes=15),
        retry_exponential_backoff=True,
        dag=dag)

t1 >> t2 >> t6 >> t7 >> t5
t1 >> t3 >> t4 >> t5
t1 >> t8 >> t9 >> t5
