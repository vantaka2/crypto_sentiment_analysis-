import os
import sys
sys.path.append("/usr/local/lib/python2.7/site-packages")
from secrets import *
import pandas as pd
import praw
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy import create_engine
import re

def get_reddit_dict():
    cl = reddit_pull()
    cl.get_new_posts()
    wanted_keys = ['id','subreddit','selftext','author','score','created_utc','title','num_comments']
    rd = cl.filter_dict(wanted_keys)
    return rd

def get_reddit_trends_dict(list_id):
    list_w_prefix = ['t3_{}'.format(i) for i in list_id]
    k = len(list_w_prefix)
    print('Length of list of posts is {}'.format(k))
    cl = reddit_pull()
    cl.get_post_by_id(list_w_prefix)
    wanted_keys = ['id','score','num_comments']
    rd = cl.filter_dict(wanted_keys)
    return rd

def assign_coin_to_posts():
    cl = reddit_compare()
    coin_dict = cl.get_coin_keywords()
    post_dict = cl.get_new_posts()
    df2 = cl.compare_lists(post_dict,coin_dict)
    cl.write_to_stg_table(df2)



class reddit_pull:
    def __init__(self):
        self.list_posts = []
        self.reddit = praw.Reddit(client_id=api_keys('reddit_client_id'),
                                client_secret=api_keys('reddit_client_secret'),
                                password=password('reddit'),
                                user_agent='testscript by /u/fakebot3',
                                username = usernames('reddit')
                                )

    def get_new_posts(self):
        for sub in self.reddit.subreddit('cryptocurrency').new():
            self.list_posts.append(vars(sub))

    def get_post_by_id(self,list_w_prefix):
        for sub in self.reddit.info(list_w_prefix):
            self.list_posts.append(vars(sub))
        

    def filter_dict(self,wanted_keys):
        subdict = [{x: k[x] for x in wanted_keys} for k in self.list_posts]
        return subdict

class reddit_compare:
    def __init__(self):
        db_conn = sql_conn('postgres')
        self.engine = sql_conn('postgres')
    
    def get_coin_keywords(self):
        sql = """Select id, array_remove(array_remove(keywords || keywords_to_include, unnest(key_words_to_ignore)),null) as keywords 
                from (
                        Select d.id, d.keywords, 
                                    array_agg(lower(keywords_to_ignore):: varchar ) as key_words_to_ignore,
                                    array_agg(lower(keywords_to_include):: varchar) as keywords_to_include FROM (
                        select a.id,array_agg(keyword:: varchar) as keywords from (
                        Select id, lower(id) as keyword from coin.dim_coin 
                        union  
                        select id, lower(name) as keyword from coin.dim_coin
                        union 
                        select id, lower(symbol) as keyword from coin.dim_coin
                            )a
                            group by 1
                            )d
                            left join coin.dim_ignore_keywords b 
                            on d.id = b.id 
                            left join coin.dim_include_keywords c 
                            on d.id = c.id
                            group by 1,2 ) g"""
        coin_keyword_df = pd.read_sql(sql, self.engine)
        coin_keyword_dict = coin_keyword_df.to_dict('records')
        return coin_keyword_dict

    def get_new_posts(self):
        sql = """ Select a.post_id, 
                    title, 
                    selftext  
                from coin.dim_reddit_post a
                left join coin.xref_post_to_coin b
                on a.post_id = b.post_id
                where b.post_id is null
                limit 250    
            """
        post_df = pd.read_sql(sql,self.engine)
        post_dict = post_df.to_dict('records')
        return post_dict

    def compare_lists(self,reddit_list,coin_list):
        list_of_dict = []
        for i in reddit_list:
            i['title_new'] = re.findall(r"[\w']+",i['title'].lower())
            i['selftext_new'] = re.findall(r"[\w']+",i['selftext'].lower())
            for x in coin_list:
                for y in x['keywords']:
                    if y in i['title_new']:
                        reddit_title={}
                        reddit_title['reddit_id']=i['post_id']
                        reddit_title['coin_id']= x['id']
                        reddit_title['keyword']= y
                        reddit_title['match_type'] = 1
                        list_of_dict.append(reddit_title)
                    else:
                        reddit_null={}
                        reddit_null['reddit_id']=i['post_id']
                        reddit_null['coin_id']=None
                        reddit_null['keyword']= None
                        reddit_null['match_type'] = None
                        list_of_dict.append(reddit_null)
                        continue
                    if y in i['selftext_new']:
                        reddit_selftext={}
                        reddit_selftext['reddit_id']=i['post_id']
                        reddit_selftext['coin_id']= x['id']
                        reddit_selftext['keyword']= y
                        reddit_selftext['match_type'] = 2
                        list_of_dict.append(reddit_selftext)
            list_of_dict_2 = [dict(t) for t in set([tuple(d.items()) for d in list_of_dict])]
            df = pd.DataFrame(list_of_dict_2)
        return df

    def write_to_stg_table(self,df):
        sql = """Drop table if exists coin.stg_xref_post_to_coin"""
        main_db = create_engine(self.engine)
        main_db.execute(sql)
        df.to_sql(name = 'stg_xref_post_to_coin',
                con = self.engine,
                index = False,
                schema = 'coin')