import sys
sys.path.append("/usr/local/lib/python2.7/site-packages")
from secrets import sql_conn

import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go
from dash.dependencies import Input, Output





app = dash.Dash()
## bootstamp CSS (From https://github.com/amyoshino/DASH_Tutorial_ARGO_Labs/blob/master/app.py)
app.css.append_css(
    {'external_url':'https://cdn.rawgit.com/plotly/dash-app-stylesheets/2d266c578d2a6e8850ebce48fdb52759b2aef506/stylesheet-oil-and-gas.css'})

##has the sql connection information (from 'secrets')
sql_con = sql_conn('postgres')
## get market cap data frame
def market_cap_df(pg_conn=sql_con):
    """Returns the dataframe used for marketcap graphs"""
    sql = """
    select id, name, current_rank, last_updated,insert_timestamp, market_cap_usd
    From coin.mc_graph_data 
    group by 1,2,3,4,5,6
        """
    df = pd.read_sql(sql, pg_conn)
    return df
## execute mc data function:

def reddit_agg_by_day(pg_conn=sql_con):
    "queries database for reddit post data"
    sql = """select num_posts, name, created
        from coin.reddit_post_by_day_agg"""
    df = pd.read_sql(sql, pg_conn)
    return df

def reddit_trends_df(pg_conn=sql_con):
    "queries database for reddit post trends data"
    sql = """Select post_id, created, title, diff, score, num_comments, name
    from coin.reddit_trends
    where diff <= 1000 """
    df = pd.read_sql(sql, pg_conn)
    return df

df_rt = reddit_trends_df(sql_con)
df_mc = market_cap_df(sql_con)
coin_list = list(df_mc['name'].unique())
df_red_agg = reddit_agg_by_day(sql_con) 

## layout
app.layout = html.Div([
    html.Div(
        [
            html.Div(id='display_pct_change',
                className='three columns'),
            html.H2(children='Crypto Currency Dashboard',
                    style={'text-align':'center'},
                    className='six columns'),
            html.H5(children="""Created by: Keerthan Vantakala
                             https://github.com/vantaka2""",
                    style={'float':'right'},
                    className='three columns')
        ], className="row"
    ),
    html.Div(
        [
            html.Div(
                [
                    html.P('Search by Coin:'),
                    dcc.Dropdown(
                        id='coin_select',
                        options=[
                            {'label':i, 'value':i}
                            for i in coin_list
                        ],
                        multi=True
                    ),
                ], className='seven columns'
            ),
            html.Div(
                [
                    html.P('Quick Filters'),
                    dcc.RadioItems(
                        id='quick_filter',
                        options=[
                            {'label':'Top 5', 'value':5},
                            {'label':'Top 10', 'value':10},
                            {'label':'Top 25', 'value':25},
                            {'label':'Top 50', 'value':50},
                            {'label':'Top 100', 'value':100}
                        ],
                        labelStyle={'display':'inline-block'}
                    ),
                ], className='three columns'
            ),
            html.Div(
                [
                    html.P('Date filter'),
                    dcc.RadioItems(
                        id='date_filter',
                        options=[
                            {'label':'Last 7 Days', 'value':7},
                            {'label':'Last 24 Hours', 'value':1},
                        ],
                        value=7,
                        labelStyle={'display': 'inline-block',
                                    'text-align':'left'},
                    ),
                ], className='two columns'
            ),

        ], className="row"
    ),
    html.Div(
        [
            html.Div(
                [
                    dcc.Graph(
                        id='total_mc'
                    ),
                ], className='six columns'
            ),
            html.Div(
                [
                    dcc.Graph(
                        id='mc_by_coin'
                    ),
                ], className='six columns'
            )
        ], className="row"
    ),
    html.Div(
        [
            html.Div(
                [
                    dcc.Graph(
                        id='reddit_trends'
                    ),
                    ], className='six columns'
            ),
            html.Div(
                [
                    dcc.Graph(
                        id='reddit_post_agg'
                    ),
                ], className='six columns'
            ),
        ], className="row"
    )
], className='ten columns offset-by-one'
)

## Callbacks
@app.callback(
    dash.dependencies.Output('coin_select', 'value'),
    [dash.dependencies.Input('quick_filter', 'value')])
def set_coin_select(qf_value):
    print(qf_value)
    if qf_value == None:
        value = ['Nano', 'VeChain', 'Enigma']
    else:
        value = df_mc[df_mc['current_rank'] <= qf_value]['name'].unique()
    print(value)
    return value

@app.callback(
    dash.dependencies.Output('display_pct_change', 'children'),
    [dash.dependencies.Input('coin_select', 'value'),
    dash.dependencies.Input('date_filter', 'value')])
def pct_change(coin_select, date_filter):
    df = filter_df(df_mc, coin_select, date_filter)
    start = df[df['insert_timestamp'] == df.min()['insert_timestamp']].sum()['market_cap_usd']
    end = df[df['insert_timestamp'] == df.max()['insert_timestamp']].sum()['market_cap_usd']
    pct_change = round(((end-start)/start)*100)
    return 'Pct Change: {} %'.format(pct_change)

#total_MC_Graph
@app.callback(
    dash.dependencies.Output('total_mc', 'figure'),
    [dash.dependencies.Input('coin_select', 'value'),
    dash.dependencies.Input('date_filter', 'value')])
def update_total_mc(coin_select, date_filter):
    print("Coin_select: {}".format(coin_select))
    print("date_filter: {}".format(date_filter))
    df_total_mc = filter_df(df_mc, coin_select, date_filter)
    print(df_total_mc.head())
    data = [{
        'x':df_total_mc.groupby('insert_timestamp', as_index=False).agg('sum').sort_values('insert_timestamp')['insert_timestamp'],
        'y':df_total_mc.groupby('insert_timestamp', as_index=False).agg('sum').sort_values('insert_timestamp')['market_cap_usd'],
        'type': 'line',
        'name': 'Total MC'}]
    return {'data':data,
            'layout':{
                'title': 'Top Market cap'}
            }
## MC by coin graph
@app.callback(
    dash.dependencies.Output('mc_by_coin', 'figure'),
    [dash.dependencies.Input('coin_select', 'value'),
    dash.dependencies.Input('date_filter', 'value')])
def update_mc_by_coin(coin_select, date_filter):
    df_coin_mc_stg = filter_df(df_mc, coin_select, date_filter)
    df_coin_mc = df_coin_mc_stg.sort_values(by=['id','insert_timestamp'])
    data = [
        go.Scatter(
            x=df_coin_mc[df_coin_mc['name'] == i]['last_updated'],
            y=df_coin_mc[df_coin_mc['name'] == i]['pct_change'],
            mode='line',
            opacity=0.8,
            name=i
        ) for i in coin_select
    ]
    layout = go.Layout(
        title='Coin Level Market Cap',
        yaxis=dict(
            title='Percent Change'    
        ),
        hovermode='closest'
    )
    figure = {'data':data,
    'layout':layout}
    return figure
##reddit agg graph
@app.callback(
    dash.dependencies.Output('reddit_post_agg', 'figure'),
    [dash.dependencies.Input('coin_select', 'value'),
    dash.dependencies.Input('date_filter', 'value')])
def update_reddit_bar(coin_select, date_filter):
    df_reddit = filter_reddit(df_red_agg, coin_select, date_filter)
    data = [
        go.Bar(
            x=df_reddit[df_reddit['name'] == i]['created'],
            y=df_reddit[df_reddit['name'] == i]['num_posts'],
            name = i
        ) for i in coin_select
    ]
    layout = go.Layout(
        title='Reddit Posts by Day',
        barmode='stack'
        )
    figure={'data':data,
        'layout':layout}
    return figure


##reddit post trends
@app.callback(
    dash.dependencies.Output('reddit_trends', 'figure'),
    [dash.dependencies.Input('coin_select', 'value'),
     dash.dependencies.Input('date_filter', 'value')])
def update_reddit_trends(coin_select, date_filter):
    df_2 = filter_reddit(df_rt, coin_select, date_filter)
    df_trends = df_2.sort_values(['diff']).reset_index(drop=True)
    posts = list(df_trends['post_id'].unique())
    data2 = [
        go.Scatter(
            x=df_trends[df_trends['post_id'] == i]['diff'],
            y=df_trends[df_trends['post_id'] == i]['score'],
            mode='line',
            opacity=0.8,
            name=i,
            hovertext=str(df_trends[df_trends['post_id'] == i]['title'].unique()[0])
        ) for i in posts
    ]
    layout = go.Layout(
        title='Reddit Post Trends',
        yaxis=dict(
            title='Score'
        ),
        hovermode='closest'
    )
    figure = {
        'data':data2,
        'layout':layout
    }
    return figure

#helper function for price data
def filter_df(df=None, coin_select=None, date_filter=None):
    date_cutoff = df.max()['insert_timestamp'] - pd.Timedelta(days=date_filter)
    #coin_filter
    df_stg = df[df['name'].isin(coin_select)]
    #date_filter
    df_stg_2 = df_stg[df_stg['insert_timestamp'] >= date_cutoff]
    coin_list = list(df_stg_2['name'].unique())
    frame = []
    for i in coin_list:
        df_stg_3 = df_stg_2[df_stg_2['name']== i]
        base = df_stg_3[df_stg_3['insert_timestamp'] == df_stg_3.min()['insert_timestamp']]['market_cap_usd'].reset_index(drop=True)[0]
        print(base)
        df_stg_3['pct_change'] = (((df_stg_3['market_cap_usd']-base)/ base) * 100 )
        frame.append(df_stg_3)
    df_stg_4 = pd.concat(frame)
    return df_stg_4
#base2 = df2[df2['insert_timestamp'] == df2.min()['insert_timestamp']]['market_cap_usd']
# frames = []
# for i in coin_list:
#     df3 = df[df['id']== i]
#     base = df3.min()['market_cap_usd']
#     df3['pct_chang']=( ( (df3['market_cap_usd'] - base ) / base ) * 100 )
#     frames.append(df3)

def filter_reddit(df=None, coin_select=None, date_filter=None):
    df_stg = df[df['name'].isin(coin_select)]
    date_cutoff = df.max()['created'] - pd.Timedelta(days=date_filter)
    df_stg_2 = df_stg[df_stg['created'] >= date_cutoff]
    return df_stg_2

if __name__ == '__main__':
    app.run_server(debug=True, port=8049)
