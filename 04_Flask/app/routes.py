from __future__ import print_function
import os
import sys
import inspect
import json
from flask import render_template, request, Flask
from cassandra.cluster import Cluster
import pandas as pd
from flask_config import Config
import plotly
import plotly.plotly as py
import plotly.tools as tls
import plotly.graph_objs as go
from app import app

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(os.path.dirname(currentdir))
sys.path.insert(0,parentdir) 
import config

########################
### connect to database
########################
cluster = Cluster(config.Config().cass_cluster_IP)
session = cluster.connect('demo1')

# 404 page
@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html')


# landing page
@app.route('/')
@app.route('/index')
def landingpage():
    try:
        return render_template('landingpage.html')
    except Exception as e:
        return render_template("404.html")


@app.route('/realtime2/')
def realtime_monitor2():
    return render_template('realtime_portal.html')


# realtime tracking page
@app.route('/realtime/<ticker>')
def realtime_monitor(ticker):
    # try:
    ticker_str = "'"+ticker.replace("q=?", "").upper()+"'"

    if len(ticker_str)==0:
        ticker_str = "SPX"
    print ("checking ", ticker_str, file=sys.stderr)


    ########################
    ### populate table 1
    ########################
    cql_str1 = '''SELECT underlying_symbol, total_prem, quote_datetime, expiration, buy_sell, option_type, \
              strike, trade_size, z_score FROM optionflowstreaming3 WHERE underlying_symbol=%s ORDER BY total_prem DESC LIMIT 10''' %(ticker_str)
    
    df_tbl_1 = pd.DataFrame(list(session.execute(cql_str1)))
    # format tbl
    df_tbl_1.rename(columns={'underlying_symbol': "Ticker", 
                             'trade_size': "Volume",
                             'z_score': "Z-score (Vol)",
                             'total_prem': "Premium (USD)",
                             'expiration': "Expiration",
                             'buy_sell': "Buy or Sell",
                             'option_type': "Call or Put",
                             'strike': "Strike",
                             'quote_datetime': "Transaction Time"
                           }, inplace=True)

    df_tbl_1 = df_tbl_1[['Ticker', 'Volume', "Z-score (Vol)", "Expiration", "Buy or Sell", "Call or Put", "Strike", "Premium (USD)", "Transaction Time"]]
    df_tbl_1["Transaction Time"] = df_tbl_1["Transaction Time"].apply(lambda x: x.replace(microsecond=0))
    df_tbl_1['Premium (USD)'] = df_tbl_1['Premium (USD)'].apply(lambda x: "{:,}".format(x))
    df_tbl_1_html = df_tbl_1.to_html(index=False, header=True)
    # print (df_tbl_1, file=sys.stderr)

    ########################
    ### populate table 2
    ########################
    cql_str2 = '''SELECT * FROM intrawindow WHERE underlying_symbol=%s ORDER BY quote_datetime DESC LIMIT 100''' %(ticker_str)
    df1 = pd.DataFrame(list(session.execute(cql_str2)))
    

    # prepare for plot
    df2 = df1.groupby(['quote_datetime', 'option_type'])[["cum_delta"]].sum().reset_index()
    print (df2.shape, file=sys.stderr)
    print (df2.tail(), file=sys.stderr)

    # if includes weekend or cross days, just drop them
    n_row = len(df2.index)
    df2_date_first = df2.loc[0, 'quote_datetime']
    df2_date_last = df2.loc[(n_row-1), 'quote_datetime']
    day_dif = abs(df2_date_first.day-df2_date_last.day)
    # print (day_dif, file=sys.stderr)

    if day_dif>0:
    	print ("cross day before trim... ", df2.shape, file=sys.stderr)
        df2 = df2.tail(10)
    	print ("cross day ", day_dif, file=sys.stderr)
    
    df2_calls = df2.loc[df2['option_type'] == "call"] 
    df2_puts = df2.loc[df2['option_type'] == "put"] 
    
    trace1 = dict(
            x=df2_calls['quote_datetime'].tolist(),
            y=df2_calls['cum_delta'].tolist(),
            name="calls",
            type='lines',
            )

    trace2 = dict(
            x=df2_puts['quote_datetime'].tolist(),
            y=df2_puts['cum_delta'].tolist(),
            name="puts",
            type='lines'
        )

    trace1_JSON = json.dumps(trace1, cls=plotly.utils.PlotlyJSONEncoder)
    trace2_JSON = json.dumps(trace2, cls=plotly.utils.PlotlyJSONEncoder)

    return render_template('realtime_track.html', trace1_data=trace1_JSON, trace2_data=trace2_JSON, table=df_tbl_1_html)
    # except Exception as e:
    #     return render_template("404.html")



