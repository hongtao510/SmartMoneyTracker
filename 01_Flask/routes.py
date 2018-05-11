from __future__ import print_function
import os,sys,inspect
from flask import render_template, flash, make_response
from cassandra.cluster import Cluster
import pandas as pd
from flask import Flask
from flask_config import Config
from werkzeug.serving import WSGIRequestHandler
from flask_socketio import SocketIO, emit
import time
from random import randint
from forms import TickerForm

import json
from random import random

import plotly
import plotly.plotly as py  
import plotly.tools as tls   
import plotly.graph_objs as go


# add one level up path
sys.path.append("..")
import config

app = Flask(__name__)
app.config.from_object(Config)


@app.route('/')
@app.route('/index')
def landingpage():
    return render_template('landingpage.html')


@app.route('/realtime/<ticker>')
def realtime_monitor(ticker):
    ticker = ticker.upper()
    print (ticker, file=sys.stderr)

    cluster = Cluster(config.Config().cass_cluster_IP)
    session = cluster.connect('demo1')
    #cql_str = "SELECT * FROM intrawindow WHERE underlying_symbol='"+str(ticker)+"'"
    #print (cql_str, file=sys.stderr)
    df = pd.DataFrame(list(session.execute('''SELECT * FROM intrawindow''')))
    df_tbl = pd.DataFrame(list(session.execute('''SELECT underlying_symbol, total_prem, quote_datetime, expiration, buy_sell, option_type, strike, trade_size, z_score FROM optionflowstreaming3 WHERE underlying_symbol='SPX' ORDER BY total_prem DESC LIMIT 10''')))
    df_tbl.rename(columns={'underlying_symbol': 'Ticker', 
                           'total_prem': 'Premium',
                           'quote_datetime': 'Transaction Time',
                           'expiration': "Expiration",
                           'buy_sell': "Buy or Sell",
                           'option_type': "Call or Put",
                           'strike': "Strike",
                           'trade_size': "Size",
                           'z_score': "Z-score"
                           }, inplace=True)
    df_tbl_html = df_tbl.to_html(index=False, header=True, justify='center')


    df1 = df.loc[df['underlying_symbol']=='SPX'] 
    df2 = df1.groupby(['quote_datetime', 'option_type'])[["cum_delta"]].sum().reset_index().tail(30)

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

    return render_template('demo.html', trace1_data=trace1_JSON, trace2_data=trace2_JSON, table=df_tbl_html)


if __name__ == '__main__':
    app.run(host= '0.0.0.0', port=80, debug=True, threaded=True)
    # app.run(debug=True) 



