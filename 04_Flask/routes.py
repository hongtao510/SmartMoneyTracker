from __future__ import print_function
import os
import sys
from flask import render_template, flash, make_response
from cassandra.cluster import Cluster
import pandas as pd
from flask import Flask
from flask_config import Config
from werkzeug.serving import WSGIRequestHandler
import json
import plotly
import plotly.plotly as py  
import plotly.tools as tls   
import plotly.graph_objs as go


# add one level up path
sys.path.append("..")
import config

app = Flask(__name__)
app.config.from_object(Config)

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


# realtime tracking page
@app.route('/realtime/<ticker>')
def realtime_monitor(ticker):
    try:
        ticker_str = "'"+ticker.upper()+"'"
        if len(ticker_str)==0:
            ticker_str = "SPX"
        print ("checking ", ticker_str, file=sys.stderr)

        ########################
        ### connect to database
        ########################
        cluster = Cluster(config.Config().cass_cluster_IP)
        session = cluster.connect('demo1')

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
        # print (df_tbl_1_html, file=sys.stderr)

        ########################
        ### populate table 2
        ########################
        cql_str2 = '''SELECT * FROM intrawindow WHERE underlying_symbol=%s ORDER BY quote_datetime DESC LIMIT 100''' %(ticker_str)
        
        df1 = pd.DataFrame(list(session.execute(cql_str2)))
        # print (df1.head(), file=sys.stderr)

        # prepare for plot
        df2 = df1.groupby(['quote_datetime', 'option_type'])[["cum_delta"]].sum().reset_index()
        # if includes weekend or cross days, just drop them
        n_row = len(df2.index)
        df2_date_first = df2.loc[0, 'quote_datetime']
        df2_date_last = df2.loc[(n_row-1), 'quote_datetime']
        day_dif = abs(df2_date_first.day-df2_date_last.day)
        print (day_dif, file=sys.stderr)
        if day_dif>0:
            df2 = df2.tail(6)
        # print (df2_date_last.day, file=sys.stderr)
        
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
    except Exception as e:
        return render_template("404.html")

if __name__ == '__main__':
    app.run(host= '0.0.0.0', port=80, threaded=True)
    # app.run(debug=True, threaded=True) 



