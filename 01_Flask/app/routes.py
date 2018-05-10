from __future__ import print_function
import os,sys,inspect
from flask import render_template, flash
from cassandra.cluster import Cluster
import pandas as pd
from app import app


currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(os.path.dirname(currentdir))
sys.path.insert(0,parentdir) 
import config

cluster = Cluster(config.Config().cass_cluster_IP)
session = cluster.connect('demo1')

@app.route('/')
@app.route('/index')
def landingpage():
    return render_template('landingpage.html')


@app.route('/realtime')
def realtime_monitor():
    # df = pd.DataFrame(list(session.execute('''SELECT underlying_symbol, total_prem, strike, option_type, expiration, quote_datetime, buy_sell, days_to_exp, trade_delta, z_score
    #  FROM optionflowstreaming3 WHERE underlying_symbol='DJX' ORDER BY total_prem DESC''')))


    df = pd.DataFrame(list(session.execute('''SELECT * FROM intrawindow''')))

    # df = pd.DataFrame(list(session.execute('''SELECT underlying_symbol, quote_datetime, option_type, sum(cum_delta) FROM intrawindow GROUP BY underlying_symbol, quote_datetime, option_type''')))


    # print(df.dtypes, file=sys.stderr)
    df = df[(df.underlying_symbol == "AAPL")]
    df2 = df.groupby(['underlying_symbol', 'quote_datetime', 'option_type'])[["cum_delta"]].sum().reset_index().tail(60)
    # print(df2.dtypes, file=sys.stderr)
    # df2 =df2.sort_values(by=['underlying_symbol'])

    display_df_html_t = df2.to_html(index=True, header=True, justify='center')

    # for row in rows:
    #     flash('You were successfully logged in')
    #     print(type(row), file=sys.stderr)

    
    return render_template('demo.html', table=display_df_html_t)

