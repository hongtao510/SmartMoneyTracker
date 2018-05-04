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
session = cluster.connect('demo2')

@app.route('/')
@app.route('/index')
def landingpage():
    return render_template('landingpage.html')


@app.route('/realtime/<ticker>')
def realtime_monitor(ticker):
#     # print(app.config['cass_cluster_IP'], file=sys.stderr)
    # cql_str = '''SELECT * FROM optionflowstreaming3 WHERE underlying_symbol=' '''+ticker+''' ' ORDER BY total_prem DESC
    # '''
# #     response = session.execute(stmt, parameters=[metric])
    # rows = session.execute(cql_str)
#     total_n = session.execute("SELECT COUNT(*) FROM optionflowstreaming;")

#     # df = pd.DataFrame(list(session.execute("SELECT * FROM optionflowstreaming LIMIT 10")))
    df = pd.DataFrame(list(session.execute('''SELECT underlying_symbol, total_prem, strike, option_type, expiration, quote_datetime, buy_sell, days_to_exp, trade_delta, z_score
     FROM optionflowstreaming3 WHERE underlying_symbol='DJX' ORDER BY total_prem DESC''')))

    display_df_html_t = df.to_html(index=True, header=True, justify='center')

    # for row in rows:
    #     flash('You were successfully logged in')
    #     print(type(row), file=sys.stderr)

    
    return render_template('demo.html', table=display_df_html_t)

