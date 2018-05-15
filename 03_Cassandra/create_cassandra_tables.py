#!/usr/bin/env python2
"""
This file creates the two tables in Cassandra used for storage after stream processing.
You can also perform some CRUD operations using this file.
To be able to execute, install cassandra-driver: sudo pip install cassandra-driver
"""
import sys
from cassandra.cluster import Cluster
sys.path.append("..")
import config
import datetime
import pytz
import time
import pandas as pd 
import seaborn as sns
import matplotlib.pyplot as plt
sns.set_style("whitegrid")


if __name__ == "__main__":
    cluster = Cluster(config.Config().cass_cluster_IP)
    session = cluster.connect()

    ### create a keyspace (like database in RDB)
    # session.execute("CREATE KEYSPACE demo1 WITH replication={'class': 'SimpleStrategy', 'replication_factor': '1'}")

    ### choose a keyspace
    session.execute("USE demo1")

    ######################
    ### create a table
    ######################
    session.execute('''CREATE TABLE IF NOT EXISTS intrawindow(
      underlying_symbol text, quote_datetime timestamp, option_type text, 
      cum_delta float, PRIMARY KEY((underlying_symbol), quote_datetime, option_type, cum_delta))''')


    session.execute('''CREATE TABLE IF NOT EXISTS optionflowstreaming3(
      underlying_symbol text, quote_datetime timestamp, expiration timestamp, strike float, option_type text, 
      unusual text, trade_size int, total_prem float, z_score float, buy_sell text, 
      best_bid float, best_ask float, trade_price float, days_to_exp int, error text,
      exp_bin text, delta_bin text, trade_delta float, trade_iv float, trade_condition_id int,
      canceled_trade_condition_id int, PRIMARY KEY((underlying_symbol), total_prem, expiration, quote_datetime))''')

    ######################################
    ### test to insert a few records
    ######################################
    # session.execute('''INSERT INTO intrawindow (underlying_symbol, quote_datetime, option_type, 
    #                  cum_delta) 
    #                  VALUES (%s, %s, %s, %s)''', 
    #                    ("DJX", "2018-01-02 09:31:00", "call", -12.90)
    #                 )


    # session.execute('''INSERT INTO optionflowstreaming3 (underlying_symbol, quote_datetime, expiration, strike, option_type, 
    #                  unusual, trade_size, total_prem, z_score, buy_sell, 
    #                  best_bid, best_ask, trade_price, days_to_exp, error, 
    #                  exp_bin, delta_bin, trade_delta, trade_iv, trade_condition_id, canceled_trade_condition_id) 
    #                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', 
    #                    ("DJX", "2018-02-02 10:20:06.200", "2018-12-21", 530.000, "call", 
    #                     "yes", 99, 777, 0.28, "SELL",
    #                     116.0000, 117.3000, 116.3600, 353, "",
    #                     "long", "40-60", 0.5000, 0.0000, 35, 0)
    #                 )

    # session.execute('''INSERT INTO optionflowstreaming3 (underlying_symbol, quote_datetime, expiration, strike, option_type, 
    #                  unusual, trade_size, total_prem, z_score, buy_sell, 
    #                  best_bid, best_ask, trade_price, days_to_exp, error, 
    #                  exp_bin, delta_bin, trade_delta, trade_iv, trade_condition_id, canceled_trade_condition_id) 
    #                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', 
    #                    ("SPX", "2018-01-02 13:20:06.200", "2018-10-21", 180.000, "call", 
    #                     "yes", 99, 1151964, 0.28, "SELL",
    #                     116.0000, 117.3000, 116.3600, 353, "",
    #                     "long", "40-60", 0.5000, 0.0000, 35, 0)
    #                 )

    # session.execute('''INSERT INTO optionflowstreaming3 (underlying_symbol, quote_datetime, expiration, strike, option_type, 
    #                  unusual, trade_size, total_prem, z_score, buy_sell, 
    #                  best_bid, best_ask, trade_price, days_to_exp, error, 
    #                  exp_bin, delta_bin, trade_delta, trade_iv, trade_condition_id, canceled_trade_condition_id) 
    #                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', 
    #                    ("SPX", "2018-01-02 13:20:06.200", "2018-11-21", 280.000, "call", 
    #                     "yes", 99, 6666, 0.28, "SELL",
    #                     116.0000, 117.3000, 116.3600, 353, "",
    #                     "long", "40-60", 0.5000, 0.0000, 35, 0)
    #                 )

    # session.execute('''INSERT INTO optionflowstreaming3 (underlying_symbol, quote_datetime, expiration, strike, option_type, 
    #                  unusual, trade_size, total_prem, z_score, buy_sell, 
    #                  best_bid, best_ask, trade_price, days_to_exp, error, 
    #                  exp_bin, delta_bin, trade_delta, trade_iv, trade_condition_id, canceled_trade_condition_id) 
    #                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', 
    #                 (u'DJX', u'2018-01-02 10:38:23.700', u'2019-12-20', 210.0, u'call', 
    #                     'yes', 40, 190000.0, 0.31, u'SELL', 
    #                     47.5, 48.55, 47.5, 717, u'', 
    #                     u'long', u'60-80', 0.7751, 0, 0, 0)
    #                 )


    ######################################
    ## To retrieve what I have just inserted:
    ######################################
    #rows = session.execute('''SELECT * FROM optionflowstreaming3 WHERE underlying_symbol='AAPL' ORDER BY total_prem DESC''')

    #df = pd.DataFrame(list(session.execute('''SELECT underlying_symbol, total_prem, quote_datetime, expiration, buy_sell, option_type, strike, trade_size, z_score FROM optionflowstreaming3 WHERE underlying_symbol='SPX' ORDER BY total_prem DESC LIMIT 10''')))
    # print df

    #rows = session.execute('''SELECT * FROM intrawindow WHERE underlying_symbol='SPX' ''')
    # df = pd.DataFrame(list(session.execute('''SELECT * FROM intrawindow WHERE underlying_symbol='AAPL' ''')))
    # print df.tail(50)

    ######################################
    ## Delete table
    # session.execute('''DROP TABLE intrawindow;''')

    ## remove all rows but keep schema
    rows1 = session.execute('''TRUNCATE intrawindow''')
    rows2 = session.execute('''TRUNCATE optionflowstreaming3''')

    # for row in rows:
    # # #     print row, type(row), len(row)
    #     print row[0], row[1], row[2], row[3]
    #     # print row['underlying_symbol'], datetime.datetime.strptime(row['quote_datetime'], "%Y-%m-%d %I:%M:%S"), row['option_type'], row['cum_delta']
        # print row.__dict__


    # session.execute("CREATE TABLE metrics (type text, event_time timestamp, value int, PRIMARY KEY (type, event_time))")
    # session.execute("CREATE TABLE visit_rank (type text, event_time timestamp, rank int, ip text, visits int, PRIMARY KEY (type, event_time, rank))")


