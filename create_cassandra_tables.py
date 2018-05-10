#!/usr/bin/env python2
"""
This file creates the two tables in Cassandra used for storage after stream processing.
To be able to execute, install cassandra-driver: sudo pip install cassandra-driver
"""

from cassandra.cluster import Cluster
import config
import datetime
import pytz
import time
import pandas as pd 

# quote_datetime = datetime.datetime.strptime("2018-01-02 10:20:06.200", "%Y-%m-%d %I:%M:%S.%f")
# unixtime = time.mktime(quote_datetime.timetuple())
# print quote_datetime, unixtime



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



    ## To retrieve what I have just inserted:
    # rows = session.execute('''SELECT * FROM optionflowstreaming3 WHERE underlying_symbol='DJX' ORDER BY total_prem DESC''')
    rows = session.execute('''SELECT * FROM intrawindow''')

    ## Delete table
    # session.execute('''DROP TABLE intrawindow;''')


    ## remove all rows but keep schema
    # rows = session.execute('''TRUNCATE intrawindow''')

    df = pd.DataFrame(list(rows))

    print df 
    # for row in rows:
    #     print row, type(row), len(row)
        # print row[0], row[1], row[2], round(row[3], 2)
        # print row['underlying_symbol'], datetime.datetime.strptime(row['quote_datetime'], "%Y-%m-%d %I:%M:%S"), row['option_type'], row['cum_delta']
        # print row.__dict__

# OrderedDict([('underlying_symbol', u'FDX'), ('quote_datetime', datetime.datetime(2018, 1, 2, 11, 29)), ('option_type', u'put'), ('cum_delta', -1.1799999475479126)])

    # session.execute("CREATE TABLE metrics (type text, event_time timestamp, value int, PRIMARY KEY (type, event_time))")
    # session.execute("CREATE TABLE visit_rank (type text, event_time timestamp, rank int, ip text, visits int, PRIMARY KEY (type, event_time, rank))")


'''
{u'trade_iv': u'0.0000', 
 u'days_to_exp': 353, 
 u'best_bid': u'116.0000', 
 u'exp_bin': u'long', 
 u'buy_sell': u'SELL', 
 u'underlying_symbol': u'DJX', 
 u'quote_datetime': u'2018-01-02 10:20:06.200', 
 u'total_prem': 1151964, 
 u'best_ask': u'117.3000', 
 u'unusual': 'yes', 
 u'option_type': u'call', 
 u'z_score': 0.28, 
 u'delta_bin': u'40-60', 
 u'expiration': u'2018-12-21', 
 u'error': u'', 
 u'strike': u'130.000', 
 u'canceled_trade_condition_id': u'0', 
 u'trade_size': u'99', 
 u'trade_delta': u'0.5000', 
 u'trade_condition_id': u'35', 
 u'trade_price': u'116.3600'}
'''


# {u'trade_iv': u'0.0876', u'days_to_exp': 3, u'best_bid': u'1.2900', u'exp_bin': u'short', u'buy_sell': u'BUY', u'underlying_symbol': u'DJX', u'quote_datetime': u'2018-01-02 10:37:49.200', u'total_prem': 14796, u'best_ask': u'1.3700', u'unusual': 'yes', u'option_type': u'put', u'z_score': 0.34, u'delta_bin': u'0-20', u'expiration': u'2018-01-05', u'error': u'', u'strike': u'249.000', u'canceled_trade_condition_id': u'0', u'trade_size': u'108', u'trade_delta': u'-0.6912', u'trade_condition_id': u'0', u'trade_price': u'1.3700'}

# {u'trade_iv': u'0.0876', u'days_to_exp': 3, u'best_bid': u'1.2900', u'exp_bin': u'short', u'buy_sell': u'BUY', u'underlying_symbol': u'DJX', u'quote_datetime': u'2018-01-02 10:37:49.200', u'total_prem': 14796, u'best_ask': u'1.3700', u'unusual': 'yes', u'option_type': u'put', u'z_score': 0.34, u'delta_bin': u'0-20', u'expiration': u'2018-01-05', u'error': u'', u'strike': u'249.000', u'canceled_trade_condition_id': u'0', u'trade_size': u'108', u'trade_delta': u'-0.6912', u'trade_condition_id': u'0', u'trade_price': u'1.3700'}

# {u'trade_iv': u'0.0000', u'days_to_exp': 353, u'best_bid': u'116.0000', u'exp_bin': u'long', u'buy_sell': u'SELL', u'underlying_symbol': u'DJX', u'quote_datetime': u'2018-01-02 10:20:06.200', u'total_prem': 1151964, u'best_ask': u'117.3000', u'unusual': 'yes', u'option_type': u'call', u'z_score': 0.28, u'delta_bin': u'40-60', u'expiration': u'2018-12-21', u'error': u'', u'strike': u'130.000', u'canceled_trade_condition_id': u'0', u'trade_size': u'99', u'trade_delta': u'0.5000', u'trade_condition_id': u'35', u'trade_price': u'116.3600'}

# {u'trade_iv': u'0.1900', u'days_to_exp': 717, u'best_bid': u'38.5500', u'exp_bin': u'long', u'buy_sell': u'BUY', u'underlying_symbol': u'DJX', u'quote_datetime': u'2018-01-02 10:20:06.200', u'total_prem': 800514, u'best_ask': u'41.1500', u'unusual': 'yes', u'option_type': u'call', u'z_score': 3.16, u'delta_bin': u'60-80', u'expiration': u'2019-12-20', u'error': u'', u'strike': u'220.000', u'canceled_trade_condition_id': u'0', u'trade_size': u'198', u'trade_delta': u'0.7253', u'trade_condition_id': u'35', u'trade_price': u'40.4300'}

# {u'trade_iv': u'0.1900', u'days_to_exp': 717, u'best_bid': u'38.5500', u'exp_bin': u'long', u'buy_sell': u'BUY', u'underlying_symbol': u'DJX', u'quote_datetime': u'2018-01-02 10:20:06.200', u'total_prem': 800514, u'best_ask': u'41.1500', u'unusual': 'yes', u'option_type': u'call', u'z_score': 3.16, u'delta_bin': u'60-80', u'expiration': u'2019-12-20', u'error': u'', u'strike': u'220.000', u'canceled_trade_condition_id': u'0', u'trade_size': u'198', u'trade_delta': u'0.7253', u'trade_condition_id': u'35', u'trade_price': u'40.4300'}