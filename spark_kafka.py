#!/usr/bin/env python2
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import config
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement
from pyspark.sql import SQLContext
from pyspark.sql.functions import broadcast
import ast
import datetime

# from pyspark.sql.functions import udf
# from pyspark.sql.types import BooleanType

# https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-python.html

reload(sys)
sys.setdefaultencoding('utf-8')


def push_to_cassandra(raw_data):
    '''CREATE TABLE optionflowstreaming3(
          underlying_symbol text, quote_datetime timestamp, expiration timestamp, strike float, option_type text, 
          unusual text, trade_size int, total_prem float, z_score float, buy_sell text, 
          best_bid float, best_ask float, trade_price float, days_to_exp int, error text,
          exp_bin text, delta_bin text, trade_delta float, trade_iv float, trade_condition_id int,
          canceled_trade_condition_id int, PRIMARY KEY((underlying_symbol), total_prem, expiration, quote_datetime))
    '''
    print "========"
    cassandra_cluster = Cluster(config.Config().cass_cluster_IP)
    cassandra_session = cassandra_cluster.connect('demo2')
    insert_option_flow = cassandra_session.prepare('''INSERT INTO optionflowstreaming3 (underlying_symbol, quote_datetime, expiration, strike, option_type, 
                                                      unusual, trade_size, total_prem, z_score, buy_sell, 
                                                      best_bid, best_ask, trade_price, days_to_exp, error, 
                                                      exp_bin, delta_bin, trade_delta, trade_iv, trade_condition_id, canceled_trade_condition_id) 
                                                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                   ''')
    batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)


    for d in raw_data:
        d = ast.literal_eval(json.dumps(d))
        print "d====", type(d), d

        try:
            quote_datetime_t = datetime.datetime.strptime(d['quote_datetime'], "%Y-%m-%d %I:%M:%S.%f")
        except:
            quote_datetime_t = datetime.datetime(2050,9,29)

        try:
            expiration_t = datetime.datetime.strptime(d['expiration'], "%Y-%m-%d")
        except:
            expiration_t = datetime.datetime(2050,9,29)

        try:
            strike_t = float(d['strike'])
        except:
            strike_t = 0.001

        try:
            trade_size_t = int(float(d['trade_size']))
        except:
            trade_size_t = 0

        try:
            total_prem_t = float(d['total_prem'])
        except:
            total_prem_t = 0.001

        try:
            z_score_t = float(d['z_score'])
        except:
            z_score_t = 100.0

        try:
            best_bid_t = float(d['best_bid'])
        except:
            best_bid_t = 0.001

        try:
            best_ask_t = float(d['best_ask'])
        except:
            best_ask_t = 0.001

        try:
            trade_price_t = float(d['trade_price'])
        except:
            trade_price_t = 0.001

        try:
            days_to_exp_t = int(float(d['days_to_exp']))
        except:
            days_to_exp_t = 0

        try:
            trade_delta_t = float(d['trade_delta'])
        except:
            trade_delta_t = 0.001

        try:
            trade_iv_t = float(d['trade_iv'])
        except:
            trade_iv_t = 0.001

        try:
            trade_condition_id_t = int(float(d['trade_condition_id']))
        except:
            trade_condition_id_t = 0

        try:
            trade_iv_t = int(float(d['trade_condition_id']))
        except:
            trade_iv_t = 0

        try:
            canceled_trade_condition_id_t = int(float(d['canceled_trade_condition_id']))
        except:
            canceled_trade_condition_id_t = 0


        batch.add(insert_option_flow, (d['underlying_symbol'],
                                       quote_datetime_t,
                                       expiration_t,
                                       strike_t,
                                       d['option_type'],
                                       d['unusual'],
                                       trade_size_t,
                                       total_prem_t,
                                       z_score_t,
                                       d['buy_sell'],
                                       best_bid_t,
                                       best_ask_t,
                                       trade_price_t,
                                       days_to_exp_t,
                                       d['error'],
                                       d['exp_bin'],
                                       d['delta_bin'],
                                       trade_delta_t,
                                       trade_iv_t,
                                       trade_condition_id_t,
                                       canceled_trade_condition_id_t))


    cassandra_session.execute(batch)
    cassandra_cluster.shutdown()
    print "************SAVED*********"

def sparkrowtodict(row):
    '''
    function convert pyspark row to python dict
    '''
    dict_temp = row.asDict()
    dict_reorged_temp = {(dict_temp['Symbol'], dict_temp['PutCall'], dict_temp['DayDiffBins'], dict_temp['OptionsBins']):[dict_temp['mean_vol'], dict_temp['median_vol'], dict_temp['sd_vol']]}
    return dict_reorged_temp

def sparkfilter(x):
    '''
    function used to query a broadcasted hash-table to get background 
    option transaction volume and compare with streaming data
    example of key_tuple = ('DJX', 'call', 'short', '60-80')

    {"trade_iv": "0.0000", "days_to_exp": 11, "best_bid": "0.0000", "exp_bin": "short", 
     "buy_sell": "SELL", "underlying_symbol": "BKX", "quote_datetime": "2018-01-08 11:11:05.375", 
     "total_prem": 28500, "best_ask": "5.0000", "option_type": "call", "delta_bin": "40-60", 
     "expiration": "2018-01-19", "error": "", "strike": "110.000", "canceled_trade_condition_id": "0", 
     "trade_size": "570", "trade_delta": "0.5000", "trade_condition_id": "0", "trade_price": "0.5000"}
    '''
    key_tuple = (x['underlying_symbol'], x['option_type'], x['exp_bin'], x['delta_bin'])
    try:
        temp_value=list_5.value[key_tuple]
        trade_size_diff_temp = float(x['trade_size']) - float(temp_value[0])

        if trade_size_diff_temp>0:
            x['unusual'] = "yes"
            z_score_temp = round(trade_size_diff_temp/float(temp_value[2]), 2)
            # print z_score_temp
            x['z_score'] = z_score_temp
    except:
        pass
    return x



if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingDirectKafka")
    ssc = StreamingContext(sc, config.Config().ss_interval)
    sql_sc = SQLContext(sc)
    # load one year end of day average results
    lines = sql_sc.read.csv("s3n://taohonginsight18b/eod_summary_withoutzeros.csv", 
                            header=True, mode="DROPMALFORMED")
    list_2 = lines.rdd.map(sparkrowtodict)
    list_3 = sc.broadcast(list_2.collect())

    list_4 = {}
    # for d in list_3.value:
    for d in list_2.collect():
        list_4.update(d)
    list_5 = sc.broadcast(list_4)

    kvs = KafkaUtils.createDirectStream(ssc, [config.Config().kafka_topic],
                                        {"metadata.broker.list": config.Config().bootstrap_servers})
    parsed_msg = kvs.map(lambda (key, value): json.loads(value))
    # parsed_msg.pprint()
    parsed_msg2 = parsed_msg.map(sparkfilter)
    # parsed_msg2.collect()

    unusual_rows = parsed_msg2.filter(lambda x: "yes" in x['unusual'])
    unusual_rows.pprint()
    # print (type(unusual_rows))

    # parsed_msg2.pprint()
    unusual_rows.foreachRDD(lambda rdd: rdd.foreachPartition(push_to_cassandra))

    ssc.start()
    ssc.awaitTermination()


