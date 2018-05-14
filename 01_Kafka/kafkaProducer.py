#!/usr/bin/env python2
from kafka import KafkaProducer
import time
import streaming_intraday
import json 
import smart_open 
import config

############################################################
# This python script is a producer for kafka. It receives  
# streaming option transaction data from S3 (csv file) and 
# send to kafka. The data is in JSON
# format. Here is the schema:
#
# {"trade_iv": "0.304", "days_to_exp": 73, "best_bid": "7", 
#  "unusual": "no", "exp_bin": "long", "buy_sell": "SELL", 
#   "underlying_symbol": "WYNN", "quote_datetime": "2018-01-02 09:30:00.473", 
#   "total_prem": 35000, "best_ask": "7.15", "option_type": "call", 
#   "z_score": "na", "delta_bin": "40-60", "expiration": "2018-03-16", 
#   "error": "", "strike": "170", "canceled_trade_condition_id": "0", 
#   "trade_size": "50", "trade_delta": "0.4482", "trade_condition_id": "18", 
#   "trade_price": "7"}
############################################################

def main():
    ##############################
    # load pre-defined settings
    ##############################
    config_pool = config.Config()
    S3_KEY = config_pool.S3_KEY
    S3_SECRET = config_pool.S3_SECRET
    S3_BUCKET = config_pool.S3_BUCKET
    num_record = config_pool.num_record_streamed
    intraday_fname = config_pool.intraday_fname
    bootstrap_servers_address = config_pool.bootstrap_servers_address
    kafka_topic = config_pool.kafka_topic

    ##############################################################
    # create a streaming object from S3 and setup kafka producer
    ##############################################################
    obj_stream = smart_open.smart_open("https://s3-us-west-2.amazonaws.com/"+S3_BUCKET+"/"+intraday_fname)

    # setup Kafka producer and topic
    # producer = KafkaProducer(bootstrap_servers = [bootstrap_servers_address], 
    #                          compression_type='lz4',
    #                          batch_size=720000, 
    #                          linger_ms=1500)
    producer = KafkaProducer(bootstrap_servers = bootstrap_servers_address) 

    k=0
    start = time.time()
    for line in obj_stream:
        try:
            list_temp = line.split(",")
            # skip header
            if k!=0:
                message_info = streaming_intraday.streaminglinestodict(list_temp)
                message_info = json.dumps(message_info).encode('utf-8')
                producer.send(kafka_topic, message_info)
                if k%10000==0:
                    end = time.time()
                    print "msg=", k, end-start, message_info
                    start = time.time()
                    print "\n"
            k+=1
            if k==num_record:
                print "reaching target"
                end = time.time()
                elapsed = end - start
                print elapsed
                break
        except:
            print "running into an error in kafka producer..."


if __name__ == '__main__':
    main() 

