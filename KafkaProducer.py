#!/usr/bin/env python2
import random
import sys
import datetime
from kafka import KafkaProducer
import time
import pandas
import config
import streaming_intraday
import json 

############################################################
# This python script is a producer for kafka. It receives  
# streaming option transaction data from S3 (csv file) and 
# send to kafka. The data is in JSON
# format. Here is the schema:
#
# 
############################################################

def main():
    # load pre-defined settingsß
    config_pool = config.Config()
    S3_KEY = config_pool.S3_KEY
    S3_SECRET = config_pool.S3_SECRET
    S3_BUCKET = config_pool.S3_BUCKET
    num_record = config_pool.num_record_streamed
    intraday_fname = config_pool.config_pool
    bootstrap_servers_address = config_pool.bootstrap_servers_address
    kafka_topic = config_pool.kafka_topic

    # create a streaming object from S3
    obj_stream = streaming_intraday.S3ObjectInterator(S3_KEY, S3_SECRET, S3_BUCKET, intraday_fname)

    # setup Kafka producer and topic
    producer = KafkaProducer(bootstrap_servers = [bootstrap_servers_address])

    k=0
    for line in obj_stream:
        try:
            list_temp = line.split(",")
            # skip header
            if k!=0:
                message_info = streaming_intraday.streaminglinestodict(list_temp)
                message_info = json.dumps(message_info).encode('utf-8')
                print "msg=", k, message_info
                producer.send(kafka_topic, message_info.encode('utf-8'))
                time.sleep(1)
                print "\n"
            k+=1
            if k==num_record:
                break
        except:
            print "running into an error in kafka producer, but it is OK..."

    # block until all async messages are sent
    # producer.flush()

    # configure multiple retries
    # producer = KafkaProducer(retries=5)


# try:

#     print "streaming record ", k
#     # print test_sub.loc[k,], "\n"
#     # There could be more than 1 record per user per second, so microsecond is added to make each record unique.
#     time_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %f")
#     message_info = '''{"time_sent_kafka": "%s", 
#                      "underlying_symbol": "%s", 
#                      "quote_datetime": "%s", 
#                      "expiration": "%s", 
#                      "strike": "%s", 
#                      "option_type": "%s"}''' \
#                      % (time_stamp, 
#                         test.loc[k, 'underlying_symbol'], 
#                         test.loc[k, 'quote_datetime'], 
#                         test.loc[k, 'expiration'],
#                         test.loc[k,'strike'], 
#                         test.loc[k, 'option_type'])

#     producer.send(topic, message_info.encode('utf-8'))
#     print message_info
#     time.sleep(2)
# except:
#     print "running into an error in kafka producer, but it is OK..."



# # import a sample data (intra-day transactions for SPX) for demo
# test = pandas.read_csv('./data/UnderlyingOptionsTradesCalcs_2017-01-03.csv', sep=',')
# # test_sub = test[['underlying_symbol', 'quote_datetime', 'expiration', 'strike', 'option_type']]
# # print test_sub.head()


# def main():
#     # number of records
#     nRecords = test.shape[0]
#     while True:
#         try:
#             producer = KafkaProducer(bootstrap_servers = [config.Config().bootstrap_servers])
#             topic = config.Config().kafka_topic
#             k = random.randint(0,nRecords-1)
#             print "streaming record ", k
#             # print test_sub.loc[k,], "\n"
#             # There could be more than 1 record per user per second, so microsecond is added to make each record unique.
#             time_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %f")
#             message_info = '''{"time_sent_kafka": "%s", 
#                              "underlying_symbol": "%s", 
#                              "quote_datetime": "%s", 
#                              "expiration": "%s", 
#                              "strike": "%s", 
#                              "option_type": "%s"}''' \
#                              % (time_stamp, 
#                                 test.loc[k, 'underlying_symbol'], 
#                                 test.loc[k, 'quote_datetime'], 
#                                 test.loc[k, 'expiration'],
#                                 test.loc[k,'strike'], 
#                                 test.loc[k, 'option_type'])

#             producer.send(topic, message_info.encode('utf-8'))
#             print message_info
#             time.sleep(2)
#         except:
#             print "running into an error in kafka producer, but it is OK..."
    
    
    # # block until all async messages are sent
    # producer.flush()
    
    # # configure multiple retries
    # producer = KafkaProducer(retries=5)

#     return


if __name__ == '__main__':
    main() 

