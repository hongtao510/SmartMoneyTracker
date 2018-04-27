############################################################
# This python script is a producer for kafka. It creates 
# random user data and send to kafka. The data is in JSON
# format. Here is the schema:
#
# {"userid": text, 
#  "time": timestamp, 
#  "acc": float}
# To send it to kafka, each record is first converted to 
# string then to bytes using str.encode('utf-8') method.
#
# The parameters
# config.KAFKA_SERVERS: public DNS and port of the servers
# config.ANOMALY_PERIOD: How often to create an outlier  
# config.ANOMALY_VALUE: The value to add to create an outlier
# were written in a separate "config.py".
############################################################


import random
import sys
import datetime
from kafka import KafkaProducer
import time
import pandas
import config


# import a sample data (intra-day transactions for SPX) for demo
test = pandas.read_csv('./data/UnderlyingOptionsTradesCalcs_2017-01-03.csv', sep=',')
# test_sub = test[['underlying_symbol', 'quote_datetime', 'expiration', 'strike', 'option_type']]
# print test_sub.head()


def main():
    # number of records
    nRecords = test.shape[0]
    while True:
        try:
            producer = KafkaProducer(bootstrap_servers = [config.Config().bootstrap_servers])
            topic = config.Config().kafka_topic
            k = random.randint(0,nRecords-1)
            print "streaming record ", k
            # print test_sub.loc[k,], "\n"
            # There could be more than 1 record per user per second, so microsecond is added to make each record unique.
            time_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %f")
            message_info = '''{"time_sent_kafka": "%s", 
                             "underlying_symbol": "%s", 
                             "quote_datetime": "%s", 
                             "expiration": "%s", 
                             "strike": "%s", 
                             "option_type": "%s"}''' \
                             % (time_stamp, 
                                test.loc[k, 'underlying_symbol'], 
                                test.loc[k, 'quote_datetime'], 
                                test.loc[k, 'expiration'],
                                test.loc[k,'strike'], 
                                test.loc[k, 'option_type'])

            producer.send(topic, message_info.encode('utf-8'))
            print message_info
            time.sleep(2)
        except:
            print "running into an error in kafka producer, but it is OK..."
    
    
    # block until all async messages are sent
    producer.flush()
    
    # configure multiple retries
    producer = KafkaProducer(retries=5)

    return


if __name__ == '__main__':
    main() 

