############################################################
# This python script is a producer for kafka. It creates 
# random user data and send to kafka. The data is in JSON
# format. Here is the schema:
#
# {"userid": text, 
#  "time": timestamp, 
#  "acc": float}
# The "acc" column is the acceleration of the user.

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

# configuration file
# import config

# import a sample data (intra-day transactions for SPX) for demo
test = pandas.read_csv('./data/UnderlyingOptionsTradesCalcs_2017-01-03.csv', sep=',')
test_sub = test[['underlying_symbol', 'quote_datetime', 'expiration', 'strike', 'option_type']]
test_sub.strike.apply(str)

# test_sub = test[['underlying_symbol']]

# print test_sub.head()



# 2017-01-03 09:30:03.350


def main():
    # number of users in the system
    # nRecords = int(sys.argv[1])
    nRecords = 10

    producer = KafkaProducer(bootstrap_servers = ['localhost:9092'])

    topic = 'th-topic'

    for k in range(10):
        # print test_sub.loc[k,], "\n"
    
        # There could be more than 1 record per user per second, so microsecond is added to make each record unique.
        time_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %f")
        test_sub_dict = test_sub.loc[k,].to_dict()
        test_sub_dict['time_sent_kafka'] = time_stamp

        producer.send(topic, test_sub_dict.encode('utf-8'))
        print test_sub_dict

        # print ("streaming ", count, "_", userid_field)
    
    
    # block until all async messages are sent
    producer.flush()
    
    # configure multiple retries
    producer = KafkaProducer(retries=5)

    return


if __name__ == '__main__':
    main() 

