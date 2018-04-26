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

# configuration file
# import config


def main():
    # number of users in the system
    nRecords = int(sys.argv[1])
    
    producer = KafkaProducer(bootstrap_servers = ['localhost:9092'])
    topic = 'th-topic'

    count = 0
    while count<=nRecords:
    
        for userid_field in range(nRecords):
            time= datetime.datetime.now() 
    
            # There could be more than 1 record per user per second, so microsecond is added to make each record unique.
            time_field = time.strftime("%Y-%m-%d %H:%M:%S %f")
    
            message_info = '{"userid": "%s", "time": "%s"}' \
                           % (userid_field, time_field)
        
            producer.send(topic, message_info.encode('utf-8'))
    		print ("streaming ", count, "_", userid_field)
            count += 1
    
    
    # block until all async messages are sent
    producer.flush()
    
    # configure multiple retries
    producer = KafkaProducer(retries=5)

    return


if __name__ == '__main__':
    main()

