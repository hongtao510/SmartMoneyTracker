import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import config
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement


reload(sys)
sys.setdefaultencoding('utf-8')


def push_to_cassandra(raw_data):
    print "========"
    cassandra_cluster = Cluster(config.Config().cass_cluster_IP)
    cassandra_session = cassandra_cluster.connect('demo1')
    insert_option_flow = cassandra_session.prepare("INSERT INTO optionflowstreaming (time_sent_kafka, underlying_symbol, quote_datetime, expiration, strike, option_type) VALUES (?, ?, ?, ?, ?, ?)")
    batch = BatchStatement(consistency_level=ConsistencyLevel.ANY)

    for d in raw_data:
        print "d====", type(d)
        batch.add(insert_option_flow, (d['time_sent_kafka'], d['underlying_symbol'], d['quote_datetime'], d['expiration'], d['strike'], d['option_type']))

    cassandra_session.execute(batch)
    cassandra_cluster.shutdown()
        # cassandra_session.execute("INSERT INTO optionflowstreaming (time_sent_kafka, underlying_symbol, quote_datetime, expiration, strike, option_type) VALUES (%s, %s, %s, %s, %s, %s)", (d['time_sent_kafka'], d['underlying_symbol'], d['quote_datetime'], d['expiration'], d['strike'], d['option_type']))
    print "************SAVED*********"

if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingDirectKafka")
    ssc = StreamingContext(sc, config.Config().ss_interval)

    kvs = KafkaUtils.createDirectStream(ssc, [config.Config().kafka_topic],
                                        {"metadata.broker.list": config.Config().bootstrap_servers})
    parsed_msg = kvs.map(lambda (key, value): json.loads(value))
    parsed_msg.pprint()
    parsed_msg.foreachRDD(lambda rdd: rdd.foreachPartition(push_to_cassandra))

    ssc.start()
    ssc.awaitTermination()

