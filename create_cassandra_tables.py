"""
This file creates the two tables in Cassandra used for storage after stream processing.

To be able to execute, install cassandra-driver: sudo pip install cassandra-driver
"""

from cassandra.cluster import Cluster

CASSANDRA_RESOURCE_LOCATION = 'resources/cassandra.config'
CASSANDRA_KEYSPACE = 'webtrafficanalytics'
CASSANDRA_TABLE_VISIT_RANK = 'visit_rank'
CASSANDRA_TABLE_METRICS = 'metrics'

# obtain cassandra hosts from config
with open(CASSANDRA_RESOURCE_LOCATION) as f:
	line1 = f.readline()
	cassandra_hosts = line1.strip().split('=')[1].split(',')

cluster = Cluster(config.CASSANDRA_SERVER)
session = cluster.connect(CASSANDRA_KEYSPACE)

session.execute("CREATE TABLE metrics (type text, event_time timestamp, value int, PRIMARY KEY (type, event_time))")
#session.execute("CREATE TABLE visit_rank (type text, event_time timestamp, rank int, ip text, visits int, PRIMARY KEY (type, event_time, rank))")