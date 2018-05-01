"""
This file creates the two tables in Cassandra used for storage after stream processing.

To be able to execute, install cassandra-driver: sudo pip install cassandra-driver
"""

from cassandra.cluster import Cluster
import config


if __name__ == "__main__":
	cluster = Cluster(config.Config().cass_cluster_IP)
	
	session = cluster.connect()

	# create a keyspace (like database in RDB)
	#session.execute("CREATE KEYSPACE demo1 WITH replication={'class': 'SimpleStrategy', 'replication_factor': '1'}")

	# choose a keyspace
	session.execute("USE demo1")

	# create a table
	# session.execute("CREATE TABLE optionflowstreaming(time_sent_kafka text, underlying_symbol text, quote_datetime text, expiration text, strike text, option_type text, PRIMARY KEY(time_sent_kafka))")
	# session.execute("INSERT INTO optionflowstreaming (time_sent_kafka, underlying_symbol, quote_datetime, expiration, strike, option_type) VALUES (%s, %s, %s, %s, %s, %s)", ("2018-04-27 05:04:57 777777", "^SPX", "2017-01-03 14:01:47.825", "2017-01-03", "2050.0", "C"))


	# To retrieve what I have just inserted:
	rows = session.execute("SELECT * FROM optionflowstreaming")
	for row in rows:
		print row.__dict__


	# session.execute("CREATE TABLE metrics (type text, event_time timestamp, value int, PRIMARY KEY (type, event_time))")
	# session.execute("CREATE TABLE visit_rank (type text, event_time timestamp, rank int, ip text, visits int, PRIMARY KEY (type, event_time, rank))")

