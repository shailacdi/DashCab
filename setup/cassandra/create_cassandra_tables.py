from cassandra.cluster import Cluster

server_name = "ec2-18-235-39-97.compute-1.amazonaws.com"
keyspace_name = "trip_batch"
table_name = "trip_stats"

cluster = Cluster([server_name, server_name])
session = cluster.connect()
session.execute("create keyspace if not exists "+ keyspace_name+ " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }")
print("use "+keyspace_name)
session.execute("use {0}".format(keyspace_name))

session.execute("CREATE TABLE if not exists trip_stats (time_block int, month text,day text, borough_code int, borough_name text, mean int, PRIMARY KEY (borough_code,time_block, month, day))")

session.execute("CREATE TABLE if not exists real_trip (assign_date text, time_block int, month text, day text, borough_code int, borough_name text,PRIMARY KEY (assign_date))")

session.execute("CREATE TABLE if not exists real_trip_stats (time_stamp text, assign_date text, time_block int, month text,day text, borough_code int, borough_name text,actual_trips int, mean int, PRIMARY KEY (time_stamp,assign_date,borough_code,time_block, month, day))")
