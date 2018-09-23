from cassandra.cluster import Cluster

server_name = "ec2-18-235-39-97.compute-1.amazonaws.com"
keyspace_name = "trip_batch"
table_name = "trip_stats"

cluster = Cluster([server_name, server_name])
session = cluster.connect()
session.execute("create keyspace if not exists "+ keyspace_name+ " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }")
print("use "+keyspace_name)
session.execute("use {0}".format(keyspace_name))
session.execute("CREATE TABLE if not exists trip_stats (time_block int, month text,day text, borough_code int, borough_name text,std_dev int, mean int, PRIMARY KEY (borough_code,time_block, month, day))")

session.execute("CREATE TABLE if not exists medallion_master (medallion_id text, PRIMARY KEY (medallion_id))")

session.execute("CREATE TABLE if not exists driver_master (driver_id int, driver_rating int, preferred_borough int, active int, PRIMARY KEY (driver_id))")

session.execute("CREATE TABLE if not exists driver_assignment (assign_date text, driver_id int, medallion_id, PRIMARY KEY (assign_date, driver_id))")
