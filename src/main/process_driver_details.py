from pyspark import SparkConf, SparkContext, SQLContext
from cassandra.cluster import Cluster

#need to pass as arguments. testing for now
conf = SparkConf().setAppName("appName").setMaster("local")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

r = sc.parallelize([(3,80),(4,78),(5,79)])

cluster = Cluster(['ec2-18-235-39-97.compute-1.amazonaws.com'])
session = cluster.connect()

for i in r.take(5):
        session.execute("insert into test.patient (id,heart_rate) values (i[0],i[1])")


#session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }")
#session.execute("CREATE TABLE IF NOT EXISTS test.patient(id int, heart_rate int, PRIMARY KEY(id))")

#session.execute("insert into test.patient (id,heart_rate) values (3,80)")
#session.execute("insert into test.patient (id,heart_rate)values (4,85)")
#session.execute("insert into test.patient (id,heart_rate)values (5,82)")

rows = session.execute("select * from test.patient")

for row in rows:
  print row
