import pyspark
from pyspark import SparkConf, SparkContext, SQLContext
from cassandra.cluster import Cluster
import ConfigParser as cp
import sys
import datetime
import pyspark.sql.functions

server_name = "ec2-18-235-39-97.compute-1.amazonaws.com"
keyspace_name = "trip_batch"
stats_table = "trip_stats"
driver_table = "driver_details"
medallion_master = "medallion_master"
medallion_driver_table = "medallion_driver_assignment"

conf = SparkConf().setAppName("daily_allotment").set("spark.cassandra.connection.host",server_name)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

print type(sqlContext)

trip_stats = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="trip_stats", keyspace="trip_batch").load()
trip_stats = trip_stats.rdd.map(tuple).toDF(schema =["borough_code","time_block","month","day","borough_name","mean","std_dev"])

for i in trip_stats.take(5):
    print i

today=datetime.datetime.now()
day = today.strftime('%A')
#month = today.strftime('%B')
month = "January"
print day, month

trip_forecast = trip_stats.filter((trip_stats.day==day) & (trip_stats.month==month)) \
                            .select(trip_stats.borough_code, trip_stats.borough_name, trip_stats.mean, trip_stats.std_dev)
for i in trip_forecast.take(5):
    print i

medallion_limit = 12000

#get driver data
driver_data = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="driver_details", keyspace="trip_batch").load()
#driver_details_df = driver_data.rdd.map(tuple).toDF(schema =["driver_id","active","current_borough_id","driver_fname","driver_lname","driver_rating","start_borough_id"]) \
#                        .select('driver_id').orderBy(rand()).limit(medallion_limit)

driver_data = driver_data.select("driver_id").rdd.takeSample(False, medallion_limit)

#get medallions
medallion_data = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="medallion_master", keyspace="trip_batch").load()
medallion_data = medallion_data.select("medallion_id").collect()

assigned_dt = datetime.datetime.today().strftime('%Y-%m-%d')
medallion_driver_allotment = []

for medallion, driver in zip(medallion_data, driver_data):
    medallion_driver_allotment.append((assigned_dt, int(driver['driver_id']), medallion['medallion_id']))

for i in medallion_driver_allotment:
    print i

assignment_df = sqlContext.createDataFrame(medallion_driver_allotment, ["assign_date","driver_id","medallion_id"])
assignment_df.write.format("org.apache.spark.sql.cassandra").mode("append").options(table="medallion_driver_assignment", keyspace="trip_batch").save()

#cluster = Cluster([server_name, server_name])
#session = cluster.connect()
#session.execute("use {}".format(keyspace_name))
#results = session.execute("select * from {}".format(table_name))
#for i in results:
#    print (i)


