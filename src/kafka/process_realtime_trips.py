"""
This program receives data from the kafka stream, and processes the
DStreams.The data stream is real time taxi trip requests. The taxi trip request
has the following format

VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,
RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID

The records are processed using series of spark transformations, actions to compute the
number of trips per timeblock, joined with historical averages under similar conditions,
and saved into the database.

The following info is saved into the database table real_trips
"borough_code","time_block","month","day","assign_date","borough_name","actual_trips"

The following info is saved into the database table real_trips_stats
"assign_date,borough_code,time_block,month,day,actual_trips,borough_name,mean"

"""

import pyspark
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession, SQLContext
from cassandra.cluster import Cluster
from datetime import datetime
import calendar
import pandas as pd
import util
from pyspark.sql.functions import rand

def process_stream(rdd, real_trip_table,real_trip_stats_table,keyspace):
    """
    This function runs a series of transformations on the RDD, and accomplishes the following
    1. saves the real trip records to the database table real_trip
    2. calculates metrics for real time data for every time block
    3. joins the averages from the historical data with #2 above and saves it to the table real_trip_stats
    """
    dataRaw = rdd.map(lambda x: x[1]) \
                .map(lambda row: util.process_trip_record(row,zone_info.value)) \
                .filter(lambda row: row != None)

    # just flags and enables toDF()
    spark = SparkSession(sc)
    hasattr(dataRaw, "toDF")

    for i in dataRaw.take(10):
        print i

    #save the real trip data into the table real_trip
    dataRaw.toDF(schema=["assign_date","time_block","month","day","borough_code","borough_name"],sampleRatio=0.2).write.format("org.apache.spark.sql.cassandra").mode("append").options(table=real_trip_table, keyspace=keyspace).save()

    #get the historical stats broadcast variable
    stats = stats_for_day.value
    stats_df = SQLContext(sc).createDataFrame(stats)

    #(key, value) = ((borough_code,time_block,month,day),date,borough_name,1)
    dataRaw_df=dataRaw.map(lambda x : ((x[4], x[1], x[2], x[3]),(x[0].split(" ")[0], x[5],1 ))) \
                .reduceByKey(lambda x,y : (x[0],x[1],x[2]+y[2])) \
                .map(lambda x : (x[0][0],x[0][1],x[0][2],x[0][3],x[1][0],x[1][1],x[1][2])) \
                .toDF(schema=["borough_code","time_block","month","day","assign_date","borough_name","actual_trips"])
    #join the trip stats with the historical stats and store the consolidate values in table real_trip_stats
    #add a unique row differentiator since every rdd creates the output for which the key may be the same.
    dataRaw_df = dataRaw_df.join(stats_df,["borough_code","time_block"]) \
        .withColumn("time_stamp", rand())
    dataRaw_df.write.format("org.apache.spark.sql.cassandra").mode("append").options(table=real_trip_stats_table, keyspace=keyspace).save()

"""
    This is the main method to process real-time trip data. Loads the required properties from, 
    initializes the context, connect to the stream and process the streaming data 
"""


if __name__ == '__main__':
    if len(sys.argv) != 4:
        sys.stderr("<Usage error> Please check the command line options \n")
        sys.exit(-1)
    #section  in properties file, properties file name
    env = sys.argv[1]
    properties_file = sys.argv[2]

    #get date for which trips are simulated
    trip_date = datetime.strptime(sys.argv[3],'%Y-%m-%d')
    month = calendar.month_name[trip_date.month]
    day = trip_date.strftime('%A')

    #load properties file
    properties = util.load_application_properties(env, properties_file)
    interval = properties["interval"]
    topic=properties["topic"]
    broker_ips=properties["broker_ips"]
    cassandra_server=properties["cassandra.host.name"]
    trip_stats_table = properties["cassandra.trip_stats_table"]
    real_trip_table = properties["cassandra.real_trip_table"]
    real_trip_stats_table = properties["cassandra.real_trip_stats_table"]
    cassandra_keyspace = properties["cassandra.trip.keyspace"]
    #borough_file = properties["nyc_borough"]
    zone_file=properties["nyc_zones"]

    #set the spark context with database connection properties
    conf = SparkConf().setAppName("real_trips").set("spark.cassandra.connection.host", cassandra_server)
    sc = SparkContext(conf=conf)
    cluster = Cluster([cassandra_server, cassandra_server])
    session = cluster.connect()

    #load the historical statistics from database based on the day of the week and month
    query_str = "select time_block, borough_code, mean from {0} where day='{1}' and month='{2}' allow filtering".format(trip_stats_table,day,month)
    session.execute("use {0}".format(cassandra_keyspace))
    stats = session.execute(query_str)
    stats_df = pd.DataFrame(list(stats))
    #broadcast the historical stats
    stats_for_day = sc.broadcast(stats_df)

    #load borough co-ordinates
    zone_info = sc.broadcast(util.get_zone_dict(zone_file))

    #initialize streaming context and create a direct consumer
    ssc = StreamingContext(sc, int(interval))
    tripStream = KafkaUtils.createDirectStream(ssc, [topic],
                                                {"metadata.broker.list": broker_ips})
    #process for each DStream RDD
    tripStream.foreachRDD(lambda x : process_stream(x,real_trip_table,real_trip_stats_table,cassandra_keyspace))
    ssc.start()
    ssc.awaitTermination()
