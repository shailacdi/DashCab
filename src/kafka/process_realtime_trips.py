# simulate 2013-05-05 trips
import pyspark
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
import ConfigParser
import sys
import json
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from datetime import datetime
import calendar
from shapely.geometry import Point, shape

#read the application properties file
def load_application_properties(env, config_file):
    props = ConfigParser.RawConfigParser()
    props.read(config_file)
    properties = {}
    for option in props.options(env):
        properties[option]=props.get(env,option)
    return properties

def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

def get_borough_zone(a_long, a_lat):
    """
    This function looks up a give GPS co-ordinate in the NYC borough data and
    returns information such as borough id and borough name
    """
    #point = Point(-73.972736,40.762475)
    borough_list = borough_info.value
    print "QQQQQQQQQQQQQQQQQQQQQQQQQQ ", a_long, a_lat
    point = Point(a_long, a_lat)
    for key in borough_list:
        for polygon in borough_list[key][2]:
            if point.within(polygon):
                return (key,borough_list[key][0])
    return None


def get_borough_data_dict(borough_file):
    """
    This method loads the geojson file for NYC which contains the GPS coordinates for the
    five boroughs and the name of the borough. The structure is built as a dictionary as shown

    {1 : (b_zone_name, list(b_zone_coordinates)),
     2 : (b_zone_name, list(b_zone_coordinates)),
     3 : (b_zone_name, list(b_zone_coordinates)),
     4 : (b_zone_name, list(b_zone_coordinates)),
     5 : (b_zone_name, list(b_zone_coordinates))}

    """
    borough_dict = {}
    with open(borough_file) as f:
        gj = json.load(f)
        for feature in gj['features']:
            polygon = shape(feature['geometry'])
            borough_name = feature['properties']['borough']
            borough_zone = feature['properties']['boroughCode']
            borough_coord = feature['geometry']['coordinates']
            try:
                (zone_name, zone_coord, polygon_list) = borough_dict[borough_zone]
                zone_coord.append(borough_coord)
                polygon_list.append(polygon)
                borough_dict[borough_zone] = (zone_name, zone_coord, polygon_list)
            except KeyError:
                borough_dict[borough_zone] = (borough_name, list([borough_coord]), list([polygon]))
    return (borough_dict)

def trip_time_info(timestamp):
    """
    Need time blocks in one day to perform statistical calculations. Each block is
    of duration of 6 minutes
    input :
        timestamp -  contains time in the following format yyyy-mm-dd hh:mm:ss
    output:
        blocknumber of the 6-minute slot
    """
    date = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    time_block = (date.hour * 60 + date.minute) /30
    month = calendar.month_name[date.month]
    #month = date.strftime('%B')
    day = date.strftime('%A')
    return (time_block, month, day)

def check_medallion(hack_license):
    for i in driver_medallions_bc.value:
        if hack_license == i["driver_id"]:
            return i["medallion_id"]
        else:
            return "NA"

def process_trip_record(line):
    print "GGGGGGGGGGGGGGGGGGGGGG", line
    fields = line.rstrip().split(",")
    if(len(fields) < 7):
        return None

    t_timestamp = fields[5]
    if isfloat(fields[10]):
        t_long = float(fields[10])
    else:
        return None
    if isfloat(fields[11]):
        t_lat = float(fields[11])
    else:
        return None

    if (t_long ==0 or t_lat == 0):
        return None
    hack_license = fields[1]
    medallion = check_medallion(hack_license)
    t_date = t_timestamp.split(" ")[0]
    t_time = trip_time_info(t_timestamp)
    t_borough = get_borough_zone(t_long,t_lat)
    if (t_borough != None):
        return  (t_timestamp, hack_license, medallion, t_time[0], t_time[1], t_time[2], t_borough[0], t_borough[1], t_long,t_lat)
    else:
        return None



def process_stream(rdd):
    print "XXXXXXXXXXXXXXXXXXXxinside process stream"
    dataRaw = rdd.map(lambda x: x[1]) \
                .map(lambda row: process_trip_record(row)) \
                .filter(lambda row: row != None)

    print "zzzzzzzzzzzzz before create sparkcontext"
    spark = SparkSession(sc)
    hasattr(dataRaw, "toDF")
    for i in dataRaw.take(5):
        print i
    aa = dataRaw.toDF(schema=["assign_date","hack_license","medallion_id","time_block","month","day","borough_code","borough_name","long","lat"])
    print "zzzzzzzzzzzzz before writing to db"
    #dataRaw.toDF(schema=["assign_date","hack_license","medallion_id","time_block","month","day","borough_code","borough_name","long","lat"]).write.format("org.apache.spark.sql.cassandra").mode("append").options(table="real_trip", keyspace="trip_batch").save()
    print "zzzzzzzzzzzzz after db write"


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr("<Usage error> Please check the command line options \n")
        sys.exit(-1)

    env = sys.argv[1]
    properties_file = sys.argv[2]

    properties = load_application_properties(env, properties_file)
    interval = properties["interval"]
    topic=properties["topic"]
    partitions=properties["partitions"]
    broker_ips=properties["broker_ips"]
    cassandra_server=properties["cassandra.host.name"]
    cassandra_table = properties["cassandra.trip_stats_table"]
    driver_medallion_table=properties["driver_medallion"]
    cassandra_keyspace = properties["cassandra.trip.keyspace"]
    borough_file = properties["nyc_borough"]
    #spark_master = properties["spark.master"]
    print "XXXXXXXXXXXXXXXXXXXXXXXXX"
    print cassandra_server
    conf = SparkConf().setAppName("real_trips").set("spark.cassandra.connection.host", cassandra_server)
    #conf = SparkConf().setAppName("real_trips")
    sc = SparkContext(conf=conf)

    #load the batch stats for the day, month
    cluster = Cluster([cassandra_server, cassandra_server])
    session = cluster.connect()

    #process as command line argument - pass date in the trip simulator
    day = "Monday"
    month = "January"
    trip_date = "2018-09-23"

    query_str = "select time_block, borough_code, borough_name, mean from {0} where day='{1}' and month='{2}' allow filtering".format(cassandra_table,day,month)
    session.execute("use {0}".format(cassandra_keyspace))
    stats = session.execute(query_str)
    for i in stats:
        print i
    print "Obtaining stats data"


    #load driver_medallion assignment details
    query_str = "select driver_id, medallion_id from {0} where assign_date='{1}'".format(driver_medallion_table, trip_date)
    driver_medallions = session.execute(query_str)

    for i in driver_medallions:
        print i

    driver_medallions_bc = sc.broadcast(list(driver_medallions))

    #load borough co-ordinates
    borough_info = sc.broadcast(get_borough_data_dict(borough_file))

    ssc = StreamingContext(sc, int(interval))
    print ssc

    offset = 0
    print offset, partitions

    try:
        fromOffsets = {TopicAndPartition(topic, i): long(offset) for i in range(partitions)}
        print "inside try"
    except:
        fromOffsets = None
        print "exception"

    print "before create direct str"
    tripStream = KafkaUtils.createDirectStream(ssc, [topic],
                                                {"metadata.broker.list": broker_ips},
                                                fromOffsets=fromOffsets)
    print "befpre repartition"

    process_rdd = process_stream
    tripStream.foreachRDD(process_rdd)

    ssc.start()
    ssc.awaitTermination()


