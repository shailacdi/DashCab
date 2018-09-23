"""
This program loads the taxi trip historical data, extracts relevant fields and computes
values required for analysis.

The input file consists of the following format
vendor_name,Trip_Pickup_DateTime,Trip_Dropoff_DateTime,Passenger_Count,Trip_Distance,
Start_Lon,Start_Lat,Rate_Code,store_and_forward,End_Lon,End_Lat,Payment_Type,
Fare_Amt,surcharge,mta_tax,Tip_Amt,Tolls_Amt,Total_Amt

field indexes used - 1, 5, 6

Performs transformations and actions on Spark RDD to obtain the following fields

Trip_Pickup_DateTime(only date)
Start_Lon (pick up location, longitude)
Start_Lat (pick up location, latitude)
time block (computed out of the time component in the above original field)
number of trips per time block (unit time block is 6mins. 24hours will have 240 time blocks)

"""
import pyspark
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
import statistics
import sys
from datetime import datetime
from shapely.geometry import Point, shape
import ConfigParser
import json
import calendar
from cassandra.cluster import Cluster

def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

def process_trip_record(line, borough_info):
    #print line
    fields = line.rstrip().split(",")
    if(len(fields) < 7):
        return None

    t_timestamp = fields[1]
    if isfloat(fields[5]):
        t_long = float(fields[5])
    else:
        return None
    if isfloat(fields[6]):
        t_lat = float(fields[6])
    else:
        return None

    if (t_long ==0 or t_lat == 0):
        return None
    t_date = t_timestamp.split(" ")[0]
    t_time = trip_time_info(t_timestamp)
    t_borough = get_borough_zone(t_long,t_lat,borough_info)
    if (t_borough != None):
        return (t_date, t_time[0], t_time[1], t_time[2], t_borough[0], t_borough[1], t_long,t_lat)
    else:
        return None

def get_statistics_stdev(l_data):
    return round(statistics.stdev(l_data)) if len(l_data) > 1 else 0

def get_statistics_mean(l_data):
     return round(statistics.mean(l_data)) if len(l_data) > 1 else l_data[0]

def process_batch_data(sc, borough_info):
    #send borough info to all data nodes
    dataRaw = sc.textFile(properties["s3_url"])
    header = dataRaw.first()
    data_stats = dataRaw.filter(lambda row: row!=header) \
                     .map(lambda row : process_trip_record(row, borough_info)) \
                     .filter(lambda row: row != None) \
                     .map(lambda row : ((row[1],row[2],row[3], row[4], row[5]),1)) \
                     .reduceByKey(lambda val1, val2 : val1+val2) \
                     .groupByKey() \
                     .map(lambda row : (row[0][0],row[0][1],row[0][2],row[0][3], row[0][4],get_statistics_stdev(list(row[1])),get_statistics_mean(list(row[1]))))

    #                     .map(lambda x : str(x[0][0])+","+str(x[0][1])+","+str(x[0][2])+","+str(x[0][3])+","+str(x[0][4])+","+str(x[1]))
    #    data_stats.coalesce(1).saveAsTextFile("data.csv")
    return (data_stats)


def save_batch_trip_stats(sc, data_stats, trip_keyspace, trip_stats_table):
    spark = SparkSession(sc)
    hasattr(data_stats, "toDF")
    print trip_stats_table, trip_keyspace
    data_stats.toDF(schema=["time_block","month","day","borough_code","borough_name","std_dev","mean"]).write.format("org.apache.spark.sql.cassandra").mode("append").options(table=trip_stats_table, keyspace=trip_keyspace).save()
    print ("Saved data successfully")

#read the application properties file
def load_application_properties(env, config_file):
    props = ConfigParser.RawConfigParser()
    props.read(config_file)
    properties = {}
    for option in props.options(env):
        properties[option]=props.get(env,option)
    return properties

def get_borough_zone(a_long, a_lat, borough_info):
    """
    This function looks up a give GPS co-ordinate in the NYC borough data and
    returns information such as borough id and borough name
    """
    #point = Point(-73.972736,40.762475)

    point = Point(a_long, a_lat)
    for key in borough_info:
        for polygon in borough_info[key][2]:
            if point.within(polygon):
                return (key,borough_info[key][0])
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
    day = date.strftime('%A')
    return (time_block, month, day)

# main method to process historical trip data

if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write("Please check the command line options and arguments")
        sys.exit(-1)
    print sys.argv
    config_file= sys.argv[1]
    env = sys.argv[2]
    properties = load_application_properties(env, config_file)
    cassandra_server = properties["cassandra.host.name"]
    cassandra_table = properties["cassandra.trip_stats_table"]
    cassandra_keyspace = properties["cassandra.trip.keyspace"]
    spark_master = properties["spark.master"]
    print cassandra_server
    conf = SparkConf().setAppName("trip").set("spark.cassandra.connection.host",cassandra_server)
    sc = SparkContext(conf=conf)
    borough_info = get_borough_data_dict(properties["nyc_borough"])
    try:
        data_stats = process_batch_data(sc,borough_info)
        save_batch_trip_stats(sc, data_stats, cassandra_keyspace, cassandra_table)
    except Exception:
        print "Error processing the trip batch data"
        sys.exit(-1)

