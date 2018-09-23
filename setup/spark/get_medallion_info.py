"""
Need a list of drivers and medallions. So, getting this data from historical dataset
"""
import pyspark
from pyspark import SparkConf, SparkContext
import sys

"""
def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False
"""

def process_record(line):
    #print line
    print "line XXXXXXXXXXX ", line
    fields = line.rstrip().split(",")
    #if(len(fields) < 7):
    #    return None
    medallion_id = fields[0]
    #hacker_license = fields[1]
    #t_date = fields[5].split(" ")[0]
    #print (medallion_id, hacker_license,t_date)
    #if len(medallion_id)==0 or len(hacker_license)==0 or len(t_date ==0):
    #    return None
    #print(medallion_id, hacker_license,t_date)
    #return (medallion_id, hacker_license,t_date)
    return medallion_id

def process_batch_data(sc):
    #send borough info to all data nodes
    print "before reading csv"
    dataRaw = sc.textFile("s3n://scdi-data/medallion")
    print "after reading csv"

    header = dataRaw.first()
    data_stats = dataRaw.filter(lambda row: row!=header) \
                     .map(lambda row : process_record(row)) \
                     .filter(lambda row: row != None)
    data_stats= data_stats.map(lambda row : (row,1)) \
                     .reduceByKey(lambda x,y : x)
    #for i in data_stats.take(5):
    #    print i

    #data_stats= data_stats.reduceByKey(lambda val1, val2 : val1+val2)
    #data_stats.coalesce(1).saveAsTextFile("no_of_trips_daily")
    #print "saved no of trips"
    #data_stats = data_stats.map(lambda x : ((x[0][0], x[0][1]), 1)) \
    #                 .reduceByKey(lambda x,y : x)
    data_stats.coalesce(1).saveAsTextFile("list_of_medallions")
    print "saved list of medallions"
    return (data_stats)

    """
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

    def trip_time_info(timestamp):
    """
    """
    Need time blocks in one day to perform statistical calculations. Each block is
    of duration of 6 minutes
    input :
        timestamp -  contains time in the following format yyyy-mm-dd hh:mm:ss
    output:
        blocknumber of the 6-minute slot
    """
    """
    date = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    time_block = (date.hour * 60 + date.minute) /30
    month = calendar.month_name[date.month]
    day = date.strftime('%A')
    return (time_block, month, day)
    """

# main method to process historical trip data

if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.stderr.write("Please check the command line options and arguments")
        sys.exit(-1)
    trip_data_file= sys.argv[1]
#    env = sys.argv[2]
#    properties = load_application_properties(env, config_file)
#    cassandra_server = properties["cassandra.host.name"]
#    cassandra_table = properties["cassandra.trip_stats_table"]
#    cassandra_keyspace = properties["cassandra.trip.keyspace"]
#    spark_master = properties["spark.master"]
#    print cassandra_server
    conf = SparkConf().setAppName("medallion")
#.set("spark.cassandra.connection.host",cassandra_server)
    sc = SparkContext(conf=conf)
#    borough_info = get_borough_data_dict(properties["nyc_borough"])
    try:
        print "before batch processing"
        data_stats = process_batch_data(sc)
#        save_batch_trip_stats(sc, data_stats, cassandra_keyspace, cassandra_table)
    except Exception:
        print "Error processing the trip batch data"
        sys.exit(-1)

