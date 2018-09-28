"""
This main program loads the taxi trip historical data, extracts relevant fields and computes
values required for generating metrics.

The input file consists of the following format
vendor_name,Trip_Pickup_DateTime,Trip_Dropoff_DateTime,Passenger_Count,Trip_Distance,
Start_Lon,Start_Lat,Rate_Code,store_and_forward,End_Lon,End_Lat,Payment_Type,
Fare_Amt,surcharge,mta_tax,Tip_Amt,Tolls_Amt,Total_Amt

Fields extracted - Trip_Pickup_DateTime,Start_Lon,Start_Lat (indexes 1, 5, 6)

Performs transformations and actions on Spark RDD to obtain the following fields

Trip_Pickup_DateTime(only date)
Start_Lon (pick up location, longitude)
Start_Lat (pick up location, latitude)
Time block (computed out of the time component in the above original field)
Number of trips per time block (unit time block is 15 mins. 24hours will have 96 time blocks)

"""
import pyspark
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
import sys
from cassandra.cluster import Cluster
import util

class TaxiBatch:
    """
    This class entirely processes the batch data with help of util functions.
    It reads the settings from application.properties file, process the records line
    by line with categorization by timeblock, day, month, and borough.

    It does a series of transformations and finally saves it into the database
    """

    def process_batch_data(self):
        """
        This function processes batch dataset. It loads the raw data from s3
        and does a series of transformations, computations by key
        """
        #load raw files using spark context
        self.data_stats = self.sc.textFile(self.s3_url)

        #the first row is the header
        header = self.data_stats.first()
        #filter the header, map and get total trips in one timeblock using
        #reducebykey time block, day, month, borough
        #groupByKey using the same fields above
        borough_info_bc = self.sc.broadcast(self.borough_info)
        self.data_stats = self.data_stats.filter(lambda row: row!=header) \
                     .map(lambda row : util.process_trip_record(row, borough_info_bc.value)) \
                     .filter(lambda row: row != None) \
                     .map(lambda row : ((row[1],row[2],row[3], row[4], row[5]),1)) \
                     .reduceByKey(lambda val1, val2 : val1+val2) \
                     .groupByKey() \
                     .map(lambda row : (row[0][0],row[0][1],row[0][2],row[0][3], row[0][4],util.get_statistics_stdev(list(row[1])),util.get_statistics_mean(list(row[1]))))

    def save_batch_trip_stats(self):
        """
        This function saves the batch processing results into the database
        """
        spark = SparkSession(self.sc)
        hasattr(self.data_stats, "toDF")
        self.data_stats.toDF(schema=["time_block","month","day","borough_code","borough_name","std_dev","mean"]).write.format("org.apache.spark.sql.cassandra").mode("append").options(table=self.cassandra_table, keyspace=self.cassandra_keyspace).save()
        print ("Saved data successfully")



    def __init__(self,env,config_file):
        """
        This initializes the class and loads the properties from the
        application.properties file. It initiates the SparkContext and
        loads the borough coordinates for nyc
        """
        #load all the properties
        self.properties = util.load_application_properties(env, config_file)
        self.cassandra_server = self.properties["cassandra.host.name"]
        self.cassandra_table = self.properties["cassandra.trip_stats_table"]
        self.cassandra_keyspace = self.properties["cassandra.trip.keyspace"]
        self.spark_master = self.properties["spark.master"]
        self.s3_url=self.properties["s3_url"]
        self.nyc_borough = self.properties["nyc_borough"]

        #initialize SparkConf and SparkContext along  with cassandra settings
        self.conf = SparkConf().setAppName("trip").set("spark.cassandra.connection.host",self.cassandra_server)
        self.sc = SparkContext(conf=self.conf)

        #load the nyc borough coordinates from geojson file
        self.borough_info = util.get_borough_data_dict(self.nyc_borough)


if __name__ == '__main__':
    """
    main method to process historical trip data. This instantiates the class
    TaxiBatch and process the batch data and saves it to the database
    """
    #check for proper arguments
    if len(sys.argv) != 3:
        sys.stderr.write("Please check the command line options and arguments")
        sys.exit(-1)

    #application.properties filename
    config_file= sys.argv[1]
    #section in the properties filename
    env = sys.argv[2]

    #instantiate the batch processing class
    taxi_batch = TaxiBatch(env, config_file)

    try:
        #initiate batch processing
        taxi_batch.process_batch_data()
        #saving results to the database
        taxi_batch.save_batch_trip_stats()
    except Exception as e:
        print "Error processing the trip batch data", e
        sys.exit(-1)

