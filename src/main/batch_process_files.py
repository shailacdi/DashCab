"""
This main program loads the taxi trip historical data, extracts relevant fields and computes
values required for generating metrics.

The input file may have one of the following two schemas:
1   VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,
    RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,
    extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount
    Fields extracted mainly are - tpep_pickup_datetime,PULocationID (indexes 1, 7)
2.  vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,pickup_longitude,
    pickup_latitude,rate_code,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,
    fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount
    Fields extracted mainly are - pickup_datetime, pickup_longitude, pickup_latitude

Performs transformations and actions on Spark RDD to obtain the following fields
    assign_date(only date)
    borough name (lookup using pick up location)
    time block (computed out of the time component in the above original field)
    number of trips per time block (unit time block is 15 mins. 24hours will have 96 time blocks)

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

    def process_batch_data(self, file_name):
        """
        This function processes batch dataset. It loads the raw data from s3
        and does a series of transformations, computations by key
        """
        #load raw file using spark context
        self.data_stats = self.sc.textFile(self.s3_url + file_name)

        #map and get total trips in one timeblock using
        #reducebykey time block, day, month, borough
        zone_info_bc = self.sc.broadcast(self.zone_info)

        if self.location_type == 'TAXIZONE':
            process_function = util.process_trip_record
        else:
            process_function = util.process_trip_record_gps
        self.data_stats = self.data_stats.map(lambda row : process_function(row, zone_info_bc.value)) \
                     .filter(lambda row: row != None) \
                     .map(lambda row : ((row[0].split(" ")[0],row[1],row[2],row[3], row[4], row[5]),1)) \
                     .reduceByKey(lambda x,y : x+y) \
                     .map(lambda x : (x[0][0],x[0][1],x[0][2],x[0][3],x[0][4],x[0][5],x[1]))

    def save_batch_trip_stats(self):
        """
        This function saves the batch processing results into the database
        """
        spark = SparkSession(self.sc)
        hasattr(self.data_stats, "toDF")

        self.data_stats.toDF(schema=["assign_date","time_block","month","day","borough_code","borough_name","num_trips"]).write.format("org.apache.spark.sql.cassandra").mode("append").options(table=self.cassandra_table, keyspace=self.cassandra_keyspace).save()
        print ("Saved data successfully")

    def __init__(self,env,config_file, location_type):
        """
        This initializes the class and loads the properties from the
        application.properties file. It initiates the SparkContext and
        loads the borough coordinates or taxi zone coordinates for nyc
        based on the location indicator
        """
        #load all the properties
        self.properties = util.load_application_properties(env, config_file)
        self.cassandra_server = self.properties["cassandra.host.name"]
        self.cassandra_table = self.properties["cassandra.trip_data_table"]
        self.cassandra_keyspace = self.properties["cassandra.trip.keyspace"]
        self.spark_master = self.properties["spark.master"]
        self.s3_url=self.properties["batch_s3_url"]
        self.nyc_borough = self.properties["nyc_borough"]
        self.nyc_zones=self.properties["nyc_zones"]
        self.location_type = location_type
        #initialize SparkConf and SparkContext along  with cassandra settings
        self.conf = SparkConf().setAppName("trip").set("spark.cassandra.connection.host",self.cassandra_server)
        self.sc = SparkContext(conf=self.conf)

        #load nyc borough coordinates from geojson file if data file has GPS coordinates for pickup location
        #load nyc taxi zone mappings if data file has taxi zone for pickup location
        if (self.location_type == 'TAXIZONE'):
            self.zone_info = util.get_zone_dict(self.nyc_zones)
        else:
            self.zone_info = util.get_borough_data_dict(self.nyc_borough)


"""
The main method to process historical trip data. This instantiates the class
TaxiBatch and process the batch data and saves it to the database
"""
if __name__ == '__main__':

    #check for proper arguments
    if len(sys.argv) != 5:
        sys.stderr.write("Please check the command line options and arguments")
        sys.exit(-1)

    #application.properties filename
    config_file= sys.argv[1]
    #section in the properties filename
    env = sys.argv[2]

    #GPS or Taxizone indicator
    location_type = sys.argv[3]

    #data file name currently being processed
    file_name = sys.argv[4]

    #instantiate the batch processing class
    taxi_batch = TaxiBatch(env, config_file, location_type)

    try:
        #initiate batch processing
        taxi_batch.process_batch_data(file_name)
        #saving results to the database
        taxi_batch.save_batch_trip_stats()
    except Exception as e:
        print "Error processing the trip batch data", e
        sys.exit(-1)
