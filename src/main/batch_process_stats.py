"""
This main program loads the processed historical taxi trip data, and computes metrics.

The input file consists of the following format
assign_date, time_block, month, day, borough_name, num_trips

Performs aggregation to obtain the following fields
time_block,day,month,borough_name,average #of trips
"""
import pyspark
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession, functions as func
import sys
from cassandra.cluster import Cluster
import util

class TaxiStats:
    """
    This class loads the processed historical trip data with help of util functions and saves
    the calculated  metrics
    """

    def calculate_metrics(self):
        """
        This function loads the processed historical trip records from database table 'trip_data'
        and aggregates by the following key to compute the average number of trips
        (time_block, day, month, borough)
        """
        self.data_stats = self.sqlContext.read.format("org.apache.spark.sql.cassandra").options(table=self.cassandra_trip_table, keyspace=self.cassandra_keyspace).load()
        self.data_stats = self.data_stats.groupBy(['time_block','day','month','borough_name']).agg(func.avg('num_trips').alias('mean'))


    def save_metrics(self):
        """
        This function saves the batch processing results into the database table 'trip_stats'
        """
        self.data_stats.write.format("org.apache.spark.sql.cassandra").mode("append").options(table=self.cassandra_stats_table, keyspace=self.cassandra_keyspace).save()
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
        self.cassandra_trip_table = self.properties["cassandra.trip_data_table"]
        self.cassandra_stats_table = self.properties["cassandra.trip_stats_table"]
        self.cassandra_keyspace = self.properties["cassandra.trip.keyspace"]
        self.spark_master = self.properties["spark.master"]
        self.s3_url=self.properties["batch_s3_url"]

        #initialize SparkConf and SparkContext along  with cassandra settings
        self.conf = SparkConf().setAppName("trip").set("spark.cassandra.connection.host",self.cassandra_server)
        self.sc = SparkContext(conf=self.conf)
        self.sqlContext = SQLContext(self.sc)


"""
The main method to process historical trip data. This instantiates the class
TaxiBatch and process the batch data and saves it to the database
"""
if __name__ == '__main__':

    #check for proper arguments
    if len(sys.argv) != 3:
        sys.stderr.write("Please check the command line options and arguments")
        sys.exit(-1)

    #application.properties filename
    config_file= sys.argv[1]
    #section in the properties filename
    env = sys.argv[2]

    #instantiate the batch stats computing class
    taxi_stats = TaxiStats(env, config_file)

    try:
        #calculate metrics and save metrics
        taxi_stats.calculate_metrics()
        taxi_stats.save_metrics()
    except Exception as e:
        print "Error processing the trip batch data", e
        sys.exit(-1)

