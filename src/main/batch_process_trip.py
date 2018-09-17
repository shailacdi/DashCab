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

from pyspark import SparkConf, SparkContext, SQLContext
import util

#need to pass as arguments. testing for now
conf = SparkConf().setAppName("appName").setMaster("yarn")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#process borough list. Make this available as broadcast variable later on
borough_list = util.get_borough_data_dict("boroughboundaries.geojson")

#read the taxi data and process

trip_data = sqlContext.read.format("com.databricks.spark.csv") \
                       .option("header", "true") \
                       .load("s3n://scdidata/taxi/yellow_tripdata_2009-01.csv")

trip_data_filter=trip_data.select('Trip_Pickup_DateTime','Start_Lon','Start_Lat')

# Need to save this for one output graph
trip_data_rdd = trip_data_filter.rdd.map(tuple) \
                    .map(lambda x : ((x[0].split(" ")[0],util.trip_time_block(x[0]), util.get_borough_zone(float(x[1]),float(x[2]),borough_list)),
                                     float(x[1]),float(x[2])))

trip_data_rdd_stats = trip_data_rdd.map(lambda x : (x[0],1)) \
                    .reduceByKey(lambda x,y : x+y)

for i in trip_data_rdd_stats.take(50):
    print i