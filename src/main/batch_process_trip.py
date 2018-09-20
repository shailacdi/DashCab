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
import statistics

#need to pass as arguments. testing for now
conf = SparkConf().setAppName("appName").setMaster("local")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

#process borough list. Make this available as broadcast variable later on
borough_list = util.get_borough_data_dict("/home/ubuntu/data/boroughboundaries.geojson")

#read the taxi data and process

trip_data = sc.textFile("s3n://scdi-data/yellow_tripdata_2015-01_part1.csv")

#trip_data = sc.textFile("s3n://scdi-data/2014part2.csv")

first = trip_data.first()


def get_trip_fields(line):
#	print line
	fields = line.split(",")
	t_dt = fields[1]
	t_long = fields[5]
	t_lat = fields[6]
	return(t_dt, float( t_long),float(t_lat))

trip_data_filter=trip_data.filter(lambda x : x!=first) \
			.map(lambda x : get_trip_fields(x))

for i in trip_data_filter.take(5):
	print i

# Need to save this for one output graph
trip_data_rdd = trip_data_filter.map(lambda x : ((x[0].split(" ")[0],util.trip_time_block(x[0]), \
			util.get_borough_zone(x[1],x[2],borough_list)),x[1],x[2])) 

for i in trip_data_rdd.take(10):
	print (i)

#print "before filter"

#ss = trip_data_rdd.filter(lambda x : (x[0][2] is not None))
#for i in ss.take(10):

#print "printing ", ss.count()
#print type(ss)

#print "after filter"

trip_data_rdd_stats = trip_data_rdd.map(lambda x : (x[0],1)) \
                    .reduceByKey(lambda x,y : x+y)

trip_avg = trip_data_rdd_stats.map(lambda x : ((x[0][1],x[0][2][0],x[0][2][1]),x[1])) \
		.groupByKey()
#trip_avg_filter = trip_avg.filter(lambda x : int(x[0][1])>0)

for i in trip_avg.take(10):
	print i

def get_statistics_stdev(l_data):
	return statistics.stdev(l_data) if len(l_data) > 1 else l_data[0]

def get_statistics_mean(l_data):
	 return statistics.mean(l_data) if len(l_data) > 1 else l_data[0]


trip_avg_stats = trip_avg.map(lambda x : (x[0][0],x[0][1],x[0][2],get_statistics_stdev(list(x[1])),round(get_statistics_mean(list(x[1])))))

print "time slot, borough, standard dev, mean"

for i in trip_avg_stats.collect():
	print i
