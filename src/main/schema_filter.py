"""
Taxi data files have different schemas
1   Post 2016
    VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,
    RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,
    extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount
2.  Pre 2016
    vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,pickup_longitude,
    pickup_latitude,rate_code,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,
    fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount

It is expensive to check for this at a record level, hence this program was introduced to
optimize batch processing time by checking once per data file, using an indicator, process
the entire data file accordingly.
This is a program that reads data files one by one from s3, determines the schema,
and initiate batch processing for the data file with an appropriate indicator

If the data file has GPS coordinates for pickup and drop location, it sets flag to 'GPS'
If the data file has taxi zone values for pickup and drop location, it sets flag to 'TAXIZONE'
"""
import sys
import os
import botocore
import boto3
import lazyreader

def check_header(obj):
    """
    Reads the header from the file and checks for the pickup location in the header
    Input : filename from s3 being read
    Output : returns True if its taxi zone
             returns False if its GPS
             returns None if record is invalue
    """
    # Looping only once - to read the header and return
    for line in lazyreader.lazyread(obj.get()['Body'], delimiter='\n'):
        header = line.strip()
        print header
        if header:
            fields = header.split(",")
            if (len(fields) > 8):
                if fields[7] == 'PULocationID':
                    return False
                else:
                    return True
        return None


def get_s3files(s3_bucket):
    # iterate and process data file in s3, one at a time
    file_list = []
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)

    #for all data files check header
    for obj in bucket.objects.all():
        file_name = obj.key
        if not file_name.startswith('yellow'):
            continue
        uses_gps_coord = check_header(obj)
        msg = "Checking schema for file {0} : ".format(file_name)
        if uses_gps_coord is None:
            print msg + "Undefined"
            continue
        # GPS
        elif uses_gps_coord:
            print msg + "GPS"
            os.system('./run.sh GPS {0}'.format(file_name))
        # Taxi zone
        else:
            print msg + "TAXIZONE"
            os.system('./run.sh TAXIZONE {0}'.format(file_name))


if (__name__=='__main__'):

    if len(sys.argv) != 2:
        sys.stderr.write("Please check the command line options and arguments")
        sys.exit(-1)

    #s3 bucket name
    s3_bucket = sys.argv[1]

    try:
        get_s3files(s3_bucket)
        print "Processing of s3 trip data files complete"
    except Exception as e:
        print "Processing of s3 trip data files failed", e

