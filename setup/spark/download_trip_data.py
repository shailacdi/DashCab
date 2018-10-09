"""
This script downloads all the datasets from the web to ec2
and uploads the same to s3 bucket
"""
import os

baseUrl = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_"

year = range(2010,2018)
month = range (1,13)

#in a loop, download data from TLC website into ec2, upload to s3 and
#clear the downloaded file in ec2
try:
    for yr in year:
        for mon in month:
            yearStr = str(yr)
            monStr = str(mon)

            if len(monStr) == 1:
                monthStr = "0"+monStr
            url = baseUrl+yearStr+'-'+monthStr+".csv"
            os.system("wget " + url + " -P /home/ubuntu/taxi-data --trust-server-names")
            os.system("echo 'transferring yellow taxi data files to s3'")
            os.system("aws s3 cp /home/ubuntu/taxi-data s3://scdi-data/ --recursive")

            os.system("echo 'removing downloaded files in ec2 instance'")
            os.system("rm -R /home/ubuntu/taxi-data")
    print "Uploading data files to s3 completed!"
except Exception as e:
    print "Failed to upload data files to s3", e

