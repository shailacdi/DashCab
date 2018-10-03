"""
This script downloads all the datasets from the web to ec2
"""
import os

baseUrl = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_"

#year = range(2009,2016)
#month = range (1,13)

year = range(2009,2010)
month = range (1,2)


print year
print month

taxiUrls = []

for yr in year:
    for mon in month:
        yearStr = str(yr)
        monStr = str(mon)
        print yearStr, monStr

        if len(monStr) == 1:
           monthStr = "0"+monStr
        url = baseUrl+yearStr+'-'+monthStr+".csv"
        taxiUrls.append(url)

for url in taxiUrls:
    os.system("wget " + url + " -P /home/ubuntu/taxi-data --trust-server-names")

os.system("echo 'transferring yellow taxi data files to s3'")
os.system("aws s3 cp /home/ubuntu/taxi-data s3://scdi-data/ --recursive")

os.system("echo 'removing downloaded files in ec2 instance'")
os.system("rm -R /home/ubuntu/taxi-data")

