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
    os.system("wget " + url + " -P /home/ubuntu/taxi --trust-server-names")


# get the borough polygon boundary coordinates
os.system("wget http://data.beta.nyc//dataset/68c0332f-c3bb-4a78-a0c1-32af515892d6/resource/7c164faa-4458-4ff2-9ef0-09db00b509ef/download/42c737fd496f4d6683bba25fb0e86e1dnycboroughboundaries.geojson -P /home/ubuntu/taxi --trust-server-names")