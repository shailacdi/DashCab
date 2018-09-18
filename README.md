# Problem

# Solution

# Get started

Notes :

Steps to setup Ecosystem and required python packages/libraries

    Setup Pegasus
    Setup EC2 clusters 1 Masters, 3 Workers in EC2
    Install Hadoop
    Install Spark
    Install Zookeeper
    Install Kafka
    Install python shapely
    Install Cassandra

Run scripts to download required datasets from internet and move it to s3

    Yellow taxi trip data
    Taxi Driver details
    NYC Borough coordinates (geojson)
    Bus trip data (to stream and simulate real time taxi trips - TBD)
    Weather data (TBD, requested and got hourly data)

Batch Processing steps

    Process Taxi trip data, run analytics and save the output to it to Cassandra
    Run analytics to allot medallions on a daily basis based on step 1 (TBD -- join Weather and calendar data and add more analytics)
    Simulate the real time trip request, process and save the streaming request (TBD -- Get hourly Real-time weather and join it with the above step 3)
    Plot 4 charts a. Statictics (medallion ranges required per borough in all timeslots - provide filters to select Borough, weather (TBD) ,calendar (TBD) b. Driver view (average # of trips per hour) c. Company view (real time display of number of medallions in all timeslots and staying under threshold capacity d. Real-time/current cab positions in all boroughs in all timeslots

