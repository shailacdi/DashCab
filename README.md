# Table of Contents
1. [Motivation](README.md#analysis)
2. [Solution](README.md#solution)
3. [Pipeline](README.md#pipeline)
4. [Results](README.md#results)
5. [Datasource](README.md#datasource)
6. [Setup](README.md#setup)
7. [Running programs](README.md#run)

# Motivation

Taxi companies in NYC have plunged into an existential crisis due to Ride-Sharing services such as Uber and Lyft. In such a crisis,
how do Taxi companies optimize their resources, maximize their profit and stay competitive?

# Solution 
Companies look at the past data for any analysis, derive guidelines or find answers to their problems. 
DashCab is a real time dashboard for taxi companies. It uses historical trip data (atleast a few years) calculates averages for analysis of any operational patterns. On a daily basis, as they fulfil real time trip request, the dashboard shows how they have been performing as against the historical averages. This enables the companies to scale up or scale down their resources in real time.   

In technical words, build a pipeline to 
1. process historical taxi trip data (at least a few years) as a Batch process
2. in real-time, display historical vs actual trend for daily trip fulfillment
3. categorize metrics by 
    -NYC boroughs
    -time of the day
    -day of the week 
    -time of year

# Pipeline

Displayed below is the pipeline built for DashCab. As shows, the file are downloaded from TLC into s3, processed by Spark and results stored in Cassandra. The entire flow is automated by Apache Airflow.
For streaming, Kafka has been used to stream in real time trips, Spark streaming processes it, and stores it in the database. Dashboard display is implemented by the Visualization tool, Dash

<img src=https://github.com/shailacdi/DashCab/blob/master/doc/pipeline.png>

# Results
Displayed below is the Dashboard snapshot. As you can see, there is one chart for historical averages and one daily, real time graph for each of the borough displaying actual against historical averages.
<img src=https://github.com/shailacdi/DashCab/blob/master/doc/historical_avg.png>
<img src=https://github.com/shailacdi/DashCab/blob/master/doc/newplot.png>


# DataSource
Historical Data Sources - NYC TLC Taxi Trip data
PS : 1 data file is ~ 1 months' trip data
prior to 06/2016 : Pickup and drop locations are GPS coordinates
post 06/2016 : Pickup and drop locations are Taxi zone codes

Streaming Datasource : 1 day's taxi trip data from 2017-07-06

# Setup
Please refer to the following files for setup
1. Spark, Web and Airflow - https://github.com/shailacdi/DashCab/blob/master/setup/setup_env.sh
2. Cassandra - https://github.com/shailacdi/DashCab/blob/master/setup/cassandra/create_cassandra_tables.py

CLUSTER STRUCTURE:
Setup AWS CLI and spin up cluster using https://github.com/InsightDataScience/pegasus
The following is the cluster used for deployment - 8 m4.large AWS EC2 instances:
- 4 nodes Spark, Kafka,Airflow Cluster
- 3 nodes Cassandra Cluster
- Dash Node

Finally, download the repository 
1. git clone github.com/shailacdi/DashCab in the Master Node of Spark Cluster
2. Copy DashCab/web folder into Dash Node  
3. Copy DashCab/airflow folder into Master node (Spark Cluster) airflow directory

Now, start the hadoop, spark, zookeeper, airflow services in Spark Cluster. Start Cassandra service in Cassandra cluster.
 
# Run program

BATCH PROCESSING:
1. check and set values in config/application.properties
2. airflow backfill dashcab -s <date>

Instead of step#2 above, follow the sequence of programs given below to get the same results
- run ./upload_dataset_to_s3.sh
- run ./start_batch_job.sh
- run ./process_stats.sh

The above will generate historical averages for the taxi trip dataset

REAL TRIP SIMULATION:
1. run trip_producer.sh
2. run real_trips.sh

REPORTS:
Open a terminal in the Dash Node
1. run ./runreport.sh
2. Go to http://dashcab.live 
The DashCab dashboard displays the historical averages and the real time trip processing trend charts.

Demo 
<iframe width="420" height="315"
src="https://github.com/shailacdi/DashCab/blob/master/doc/DashCab_Demo.mp4">
</iframe>
