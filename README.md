# Table of Contents
1. [Motivation](README.md#analysis)
2. [Solution](README.md#solution)
3. [Pipeline](README.md#pipeline)
4. [Setup](README.md#setup)
5. [Results](README.md#results)
6. [Assumptions and Dependencies](README.md#assumptions)

# Motivation

Taxi companies in NYC have plunged into an existential crisis due to Ride-Sharing services such as Uber and Lyft. In such a crisis,
how do Taxi companies optimize their resources, maximize their profit and stay competitive?

# Solution 
Companies look at the past data for any analysis, derive guidelines or find answers to their problems. 
DashCab is a real time dashboard for taxi companies. It uses historical trip data (atleast a few years) calculates averages,  processes the trip data in real time, and offers a set of graphs. This enables the companies to scale up or scale down their resources in real time.

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

<img> https://github.com/shailacdi/DashCab/blob/master/doc/pipeline.png </img>


# Setup
Please refer to the following files for setup
1. Spark, Web and Airflow - https://github.com/shailacdi/DashCab/blob/master/setup/setup_env.sh
2. Cassandra - https://github.com/shailacdi/DashCab/blob/master/setup/cassandra/create_cassandra_tables.py

# Results
Displayed below is the Dashboard snapshot. As you can see, there is one chart for historical averages and one daily, real time graph for each of the borough displaying actual against historical averages.
<img>https://github.com/shailacdi/DashCab/blob/master/doc/historical_avg.png</img>


# Assumptions and Dependencies
