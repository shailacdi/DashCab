#!/bin/bash
CONFIG_FILE=$PWD/config/kafka.properties
#PFILES=$PWD/src/main/util.py
#FILES=$PWD/data/boroughboundaries.geojson
CASSANDRA=com.datastax.spark:spark-cassandra-connector_2.11:2.3.2
STREAMING=org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0
SPARK_BATCH_CLUSTER=ec2-35-171-12-195.compute-1.amazonaws.com
spark-submit --master spark://$SPARK_BATCH_CLUSTER:7077 \
	         --packages $STREAMING,$CASSANDRA \
	         --driver-memory 4G \
             --executor-memory 4G \
             src/kafka/process_realtime_trips.py \
             kafka $CONFIG_FILE

