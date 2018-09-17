#!/bin/sh

# transfer trip data files from ec2 to s3
# transfer borough coordinates from ec2 to s3
# install python shapely library to handle geojson file/GPS co-ordinates

s3bucket=scdidata

echo "transferring yellow taxi data files to s3"
aws s3 cp /home/ubuntu/taxi s3://$s3bucket/taxi/ --recursive

pip install shapely
pip install ConfigParser

#In /usr/local/spark/conf/spark-env.sh
#export HADOOP_CONF_DIR=$DEFAULT_HADOOP_HOME/etc/hadoop
