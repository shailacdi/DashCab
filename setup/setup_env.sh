#!/bin/sh

# transfer trip data files from ec2 to s3
# transfer borough coordinates from ec2 to s3
# install python shapely library to handle geojson file/GPS co-ordinates

hadoop credential create fs.s3a.access.key -provider jceks://hdfs/user/root/awskeyfile.jceks xxxx
hadoop credential create fs.s3a.secret.key -provider jceks://hdfs/user/root/awskeyfile.jceks xxxx

aws s3api create-bucket --bucket scdi-data --region us-east-1

s3bucket=scdi-data

echo "transferring yellow taxi data files to s3"
aws s3 cp /home/ubuntu/taxi s3://$s3bucket/taxi/ --recursive

# on ec2 master
pip install cassandra-driver
pip install shapely
pip install ConfigParser
pip install sklearn
pip install statistics

#In /usr/local/spark/conf/spark-env.sh
#export HADOOP_CONF_DIR=$DEFAULT_HADOOP_HOME/etc/hadoop
