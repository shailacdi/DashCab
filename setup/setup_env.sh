#!/bin/sh

# transfer trip data files from ec2 to s3
# transfer borough coordinates from ec2 to s3
# install python shapely library to handle geojson file/GPS co-ordinates

#install spark and cassandra in the cluster

CASSANDRA_CLUSTER_NAME=s-cassandra
SPARK_CLUSTER_NAME=s-spark

source ~/.bash_profile

#hadoop and spark setup
peg up setup/spark/master.yml
peg up setup/spark/workers.yml
peg fetch ${SPARAK_CLUSTER_NAME}
peg install ${SPARK_CLUSTER_NAME} ssh
peg install ${SPARK_CLUSTER_NAME} aws
peg sshcmd-cluster ${SPARK_CLUSTER_NAME} "sudo apt-get install bc"
peg install ${SPARK_CLUSTER_NAME} hadoop
peg service ${SPARK_CLUSTER_NAME} hadoop start
peg install ${SPARK_CLUSTER_NAME} spark
peg service ${SPARK_CLUSTER_NAME} spark start
peg sshcmd-cluster s-spark "sudo pip install cassandra-driver"
peg sshcmd-cluster s-spark "sudo pip install statistics"


#cassandra setup
peg up setup/cassandra/cassandra-master.yml
peg up setup/cassandra/cassandra-workers.yml
peg fetch ${CASSANDRA_CLUSTER_NAME}
peg install ${CASSANDRA_CLUSTER_NAME} ssh
peg install ${CASSANDRA_CLUSTER_NAME} aws
peg install ${CASSANDRA_CLUSTER_NAME} cassandra
peg service ${CASSANDRA_CLUSTER_NAME} cassandra start


# on ec2 master
pip install cassandra-driver
pip install shapely
pip install ConfigParser
pip install sklearn
pip install statistics
pip install plotly
pip install dash
pip install dash_core_components
pip install dash_html_components

#In /usr/local/spark/conf/spark-env.sh
#export HADOOP_CONF_DIR=$DEFAULT_HADOOP_HOME/etc/hadoop

hadoop credential create fs.s3a.access.key -provider jceks://hdfs/user/root/awskeyfile.jceks xxxx
hadoop credential create fs.s3a.secret.key -provider jceks://hdfs/user/root/awskeyfile.jceks xxxx

aws s3api create-bucket --bucket scdi-data --region us-east-1

s3bucket=scdi-data

echo "transferring yellow taxi data files to s3"
aws s3 cp /home/ubuntu/taxi s3://$s3bucket/taxi/ --recursive
