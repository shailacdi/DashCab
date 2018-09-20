#!/bin/bash

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


#cassandra setup
peg up setup/cassandra/cassandra-master.yml
peg up setup/cassandra/cassandra-workers.yml
peg fetch ${CASSANDRA_CLUSTER_NAME}
peg install ${CASSANDRA_CLUSTER_NAME} ssh
peg install ${CASSANDRA_CLUSTER_NAME} aws
peg install ${CASSANDRA_CLUSTER_NAME} cassandra
peg service ${CASSANDRA_CLUSTER_NAME} cassandra start

