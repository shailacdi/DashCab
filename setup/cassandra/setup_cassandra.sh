#!/bin/bash

#install cassandra in the cluster

PEGASUS_ROOT=/home/shaila/pegasus

CLUSTER_NAME=scdi-cassandra

peg up /home/shaila/dynamic_medallions/setup/cassandra/cassandra-master.yml

peg up /home/shaila/dynamic_medallions/setup/cassandra/cassandra-workers.yml

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} cassandra