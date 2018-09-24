import sys
sys.path.append("./helpers/")

import ConfigParser
import time
import boto3
import lazyreader
import helpers
from kafka.producer import KafkaProducer

#read the application properties file
def load_application_properties(env, config_file):
    props = ConfigParser.RawConfigParser()
    props.read(config_file)
    properties = {}
    for option in props.options(env):
        properties[option]=props.get(env,option)
    return properties


def produce_msgs(broker_ips, topic, bucket, data_file):
    producer = KafkaProducer(bootstrap_servers=broker_ips)

    while True:
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket,
                            Key=data_file)

        for line in lazyreader.lazyread(obj['Body'], delimiter='\n'):
            message_info = line.strip()
            #extract only driver/medallion, pickup lat, pickup long
            #add borough info
            if line is not None:
                producer.send(topic, value=line,
                                   key=line)
            time.sleep(0.001)


# main program
if __name__ == "__main__":

    if len(sys.argv) != 2:
        sys.stderr("<Usage error> Please check the command line options \n")
        sys.exit(-1)

    properties_file = sys.argv[2]
    properties = load_application_properties(env, config_file)
    broker_ips = properties["broker_ips"]
    topic = properties["topic"]
    bucket = properties["s3_bucket"]
    data_file = properties["s3_file"]
    produce_msgs(broker_ips, topic, bucket, data_file)