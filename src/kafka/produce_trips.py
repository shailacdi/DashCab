"""
This program simulates real trip requests. Reads the records from a
sample trip file stored in s3, performs minimal extraction of fields
and adds it to the kafka topic.
Note : since this is a trip dataset thats past, the date of the trip is made
current while retaining the time (H:M:S) of the original trip records.
"""
import sys
import ConfigParser
import time
import boto3
import lazyreader
from kafka.producer import KafkaProducer
import datetime

def produce_msgs(broker_ips, topic, bucket, data_file):
    """
    This method produces messages and adds it to the topic
    """
    # initialize the producer, s3 connection and get the file from s3 to simulate real trips
    producer = KafkaProducer(bootstrap_servers=broker_ips)
    i = 0
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=data_file)

    while True:

        for line in lazyreader.lazyread(obj['Body'], delimiter='\n'):
            message_info = line.strip()
            if not message_info:
                continue

            line_fields = message_info.split(",")
            #check if the field is a timestamp
            try:
                time.strptime(line_fields[1], '%Y-%m-%d %H:%M:%S')
            except ValueError:
                continue
            #manipulating to make the date current however retain the time of the past trip record
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            time_stamp = line_fields[1].split(" ")[1]
            line_fields[1] = today + " " + time_stamp
            #since trip is just initiated, feed only minimum and relevant info
            msg = ",".join(line_fields[:9])

            # add the record to the topic with a unique key
            if msg is not None:
                producer.send(topic, value=msg,key=str(i).encode())
                print i, msg
                i = i+1
            time.sleep(0.001)

def load_application_properties(env, config_file):
    """
    reads the application properties file using a parser and builds a
    dictionary with key,value pairs
    """
    props = ConfigParser.RawConfigParser()
    props.read(config_file)
    # builds a dictionary
    properties = {}
    for option in props.options(env):
        properties[option] = props.get(env, option)
    return properties

# main program
if __name__ == "__main__":

    if len(sys.argv) != 3:
        sys.stderr("<Usage error> Please check the command line options \n")
        sys.exit(-1)
    env=sys.argv[1]
    properties_file = sys.argv[2]
    properties = load_application_properties(env, properties_file)
    broker_ips = properties["broker_ips"]
    topic = properties["topic"]
    bucket = properties["kafka_s3_bucket"]
    data_file = properties["kafka_s3_key"]
    produce_msgs(broker_ips, topic, bucket, data_file)