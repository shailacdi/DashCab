# simulate 2013-05-05 trips
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
import ConfigParser
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

#read the application properties file
def load_application_properties(env, config_file):
    props = ConfigParser.RawConfigParser()
    props.read(config_file)
    properties = {}
    for option in props.options(env):
        properties[option]=props.get(env,option)
    return properties


if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.stderr("<Usage error> Please check the command line options \n")
        sys.exit(-1)

    properties_file = sys.argv[2]
    properties = load_application_properties(env, config_file)

    interval = properties["interval"]
    topic=properties["topic"]
    partitions=properties["partitions"]
    broker_ips=properties["broker_ips"]

    sc = SparkContext().setAppName("real_trips").getOrCreate()
    ssc = StreamingContext(sc, interval)

    offset = 0

    try:
        fromOffsets = {TopicAndPartition(topic, i): long(offset) for i in range(n)}
    except:
        fromOffsets = None

    tripStream = KafkaUtils.createDirectStream(ssc, [topic],
                                                {"metadata.broker.list": broker_ips},
                                                fromOffsets=fromOffsets)
    tripStream = tripStream.repartition(partitions) \
                    .map(lambda x: json.loads(x[1]))
    print tripStream


    ssc.start()
    ssc.awaitTermination()


