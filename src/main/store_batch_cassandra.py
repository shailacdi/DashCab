from cassandra.cluster import Cluster


#added comments

def initialize(cassandra_server_name,cassandra_keyspace_name):

    cluster = Cluster([cassandra_server_name, cassandra_server_name])
    session = cluster.connect()
    session.execute("use {}".format(cassandra_keyspace_name))
    session.execute("create table xyz (name string, age int")
