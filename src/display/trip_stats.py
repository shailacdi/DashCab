from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra import ReadTimeout
import pandas as pd

def start_connection(host, keyspace):
    cluster = Cluster([host])
    session = cluster.connect(keyspace)
    session.row_factory = dict_factory
    return session

def prepare_stats_query(session):
    query = "SELECT time_block,mean FROM trip_stats WHERE day=? and month=? and borough_name=? ALLOW FILTERING"
    return session.prepare(query)

def get_stats_query(d_day,d_mon,d_borough, prepared_query, session):
    count = session.execute_async(prepared_query, [d_day,d_mon,d_borough])
    try:
        rows = count.result()
        df = pd.DataFrame(list(rows))
    except ReadTimeout:
        log.exception("Query timed out:")
    return df