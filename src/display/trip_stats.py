"""
This program is a utility class that aids in generating reports.
Mainly creates prepared statements for report queries, executes and returns result sets
"""
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra import ReadTimeout
import pandas as pd
import datetime

def start_connection(host, keyspace):
    #creates a connection to cassandra database
    cluster = Cluster([host])
    session = cluster.connect(keyspace)
    session.row_factory = dict_factory
    return session

def prepare_stats_query(session):
    #prepared statement for getting metrics from the batch statistics
    query = "SELECT time_block,mean FROM trip_stats WHERE day=? and month=? and borough_name=? ALLOW FILTERING"
    return session.prepare(query)

def get_time(t_time):
    return ("%02d" % (t_time*10/60) + ":" + "%02d" % (t_time*10 % 60))

def get_stats_query(d_day,d_mon,d_borough, prepared_query, session):
    #executes and returns the dataset from the batch statistics dataset
    result_set = session.execute_async(prepared_query, [d_day,d_mon,d_borough])
    try:
        rows = result_set.result()
        df = pd.DataFrame(list(rows))
        if (len(df.index) == 0):
            return None
        df = df.sort_values(['time_block'])
        df['time_block'] = df['time_block'].apply(lambda x: get_time(x))
    except ReadTimeout:
        log.exception("Query timed out:")
    return df


def prepare_actual_stats_query(session):
    # prepared statement for getting metrics from real-time trips
    #ignoring date for demo purposes only
    #query = "SELECT time_block,mean,actual_trips FROM real_trip_stats WHERE assign_date=? and borough_name=? ALLOW FILTERING"
    query = "SELECT time_block,mean,actual_trips FROM real_trip_stats WHERE borough_name=? ALLOW FILTERING"
    return session.prepare(query)


def get_actual_stats_query(borough, prepared_query, session):
    """
    executes and returns the dataset from the real-time trip stats table for today
    processes the dataframe to aggregate the actual number of trips made in various time slots
    """
    #ignoring date for demo purposes only
    #d_date = datetime.datetime.now().strftime("%Y-%m-%d")
    #result_set = session.execute_async(prepared_query, [d_date,borough])
    result_set = session.execute_async(prepared_query, [borough])

    try:
        rows = result_set.result()
        df = pd.DataFrame(list(rows))
        if (len(df.index)==0):
            return None
        df = df.groupby(['time_block','mean']).agg({"actual_trips": "sum"}).reset_index()
        df = df.sort_values(['time_block'])
        df['time_block'] = df['time_block'].apply(lambda x: get_time(x))

    except ReadTimeout:
        log.exception("Query timed out:")
    return df
