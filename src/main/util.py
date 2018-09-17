"""
This package has all utility/validation functions
"""
from datetime import datetime
import json
from shapely.geometry import Point, shape
import ConfigParser as cp
import sys

def read_config_file():
    props = cp.RawConfigParser()
    props.read("setup/application.properties")
#    env = sys.argv[1]
    env = 'test'
    cassandra_server_name = props.get(env, 'CASSANDRA.host.name')
    cassandra_keyspace_name = props.get(env, 'CASSANDRA.keyspace.name')

# from geojson import Polygon

def trip_time_block(timestamp):
    """
    Need time blocks in one day to perform statistical calculations. Each block is
    of duration of 6 minutes
    input :
        timestamp -  contains time in the following format yyyy-mm-dd hh:mm:ss
    output:
        blocknumber of the 6-minute slot
    """
    date = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    return (date.hour * 60 + date.minute) / 6


#def get_borough_zone(a_long, a_lat, borough_dict):
def get_borough_zone(a_long, a_lat, borough_dict):
    """
    This function looks up a give GPS co-ordinate in the NYC borough data and
    returns information such as borough id and borough name
    """
    #point = Point(-73.972736,40.762475)
    point = Point(a_long, a_lat)
    for key in borough_dict:
        for polygon in borough_dict[key][2]:
            if point.within(polygon):
                return key


def get_borough_data_dict(borough_file):
    """
    This method loads the geojson file for NYC which contains the GPS coordinates for the
    five boroughs and the name of the borough. The structure is built as a dictionary as shown

    {1 : (b_zone_name, list(b_zone_coordinates)),
     2 : (b_zone_name, list(b_zone_coordinates)),
     3 : (b_zone_name, list(b_zone_coordinates)),
     4 : (b_zone_name, list(b_zone_coordinates)),
     5 : (b_zone_name, list(b_zone_coordinates))}

    """
    borough_dict = {}
    with open(borough_file) as f:
        gj = json.load(f)
        # print len(gj['features'])
        for feature in gj['features']:
            polygon = shape(feature['geometry'])
            borough_name = feature['properties']['borough']
            borough_zone = feature['properties']['boroughCode']
            borough_coord = feature['geometry']['coordinates']
            try:
                (zone_name, zone_coord, polygon_list) = borough_dict[borough_zone]
                zone_coord.append(borough_coord)
                polygon_list.append(polygon)
                borough_dict[borough_zone] = (zone_name, zone_coord, polygon_list)
            except KeyError:
                borough_dict[borough_zone] = (borough_name, list([borough_coord]), list([polygon]))
    return (borough_dict)
