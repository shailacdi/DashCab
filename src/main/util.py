"""
This package has all utility/validation functions
"""
from datetime import datetime
import json
from shapely.geometry import Point, shape
import ConfigParser
import sys

#read the application properties file
def load_application_properties(env, config_file):
    props = ConfigParser.RawConfigParser()
    props.read(config_file)
    properties = {}
    for option in props.options(env):
        properties[option]=props.get(env,option)
        print option, properties[option]
    return properties

def get_borough_zone(a_long, a_lat):
    """
    This function looks up a give GPS co-ordinate in the NYC borough data and
    returns information such as borough id and borough name
    """
    #point = Point(-73.972736,40.762475)
    print "getting borough zone", a_long, a_lat
    point = Point(a_long, a_lat)
#    print point
    for key in borough_info:
        for polygon in borough_info[key][2]:
            if point.within(polygon):
                return (key,borough_info[key][0])


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
    print "loaded borough_dict"
    return (borough_dict)

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
    return (date.hour * 60 + date.minute) /15 


