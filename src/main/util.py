"""
This package has all utility/validation functions
"""
from datetime import datetime
import json
from shapely.geometry import Point, shape
import ConfigParser
import sys
import statistics
import calendar

def get_statistics_stdev(l_data):
    return round(statistics.stdev(l_data)) if len(l_data) > 1 else 0

def get_statistics_mean(l_data):
     return round(statistics.mean(l_data)) if len(l_data) > 1 else l_data[0]

def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

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

def process_trip_record(line, borough_info):
    """
    input : line - corresponds to one trip record
    borough_info - geojson co-ordinates for NYC
    output : fields in the format
    date,time block,month,day,borough,borough code,long,lat
    """
    fields = line.rstrip().split(",")
    #check for existence of atleast first 7 fields
    if(len(fields) < 7):
        return None
    #check if latitude is valid
    t_timestamp = fields[1]
    if isfloat(fields[5]):
        t_long = float(fields[5])
    else:
        return None
    # check if longitude is valid
    if isfloat(fields[6]):
        t_lat = float(fields[6])
    else:
        return None
    #check if latitude or longitude is 0
    if (t_long ==0 or t_lat == 0):
        return None
    t_date = t_timestamp.split(" ")[0]

    #using timestamp, get the corresponding time block
    t_time = trip_time_info(t_timestamp)

    #using lat and long, get the corresponding borough details
    t_borough = get_borough_zone(t_long,t_lat,borough_info)
    if (t_borough != None):
        return (t_date, t_time[0], t_time[1], t_time[2], t_borough[0], t_borough[1], t_long,t_lat)
    else:
        return None



def get_borough_zone(a_long, a_lat,borough_info):
    """
    This function looks up a give GPS co-ordinate in the NYC borough data and
    returns information such as borough id and borough name
    """
    #point = Point(-73.972736,40.762475)

    point = Point(a_long, a_lat)
    for key in borough_info:
        for polygon in borough_info[key][2]:
            if point.within(polygon):
                return (key,borough_info[key][0])
    return None



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

def trip_time_info(timestamp):
    """
    Need time blocks in one day to perform statistical calculations. Each block is
    of duration of 6 minutes
    input :
        timestamp -  contains time in the following format yyyy-mm-dd hh:mm:ss
    output:
        blocknumber of the 6-minute slot
    """
    date = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    time_block = (date.hour * 60 + date.minute) /30
    month = calendar.month_name[date.month]
    day = date.strftime('%A')
    return (time_block, month, day)


