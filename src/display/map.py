import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import dash
import dash.dependencies
import plotly.plotly as py
import plotly.graph_objs as go
import flask
import numpy as np
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra import ReadTimeout

cassandra_host_name="ec2-18-235-39-97.compute-1.amazonaws.com"
cassandra_trip_keyspace="trip_batch"

def start_connection(host, keyspace):
    cluster = Cluster([host])
    session = cluster.connect(keyspace)
    session.row_factory = dict_factory
    return session

def query_cab_positions(session):
    try:
        dataRows = session.execute("SELECT lat,long FROM medallion_driver_assignment")
        lat = []
        long = []
        for row in dataRows:
            if ((row['long'] != None) and (row['lat'] != None)):
                lat.append(row['lat'])
                long.append(row['long'])
    except ReadTimeout:
        log.exception("Query timed out:")
    return (lat,long)



session = start_connection(cassandra_host_name, cassandra_trip_keyspace)
print ("before query")
(lat,long) = query_cab_positions(session)
#df.head()
print ("after query")
scl = [ [0,"rgb(5, 10, 172)"],[0.35,"rgb(40, 60, 190)"],[0.5,"rgb(70, 100, 245)"],\
    [0.6,"rgb(90, 120, 245)"],[0.7,"rgb(106, 137, 247)"],[1,"rgb(220, 220, 220)"] ]

server = flask.Flask(__name__)
print (server)
app = dash.Dash(__name__, server=server)


data = [ dict(
        type = 'scattergeo',
        locationmode = 'USA-states',
        lon = long,
        lat = lat,
        text = lat,
        mode = 'markers',
)]

layout = dict(
        title = 'Current Cab Positions',
        colorbar = True,
        geo = dict(
            scope='usa',
            projection=dict( type='albers usa' ),
            showland = True,
            landcolor = "rgb(250, 250, 250)",
            subunitcolor = "rgb(217, 217, 217)",
            countrycolor = "rgb(217, 217, 217)",
            countrywidth = 0.5,
            subunitwidth = 0.5
        ),
    )

fig = dict( data=data, layout=layout )

app.layout  = html.Div([
    dcc.Graph(id='graph', figure=fig)
])

if __name__ == '__main__':
    app.run_server(host="ec2-54-196-134-177.compute-1.amazonaws.com",port=80)