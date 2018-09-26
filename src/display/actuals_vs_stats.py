import dash
import dash.dependencies
import dash_core_components as dcc
import dash_html_components as html
import plotly.plotly as py
import plotly.graph_objs as go
import trip_stats
from datetime import datetime
import flask
import numpy as np
import calendar

cassandra_host_name="ec2-18-235-39-97.compute-1.amazonaws.com"
cassandra_trip_keyspace="trip_batch"
cassandra_trip_stats_table="real_trip_stats"
DAY = list(calendar.day_name)
Borough = ('Queens','Bronx','Brooklyn','Manhattan','Staten Island')

session = trip_stats.start_connection(cassandra_host_name, cassandra_trip_keyspace)
prep_trip_query = trip_stats.prepare_actual_stats_query(session)

DAY = list(calendar.day_name)
MONTH = list(calendar.month_name)
Borough = ('Queens','Bronx','Brooklyn','Manhattan','Staten Island')

POINTS_MIN = 10
POINTS_MAX = 100

server = flask.Flask(__name__)
print (server)
app = dash.Dash(__name__, server=server)

app.layout = html.Div(
    [
        html.Div([
        html.H2('Taxi Statistics - historical vs realtime',
                style={'float': 'left',
                       }),
        ]),
        html.Div(children=html.Div(id='graphs'), className='row'),
        dcc.Dropdown(id='t_borough',
                     options=[{'label': p, 'value': p} for p in Borough],
                     multi=False, value='Queens'
                     ),
        dcc.Dropdown(id='t_day',
                     options=[{'label': p, 'value': p} for p in DAY],
                     multi=False, value='Monday'
                     ),
        dcc.Interval(
            id='graph-update',
            interval=1*1000
        ),
        dcc.Graph(id='graph')
    ])


@app.callback(
    dash.dependencies.Output('graph', 'figure'),
    [dash.dependencies.Input('t_borough', 'value'),
    dash.dependencies.Input('t_day', 'value')],
    events=[dash.dependencies.Event('graph-update', 'interval')],
    )
#events=[dash.dependencies.Event('graph-update', 'interval')]

def update_graph(t_borough,t_day):
    t_day = 'Sunday'
    t_borough='Manhattan'
    t_date = "2013-05-05"
    print ("inside update graph")
    df1 = trip_stats.get_actual_stats_query(t_date, t_day, t_borough, prep_trip_query, session)

    return {
        'data': [{
            'x': df1['time_block'].values,
            'y': df1['actual_trips'].values
        },
        {
            'x': df1['time_block'].values,
            'y': df1['mean'].values
        }

        ]
    }
#    return{ go.Bar(
#        x= df1['time_block'].values,
#        y=df1['mean'].values
#        )}


if __name__ == '__main__':
    app.run_server(host="ec2-54-209-163-211.compute-1.amazonaws.com", port=80)