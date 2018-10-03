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

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

cassandra_host_name="ec2-18-235-39-97.compute-1.amazonaws.com"
#cassandra_host_name="10.0.0.13"
cassandra_trip_keyspace="trip_batch"

session = trip_stats.start_connection(cassandra_host_name,cassandra_trip_keyspace)
prep_trip_query1 = trip_stats.prepare_stats_query(session)
prep_trip_query2 = trip_stats.prepare_actual_stats_query(session)

DAY = list(calendar.day_name)
MONTH = list(calendar.month_name)
Borough = ('EWR','Queens','Bronx','Brooklyn','Manhattan','Staten Island')

server = flask.Flask(__name__)
print (server)
app = dash.Dash(__name__, external_stylesheets=external_stylesheets, server=server)

app.layout = html.Div([
    html.Div(
    [
        html.Div([
        html.H2('Taxi trips : Historical averages',
                style={'float': 'left',
                       }),
        ]),
        dcc.Dropdown(id='t_borough1',
                     options=[{'label': p, 'value': p} for p in Borough],
                     multi=False, value = 'Queens'
                     ),
        dcc.Dropdown(id='t_day1',
                     options=[{'label': p, 'value': p} for p in DAY],
                     multi=False, value = 'Monday'
                    ),
        dcc.Dropdown(id='t_month1',
                     options=[{'label': p, 'value': p} for p in MONTH],
                     multi=False, value = 'January'
                     ),
        dcc.Graph(id='graph1')
    ]),
    html.Div(
    [
        html.Div([
        html.H2("Taxi trips : Today",
                style={'float': 'left',
                       }),
        ]),
        html.Div(children=html.Div(id='graphs'), className='row'),
        dcc.Dropdown(id='t_borough2',
                     options=[{'label': p, 'value': p} for p in Borough],
                     multi=False, value='Queens'
                     ),
        dcc.Interval(
            id='graph-update',
            interval=1*1000
        ),
        dcc.Graph(id='graph2')
    ]),
])


@app.callback(
    dash.dependencies.Output('graph1', 'figure'),
    [dash.dependencies.Input('t_borough1', 'value'),
     dash.dependencies.Input('t_day1', 'value'),
     dash.dependencies.Input('t_month1', 'value')],
    )
def update_graph1(t_borough1,t_day1,t_month1):
    df1 = trip_stats.get_stats_query(t_day1, t_month1, t_borough1, prep_trip_query1, session)

    return {
        'data': [{
            'x': df1['time_block'].values,
            'y': df1['mean'].values,
            'type': 'bar'
        }]
    }

@app.callback(
    dash.dependencies.Output('graph2', 'figure'),
    [dash.dependencies.Input('t_borough2', 'value'),],
    events=[dash.dependencies.Event('graph-update', 'interval')],
    )
#events=[dash.dependencies.Event('graph-update', 'interval')]

def update_graph2(t_borough2):
    print ("inside update graph")
    df2 = trip_stats.get_actual_stats_query(t_borough2, prep_trip_query2, session)
    #print(df2.to_string())
    #df = df2.groupby(['time_block','mean']).agg({"actual_trips": "sum"}).reset_index()
    #print(df.to_string())
    return {
        'data': [{
            'x': df2['time_block'].values,
            'y': df2['actual_trips'].values
        },
        {
            'x': df2['time_block'].values,
            'y': df2['mean'].values
        }

        ]
    }

if __name__ == '__main__':
    app.run_server(host="ec2-100-24-0-72.compute-1.amazonaws.com", port=80)