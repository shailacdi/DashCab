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
prep_trip_query1 = trip_stats.prepare_stats_query(session)
prep_trip_query2 = trip_stats.prepare_actual_stats_query(session)

DAY = list(calendar.day_name)
MONTH = list(calendar.month_name)
Borough = ('Queens','Bronx','Brooklyn','Manhattan','Staten Island')

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
server = flask.Flask(__name__)
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.H1(children='DashCab Monitor'),
    html.Div(children=html.Div(id='graphs'), className='row'),
    dcc.Dropdown(id='t_borough',
                 options=[{'label': p, 'value': p} for p in Borough],
                 multi=False, value='Queens'
                 ),
    dcc.Dropdown(id='t_day',
                 options=[{'label': p, 'value': p} for p in DAY],
                 multi=False, value='Monday'
                 ),
    dcc.Dropdown(id='t_month',
                 options=[{'label': p, 'value': p} for p in MONTH],
                 multi=False, value='January'
                 ),
    dcc.Graph(id='historical'),

    dcc.Input(id='EWR', value='EWR', type='text', readonly=True),
    dcc.Graph(id='EWR_graph'),

    dcc.Input(id='Manhattan', value='Manhattan', type='text', readonly=True),
    dcc.Graph(id='Manhattan_graph'),

    dcc.Input(id='Bronx', value='Bronx', type='text', readonly=True),
    dcc.Graph(id='Bronx_graph'),

    dcc.Input(id='Queens', value='Queens', type='text', readonly=True),
    dcc.Graph(id='Queens_graph'),

    dcc.Input(id='Staten_Island', value='Staten Island', type='text', readonly=True),
    dcc.Graph(id='Staten_Island_graph'),

    dcc.Input(id='Brooklyn', value='Brooklyn', type='text', readonly=True),
    dcc.Graph(id='Brooklyn_graph'),

    dcc.Interval(
        id='graph-update',
        interval=1 * 1000
    ),
])

@app.callback(
    dash.dependencies.Output('historical', 'figure'),
    [dash.dependencies.Input('t_borough', 'value'),
    dash.dependencies.Input('t_day', 'value'),
    dash.dependencies.Input('t_month', 'value')],
    )

def update_graph1(t_borough,t_day,t_month):
    df1 = trip_stats.get_stats_query(t_day, t_month, t_borough, prep_trip_query1, session)

    if (df1 is None):
        return

    return {
        'data': [{
            'x': df1['time_block'].values,
            'y': df1['mean'].values,
            'type': 'bar'
        }],
        'layout': {
            'title': 'Historical Trip Averages by day, month, borough',
            'xaxis': {
                'title': 'Time'
            },
            'yaxis': {
                'title': 'Number of Trips'
            }
        }
    }

@app.callback(
    dash.dependencies.Output('EWR_graph', 'figure'),
    [dash.dependencies.Input('EWR', 'value'),],
    events=[dash.dependencies.Event('graph-update', 'interval')],
    )

def update_EWR(EWR):
    return (update_graph(EWR))


@app.callback(
    dash.dependencies.Output('Manhattan_graph', 'figure'),
    [dash.dependencies.Input('Manhattan', 'value'),],
    events=[dash.dependencies.Event('graph-update', 'interval')],
    )

def update_Manhattan(Manhattan):
    return (update_graph(Manhattan))

@app.callback(
    dash.dependencies.Output('Bronx_graph', 'figure'),
    [dash.dependencies.Input('Bronx', 'value'),],
    events=[dash.dependencies.Event('graph-update', 'interval')],
    )

def update_Bronx(Bronx):
    return (update_graph(Bronx))


@app.callback(
    dash.dependencies.Output('Staten_Island_graph', 'figure'),
    [dash.dependencies.Input('Staten_Island', 'value'),],
    events=[dash.dependencies.Event('graph-update', 'interval')],
    )

def update_Staten_Island(Staten_Island):
    return(update_graph(Staten_Island))



def update_graph(t_borough):
    df1 = trip_stats.get_actual_stats_query(t_borough, prep_trip_query2, session)

    if (df1 is None):
        return

    return {
        'data': [{
            'x': df1['time_block'].values,
            'y': df1['actual_trips'].values
        },
        {
            'x': df1['time_block'].values,
            'y': df1['mean'].values
        }

        ],
        'layout': {
            'title': "Today's trips in {0} vs historical averages".format(t_borough),
            'xaxis': {
                'title': 'Time'
            },
            'yaxis': {
                'title': 'Number of Trips'
            }
        }
    }


if __name__ == '__main__':
    app.run_server(host="ec2-100-24-0-72.compute-1.amazonaws.com", port=80)