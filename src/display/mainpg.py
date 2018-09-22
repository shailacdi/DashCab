import dash
import dash.dependencies
import dash_core_components as dcc
import dash_html_components as html
import plotly
import random
import plotly.graph_objs as go
from collections import deque
import trip_stats
from datetime import datetime
import pandas as pd
import flask
import numpy as np
import calendar

cassandra_host_name="ec2-18-235-39-97.compute-1.amazonaws.com"
cassandra_trip_keyspace="trip_batch"
cassandra_trip_stats_table="trip_stats"
website = 'http://taxilimited.com/'

session = trip_stats.start_connection(cassandra_host_name, cassandra_trip_keyspace)
prep_trip_query = trip_stats.prepare_stats_query(session)

DAY = list(calendar.day_name)
MONTH = list(calendar.month_name)
Borough = ('Queens','Bronx','Brooklyn','Manhattan','Staten Island')

POINTS_MIN = 10
POINTS_MAX = 100

server = flask.Flask(__name__)
app = dash.Dash(__name__, server=server)
"""    
dcc.Input(id='t_day', value='', type='text'),
    dcc.Dropdown(id='day',
         options=[{'label': p, 'value': p} for p in DAY],
         multi=True
     ),
dcc.Input(id='t_month', value='', type='text'),
    dcc.Dropdown(id='month',
                 options=[{'label': p, 'value': p} for p in MONTH],
                 multi=True
                 ),
"""

app.layout = html.Div(
    [
        html.Div([
        html.H2('Taxi Stats',
                style={'float': 'left',
                       }),
        ]),
    dcc.Input(id='t_borough', value='', type='text'),
        dcc.Dropdown(id='borough',
                     options=[{'label': p, 'value': p} for p in Borough],
                     multi=True
                     ),
    html.Div(children=html.Div(id='graphs'), className='row'),
        dcc.Interval(
            id='graph-update',
            interval=1*1000
        ),
    ], className="container",style={'width':'98%','margin-left':10,'margin-right':10,'max-width':50000}
)

#dash.dependencies.Input('t_day', 'value'),
#dash.dependencies.Input('t_month', 'value'),


@app.callback(
    dash.dependencies.Output('graphs', 'children'),
    [dash.dependencies.Input('t_borough', 'value')],
    events=[dash.dependencies.Event('graph-update', 'interval')])
def update_graph(num_points, page_name):
    graphs = []

    # configure input for number of points to be displayed on the UI
    try:
        points = int(num_points)
        if points < POINTS_MIN:
            points = POINTS_MIN
        elif points > POINTS_MAX:
            points = POINTS_MAX
    except:
        points = POINTS_MIN


    for b in Borough:
        df1 = trip_stats.get_stats_query(website + b, prep_trip_query, session)

        X = df1.time_block.values[:points]
        Y = df1.mean.values[:points]
        class_choice = 'col s12'

        data = plotly.graph_objs.Scatter(
                x=list(X),
                y=list(Y),
                name='Averages',
                mode= 'lines+markers'
                )

        graphs.append(html.Div(dcc.Graph(
            id=p,
            animate=True,
            figure={'data': [data],'layout' : go.Layout(xaxis=dict(range=[min(X),max(X)], tickformat='%H:%M:%S'),
                                                yaxis=dict(range=[min(Y),max(Y)]),
                                                title=p,
                                                shapes=[
                                                        {
                                                            'type': 'line',
                                                            'xref': 'paper',
                                                            'x0': 0,
                                                            'y0': avg_line, # use absolute value or variable here
                                                            'x1': 1,
                                                            'y1': avg_line, # ditto
                                                            'line': {
                                                                'color': 'rgb(50, 171, 96)',
                                                                'width': 1,
                                                                'dash': 'dash',
                                                            },
                                                        },
                                                    ])}), className=class_choice))
    return graphs

external_css = ["https://cdnjs.cloudflare.com/ajax/libs/materialize/0.100.2/css/materialize.min.css"]
for css in external_css:
    app.css.append_css({"external_url": css})

external_js = ['https://cdnjs.cloudflare.com/ajax/libs/materialize/0.100.2/js/materialize.min.js']
for js in external_css:
    app.scripts.append_script({'external_url': js})

if __name__ == '__main__':
    app.run_server(host="0.0.0.0", port=80)