import dash
import dash.dependencies
import dash_core_components as dcc
import dash_html_components as html
import plotly
import plotly.plotly as py
import plotly.graph_objs as go
import trip_stats
from datetime import datetime
import flask
import numpy as np
import calendar

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

cassandra_host_name="ec2-18-235-39-97.compute-1.amazonaws.com"
cassandra_trip_keyspace="trip_batch"

session = trip_stats.start_connection(cassandra_host_name,cassandra_trip_keyspace)
prep_trip_query1 = trip_stats.prepare_stats_query(session)
prep_trip_query2 = trip_stats.prepare_actual_stats_query(session)

DAY = list(calendar.day_name)
MONTH = list(calendar.month_name)
Borough = ('EWR','Queens','Bronx','Brooklyn','Manhattan','Staten Island')

server = flask.Flask(__name__)
app = dash.Dash(__name__, server=server)

app.layout = html.Div([
    html.Div(
    [
        html.Div([
        html.H2('Taxi trip monitor',
                style={'float': 'center',
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
                style={'float': 'center',
                       }),
        ]),
        dcc.Dropdown(id='t_borough2',
                     options=[{'label': p, 'value': p} for p in Borough],
                     multi=True
                     ),
        html.Div(children=html.Div(id='graphs'), className='row'),
        dcc.Interval(
            id='graph-update',
            interval=10*1000
        ),
        #dcc.Graph(id='graph2')
    ]),
],className="container",style={'width':'98%','margin-left':10,'margin-right':10,'max-width':50000}
)


@app.callback(
    dash.dependencies.Output('graph1', 'figure'),
    [dash.dependencies.Input('t_borough1', 'value'),
     dash.dependencies.Input('t_day1', 'value'),
     dash.dependencies.Input('t_month1', 'value')],
    )
def update_graph1(t_borough1,t_day1,t_month1):
    df1 = trip_stats.get_stats_query(t_day1, t_month1, t_borough1, prep_trip_query1, session)

    if (df1 is None):
        return

    return {
        'data': [{
            'x': df1['time_block'].values,
            'y': df1['mean'].values,
            'type': 'bar'
        }],
        'layout': {'title':'Historical Trip averages'}
    }


@app.callback(
    dash.dependencies.Output('graphs', 'children'),
    [dash.dependencies.Input('t_borough2', 'value'),],
    events=[dash.dependencies.Event('graph-update', 'interval')],
    )
#events=[dash.dependencies.Event('graph-update', 'interval')]

def update_graph(t_borough2):
    graphs = []

    if (t_borough2 is None):
        return

    if len(t_borough2) > 2:
        class_choice = 'col s12 m6 l4'
    elif len(t_borough2) == 2:
        class_choice = 'col s12 m6 l6'
    else:
        class_choice = 'col s12'

    for br in t_borough2:
        df2 = trip_stats.get_actual_stats_query(br, prep_trip_query2, session)
        if (df2 is None):
            continue
        X = df2['time_block'].values
        Y = df2['actual_trips'].values
        Z = df2['mean'].values
        """
        actuals = go.Scatter(
            x=list(X),
            y=list(Y),
            name="Today's trips",
            mode='lines'
        )

        historical = go.Scatter(
            x=list(X),
            y=list(Z),
            name='Historical averages',
            mode='lines'
        )
        """
        data = go.Scatter(
            x=list(X),
            y=list(Z),
            name="Today's trips",
            mode='lines'
        )

        graphs.append(html.Div(dcc.Graph(
            id=br,
            animate=True,
            figure={'data': [data], 'layout': go.Layout(xaxis=dict(title='Time of day'),
                                                        yaxis=dict(range=[min(Y), max(Y)],title='Number of Trips'),
                                                        title=br,
                                                        shapes=[
                                                            {
                                                                'type': 'line',
                                                                'xref': 'paper',
                                                                'x0': 0,
                                                                'y0': 0,  # use absolute value or variable here
                                                                'x1': 1,
                                                                'y1': 0,  # ditto
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
    app.run_server(host="ec2-100-24-0-72.compute-1.amazonaws.com", port=80)