

from flask import Flask
import dash
from dash import dash_table, html
from dash.dependencies import Input, Output, State
import pandas as pd

from weather_service.db_utils import get_alerts

def load_data():
    
    data = []
    alerts = get_alerts()
    for alert in alerts:
        data.append({
            'dt': alert.dt,
            'city': alert.city,
            'trigger': alert.trigger,
            'reason': alert.reason,
        })
    
    return pd.DataFrame(data)

app = dash.Dash(__name__, requests_pathname_prefix='/alerts/')

app.layout = html.Div([
    html.H1("Thresholds"),
    dash_table.DataTable(
        id='table',
        columns=[{"name": i, "id": i} for i in load_data().columns],
        data=load_data().to_dict('records'),
        editable=True,
    ),
])

# Run the server
if __name__ == '__main__':
    app.run_server(debug=True)
