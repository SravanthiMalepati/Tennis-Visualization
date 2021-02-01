import os
import pandas as pd
from dash import Dash, callback_context, no_update
from dash.dependencies import Input, Output
from dash_table import DataTable
import dash_core_components as dcc
import dash_html_components as html

path = "./data/sample_tennis_data_table.csv"
df = pd.read_csv(path)

app = Dash(__name__)
app.layout = html.Div(
    [
        dcc.Location(id = 'url', refresh = False),
        DataTable(
            id="table",
            columns=[{"name": i, "id": i} for i in df.columns],
            data=df.to_dict("records"),
            export_format="csv",
        )
    ]
)

@app.callback(Output('table', 'data'),
              Output('table', 'columns'),
              [Input('url', 'pathname')])
def display_page(pathname):
    df = pd.read_csv(f'./data{pathname}.csv')
    return df.to_dict("records"), [{"name": i, "id": i} for i in df.columns]

if __name__ == "__main__":
    app.run_server(host='127.0.0.1',debug=True)