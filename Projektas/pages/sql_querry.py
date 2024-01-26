import dash
from dash import Output, Input, State
from dash import dcc, html, dash_table, callback
import snowflake.connector
import json

with open('config.json') as f:
    config = json.load(f)

external_css = ['https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css']

dash.register_page(__name__)

layout = html.Div([
    dcc.Textarea(
        id='sql-input',
        placeholder='Enter SQL query...',
        style={'width': '100%', 'height': 100, 'font-family': 'Courier New, monospace'},
    ),
    html.Button('Execute Query', id='execute-button', className='btn btn-primary'),
    html.Button('Remove Table', id='remove-table-button', className='btn btn-danger'),
    html.Div([
        html.Div(id='query-output'),
        dash_table.DataTable(
            id='table-output',
            style_table={'font-family': 'Arial, sans-serif'},
            style_header={
                'backgroundColor': 'lightgrey',
                'fontWeight': 'bold'
            },
            style_cell={
                'textAlign': 'left',
                'minWidth': '50px', 'maxWidth': '180px',
                'whiteSpace': 'normal',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
            },
        ),
    ])
])


@callback(
    [Output('query-output', 'children'),
     Output('table-output', 'data')],
    [Input('execute-button', 'n_clicks'),
     Input('remove-table-button', 'n_clicks')],
    [State('sql-input', 'value')]
)
def execute_sql_query_or_remove_table(n_clicks_execute, n_clicks_remove, sql_query):
    ctx = dash.callback_context

    if not ctx.triggered_id:
        raise dash.exceptions.PreventUpdate

    if 'execute-button' in ctx.triggered_id:
        # Connect to Snowflake database
        sf_conn = snowflake.connector.connect(
            user=config['SF_USER'],
            password=config['SF_PASSWORD'],
            account=config['SF_ACCOUNT'],
            warehouse=config['SF_WAREHOUSE'],
            database=config['SF_DATABASE'],
            schema=config['SF_SCHEMA']
        )
        cursor = sf_conn.cursor()

        try:
            cursor.execute(sql_query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            data = [dict(zip(columns, row)) for row in rows]
            query_output = f"Query: {sql_query}"
            return query_output, data

        except Exception as e:
            error_message = f"Error executing SQL query: {e}"
            return error_message, []

        finally:
            # Closing database connection
            cursor.close()
            sf_conn.close()

    elif 'remove-table-button' in ctx.triggered_id:
        # returning empty data to remove the table
        return "", []

