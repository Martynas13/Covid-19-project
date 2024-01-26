import dash
from dash import Output, Input
from dash import dcc, html, callback
import snowflake.connector
from pymongo import MongoClient
from statsmodels.tsa.arima.model import ARIMA
import pandas as pd
import json

with open('config.json') as f:
    config = json.load(f)

# Mongodb connection
mongo_client = MongoClient(f"mongodb+srv://{config['MONGO_USERNAME']}:{config['MONGO_PASSWORD']}@{config['MONGO_CLUSTER']}/?retryWrites=true&w=majority")

db = mongo_client['Covid']
external_css = ['https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css']

dash.register_page(__name__)

# Snowflake connection
def get_snowflake_connection(config):
    return snowflake.connector.connect(
        user=config['SF_USER'],
        password=config['SF_PASSWORD'],
        account=config['SF_ACCOUNT'],
        warehouse=config['SF_WAREHOUSE'],
        database=config['SF_DATABASE'],
        schema=config['SF_SCHEMA']
    )


# Arima forecasting
def forecast_time_series(x, y, forecast_steps=60):
    model = ARIMA(y, order=(1, 1, 0))
    fitted_model = model.fit()

    forecast_values = fitted_model.forecast(steps=forecast_steps)

    future_dates = pd.date_range(start=x[-1], periods=forecast_steps + 1, freq='D')[1:]
    extended_x = x + list(future_dates)

    return extended_x, list(y) + list(forecast_values)


# Query Snowflake for data in a specific country
def query_snowflake(country):
    sf_conn = get_snowflake_connection(config)
    cursor = sf_conn.cursor()
    cursor.execute('SELECT * FROM JHU_COVID_19 WHERE COUNTRY_REGION = %s', (country,))
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    sf_conn.close()

    result = []
    for row in rows:
        result.append(dict(zip(columns, row)))

    return result


dash.register_page(__name__, path='/')

layout = html.Div(children=[
    html.Div(
        id='info-container',
        style={'display': 'flex',
               'flex-direction': 'row',
               'align-items': 'center',
               'justifyContent': 'center'},
        children=[
            html.Div(
                id='kpi-1',
                style={
                    'overflow': 'hidden',
                    'width': '200px',
                    'height': '120px',
                    'border': '1px solid black',
                    'border-radius': '5px',
                    'background-color': '#343434',
                    'fontFamily': 'Helvetica, sans-serif',
                    'color': 'white',
                    'textAlign': 'center',
                    'padding': '10px',
                    'margin': '50px',
                    'boxShadow': '1px 1px 3px rgba(0,0,0,0.75)',
                },
            ),
            html.Div(
                id='kpi-2',
                style={
                    'overflow': 'hidden',
                    'width': '200px',
                    'height': '120px',
                    'border': '1px solid black',
                    'border-radius': '5px',
                    'background-color': '#343434',
                    'fontFamily': 'Helvetica, sans-serif',
                    'color': 'white',
                    'textAlign': 'center',
                    'padding': '10px',
                    'margin': '50px',
                    'boxShadow': '1px 1px 3px rgba(0,0,0,0.75)'
                },
            ),
            html.Div(
                id='kpi-3',
                style={
                    'overflow': 'hidden',
                    'width': '200px',
                    'height': '120px',
                    'border': '1px solid black',
                    'border-radius': '5px',
                    'background-color': '#343434',
                    'fontFamily': 'Helvetica, sans-serif',
                    'color': 'white',
                    'textAlign': 'center',
                    'padding': '10px',
                    'margin': '50px',
                    'boxShadow': '1px 1px 3px rgba(0,0,0,0.75)'
                },
            ),
            html.Div(
                id='kpi-4',
                style={
                    'overflow': 'hidden',
                    'width': '200px',
                    'height': '120px',
                    'border': '1px solid black',
                    'border-radius': '5px',
                    'background-color': '#343434',
                    'fontFamily': 'Helvetica, sans-serif',
                    'color': 'white',
                    'textAlign': 'center',
                    'padding': '10px',
                    'margin': '50px',
                    'boxShadow': '1px 1px 3px rgba(0,0,0,0.75)'
                },
            )
        ]
    ),
    dcc.Dropdown(
        id='country-dropdown',
        options=[],
        value='Lithuania',
        multi=False
    ),

    dcc.Graph(id='covid-line-chart'),
])


@callback(
    [Output(f'kpi-{i}', 'children') for i in range(1, 5)],
    [Input('country-dropdown', 'value')]
)
def update_kpis(selected_country):
    # checking if country is selected
    if selected_country:
        # making connection to snowflake
        sf_conn = get_snowflake_connection(config)
        cursor = sf_conn.cursor()

        # SQL query
        queries = [
            "SELECT MAX(CASES) AS TOTAL_CONFIRMED FROM JHU_COVID_19"
            " WHERE CASE_TYPE = 'Confirmed' AND COUNTRY_REGION = %s",
            "SELECT MAX(CASES) AS TOTAL_DEATHS FROM JHU_COVID_19"
            " WHERE CASE_TYPE = 'Deaths' AND COUNTRY_REGION = %s",
            "SELECT MAX(CASES) AS TOTAL_ACTIVE FROM JHU_COVID_19"
            " WHERE CASE_TYPE = 'Active' AND COUNTRY_REGION = %s",
            "SELECT MAX(CASES) AS TOTAL_RECOVERED FROM JHU_COVID_19"
            " WHERE CASE_TYPE = 'Recovered' AND COUNTRY_REGION = %s"
        ]

        # labels for KPIs
        labels = ["Confirmed", "Deaths", "Active", "Recovered"]

        results = []    # Storing results for each KPI in a list
        for i, query in enumerate(queries, start=1):
            # Executing SQL query
            cursor.execute(query, (selected_country,))
            total_value = cursor.fetchone()[0]  # Fetching results of query
            kpi_label = f"Total {labels[i-1]}: "    # Making KPI label
            results.append([
                html.H3(kpi_label,
                        style={'textAlign': 'center', 'color': 'white', 'fontWeight': 'bold', 'fontSize': '25px'}),
                html.Div(f"{int(total_value)}",
                         style={'textAlign': 'center', 'color': 'white', 'fontWeight': 'bold', 'fontSize': '25px'})
            ])

        cursor.close()
        sf_conn.close()

        return results


@callback(
    Output('country-dropdown', 'options'),
    [Input('country-dropdown', 'value')]
)
def update_dropdown_options(selected_country):
    # Fetch distinct countries from Snowflake
    query = 'SELECT DISTINCT COUNTRY_REGION FROM JHU_COVID_19'
    sf_conn = get_snowflake_connection(config)
    cursor = sf_conn.cursor()
    cursor.execute(query)
    countries = [row[0] for row in cursor.fetchall()]
    cursor.close()
    sf_conn.close()

    # Format options for the dropdown
    options = [{'label': country, 'value': country} for country in countries]

    return options


@callback(
    Output('covid-line-chart', 'figure'),
    [Input('country-dropdown', 'value')]
)
def update_data(selected_country):
    # Fetch snowflake data for the specific country

    # SQL query
    confirmed_query = f"SELECT DATE, MAX(CASES) AS TOTAL_CONFIRMED FROM JHU_COVID_19 WHERE CASE_TYPE = 'Confirmed'" \
                      f" AND COUNTRY_REGION = '{selected_country}' GROUP BY DATE ORDER BY DATE"
    deaths_query = f"SELECT DATE, MAX(CASES) AS TOTAL_DEATHS FROM JHU_COVID_19 WHERE CASE_TYPE = 'Deaths'" \
                   f" AND COUNTRY_REGION = '{selected_country}' GROUP BY DATE ORDER BY DATE"
    active_query = f"SELECT DATE, MAX(CASES) AS TOTAL_DEATHS FROM JHU_COVID_19 WHERE CASE_TYPE = 'Active'" \
                   f" AND COUNTRY_REGION = '{selected_country}' GROUP BY DATE ORDER BY DATE"
    recovered_query = f"SELECT DATE, MAX(CASES) AS TOTAL_DEATHS FROM JHU_COVID_19 WHERE CASE_TYPE = 'Recovered'" \
                      f" AND COUNTRY_REGION = '{selected_country}' GROUP BY DATE ORDER BY DATE"

    # Making connection to snowflake
    sf_conn = get_snowflake_connection(config)

    # Creating cursors to execute queries
    cursor_confirmed = sf_conn.cursor()
    cursor_deaths = sf_conn.cursor()
    cursor_active = sf_conn.cursor()
    cursor_recovered = sf_conn.cursor()

    # Queries to fetch data for every case type
    cursor_confirmed.execute(confirmed_query)
    cursor_deaths.execute(deaths_query)
    cursor_active.execute(active_query)
    cursor_recovered.execute(recovered_query)

    # Fetching results for every case type
    confirmed_data = cursor_confirmed.fetchall()
    deaths_data = cursor_deaths.fetchall()
    active_data = cursor_active.fetchall()
    recovered_data = cursor_recovered.fetchall()

    # Closing cursors
    cursor_confirmed.close()
    cursor_deaths.close()
    cursor_active.close()
    cursor_recovered.close()
    sf_conn.close()

    # Extract x and y values for the COVID-19 charts
    confirmed_x_values = [row[0] for row in confirmed_data]
    confirmed_y_values = [row[1] for row in confirmed_data]

    deaths_x_values = [row[0] for row in deaths_data]
    deaths_y_values = [row[1] for row in deaths_data]

    active_x_values = [row[0] for row in active_data]
    active_y_values = [row[1] for row in active_data]

    recovered_x_values = [row[0] for row in recovered_data]
    recovered_y_values = [row[1] for row in recovered_data]

    # Forecasting for confirmed cases
    confirmed_x_values_forecast, confirmed_y_values_forecast = forecast_time_series(confirmed_x_values, confirmed_y_values, forecast_steps=30)

    # Defining traces for every case type and forecasting
    trace_confirmed = {'x': confirmed_x_values, 'y': confirmed_y_values, 'type': 'line', 'name': 'Confirmed'}
    trace_confirmed_forecast = {'x': confirmed_x_values_forecast, 'y': confirmed_y_values_forecast, 'type': 'line', 'name': 'Confirmed Forecast'}
    trace_deaths = {'x': deaths_x_values, 'y': deaths_y_values, 'type': 'line', 'name': 'Deaths'}
    trace_active = {'x': active_x_values, 'y': active_y_values, 'type': 'line', 'name': 'Active'}
    trace_recovered = {'x': recovered_x_values, 'y': recovered_y_values, 'type': 'line', 'name': 'Recovered'}

    new_figure = {
        'data': [trace_confirmed, trace_confirmed_forecast, trace_deaths, trace_active, trace_recovered],
        'layout': {
            'title': f'COVID-19 Confirmed Cases and Deaths Over Time - {selected_country}',
            'xaxis': {'title': 'Date'},
            'yaxis': {'title': 'Cases'},
        }
    }

    return new_figure
