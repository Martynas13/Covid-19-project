import dash
from dash import dcc, html
import snowflake.connector
import pandas as pd
import plotly.graph_objs as go
import numpy as np
from pymongo import MongoClient
import json

with open('config.json') as f:
    config = json.load(f)


# Connect to mongodb
conn = MongoClient(f"mongodb+srv://{config['MONGO_USERNAME']}:{config['MONGO_PASSWORD']}@{config['MONGO_CLUSTER']}/?retryWrites=true&w=majority")
db = conn['Covid']
collection_stocks = db['Stocks']

data_from_mongo = list(collection_stocks.find({}, {"_id": 1, "Date": 1,
                                                   "Close_BioNTech": 1, "Close_Moderna": 1, "Close_Johnson & Johnson": 1,
                                                   "Close_Inovio Pharmaceuticals": 1, "Close_Sinovac": 1, "Close_Sinopharm": 1,
                                                   "Close_Novavax": 1, "Close_Astrazeneca": 1}))

df_mongo = pd.DataFrame(data_from_mongo)

# Convert _id to string and 'Date' to datetime
df_mongo['_id'] = df_mongo['_id'].astype(str)
df_mongo['Date'] = pd.to_datetime(df_mongo['Date'])

# Melting the DataFrame
df_mongo_long = df_mongo.melt(id_vars=['_id', 'Date'], var_name='Stock', value_name='Close')

# SQL query
sql_query = """
SELECT DATE, MAX(CASES) AS TOTAL_CONFIRMED 
FROM JHU_COVID_19 
WHERE case_type = 'Confirmed'
GROUP BY DATE;
"""

# Connect to Snowflake
sf_conn = snowflake.connector.connect(
    user=config['SF_USER'],
    password=config['SF_PASSWORD'],
    account=config['SF_ACCOUNT'],
    warehouse=config['SF_WAREHOUSE'],
    database=config['SF_DATABASE'],
    schema=config['SF_SCHEMA']
)
cursor = sf_conn.cursor()
cursor.execute(sql_query)

# Load data from Snowflake
df_snowflake = pd.DataFrame(cursor.fetchall(), columns=['Date', 'Total_Confirmed'])
df_snowflake['Date'] = pd.to_datetime(df_snowflake['Date'])

# Merge the two DataFrames
merged_df = df_mongo_long.merge(df_snowflake, on="Date", how="inner")

# Separate DataFrames by stock
df_biontech = merged_df[merged_df['Stock'] == 'Close_BioNTech']
df_moderna = merged_df[merged_df['Stock'] == 'Close_Moderna']
df_jj = merged_df[merged_df['Stock'] == 'Close_Johnson & Johnson']
df_ip = merged_df[merged_df['Stock'] == 'Close_Inovio Pharmaceuticals']
df_sinovac = merged_df[merged_df['Stock'] == 'Close_Sinovac']
df_sinopharm = merged_df[merged_df['Stock'] == 'Close_Sinopharm']
df_novavax = merged_df[merged_df['Stock'] == 'Close_Novavax']
df_astra = merged_df[merged_df['Stock'] == 'Close_Astrazeneca']

min_stock_date = df_mongo['Date'].min()
max_stock_date = df_mongo['Date'].max()
df_snowflake['Date_str'] = df_snowflake['Date'].dt.strftime('%Y-%m-%d')
min_stock_date_str = min_stock_date.strftime('%Y-%m-%d')
max_stock_date_str = max_stock_date.strftime('%Y-%m-%d')

external_css = ['https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css']

dash.register_page(__name__)

title = 'COVID-19 Clustering Dashboard'
layout = html.Div([
    dcc.Graph(
        id='combined-chart',
        figure=go.Figure(
            data=[
                go.Bar(x=df_snowflake['Date_str'], y=df_snowflake['Total_Confirmed'], name='Total Confirmed Cases'),
                go.Scatter(x=df_biontech['Date'], y=df_biontech['Close'].astype(np.float16), mode='lines', name='BioNTech', line=dict(color='green'), yaxis='y2'),
                go.Scatter(x=df_moderna['Date'], y=df_moderna['Close'].astype(np.float16), mode='lines', name='Moderna', line=dict(color='blue'), yaxis='y2'),
                go.Scatter(x=df_jj['Date'], y=df_jj['Close'].astype(np.float16), mode='lines', name='Johnson & Johnson', line=dict(color='red'), yaxis='y2'),
                go.Scatter(x=df_ip['Date'], y=df_ip['Close'].astype(np.float16), mode='lines', name='Inovio Pharmaceuticals', line=dict(color='brown'), yaxis='y2'),
                go.Scatter(x=df_sinovac['Date'], y=df_sinovac['Close'].astype(np.float16), mode='lines', name='Sinovac', line=dict(color='grey'), yaxis='y2'),
                go.Scatter(x=df_sinopharm['Date'], y=df_sinopharm['Close'].astype(np.float16), mode='lines', name='Sinopharm', line=dict(color='yellow'), yaxis='y2'),
                go.Scatter(x=df_novavax['Date'], y=df_novavax['Close'].astype(np.float16), mode='lines', name='Novavax', line=dict(color='purple'), yaxis='y2'),
                go.Scatter(x=df_astra['Date'], y=df_astra['Close'].astype(np.float16), mode='lines', name='Astrazeneca', line=dict(color='black'), yaxis='y2'),
            ],
            layout=go.Layout(
                title='COVID-19 Cases and Stock Prices',
                xaxis=dict(title='Date', range=['2020-01-22', max_stock_date_str]),  # Set the x-axis range to start from the minimum stock date
                yaxis=dict(title='Total Confirmed Cases'),
                yaxis2=dict(title='Stock Value', overlaying='y', side='right'),
            )
        )
    ),
])