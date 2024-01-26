import dash
from dash import dcc, html
import snowflake.connector
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import plotly.express as px
import json

with open('config.json') as f:
    config = json.load(f)


def fetch_data_from_snowflake(query):
    sf_conn = snowflake.connector.connect(
        user=config['SF_USER'],
        password=config['SF_PASSWORD'],
        account=config['SF_ACCOUNT'],
        warehouse=config['SF_WAREHOUSE'],
        database=config['SF_DATABASE'],
        schema=config['SF_SCHEMA']
    )
    cursor = sf_conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    sf_conn.close()

    return pd.DataFrame(rows, columns=columns)


confirmed_query = 'SELECT PROVINCE_STATE, MAX(CASES) AS CONFIRMED FROM JHU_COVID_19 WHERE CASE_TYPE = \'Confirmed\' GROUP BY PROVINCE_STATE'
confirmed_data = fetch_data_from_snowflake(confirmed_query)


deaths_query = 'SELECT PROVINCE_STATE, MAX(CASES) AS DEATHS FROM JHU_COVID_19 WHERE CASE_TYPE = \'Deaths\' GROUP BY PROVINCE_STATE'
deaths_data = fetch_data_from_snowflake(deaths_query)

data = pd.merge(confirmed_data, deaths_data, on='PROVINCE_STATE', how='inner')

scaler = StandardScaler()
scaled_data = scaler.fit_transform(data[['CONFIRMED', 'DEATHS']])

num_clusters = 5
kmeans = KMeans(n_clusters=num_clusters, random_state=42)
clusters = kmeans.fit_predict(scaled_data)

data['Cluster'] = clusters


external_css = ['https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css']

dash.register_page(__name__)
title = 'COVID-19 Clustering Dashboard'
layout = html.Div([
    dcc.Graph(
        id='scatter-plot',
        figure=px.scatter(
            data, x='CONFIRMED', y='DEATHS', color='Cluster',
            hover_data=['PROVINCE_STATE', 'CONFIRMED', 'DEATHS', 'Cluster'],
            labels={'CONFIRMED': 'Confirmed Cases', 'DEATHS': 'Deaths', 'PROVINCE_STATE': 'State'},
            title='K-Means Clustering'
        )
    ),
])

