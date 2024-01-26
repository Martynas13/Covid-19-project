Project name: "COVID-19 Data Integration, Analysis, and Visualization Platform"
This project is a COVID-19 dashboard implemented using Dash, web application framework for
Python, application provides real-time visualizations and KPI metrics related to pandemic, leveraging
data from snowflake database and additional vaccine stock data from MongoDB. Backend interacts
with snowflake using ‘snowflake.connector’ library to query data as well MongoDB which stores
comments and retrieves additional data. Dashboard incorporates time series forecasting for
confirmed cases using ARIMA model. Frontend is developed using Dash’s HTML and CSS
components, app dynamically updates KPIs and charts based on selected country from dropdown
menu. There is option to write query in a webpage which uses Covid-19 database, using
‘snowflake.connector’ helps us to retrieve data. Clustering graph applies K-Means to identify patterns
in the data and visualizes the results using a scatter plot in the Dash web application, providing
insights into relationships between confirmed cases and deaths.
