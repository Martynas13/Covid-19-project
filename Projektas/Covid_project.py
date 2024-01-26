import dash
from dash import Dash, dcc, html, Input, Output, State
from pymongo import MongoClient
from datetime import datetime
import uuid
import json

with open('config.json') as f:
    config = json.load(f)

# Mongodb comment schema
comment_schema = {
    'id': str,
    'text': str,
    'created_at': datetime,
}

client = MongoClient(f"mongodb+srv://{config['MONGO_USERNAME']}:{config['MONGO_PASSWORD']}@{config['MONGO_CLUSTER']}"
                     f"/?retryWrites=true&w=majority")
db = client['Covid']
collection = db['Comments']

external_css = ['https://cdn.jsdelivr.net/npm/bootstrap@5.3.1/dist/css/bootstrap.min.css']

app = Dash(__name__, pages_folder='pages', use_pages=True, external_stylesheets=external_css)

app.layout = html.Div(children=[
    html.Nav(
        className="navbar navbar-expand-lg navbar-dark bg-dark",
        children=[
            html.Ul(
                className="navbar-nav",
                children=[
                    html.Li(
                        dcc.Link(page['name'], href=page["relative_path"], className="nav-link")
                    ) for page in dash.page_registry.values()
                ]
            )
        ]
    ),
    dash.page_container,
    html.Div([
        dcc.Input(id='comment-text', type='text', placeholder='Enter your comment'),
        html.Button('Submit Comment', id='submit-button'),
    ]),
],
    className="col-12 mx-auto",
    style={'max-width': 'none'}
)


# Handle comment submission
@app.callback(
    Output('comment-text', 'value'),
    [Input('submit-button', 'n_clicks')],
    [State('comment-text', 'value')]
)
def handle_comment_submit(n_clicks, comment_text):
    if n_clicks and comment_text:
        new_comment = {
            'id': str(uuid.uuid4()),
            'text': comment_text,
            'created_at': datetime.now(),
        }
        collection.insert_one(new_comment)

    return ''


if __name__ == '__main__':
    app.run_server(debug=True, port = 8888)