from threading import Thread
from json import loads
from time import sleep
from datetime import datetime
from queue import Queue

from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
from kafka import KafkaConsumer

# Initialize Dash app with a dark theme
load_figure_template("darkly")

app = Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])

data_queue = Queue()
streaming_data = []


# Kafka consumer function
def kafka_consumer():
    consumer = KafkaConsumer(
        "data-distribution",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="latest",
        value_deserializer=lambda x: loads(x.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    while True:
        for message in consumer:
            data = message.value
            # Add timestamp if not present
            if "timestamp" not in data:
                data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            data_queue.put(data)
        # Sleep briefly to prevent tight loop if no messages
        sleep(1)


@app.callback(
    Output("traffic-count-graph", "figure"),
    [Input("interval-component", "n_intervals")],
)
def update_count_graph(n_intervals):
    while not data_queue.empty():
        data = data_queue.get()
        streaming_data.append(data)

    if not streaming_data:
        return px.line(title="Waiting for Data...")

    # Copy data to avoid threading issues
    data_snapshot = streaming_data.copy()

    # Convert to DataFrame
    df = pd.DataFrame(data_snapshot)

    df["count"] = pd.to_numeric(df["count"], errors="coerce")
    df["category"] = df["category"].astype(str)

    # Drop rows with NaN values
    df.dropna(subset=["count", "category"], inplace=True)

    df_grouped = df.groupby("category", as_index=False)["count"].sum()

    custom_colors = {
        'car': '#1f77b4',
        'bicycle': '#ff7f0e',
        'van': '#d62728',
        'pedestrian': '#9467bd',
        'motorcycle': '#8c564b',
        'bus': '#e377c2',
        'heavy': '#2ca02c',
        'light': '#7f7f7f'
    }

    # Create the Plotly figure
    fig = px.bar(
        df_grouped,
        x="category",
        y="count",
        title="Total Trafic Counts by Vehicle Type",
        labels={"category": "vehicle type", "count": "total count"},
        color='category',
        color_discrete_map=custom_colors
    )

    fig.update_layout(
        xaxis_title="Vehicle Type",
        yaxis_title="Total Count",
        legend_title="Category",
    )

    return fig


# Define Dash layout with a graph and interval for updates
app.layout = html.Div(
    [
        html.Div(
            [
                html.H1("Smart City Traffic Management", style={"textAlign": "center"}),
                html.Button("Update Data", id="update-button", n_clicks=0),
            ],
            style={"display": "flex", "justify-content": "space-between", "align-items": "center"},
        ),
        html.Div(
            [
                dcc.Graph(id="traffic-count-graph", style={"width": "50%"}),
                dcc.Graph(id="traffic-timeline-graph"),
            ],
            style={"display": "flex"}
        ),
        html.Div([]),
        dcc.Interval(
            id="interval-component", interval=2000, n_intervals=1000
        ),  # Update every 2 seconds
    ]
)

# Start Dash app
if __name__ == "__main__":
    # Start Kafka consumer in a separate thread
    consumer_thread = Thread(target=kafka_consumer, daemon=True)
    consumer_thread.start()

    app.run_server(debug=True)
