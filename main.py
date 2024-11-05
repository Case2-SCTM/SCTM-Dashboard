from threading import Thread
from json import loads
from time import sleep
from datetime import datetime
from queue import Queue

import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from dash_bootstrap_templates import load_figure_template
from kafka import KafkaConsumer
from utils import runThread, runThread1Arg


# Initialize Dash app with a dark theme
load_figure_template("DARKLY")

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])

data_queue = Queue()
streaming_data = []

custom_colors = {
    "car": "#1f77b4",
    "bicycle": "#ff7f0e",
    "van": "#d62728",
    "pedestrian": "#9467bd",
    "motorcycle": "#8c564b",
    "bus": "#e377c2",
    "heavy": "#2ca02c",
    "light": "#7f7f7f",
}


# Kafka consumer function
def kafka_consumer(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=["localhost:9092"],
        value_deserializer=lambda x: loads(x.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    while True:
        for message in consumer:
            data = message.value
            data_queue.put(data)
        # Sleep briefly to prevent tight loop if no messages
        sleep(1)


@app.callback(
    [
        Output("traffic-count-graph", "figure"),
        Output("traffic-pie-graph", "figure"),
        Output("traffic-timeline-graph", "figure"),
    ],
    [Input("traffic-interval", "n_intervals")],
    [
        State("traffic-count-graph", "figure"),
        State("traffic-pie-graph", "figure"),
        State("traffic-timeline-graph", "figure"),
    ],
)
def updateDistributionGraphs(n, state_bar, state_pie, state_timeline):
    while not data_queue.empty():
        data = data_queue.get()
        streaming_data.append(data)

    if not streaming_data:
        waiting = (
            px.bar(title="Waiting for Data..."),
            px.pie(title="Waiting for Data..."),
            px.line(title="Waiting for Data..."),
        )
        return waiting

    # Copy data to avoid threading issues
    data_snapshot = streaming_data.copy()

    # Convert to DataFrame
    df = pd.DataFrame(data_snapshot)

    # Bar and Pie
    df["count"] = pd.to_numeric(df["count"], errors="coerce")
    df["category"] = df["category"].astype(str)

    # Timeline
    df["start_timestamp"] = pd.to_datetime(
        df["start_timestamp"], unit="s", errors="coerce"
    )
    df["end_timestamp"] = pd.to_datetime(df["end_timestamp"], unit="s", errors="coerce")

    # Groupped data for bar and pie graph
    df_grouped_category = df.groupby("category", as_index=False)["count"].sum()

    # Groupped data for timeline graph
    df_grouped_timeline = df.groupby(["start_timestamp", "category"], as_index=False)[
        "count"
    ].sum()

    df_grouped_timeline.sort_values("start_timestamp", inplace=True)

    # Create Plotly Bar Graph Figure
    figure_bar = px.bar(
        df_grouped_category,
        x="category",
        y="count",
        title="Total Traffic Counts by Vehicle Type",
        labels={"category": "vehicle type", "count": "total count"},
        color="category",
        color_discrete_map=custom_colors,
    )

    # Create Plotly Pie Graph Figure
    figure_pie = px.pie(
        df_grouped_category,
        names="category",
        values="count",
        title="Traffic Distribution by Vehicle Type",
        color="category",
        color_discrete_map=custom_colors,
    )

    # Create Plotly Line Graph
    figure_timeline = px.line(
        df_grouped_timeline,
        x="start_timestamp",
        y="count",
        title="Total Traffic Counts by Vehicle Type",
        labels={"category": "vehicle type", "count": "total count"},
        color="category",
        color_discrete_map=custom_colors,
    )

    # Update Layout for Bar Graph
    figure_bar.update_layout(
        xaxis_title="Vehicle Type",
        yaxis_title="Total Count",
        legend_title="Category",
    )

    # Update Layout for Line Graph
    figure_timeline.update_layout(
        xaxis_title="Timestamp",
        yaxis_title="Traffic Count",
        legend_title="Vehicle type",
        xaxis=dict(tickformat="%H:%M"),
    )

    # Update layout only if the existing figures are not the "Waiting for Data" placeholders
    if (
        state_bar
        and state_bar.get("layout", {}).get("title", {}).get("text")
        != "Waiting for Data..."
    ):
        figure_bar.update_layout(state_bar["layout"], overwrite=False)

    if (
        state_pie
        and state_pie.get("layout", {}).get("title", {}).get("text")
        != "Waiting for Data..."
    ):
        figure_pie.update_layout(state_pie["layout"], overwrite=False)

    if (
        state_timeline
        and state_timeline.get("layout", {}).get("title", {}).get("text")
        != "Waiting for Data..."
    ):
        figure_timeline.update_layout(state_timeline["layout"], overwrite=False)

    # Return Bar, Pie and Line Graph
    return (figure_bar, figure_pie, figure_timeline)


# Define Dash layout with a graph and interval for updates
app.layout = html.Div(
    [
        dbc.NavbarSimple(
            brand="Smart City Traffic Management",
            brand_href="#",
            color="dark",
            dark=True,
            children=[
                dbc.NavItem(
                    html.Div(
                        "Lavet af Simon og Tue",
                        style={
                            "color": "white",
                            "fontSize": "16px",
                            "marginRight": "15px",
                        },
                    )
                )
            ],
            expand="lg",
            className="ml-auto",
        ),
        dbc.Container(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            dcc.Graph(id="traffic-count-graph"),
                            style={"padding": "1px"},
                            width=7,
                        ),
                        dbc.Col(
                            dcc.Graph(id="traffic-pie-graph"),
                            style={"padding": "1px"},
                            width=5,
                        ),
                    ]
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            dcc.Graph(id="traffic-timeline-graph"),
                            style={"padding": "1px"},
                            width=8,
                        ),
                        dbc.Col(
                            dcc.Graph(id="traffic-statistics-graph"),
                            style={"padding": "1px"},
                            width=4,
                        ),
                    ]
                ),
                dcc.Interval(
                    id="traffic-interval", interval=1 * 1000, n_intervals=0
                ),  # Update every 2 seconds
            ],
            fluid=True,
        ),
    ]
)

# Start Dash app
if __name__ == "__main__":
    # Start Kafka consumer in a separate thread
    runThread1Arg(kafka_consumer, "data-distribution")
    # runThread1Arg(kafka_consumer, "data-odmatrix")

    app.run(debug=True)
