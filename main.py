import dash
import pandas as pd
import plotly.express as px
import dash_bootstrap_components as dbc
from dash import dcc, html
from dash.dependencies import Input, Output
from kafka import KafkaConsumer
from utils import runThread1Arg
from json import loads
from time import sleep
from queue import Queue
from dash_bootstrap_templates import load_figure_template


# Initialize Dash app with a dark theme
load_figure_template("DARKLY")

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])

data_queue = Queue()
streaming_data = []


# Kafka consumer function
def kafka_consumer(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="latest",
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
)
def updateDistributionGraphs(n):
    while not data_queue.empty():
        data = data_queue.get()
        streaming_data.append(data)

    if not streaming_data:
        bar = px.bar(title="Waiting for Data...").update_layout(
            margin=dict(t=80, r=50, b=50, l=50)
        )
        pie = px.pie(title="Waiting for Data...").update_layout(
            margin=dict(t=80, r=50, b=50, l=50)
        )
        line = px.line(title="Waiting for Data...").update_layout(
            margin=dict(t=80, r=50, b=50, l=50)
        )

        waiting = (bar, pie, line)
        return waiting

    # Copy data to avoid threading issues and convert to DataFrame
    data_snapshot = streaming_data.copy()
    df = pd.DataFrame(data_snapshot)

    df["count"] = pd.to_numeric(df["count"], errors="coerce")
    df["category"] = df["category"].astype(str)
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

    # Custom colors for each category
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
        margin=dict(t=80, r=50, b=50, l=50),
    )

    # Update Layout for Pie Graph
    figure_pie.update_layout(
        margin=dict(t=80, r=50, b=50, l=50),
    )

    # Update Layout for Line Graph
    figure_timeline.update_layout(
        xaxis_title="Timestamp",
        yaxis_title="Traffic Count",
        legend_title="Vehicle type",
        xaxis=dict(tickformat="%H:%M"),
        margin=dict(t=80, r=50, b=50, l=50),
    )

    # Return Bar, Pie and Line Graph
    return figure_bar, figure_pie, figure_timeline


# Styles
nav_style = {"padding": "0px"}
container_style = {"height": "calc(100vh - 40px)"}
col_style = {"height": "100%", "padding": "1px"}
graph_style = {"height": "100%", "margin": "1px"}

# Define Dash layout with a graph and interval for updates
app.layout = html.Div(
    [
        dbc.NavbarSimple(
            brand="Smart City Traffic Management",
            brand_href="#",
            color="dark",
            dark=True,
            expand="lg",
            className="ml-auto",
            style=nav_style,
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
        ),
        dbc.Container(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            dcc.Graph(id="traffic-count-graph", style=graph_style),
                            style=col_style,
                            width=7,
                        ),
                        dbc.Col(
                            dcc.Graph(id="traffic-pie-graph", style=graph_style),
                            style=col_style,
                            width=5,
                        ),
                    ],
                    className="h-50",
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            dcc.Graph(id="traffic-timeline-graph", style=graph_style),
                            style=col_style,
                            width=12,
                        ),
                    ],
                    className="h-50",
                ),
                dcc.Interval(
                    id="traffic-interval", interval=5 * 1000, n_intervals=0
                ),  # Update every 2 seconds
            ],
            style=container_style,
            fluid=True,
        ),
    ]
)

# Start Dash app
if __name__ == "__main__":
    # Start Kafka consumer in a separate thread
    runThread1Arg(kafka_consumer, "data-distribution")

    app.run(debug=True, dev_tools_ui=False)
