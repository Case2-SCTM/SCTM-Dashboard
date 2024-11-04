import threading
import json
import time;
from datetime import datetime
import queue

from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
from kafka import KafkaConsumer

# Initialize Dash app with a dark theme
load_figure_template("darkly")
app = Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])

data_queue = queue.Queue()

# Kafka consumer function
def kafka_consumer():
    consumer = KafkaConsumer(
        'traffic-data',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=1000
    )
    while True:
        for message in consumer:
            data = message.value
            # Add timestamp if not present
            if 'timestamp' not in data:
                data['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data_queue.put(data)
        # Sleep briefly to prevent tight loop if no messages
        time.sleep(1)

# Start Kafka consumer in a separate thread
consumer_thread = threading.Thread(target=kafka_consumer, daemon=True)
consumer_thread.start()

streaming_data = []

# Define Dash layout with a graph and interval for updates
app.layout = html.Div([
    html.H1("Real-Time Traffic Counts", style={'textAlign': 'center'}),
    dcc.Graph(id="traffic-graph"),
    dcc.Interval(id="interval-component", interval=2000, n_intervals=1000)  # Update every 2 seconds
])

@app.callback(Output("traffic-graph", "figure"), [Input("interval-component", "n_intervals")])
def update_graph(n):
    while not data_queue.empty():
        data = data_queue.get()
        streaming_data.append(data)

    if not streaming_data:
        return px.line(title="Waiting for Data...")

    # Copy data to avoid threading issues
    data_snapshot = streaming_data.copy()

    # Convert to DataFrame
    df = pd.DataFrame(data_snapshot)

    df['count'] = pd.to_numeric(df['count'], errors='coerce')
    df['category'] = df['category'].astype(str)

    # Drop rows with NaN values
    df.dropna(subset=['count', 'category'], inplace=True)

    df_grouped = df.groupby('category', as_index=False)['count'].sum()

    # Create the Plotly figure
    fig = px.bar(
        df_grouped,
        x='category',
        y='count',
        title='Total Trafic Counts by Vehicle Type',
        labels={'category': 'vehicle type', 'count': 'total count'}
    )

    fig.update_layout(
        xaxis_title='Vehicle Type',
        yaxis_title='Total Count',
        legend_title='Category',
    )

    return fig

# Start Dash app
if __name__ == "__main__":
    app.run_server(debug=True)



# Command to start the zookeeper server:
# .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

# Command to start the Kafka server:
#.\bin\windows\kafka-server-start.bat .\config\server.properties




# def main():
#     print("Hello World!")


# if __name__ == "__main__":
#     main()