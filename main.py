import threading
import json
import time;
from datetime import datetime

from dash import Dash, html, dcc, callback, Output, Input
import plotly.express as px
import pandas as pd
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
from kafka import KafkaConsumer

# Initialize Dash app with a dark theme
load_figure_template("darkly")
app = Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])

# Global list to store streaming data
streaming_data = []

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
            streaming_data.append(data)
        # Sleep briefly to prevent tight loop if no messages
        time.sleep(1)

# Start Kafka consumer in a separate thread
consumer_thread = threading.Thread(target=kafka_consumer, daemon=True)
consumer_thread.start()

# Define Dash layout with a graph and interval for updates
app.layout = html.Div([
    html.H1("Real-Time Traffic Data", style={'textAlign': 'center'}),
    dcc.Graph(id="traffic-graph"),
    dcc.Interval(id="interval-component", interval=2000, n_intervals=0)  # Update every 2 seconds
])

@app.callback(Output("traffic-graph", "figure"), [Input("interval-component", "n_intervals")])
def update_graph(n):
    if not streaming_data:
        return px.line(title="Waiting for Data...")

    # Copy data to avoid threading issues
    data_snapshot = streaming_data.copy()
    # Limit to the last 100 data points for performance
    data_snapshot = data_snapshot[-100:]

    # Convert to DataFrame
    df = pd.DataFrame(data_snapshot)

    # Ensure data types are correct
    df['speed'] = pd.to_numeric(df['speed'], errors='coerce')
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    # Drop rows with NaN values
    df.dropna(subset=['speed', 'timestamp', 'vehicle_id'], inplace=True)

    # Sort by timestamp
    df.sort_values('timestamp', inplace=True)

    # Create the Plotly figure
    fig = px.line(
        df,
        x="timestamp",
        y="speed",
        color="vehicle_id",
        title="Traffic Speed Over Time"
    )

    fig.update_layout(xaxis_title="Timestamp", yaxis_title="Speed (km/h)")

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