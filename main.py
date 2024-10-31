from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder.appName("TrafficDataProcessing").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1").getOrCreate()

kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "traffic-data"

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers).option("subscribe", kafka_topic).load()

schema = StructType().add("vehicle_id", StringType()).add("speed", IntegerType()).add("location", StringType()).add("timestamp", StringType())

parse_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

query = parse_df.writeStream.outputMode("append").format("console").start()

query.awaitTermination()


# Command to start the zookeeper server:
# .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

# Command to start the Kafka server:
# .\bin\windows\kafka-server-start.bat .\config\server.properties













# def main():
#     print("Hello World!")


# if __name__ == "__main__":
#     main()


# from dash import Dash, html, dcc, callback, Output, Input
# import plotly.express as px
# import pandas as pd
# import dash_bootstrap_components as dbc
# from dash_bootstrap_templates import load_figure_template

# # loads the "darkly" template and sets it as the default
# load_figure_template("darkly")

# df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/gapminder_unfiltered.csv')

# app = Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])

# app.layout = [
#     html.H1(children='Title of Dash App', style={'textAlign':'center'}),
#     dcc.Dropdown(df.country.unique(), 'Canada', id='dropdown-selection'),
#     dcc.Graph(id='graph-content')
# ]

# @callback(
#     Output('graph-content', 'figure'),
#     Input('dropdown-selection', 'value')
# )
# def update_graph(value):
#     dff = df[df.country==value]
#     return px.line(dff, x='year', y='pop')

# if __name__ == '__main__':
#     app.run(debug=True)