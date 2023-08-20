# -*- coding: utf-8 -*-
"""Basic Stream Processing with Spark Streaming_Consumer - Dennis Mutai


**Basic Stream Processing with Spark Streaming_Consumer Dennis Mutai**

Real-Time Network Traffic Analysis for Telecommunications
"""

!pip install Kafka-Python

!pip install streamlit

!pip install confluent-kafka

!pip install pandas

!pip install pyspark

import pandas as pd
#from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka import KafkaConsumer, KafkaProducer
import time
import random
from confluent_kafka import Producer, Consumer, KafkaError
import streamlit as st
import json
import matplotlib.pyplot as plt

# Start SparkSession


# Kafka producer configuration
bootstrap_servers = 'pkc-lzvrd.us-west4.gcp.confluent.cloud:9092'
sasl_username = 'T2RAQ7I6IKMPA3VR'
sasl_password = 'KETZqHY8AbJzDODAtyjAvzSya7PUNkMjOjdYRkQQdHMD6tjjCyCT4HmI2xwVjKfm'
topic_name = 'network-traffic'
processed_topic = 'network-traffic'


st.write("Consuming data stream from kafka")

consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers,
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username=sasl_username,
    sasl_plain_password=sasl_password
)

# Subscribe to the Kafka topic
consumer.subscribe([topic_name])

#Consume data from kafka
# Read data from Kafka and perform real-time visualization
def read_kafka_msg():
    # Create a Streamlit app
    st.title("Real-Time Network Traffic Analysis")

    # Create a plot to visualize processed data
    fig, ax = plt.subplots()

    # Initialize an empty list to store processed data
    processed_data = []

    # Function to process incoming Kafka messages and update the plot

def process_message(message):
        nonlocal processed_data
        if message is None:
            return
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                return
            else:
                print(f"Error: {message.error()}")
                return

        value = message.value().decode('utf-8')
        data = value.split(',')
        source_ip = data[0]
        destination_ip = data[1]
        bytes_sent = int(data[2])
        event_time = datetime.now()

        row = (source_ip, destination_ip, bytes_sent, event_time)
        df = spark.createDataFrame([row], schema=schema)

        # Perform window-based aggregations
        aggregated_df = df \
            .groupBy("source_ip") \
            .agg(sum("bytes_sent").alias("total_bytes_sent")) \
            .orderBy(desc("total_bytes_sent"))

        # Convert the aggregated DataFrame to JSON
        json_data = aggregated_df.select(to_json(struct("*")).alias("value")).first().value

        # Publish processed data to Kafka topic
        producer.produce(processed_topic, value=json_data.encode('utf-8'))

        # Wait for the message to be delivered to Kafka
        producer.flush()

        # Process the Kafka message and update the plot
        processed_data.append(bytes_sent)
        ax.clear()
        ax.plot(processed_data)
        ax.set_xlabel("Time")
        ax.set_ylabel("Processed Data")
        st.pyplot(fig)

while True:
      message = consumer.poll(1.0)
      if message is None:
          continue

      if message.error():
        if message.error().code() == KafkaError._PARTITION_EOF:
          continue
        else:
          print(f'error:{message.error()}')
          break

      # msg = json.loads(message.value().decode('utf-8'))
      # print(message)
      # print("read msg from kafka")

      process_message(message)

# Start reading data from Kafka and perform real-time visualization
read_kafka_msg()

# Wait for the streaming to finish
spark.streams.awaitAnyTermination()
