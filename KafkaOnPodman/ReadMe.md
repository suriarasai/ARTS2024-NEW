# Kafka Python Example
This repository contains examples of using Apache Kafka with Python. The examples include simple Kafka consumers and producers, JSON-serialized messages, and Avro-serialized messages using the Confluent Schema Registry.
## Prerequisites
To run these examples, you will need:

1. Kafka: Apache Kafka should be installed and running. You can set up Kafka locally using Docker and docker-compose.
2. Python 3.x: Make sure you have Python 3 installed on your machine.
3. Kafka-Python Library: Install the Kafka Python client library.

## Installation of Required Packages
You can install the required packages using pip:

```bash
pip install kafka-python
pip install confluent-kafka  # For Avro serialization
```

If you are using Avro serialization with a Schema Registry, ensure the Confluent libraries are installed.

## Optional
To run the environment with Docker, the podman-compose.yml file is provided for setting up Kafka, Schema Registry, and Control Center.

To set up Docker Compose, you can use:

```bash
podman-compose up -d
```

This will spin up the Kafka broker, schema registry, and Confluent Control Center.

## Files Description
### consumer.py
A simple Kafka consumer that subscribes to a topic and prints the received messages.
- Uses the KafkaConsumer to consume messages.
- Consumes from the `my-topic` Kafka topic.
### JSONSerializedConsumer.py
Consumes JSON-serialized messages from Kafka.
- The messages are automatically deserialized from JSON format.
- Consumes from the `json-test-topic`.
### JSONSerializedProducer.py
Produces JSON-serialized messages to Kafka.
- Uses the KafkaProducer to produce JSON messages.
- Sends to the `json-test-topic`.
### KafkaAvroConsumer.py
Consumes Avro-encoded messages using Confluent Schema Registry.
- Requires confluent-kafka library.
- Consumes from the Avro-based topic and deserializes the Avro data using the Schema Registry.
### KafkaAvroProducer.py
Produces Avro-encoded messages using Confluent Schema Registry.
- Requires confluent-kafka library.
- Produces Avro messages to a Kafka topic registered with the Schema Registry.
### main.py
A general entry point for running Kafka consumers and producers as needed.
producer.py
A simple Kafka producer that sends messages to a Kafka topic.
- Sends messages to `my-topic`.
### TwitterAuthentication.py
This script includes Twitter authentication to collect data from the Twitter API. The collected data can be forwarded to Kafka for further processing.
### podman-compose.yml
This Docker Compose file sets up the following services:
- Kafka broker
- Schema Registry
- Confluent Control Center.
### Running the Scripts
1. Make sure Kafka is running locally or via Docker Compose.
2. Use the Python scripts to interact with Kafka topics.

For example, to run the producer.py:

```bash
python producer.py
```

To run the consumer.py:

```bash
python consumer.py
```

For JSON and Avro examples, you can similarly run the JSONSerializedProducer.py, JSONSerializedConsumer.py, KafkaAvroProducer.py, and KafkaAvroConsumer.py scripts.

### License
This project is licensed under the MIT License.
