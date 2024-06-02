from kafka3 import KafkaConsumer

def create_consumer(broker_url, topic):
    """Create and configure a Kafka consumer."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[broker,], # List of brokers passed as a list
        auto_offset_reset='earliest', # Start reading at the earliest message
        group_id='my-consumer-group', # Consumer group ID
        consumer_timeout_ms=1000 # How long to block if no messages are found
    )
    return consumer

def consume_messages(consumer):
    """Consume messages from Kafka."""
    try:
        for message in consumer:
            print(f"Received message: {message.value.decode('utf-8')} at offset {message.offset}")
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        # Clean up: close the consumer connection on exit
        consumer.close()

# Kafka broker URL
broker = 'localhost:9092'

# Kafka topic to subscribe to
topic = 'my-topic'

if __name__ == "__main__":
    consumer = create_consumer(broker, topic)
    consume_messages(consumer)
