from confluent_kafka import Producer, Consumer, KafkaException

# Kafka broker address
bootstrap_servers = 'your_kafka_broker_address'

# Example topic
topic = 'your_topic'

# Example producer
def produce_message(producer, message):
    try:
        producer.produce(topic, key='key', value=message)
        producer.flush()
        print(f"Produced message: {message}")
    except KafkaException as e:
        print(f"Error producing message: {e}")

# Example consumer
def consume_messages(consumer):
    try:
        consumer.subscribe([topic])

        while True:
            msg = consumer.poll(1.0)  # 1-second timeout
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() is None:
                    # End of partition event
                    continue
                else:
                    print(f"Error: {msg.error()}")
            else:
                print(f"Consumed message: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

# Example usage
if __name__ == "__main__":
    # Example producer configuration
    producer_config = {
        'bootstrap.servers': bootstrap_servers,
    }

    # Example consumer configuration
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest',  # You can change this to 'latest' or 'none' as needed
    }

    # Create producer and consumer instances
    producer = Producer(producer_config)
    consumer = Consumer(consumer_config)

    # Produce and consume messages
    produce_message(producer, "Hello, Kafka!")
    consume_messages(consumer)
