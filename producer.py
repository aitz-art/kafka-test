from confluent_kafka import Consumer
from confluent_kafka import Producer

def read_ccloud_config(config_file):
    omitted_fields = set(['schema.registry.url', 'basic.auth.credentials.source', 'basic.auth.user.info'])
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                if parameter not in omitted_fields:
                    conf[parameter] = value.strip()
    return conf

def produce_message(producer, message):
    try:
        producer.produce(topic, key='key', value=message)
        producer.flush()
        print(f"Produced message: {message}")
    except KafkaException as e:
        print(f"Error producing message: {e}")

topic="my-topic"
if __name__ == "__main__":
    
    producer = Producer(read_ccloud_config("client.properties"))
    produce_message(producer, "Hello, Kafka!")
    # producer.produce(topic, key="key", value="1")
    # producer.flush()
