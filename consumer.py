from confluent_kafka import Consumer
from confluent_kafka import Producer
from confluent_kafka import KafkaException

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

def consume_messages(consumer):
    try:
        consumer.subscribe([topic])

        while True:
            msg = consumer.poll(1.0)  # 1-second timeout
            if msg is None:
                continue
            if msg.error():
                if msg is not None and msg.error() is None:
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


topic="my-topic"
if __name__ == "__main__":

    props = read_ccloud_config("consumer.properties")
    props["group.id"] = "group-consumer"
    props["auto.offset.reset"] = "earliest"

    consumer = Consumer(props)
    consumer.subscribe([topic])
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
