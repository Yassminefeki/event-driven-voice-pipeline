import json
from kafka import KafkaProducer, KafkaConsumer
from config.settings import KAFKA_BOOTSTRAP_SERVERS


def create_producer():

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )


def send_kafka_event(topic, message):

    producer = create_producer()

    producer.send(
        topic,
        message
    )

    producer.flush()
    producer.close()


def create_kafka_consumer(topic, group_id):

    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=group_id,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )