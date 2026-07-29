import json
import logging
from confluent_kafka import Consumer, Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("audio-stored-publisher")

KAFKA_BOOTSTRAP = "kafka1:9092"
SOURCE_TOPIC = "audio.uploaded"
DEST_TOPIC = "audio.stored"
MINIO_BASE_URL = "http://10.110.188.120:9000"
BUCKET = "audio-archive"

consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "audio-stored-publisher",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}

producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)
consumer.subscribe([SOURCE_TOPIC])


def build_audio_url(partition: int, offset: int) -> str:
    padded_offset = f"{offset:010d}"
    return (
        f"{MINIO_BASE_URL}/{BUCKET}/topics/{SOURCE_TOPIC}/"
        f"partition={partition}/{SOURCE_TOPIC}+{partition}+{padded_offset}.ogg"
    )


def delivery_report(err, msg):
    if err is not None:
        log.error(f"Échec publication audio.stored: {err}")
    else:
        log.info(f"Publié sur {msg.topic()} [{msg.partition()}]")


def main():
    log.info("Démarrage du publisher audio.stored...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error(f"Erreur consumer: {msg.error()}")
                continue

            try:
                value = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                log.error(f"Message audio.uploaded invalide (JSON): {e}")
                continue

            message_id = value.get("message_id")
            user_id = value.get("user_id")
            chat_id = value.get("chat_id")

            if not message_id:
                log.error(f"message_id manquant, message ignoré: {value}")
                continue

            audio_url = build_audio_url(msg.partition(), msg.offset())

            payload = {
                "message_id": message_id,
                "user_id": user_id,
                "chat_id": chat_id,
                "audio_url": audio_url,
            }

            producer.produce(
                DEST_TOPIC,
                key=str(message_id).encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
            log.info(f"audio.stored -> {payload}")

    except KeyboardInterrupt:
        log.info("Arrêt demandé.")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()