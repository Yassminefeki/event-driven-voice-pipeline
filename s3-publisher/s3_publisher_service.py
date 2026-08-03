"""
s3_publisher_service.py

Etape 6 : detecte les fichiers ecrits dans MinIO (bucket audio-archive) par le
connecteur S3 Sink (etape 5), et publie la confirmation avec le chemin/nom
d'objet reel sur le topic audio.stored.

Approche : ecoute des evenements bucket MinIO (webhook / notification S3)
plutot qu'un polling actif, pour rester reactif sans surcharger MinIO.
"""

import json
import logging
import os

from dotenv import load_dotenv
from kafka import KafkaProducer
from minio import Minio

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092"
).split(",")
TOPIC_AUDIO_STORED = os.environ.get("KAFKA_TOPIC_AUDIO_STORED", "audio.stored")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://10.110.188.120:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "audio-archive")


def _extract_message_id_from_object_name(object_name: str) -> str | None:
    """Le connecteur S3 Sink nomme les objets en se basant sur la cle Kafka
    (message_id) -- voir kafka/connect/minio-s3-sink.json.
    Convention attendue : <message_id>/... ou <message_id>.bin selon le partitioner.
    """
    if "/" in object_name:
        return object_name.split("/")[0]
    return object_name.split(".")[0] if "." in object_name else object_name


class S3PublisherService:
    def __init__(self):
        self.minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,  # NOTE SECURITE: passer a True + certificats en prod
        )
        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=5,
        )

    def on_object_created(self, object_name: str, bucket: str = MINIO_BUCKET) -> None:
        """Callback appele quand MinIO notifie la creation d'un objet
        (a brancher sur les notifications bucket MinIO, ex: webhook HTTP)."""
        message_id = _extract_message_id_from_object_name(object_name)

        if message_id is None:
            logger.error("Impossible d'extraire message_id depuis object_name=%s", object_name)
            return

        payload = {
            "message_id": message_id,
            "object_name": object_name,
            "bucket": bucket,
        }

        future = self.producer.send(TOPIC_AUDIO_STORED, key=message_id, value=payload)
        future.get(timeout=10)

        logger.info(
            "audio.stored publie (message_id=%s, object_name=%s)", message_id, object_name
        )

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()


if __name__ == "__main__":
    # Exemple minimal de boucle de polling (a remplacer par des notifications
    # MinIO en production : bien plus efficace que le polling).
    import time

    service = S3PublisherService()
    seen_objects: set[str] = set()

    logger.info("S3 Publisher Service demarre (mode polling, bucket=%s)", MINIO_BUCKET)

    try:
        while True:
            objects = service.minio_client.list_objects(MINIO_BUCKET, recursive=True)
            for obj in objects:
                if obj.object_name not in seen_objects:
                    seen_objects.add(obj.object_name)
                    service.on_object_created(obj.object_name)
            time.sleep(5)
    except KeyboardInterrupt:
        pass
    finally:
        service.close()
