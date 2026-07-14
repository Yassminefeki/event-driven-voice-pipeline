import json
import os
from typing import Any

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka1:9092,kafka2:9092,kafka3:9092",
).split(",")


class KafkaService:
    def __init__(self, bootstrap_servers: list[str] | None = None):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8") if isinstance(key, str) else key,
        )

    def publish(self, topic: str, message: dict[str, Any], key: str | None = None) -> None:
        self.producer.send(topic=topic, value=message, key=key)
        self.producer.flush()


def build_audio_uploaded_message(
    audio_id: str,
    user_id: str,
    bucket: str,
    object_name: str,
    filename: str,
) -> dict[str, Any]:
    return {
        "topic": "audio.uploaded",
        "audio_id": audio_id,
        "user_id": user_id,
        "bucket": bucket,
        "object_name": object_name,
        "filename": filename,
    }


def build_transcription_completed_message(
    audio_id: str,
    user_id: str,
    text: str,
    bucket: str,
    object_name: str,
) -> dict[str, Any]:
    return {
        "topic": "transcription.completed",
        "audio_id": audio_id,
        "user_id": user_id,
        "text": text,
        "bucket": bucket,
        "object_name": object_name,
    }
