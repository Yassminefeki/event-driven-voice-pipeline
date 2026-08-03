"""
kafka_producer.py

Wrapper autour de kafka-python pour publier les evenements du bot :
- audio.uploaded (etape 3)
- transcription.corrected (etape 13)
"""

import json
import logging
import os

from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092"
).split(",")

TOPIC_AUDIO_UPLOADED = os.environ.get("KAFKA_TOPIC_AUDIO_UPLOADED", "audio.uploaded")
TOPIC_TRANSCRIPTION_CORRECTED = os.environ.get(
    "KAFKA_TOPIC_TRANSCRIPTION_CORRECTED", "transcription.corrected"
)


class BotKafkaProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=5,
            linger_ms=20,
        )

    def publish_audio_uploaded(self, message_id: str, audio_base64: str, chat_id: int) -> None:
        """Etape 2-3 : publie l'audio encode en base64 sur audio.uploaded."""
        payload = {
            "message_id": message_id,
            "chat_id": chat_id,
            "audio_base64": audio_base64,
        }
        self._send(TOPIC_AUDIO_UPLOADED, key=message_id, value=payload)

    def publish_transcription_corrected(
        self,
        message_id: str,
        chat_id: int,
        original_text: str,
        corrected_text: str,
        wer: float,
        cer: float,
    ) -> None:
        """Etape 13 : publie la transcription validee/corrigee + metriques WER/CER."""
        payload = {
            "message_id": message_id,
            "chat_id": chat_id,
            "original_text": original_text,
            "corrected_text": corrected_text,
            "wer": wer,
            "cer": cer,
        }
        self._send(TOPIC_TRANSCRIPTION_CORRECTED, key=message_id, value=payload)

    def _send(self, topic: str, key: str, value: dict) -> None:
        try:
            future = self.producer.send(topic, key=key, value=value)
            future.get(timeout=10)
            logger.info("Message publie sur %s (message_id=%s)", topic, key)
        except KafkaError:
            logger.exception("Echec de publication Kafka sur %s (message_id=%s)", topic, key)
            raise

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()
