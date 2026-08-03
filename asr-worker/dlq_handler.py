"""
dlq_handler.py

Pattern Dead-Letter Queue (voir doc §4.2).

Route vers audio.uploaded.dlq :
- les payloads invalides (Base64 corrompu, JSON malforme, cle manquante)
- les echecs ASR irrecuperables apres plusieurs retries

Le message "poison" ne bloque plus la partition et l'offset peut etre
committe en toute securite (voir offset_manager.py).
"""

import json
import logging
import os
import time

from kafka import KafkaProducer

logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092"
).split(",")
TOPIC_DLQ = os.environ.get("KAFKA_TOPIC_AUDIO_DLQ", "audio.uploaded.dlq")


class DLQHandler:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=5,
        )

    def route_to_dlq(
        self,
        message_id: str | None,
        original_payload: bytes,
        error_reason: str,
        error_type: str,
        original_topic: str,
        original_partition: int,
        original_offset: int,
    ) -> None:
        """Publie le message en echec vers la DLQ avec l'en-tete d'erreur.

        error_type attendu : "invalid_payload" | "asr_unrecoverable"
        """
        try:
            decoded_payload = original_payload.decode("utf-8")
        except UnicodeDecodeError:
            decoded_payload = None  # payload binaire corrompu, on garde le raw en base64 si besoin

        dlq_record = {
            "message_id": message_id,
            "error_type": error_type,
            "error_reason": error_reason,
            "original_topic": original_topic,
            "original_partition": original_partition,
            "original_offset": original_offset,
            "failed_at": time.time(),
            "original_payload": decoded_payload,
        }

        headers = [
            ("x-error-type", error_type.encode("utf-8")),
            ("x-error-reason", error_reason[:200].encode("utf-8")),
            ("x-original-topic", original_topic.encode("utf-8")),
        ]

        future = self.producer.send(
            TOPIC_DLQ,
            key=message_id,
            value=dlq_record,
            headers=headers,
        )
        future.get(timeout=10)

        logger.warning(
            "Message route vers la DLQ (message_id=%s, error_type=%s, reason=%s)",
            message_id,
            error_type,
            error_reason,
        )

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()
