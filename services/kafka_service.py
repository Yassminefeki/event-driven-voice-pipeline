
"""
Kafka producer/consumer wrapper.

Règle :
- Pour audio.uploaded, les nouveaux messages sont envoyés uniquement
  vers les partitions 0 ou 2.
- La partition 1 n'est plus utilisée pour les nouveaux vocaux.
- Les autres topics gardent le comportement Kafka normal.
"""

import json
import logging
from kafka import KafkaProducer, KafkaConsumer

from config.settings import settings

logger = logging.getLogger(__name__)


def _serialize(value: dict) -> bytes:
    return json.dumps(value).encode("utf-8")


def _key(message_id: str) -> bytes:
    return str(message_id).encode("utf-8")


class KafkaService:
    def __init__(self):
        self._producer = None  # lazy init, created on first publish

    @property
    def producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=list(settings.kafka_bootstrap_servers),
                key_serializer=lambda k: k,
                value_serializer=_serialize,
                acks="all",
            )
        return self._producer

    def publish(self, topic: str, message_id: str, payload: dict) -> None:
        """
        Publie le message dans Kafka et attend la confirmation.

        Pour audio.uploaded :
        - partition 0 ou partition 2 uniquement
        - partition 1 n'est jamais utilisée pour les nouveaux messages

        Pour les autres topics :
        - Kafka choisit normalement la partition.
        """

        key = _key(message_id)

        if topic == settings.topic_audio_uploaded:
            # Utilise uniquement les partitions 0 et 2.
            # La partition 1 est volontairement exclue.
            partition = 0 if hash(message_id) % 2 == 0 else 2

            future = self.producer.send(
                topic,
                key=key,
                value=payload,
                partition=partition,
            )
        else:
            # Comportement normal pour les autres topics
            future = self.producer.send(
                topic,
                key=key,
                value=payload,
            )

        # Attend réellement la confirmation de Kafka
        record_metadata = future.get(timeout=10)

        logger.info(
            "Published message_id=%s to %s [partition=%s offset=%s]",
            message_id,
            topic,
            record_metadata.partition,
            record_metadata.offset,
        )

    def publish_audio_uploaded(
        self,
        message_id: str,
        chat_id: int,
        user_id: int,
        telegram_file_id: str,
        audio_base64: str,
        duration_seconds: int,
        timestamp: str,
    ) -> None:

        self.publish(
            settings.topic_audio_uploaded,
            message_id,
            {
                "message_id": message_id,
                "chat_id": chat_id,
                "user_id": user_id,
                "telegram_file_id": telegram_file_id,
                "audio_base64": audio_base64,
                "duration_seconds": duration_seconds,
                "timestamp": timestamp,
            },
        )

    def publish_audio_transcribed(
        self,
        message_id: str,
        chat_id: int,
        user_id: int,
        audio_url: str,
        model_transcription: str,
        asr_model_version: str,
        confidence_score: float,
        processing_time_ms: int,
        timestamp: str,
    ) -> None:

        self.publish(
            settings.topic_audio_transcribed,
            message_id,
            {
                "message_id": message_id,
                "chat_id": chat_id,
                "user_id": user_id,
                "audio_url": audio_url,
                "model_transcription": model_transcription,
                "asr_model_version": asr_model_version,
                "confidence_score": confidence_score,
                "processing_time_ms": processing_time_ms,
                "timestamp": timestamp,
            },
        )

    def publish_transcription_corrected(
        self,
        message_id: str,
        chat_id: int,
        user_id: int,
        audio_url: str,
        model_transcription: str,
        user_correction: str,
        wer: float,
        cer: float,
        is_edited: bool,
        timestamp: str,
    ) -> None:

        self.publish(
            settings.topic_transcription_corrected,
            message_id,
            {
                "message_id": message_id,
                "chat_id": chat_id,
                "user_id": user_id,
                "audio_url": audio_url,
                "model_transcription": model_transcription,
                "user_correction": user_correction,
                "wer": wer,
                "cer": cer,
                "is_edited": is_edited,
                "timestamp": timestamp,
            },
        )

    @staticmethod
    def make_consumer(topic: str, group_id: str) -> KafkaConsumer:
        return KafkaConsumer(
            topic,
            bootstrap_servers=list(settings.kafka_bootstrap_servers),
            group_id=group_id,
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )


kafka_service = KafkaService()

