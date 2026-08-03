"""
Kafka producer/consumer wrapper.

Règle :
- Pour audio.uploaded, les nouveaux messages sont envoyés uniquement
  vers les partitions 0 ou 2.
- La partition 1 n'est plus utilisée pour les nouveaux vocaux.
- Les autres topics gardent le comportement Kafka normal.

CORRECTIF (perte de messages sous forte charge) :
- Les consumers "critiques" (traitement métier) utilisent désormais
  enable_auto_commit=False. L'offset n'est commité par l'appelant
  qu'après succès du traitement (voir whisper_worker.py), ou après
  publication dans le topic DLQ dédié en cas d'échec définitif.
- Avant ce correctif, enable_auto_commit=True committait l'offset sur
  une base temporelle, indépendamment du succès du traitement : un
  message qui échouait (ex. timeout Whisper sous forte charge) était
  quand même marqué comme "lu" et donc perdu définitivement.
"""

import json
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata

from config.settings import settings

logger = logging.getLogger(__name__)

# Topic de dead-letter pour audio.uploaded : si le traitement échoue de façon
# définitive (retries épuisés), le message y est publié tel quel plutôt que
# d'être perdu silencieusement. Peut être surchargé via settings si présent.
TOPIC_AUDIO_UPLOADED_DLQ = getattr(
    settings, "topic_audio_uploaded_dlq", "audio.uploaded.dlq"
)


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

    def publish_audio_uploaded_dlq(self, original_event: dict, error: str) -> None:
        """
        Publie un message audio.uploaded qui a définitivement échoué
        (retries épuisés côté worker) vers le topic dead-letter, afin
        de ne jamais le perdre silencieusement. Le message garde son
        message_id pour rester traçable / rejouable manuellement.
        """
        message_id = original_event.get("message_id", "unknown")

        payload = dict(original_event)
        payload["dlq_error"] = error

        self.publish(TOPIC_AUDIO_UPLOADED_DLQ, message_id, payload)

        logger.error(
            "message_id=%s: envoyé en DLQ (%s) après échec définitif",
            message_id, TOPIC_AUDIO_UPLOADED_DLQ
        )

    @staticmethod
    def make_consumer(
        topic: str,
        group_id: str,
        enable_auto_commit: bool = False,
        auto_offset_reset: str = "earliest",
    ) -> KafkaConsumer:
        """
        enable_auto_commit=False par défaut : l'appelant DOIT committer
        explicitement via commit_offset() (ou consumer.commit()) une fois
        le traitement terminé avec succès. Ne pas committer un message qui
        a échoué -> il sera re-livré au prochain poll (at-least-once).

        Les consumers non critiques (ex. simple cache de lecture, sans
        effet de bord métier à protéger) peuvent explicitement repasser
        à enable_auto_commit=True s'ils le souhaitent.
        """
        return KafkaConsumer(
            topic,
            bootstrap_servers=list(settings.kafka_bootstrap_servers),
            group_id=group_id,
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            max_poll_interval_ms=600000,  # marge de sécurité si Whisper est lent sous charge
        )

    @staticmethod
    def commit_offset(consumer: KafkaConsumer, record) -> None:
        """
        Committe explicitement l'offset du message donné (offset+1, comme
        l'exige l'API Kafka) sur sa partition, une fois le traitement (ou
        l'envoi en DLQ) terminé avec succès.
        """
        tp = TopicPartition(record.topic, record.partition)
        consumer.commit({tp: OffsetAndMetadata(record.offset + 1, None)})
    
    @staticmethod
    def commit_offsets(consumer: KafkaConsumer, offsets: dict) -> None:
        """
        Committe plusieurs partitions en une seule fois, en fin de lot concurrent.

        offsets : dict[TopicPartition, int] -> offset "suivant" à committer
        (c'est-à-dire offset du dernier message sûr traité + 1), déjà calculé par
        l'appelant selon la règle : ne jamais committer au-delà d'un message en
        échec transitoire dans le lot (voir whisper_worker.py::run()).

        Une partition absente du dict n'est simplement pas committée ce tour-ci
        (ex: le tout premier message de cette partition dans le lot a déjà
        échoué transitoirement -> rien de "sûr" à committer).
        """
        if not offsets:
            return

        to_commit = {
            tp: OffsetAndMetadata(next_offset, None)
            for tp, next_offset in offsets.items()
        }
        consumer.commit(to_commit)

        for tp, next_offset in offsets.items():
            logger.info(
                "Batch commit: topic=%s partition=%s -> offset=%d",
                tp.topic, tp.partition, next_offset
            )


kafka_service = KafkaService()