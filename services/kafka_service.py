import os
import json
from kafka import KafkaProducer


# Configuration Kafka et définition des serveurs kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka1:9092,kafka2:9092,kafka3:9092"
)

AUDIO_RAW_TOPIC = "audio.uploaded"
ASR_COMPLETED_TOPIC = "asr.completed"
TRANSCRIPTION_EVALUATED_TOPIC = "transcription.evaluated"


class KafkaService:

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            key_serializer=lambda key: key.encode("utf-8") if isinstance(key, str) else key,
            # Pas de value_serializer global : on sérialise nous-mêmes selon le
            # type de contenu (JSON pour les événements, octets bruts pour l'audio).
            # max_request_size augmenté car un vocal peut dépasser la limite
            # par défaut de 1 Mo de Kafka. À adapter selon la durée max des vocaux,
            # et à répercuter côté broker (message.max.bytes) et côté consumer
            # (max_partition_fetch_bytes dans whisper_worker.py).
            max_request_size=20 * 1024 * 1024,  # 20 Mo
        )

    def publish(self, topic, message, key=None):
        """Publie un événement JSON (métadonnées / résultats)."""
        self.producer.send(
            topic,
            value=json.dumps(message, ensure_ascii=False).encode("utf-8"),
            key=key
        )
        self.producer.flush()

    def publish_audio(self, audio_bytes, object_name, message_id, user_id, filename):
        """
        Publie l'audio brut sur AUDIO_RAW_TOPIC.

        - value  : octets bruts du fichier .wav.
        - key    : message_id pour garantir le partitionnement Kafka par message.
        - headers: métadonnées nécessaires à la consommation et à la corrélation.
        """
        headers = [
            ("message_id", str(message_id).encode("utf-8")),
            ("user_id", str(user_id).encode("utf-8")),
            ("filename", filename.encode("utf-8")),
            ("object_name", object_name.encode("utf-8")),
        ]
        self.producer.send(
            AUDIO_RAW_TOPIC,
            value=audio_bytes,
            key=str(message_id),
            headers=headers,
        )
        self.producer.flush()


# ==================================================
# Message builders pour les événements Kafka
# ==================================================

def build_audio_uploaded_message(message_id, user_id, bucket, object_name, filename):
    return {
        "topic": AUDIO_RAW_TOPIC,
        "message_id": message_id,
        "user_id": user_id,
        "bucket": bucket,
        "object_name": object_name,
        "filename": filename,
    }


def build_asr_completed_message(message_id, user_id, audio_url, transcription_initiale):
    return {
        "message_id": message_id,
        "user_id": user_id,
        "audio_url": audio_url,
        "transcription_initiale": transcription_initiale,
    }


def build_transcription_evaluated_message(
    message_id,
    user_id,
    audio_url,
    transcription_initiale,
    correction,
    wer,
    cer,
    status="completed",
):
    return {
        "message_id": message_id,
        "user_id": user_id,
        "audio_url": audio_url,
        "transcription_initiale": transcription_initiale,
        "correction": correction,
        "wer": float(wer),
        "cer": float(cer),
        "status": status,
    }
