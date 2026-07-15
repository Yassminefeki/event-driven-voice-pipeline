import os
import json
from kafka import KafkaProducer


# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka1:9092,kafka2:9092,kafka3:9092"
)


class KafkaService:

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            key_serializer=lambda key: key.encode("utf-8"),
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8")
        )


    def publish(self, topic, message, key=None):
        self.producer.send(
            topic,
            value=message,
            key=key
        )
        self.producer.flush()



# ==================================================
# Message envoyé quand un audio est reçu
# Topic : audio.uploaded
# ==================================================

def build_audio_uploaded_message(
    audio_id,
    user_id,
    bucket,
    object_name,
    filename
):
    return {
        "audio_id": audio_id,
        "user_id": user_id,
        "bucket": bucket,
        "object_name": object_name,
        "filename": filename
    }



# ==================================================
# Message envoyé après transcription
# Topic : transcription.completed
#
# Nouveau format :
# {
#   audio_url,
#   transcription_initiale,
#   correction,
#   wer,
#   cer
# }
# ==================================================

def build_transcription_completed_message(
    audio_url,
    transcription_initiale,
    correction,
    wer,
    cer
):
    return {
        "audio_url": audio_url,
        "transcription_initiale": transcription_initiale,
        "correction": correction,
        "wer": wer,
        "cer": cer
    }
