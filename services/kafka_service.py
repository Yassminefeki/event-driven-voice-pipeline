import json
from kafka import KafkaProducer
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    AUDIO_RAW_TOPIC,
    TRANSCRIPTION_COMPLETED_TOPIC,
    TRANSCRIPTION_CORRECTED_TOPIC,
)


class KafkaService:

    def __init__(self):
        bootstrap_list = [s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",") if s.strip()]
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_list,
            key_serializer=lambda key: key.encode("utf-8") if isinstance(key, str) else key,
            max_request_size=20 * 1024 * 1024,  # 20 MB max request size
        )

    def publish(self, topic: str, message: dict, key: str = None):
        """Publishes a JSON metadata payload."""
        payload_bytes = json.dumps(message, ensure_ascii=False).encode("utf-8")
        self.producer.send(topic, value=payload_bytes, key=key)
        self.producer.flush()

    def publish_audio(self, audio_bytes: bytes, object_name: str, message_id: str, user_id: str, bucket: str):
        """Publishes raw audio binary data with headers."""
        headers = build_audio_uploaded_headers(
            message_id=message_id,
            user_id=user_id,
            bucket=bucket,
            object_name=object_name,
        )
        self.producer.send(
            AUDIO_RAW_TOPIC,
            value=audio_bytes,
            key=str(message_id),
            headers=headers,
        )
        self.producer.flush()


# ==================================================
# Unified Builders for All Pipeline Topics
# ==================================================

def build_audio_uploaded_headers(message_id: str, user_id: str, bucket: str, object_name: str) -> list:
    return [
        ("message_id", str(message_id).encode("utf-8")),
        ("user_id", str(user_id).encode("utf-8")),
        ("bucket", str(bucket).encode("utf-8")),
        ("object_name", str(object_name).encode("utf-8")),
        ("content_type", b"audio/ogg"),
    ]


def build_audio_transcribed_message(
    message_id: str, user_id: str, audio_url: str, transcription_initiale: str, object_name: str = None
) -> dict:
    return {
        "message_id": message_id,
        "user_id": user_id,
        "audio_url": audio_url,
        "object_name": object_name,
        "transcription_initiale": transcription_initiale,
    }


def build_transcription_corrected_message(
    message_id: str,
    user_id: str,
    audio_url: str,
    transcription_initiale: str,
    transcription_corrigee: str,
    wer: float,
    cer: float,
    status: str = "completed",
) -> dict:
    return {
        "message_id": message_id,
        "user_id": user_id,
        "audio_url": audio_url,
        "transcription_initiale": transcription_initiale,
        "transcription_corrigee": transcription_corrigee,
        "wer": float(wer),
        "cer": float(cer),
        "status": status,
    }