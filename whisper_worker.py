"""
Entry point: ASR Worker.

Step 6: consumes audio.uploaded.
Step 7: decodes Base64 and sends audio to Whisper API.
Step 8: receives the transcription.
Step 9: publishes audio.transcribed.

The worker does NOT download the audio from MinIO.
The audio is received directly from Kafka as Base64.
"""

import base64
import logging
from datetime import datetime, timezone

from config.settings import settings
from services.kafka_service import kafka_service
from services.whisper_service import whisper_service


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)


def process_message(event: dict) -> None:

    message_id = event["message_id"]

    logger.info(
        "message_id=%s: decoding audio from Kafka",
        message_id
    )

    # Kafka → Base64 → bytes
    audio_base64 = event["audio_base64"]

    audio_bytes = base64.b64decode(audio_base64)

    logger.info(
        "message_id=%s: audio decoded successfully (%d bytes)",
        message_id,
        len(audio_bytes)
    )

    # Send directly to Whisper API
    result = whisper_service.transcribe(audio_bytes)

    # Logical MinIO location.
    # The MinIO Sink Connector creates this object independently.
    audio_url = f"s3://audio-archive/{message_id}.ogg"

    kafka_service.publish_audio_transcribed(
        message_id=message_id,
        chat_id=event["chat_id"],
        user_id=event["user_id"],
        audio_url=audio_url,
        model_transcription=result["text"],
        asr_model_version=result["model_version"],
        confidence_score=result.get("confidence_score") or 0.0,
        processing_time_ms=result["processing_time_ms"],
        timestamp=datetime.now(timezone.utc).isoformat(),
    )

    logger.info(
        "message_id=%s: transcription published successfully",
        message_id
    )


def run() -> None:

    consumer = kafka_service.make_consumer(
        settings.topic_audio_uploaded,
        group_id=settings.kafka_group_id_worker,
    )

    logger.info(
        "ASR Worker listening on topic=%s group=%s",
        settings.topic_audio_uploaded,
        settings.kafka_group_id_worker
    )

    for record in consumer:

        try:
            process_message(record.value)

        except Exception:

            logger.exception(
                "message_id=%s: processing FAILED",
                record.value.get("message_id")
            )


if __name__ == "__main__":
    run()