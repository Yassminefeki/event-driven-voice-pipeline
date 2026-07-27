"""
Entry point: ASR Worker.
Step 6: consumes audio.uploaded (in parallel with the MinIO Sink Connector).
Step 7: sends audio to the Whisper API.
Step 8: receives the transcription.
Step 9: publishes audio.transcribed.

Stateless and horizontally scalable — run as many instances as you want,
as long as `audio.uploaded` has at least that many partitions.
"""
import logging
from datetime import datetime, timezone

from config.settings import settings
from services.kafka_service import kafka_service
from services.minio_service import minio_service
from services.whisper_service import whisper_service

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def process_message(event: dict) -> None:
    message_id = event["message_id"]
    logger.info("message_id=%s: fetching audio for transcription", message_id)

    audio_bytes = minio_service.download_audio(message_id)
    result = whisper_service.transcribe(audio_bytes)

    kafka_service.publish_audio_transcribed(
        message_id=message_id,
        chat_id=event["chat_id"],
        user_id=event["user_id"],
        audio_url=event["audio_url"],
        model_transcription=result["text"],
        asr_model_version=result["model_version"],
        confidence_score=result.get("confidence_score") or 0.0,
        processing_time_ms=result["processing_time_ms"],
        timestamp=datetime.now(timezone.utc).isoformat(),
    )
    logger.info("message_id=%s: transcription published successfully", message_id)


def run() -> None:
    consumer = kafka_service.make_consumer(
        settings.topic_audio_uploaded,
        group_id=settings.kafka_group_id_worker,
    )
    logger.info("ASR Worker listening on topic=%s group=%s",
                settings.topic_audio_uploaded, settings.kafka_group_id_worker)

    for record in consumer:
        try:
            process_message(record.value)
        except Exception:
            # Never swallow silently — a past incident had a worker log
            # "success" despite a downstream publish failure.
            logger.exception("message_id=%s: processing FAILED", record.value.get("message_id"))


if __name__ == "__main__":
    run()
