"""
Entry point: ASR Worker.

Step 6: consumes audio.uploaded.
Step 7: decodes Base64 and sends audio to Whisper API.
Step 8: receives the transcription.
Step 9: publishes audio.transcribed.

The worker does NOT download the audio from MinIO.
The audio is received directly from Kafka as Base64.

The real MinIO URL is obtained from `audio.stored`, published by the
Kafka Connect S3 Sink pipeline (audio-stored-publisher service), and
looked up here by message_id via a small in-memory cache fed by a
background consumer thread.
"""

import base64
import logging
import threading
import time
from datetime import datetime, timezone

from config.settings import settings
from services.kafka_service import kafka_service
from services.whisper_service import whisper_service


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)

TOPIC_AUDIO_STORED = getattr(settings, "topic_audio_stored", "audio.stored")

# message_id -> audio_url, fed by the background listener below
_audio_url_cache: dict = {}
_audio_url_cache_lock = threading.Lock()


def _audio_stored_listener() -> None:
    """Background thread: continuously consumes audio.stored and fills the cache."""

    consumer = kafka_service.make_consumer(
        TOPIC_AUDIO_STORED,
        group_id=f"{settings.kafka_group_id_worker}-audio-stored-cache",
    )

    logger.info("audio.stored listener started (topic=%s)", TOPIC_AUDIO_STORED)

    for record in consumer:
        try:
            event = record.value
            message_id = event.get("message_id")
            audio_url = event.get("audio_url")

            if not message_id or not audio_url:
                logger.warning("audio.stored: message incomplet ignoré: %s", event)
                continue

            with _audio_url_cache_lock:
                _audio_url_cache[message_id] = audio_url

            logger.info(
                "audio.stored cached: message_id=%s audio_url=%s",
                message_id, audio_url
            )

        except Exception:
            logger.exception("audio.stored listener: erreur de traitement d'un message")


def _get_audio_url(message_id: str, timeout_seconds: float = 15.0, poll_interval: float = 0.3) -> str:
    """
    Attend que la vraie URL MinIO soit disponible dans le cache (alimenté par
    audio.stored), avec un timeout de secours si le pipeline S3 Sink est en retard.
    """
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        with _audio_url_cache_lock:
            url = _audio_url_cache.get(message_id)
        if url:
            return url
        time.sleep(poll_interval)

    logger.warning(
        "message_id=%s: audio_url introuvable dans audio.stored après %.1fs, "
        "utilisation d'une URL de secours",
        message_id, timeout_seconds
    )
    # Fallback: on garde une trace explicite que l'URL n'a pas pu être résolue,
    # plutôt que de fabriquer une fausse URL silencieusement.
    return ""


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

    # Vraie URL MinIO, résolue via audio.stored (Kafka Connect S3 Sink pipeline)
    audio_url = _get_audio_url(message_id)

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

    listener_thread = threading.Thread(
        target=_audio_stored_listener,
        daemon=True,
    )
    listener_thread.start()

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