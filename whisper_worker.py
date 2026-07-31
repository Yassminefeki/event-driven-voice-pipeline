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

CORRECTIF (perte de messages sous forte charge) :
- Le consumer audio.uploaded utilise désormais enable_auto_commit=False.
  L'offset n'est committé qu'après un des deux dénouements suivants :
    1. succès complet du traitement (audio.transcribed publié) ;
    2. échec définitif classé -> message envoyé sur audio.uploaded.dlq.
  Si une erreur transitoire survient pendant la publication en aval
  (ex. Kafka indisponible un instant), on NE committe PAS : le message
  sera re-livré automatiquement (at-least-once), au lieu d'être perdu.
- L'appel Whisper lui-même a désormais un retry avec backoff exponentiel
  (voir whisper_service.py) avant d'être considéré en échec définitif.

Note sur le débit : ce worker reste volontairement mono-thread pour
garantir un commit d'offset strictement ordonné (donc correct). Pour
augmenter le débit sous test de charge, scaler horizontalement (plusieurs
instances de ce worker, un consumer group commun) : audio.uploaded n'utilise
que les partitions 0 et 2, donc jusqu'à 2 instances peuvent traiter en
parallèle sans changement de code.
"""

import base64
import binascii
import logging
import threading
import time
from datetime import datetime, timezone

from config.settings import settings
from services.kafka_service import kafka_service
from services.whisper_service import whisper_service, WhisperTranscriptionError


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
    """Background thread: continuously consumes audio.stored and fills the cache.

    Ce consumer reste en auto-commit : il n'a pas d'effet de bord métier à
    protéger (c'est un simple cache de lecture), donc pas de risque de perte
    si un offset est committé avant traitement.
    """

    consumer = kafka_service.make_consumer(
        TOPIC_AUDIO_STORED,
        group_id=f"{settings.kafka_group_id_worker}-audio-stored-cache",
        enable_auto_commit=True,
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
    return ""


def process_message(event: dict) -> None:
    """
    Peut lever :
    - WhisperTranscriptionError : échec définitif de la transcription
      (retries épuisés) -> l'appelant route vers la DLQ.
    - KeyError / binascii.Error : message malformé (poison pill) ->
      l'appelant route vers la DLQ (retenter ne servirait à rien).
    - toute autre exception : considérée transitoire (ex. Kafka down
      pendant la publication) -> l'appelant NE COMMIT PAS, le message
      sera re-livré.
    """

    message_id = event["message_id"]

    logger.info("message_id=%s: decoding audio from Kafka", message_id)

    audio_base64 = event["audio_base64"]
    audio_bytes = base64.b64decode(audio_base64, validate=True)

    logger.info(
        "message_id=%s: audio decoded successfully (%d bytes)",
        message_id, len(audio_bytes)
    )

    # Peut lever WhisperTranscriptionError après retries internes.
    result = whisper_service.transcribe(audio_bytes)

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

    logger.info("message_id=%s: transcription published successfully", message_id)


def run() -> None:

    listener_thread = threading.Thread(
        target=_audio_stored_listener,
        daemon=True,
    )
    listener_thread.start()

    # enable_auto_commit=False : commit explicite ci-dessous, uniquement
    # après succès ou envoi en DLQ. C'est LE correctif contre la perte de
    # messages sous forte charge.
    consumer = kafka_service.make_consumer(
        settings.topic_audio_uploaded,
        group_id=settings.kafka_group_id_worker,
        enable_auto_commit=False,
    )

    logger.info(
        "ASR Worker listening on topic=%s group=%s",
        settings.topic_audio_uploaded,
        settings.kafka_group_id_worker
    )

    for record in consumer:

        event = record.value
        message_id = event.get("message_id", "unknown")

        try:
            process_message(event)
            kafka_service.commit_offset(consumer, record)

        except WhisperTranscriptionError as exc:
            # Échec définitif classé (retries Whisper épuisés) : on ne perd
            # pas le message, on le route en DLQ, puis on avance.
            logger.error(
                "message_id=%s: transcription Whisper définitivement échouée -> DLQ",
                message_id
            )
            kafka_service.publish_audio_uploaded_dlq(event, str(exc))
            kafka_service.commit_offset(consumer, record)

        except (KeyError, binascii.Error, ValueError) as exc:
            # Message malformé (poison pill) : le retenter ne changera rien,
            # on le route en DLQ pour ne pas bloquer indéfiniment la partition.
            logger.error(
                "message_id=%s: message malformé (%s) -> DLQ",
                message_id, exc
            )
            kafka_service.publish_audio_uploaded_dlq(event, f"malformed: {exc}")
            kafka_service.commit_offset(consumer, record)

        except Exception:
            # Erreur imprévue / transitoire (ex. Kafka indisponible lors de
            # la publication en aval) : on NE COMMIT PAS. Le message sera
            # re-livré (at-least-once) au prochain poll ou après restart.
            logger.exception(
                "message_id=%s: processing FAILED (erreur transitoire, "
                "message NON committé -> sera re-livré)",
                message_id
            )


if __name__ == "__main__":
    run()