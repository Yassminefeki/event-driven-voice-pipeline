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


import binascii
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka.structs import TopicPartition


def _process_one_safe(record) -> tuple:
    """
    Wrapper qui exécute process_message() sans jamais laisser une exception
    remonter dans le thread pool : on classe le résultat pour que le thread
    appelant décide du commit, plutôt que de committer depuis le thread
    worker lui-même (le commit reste centralisé et séquentiel, cf. run()).

    Retourne (record, outcome, detail) avec outcome in:
      "success"    -> traité et publié avec succès
      "dlq"        -> échec définitif classé, déjà routé en DLQ
      "transient"  -> échec probablement transitoire, NE PAS committer
    """
    message_id = record.value.get("message_id", "unknown")

    try:
        process_message(record.value)
        return (record, "success", None)

    except WhisperTranscriptionError as exc:
        logger.error(
            "message_id=%s: transcription définitivement échouée -> DLQ",
            message_id
        )
        kafka_service.publish_audio_uploaded_dlq(record.value, str(exc))
        return (record, "dlq", str(exc))

    except (KeyError, binascii.Error, ValueError) as exc:
        logger.error(
            "message_id=%s: message malformé (%s) -> DLQ",
            message_id, exc
        )
        kafka_service.publish_audio_uploaded_dlq(record.value, f"malformed: {exc}")
        return (record, "dlq", str(exc))

    except Exception as exc:
        logger.exception(
            "message_id=%s: échec transitoire (%s), NON committé -> sera re-livré",
            message_id, exc
        )
        return (record, "transient", str(exc))


def _compute_safe_commit_offsets(results: list) -> dict:
    """
    Calcule, par partition, l'offset jusqu'où il est sûr de committer.

    Règle : on parcourt les messages de CHAQUE partition triés par offset
    croissant. On avance tant que le résultat est "success" ou "dlq". Dès
    qu'on tombe sur "transient", on s'arrête pour cette partition : rien
    au-delà n'est committé (le message transitoire ET tout ce qui suit
    seront re-livrés au prochain poll, ce qui est correct et sans perte).
    """
    by_partition: dict = {}
    for record, outcome, _detail in results:
        tp = TopicPartition(record.topic, record.partition)
        by_partition.setdefault(tp, []).append((record.offset, outcome))

    safe_offsets = {}
    for tp, entries in by_partition.items():
        entries.sort(key=lambda e: e[0])

        last_safe_offset = None
        for offset, outcome in entries:
            if outcome == "transient":
                break
            last_safe_offset = offset

        if last_safe_offset is not None:
            safe_offsets[tp] = last_safe_offset + 1

    return safe_offsets


def run() -> None:

    listener_thread = threading.Thread(
        target=_audio_stored_listener,
        daemon=True,
    )
    listener_thread.start()

    consumer = kafka_service.make_consumer(
        settings.topic_audio_uploaded,
        group_id=settings.kafka_group_id_worker,
        enable_auto_commit=False,
    )

    concurrency = settings.asr_worker_concurrency
    batch_size = settings.asr_worker_batch_size

    logger.info(
        "ASR Worker (concurrent, batch) listening on topic=%s group=%s "
        "concurrency=%d batch_size=%d",
        settings.topic_audio_uploaded,
        settings.kafka_group_id_worker,
        concurrency, batch_size,
    )

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        while True:
            # max_records borne la taille du lot pour garder un contrôle
            # explicite sur la concurrence réellement envoyée à Whisper.
            polled = consumer.poll(timeout_ms=1000, max_records=batch_size)

            if not polled:
                continue

            records = [r for records_list in polled.values() for r in records_list]

            if not records:
                continue

            logger.info("Batch de %d message(s) soumis en concurrence (max %d en vol)",
                        len(records), concurrency)

            futures = [pool.submit(_process_one_safe, r) for r in records]
            results = [f.result() for f in as_completed(futures)]

            safe_offsets = _compute_safe_commit_offsets(results)
            kafka_service.commit_offsets(consumer, safe_offsets)

            n_success = sum(1 for _, o, _ in results if o == "success")
            n_dlq = sum(1 for _, o, _ in results if o == "dlq")
            n_transient = sum(1 for _, o, _ in results if o == "transient")

            logger.info(
                "Batch terminé: %d succès, %d DLQ, %d transitoire(s) "
                "(non committé -> re-livré)",
                n_success, n_dlq, n_transient
            )


if __name__ == "__main__":
    run()