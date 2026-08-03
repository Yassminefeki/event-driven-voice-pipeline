"""
whisper_worker.py

ASR Worker : consomme audio.uploaded, transcrit via l'API Whisper, publie sur
audio.transcribed (succes) ou audio.uploaded.dlq (echec irrecuperable).

Points cles (voir doc §4) :
- enable_auto_commit=False : le commit est manuel, calcule via
  offset_manager.compute_safe_commit_offsets pour garantir Zero Message Loss.
- ThreadPoolExecutor : traitement parallele d'un lot, concurrence pilotee par
  ASR_WORKER_CONCURRENCY.
- Retry avec Exponential Backoff sur erreurs HTTP 5xx/429/reseau transitoires.
- Single Long Timeout (~900s) sur les timeouts HTTP, pour ne pas aggraver
  l'engorgement de Whisper en pic de charge (pas de retry immediat = pas de
  requetes doublonnees).
"""

import base64
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import requests
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition

from dlq_handler import DLQHandler
from offset_manager import ProcessingStatus, RecordResult, compute_safe_commit_offsets

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092"
).split(",")
TOPIC_AUDIO_UPLOADED = os.environ.get("KAFKA_TOPIC_AUDIO_UPLOADED", "audio.uploaded")
TOPIC_AUDIO_TRANSCRIBED = os.environ.get("KAFKA_TOPIC_AUDIO_TRANSCRIBED", "audio.transcribed")
GROUP_ID = os.environ.get("KAFKA_CONSUMER_GROUP_ASR", "asr-worker-group")

WHISPER_API_URL = os.environ.get("WHISPER_API_URL", "http://10.110.150.77/v1/audio/transcriptions")
CONCURRENCY = int(os.environ.get("ASR_WORKER_CONCURRENCY", "5"))
LONG_TIMEOUT_S = int(os.environ.get("ASR_WORKER_TIMEOUT_S", "900"))
MAX_RETRIES = int(os.environ.get("ASR_WORKER_MAX_RETRIES", "3"))
POLL_TIMEOUT_MS = int(os.environ.get("ASR_WORKER_POLL_TIMEOUT_MS", "5000"))
BATCH_SIZE = int(os.environ.get("ASR_WORKER_BATCH_SIZE", "20"))

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


@dataclass
class ConsumedRecord:
    partition: int
    offset: int
    key: str | None
    raw_value: bytes
    payload: dict | None  # None si JSON invalide


def _parse_payload(raw_value: bytes) -> tuple[dict | None, str | None]:
    """Parse le JSON du message et valide les cles attendues.
    Retourne (payload, error_reason). error_reason est None si tout est valide."""
    try:
        payload = json.loads(raw_value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        return None, f"JSON invalide: {exc}"

    for required_key in ("message_id", "chat_id", "audio_base64"):
        if required_key not in payload:
            return None, f"Cle manquante dans le payload: {required_key}"

    try:
        base64.b64decode(payload["audio_base64"], validate=True)
    except Exception as exc:
        return None, f"Base64 invalide: {exc}"

    return payload, None


def _call_whisper_with_retry(audio_bytes: bytes) -> tuple[str | None, str | None]:
    """Appelle l'API Whisper avec retry + exponential backoff sur erreurs
    transitoires, et Single Long Timeout sur timeout HTTP.

    Retourne (transcription, error_reason). error_reason est None en cas de succes.
    """
    backoff_s = 1.0

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(
                WHISPER_API_URL,
                files={"file": ("audio.ogg", audio_bytes)},
                timeout=LONG_TIMEOUT_S,
            )
        except requests.exceptions.Timeout:
            # Timeout : ne PAS retry immediatement (voir doc §4.3), on considere
            # que la requete a echoue definitivement pour ce lot -> DLQ.
            # Un rejeu manuel/planifie pourra reprendre le message depuis la DLQ.
            return None, f"Timeout Whisper apres {LONG_TIMEOUT_S}s (tentative {attempt})"
        except requests.exceptions.RequestException as exc:
            # Erreur reseau transitoire -> retry avec backoff
            logger.warning("Erreur reseau Whisper (tentative %d/%d): %s", attempt, MAX_RETRIES, exc)
            if attempt == MAX_RETRIES:
                return None, f"Erreur reseau apres {MAX_RETRIES} tentatives: {exc}"
            time.sleep(backoff_s)
            backoff_s *= 2
            continue

        if response.status_code == 200:
            return response.json().get("text", ""), None

        if response.status_code in RETRYABLE_STATUS_CODES:
            logger.warning(
                "Whisper a repondu %d (tentative %d/%d), retry avec backoff",
                response.status_code, attempt, MAX_RETRIES,
            )
            if attempt == MAX_RETRIES:
                return None, f"HTTP {response.status_code} apres {MAX_RETRIES} tentatives"
            time.sleep(backoff_s)
            backoff_s *= 2
            continue

        # Erreur non retryable (ex: 400 Bad Request) -> echec immediat
        return None, f"HTTP {response.status_code} (non retryable): {response.text[:200]}"

    return None, "Nombre max de tentatives atteint"


def _process_single_record(record: ConsumedRecord) -> tuple[ConsumedRecord, ProcessingStatus, dict | None, str | None]:
    """Traite un message individuel. Retourne (record, status, transcribed_event, error_info)."""
    payload, parse_error = _parse_payload(record.raw_value)

    if parse_error:
        return record, ProcessingStatus.FAILED_DLQ, None, parse_error

    audio_bytes = base64.b64decode(payload["audio_base64"])
    transcription, asr_error = _call_whisper_with_retry(audio_bytes)

    if asr_error:
        return record, ProcessingStatus.FAILED_DLQ, None, asr_error

    transcribed_event = {
        "message_id": payload["message_id"],
        "chat_id": payload["chat_id"],
        "transcription": transcription,
    }
    return record, ProcessingStatus.SUCCESS, transcribed_event, None


def run_worker() -> None:
    consumer = KafkaConsumer(
        TOPIC_AUDIO_UPLOADED,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=False,  # commit manuel obligatoire (voir doc §4.1)
        auto_offset_reset="earliest",
        max_poll_records=BATCH_SIZE,
    )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
    )

    dlq_handler = DLQHandler()

    logger.info(
        "ASR Worker demarre (concurrency=%d, timeout=%ds, max_retries=%d)",
        CONCURRENCY, LONG_TIMEOUT_S, MAX_RETRIES,
    )

    try:
        while True:
            poll_result = consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=BATCH_SIZE)
            if not poll_result:
                continue

            batch: list[ConsumedRecord] = []
            for _tp, records in poll_result.items():
                for r in records:
                    batch.append(
                        ConsumedRecord(
                            partition=r.partition,
                            offset=r.offset,
                            key=r.key.decode("utf-8") if r.key else None,
                            raw_value=r.value,
                            payload=None,
                        )
                    )

            results: list[RecordResult] = []

            with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
                futures = {executor.submit(_process_single_record, rec): rec for rec in batch}

                for future in as_completed(futures):
                    record, status, transcribed_event, error_info = future.result()

                    if status == ProcessingStatus.SUCCESS:
                        producer.send(
                            TOPIC_AUDIO_TRANSCRIBED,
                            key=transcribed_event["message_id"],
                            value=transcribed_event,
                        )
                        logger.info(
                            "Transcription publiee (message_id=%s)", transcribed_event["message_id"]
                        )
                    else:
                        message_id = None
                        try:
                            message_id = json.loads(record.raw_value.decode("utf-8")).get("message_id")
                        except Exception:
                            pass

                        dlq_handler.route_to_dlq(
                            message_id=message_id,
                            original_payload=record.raw_value,
                            error_reason=error_info or "Erreur inconnue",
                            error_type="asr_unrecoverable" if message_id else "invalid_payload",
                            original_topic=TOPIC_AUDIO_UPLOADED,
                            original_partition=record.partition,
                            original_offset=record.offset,
                        )

                    results.append(
                        RecordResult(partition=record.partition, offset=record.offset, status=status)
                    )

            producer.flush()

            safe_offsets = compute_safe_commit_offsets(results)
            if safe_offsets:
                commit_dict = {
                    TopicPartition(TOPIC_AUDIO_UPLOADED, partition): offset
                    for partition, offset in safe_offsets.items()
                }
                consumer.commit(offsets=commit_dict)
                logger.info("Offsets committes: %s", safe_offsets)

    finally:
        consumer.close()
        producer.close()
        dlq_handler.close()


if __name__ == "__main__":
    run_worker()
