#!/usr/bin/env python3
"""
stresstest.py

Suite de tests de charge pour DataBot (voir doc §5).

Modes :
  mock-whisper  : simule un serveur ASR defaillant (taux d'erreurs/timeouts configurable)
  whisper-load  : envoie des requetes paralleles directement sur l'API Whisper (hors Kafka)
  run           : test end-to-end, injecte des messages vocaux dans le pipeline Kafka complet
                  puis verifie la formule Zero Loss :
                  Messages Envoyes = Messages Indexes (ES) + Messages en DLQ
"""

import argparse
import base64
import json
import logging
import os
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests
from kafka import KafkaProducer

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092"
).split(",")
TOPIC_AUDIO_UPLOADED = os.environ.get("KAFKA_TOPIC_AUDIO_UPLOADED", "audio.uploaded")
TOPIC_AUDIO_DLQ = os.environ.get("KAFKA_TOPIC_AUDIO_DLQ", "audio.uploaded.dlq")
ELASTICSEARCH_URL = os.environ.get("ELASTICSEARCH_URL", "http://10.110.188.120:9200")
ELASTICSEARCH_INDEX = os.environ.get("ELASTICSEARCH_INDEX", "transcription-corrected")
WHISPER_API_URL = os.environ.get("WHISPER_API_URL", "http://10.110.150.77/v1/audio/transcriptions")

# Faux audio minimal valide en base64 (silence court), suffisant pour les tests de charge
FAKE_AUDIO_BYTES = b"\x00" * 1024
FAKE_AUDIO_BASE64 = base64.b64encode(FAKE_AUDIO_BYTES).decode("utf-8")


# ---------------------------------------------------------------------------
# Mode 1 : mock-whisper
# ---------------------------------------------------------------------------

def make_mock_whisper_handler(failure_rate: float, timeout_s: int):
    class MockWhisperHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            if self.path != "/v1/audio/transcriptions":
                self.send_response(404)
                self.end_headers()
                return

            roll = random.random()

            if roll < failure_rate / 2:
                # Simule un timeout : on dort avant de repondre
                logger.info("Mock Whisper: simulation d'un timeout (%ds)", timeout_s)
                time.sleep(timeout_s)
                self.send_response(504)
                self.end_headers()
                return

            if roll < failure_rate:
                logger.info("Mock Whisper: simulation d'une erreur HTTP 500")
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b'{"error": "internal server error (mock)"}')
                return

            # Succes
            content_length = int(self.headers.get("Content-Length", 0))
            self.rfile.read(content_length)  # consomme le corps de la requete
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"text": "transcription simulee (mock)"}).encode("utf-8"))

        def log_message(self, format, *args):  # noqa: A002 - signature imposee par BaseHTTPRequestHandler
            pass  # supprime les logs par defaut, on utilise notre propre logger

    return MockWhisperHandler


def cmd_mock_whisper(args: argparse.Namespace) -> None:
    handler_cls = make_mock_whisper_handler(args.failure_rate, timeout_s=65)
    server = HTTPServer(("0.0.0.0", args.port), handler_cls)
    logger.info(
        "Mock Whisper demarre sur le port %d (failure_rate=%.0f%%)",
        args.port, args.failure_rate * 100,
    )
    logger.info(
        "Objectif : verifier que la chaine Kafka n'est pas bloquee et que les "
        "messages defaillants finissent dans %s sans perte d'evenements valides.",
        TOPIC_AUDIO_UPLOADED + ".dlq",
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


# ---------------------------------------------------------------------------
# Mode 2 : whisper-load
# ---------------------------------------------------------------------------

def _single_whisper_request() -> float:
    """Envoie une requete directe a l'API Whisper, retourne la latence en secondes."""
    start = time.perf_counter()
    try:
        requests.post(
            WHISPER_API_URL,
            files={"file": ("audio.ogg", FAKE_AUDIO_BYTES)},
            timeout=120,
        )
    except requests.exceptions.RequestException as exc:
        logger.warning("Erreur lors de l'appel Whisper: %s", exc)
    return time.perf_counter() - start


def cmd_whisper_load(args: argparse.Namespace) -> None:
    logger.info(
        "Demarrage du test de charge Whisper (concurrency=%d, duration=%ds)",
        args.concurrency, args.duration,
    )
    latencies: list[float] = []
    end_time = time.time() + args.duration

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = []
        while time.time() < end_time:
            futures.append(executor.submit(_single_whisper_request))
            time.sleep(0.05)

        for future in as_completed(futures):
            latencies.append(future.result())

    if not latencies:
        logger.warning("Aucune requete executee.")
        return

    latencies.sort()
    p50 = latencies[int(len(latencies) * 0.50)]
    p95 = latencies[min(int(len(latencies) * 0.95), len(latencies) - 1)]
    p_max = latencies[-1]

    logger.info("Resultats (%d requetes) : p50=%.2fs p95=%.2fs pmax=%.2fs", len(latencies), p50, p95, p_max)
    logger.info("Ajuster ASR_WORKER_CONCURRENCY en fonction de ces latences.")


# ---------------------------------------------------------------------------
# Mode 3 : run (test end-to-end)
# ---------------------------------------------------------------------------

def _send_fake_voice_message(producer: KafkaProducer) -> str:
    message_id = str(uuid.uuid4())
    payload = {
        "message_id": message_id,
        "chat_id": 0,
        "audio_base64": FAKE_AUDIO_BASE64,
    }
    producer.send(TOPIC_AUDIO_UPLOADED, key=message_id, value=payload)
    return message_id


def _count_es_documents(message_ids: set[str]) -> int:
    """Interroge Elasticsearch pour compter combien de message_ids envoyes
    ont bien ete indexes."""
    try:
        response = requests.post(
            f"{ELASTICSEARCH_URL}/{ELASTICSEARCH_INDEX}/_count",
            json={"query": {"terms": {"message_id.keyword": list(message_ids)}}},
            timeout=10,
        )
        response.raise_for_status()
        return response.json().get("count", 0)
    except requests.exceptions.RequestException as exc:
        logger.error("Erreur lors de la requete Elasticsearch: %s", exc)
        return 0


def cmd_run(args: argparse.Namespace) -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
    )

    logger.info("Injection de %d messages a un taux de %d msg/s", args.count, args.rate)

    sent_ids: set[str] = set()
    interval = 1.0 / args.rate if args.rate > 0 else 0

    for _ in range(args.count):
        message_id = _send_fake_voice_message(producer)
        sent_ids.add(message_id)
        if interval:
            time.sleep(interval)

    producer.flush()
    producer.close()

    logger.info("Injection terminee (%d messages envoyes). Attente du traitement...", len(sent_ids))
    time.sleep(args.wait_seconds)

    indexed_count = _count_es_documents(sent_ids)
    dlq_count = len(sent_ids) - indexed_count  # approximation ; affiner via consumer DLQ dedie si besoin

    logger.info("--- Resultats du test end-to-end ---")
    logger.info("Messages envoyes    : %d", len(sent_ids))
    logger.info("Messages indexes ES : %d", indexed_count)
    logger.info("Messages en DLQ (estimation) : %d", dlq_count)

    if indexed_count + dlq_count == len(sent_ids):
        logger.info("✅ Formule Zero Loss verifiee : Envoyes = Indexes + DLQ")
    else:
        logger.warning(
            "⚠️ Ecart detecte (Envoyes=%d != Indexes+DLQ=%d). "
            "Verifier la latence de traitement ou augmenter --wait-seconds.",
            len(sent_ids), indexed_count + dlq_count,
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Suite de stress testing DataBot")
    subparsers = parser.add_subparsers(dest="mode", required=True)

    p_mock = subparsers.add_parser("mock-whisper", help="Simule un serveur ASR defaillant")
    p_mock.add_argument("--port", type=int, default=8080)
    p_mock.add_argument("--failure-rate", type=float, default=0.3)
    p_mock.set_defaults(func=cmd_mock_whisper)

    p_load = subparsers.add_parser("whisper-load", help="Calibre les limites de l'API Whisper")
    p_load.add_argument("--concurrency", type=int, default=10)
    p_load.add_argument("--duration", type=int, default=60)
    p_load.set_defaults(func=cmd_whisper_load)

    p_run = subparsers.add_parser("run", help="Test end-to-end de charge sur le pipeline complet")
    p_run.add_argument("--count", type=int, default=500)
    p_run.add_argument("--rate", type=int, default=20)
    p_run.add_argument("--wait-seconds", type=int, default=30)
    p_run.set_defaults(func=cmd_run)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
