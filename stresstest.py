#!/usr/bin/env python3
"""
stresstest.py — Test de charge end-to-end de la pipeline.

audio.uploaded -> ASR Worker -> audio.transcribed
             -> (auto-validation simulée ici)    -> transcription.corrected
             -> Elasticsearch Sink Connector     -> index Elasticsearch

Objectif : injecter du trafic (y compris en simulant un Whisper lent/en
échec) et PROUVER qu'aucun message n'est perdu : chaque message envoyé doit
finir soit indexé dans Elasticsearch, soit dans le topic audio.uploaded.dlq,
mais jamais disparaître silencieusement.

Ce script NE remplace PAS les composants réels (ASR Worker, publisher S3,
Kafka Connect ES Sink) : il suppose qu'ils tournent déjà. Il a trois modes :

  1) mock-whisper : sert un faux serveur Whisper HTTP avec injection de
     pannes contrôlées (latence, timeouts, 500, 429), pour reproduire le
     bug "Whisper bloque sous forte charge" de façon reproductible.
     -> Pointer WHISPER_ENDPOINT du worker réel vers ce serveur avant de
         lancer le test, puis (re)démarrer le worker.

  2) whisper-load : envoie une charge réelle CONCURRENTE directement vers
     l'endpoint Whisper réel, SANS passer par Kafka ni le worker. Isole la
     vraie capacité de concurrence du pod Whisper (throughput, latence,
     point de rupture) indépendamment de tout goulot du pipeline (nombre
     de partitions, worker mono-thread, etc). À utiliser AVANT le mode
     `run` pour connaître la valeur à donner à ASR_WORKER_CONCURRENCY.

  3) run : génère la charge (audio.uploaded), consomme audio.transcribed
     pour auto-valider (publie transcription.corrected), consomme
     audio.uploaded.dlq pour comptabiliser les échecs définitifs, puis
     interroge Elasticsearch pour vérifier que 100% des messages envoyés
     sont retrouvés (indexés OU en DLQ), avec mesure de latence e2e.

Usage :
    # Étape 0 (recommandé) : mesurer la vraie capacité de concurrence de
    # Whisper, sans Kafka, pour calibrer ASR_WORKER_CONCURRENCY
    python stresstest.py whisper-load --count 200 --concurrency 20 --audio-file sample.ogg

    # Terminal 1 : faux Whisper à 30% de pannes (mix timeout / 500)
    python stresstest.py mock-whisper --port 8090 --fault-rate 0.3

    # (pointer WHISPER_ENDPOINT=http://localhost:8090/transcribe sur le
    #  worker réel et le redémarrer, avant l'étape suivante)

    # Terminal 2 : injecter 500 messages à 20 msg/s
    python stresstest.py run --count 500 --rate 20 --audio-file sample.ogg
"""

import argparse
import base64
import json
import logging
import random
import statistics
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("stresstest")

STRESSTEST_START_FROM_LATEST = True


# ---------------------------------------------------------------------------
# Mode 1 : faux serveur Whisper avec injection de pannes
# ---------------------------------------------------------------------------

def cmd_mock_whisper(args: argparse.Namespace) -> None:
    fault_rate = args.fault_rate
    fault_mode = args.fault_mode  # timeout | http500 | both
    slow_seconds = args.slow_seconds

    class MockWhisperHandler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *a):
            logger.info("mock-whisper: %s", fmt % a)

        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            _ = self.rfile.read(length)

            roll = random.random()

            if roll < fault_rate:
                mode = fault_mode
                if mode == "both":
                    mode = random.choice(["timeout", "http500"])

                if mode == "timeout":
                    logger.info("mock-whisper: simulate TIMEOUT (sleep %.1fs, no response)", slow_seconds)
                    time.sleep(slow_seconds)
                    self.connection.close()
                    return

                if mode == "http500":
                    logger.info("mock-whisper: simulate HTTP 500")
                    body = json.dumps({"error": "simulated overload"}).encode()
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

            time.sleep(random.uniform(0.05, 0.3))
            body = json.dumps({
                "text": "ceci est une transcription simulée",
                "model": "whisper-mock",
                "confidence": 0.95,
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("0.0.0.0", args.port), MockWhisperHandler)
    logger.info(
        "Mock Whisper démarré sur http://0.0.0.0:%d (fault_rate=%.0f%%, mode=%s)",
        args.port, fault_rate * 100, fault_mode
    )
    logger.info(
        "-> pointer WHISPER_ENDPOINT du worker réel vers "
        "http://<host>:%d puis redémarrer le worker avant `run`.",
        args.port
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Arrêt du mock Whisper.")


# ---------------------------------------------------------------------------
# Utilitaire partagé : charger un échantillon audio
# ---------------------------------------------------------------------------

def _load_audio_bytes(args: argparse.Namespace) -> bytes:
    if args.audio_file:
        with open(args.audio_file, "rb") as f:
            return f.read()

    import subprocess
    import tempfile
    import os

    tmp_path = tempfile.mktemp(suffix=".ogg")
    try:
        subprocess.run(
            [
                "ffmpeg", "-y", "-f", "lavfi", "-i", "anullsrc=r=16000:cl=mono",
                "-t", "1", "-c:a", "libopus", tmp_path,
            ],
            check=True, capture_output=True,
        )
        with open(tmp_path, "rb") as f:
            return f.read()
    except Exception as exc:
        logger.error(
            "Impossible de générer un audio de test via ffmpeg (%s). "
            "Fournissez un vrai échantillon avec --audio-file sample.ogg",
            exc
        )
        sys.exit(1)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


# ---------------------------------------------------------------------------
# Mode 2 : charge réelle concurrente DIRECTE contre Whisper (sans Kafka)
# ---------------------------------------------------------------------------

def cmd_whisper_load(args: argparse.Namespace) -> None:
    import requests
    from config.settings import settings

    audio_bytes = _load_audio_bytes(args)
    endpoint = args.endpoint or settings.whisper_endpoint

    if not endpoint:
        logger.error(
            "Aucun endpoint Whisper fourni : passez --endpoint ou définissez "
            "WHISPER_ENDPOINT dans .env"
        )
        sys.exit(1)

    logger.info(
        "Envoi de %d requêtes réelles vers %s avec %d en concurrence",
        args.count, endpoint, args.concurrency
    )

    results = []
    lock = threading.Lock()

    def send_one(i: int):
        start = time.monotonic()
        try:
            resp = requests.post(
                endpoint,
                files={"file": (f"stresstest-{i}.ogg", audio_bytes)},
                timeout=args.timeout,
            )
            elapsed_ms = (time.monotonic() - start) * 1000
            with lock:
                results.append({
                    "i": i,
                    "status": resp.status_code,
                    "elapsed_ms": elapsed_ms,
                    "error": None if resp.status_code == 200 else f"http_{resp.status_code}",
                })
        except requests.exceptions.Timeout:
            elapsed_ms = (time.monotonic() - start) * 1000
            with lock:
                results.append({
                    "i": i,
                    "status": None,
                    "elapsed_ms": elapsed_ms,
                    "error": "timeout",
                })
        except Exception as exc:
            elapsed_ms = (time.monotonic() - start) * 1000
            with lock:
                results.append({
                    "i": i,
                    "status": None,
                    "elapsed_ms": elapsed_ms,
                    "error": str(exc),
                })

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        list(pool.map(send_one, range(args.count)))
    total_duration = time.monotonic() - t0

    ok = [r for r in results if r["status"] == 200]
    errors = [r for r in results if r["error"]]
    latencies = [r["elapsed_ms"] for r in ok]

    error_breakdown: dict = {}
    for r in errors:
        key = r["error"]
        error_breakdown[key] = error_breakdown.get(key, 0) + 1

    report = {
        "endpoint": endpoint,
        "concurrency": args.concurrency,
        "count": args.count,
        "total_duration_s": round(total_duration, 1),
        "throughput_req_per_s": round(args.count / total_duration, 2) if total_duration > 0 else None,
        "success": len(ok),
        "errors": len(errors),
        "error_breakdown": error_breakdown,
        "latency_ms_p50": round(statistics.median(latencies), 1) if latencies else None,
        "latency_ms_p95": (
            round(statistics.quantiles(latencies, n=20)[18], 1)
            if len(latencies) >= 20 else None
        ),
        "latency_ms_max": round(max(latencies), 1) if latencies else None,
    }

    print("\n" + "=" * 60)
    print(f"CHARGE RÉELLE CONCURRENTE — Whisper (concurrency={args.concurrency})")
    print("=" * 60)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print("=" * 60)

    if errors:
        print(
            f"\n⚠️  {len(errors)}/{args.count} requête(s) en échec à ce niveau de "
            f"concurrence ({args.concurrency}). Détail: {error_breakdown}"
        )
    else:
        print(
            f"\n✅ {len(ok)}/{args.count} requêtes réussies à concurrence={args.concurrency}. "
            f"Vous pouvez tenter une concurrence plus élevée pour trouver le point de rupture."
        )

    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    logger.info("Rapport écrit dans %s", args.report)


# ---------------------------------------------------------------------------
# Mode 3 : génération de charge end-to-end + vérification
# ---------------------------------------------------------------------------

@dataclass
class RunStats:
    sent_at: dict = field(default_factory=dict)         # message_id -> send timestamp (monotonic)
    transcribed_at: dict = field(default_factory=dict)    # message_id -> receive timestamp
    dlq_ids: set = field(default_factory=set)
    indexed_ids: set = field(default_factory=set)
    lock: threading.Lock = field(default_factory=threading.Lock)


def _producer_worker(audio_b64: str, rate: float, count: int, stats: RunStats) -> None:
    from services.kafka_service import kafka_service

    interval = 1.0 / rate if rate > 0 else 0.0

    def send_one(i: int):
        message_id = str(uuid.uuid4())
        with stats.lock:
            stats.sent_at[message_id] = time.monotonic()

        kafka_service.publish_audio_uploaded(
            message_id=message_id,
            chat_id=6853750236,
            user_id=6853750236,
            telegram_file_id=f"stresstest-{i}",
            audio_base64=audio_b64,
            duration_seconds=1,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        return message_id

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = []
        start = time.monotonic()
        for i in range(count):
            futures.append(pool.submit(send_one, i))
            target_time = start + i * interval
            sleep_for = target_time - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)

        for fut in futures:
            fut.result()

    logger.info("Producteur: %d messages audio.uploaded envoyés", count)


def _transcribed_consumer(stats: RunStats, stop_event: threading.Event) -> None:
    from services.kafka_service import kafka_service

    consumer = kafka_service.make_consumer(
        "audio.transcribed",
        group_id="stresstest-transcribed",
        enable_auto_commit=True,
        auto_offset_reset="latest" if STRESSTEST_START_FROM_LATEST else "earliest",
    )

    for record in consumer:
        if stop_event.is_set():
            break

        event = record.value
        message_id = event["message_id"]

        with stats.lock:
            stats.transcribed_at[message_id] = time.monotonic()

        kafka_service.publish_transcription_corrected(
            message_id=message_id,
            chat_id=event["chat_id"],
            user_id=event["user_id"],
            audio_url=event.get("audio_url", ""),
            model_transcription=event["model_transcription"],
            user_correction=event["model_transcription"],
            wer=0.0,
            cer=0.0,
            is_edited=False,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )


def _dlq_consumer(stats: RunStats, stop_event: threading.Event) -> None:
    from services.kafka_service import kafka_service, TOPIC_AUDIO_UPLOADED_DLQ

    consumer = kafka_service.make_consumer(
        TOPIC_AUDIO_UPLOADED_DLQ,
        group_id=f"stresstest-dlq-{uuid.uuid4()}",
        enable_auto_commit=True,
        auto_offset_reset="latest" if STRESSTEST_START_FROM_LATEST else "earliest",
    )

    for record in consumer:
        if stop_event.is_set():
            break

        event = record.value
        message_id = event.get("message_id")
        with stats.lock:
            stats.dlq_ids.add(message_id)

        logger.warning(
            "DLQ: message_id=%s reçu en échec définitif (%s)",
            message_id, event.get("dlq_error")
        )


def _verify_elasticsearch(stats: RunStats, expected_ids: list, timeout_seconds: float) -> None:
    from services.elastic_service import elastic_service
    from config.settings import settings

    remaining = set(expected_ids) - stats.dlq_ids
    deadline = time.monotonic() + timeout_seconds

    while remaining and time.monotonic() < deadline:
        found_this_round = set()

        for message_id in list(remaining):
            try:
                elastic_service._client.get(index=settings.elastic_index, id=message_id)
                found_this_round.add(message_id)
            except Exception:
                pass  # pas encore indexé

        if found_this_round:
            with stats.lock:
                stats.indexed_ids |= found_this_round
            remaining -= found_this_round

        if remaining:
            time.sleep(2.0)

    if remaining:
        logger.warning(
            "%d message(s) toujours introuvables dans Elasticsearch après %.0fs: %s",
            len(remaining), timeout_seconds,
            list(remaining)[:20]
        )


def cmd_run(args: argparse.Namespace) -> None:
    audio_bytes = _load_audio_bytes(args)
    audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")
    logger.info("Échantillon audio: %d bytes (base64: %d chars)", len(audio_bytes), len(audio_b64))

    stats = RunStats()
    stop_event = threading.Event()

    consumer_threads = [
        threading.Thread(target=_transcribed_consumer, args=(stats, stop_event), daemon=True),
        threading.Thread(target=_dlq_consumer, args=(stats, stop_event), daemon=True),
    ]
    for t in consumer_threads:
        t.start()

    logger.info(
        "Démarrage de la charge: %d messages @ %.1f msg/s (durée théorique: %.1fs)",
        args.count, args.rate, args.count / args.rate if args.rate > 0 else 0
    )

    t0 = time.monotonic()
    _producer_worker(audio_b64, args.rate, args.count, stats)
    send_duration = time.monotonic() - t0

    logger.info(
        "Charge envoyée en %.1fs. Attente de la fin du traitement en aval "
        "(jusqu'à %.0fs)...",
        send_duration, args.es_wait_timeout
    )

    _verify_elasticsearch(stats, list(stats.sent_at.keys()), args.es_wait_timeout)

    stop_event.set()

    sent_ids = set(stats.sent_at.keys())
    indexed_ids = stats.indexed_ids
    dlq_ids = stats.dlq_ids & sent_ids
    accounted_ids = indexed_ids | dlq_ids
    lost_ids = sent_ids - accounted_ids

    latencies_ms = []
    for mid in indexed_ids:
        if mid in stats.transcribed_at:
            latencies_ms.append((stats.transcribed_at[mid] - stats.sent_at[mid]) * 1000)

    report = {
        "sent": len(sent_ids),
        "indexed_in_elasticsearch": len(indexed_ids),
        "routed_to_dlq": len(dlq_ids),
        "accounted_for": len(accounted_ids),
        "lost_silently": len(lost_ids),
        "lost_ids_sample": list(lost_ids)[:20],
        "send_duration_seconds": round(send_duration, 1),
        "actual_send_rate_msg_per_s": round(len(sent_ids) / send_duration, 1) if send_duration > 0 else None,
        "latency_ms_p50": round(statistics.median(latencies_ms), 1) if latencies_ms else None,
        "latency_ms_p95": (
            round(statistics.quantiles(latencies_ms, n=20)[18], 1)
            if len(latencies_ms) >= 20 else None
        ),
        "latency_ms_max": round(max(latencies_ms), 1) if latencies_ms else None,
    }

    print("\n" + "=" * 60)
    print("RAPPORT DE TEST DE CHARGE")
    print("=" * 60)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print("=" * 60)

    if lost_ids:
        print(
            f"\n❌ ÉCHEC : {len(lost_ids)} message(s) perdu(s) silencieusement "
            f"(ni indexés, ni en DLQ)."
        )
    else:
        print(
            f"\n✅ SUCCÈS : 0 perte. {len(indexed_ids)} indexés, "
            f"{len(dlq_ids)} routés en DLQ (attendu si fault-rate > 0)."
        )

    with open(args.report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    logger.info("Rapport écrit dans %s", args.report)

    sys.exit(1 if lost_ids else 0)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p_mock = sub.add_parser("mock-whisper", help="Sert un faux Whisper avec injection de pannes")
    p_mock.add_argument("--port", type=int, default=8090)
    p_mock.add_argument("--fault-rate", type=float, default=0.3, help="Fraction (0-1) de requêtes en échec")
    p_mock.add_argument("--fault-mode", choices=["timeout", "http500", "both"], default="both")
    p_mock.add_argument("--slow-seconds", type=float, default=65.0, help="Durée du blocage simulé en mode timeout")
    p_mock.set_defaults(func=cmd_mock_whisper)

    p_wload = sub.add_parser(
        "whisper-load",
        help="Charge réelle concurrente DIRECTE vers Whisper (sans Kafka) — calibre ASR_WORKER_CONCURRENCY",
    )
    p_wload.add_argument("--endpoint", type=str, default=None, help="Défaut: WHISPER_ENDPOINT de .env")
    p_wload.add_argument("--count", type=int, default=100, help="Nombre total de requêtes à envoyer")
    p_wload.add_argument("--concurrency", type=int, default=10, help="Nombre de requêtes réellement en vol simultanément")
    p_wload.add_argument("--timeout", type=float, default=60.0)
    p_wload.add_argument("--audio-file", type=str, default=None)
    p_wload.add_argument("--report", type=str, default="whisper_load_report.json")
    p_wload.set_defaults(func=cmd_whisper_load)

    p_run = sub.add_parser("run", help="Génère la charge et vérifie l'absence de perte end-to-end")
    p_run.add_argument("--count", type=int, default=200, help="Nombre de messages à envoyer")
    p_run.add_argument("--rate", type=float, default=10.0, help="Débit cible en messages/seconde")
    p_run.add_argument("--audio-file", type=str, default=None, help="Échantillon .ogg réel à utiliser")
    p_run.add_argument("--es-wait-timeout", type=float, default=180.0, help="Secondes d'attente max pour la vérification ES")
    p_run.add_argument("--report", type=str, default="stresstest_report.json")
    p_run.set_defaults(func=cmd_run)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()