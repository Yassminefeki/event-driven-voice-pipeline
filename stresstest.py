#!/usr/bin/env python3
"""
stresstest.py — Module de Stress Test & Charge Extrême (Whisper / Pipeline Audio)
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
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("stresstest")


@dataclass
class SmartRunStats:
    sent_at: dict = field(default_factory=dict)
    transcribed_events: dict = field(default_factory=dict)
    transcribed_at: dict = field(default_factory=dict)
    dlq_ids: set = field(default_factory=set)
    indexed_ids: set = field(default_factory=set)
    failed_sessions: list = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)
    transcribed_events_cond: threading.Condition = field(default_factory=threading.Condition)

    def register_transcription(self, event: dict):
        mid = event.get("message_id")
        with self.transcribed_events_cond:
            self.transcribed_events[mid] = event
            self.transcribed_at[mid] = time.monotonic()
            self.transcribed_events_cond.notify_all()

    def wait_for_transcription(self, message_id: str, timeout: float = 45.0) -> dict:
        deadline = time.monotonic() + timeout
        with self.transcribed_events_cond:
            while message_id not in self.transcribed_events:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self.transcribed_events_cond.wait(timeout=remaining)
            return self.transcribed_events[message_id]


def _transcribed_listener(stats: SmartRunStats, stop_event: threading.Event):
    from services.kafka_service import kafka_service

    # Force auto_offset_reset='latest' pour ignorer l'historique du topic
    consumer = kafka_service.make_consumer(
        "audio.transcribed",
        group_id=f"stresstest-listener-{uuid.uuid4()}",
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )

    while not stop_event.is_set():
        records = consumer.poll(timeout_ms=500)
        for tp, msgs in records.items():
            for record in msgs:
                if record.value:
                    mid = record.value.get("message_id")
                    # On ne comptabilise que les messages de cette session
                    with stats.lock:
                        if mid in stats.sent_at:
                            stats.register_transcription(record.value)
def _dlq_listener(stats: SmartRunStats, stop_event: threading.Event):
    from services.kafka_service import kafka_service, TOPIC_AUDIO_UPLOADED_DLQ

    consumer = kafka_service.make_consumer(
        TOPIC_AUDIO_UPLOADED_DLQ,
        group_id=f"stresstest-dlq-{uuid.uuid4()}",
        enable_auto_commit=True,
    )

    while not stop_event.is_set():
        records = consumer.poll(timeout_ms=500)
        for tp, msgs in records.items():
            for record in msgs:
                if record.value:
                    mid = record.value.get("message_id")
                    with stats.lock:
                        stats.dlq_ids.add(mid)


def simulate_user_session(user_idx: int, session_id: int, audio_b64: str, stats: SmartRunStats, args: argparse.Namespace):
    from services.kafka_service import kafka_service

    message_id = str(uuid.uuid4())
    user_id = 1000 + user_idx
    chat_id = 2000 + user_idx

    with stats.lock:
        stats.sent_at[message_id] = time.monotonic()

    try:
        kafka_service.publish_audio_uploaded(
            message_id=message_id,
            chat_id=chat_id,
            user_id=user_id,
            telegram_file_id=f"stresstest-{message_id[:8]}.ogg",
            audio_base64=audio_b64,
            duration_seconds=1,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
    except Exception as exc:
        with stats.lock:
            stats.failed_sessions.append((message_id, f"Kafka error: {exc}"))
        return

    # Si on est en mode burst pur, on ne bloque pas pour attendre la réponse ASR
    if getattr(args, "burst", False):
        return

    transcription = stats.wait_for_transcription(message_id, timeout=args.asr_timeout)
    if not transcription:
        if message_id not in stats.dlq_ids:
            with stats.lock:
                stats.failed_sessions.append((message_id, "ASR Timeout"))
        return

    time.sleep(random.uniform(0.1, 0.5))

    kafka_service.publish_transcription_corrected(
        message_id=message_id,
        chat_id=chat_id,
        user_id=user_id,
        audio_url=transcription.get("audio_url", ""),
        model_transcription=transcription.get("model_transcription", ""),
        user_correction=transcription.get("model_transcription", ""),
        wer=0.0,
        cer=0.0,
        is_edited=False,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


def _verify_elasticsearch_wildcard(stats: SmartRunStats, expected_ids: set, timeout: float):
    """Vérification globale sur tous les index Elasticsearch (*) avec fallback search payload."""
    from services.elastic_service import elastic_service

    remaining = set(expected_ids) - stats.dlq_ids
    deadline = time.monotonic() + timeout

    logger.info(f"Recherche globale dans Elasticsearch (Index '*') pour {len(remaining)} messages...")

    while remaining and time.monotonic() < deadline:
        found = set()
        for mid in list(remaining):
            try:
                # Query wildcard globale
                res = elastic_service._client.search(
                    index="_all",
                    body={
                        "query": {
                            "query_string": {
                                "query": f'"{mid}"'
                            }
                        }
                    },
                    size=1
                )
                hits = res.get("hits", {}).get("hits", [])
                if hits:
                    found.add(mid)
            except Exception:
                pass

        if found:
            with stats.lock:
                stats.indexed_ids |= found
            remaining -= found

        if remaining:
            time.sleep(2.0)


def cmd_smart_run(args: argparse.Namespace):
    if not args.audio_file:
        logger.error("Veuillez fournir --audio-file sample.ogg")
        sys.exit(1)

    with open(args.audio_file, "rb") as f:
        audio_b64 = base64.b64encode(f.read()).decode("utf-8")

    stats = SmartRunStats()
    stop_event = threading.Event()

    t_transcribed = threading.Thread(target=_transcribed_listener, args=(stats, stop_event), daemon=True)
    t_dlq = threading.Thread(target=_dlq_listener, args=(stats, stop_event), daemon=True)
    t_transcribed.start()
    t_dlq.start()

    logger.info("🔥 === DÉMARRAGE DU TEST DE CHARGE ===")
    logger.info(f"Threads Simultanés : {args.users} | Total Messages : {args.total}")

    t0 = time.monotonic()

    with ThreadPoolExecutor(max_workers=args.users) as executor:
        futures = []
        for i in range(args.total):
            user_idx = i % args.users
            session_id = i // args.users
            futures.append(executor.submit(simulate_user_session, user_idx, session_id, audio_b64, stats, args))

        for f in futures:
            f.result()

    total_execution_time = time.monotonic() - t0

    sent_ids = set(stats.sent_at.keys())
    
    # Mode verification ES
    if not args.skip_es_check:
        _verify_elasticsearch_wildcard(stats, sent_ids, args.es_wait_timeout)

    stop_event.set()

    indexed_len = len(stats.indexed_ids)
    dlq_len = len(stats.dlq_ids & sent_ids)
    failed_len = len(stats.failed_sessions)
    
    latencies = [
        (stats.transcribed_at[mid] - stats.sent_at[mid]) * 1000
        for mid in stats.transcribed_at if mid in stats.sent_at
    ]

    report = {
        "sent": len(sent_ids),
        "transcribed_by_asr": len(stats.transcribed_events),
        "indexed_in_elasticsearch": indexed_len if not args.skip_es_check else "CHECK_SKIPPED",
        "routed_to_dlq": dlq_len,
        "failed_sessions": failed_len,
        "total_duration_seconds": round(total_execution_time, 2),
        "throughput_msg_per_s": round(len(sent_ids) / total_execution_time, 2) if total_execution_time > 0 else 0,
        "latency_ms_p50": round(statistics.median(latencies), 1) if latencies else None,
        "latency_ms_p95": round(statistics.quantiles(latencies, n=20)[18], 1) if len(latencies) >= 20 else None,
    }

    print("\n" + "=" * 60)
    print("RAPPORT DE STRESS TEST PIPELINE")
    print("=" * 60)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Stress test extrema pipeline ASR")
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_smart = subparsers.add_parser("smart-run", help="Lance la simulation de charge")
    p_smart.add_argument("--users", type=int, default=10, help="Nombre de threads simultanés")
    p_smart.add_argument("--total", type=int, default=50, help="Nombre total de vocaux")
    p_smart.add_argument("--audio-file", type=str, required=True, help="Fichier sample.ogg")
    p_smart.add_argument("--asr-timeout", type=float, default=60.0, help="Timeout ASR (s)")
    p_smart.add_argument("--es-wait-timeout", type=float, default=30.0, help="Timeout ES (s)")
    p_smart.add_argument("--burst", action="store_true", help="Injection ultra rapide sans attente ASR")
    p_smart.add_argument("--skip-es-check", action="store_true", help="Ignore l'inspection ES si déjà vérifié visuellement")
    p_smart.set_defaults(func=cmd_smart_run)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()