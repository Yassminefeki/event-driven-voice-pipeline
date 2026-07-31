#!/usr/bin/env python3
"""
stresstest.py — Test de charge intelligent End-to-End de la pipeline.

Simule de véritables utilisateurs Telegram interagissant en parallèle avec le système :
audio.uploaded -> ASR Worker -> audio.transcribed -> (Auto-Validation) -> transcription.corrected -> ES / S3

Utilisation :
  python stresstest.py smart-run --users 10 --total 50 --audio-file sample.ogg
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


# ---------------------------------------------------------------------------
# Classe de synchronisation et métriques
# ---------------------------------------------------------------------------

@dataclass
class SmartRunStats:
    sent_at: dict = field(default_factory=dict)         # message_id -> timestamp
    transcribed_events: dict = field(default_factory=dict) # message_id -> event data
    transcribed_at: dict = field(default_factory=dict)   # message_id -> timestamp
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


# ---------------------------------------------------------------------------
# Consommateurs d'écouteurs d'arrière-plan
# ---------------------------------------------------------------------------

def _transcribed_listener(stats: SmartRunStats, stop_event: threading.Event):
    """Écoute le topic audio.transcribed pour notifier les utilisateurs virtuels."""
    from services.kafka_service import kafka_service

    consumer = kafka_service.make_consumer(
        "audio.transcribed",
        group_id=f"stresstest-listener-{uuid.uuid4()}",
        enable_auto_commit=True,
    )

    while not stop_event.is_set():
        records = consumer.poll(timeout_ms=500)
        for tp, msgs in records.items():
            for record in msgs:
                stats.register_transcription(record.value)


def _dlq_listener(stats: SmartRunStats, stop_event: threading.Event):
    """Consomme audio.uploaded.dlq pour capturer les échecs définitifs."""
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
                mid = record.value.get("message_id")
                with stats.lock:
                    stats.dlq_ids.add(mid)
                logger.warning("DLQ capturé pour le message: %s", mid)


# ---------------------------------------------------------------------------
# Modèle d'Utilisateur Virtuel (Session Intelligente Séquentielle)
# ---------------------------------------------------------------------------

def simulate_user_session(user_idx: int, session_id: int, audio_b64: str, stats: SmartRunStats, args: argparse.Namespace):
    from services.kafka_service import kafka_service

    message_id = str(uuid.uuid4())
    user_id = 1000 + user_idx
    chat_id = 2000 + user_idx

    logger.info(f"[User {user_idx} | Session {session_id}] 🟢 Démarrage (message_id={message_id[:8]}...)")

    with stats.lock:
        stats.sent_at[message_id] = time.monotonic()

    # ÉTAPE 1 : Publication du message audio.uploaded
    try:
        kafka_service.publish_audio_uploaded(
            message_id=message_id,
            chat_id=chat_id,
            user_id=user_id,
            telegram_file_id=f"stresstest-user-{user_idx}-{session_id}.ogg",
            audio_base64=audio_b64,
            duration_seconds=1,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
    except Exception as exc:
        logger.error(f"[User {user_idx}] Échec envoi Kafka: {exc}")
        with stats.lock:
            stats.failed_sessions.append((message_id, f"Kafka publish error: {exc}"))
        return

    # ÉTAPE 2 : Attente active de la transcription ASR
    transcription = stats.wait_for_transcription(message_id, timeout=args.asr_timeout)
    if not transcription:
        # Vérifier s'il est tombé en DLQ entre temps
        if message_id in stats.dlq_ids:
            logger.warning(f"[User {user_idx}] Message dérouté vers DLQ (Échec ASR géré).")
        else:
            logger.error(f"[User {user_idx}] ❌ TIMEOUT: Aucune transcription reçue en {args.asr_timeout}s")
            with stats.lock:
                stats.failed_sessions.append((message_id, "ASR Timeout"))
        return

    # ÉTAPE 3 : Pause réaliste "Temps de lecture de l'utilisateur"
    time.sleep(random.uniform(0.5, 1.5))

    # ÉTAPE 4 : Envoi de la validation (transcription.corrected)
    logger.info(f"[User {user_idx}] ✅ Validation de la transcription par l'utilisateur")
    kafka_service.publish_transcription_corrected(
        message_id=message_id,
        chat_id=chat_id,
        user_id=user_id,
        audio_url=transcription.get("audio_url", ""),
        model_transcription=transcription["model_transcription"],
        user_correction=transcription["model_transcription"],
        wer=0.0,
        cer=0.0,
        is_edited=False,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )

    # ÉTAPE 5 : Vérification de la disponibilité S3 / MinIO (si URL présente)
    audio_url = transcription.get("audio_url")
    if audio_url:
        _verify_s3_availability(audio_url, user_idx, args.s3_wait_retries)


def _verify_s3_availability(url: str, user_idx: int, retries: int = 5):
    """Vérifie que MinIO / S3 a bien flushé le fichier .ogg et qu'il ne renvoie pas un NoSuchKey XML."""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.head(url, timeout=3.0)
            if resp.status_code == 200:
                return True
            elif resp.status_code == 404 or resp.status_code == 403:
                time.sleep(1.0) # Attente du S3 connector flush
        except Exception:
            time.sleep(1.0)
    logger.warning(f"[User {user_idx}] ⚠️ Fichier S3 non encore flushé/accessible à l'URL : {url}")
    return False


# ---------------------------------------------------------------------------
# Vérification Elasticsearch Finale
# ---------------------------------------------------------------------------

def _verify_elasticsearch(stats: SmartRunStats, expected_ids: set, timeout: float):
    from services.elastic_service import elastic_service
    from config.settings import settings

    remaining = set(expected_ids) - stats.dlq_ids
    deadline = time.monotonic() + timeout

    logger.info(f"Vérification de l'indexation Elasticsearch pour {len(remaining)} messages...")

    while remaining and time.monotonic() < deadline:
        found = set()
        for mid in list(remaining):
            try:
                elastic_service._client.get(index=settings.elastic_index, id=mid)
                found.add(mid)
            except Exception:
                pass

        if found:
            with stats.lock:
                stats.indexed_ids |= found
            remaining -= found

        if remaining:
            time.sleep(2.0)

    if remaining:
        logger.warning(f"{len(remaining)} message(s) introuvables dans ES après {timeout}s.")


# ---------------------------------------------------------------------------
# Commande Principale `smart-run`
# ---------------------------------------------------------------------------

def cmd_smart_run(args: argparse.Namespace):
    # Charger l'audio de test
    if args.audio_file:
        with open(args.audio_file, "rb") as f:
            audio_bytes = f.read()
    else:
        logger.error("Veuillez fournir un fichier audio avec --audio-file sample.ogg")
        sys.exit(1)

    audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")
    stats = SmartRunStats()
    stop_event = threading.Event()

    # Démarrer les threads d'écoute en arrière-plan
    t_transcribed = threading.Thread(target=_transcribed_listener, args=(stats, stop_event), daemon=True)
    t_dlq = threading.Thread(target=_dlq_listener, args=(stats, stop_event), daemon=True)
    t_transcribed.start()
    t_dlq.start()

    logger.info(f"=== TEST DE CHARGE INTELLIGENT ===")
    logger.info(f"Utilisateurs simultanés (Threads) : {args.users}")
    logger.info(f"Nombre total de messages          : {args.total}")

    t0 = time.monotonic()

    # Lancer le pool d'utilisateurs virtuels
    with ThreadPoolExecutor(max_workers=args.users) as executor:
        futures = []
        for i in range(args.total):
            user_idx = i % args.users
            session_id = i // args.users
            futures.append(executor.submit(simulate_user_session, user_idx, session_id, audio_b64, stats, args))

        for f in futures:
            f.result()

    total_execution_time = time.monotonic() - t0

    # Attente et vérification finale sur Elasticsearch
    sent_ids = set(stats.sent_at.keys())
    _verify_elasticsearch(stats, sent_ids, args.es_wait_timeout)

    stop_event.set()

    # ---------------------------------------------------------------------------
    # Génération du Rapport
    # ---------------------------------------------------------------------------
    indexed_len = len(stats.indexed_ids)
    dlq_len = len(stats.dlq_ids & sent_ids)
    failed_len = len(stats.failed_sessions)
    lost_len = len(sent_ids - (stats.indexed_ids | stats.dlq_ids))

    latencies = []
    for mid in stats.indexed_ids:
        if mid in stats.transcribed_at and mid in stats.sent_at:
            latencies.append((stats.transcribed_at[mid] - stats.sent_at[mid]) * 1000)

    report = {
        "sent": len(sent_ids),
        "indexed_in_elasticsearch": indexed_len,
        "routed_to_dlq": dlq_len,
        "failed_sessions": failed_len,
        "lost_silently": lost_len,
        "total_duration_seconds": round(total_execution_time, 2),
        "effective_throughput_msg_per_s": round(len(sent_ids) / total_execution_time, 2) if total_execution_time > 0 else 0,
        "latency_ms_p50": round(statistics.median(latencies), 1) if latencies else None,
        "latency_ms_p95": round(statistics.quantiles(latencies, n=20)[18], 1) if len(latencies) >= 20 else None,
        "latency_ms_max": round(max(latencies), 1) if latencies else None,
    }

    print("\n" + "=" * 60)
    print("RAPPORT DE TEST DE CHARGE INTELLIGENT")
    print("=" * 60)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print("=" * 60)

    if lost_len > 0 or failed_len > 0:
        print(f"\n❌ ÉCHEC : {lost_len} perdu(s) silencieusement, {failed_len} session(s) en échec.")
        sys.exit(1)
    else:
        print(f"\n✅ SUCCÈS : 0 perte. Tous les messages sont ordonnés et indexés correctement.")
        sys.exit(0)


# ---------------------------------------------------------------------------
# CLI Parser
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Test de charge intelligent de pipeline")
    subparsers = parser.add_parser_subparsers(dest="command", required=True)

    p_smart = subparsers.add_parser("smart-run", help="Lance la simulation d'utilisateurs simultanés ordonnée")
    p_smart.add_argument("--users", type=int, default=5, help="Nombre d'utilisateurs virtuels simultanés (Threads)")
    p_smart.add_argument("--total", type=int, default=20, help="Nombre total de vocaux à envoyer")
    p_smart.add_argument("--audio-file", type=str, required=True, help="Chemin vers le fichier sample.ogg")
    p_smart.add_argument("--asr-timeout", type=float, default=45.0, help="Timeout max attente ASR par vocal (s)")
    p_smart.add_argument("--es-wait-timeout", type=float, default=60.0, help="Timeout attente ES finale (s)")
    p_smart.add_argument("--s3-wait-retries", type=int, default=5, help="Tentatives de vérification S3 (1 retry/sec)")
    p_smart.set_defaults(func=cmd_smart_run)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()