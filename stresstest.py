#!/usr/bin/env python3
"""
Injecteur de charge pour le pipeline ASR (bypass Telegram).

Publie directement sur le topic `audio.uploaded` en respectant EXACTEMENT
le format produit par kafka_service.publish_audio_uploaded(), afin que le
reste du pipeline (MinIO Sink Connector, ASR Worker, publisher.py) réagisse
comme en conditions réelles.

Installation :
    pip install confluent-kafka

Exemples d'utilisation :

    # Charge constante : 10 msg/s pendant 5 minutes
    python stress_test_producer.py --rate 10 --duration 300 \\
        --audio-file sample.ogg

    # Montée en charge par paliers : 1 msg/s (60s), 5 (60s), 10 (60s), 20 (60s)
    python stress_test_producer.py --steps "1:60,5:60,10:60,20:60" \\
        --audio-file sample.ogg

    # Sans fichier audio réel (bytes aléatoires) : utile pour tester la
    # résilience de Kafka / publisher.py / MinIO Sink sans solliciter
    # réellement l'API Whisper (le texte transcrit sera alors sans valeur).
    python stress_test_producer.py --rate 20 --duration 120 --audio-size-bytes 60000

IMPORTANT :
- Utilisez un vrai petit fichier .ogg (--audio-file) si vous voulez aussi
  valider le comportement réel de l'API Whisper sous charge.
- Le rate ci-dessous cible le débit de PUBLICATION sur Kafka, pas le débit
  réellement absorbé par les consumers en aval : c'est justement l'écart
  entre les deux qui vous intéresse (= le lag).
"""

import argparse
import base64
import csv
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("stress-test-producer")

# Doit correspondre au nom réel du topic dans settings.topic_audio_uploaded
TOPIC = "audio.uploaded"

_shutdown = False


def _handle_sigint(signum, frame):
    global _shutdown
    log.info("Arrêt demandé (Ctrl+C), flush en cours...")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_sigint)


def load_audio_bytes(audio_file: str | None, synthetic_size: int) -> bytes:
    if audio_file:
        with open(audio_file, "rb") as f:
            data = f.read()
        log.info("Audio réel chargé: %s (%d bytes)", audio_file, len(data))
        return data

    log.warning(
        "Aucun --audio-file fourni: génération de %d bytes aléatoires. "
        "L'API Whisper produira probablement des erreurs ou du texte sans "
        "valeur — adapté pour tester Kafka/MinIO/publisher.py, pas l'ASR.",
        synthetic_size,
    )
    return os.urandom(synthetic_size)


def build_message(audio_b64: str, user_pool: list[int], duration_range: tuple) -> dict:
    user_id = random.choice(user_pool)
    chat_id = user_id  # simplification: chat privé 1:1, comme un DM Telegram

    return {
        "message_id": str(uuid.uuid4()),
        "chat_id": chat_id,
        "user_id": user_id,
        "telegram_file_id": f"stress-{uuid.uuid4().hex[:12]}",
        "audio_base64": audio_b64,
        "duration_seconds": random.randint(*duration_range),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


class Stats:
    def __init__(self):
        self.sent = 0
        self.delivered = 0
        self.failed = 0
        self.start = time.monotonic()

    def delivery_report(self, err, msg):
        if err is not None:
            self.failed += 1
            log.error("Échec livraison: %s", err)
        else:
            self.delivered += 1

    def summary(self) -> str:
        elapsed = time.monotonic() - self.start
        rate = self.sent / elapsed if elapsed > 0 else 0
        return (
            f"sent={self.sent} delivered={self.delivered} failed={self.failed} "
            f"elapsed={elapsed:.1f}s effective_rate={rate:.2f} msg/s"
        )


def run_rate(producer, stats, csv_writer, audio_b64, user_pool, duration_range,
             rate: float, duration_seconds: float):
    """Publie à un débit cible (rate msg/s) pendant duration_seconds."""
    interval = 1.0 / rate if rate > 0 else 0
    end_time = time.monotonic() + duration_seconds
    next_send = time.monotonic()

    while time.monotonic() < end_time and not _shutdown:
        now = time.monotonic()
        if now < next_send:
            time.sleep(next_send - now)

        payload = build_message(audio_b64, user_pool, duration_range)
        send_ts = time.time()

        try:
            producer.produce(
                TOPIC,
                key=str(payload["message_id"]).encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
                callback=stats.delivery_report,
            )
            stats.sent += 1
            csv_writer.writerow([send_ts, payload["message_id"], payload["chat_id"]])
        except BufferError:
            # File d'attente locale du producer pleine: on laisse le temps
            # aux callbacks de se vider avant de continuer.
            log.warning("Producer queue pleine, poll(1) puis retry")
            producer.poll(1)
            continue

        producer.poll(0)  # sert les callbacks sans bloquer
        next_send += interval

    producer.poll(0)


def parse_steps(steps_str: str):
    """'1:60,5:60,10:60' -> [(1.0, 60.0), (5.0, 60.0), (10.0, 60.0)]"""
    steps = []
    for chunk in steps_str.split(","):
        rate_str, dur_str = chunk.split(":")
        steps.append((float(rate_str), float(dur_str)))
    return steps


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap-servers", default="kafka1:9092")
    parser.add_argument("--audio-file", default=None, help="Chemin vers un vrai .ogg court")
    parser.add_argument("--audio-size-bytes", type=int, default=50_000,
                         help="Taille des bytes synthétiques si --audio-file absent")
    parser.add_argument("--rate", type=float, default=None, help="msg/s pour une charge constante")
    parser.add_argument("--duration", type=float, default=60.0, help="durée en secondes (mode --rate)")
    parser.add_argument("--steps", default=None,
                         help="Montée en paliers, ex: '1:60,5:60,10:60,20:60' (rate:duration_s)")
    parser.add_argument("--num-users", type=int, default=50,
                         help="Taille du pool d'user_id/chat_id simulés")
    parser.add_argument("--min-duration-s", type=int, default=3, help="durée vocale simulée min")
    parser.add_argument("--max-duration-s", type=int, default=30, help="durée vocale simulée max")
    parser.add_argument("--log-csv", default="stress_test_sent.csv",
                         help="fichier CSV où logger chaque envoi (pour corréler avec le lag ensuite)")
    args = parser.parse_args()

    if not args.rate and not args.steps:
        parser.error("Fournissez --rate ou --steps")

    audio_bytes = load_audio_bytes(args.audio_file, args.audio_size_bytes)
    audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")
    user_pool = list(range(1, args.num_users + 1))
    duration_range = (args.min_duration_s, args.max_duration_s)

    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "queue.buffering.max.messages": 200_000,
        "linger.ms": 5,
    })
    stats = Stats()

    with open(args.log_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["sent_at_epoch", "message_id", "chat_id"])

        if args.steps:
            for rate, dur in parse_steps(args.steps):
                if _shutdown:
                    break
                log.info("Palier: rate=%.2f msg/s pendant %.0fs", rate, dur)
                run_rate(producer, stats, writer, audio_b64, user_pool, duration_range, rate, dur)
                log.info("Fin palier -> %s", stats.summary())
        else:
            run_rate(producer, stats, writer, audio_b64, user_pool, duration_range,
                      args.rate, args.duration)

    log.info("Flush final...")
    producer.flush(30)
    log.info("Terminé -> %s", stats.summary())
    log.info("Détail des envois: %s (à corréler avec le lag / audio.transcribed / transcription.corrected)",
              args.log_csv)


if __name__ == "__main__":
    main()