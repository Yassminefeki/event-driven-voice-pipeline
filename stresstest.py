
#!/usr/bin/env python3
"""
Injecteur de charge pour le pipeline ASR.

Publie directement sur le topic audio.uploaded avec le même format
que KafkaService.publish_audio_uploaded().

Exemples :

    # Test simple : 1 message/s pendant 30 secondes
    python3 stresstest.py

    # 5 messages/s pendant 60 secondes
    python3 stresstest.py --rate 5 --duration 60

    # Avec un vrai fichier audio
    python3 stresstest.py --rate 1 --duration 30 --audio-file test.ogg

    # Avec des données synthétiques
    python3 stresstest.py --rate 1 --duration 30 --audio-size-bytes 10000

    # Test par paliers
    python3 stresstest.py --steps "1:30,5:30,10:30"
"""

import argparse
import base64
import csv
import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

log = logging.getLogger("stress-test-producer")


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

        log.info(
            "Audio réel chargé: %s (%d bytes)",
            audio_file,
            len(data),
        )

        return data

    log.warning(
        "Aucun fichier audio fourni. Génération de %d bytes synthétiques.",
        synthetic_size,
    )

    return os.urandom(synthetic_size)


def build_message(
    audio_b64: str,
    user_pool: list[int],
    duration_range: tuple,
) -> dict:

    user_id = random.choice(user_pool)
    chat_id = user_id

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

            log.info(
                "Kafka -> topic=%s partition=%s offset=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    def summary(self) -> str:

        elapsed = time.monotonic() - self.start

        rate = (
            self.sent / elapsed
            if elapsed > 0
            else 0
        )

        return (
            f"sent={self.sent} "
            f"delivered={self.delivered} "
            f"failed={self.failed} "
            f"elapsed={elapsed:.1f}s "
            f"effective_rate={rate:.2f} msg/s"
        )


def run_rate(
    producer,
    stats,
    csv_writer,
    audio_b64,
    user_pool,
    duration_range,
    rate,
    duration_seconds,
):

    if rate <= 0:
        raise ValueError("Le rate doit être supérieur à 0.")

    interval = 1.0 / rate

    end_time = time.monotonic() + duration_seconds
    next_send = time.monotonic()

    while (
        time.monotonic() < end_time
        and not _shutdown
    ):

        now = time.monotonic()

        if now < next_send:
            time.sleep(next_send - now)

        payload = build_message(
            audio_b64,
            user_pool,
            duration_range,
        )

        send_ts = time.time()

        try:

            producer.produce(
                TOPIC,
                key=str(payload["message_id"]).encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
                callback=stats.delivery_report,
            )

            stats.sent += 1

            csv_writer.writerow(
                [
                    send_ts,
                    payload["message_id"],
                    payload["chat_id"],
                ]
            )

        except BufferError:

            log.warning(
                "Producer queue pleine, poll(1) puis retry"
            )

            producer.poll(1)

            continue

        producer.poll(0)

        next_send += interval

    producer.poll(0)


def parse_steps(steps_str):

    steps = []

    for chunk in steps_str.split(","):

        rate_str, dur_str = chunk.split(":")

        rate = float(rate_str)
        duration = float(dur_str)

        if rate <= 0:
            raise ValueError(
                "Le rate doit être supérieur à 0."
            )

        if duration <= 0:
            raise ValueError(
                "La durée doit être supérieure à 0."
            )

        steps.append(
            (rate, duration)
        )

    return steps


def main():

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--bootstrap-servers",
        default="kafka1:9092",
        help="Serveurs Kafka",
    )

    parser.add_argument(
        "--audio-file",
        default=None,
        help="Chemin vers un fichier .ogg réel",
    )

    parser.add_argument(
        "--audio-size-bytes",
        type=int,
        default=10_000,
        help="Taille des données synthétiques",
    )

    # MODIFICATION PRINCIPALE :
    # Le script possède maintenant un rate par défaut.
    parser.add_argument(
        "--rate",
        type=float,
        default=1.0,
        help="Messages/seconde (défaut: 1)",
    )

    parser.add_argument(
        "--duration",
        type=float,
        default=30.0,
        help="Durée en secondes (défaut: 30)",
    )

    parser.add_argument(
        "--steps",
        default=None,
        help='Paliers, exemple: "1:30,5:30,10:30"',
    )

    parser.add_argument(
        "--num-users",
        type=int,
        default=50,
        help="Nombre d'utilisateurs simulés",
    )

    parser.add_argument(
        "--min-duration-s",
        type=int,
        default=3,
    )

    parser.add_argument(
        "--max-duration-s",
        type=int,
        default=30,
    )

    parser.add_argument(
        "--log-csv",
        default="stress_test_sent.csv",
    )

    args = parser.parse_args()

    # Vérifications

    if args.rate <= 0:
        parser.error("--rate doit être supérieur à 0")

    if args.duration <= 0:
        parser.error("--duration doit être supérieur à 0")

    if args.num_users <= 0:
        parser.error("--num-users doit être supérieur à 0")

    if args.min_duration_s > args.max_duration_s:
        parser.error(
            "--min-duration-s doit être <= --max-duration-s"
        )

    # Audio

    audio_bytes = load_audio_bytes(
        args.audio_file,
        args.audio_size_bytes,
    )

    audio_b64 = base64.b64encode(
        audio_bytes
    ).decode("utf-8")

    user_pool = list(
        range(1, args.num_users + 1)
    )

    duration_range = (
        args.min_duration_s,
        args.max_duration_s,
    )

    # Producer Kafka

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "queue.buffering.max.messages": 200_000,
            "linger.ms": 5,
        }
    )

    stats = Stats()

    log.info(
        "Kafka bootstrap servers: %s",
        args.bootstrap_servers,
    )

    log.info(
        "Topic: %s",
        TOPIC,
    )

    log.info(
        "Rate: %.2f message/s",
        args.rate,
    )

    log.info(
        "Durée: %.1f secondes",
        args.duration,
    )

    with open(
        args.log_csv,
        "w",
        newline="",
    ) as f:

        writer = csv.writer(f)

        writer.writerow(
            [
                "sent_at_epoch",
                "message_id",
                "chat_id",
            ]
        )

        if args.steps:

            log.info(
                "Mode paliers: %s",
                args.steps,
            )

            for rate, duration in parse_steps(
                args.steps
            ):

                if _shutdown:
                    break

                log.info(
                    "Palier: %.2f msg/s pendant %.0fs",
                    rate,
                    duration,
                )

                run_rate(
                    producer,
                    stats,
                    writer,
                    audio_b64,
                    user_pool,
                    duration_range,
                    rate,
                    duration,
                )

                log.info(
                    "Fin palier -> %s",
                    stats.summary(),
                )

        else:

            log.info(
                "Test simple: %.2f msg/s pendant %.0fs",
                args.rate,
                args.duration,
            )

            run_rate(
                producer,
                stats,
                writer,
                audio_b64,
                user_pool,
                duration_range,
                args.rate,
                args.duration,
            )

    log.info("Flush final...")

    producer.flush(30)

    log.info(
        "Terminé -> %s",
        stats.summary(),
    )

    log.info(
        "CSV créé: %s",
        args.log_csv,
    )


if __name__ == "__main__":
    main()
