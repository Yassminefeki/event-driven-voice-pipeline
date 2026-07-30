
#!/usr/bin/env python3
"""
stresstest.py
=============
Test de charge intelligent de toute la pipeline :

Telegram/Bot (simulé)
        ↓
Kafka audio.uploaded
        ↓
Kafka Connect → MinIO
        ↓
publisher.py → audio.stored
        ↓
Whisper Worker → audio.transcribed
        ↓
correction → transcription.corrected
        ↓
Kafka Connect → Elasticsearch

Le script ne modifie PAS la partition Kafka.
Kafka choisit automatiquement la partition en fonction de la clé message_id.

Exemples :

1) Test simple :
   python3 stresstest.py --rate 1 --duration 30 --audio-file sample.ogg

2) Test progressif :
   python3 stresstest.py --steps "1:30,2:30,5:30,10:30" --audio-file sample.ogg

3) Test rapide :
   python3 stresstest.py --rate 5 --duration 60 --audio-file sample.ogg
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


# ============================================================
# CONFIGURATION
# ============================================================

TOPIC = "audio.uploaded"

DEFAULT_BOOTSTRAP = "kafka1:9092"

_shutdown = False


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

log = logging.getLogger("pipeline-stresstest")


# ============================================================
# CTRL+C
# ============================================================

def handle_sigint(signum, frame):
    global _shutdown

    if not _shutdown:
        log.warning("Arrêt demandé avec Ctrl+C...")
        _shutdown = True


signal.signal(signal.SIGINT, handle_sigint)


# ============================================================
# AUDIO
# ============================================================

def load_audio(audio_file=None, synthetic_size=10000):
    """
    Charge un vrai fichier OGG si fourni.

    Sinon génère des bytes synthétiques.
    ATTENTION :
    les bytes synthétiques permettent de tester Kafka/MinIO,
    mais Whisper ne pourra pas forcément les traiter.
    """

    if audio_file:
        if not os.path.exists(audio_file):
            raise FileNotFoundError(
                f"Fichier audio introuvable : {audio_file}"
            )

        with open(audio_file, "rb") as f:
            data = f.read()

        log.info(
            "Fichier audio réel chargé : %s (%d bytes)",
            audio_file,
            len(data),
        )

        return data

    log.warning(
        "Aucun fichier audio fourni. "
        "Génération de %d bytes synthétiques.",
        synthetic_size,
    )

    return os.urandom(synthetic_size)


# ============================================================
# MESSAGE
# ============================================================

def build_message(audio_b64, user_id):
    """
    Construit EXACTEMENT le format attendu par
    kafka_service.publish_audio_uploaded().
    """

    message_id = str(uuid.uuid4())

    return {
        "message_id": message_id,
        "chat_id": user_id,
        "user_id": user_id,
        "telegram_file_id": f"stress-{uuid.uuid4().hex[:12]}",
        "audio_base64": audio_b64,
        "duration_seconds": random.randint(3, 30),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ============================================================
# DELIVERY REPORT
# ============================================================

class Statistics:

    def __init__(self):
        self.sent = 0
        self.delivered = 0
        self.failed = 0

        self.partition_count = {
            0: 0,
            1: 0,
            2: 0,
        }

        self.start_time = time.monotonic()

    def delivery_report(self, err, msg):

        if err is not None:
            self.failed += 1

            log.error(
                "Kafka delivery FAILED : %s",
                err,
            )

            return

        self.delivered += 1

        partition = msg.partition()

        if partition not in self.partition_count:
            self.partition_count[partition] = 0

        self.partition_count[partition] += 1

        log.info(
            "Kafka -> topic=%s partition=%s offset=%s",
            msg.topic(),
            partition,
            msg.offset(),
        )

    def summary(self):

        elapsed = time.monotonic() - self.start_time

        effective_rate = (
            self.sent / elapsed
            if elapsed > 0
            else 0
        )

        return (
            "\n"
            "=============================\n"
            "       TEST SUMMARY\n"
            "=============================\n"
            f"Sent       : {self.sent}\n"
            f"Delivered  : {self.delivered}\n"
            f"Failed     : {self.failed}\n"
            f"Elapsed    : {elapsed:.1f}s\n"
            f"Rate       : {effective_rate:.2f} msg/s\n"
            "\n"
            "Kafka partitions:\n"
            f"  partition 0 : {self.partition_count.get(0, 0)}\n"
            f"  partition 1 : {self.partition_count.get(1, 0)}\n"
            f"  partition 2 : {self.partition_count.get(2, 0)}\n"
            "=============================\n"
        )


# ============================================================
# PUBLISH
# ============================================================

def send_message(
    producer,
    stats,
    csv_writer,
    audio_b64,
    user_pool,
):
    """
    Publie un message sur audio.uploaded.
    """

    user_id = random.choice(user_pool)

    payload = build_message(
        audio_b64,
        user_id,
    )

    message_id = payload["message_id"]

    try:

        producer.produce(
            TOPIC,

            # IMPORTANT :
            # même logique que kafka_service.py
            key=message_id.encode("utf-8"),

            value=json.dumps(payload).encode("utf-8"),

            callback=stats.delivery_report,
        )

        stats.sent += 1

        csv_writer.writerow([
            time.time(),
            message_id,
            user_id,
        ])

    except BufferError:

        log.warning(
            "Buffer Kafka plein. Attente..."
        )

        producer.poll(1)

        return False

    producer.poll(0)

    return True


# ============================================================
# RATE CONSTANT
# ============================================================

def run_rate(
    producer,
    stats,
    csv_writer,
    audio_b64,
    user_pool,
    rate,
    duration,
):
    """
    Exécute un débit constant.
    """

    log.info(
        "Test : %.2f msg/s pendant %.1f secondes",
        rate,
        duration,
    )

    interval = 1.0 / rate

    end_time = time.monotonic() + duration

    next_send = time.monotonic()

    while (
        time.monotonic() < end_time
        and not _shutdown
    ):

        now = time.monotonic()

        if now < next_send:
            time.sleep(next_send - now)

        send_message(
            producer,
            stats,
            csv_writer,
            audio_b64,
            user_pool,
        )

        next_send += interval

    producer.poll(0)


# ============================================================
# STEPS
# ============================================================

def parse_steps(value):

    result = []

    for item in value.split(","):

        rate, duration = item.split(":")

        result.append(
            (
                float(rate),
                float(duration),
            )
        )

    return result


# ============================================================
# MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description="Test de charge intelligent de toute la pipeline."
    )

    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP,
    )

    parser.add_argument(
        "--audio-file",
        default=None,
        help="Vrai fichier .ogg pour tester Whisper.",
    )

    parser.add_argument(
        "--audio-size-bytes",
        type=int,
        default=10000,
        help="Taille des bytes synthétiques.",
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Nombre de messages par seconde.",
    )

    parser.add_argument(
        "--duration",
        type=float,
        default=30,
        help="Durée du test en secondes.",
    )

    parser.add_argument(
        "--steps",
        default=None,
        help="Exemple : 1:30,2:30,5:30,10:30",
    )

    parser.add_argument(
        "--num-users",
        type=int,
        default=50,
    )

    parser.add_argument(
        "--log-csv",
        default="stress_test_sent.csv",
    )

    args = parser.parse_args()

    # --------------------------------------------------------
    # Vérification arguments
    # --------------------------------------------------------

    if args.rate is None and args.steps is None:

        parser.error(
            "Fournissez --rate ou --steps"
        )

    if args.rate is not None and args.rate <= 0:

        parser.error(
            "--rate doit être > 0"
        )

    # --------------------------------------------------------
    # Audio
    # --------------------------------------------------------

    audio_bytes = load_audio(
        args.audio_file,
        args.audio_size_bytes,
    )

    audio_b64 = base64.b64encode(
        audio_bytes
    ).decode("utf-8")

    # --------------------------------------------------------
    # Users
    # --------------------------------------------------------

    user_pool = list(
        range(
            1,
            args.num_users + 1
        )
    )

    # --------------------------------------------------------
    # Kafka Producer
    # --------------------------------------------------------

    log.info(
        "Kafka bootstrap servers: %s",
        args.bootstrap_servers,
    )

    log.info(
        "Topic: %s",
        TOPIC,
    )

    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,

        # évite que le producer soit rapidement saturé
        "queue.buffering.max.messages": 200000,

        "linger.ms": 5,

        # améliore la fiabilité
        "acks": "all",
    })

    stats = Statistics()

    # --------------------------------------------------------
    # CSV
    # --------------------------------------------------------

    with open(
        args.log_csv,
        "w",
        newline="",
    ) as csv_file:

        writer = csv.writer(csv_file)

        writer.writerow([
            "sent_at",
            "message_id",
            "user_id",
        ])

        # ----------------------------------------------------
        # MODE RATE
        # ----------------------------------------------------

        if args.rate is not None:

            run_rate(
                producer,
                stats,
                writer,
                audio_b64,
                user_pool,
                args.rate,
                args.duration,
            )

        # ----------------------------------------------------
        # MODE STEPS
        # ----------------------------------------------------

        else:

            steps = parse_steps(
                args.steps
            )

            for rate, duration in steps:

                if _shutdown:
                    break

                log.info(
                    "========== NOUVEAU PALIER =========="
                )

                log.info(
                    "Rate = %.2f msg/s",
                    rate,
                )

                log.info(
                    "Durée = %.1f secondes",
                    duration,
                )

                run_rate(
                    producer,
                    stats,
                    writer,
                    audio_b64,
                    user_pool,
                    rate,
                    duration,
                )

    # --------------------------------------------------------
    # FLUSH
    # --------------------------------------------------------

    log.info(
        "Flush final Kafka..."
    )

    remaining = producer.flush(30)

    if remaining > 0:

        log.warning(
            "%d messages encore dans la queue Kafka.",
            remaining,
        )

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    log.info(
        stats.summary()
    )

    log.info(
        "CSV créé : %s",
        args.log_csv,
    )

    log.info(
        "Le test Kafka est terminé."
    )

    log.info(
        "Vérifiez maintenant :"
    )

    log.info(
        "  audio.uploaded"
    )

    log.info(
        "  MinIO audio-archive"
    )

    log.info(
        "  audio.stored"
    )

    log.info(
        "  audio.transcribed"
    )

    log.info(
        "  transcription.corrected"
    )

    log.info(
        "  Elasticsearch / Kibana"
    )


if __name__ == "__main__":
    main()

