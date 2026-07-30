
#!/usr/bin/env python3
import json
import logging
import subprocess
import time
from confluent_kafka import Consumer, Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("audio-stored-publisher")

KAFKA_BOOTSTRAP = "kafka1:9092"
SOURCE_TOPIC = "audio.uploaded"
DEST_TOPIC = "audio.stored"

MINIO_BASE_URL = "http://10.110.188.120:9000"
BUCKET = "audio-archive"
MC_ALIAS = "local"  # doit correspondre à `mc alias set local ...`

# Correction : on laisse davantage de temps à Kafka Connect
# pour créer l'objet dans MinIO avant de modifier son Content-Type.
CONTENT_TYPE_FIX_MAX_RETRIES = 30
CONTENT_TYPE_FIX_RETRY_DELAY = 1.0  # secondes

consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "audio-stored-publisher",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}

producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)
consumer.subscribe([SOURCE_TOPIC])


def build_object_key(partition: int, offset: int) -> str:
    padded_offset = f"{offset:010d}"
    return f"topics/{SOURCE_TOPIC}/partition={partition}/{SOURCE_TOPIC}+{partition}+{padded_offset}.ogg"


def build_audio_url(object_key: str) -> str:
    return f"{MINIO_BASE_URL}/{BUCKET}/{object_key}"


def fix_content_type(object_key: str) -> bool:
    """
    Attend que l'objet existe dans MinIO puis force son Content-Type
    à audio/ogg.
    """
    mc_path = f"{MC_ALIAS}/{BUCKET}/{object_key}"

    for attempt in range(1, CONTENT_TYPE_FIX_MAX_RETRIES + 1):

        # Vérifier que l'objet existe
        stat = subprocess.run(
            ["mc", "stat", mc_path],
            capture_output=True,
            text=True
        )

        if stat.returncode != 0:
            log.info(
                f"Objet {object_key} pas encore prêt "
                f"(tentative {attempt}/{CONTENT_TYPE_FIX_MAX_RETRIES})"
            )
            time.sleep(CONTENT_TYPE_FIX_RETRY_DELAY)
            continue

        # Vérifier le Content-Type actuel
        if "Content-Type: audio/ogg" in stat.stdout:
            log.info(
                f"Content-Type déjà correct pour {object_key}: audio/ogg"
            )
            return True

        # Corriger le Content-Type
        cp = subprocess.run(
            [
                "mc",
                "cp",
                "--attr",
                "Content-Type=audio/ogg",
                mc_path,
                mc_path
            ],
            capture_output=True,
            text=True
        )

        if cp.returncode == 0:
            log.info(
                f"Content-Type corrigé pour {object_key}: audio/ogg"
            )
            return True

        log.error(
            f"Échec mc cp --attr pour {object_key}: "
            f"{cp.stderr.strip()}"
        )

        time.sleep(CONTENT_TYPE_FIX_RETRY_DELAY)

    log.error(
        f"Impossible de corriger le Content-Type pour {object_key} "
        f"après {CONTENT_TYPE_FIX_MAX_RETRIES} tentatives"
    )

    return False


def delivery_report(err, msg):
    if err is not None:
        log.error(f"Échec publication audio.stored: {err}")
    else:
        log.info(
            f"Publié sur {msg.topic()} "
            f"[partition={msg.partition()} offset={msg.offset()}]"
        )


def main():
    log.info("Démarrage du publisher audio.stored...")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                log.error(f"Erreur consumer: {msg.error()}")
                continue

            try:
                value = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                log.error(
                    f"Message audio.uploaded invalide (JSON): {e}"
                )
                continue

            message_id = value.get("message_id")
            user_id = value.get("user_id")
            chat_id = value.get("chat_id")

            if not message_id:
                log.error(
                    f"message_id manquant, message ignoré: {value}"
                )
                continue

            # Même logique qu'avant :
            # l'emplacement MinIO dépend de la partition et de l'offset Kafka.
            object_key = build_object_key(
                msg.partition(),
                msg.offset()
            )

            audio_url = build_audio_url(object_key)

            # CORRECTION PRINCIPALE :
            # si le Content-Type n'est pas corrigé,
            # on ne publie PAS audio.stored.
            if not fix_content_type(object_key):
                log.error(
                    f"Content-Type non corrigé pour {object_key}. "
                    f"audio.stored ne sera pas publié."
                )
                continue

            payload = {
                "message_id": message_id,
                "user_id": user_id,
                "chat_id": chat_id,
                "audio_url": audio_url,
            }

            producer.produce(
                DEST_TOPIC,
                key=str(message_id).encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
                callback=delivery_report,
            )

            producer.poll(0)

            log.info(f"audio.stored -> {payload}")

    except KeyboardInterrupt:
        log.info("Arrêt demandé.")

    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()

