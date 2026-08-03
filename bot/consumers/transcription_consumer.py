"""
transcription_consumer.py

Etape 11 : consomme audio.transcribed, resout l'URL audio via object_name_store
(alimente par audio.stored), puis envoie le clavier interactif a l'utilisateur.

Concu pour tourner dans un thread/tache asyncio dediee, en parallele du
polling Telegram (main.py).
"""

import asyncio
import json
import logging
import os

from kafka import KafkaConsumer
from telegram.ext import Application

from bot.db.object_name_store import ObjectNameStore
from bot.handlers.validation_handler import send_transcription_for_review

logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092"
).split(",")
TOPIC_AUDIO_TRANSCRIBED = os.environ.get("KAFKA_TOPIC_AUDIO_TRANSCRIBED", "audio.transcribed")
GROUP_ID = os.environ.get("KAFKA_CONSUMER_GROUP_BOT", "bot-consumer-group")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://10.110.188.120:9000")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "audio-archive")


def _build_audio_url(object_name: str, bucket: str) -> str:
    return f"{MINIO_ENDPOINT}/{bucket}/{object_name}"


async def run_transcription_consumer(application: Application) -> None:
    """Boucle de consommation bloquante executee dans un thread separe,
    qui reinjecte les envois de messages dans la boucle asyncio de l'app."""
    store = ObjectNameStore()

    consumer = KafkaConsumer(
        TOPIC_AUDIO_TRANSCRIBED,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,  # lecture non critique : au pire on renvoie 2x le clavier
        auto_offset_reset="earliest",
    )

    loop = asyncio.get_event_loop()

    for record in consumer:
        event = record.value
        message_id = event["message_id"]
        chat_id = event["chat_id"]
        transcription = event["transcription"]

        resolved = store.resolve(message_id)
        audio_url = None
        if resolved:
            audio_url = _build_audio_url(resolved["object_name"], resolved["bucket"])
        else:
            logger.warning(
                "object_name introuvable pour message_id=%s (audio.stored pas encore recu ?)",
                message_id,
            )

        asyncio.run_coroutine_threadsafe(
            send_transcription_for_review(
                application=application,
                chat_id=chat_id,
                message_id=message_id,
                transcription=transcription,
                audio_url=audio_url,
            ),
            loop,
        )
