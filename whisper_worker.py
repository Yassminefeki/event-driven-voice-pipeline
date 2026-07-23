import os
import logging
import asyncio
from kafka import KafkaConsumer

from config.settings import (
    BUCKET_NAME,
    MINIO_ENDPOINT,
    MINIO_SECURE,
)
from services.kafka_service import (
    KafkaService,
    KAFKA_BOOTSTRAP_SERVERS,
    AUDIO_TRANSCRIBED_TOPIC,
)
from services.whisper_service import WhisperService

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

kafka_service = KafkaService()


def main():
    bootstrap_list = [s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",") if s.strip()]

    consumer = KafkaConsumer(
        "audio.uploaded",
        bootstrap_servers=bootstrap_list,
        group_id="whisper-worker-group",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )

    logger.info(">>> [Whisper Worker] En écoute sur le topic 'audio.uploaded'...")

    for message in consumer:
        temp_raw_name = None
        try:
            headers = {key: value.decode("utf-8") for key, value in message.headers}
            message_id = headers.get("message_id") or (message.key.decode("utf-8") if message.key else "")
            user_id = headers.get("user_id", "")
            object_name = headers.get("object_name", f"{message_id}.ogg")
            if not message.value:
                continue

            temp_raw_name = f"temp_raw_{message_id}{os.path.splitext(object_name)[1] or '.ogg'}"
            with open(temp_raw_name, "wb") as f:
                f.write(message.value)

            transcription = asyncio.run(WhisperService.transcribe(temp_raw_name))

            payload = {
                "message_id": message_id,
                "user_id": user_id,
                "audio_url": f"{'https' if MINIO_SECURE else 'http'}://{MINIO_ENDPOINT}/{BUCKET_NAME}/{object_name}",
                "object_name": object_name,
                "transcription_initiale": transcription,
            }
            kafka_service.publish(AUDIO_TRANSCRIBED_TOPIC, payload, key=message_id)
            logger.info(f"✅ Traité avec succès pour message_id={message_id}")

        except Exception as e:
            logger.error(f"❌ Erreur: {e}")

        finally:
            if temp_raw_name and os.path.exists(temp_raw_name):
                os.remove(temp_raw_name)


if __name__ == "__main__":
    main()

