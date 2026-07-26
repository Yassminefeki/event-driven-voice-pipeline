import os
import json
import logging
import asyncio
from kafka import KafkaConsumer

from config.settings import BUCKET_NAME, MINIO_ENDPOINT, MINIO_SECURE
from services.kafka_service import (
    KafkaService,
    KAFKA_BOOTSTRAP_SERVERS,
    AUDIO_TRANSCRIBED_TOPIC,
    build_audio_transcribed_message,
)
from services.whisper_service import WhisperService

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

kafka_service = KafkaService()


def get_minio_url(object_name: str) -> str:
    protocol = "https" if MINIO_SECURE else "http"
    clean_endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    return f"{protocol}://{clean_endpoint}/{BUCKET_NAME}/{object_name}"


def main():
    bootstrap_list = [s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",") if s.strip()]

    consumer = KafkaConsumer(
        "audio.uploaded",
        bootstrap_servers=bootstrap_list,
        group_id="whisper-worker-group",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )

    logger.info(">>> [ASR Worker] Listening on topic 'audio.uploaded'...")

    for message in consumer:
        temp_raw_name = None
        message_id = ""
        try:
            headers = {key: value.decode("utf-8") for key, value in message.headers}
            message_id = headers.get("message_id") or (message.key.decode("utf-8") if message.key else "")
            user_id = headers.get("user_id", "")
            object_name = headers.get("object_name", f"{message_id}.ogg")
            
            if not message.value:
                continue

            # Write temp audio file for Whisper API input
            temp_raw_name = f"temp_{object_name}"
            with open(temp_raw_name, "wb") as f:
                f.write(message.value)

            # Transcribe audio
            transcription = asyncio.run(WhisperService.transcribe(temp_raw_name))

            # Build stateless URL directly from object_name
            audio_url = get_minio_url(object_name)

            payload = build_audio_transcribed_message(
                message_id=message_id,
                user_id=user_id,
                audio_url=audio_url,
                object_name=object_name,
                transcription_initiale=transcription,
            )

            # Publish result to audio.transcribed
            payload_json = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            future = kafka_service.producer.send(
                AUDIO_TRANSCRIBED_TOPIC, 
                value=payload_json, 
                key=message_id.encode("utf-8") if isinstance(message_id, str) else message_id
            )
            record_metadata = future.get(timeout=10)
            logger.info(f"✅ Published to {record_metadata.topic} for message_id={message_id}")

        except Exception as e:
            logger.error("Error processing message_id=%s: %s", message_id, e)

        finally:
            if temp_raw_name and os.path.exists(temp_raw_name):
                os.remove(temp_raw_name)


if __name__ == "__main__":
    main()