import os
import json
import logging
import asyncio
from kafka import KafkaConsumer

from services.kafka_service import (
    KafkaService,
    KAFKA_BOOTSTRAP_SERVERS,
    AUDIO_TRANSCRIBED_TOPIC,
    build_audio_transcribed_message,
)
from services.object_name_store import ObjectNameStore
from services.whisper_service import WhisperService

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

kafka_service = KafkaService()
object_name_store = ObjectNameStore()


def main():
    bootstrap_list = [s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",") if s.strip()]

    # Étape 6 : ASR Worker consomme le topic 'audio.uploaded'
    consumer = KafkaConsumer(
        "audio.uploaded",
        bootstrap_servers=bootstrap_list,
        group_id="whisper-worker-group",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )

    logger.info(">>> [ASR Worker] En écoute sur le topic 'audio.uploaded'...")

    for message in consumer:
        temp_raw_name = None
        message_id = ""
        try:
            headers = {key: value.decode("utf-8") for key, value in message.headers}
            message_id = headers.get("message_id") or (message.key.decode("utf-8") if message.key else "")
            user_id = headers.get("user_id", "")
            header_object_name = headers.get("object_name", f"{message_id}.ogg")
            
            if not message.value:
                continue

            stored_object_name = object_name_store.get_object_name(message_id)
            object_name = stored_object_name or header_object_name
            if not stored_object_name:
                object_name_store.set_object_name(message_id, header_object_name)

            # Écriture temporaire pour l'appel API Whisper
            temp_raw_name = f"temp_raw_{message_id}{os.path.splitext(object_name)[1] or '.ogg'}"
            with open(temp_raw_name, "wb") as f:
                f.write(message.value)

            # Étapes 7 & 8 : Envoi à la Whisper API et récupération de la transcription
            transcription = asyncio.run(WhisperService.transcribe(temp_raw_name))

            audio_url = object_name_store.resolve_audio_url(message_id)
            payload = build_audio_transcribed_message(
                message_id=message_id,
                user_id=user_id,
                audio_url=audio_url or f"s3://audio-archive/{object_name}",
                object_name=object_name,
                transcription_initiale=transcription,
            )

            # Étape 9 : ASR Worker publie la transcription générée sur 'audio.transcribed'
            try:
                payload_json = json.dumps(payload).encode("utf-8")
                future = kafka_service.producer.send(
                    AUDIO_TRANSCRIBED_TOPIC, 
                    value=payload_json, 
                    key=message_id.encode("utf-8") if isinstance(message_id, str) else message_id
                )
                record_metadata = future.get(timeout=10)
                logger.info(f"✅ [ASR Worker -> Kafka] Publié sur {record_metadata.topic} (partition {record_metadata.partition}) pour message_id={message_id}")
            except Exception as pub_err:
                logger.error(f"❌ Échec de publication Kafka pour message_id={message_id}: {pub_err}")
                raise

        except Exception as e:
            logger.error("Erreur pour message_id=%s: %s", message_id, e)

        finally:
            if temp_raw_name and os.path.exists(temp_raw_name):
                os.remove(temp_raw_name)


if __name__ == "__main__":
    main()
