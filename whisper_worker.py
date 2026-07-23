import os
import base64
import json
import logging
import subprocess
from kafka import KafkaConsumer
from minio import Minio
import whisper

from config.settings import (
    BUCKET_NAME,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
)
from services.kafka_service import (
    KafkaService,
    KAFKA_BOOTSTRAP_SERVERS,
    ASR_COMPLETED_TOPIC,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,
)

kafka_service = KafkaService()

logger.info("⏳ Chargement du modèle Whisper...")
model = whisper.load_model("base")
logger.info("✅ Modèle Whisper chargé avec succès.")


def ensure_bucket_exists():
    try:
        if not minio_client.bucket_exists(BUCKET_NAME):
            minio_client.make_bucket(BUCKET_NAME)
    except Exception as e:
        logger.error(f"❌ Erreur MinIO: {e}")


def main():
    ensure_bucket_exists()
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
        temp_wav_name = None
        try:
            data = json.loads(message.value.decode("utf-8"))
            message_id = data.get("message_id")
            user_id = data.get("user_id")
            
            # Forcer l'extension .wav pour correspondre au fichier converti
            orig_object_name = data.get("object_name", f"{message_id}.ogg")
            base_name, _ = os.path.splitext(orig_object_name)
            object_name = f"{base_name}.wav"

            file_content_b64 = data.get("file_content")
            if not file_content_b64:
                continue

            # 1. Sauvegarde brute temporaire
            audio_bytes = base64.b64decode(file_content_b64)
            temp_raw_name = f"temp_raw_{message_id}.ogg"
            with open(temp_raw_name, "wb") as f:
                f.write(audio_bytes)

            # 2. Conversion en WAV 16kHz mono via FFmpeg
            temp_wav_name = f"temp_conv_{message_id}.wav"
            cmd = [
                "ffmpeg", "-y", "-i", temp_raw_name,
                "-ar", "16000", "-ac", "1", "-c:a", "pcm_s16le",
                temp_wav_name
            ]
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)

            # 3. Upload vers MinIO au format .wav
            minio_client.fput_object(
                bucket_name=BUCKET_NAME,
                object_name=object_name,
                file_path=temp_wav_name,
                content_type="audio/wav",
            )

            # 4. Transcription Whisper
            result = model.transcribe(temp_wav_name)
            transcription = result.get("text", "").strip()

            # 5. Publication du résultat
            payload = {
                "message_id": message_id,
                "user_id": user_id,
                "transcription_initiale": transcription,
            }
            kafka_service.publish(ASR_COMPLETED_TOPIC, payload, key=message_id)
            logger.info(f"✅ Traité avec succès pour message_id={message_id}")

        except Exception as e:
            logger.error(f"❌ Erreur: {e}")

        finally:
            if temp_raw_name and os.path.exists(temp_raw_name):
                os.remove(temp_raw_name)
            if temp_wav_name and os.path.exists(temp_wav_name):
                os.remove(temp_wav_name)


if __name__ == "__main__":
    main()

