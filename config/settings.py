"""
Central environment configuration.
All other modules import from here — never read os.environ directly elsewhere.
"""
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    # --- Telegram ---
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")

    # --- Kafka ---
    kafka_bootstrap_servers: list[str] = tuple(
        os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092").split(",")
    )
    kafka_group_id_bot: str = os.getenv("KAFKA_GROUP_ID_BOT", "telegram-bot-group")
    kafka_group_id_worker: str = os.getenv("KAFKA_GROUP_ID_WORKER", "asr-worker-group")

    # --- Kafka topics (STRICTLY matches the 15-step reference table) ---
    topic_audio_uploaded: str = "audio.uploaded"
    topic_audio_transcribed: str = "audio.transcribed"
    topic_transcription_corrected: str = "transcription.corrected"

    # --- MinIO / S3 ---
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "")
    minio_bucket_name: str = os.getenv("MINIO_BUCKET_NAME", "audio-archive")  # step 5
    minio_secure: bool = os.getenv("MINIO_SECURE", "False").lower() == "true"

    # --- Whisper ASR ---
    whisper_endpoint: str = os.getenv("WHISPER_ENDPOINT", "")

    # --- Elasticsearch ---
    elastic_url: str = os.getenv("ELASTIC_URL", "")
    elastic_index: str = os.getenv("ELASTIC_INDEX", "transcription.corrected")  # step 14


settings = Settings()
