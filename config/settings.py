import os
import logging
from pathlib import Path

# Configuration des logs
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)


def load_env_file() -> None:
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"\'')
        os.environ.setdefault(key, value)


load_env_file()

# Configuration Bot & Serveurs
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
WHISPER_ENDPOINT = os.getenv("WHISPER_ENDPOINT", "http://10.110.150.77/v1/audio/transcriptions")
WHISPER_API_KEY = os.getenv("WHISPER_API_KEY", "")
WHISPER_LANGUAGE = os.getenv("WHISPER_LANGUAGE", "ar")
WHISPER_TIMEOUT = float(os.getenv("WHISPER_TIMEOUT", "60"))


# Configuration Elasticsearch
ELASTIC_URL = os.getenv("ELASTIC_URL", "http://10.110.188.120:9200")
INDEX_NAME = os.getenv("ELASTIC_INDEX_NAME", "transcription.corrected")
ELASTIC_USERNAME = os.getenv("ELASTIC_USERNAME", "")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD", "")
# --- Configuration MinIO & Stockage ---
BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "audio-archive")
MINIO_URL = os.getenv("MINIO_URL", "10.110.188.120:9000")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", MINIO_URL)
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
