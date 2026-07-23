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
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8521480703:AAHvL1pFiasxnNak26DFf_JjBDPso7nQjHg")
WHISPER_ENDPOINT = os.getenv("WHISPER_ENDPOINT", "http://10.110.150.77/v1/audio/transcriptions")


# Configuration Elasticsearch
ELASTIC_URL = os.getenv("ELASTIC_URL", "http://10.110.188.120:9200")
INDEX_NAME = os.getenv("ELASTIC_INDEX_NAME", "transcription.evaluated")
# --- Configuration MinIO & Stockage ---
BUCKET_NAME = "audio-archive"
MINIO_URL = "10.110.188.120:9000"       # Ou l'URL complète selon ton usage (ex: http://10.110.188.120:9000)
MINIO_ENDPOINT = "10.110.188.120:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin12345"
MINIO_SECURE = False
