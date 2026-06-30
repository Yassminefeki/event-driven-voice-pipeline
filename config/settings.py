import os
import logging

# Configuration des logs
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

# Configuration Bot & Serveurs
TOKEN = "8521480703:AAHvL1pFiasxnNak26DFf_JjBDPso7nQjHg"
WHISPER_ENDPOINT = "http://10.110.8.21:8000/v1/audio/transcriptions"

# Configuration MinIO
MINIO_URL = "localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin12345"
MINIO_SECURE = False
BUCKET_NAME = "telegram-audios"

# Configuration Elasticsearch
ELASTIC_URL = "http://localhost:9200"
INDEX_NAME = "telegram-voices"