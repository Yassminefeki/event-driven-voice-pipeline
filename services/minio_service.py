"""
MinIO / S3 client.
Uses a deterministic object key `{message_id}.ogg` so audio can always be
located from the Kafka key alone — no separate lookup table required.
"""
import logging
from io import BytesIO

from minio import Minio
from minio.error import S3Error

from config.settings import settings

logger = logging.getLogger(__name__)


class MinioService:
    def __init__(self):
        self._client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        )
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        if not self._client.bucket_exists(settings.minio_bucket_name):
            self._client.make_bucket(settings.minio_bucket_name)
            logger.info("Created bucket %s", settings.minio_bucket_name)

    def object_key(self, message_id: str) -> str:
        return f"{message_id}.ogg"

    def upload_audio(self, message_id: str, audio_bytes: bytes) -> str:
        key = self.object_key(message_id)

        self._client.put_object(
            settings.minio_bucket_name,
            key,
            BytesIO(audio_bytes),
            length=len(audio_bytes),
            content_type="audio/ogg",
        )

    return f"http://10.110.188.120:9000/{settings.minio_bucket_name}/{key}"

    def download_audio(self, message_id: str) -> bytes:
        key = self.object_key(message_id)
        try:
            response = self._client.get_object(settings.minio_bucket_name, key)
            return response.read()
        except S3Error:
            logger.exception("Failed to fetch object %s from bucket %s", key, settings.minio_bucket_name)
            raise
        finally:
            try:
                response.close()
                response.release_conn()
            except Exception:
                pass


minio_service = MinioService()
