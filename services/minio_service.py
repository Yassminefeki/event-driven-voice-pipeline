import io
import logging
from minio import Minio
from config.settings import (
    BUCKET_NAME,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
)

logger = logging.getLogger(__name__)


class MinioService:

    def __init__(self):
        clean_endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
        self.client = Minio(
            endpoint=clean_endpoint,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        try:
            if not self.client.bucket_exists(BUCKET_NAME):
                self.client.make_bucket(BUCKET_NAME)
                logger.info(f"✅ Created MinIO bucket '{BUCKET_NAME}'")
        except Exception as e:
            logger.error(f"❌ Error verifying MinIO bucket '{BUCKET_NAME}': {e}")

    def upload_audio_bytes(self, audio_bytes: bytes, object_name: str) -> str:
        """Uploads raw audio bytes directly to MinIO and returns the http access URL."""
        try:
            data_stream = io.BytesIO(audio_bytes)
            stream_length = len(audio_bytes)

            self.client.put_object(
                bucket_name=BUCKET_NAME,
                object_name=object_name,
                data=data_stream,
                length=stream_length,
                content_type="audio/ogg",
            )
            logger.info(f"✅ Successfully uploaded {object_name} to MinIO")
            return self.get_object_url(object_name)
        except Exception as e:
            logger.error(f"❌ Failed to upload {object_name} to MinIO: {e}")
            raise e

    def get_object_url(self, object_name: str) -> str:
        """Generates static HTTP URL for MinIO object retrieval."""
        protocol = "https" if MINIO_SECURE else "http"
        clean_endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
        return f"{protocol}://{clean_endpoint}/{BUCKET_NAME}/{object_name}"