from minio import Minio
from config.settings import MINIO_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE, BUCKET_NAME

class MinioService:
    def __init__(self):
        self.client = Minio(
            MINIO_URL,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )
        self._ensure_bucket()

    def _ensure_bucket(self):
        if not self.client.bucket_exists(BUCKET_NAME):
            self.client.make_bucket(BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' créé.")
        else:
            print(f"Bucket '{BUCKET_NAME}' déjà existant.")

    def upload_audio(self, file_path: str) -> str:
        """Téléverse le fichier et renvoie l'URL d'accès."""
        self.client.fput_object(
            BUCKET_NAME,
            file_path,
            file_path,
            content_type="audio/wav"
        )
        return f"http://{MINIO_URL}/{BUCKET_NAME}/{file_path}"