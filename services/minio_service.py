from minio import Minio
from config.settings import MINIO_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE, BUCKET_NAME

class MinioService:
    def __init__(self):
        self.bucket_name = BUCKET_NAME
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

    def upload_audio(self, file_path: str, object_name: str | None = None) -> str:
        """Téléverse le fichier et renvoie l'URL d'accès."""
        object_name = object_name or file_path
        self.client.fput_object(
            self.bucket_name,
            object_name,
            file_path,
            content_type="audio/wav"
        )
        return f"http://{MINIO_URL}/{self.bucket_name}/{object_name}"

    def download_audio(self, object_name: str, destination_path: str) -> str:
        """Télécharge un objet MinIO vers un chemin local."""
        self.client.fget_object(self.bucket_name, object_name, destination_path)
        return destination_path