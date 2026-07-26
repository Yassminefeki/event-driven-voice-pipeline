from config.settings import BUCKET_NAME, MINIO_ENDPOINT, MINIO_SECURE

def build_minio_url(object_name: str) -> str:
    protocol = "https" if MINIO_SECURE else "http"
    clean_endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    return f"{protocol}://{clean_endpoint}/{BUCKET_NAME}/{object_name}"