import os
import sys
import time
import uuid

from locust import User, task, between

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

from services.minio_service import MinioService


class MinIOStressTest(User):

    wait_time = between(0.1, 0.5)

    def on_start(self):
        self.minio = MinioService()

    @task
    def upload(self):

        audio_path = "audio.wav"
        if not os.path.exists(audio_path):
            raise FileNotFoundError(audio_path)

        unique_object_name = f"audio_{uuid.uuid4()}.wav"
        start = time.perf_counter()

        try:

            self.minio.upload_audio(audio_path, object_name=unique_object_name)

            duration = (time.perf_counter() - start) * 1000

            self.environment.events.request.fire(
                request_type="MinIO",
                name="Upload Audio",
                response_time=duration,
                response_length=os.path.getsize(audio_path),
                exception=None,
            )

        except Exception as e:

            duration = (time.perf_counter() - start) * 1000

            self.environment.events.request.fire(
                request_type="MinIO",
                name="Upload Audio",
                response_time=duration,
                response_length=0,
                exception=e,
            )