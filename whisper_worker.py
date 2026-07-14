import json
import os
import tempfile
import time
from kafka import KafkaConsumer

from services.kafka_service import KAFKA_BOOTSTRAP_SERVERS, KafkaService, build_transcription_completed_message
from services.minio_service import MinioService
from services.whisper_service import WhisperService


def main() -> None:
    consumer = KafkaConsumer(
        "audio.uploaded",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    kafka_service = KafkaService()
    minio_service = MinioService()

    print("Whisper worker started, waiting for audio.uploaded messages...")
    for message in consumer:
        payload = message.value
        print(f"Received message: {payload}")
        audio_id = payload.get("audio_id")
        object_name = payload.get("object_name") or f"{audio_id}.wav"
        user_id = payload.get("user_id", "unknown")
        print(f"Processing {object_name}")

        local_path = os.path.join(tempfile.gettempdir(), f"{audio_id}.wav")
        try:
            minio_service.download_audio(object_name, local_path)
            transcription = WhisperService.transcribe(local_path)
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)

        print(f"Transcribed {audio_id}: {transcription}")
        kafka_service.publish(
            "transcription.completed",
            build_transcription_completed_message(
                audio_id=audio_id,
                user_id=str(user_id),
                text=transcription,
                bucket=minio_service.bucket_name,
                object_name=object_name,
            ),
            key=str(user_id),
        )


if __name__ == "__main__":
    main()
