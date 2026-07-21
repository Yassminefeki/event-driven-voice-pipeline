import json
import os
import tempfile

from kafka import KafkaConsumer

from services.kafka_service import (
    KAFKA_BOOTSTRAP_SERVERS,
    KafkaService,
    ASR_COMPLETED_TOPIC,
    build_asr_completed_message,
)
from services.minio_service import MinioService
from services.whisper_service import WhisperService


def main() -> None:
    consumer = KafkaConsumer(
        "audio.uploaded",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id="whisper-worker-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    kafka_service = KafkaService()
    minio_service = MinioService()
    whisper_service = WhisperService()

    print("🎧 Whisper worker running...")

    for message in consumer:
        try:
            data = {}
            if message.value:
                try:
                    data = json.loads(message.value.decode("utf-8"))
                except (json.JSONDecodeError, AttributeError):
                    data = {}

            headers = {
                name: value.decode("utf-8") if isinstance(value, bytes) else value
                for name, value in (message.headers or [])
            }

            message_id = str(
                data.get("message_id")
                or headers.get("message_id")
                or (message.key.decode("utf-8") if message.key else "")
            )
            user_id = str(data.get("user_id") or headers.get("user_id") or "")
            bucket = data.get("bucket") or headers.get("bucket")
            object_name = data.get("object_name") or headers.get("object_name")

            if not message_id or not user_id:
                raise ValueError("Missing required message_id or user_id")

            print(f"📩 Message Kafka reçu : message_id={message_id}")

            if bucket and object_name:
                with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
                    local_path = tmp.name

                minio_service.client.fget_object(bucket, object_name, local_path)
                audio_url = f"http://{minio_service.client._base_url.host}/{bucket}/{object_name}"
            else:
                if not message.value:
                    raise ValueError("Missing audio payload and MinIO metadata")
                with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
                    local_path = tmp.name
                with open(local_path, "wb") as audio_file:
                    audio_file.write(message.value)
                audio_url = data.get("audio_url", "")

            transcription = whisper_service.transcribe(local_path)
            os.remove(local_path)

            asr_message = build_asr_completed_message(
                message_id=message_id,
                user_id=user_id,
                audio_url=audio_url,
                transcription_initiale=transcription,
            )

            kafka_service.publish(
                ASR_COMPLETED_TOPIC,
                asr_message,
                key=message_id,
            )
            print(f"✅ Published ASR result for message_id: {message_id}")

        except Exception as e:
            print(f"❌ Worker Error: {e}")
            continue


if __name__ == "__main__":
    main()
