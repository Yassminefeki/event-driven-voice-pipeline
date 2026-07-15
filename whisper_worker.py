import json
import os
import tempfile
import time

from kafka import KafkaConsumer

from services.kafka_service import (
    KAFKA_BOOTSTRAP_SERVERS,
    KafkaService,
    build_transcription_completed_message,
)

from services.minio_service import MinioService
from services.whisper_service import WhisperService


def main() -> None:

    consumer = KafkaConsumer(
        "audio.uploaded",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

        # important pour garder la position Kafka
        group_id="whisper-worker-group",

        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),

        # on garde earliest comme demandé
        auto_offset_reset="earliest",

        enable_auto_commit=True,
    )


    kafka_service = KafkaService()
    minio_service = MinioService()
    whisper_service = WhisperService()


    print("🎧 Whisper worker démarré...")
    print("En attente des messages Kafka audio.uploaded...")


    for message in consumer:

        try:

            data = message.value

            print("📩 Message Kafka reçu :", data)


            audio_id = data["audio_id"]
            user_id = data["user_id"]
            bucket = data["bucket"]
            object_name = data["object_name"]


            print(f"🎤 Traitement audio : {audio_id}")


            # téléchargement depuis MinIO

            with tempfile.NamedTemporaryFile(
                suffix=".wav",
                delete=False
            ) as tmp:

                local_path = tmp.name


            minio_service.client.fget_object(
                bucket,
                object_name,
                local_path
            )


            # transcription Whisper

            transcription = whisper_service.transcribe(
                local_path
            )


            os.remove(local_path)


            print(
                f"📝 Transcription : {transcription}"
            )


            # Pour le moment correction = transcription
            # Le bot pourra modifier plus tard

            transcription_initiale = transcription
            correction = transcription


            wer = 0.0
            cer = 0.0



            message_final = build_transcription_completed_message(
                audio_id=audio_id,
                user_id=str(user_id),

                transcription_initiale=transcription_initiale,
                correction=correction,

                wer=wer,
                cer=cer,

                bucket=bucket,
                object_name=object_name,
            )


            kafka_service.publish(
                "transcription.completed",
                message_final,
                key=str(user_id)
            )


            print(
                "✅ Message envoyé vers transcription.completed"
            )


        except json.JSONDecodeError:

            print(
                "⚠️ Message Kafka ignoré : JSON invalide"
            )

            continue


        except Exception as e:

            print(
                "❌ Erreur traitement :",
                e
            )

            continue



if __name__ == "__main__":
    main()
