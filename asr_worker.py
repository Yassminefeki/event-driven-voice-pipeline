# asr_worker.py
import base64
from services.kafka_service import create_kafka_consumer, send_kafka_event
from services.whisper_service import transcribe_audio_bytes


def run_worker():
    # Étape 6: Consomme 'audio.uploaded' en parallèle
    consumer = create_kafka_consumer(
        topic="audio.uploaded", group_id="whisper_asr_group"
    )

    for message in consumer:
        event = message.value

        # Decode raw audio binary from the Kafka JSON payload
        audio_bytes = base64.b64decode(event["audio_data"])

        # Étape 7 & 8: Envoie le fichier audio à la Whisper API
        transcription_text = transcribe_audio_bytes(
            audio_bytes, filename=event["file_name"]
        )

        # Étape 9: Publie la transcription générée dans 'audio.transcribed'
        send_kafka_event(
            topic="audio.transcribed",
            payload={
                "message_id": event["message_id"],
                "chat_id": event["chat_id"],
                "transcription": transcription_text,
                "timestamp": event["timestamp"],
            },
        )