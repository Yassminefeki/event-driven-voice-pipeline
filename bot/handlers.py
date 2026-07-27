# bot/handlers.py
from services.kafka_client import send_kafka_event


async def handle_voice_message(update, context):
    message = update.message
    file_id = message.voice.file_id

    # Étape 2: Télécharge l'audio depuis Telegram
    tg_file = await context.bot.get_file(file_id)
    audio_bytes = await tg_file.download_as_bytearray()

    # Base64 encode raw bytes so it can sit inside the Kafka JSON payload
    # for the MinIO Sink Connector and ASR worker
    audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")

    payload = {
        "message_id": message.message_id,
        "chat_id": message.chat_id,
        "user_id": message.from_user.id,
        "file_name": f"{message.message_id}.ogg",
        "audio_data": audio_b64,
        "duration": message.voice.duration,
        "timestamp": message.date.isoformat(),
    }

    # Étape 3: Publie le message audio dans Kafka -> 'audio.uploaded'
    send_kafka_event(topic="audio.uploaded", payload=payload)