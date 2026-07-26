import os
import logging
from telegram import Update
from telegram.ext import ContextTypes

from config.settings import BUCKET_NAME, MINIO_ENDPOINT, MINIO_SECURE, TRANSCRIPTION_CORRECTED_TOPIC
from services.kafka_service import KafkaService, build_transcription_corrected_message
from bot.memory import last_transcription
from utils.metrics import calculate_metrics

logger = logging.getLogger(__name__)
kafka_service = KafkaService()


def get_minio_audio_url(object_name: str) -> str:
    """Generates permanent HTTP access URL for MinIO."""
    try:
        protocol = "https" if MINIO_SECURE else "http"
        clean_endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
        return f"{protocol}://{clean_endpoint}/{BUCKET_NAME}/{object_name}"
    except Exception as e:
        logger.error(f"❌ Error generating MinIO URL: {e}")
        return f"s3://{BUCKET_NAME}/{object_name}"


def publish_correction(message_id: str, status: str, text_initial: str,
                       corrected_text: str, audio_url: str, user_id: str) -> None:
    """Publishes evaluation decision to Kafka; Elasticsearch ingests via Sink Connector."""
    if status == "kept":
        wer, cer = 0.0, 0.0
    else:
        wer, cer = calculate_metrics(reference=text_initial, hypothesis=corrected_text)

    payload = build_transcription_corrected_message(
        message_id=message_id,
        user_id=user_id,
        audio_url=audio_url,
        transcription_initiale=text_initial,
        transcription_corrigee=corrected_text,
        wer=wer,
        cer=cer,
        status=status,
    )
    kafka_service.publish(TRANSCRIPTION_CORRECTED_TOPIC, payload, key=message_id)


async def receive_voice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = str(update.effective_user.id)
    message_id = str(update.message.message_id)

    voice = update.message.voice
    if not voice:
        await update.message.reply_text("⚠️ Veuillez envoyer un message vocal valide.")
        return

    file_name = f"raw_{message_id}.ogg"
    object_name = f"{message_id}.ogg"
    audio_http_url = get_minio_audio_url(object_name)

    last_transcription[message_id] = {
        "user_id": user_id,
        "transcription_initiale": "",
        "audio_url": audio_http_url,
        "status": "pending",
    }

    file = await context.bot.get_file(voice.file_id)
    await file.download_to_drive(file_name)

    try:
        with open(file_name, "rb") as f:
            audio_bytes = f.read()

        kafka_service.publish_audio(
            audio_bytes=audio_bytes,
            object_name=object_name,
            message_id=message_id,
            user_id=user_id,
            bucket=BUCKET_NAME,
        )
        logger.info(f"✅ Audio bytes published for message_id={message_id}")
        await update.message.reply_text("⏳ Vocal reçu ! Traitement de la transcription en cours...")

    except Exception as e:
        logger.error(f"❌ Error processing voice message: {e}")
        await update.message.reply_text("❌ Une erreur s'est produite lors du traitement de votre vocal.")

    finally:
        if os.path.exists(file_name):
            os.remove(file_name)


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    data = query.data
    if ":" not in data:
        return

    action, message_id = data.split(":", 1)

    if action == "keep":
        if message_id in last_transcription:
            text_initial = last_transcription[message_id].get("transcription_initiale", "")
            audio_url = last_transcription[message_id].get("audio_url", "")
            user_id = last_transcription[message_id].get("user_id", "")
            publish_correction(message_id, "kept", text_initial, text_initial, audio_url, user_id)

            await query.edit_message_text(
                f"✅ **Transcription validée et enregistrée !**\n\n{text_initial}",
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text("✅ Transcription validée !")

    elif action == "correct":
        context.user_data["awaiting_correction_for"] = message_id
        text_initial = last_transcription.get(message_id, {}).get("transcription_initiale", "")

        msg = "✏️ **Veuillez envoyer sous forme de texte votre correction.**\n\n"
        if text_initial:
            msg += f"💡 *Cliquez sur le texte ci-dessous pour le copier :*\n\n`{text_initial}`"

        await query.message.reply_text(text=msg, parse_mode="Markdown")


async def receive_correction_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message_id = context.user_data.get("awaiting_correction_for")
    if not message_id:
        return

    corrected_text = update.message.text.strip()
    
    if message_id in last_transcription:
        text_initial = last_transcription[message_id].get("transcription_initiale", "")
        audio_url = last_transcription[message_id].get("audio_url", "")
        user_id = last_transcription[message_id].get("user_id", "")

        publish_correction(
            message_id, "corrected", text_initial, corrected_text, audio_url, user_id
        )

    del context.user_data["awaiting_correction_for"]
    await update.message.reply_text(
        f"🎯 **Correction enregistrée !**\n\n{corrected_text}",
        parse_mode="Markdown"
    )