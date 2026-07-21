import os

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ForceReply
from telegram.ext import ContextTypes

from bot.memory import last_transcription, user_active_message
from services.kafka_service import (
    KafkaService,
    TRANSCRIPTION_EVALUATED_TOPIC,
    build_transcription_evaluated_message,
)
from config.settings import BUCKET_NAME, MINIO_URL
from utils.metrics import calculate_metrics


kafka_service = KafkaService()


async def receive_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user_id = str(update.effective_user.id)
    message_id = str(update.message.message_id)

    voice = update.message.voice
    file_name = f"audio_{message_id}.wav"
    object_name = f"{message_id}.wav"

    file = await context.bot.get_file(voice.file_id)
    await file.download_to_drive(file_name)

    try:
        with open(file_name, "rb") as audio_file:
            audio_bytes = audio_file.read()

        kafka_service.publish_audio(
            audio_bytes=audio_bytes,
            object_name=object_name,
            message_id=message_id,
            user_id=user_id,
            filename=file_name,
        )
    finally:
        if os.path.exists(file_name):
            os.remove(file_name)

    last_transcription[message_id] = {
        "message_id": message_id,
        "user_id": user_id,
        "audio_url": f"http://{MINIO_URL}/{BUCKET_NAME}/{object_name}",
        "awaiting_correction": False,
    }

    await update.message.reply_text("⏳ Vocal reçu ! Traitement de la transcription en cours...")


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):

    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id

    _, message_id = query.data.split(":")
    user_id = str(query.from_user.id)

    if message_id not in last_transcription:
        await query.edit_message_text("❌ Session introuvable ou expirée.")
        return

    data = last_transcription[message_id]

    if query.data.startswith("keep:"):
        payload = build_transcription_evaluated_message(
            message_id=message_id,
            user_id=user_id,
            audio_url=data["audio_url"],
            transcription_initiale=data["transcription_initiale"],
            correction=data["transcription_initiale"],
            wer=0.0,
            cer=0.0,
            status="kept",
        )

        kafka_service.publish(TRANSCRIPTION_EVALUATED_TOPIC, payload, key=message_id)
        last_transcription.pop(message_id, None)

        await query.edit_message_text("✅ Transcription enregistrée sans modification.")

    elif query.data.startswith("correct:"):

        data["awaiting_correction"] = True
        user_active_message[user_id] = message_id

        await query.edit_message_text("✏️ Mode Correction Activé")

        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=(
                "👇 Copiez le texte ci-dessous, corrigez-le puis envoyez-le :\n\n"
                f"`{data['transcription_initiale']}`"
            ),
            parse_mode="Markdown",
            reply_markup=ForceReply(selective=True)
        )


async def receive_correction_input(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user_id = str(update.effective_user.id)

    if user_id not in user_active_message:
        return

    message_id = user_active_message[user_id]
    if message_id not in last_transcription:
        await update.message.reply_text("❌ Session de correction introuvable.")
        user_active_message.pop(user_id, None)
        return

    data = last_transcription[message_id]
    correction_text = update.message.text
    hypothesis = data["transcription_initiale"]

    wer, cer = calculate_metrics(
        reference=correction_text,
        hypothesis=hypothesis
    )

    payload = build_transcription_evaluated_message(
        message_id=message_id,
        user_id=user_id,
        audio_url=data["audio_url"],
        transcription_initiale=hypothesis,
        correction=correction_text,
        wer=wer,
        cer=cer,
        status="corrected",
    )

    kafka_service.publish(
        TRANSCRIPTION_EVALUATED_TOPIC,
        payload,
        key=message_id
    )

    last_transcription.pop(message_id, None)
    user_active_message.pop(user_id, None)

    await update.message.reply_text(
        f"✅ Correction enregistrée !\n\n"
        f"📝 Texte final :\n{correction_text}"
    )
