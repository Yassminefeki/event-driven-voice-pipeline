"""
Telegram event handlers.
Step 1: user sends voice note.
Step 2: bot downloads audio + builds metadata.
Step 3: bot publishes to audio.uploaded.
Step 11-12: user correction -> transcription.corrected.
"""
import logging
import uuid
from datetime import datetime, timezone

from telegram import Update
from telegram.ext import ContextTypes

from services.kafka_service import kafka_service
from services.minio_service import minio_service
from utils.metrics import compute_wer, compute_cer

logger = logging.getLogger(__name__)

# In-memory session store: message_id -> last model transcription (for WER/CER diffing).
# Swap for Redis/DB if the bot needs to survive restarts.
_pending_transcriptions: dict[str, dict] = {}


async def handle_voice_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Step 1 + 2 + 3."""
    voice = update.message.voice
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    message_id = str(uuid.uuid4())  # correlation key used everywhere downstream

    telegram_file = await context.bot.get_file(voice.file_id)
    audio_bytes = await telegram_file.download_as_bytearray()

    audio_url = minio_service.upload_audio(message_id, bytes(audio_bytes))

    timestamp = datetime.now(timezone.utc).isoformat()
    kafka_service.publish_audio_uploaded(
        message_id=message_id,
        chat_id=chat_id,
        user_id=user_id,
        telegram_file_id=voice.file_id,
        audio_url=audio_url,
        duration_seconds=voice.duration,
        timestamp=timestamp,
    )

    _pending_transcriptions[message_id] = {"chat_id": chat_id, "user_id": user_id}
    logger.info("message_id=%s uploaded, awaiting transcription", message_id)


async def handle_text_correction(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Step 11 + 12: user replies with a corrected transcription."""
    reply_to = update.message.reply_to_message
    if not reply_to or reply_to.message_id not in context.bot_data.get("message_id_map", {}):
        return  # not a correction reply we're tracking

    message_id = context.bot_data["message_id_map"][reply_to.message_id]
    session = _pending_transcriptions.get(message_id)
    if not session:
        logger.warning("No pending session for message_id=%s, skipping correction", message_id)
        return

    model_transcription = session.get("model_transcription", "")
    user_correction = update.message.text
    wer = compute_wer(model_transcription, user_correction)
    cer = compute_cer(model_transcription, user_correction)

    kafka_service.publish_transcription_corrected(
        message_id=message_id,
        chat_id=session["chat_id"],
        user_id=session["user_id"],
        audio_url=session.get("audio_url", ""),
        model_transcription=model_transcription,
        user_correction=user_correction,
        wer=wer,
        cer=cer,
        is_edited=(user_correction.strip() != model_transcription.strip()),
        timestamp=datetime.now(timezone.utc).isoformat(),
    )
    await update.message.reply_text("✅ Correction enregistrée, merci !")
