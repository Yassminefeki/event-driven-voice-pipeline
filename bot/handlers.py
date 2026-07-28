"""
Telegram event handlers.

Step 1-3:
    User sends voice -> audio.uploaded

Step 10:
    Whisper transcription -> audio.transcribed

Step 11-12:
    User validates or corrects -> transcription.corrected
"""

import logging
import uuid
from datetime import datetime, timezone
import base64

from telegram import Update
from telegram.ext import ContextTypes

from services.kafka_service import kafka_service

from utils.metrics import compute_wer, compute_cer

logger = logging.getLogger(__name__)


async def handle_voice_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:

    voice = update.message.voice
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    message_id = str(uuid.uuid4())

    telegram_file = await context.bot.get_file(voice.file_id)
    audio_bytes = await telegram_file.download_as_bytearray()

    # Audio → Base64
    audio_base64 = base64.b64encode(
        bytes(audio_bytes)
    ).decode("utf-8")

    timestamp = datetime.now(timezone.utc).isoformat()

    # Kafka AVANT MinIO
    kafka_service.publish_audio_uploaded(
        message_id=message_id,
        chat_id=chat_id,
        user_id=user_id,
        telegram_file_id=voice.file_id,
        audio_base64=audio_base64,
        duration_seconds=voice.duration,
        timestamp=timestamp,
    )

    pending_transcriptions = context.application.bot_data[
        "pending_transcriptions"
    ]

    pending_transcriptions[message_id] = {
        "chat_id": chat_id,
        "user_id": user_id,
    }

    logger.info(
        "message_id=%s audio published to Kafka",
        message_id
    )


async def handle_transcription_action(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:

    query = update.callback_query

    await query.answer()

    data = query.data

    if ":" not in data:
        return

    action, message_id = data.split(":", 1)

    pending_transcriptions = context.application.bot_data[
        "pending_transcriptions"
    ]

    session = pending_transcriptions.get(message_id)

    if not session:
        await query.edit_message_text(
            "⚠️ Cette transcription n'est plus disponible."
        )
        return

    # ==========================================
    # ✅ VALIDER
    # ==========================================

    if action == "validate":

        model_transcription = session.get(
            "model_transcription",
            ""
        )

        audio_url = session.get(
            "audio_url",
            ""
        )

        kafka_service.publish_transcription_corrected(
            message_id=message_id,
            chat_id=session["chat_id"],
            user_id=session["user_id"],
            audio_url=audio_url,
            model_transcription=model_transcription,
            user_correction=model_transcription,
            wer=0.0,
            cer=0.0,
            is_edited=False,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

        await query.edit_message_text(
            "✅ <b>Transcription validée !</b>\n\n"
            f"{model_transcription}",
            parse_mode="HTML",
        )

        pending_transcriptions.pop(message_id, None)

        logger.info(
            "message_id=%s transcription validated",
            message_id
        )

        return

    # ==========================================
    # ✏️ CORRIGER
    # ==========================================

    if action == "correct":

        awaiting_corrections = context.application.bot_data[
            "awaiting_corrections"
        ]

        chat_id = query.message.chat_id

        awaiting_corrections[chat_id] = message_id

        await query.edit_message_text(
            "✏️ <b>Correction</b>\n\n"
            "Copiez la transcription ci-dessous, "
            "modifiez-la si nécessaire, puis envoyez-la "
            "simplement comme un nouveau message :\n\n"
            f"<blockquote>{session.get('model_transcription', '')}</blockquote>",
            parse_mode="HTML",
        )

        logger.info(
            "message_id=%s waiting for user correction",
            message_id
        )


async def handle_text_correction(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> None:

    if not update.message or not update.message.text:
        return

    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    awaiting_corrections = context.application.bot_data[
        "awaiting_corrections"
    ]

    # Le bot attend-il une correction pour ce chat ?
    message_id = awaiting_corrections.get(chat_id)

    if not message_id:
        return

    pending_transcriptions = context.application.bot_data[
        "pending_transcriptions"
    ]

    session = pending_transcriptions.get(message_id)

    if not session:
        logger.warning(
            "No pending session for message_id=%s",
            message_id
        )
        return

    model_transcription = session.get(
        "model_transcription",
        ""
    )

    user_correction = update.message.text

    audio_url = session.get(
        "audio_url",
        ""
    )

    wer = compute_wer(
        model_transcription,
        user_correction
    )

    cer = compute_cer(
        model_transcription,
        user_correction
    )

    kafka_service.publish_transcription_corrected(
        message_id=message_id,
        chat_id=chat_id,
        user_id=user_id,
        audio_url=audio_url,
        model_transcription=model_transcription,
        user_correction=user_correction,
        wer=wer,
        cer=cer,
        is_edited=(
            user_correction.strip()
            != model_transcription.strip()
        ),
        timestamp=datetime.now(timezone.utc).isoformat(),
    )

    await update.message.reply_text(
        "✅ Correction enregistrée, merci !"
    )

    # Nettoyage
    pending_transcriptions.pop(message_id, None)
    awaiting_corrections.pop(chat_id, None)

    logger.info(
        "message_id=%s correction saved",
        message_id
    )