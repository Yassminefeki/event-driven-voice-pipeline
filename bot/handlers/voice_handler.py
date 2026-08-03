"""
voice_handler.py

Etapes 1-3 :
1. L'utilisateur envoie un message vocal.
2. Le bot telecharge le fichier, l'encode en Base64, prepare le JSON (message_id).
3. Le bot publie l'evenement sur audio.uploaded.
"""

import base64
import logging
import uuid

from telegram import Update
from telegram.ext import ContextTypes

from bot.producers.kafka_producer import BotKafkaProducer

logger = logging.getLogger(__name__)

producer = BotKafkaProducer()


async def handle_voice_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler declenche sur reception d'un message vocal Telegram."""
    voice = update.message.voice
    chat_id = update.effective_chat.id

    # message_id unique pour tracer l'evenement de bout en bout du pipeline
    message_id = str(uuid.uuid4())

    logger.info("Message vocal recu (chat_id=%s, message_id=%s)", chat_id, message_id)

    try:
        telegram_file = await context.bot.get_file(voice.file_id)
        audio_bytes = await telegram_file.download_as_bytearray()
    except Exception:
        logger.exception("Echec du telechargement du fichier vocal (message_id=%s)", message_id)
        await update.message.reply_text(
            "Desole, je n'ai pas pu recuperer votre message vocal. Merci de reessayer."
        )
        return

    audio_base64 = base64.b64encode(bytes(audio_bytes)).decode("utf-8")

    try:
        producer.publish_audio_uploaded(
            message_id=message_id,
            audio_base64=audio_base64,
            chat_id=chat_id,
        )
    except Exception:
        # Erreur de publication Kafka : on informe l'utilisateur plutot que de
        # laisser le message se perdre silencieusement.
        await update.message.reply_text(
            "Une erreur technique est survenue lors de l'envoi de votre message. "
            "Merci de reessayer dans quelques instants."
        )
        return

    # On memorise le mapping message_id <-> update pour pouvoir repondre plus
    # tard quand la transcription arrivera (voir transcription_consumer.py).
    context.bot_data.setdefault("pending_messages", {})[message_id] = {
        "chat_id": chat_id,
    }

    await update.message.reply_text(
        "🎙️ Message recu, transcription en cours... Je reviens vers vous dans un instant."
    )
