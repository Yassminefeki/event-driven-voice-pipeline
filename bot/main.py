"""
main.py

Point d'entree du Bot Telegram DataBot.
Demarre :
- le polling Telegram (messages vocaux, callbacks de validation, corrections texte)
- le consumer Kafka audio.transcribed dans un thread dedie
"""

import logging
import os
import threading

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from bot.consumers.transcription_consumer import run_transcription_consumer
from bot.handlers.validation_handler import (
    CORRECT_CALLBACK_PREFIX,
    VALIDATE_CALLBACK_PREFIX,
    handle_correction_text,
    handle_validate_callback,
)
from bot.handlers.voice_handler import handle_voice_message

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]


async def handle_correct_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """L'utilisateur a clique sur 'Corriger' : on lui demande le texte corrige
    et on memorise qu'on attend une correction pour ce message_id."""
    query = update.callback_query
    await query.answer()
    message_id = query.data.removeprefix(CORRECT_CALLBACK_PREFIX)

    context.bot_data.setdefault("awaiting_correction", {})[update.effective_chat.id] = message_id
    await query.edit_message_text("✏️ Merci d'envoyer le texte corrige dans votre prochain message.")


async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Route un message texte vers la correction en attente, si applicable."""
    chat_id = update.effective_chat.id
    awaiting = context.bot_data.get("awaiting_correction", {})
    message_id = awaiting.pop(chat_id, None)

    if message_id is None:
        await update.message.reply_text(
            "Envoyez-moi un message vocal pour commencer une transcription."
        )
        return

    await handle_correction_text(update, context, message_id)


def start_kafka_consumer_thread(application: Application) -> None:
    """Lance le consumer Kafka audio.transcribed dans un thread separe,
    pour ne pas bloquer le polling Telegram (event loop asyncio)."""

    def _run():
        import asyncio

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_transcription_consumer(application))

    thread = threading.Thread(target=_run, daemon=True, name="kafka-transcription-consumer")
    thread.start()
    logger.info("Consumer Kafka audio.transcribed demarre (thread=%s)", thread.name)


def main() -> None:
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    application.add_handler(MessageHandler(filters.VOICE, handle_voice_message))
    application.add_handler(
        CallbackQueryHandler(handle_validate_callback, pattern=f"^{VALIDATE_CALLBACK_PREFIX}")
    )
    application.add_handler(
        CallbackQueryHandler(handle_correct_callback, pattern=f"^{CORRECT_CALLBACK_PREFIX}")
    )
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))

    start_kafka_consumer_thread(application)

    logger.info("Bot Telegram DataBot demarre.")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
