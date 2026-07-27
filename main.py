"""
Entry point: Telegram Bot application.
"""
import asyncio
import logging

from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)

from config.settings import settings
from bot.handlers import (
    handle_voice_message,
    handle_text_correction,
    handle_transcription_action,
)
from bot.asr_consumer import run_asr_consumer_loop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)


async def _post_init(application) -> None:
    pending_transcriptions: dict = {}
    message_id_map: dict = {}

    # chat_id -> message_id de la transcription à corriger
    awaiting_corrections: dict = {}

    application.bot_data["pending_transcriptions"] = pending_transcriptions
    application.bot_data["message_id_map"] = message_id_map
    application.bot_data["awaiting_corrections"] = awaiting_corrections

    asyncio.create_task(
        run_asr_consumer_loop(
            application.bot,
            pending_transcriptions,
            message_id_map,
        )
    )

    logger.info("audio.transcribed consumer loop started")


def main() -> None:
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    application = (
        ApplicationBuilder()
        .token(settings.telegram_bot_token)
        .post_init(_post_init)
        .build()
    )

    # 🎤 Réception d'un vocal
    application.add_handler(
        MessageHandler(filters.VOICE, handle_voice_message)
    )

    # ✏️ Texte envoyé après "Corriger"
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_text_correction
        )
    )

    # ✅ Valider / ✏️ Corriger
    application.add_handler(
        CallbackQueryHandler(handle_transcription_action)
    )

    logger.info("Telegram bot starting (polling)...")
    application.run_polling()


if __name__ == "__main__":
    main()