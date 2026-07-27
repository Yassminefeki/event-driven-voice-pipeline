"""
Entry point: Telegram Bot application.
Initializes the bot, registers handlers, and starts the audio.transcribed
consumer loop (step 10) as a background task.
"""
import asyncio
import logging

from telegram.ext import ApplicationBuilder, MessageHandler, filters

from config.settings import settings
from bot.handlers import handle_voice_message, handle_text_correction
from bot.asr_consumer import run_asr_consumer_loop

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def _post_init(application) -> None:
    pending_transcriptions: dict = {}
    application.bot_data["pending_transcriptions"] = pending_transcriptions
    application.bot_data.setdefault("message_id_map", {})
    asyncio.create_task(run_asr_consumer_loop(application.bot, pending_transcriptions))
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

    application.add_handler(MessageHandler(filters.VOICE, handle_voice_message))
    application.add_handler(MessageHandler(filters.TEXT & filters.REPLY, handle_text_correction))

    logger.info("Telegram bot starting (polling)...")
    application.run_polling()


if __name__ == "__main__":
    main()
