import logging

from telegram.ext import ApplicationBuilder, MessageHandler, filters

from bot.asr_consumer import consume_asr_results
from bot.handlers import handle_voice_message
from config.settings import TOKEN


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


async def start_asr_consumer(application):
    application.create_task(
        consume_asr_results(application)
    )


def main():

    if not TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN doit être défini dans .env"
        )


    application = (
        ApplicationBuilder()
        .token(TOKEN)
        .post_init(start_asr_consumer)
        .build()
    )


    # Receive Telegram voice messages
    application.add_handler(
        MessageHandler(
            filters.VOICE,
            handle_voice_message
        )
    )


    logger.info(
        "🤖 Bot Telegram démarré et en écoute..."
    )

    application.run_polling()


if __name__ == "__main__":
    main()