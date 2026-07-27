
"""
Step 10: bot consumes audio.transcribed and sends the transcription
back to the user for validation.
"""
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
"""
Step 10: bot consumes audio.transcribed and sends the transcription
back to the user for validation.
"""

import asyncio
import logging

from telegram import (
    Bot,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)

from config.settings import settings
from services.kafka_service import kafka_service

logger = logging.getLogger(__name__)


async def run_asr_consumer_loop(
    bot: Bot,
    pending_transcriptions: dict,
    message_id_map: dict,
) -> None:

    consumer = kafka_service.make_consumer(
        settings.topic_audio_transcribed,
        group_id=settings.kafka_group_id_bot,
    )

    loop = asyncio.get_event_loop()

    while True:

        records = await loop.run_in_executor(
            None,
            lambda: consumer.poll(timeout_ms=1000)
        )

        for records_list in records.values():
            for record in records_list:

                event = record.value

                message_id = event["message_id"]
                chat_id = event["chat_id"]

                session = pending_transcriptions.setdefault(
                    message_id,
                    {}
                )

                session["chat_id"] = chat_id
                session["user_id"] = event["user_id"]
                session["model_transcription"] = event["model_transcription"]
                session["audio_url"] = event["audio_url"]

                keyboard = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(
                            "✅ Valider",
                            callback_data=f"validate:{message_id}"
                        ),
                        InlineKeyboardButton(
                            "✏️ Corriger",
                            callback_data=f"correct:{message_id}"
                        ),
                    ]
                ])

                sent = await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        "📝 <b>Transcription</b>\n\n"
                        f"<blockquote>{event['model_transcription']}</blockquote>\n\n"
                        "Choisissez une action :"
                    ),
                    parse_mode="HTML",
                    reply_markup=keyboard,
                )

                message_id_map[sent.message_id] = message_id

                logger.info(
                    "message_id=%s transcription delivered to chat_id=%s",
                    message_id,
                    chat_id,
                )