"""
Step 10: bot consumes audio.transcribed and sends the transcription
back to the user for validation.
"""

import asyncio
import html
import logging

from telegram import (
    Bot,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ForceReply,
)
from telegram.error import TelegramError, BadRequest

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
                session["audio_url"] = event.get("audio_url", "")

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

                try:
                    safe_transcription = html.escape(
                        event["model_transcription"]
                    )

                    # Message principal : transcription + boutons
                    sent = await bot.send_message(
                        chat_id=chat_id,
                        text=(
                            "📝 <b>Transcription</b>\n\n"
                            "<i>Appuyez sur le texte pour le copier</i>\n\n"
                            f"<code>{safe_transcription}</code>\n\n"
                            "Choisissez une action :"
                        ),
                        parse_mode="HTML",
                        reply_markup=keyboard,
                    )

                except BadRequest as e:
                    # Cas spécifique du "Chat not found" ou ID invalide
                    logger.error(
                        "message_id=%s: échec définitif (Chat not found / BadRequest) "
                        "pour chat_id=%s : %s. Message ignoré et offset commité.",
                        message_id, chat_id, e
                    )
                    pending_transcriptions.pop(message_id, None)
                    kafka_service.commit_offset(consumer, record)
                    continue

                except TelegramError:
                    # Une erreur Telegram sur CE message ne doit jamais
                    # interrompre la consommation des messages suivants.
                    #
                    # NB: les erreurs de type "Chat not found" (BadRequest)
                    # sont DEFINITIVES : le chat_id n'existe pas / n'a jamais
                    # démarré de conversation avec le bot, et un retry ne
                    # changera jamais ce résultat. On committe donc l'offset
                    # pour ne pas rejouer ce message à l'infini à chaque
                    # redémarrage du consumer.
                    logger.exception(
                        "message_id=%s: échec d'envoi Telegram à chat_id=%s, "
                        "message ignoré, la boucle continue",
                        message_id,
                        chat_id,
                    )
                    pending_transcriptions.pop(message_id, None)
                    kafka_service.commit_offset(consumer, record)
                    continue

                # Envoi réussi : on committe l'offset pour ne jamais
                # re-livrer ce message au prochain poll / redémarrage.
                kafka_service.commit_offset(consumer, record)

                message_id_map[sent.message_id] = message_id

                logger.info(
                    "message_id=%s transcription delivered to chat_id=%s",
                    message_id,
                    chat_id,
                )