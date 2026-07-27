import asyncio
import json
import logging
from kafka import KafkaConsumer

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application

from bot.memory import last_transcription
from config.settings import TRANSCRIPTION_COMPLETED_TOPIC, KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger(__name__)


def _consume_loop(app: Application, loop: asyncio.AbstractEventLoop) -> None:
    logger.info(">>> [Kafka Consumer] Initializing Kafka connection...")
    
    bootstrap_list = [s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",") if s.strip()]
    
    consumer = KafkaConsumer(
        TRANSCRIPTION_COMPLETED_TOPIC,
        bootstrap_servers=bootstrap_list,
        group_id="bot-asr-consumer-group",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else {},
    )

    logger.info(f">>> [Kafka Consumer] Listening on topic '{TRANSCRIPTION_COMPLETED_TOPIC}'...")

    for message in consumer:
        try:
            data = message.value or {}
            logger.info(f"📥 [Kafka Message Received] Payload: {data}")

            message_id = str(
                data.get("message_id") 
                or data.get("id") 
                or (message.key.decode("utf-8") if message.key else "")
            )
            user_id = str(data.get("user_id") or data.get("chat_id") or "")
            audio_url = data.get("audio_url", "")
            
            transcription_initiale = (
                data.get("transcription_initiale") 
                if data.get("transcription_initiale") is not None 
                else data.get("text") or data.get("transcript")
            )

            if not user_id or transcription_initiale is None:
                logger.warning(
                    f"❌ [Kafka Consumer] Ignored message (missing required fields) -> "
                    f"user_id: '{user_id}', transcription: '{transcription_initiale}'"
                )
                continue

            # Store state in memory for callback reference
            if message_id:
                last_transcription[message_id] = {
                    "message_id": message_id,
                    "user_id": user_id,
                    "audio_url": audio_url,
                    "transcription_initiale": transcription_initiale,
                    "awaiting_correction": False,
                }

            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ Valider", callback_data=f"keep:{message_id}"),
                    InlineKeyboardButton("✏️ Corriger", callback_data=f"correct:{message_id}"),
                ]
            ])

            # Send message to Telegram UI thread
            coroutine = app.bot.send_message(
                chat_id=int(user_id),
                text=(
                    "📝 **Transcription initiale prête :**\n\n"
                    f"`{transcription_initiale}`"
                ),
                parse_mode="Markdown",
                reply_markup=keyboard,
            )
            asyncio.run_coroutine_threadsafe(coroutine, loop)
            logger.info(f"✅ [Kafka Consumer] Successfully sent message to Telegram user {user_id}")

        except Exception as exc:
            logger.error(f"❌ [Kafka Consumer Error] : {exc}")
            continue


async def consume_asr_results(app: Application):
    loop = asyncio.get_running_loop()
    await asyncio.to_thread(_consume_loop, app, loop)