import asyncio
import json
from kafka import KafkaConsumer

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application

from bot.memory import last_transcription
from services.kafka_service import KAFKA_BOOTSTRAP_SERVERS, ASR_COMPLETED_TOPIC


def _consume_loop(app: Application, loop: asyncio.AbstractEventLoop) -> None:
    consumer = KafkaConsumer(
        ASR_COMPLETED_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id="bot-asr-consumer-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else {},
    )

    for message in consumer:
        try:
            data = message.value or {}
            message_id = str(
                data.get("message_id")
                or (message.key.decode("utf-8") if message.key else "")
            )
            user_id = str(data.get("user_id") or "")
            audio_url = data.get("audio_url")
            transcription_initiale = data.get("transcription_initiale")

            if not message_id or not user_id or transcription_initiale is None:
                print(f"❌ ASR consumer skipped invalid message: {message}")
                continue

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

            coroutine = app.bot.send_message(
                chat_id=int(user_id),
                text=(
                    "📝 Transcription initiale prête :\n\n"
                    f"`{transcription_initiale}`"
                ),
                parse_mode="Markdown",
                reply_markup=keyboard,
            )
            asyncio.run_coroutine_threadsafe(coroutine, loop)
            print(f"✅ ASR result delivered for message_id={message_id}")
        except Exception as exc:
            print(f"❌ ASR consumer error: {exc}")
            continue


async def consume_asr_results(app: Application):
    loop = asyncio.get_running_loop()
    await asyncio.to_thread(_consume_loop, app, loop)
