import asyncio
import json
from kafka import KafkaConsumer

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application

from bot.memory import last_transcription
from services.kafka_service import AUDIO_TRANSCRIBED_TOPIC, KAFKA_BOOTSTRAP_SERVERS


def _consume_loop(app: Application, loop: asyncio.AbstractEventLoop) -> None:
    print(">>> [Kafka Consumer] Initialisation de la connexion Kafka...")
    
    bootstrap_list = [s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",") if s.strip()]
    
    consumer = KafkaConsumer(
        AUDIO_TRANSCRIBED_TOPIC,
        bootstrap_servers=bootstrap_list,
        group_id="bot-asr-consumer-group",
        auto_offset_reset="latest",  # Prend les nouveaux messages immédiatement
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else {},
    )

    print(f">>> [Kafka Consumer] En écoute sur le topic '{AUDIO_TRANSCRIBED_TOPIC}'...")

    for message in consumer:
        try:
            data = message.value or {}
            print(f"📥 [Kafka Message Reçu] Payload: {data}")

            # Récupération souple des clés (supporte plusieurs formats de JSON)
            message_id = str(
                data.get("message_id") 
                or data.get("id") 
                or (message.key.decode("utf-8") if message.key else "")
            )
            user_id = str(data.get("user_id") or data.get("chat_id") or "")
            audio_url = data.get("audio_url", "")
            
            # Récupère la transcription (transcription_initiale ou text ou transcript)
            transcription_initiale = (
                data.get("transcription_initiale") 
                if data.get("transcription_initiale") is not None 
                else data.get("text") or data.get("transcript")
            )

            # Vérification de sécurité
            if not user_id or transcription_initiale is None:
                print(f"❌ [Kafka Consumer] Message ignoré (champs manquants) -> user_id: '{user_id}', transcription: '{transcription_initiale}'")
                continue

            # Sauvegarde en mémoire
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

            # Préparation de l'envoi vers Telegram via le thread principal
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
            print(f"✅ [Kafka Consumer] Message envoyé sur Telegram à l'utilisateur {user_id}")

        except Exception as exc:
            print(f"❌ [Kafka Consumer Error] : {exc}")
            continue


async def consume_asr_results(app: Application):
    loop = asyncio.get_running_loop()
    await asyncio.to_thread(_consume_loop, app, loop)
