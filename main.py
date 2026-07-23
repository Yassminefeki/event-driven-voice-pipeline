import os
import logging
import base64
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters

from services.kafka_service import KafkaService

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

kafka_service = KafkaService()
KAFKA_TOPIC = "audio.uploaded"

async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user = update.effective_user
        voice = update.message.voice

        if not voice:
            return

        await update.message.reply_text("⏳ Vocal reçu ! Traitement de la transcription en cours...")

        file = await context.bot.get_file(voice.file_id)
        file_bytes = await file.download_as_bytearray()

        file_content_b64 = base64.b64encode(file_bytes).decode("utf-8")
        message_id = str(update.message.message_id)
        object_name = f"{message_id}.wav"

        payload = {
            "message_id": message_id,
            "user_id": str(user.id),
            "object_name": object_name,
            "file_content": file_content_b64,
        }

        kafka_service.publish(KAFKA_TOPIC, payload, key=message_id)
        logger.info(f"📤 Message vocal {message_id} envoyé à Kafka sur le topic '{KAFKA_TOPIC}' avec l'objet {object_name}")

    except Exception as e:
        logger.error(f"❌ Erreur dans handle_voice: {e}")
        await update.message.reply_text("❌ Une erreur est survenue lors du traitement de votre vocal.")

def main():
    # Ton token Telegram intégré directement
    TOKEN = "8521480703:AAHvL1pFiasxnNak26DFf_JjBDPso7nQjHg"

    application = ApplicationBuilder().token(TOKEN).build()
    application.add_handler(MessageHandler(filters.VOICE | filters.AUDIO, handle_voice))

    logger.info("🤖 Bot Telegram démarré et en écoute...")
    application.run_polling()

if __name__ == "__main__":
    main()
