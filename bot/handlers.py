import os
import uuid
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ForceReply
from telegram.ext import ContextTypes

from bot.memory import last_transcription
from services.minio_service import MinioService
from services.whisper_service import WhisperService
from services.elastic_service import ElasticService
from services.kafka_service import KafkaService, build_audio_uploaded_message, build_transcription_completed_message
from utils.metrics import calculate_metrics

# Initialisation des services requis par les handlers
minio_service = MinioService()
elastic_service = ElasticService()
kafka_service = KafkaService()

async def receive_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    print(f"🎤 Premier vocal reçu de l'utilisateur {user_id}")

    # Redirection si déjà en mode correction
    if user_id in last_transcription and last_transcription[user_id].get("awaiting_correction"):
        await receive_correction_input(update, context)
        return

    voice = update.message.voice
    audio_id = str(uuid.uuid4())
    file_name = f"audio_{audio_id}.wav"

    # Téléchargement local temporaire
    file = await context.bot.get_file(voice.file_id)
    await file.download_to_drive(file_name)

    try:
        object_name = f"{audio_id}.wav"
        audio_url = minio_service.upload_audio(file_name, object_name=object_name)
        payload = build_audio_uploaded_message(
            audio_id=audio_id,
            user_id=str(user_id),
            bucket=minio_service.bucket_name,
            object_name=object_name,
            filename=file_name,
        )
        kafka_service.publish(payload["topic"], payload, key=str(user_id))
        transcription = WhisperService.transcribe(file_name)
    finally:
        if os.path.exists(file_name):
            os.remove(file_name)

    # Stockage temporaire en mémoire
    last_transcription[user_id] = {
        "audio_initial": audio_url,
        "transcription_initiale": transcription,
        "awaiting_correction": False,
        "audio_id": audio_id,
    }

    keyboard = [
        [
            InlineKeyboardButton("✅ Oui, corriger", callback_data="correct"),
            InlineKeyboardButton("❌ Non, garder", callback_data="keep")
        ]
    ]

    await update.message.reply_text(
        f"📝 Transcription :\n\n{transcription}\n\nEst-ce que ce vocal contient des erreurs ?",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    print("✅ Message avec boutons envoyé")


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print("🔘 BOUTON RECU !!!")
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id

    if user_id not in last_transcription:
        await query.edit_message_text("❌ Aucun vocal trouvé.")
        return

    data = last_transcription[user_id]

    if query.data == "keep":
        elastic_service.save_transcription(
            audio_initial=data["audio_initial"],
            hypothesis=data["transcription_initiale"],
            correction=data["transcription_initiale"],
            wer=0.0,
            cer=0.0
        )
        kafka_service.publish(
            "transcription.completed",
            build_transcription_completed_message(
                audio_id=data.get("audio_id", "unknown"),
                user_id=str(user_id),
                text=data["transcription_initiale"],
                bucket=minio_service.bucket_name,
                object_name=data.get("audio_id", "unknown") + ".wav",
            ),
            key=str(user_id),
        )
        last_transcription.pop(user_id, None)
        await query.edit_message_text("✅ Transcription enregistrée sans modification.")
        print("✅ Enregistré sans modification dans ELK")

    elif query.data == "correct":
        last_transcription[user_id]["awaiting_correction"] = True
        await query.edit_message_text("✏️ Mode Correction Activé")
        
        transcription_brute = data['transcription_initiale']
        await context.bot.send_message(
            chat_id=query.message.chat_id,
            text=(
                "👇 Pour corriger facilement :\n"
                "1️⃣ Appuie sur le texte grisé ci-dessous pour le copier.\n"
                "2️⃣ Colle-le dans ta barre de saisie, modifie-le et envoie.\n\n"
                f"`{transcription_brute}`"
            ),
            parse_mode="Markdown",
            reply_markup=ForceReply(selective=True)
        )
        print("✏️ En attente de correction par texte avec ForceReply.")


async def receive_correction_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if user_id not in last_transcription or not last_transcription[user_id].get("awaiting_correction"):
        await update.message.reply_text("Veuillez d'abord envoyer un vocal initial et cliquer sur ✅ Oui, corriger.")
        return

    if update.message.voice:
        await update.message.reply_text(
            "⚠️ Les corrections par message vocal ne sont pas acceptées.\n"
            "Veuillez copier le texte grisé au-dessus, le modifier et l'envoyer par écrit."
        )
        return

    data = last_transcription[user_id]
    print("💬 Correction reçue par TEXTE")
    
    correction_text = update.message.text if update.message.text else ""
    hypothesis = data["transcription_initiale"]

    # Calcul des métriques
    wer, cer = calculate_metrics(reference=correction_text, hypothesis=hypothesis)

    # Sauvegarde finale
    elastic_service.save_transcription(
        audio_initial=data["audio_initial"],
        hypothesis=hypothesis,
        correction=correction_text,
        wer=wer,
        cer=cer
    )
    kafka_service.publish(
        "transcription.completed",
        build_transcription_completed_message(
            audio_id=data.get("audio_id", "unknown"),
            user_id=str(user_id),
            text=correction_text,
            bucket=minio_service.bucket_name,
            object_name=data.get("audio_id", "unknown") + ".wav",
        ),
        key=str(user_id),
    )
    
    last_transcription.pop(user_id, None)

    feedback_msg = (
        f"✅ Correction enregistrée !\n\n"
        f"📝 Texte final retenu :\n{correction_text}"
    )
    await update.message.reply_text(feedback_msg)
    print("✅ Processus terminé. Données épurées enregistrées dans ELK.")