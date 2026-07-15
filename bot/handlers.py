import os
import uuid

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ForceReply
from telegram.ext import ContextTypes

from bot.memory import last_transcription
from services.minio_service import MinioService
from services.whisper_service import WhisperService
from services.kafka_service import (
    KafkaService,
    build_audio_uploaded_message,
    build_transcription_completed_message,
)
from utils.metrics import calculate_metrics


minio_service = MinioService()
kafka_service = KafkaService()


async def receive_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user_id = update.effective_user.id

    print(f"🎤 Premier vocal reçu de l'utilisateur {user_id}")


    if user_id in last_transcription and last_transcription[user_id].get("awaiting_correction"):
        await receive_correction_input(update, context)
        return


    voice = update.message.voice

    audio_id = str(uuid.uuid4())

    file_name = f"audio_{audio_id}.wav"


    file = await context.bot.get_file(voice.file_id)

    await file.download_to_drive(file_name)


    try:

        object_name = f"{audio_id}.wav"


        audio_url = minio_service.upload_audio(
            file_name,
            object_name=object_name
        )


        payload = build_audio_uploaded_message(
            audio_id=audio_id,
            user_id=str(user_id),
            bucket=minio_service.bucket_name,
            object_name=object_name,
            filename=file_name,
        )


        kafka_service.publish(
            "audio.uploaded",
            payload,
            key=str(user_id)
        )


        transcription = WhisperService.transcribe(file_name)


    finally:

        if os.path.exists(file_name):
            os.remove(file_name)



    last_transcription[user_id] = {

        "audio_initial": audio_url,

        "audio_id": audio_id,

        "object_name": object_name,

        "transcription_initiale": transcription,

        "awaiting_correction": False

    }



    keyboard = [

        [

            InlineKeyboardButton(
                "✅ Oui, corriger",
                callback_data="correct"
            ),

            InlineKeyboardButton(
                "❌ Non, garder",
                callback_data="keep"
            )

        ]

    ]


    await update.message.reply_text(

        f"📝 Transcription :\n\n{transcription}\n\n"
        "Est-ce que ce vocal contient des erreurs ?",

        reply_markup=InlineKeyboardMarkup(keyboard)

    )


    print("✅ Message avec boutons envoyé")





async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):

    query = update.callback_query

    await query.answer()


    user_id = query.from_user.id


    if user_id not in last_transcription:

        await query.edit_message_text(
            "❌ Aucun vocal trouvé."
        )

        return


    data = last_transcription[user_id]



    if query.data == "keep":


        payload = build_transcription_completed_message(

            audio_url=data["audio_initial"],

            

            transcription_initiale=data["transcription_initiale"],

            correction=data["transcription_initiale"],

            wer=0.0,

            cer=0.0,

        )


        kafka_service.publish(

            "transcription.completed",

            payload,

            key=str(user_id)

        )


        last_transcription.pop(user_id,None)


        await query.edit_message_text(
            "✅ Transcription enregistrée sans modification."
        )


        print("✅ Envoyé vers Kafka uniquement")




    elif query.data == "correct":


        data["awaiting_correction"] = True


        await query.edit_message_text(
            "✏️ Mode Correction Activé"
        )


        await context.bot.send_message(

            chat_id=query.message.chat_id,

            text=(

                "👇 Copiez le texte ci-dessous, "
                "corrigez-le puis envoyez-le :\n\n"

                f"`{data['transcription_initiale']}`"

            ),

            parse_mode="Markdown",

            reply_markup=ForceReply(selective=True)

        )





async def receive_correction_input(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user_id = update.effective_user.id


    if user_id not in last_transcription:

        await update.message.reply_text(
            "Veuillez envoyer un vocal d'abord."
        )

        return



    data = last_transcription[user_id]


    correction_text = update.message.text


    hypothesis = data["transcription_initiale"]


    wer, cer = calculate_metrics(

        reference=correction_text,

        hypothesis=hypothesis

    )



    payload = build_transcription_completed_message(

        audio_url=data["audio_initial"],
        
        transcription_initiale=hypothesis,
        correction=correction_text,

        wer=wer,

        cer=cer,



    )


    kafka_service.publish(

        "transcription.completed",

        payload,

        key=str(user_id)

    )



    last_transcription.pop(user_id,None)



    await update.message.reply_text(

        "✅ Correction enregistrée !\n\n"
        f"📝 Texte final :\n{correction_text}"

    )


    print("✅ Un seul message Kafka envoyé")
