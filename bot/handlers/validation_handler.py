"""
validation_handler.py

Etapes 11-13 :
11. Le bot envoie le clavier interactif (Valider / Corriger) avec la transcription.
12. L'utilisateur valide ou soumet une correction texte.
13. Le bot publie l'evenement final avec les metriques WER/CER sur transcription.corrected.
"""

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from bot.producers.kafka_producer import BotKafkaProducer

logger = logging.getLogger(__name__)

producer = BotKafkaProducer()

VALIDATE_CALLBACK_PREFIX = "validate:"
CORRECT_CALLBACK_PREFIX = "correct:"


def _levenshtein(a: list, b: list) -> int:
    """Distance de Levenshtein classique (utilisee pour WER et CER)."""
    m, n = len(a), len(b)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            dp[i][j] = min(
                dp[i - 1][j] + 1,      # suppression
                dp[i][j - 1] + 1,      # insertion
                dp[i - 1][j - 1] + cost,  # substitution
            )
    return dp[m][n]


def compute_wer(reference: str, hypothesis: str) -> float:
    """Word Error Rate = distance d'edition au niveau mot / nb de mots de reference."""
    ref_words = reference.split()
    hyp_words = hypothesis.split()
    if not ref_words:
        return 0.0 if not hyp_words else 1.0
    return _levenshtein(ref_words, hyp_words) / len(ref_words)


def compute_cer(reference: str, hypothesis: str) -> float:
    """Character Error Rate = distance d'edition au niveau caractere / nb de caracteres."""
    if not reference:
        return 0.0 if not hypothesis else 1.0
    return _levenshtein(list(reference), list(hypothesis)) / len(reference)


async def send_transcription_for_review(
    application, chat_id: int, message_id: str, transcription: str, audio_url: str | None
) -> None:
    """Etape 11 : envoie la transcription + clavier interactif a l'utilisateur.

    `application` est l'instance telegram.ext.Application (donne acces a
    application.bot et application.bot_data), passee depuis le consumer
    Kafka qui tourne hors du cycle de vie normal d'un Update Telegram.
    """
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Valider", callback_data=f"{VALIDATE_CALLBACK_PREFIX}{message_id}"),
                InlineKeyboardButton("✏️ Corriger", callback_data=f"{CORRECT_CALLBACK_PREFIX}{message_id}"),
            ]
        ]
    )

    text = f"📝 Transcription :\n\n{transcription}"
    if audio_url:
        text += f"\n\n🔗 Audio original : {audio_url}"
    else:
        # Race condition possible : audio.transcribed peut arriver avant audio.stored.
        text += "\n\n(Le lien vers l'audio original sera bientot disponible.)"

    application.bot_data.setdefault("pending_transcriptions", {})[message_id] = {
        "chat_id": chat_id,
        "transcription": transcription,
    }

    await application.bot.send_message(chat_id=chat_id, text=text, reply_markup=keyboard)


async def handle_validate_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Etape 12-13 : l'utilisateur valide la transcription telle quelle."""
    query = update.callback_query
    await query.answer()

    message_id = query.data.removeprefix(VALIDATE_CALLBACK_PREFIX)
    pending = context.bot_data.get("pending_transcriptions", {}).pop(message_id, None)
    if not pending:
        await query.edit_message_text("Cette transcription n'est plus disponible.")
        return

    transcription = pending["transcription"]
    chat_id = pending["chat_id"]

    # Validation = pas de correction -> WER/CER = 0
    producer.publish_transcription_corrected(
        message_id=message_id,
        chat_id=chat_id,
        original_text=transcription,
        corrected_text=transcription,
        wer=0.0,
        cer=0.0,
    )

    await query.edit_message_text(f"✅ Transcription validee :\n\n{transcription}")


async def handle_correction_text(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: str) -> None:
    """Etape 12-13 : l'utilisateur soumet un texte corrige (recu via un message texte
    apres avoir clique sur "Corriger"). Calcule WER/CER puis publie."""
    pending = context.bot_data.get("pending_transcriptions", {}).pop(message_id, None)
    if not pending:
        await update.message.reply_text("Cette transcription n'est plus disponible pour correction.")
        return

    original_text = pending["transcription"]
    corrected_text = update.message.text
    chat_id = pending["chat_id"]

    wer = compute_wer(original_text, corrected_text)
    cer = compute_cer(original_text, corrected_text)

    producer.publish_transcription_corrected(
        message_id=message_id,
        chat_id=chat_id,
        original_text=original_text,
        corrected_text=corrected_text,
        wer=wer,
        cer=cer,
    )

    await update.message.reply_text(
        f"✏️ Correction enregistree.\nWER: {wer:.2%} | CER: {cer:.2%}"
    )
