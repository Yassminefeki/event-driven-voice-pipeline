from telegram import Update
from telegram.ext import Application, MessageHandler, CallbackQueryHandler, filters

from config.settings import TOKEN
from bot.handlers import receive_voice, button_handler, receive_correction_input

def main():
    # Initialisation de l'application Telegram
    app = Application.builder().token(TOKEN).build()

    # Déclaration des écouteurs (Handlers)
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.VOICE, receive_voice))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, receive_correction_input))

    print("Bot démarré...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()