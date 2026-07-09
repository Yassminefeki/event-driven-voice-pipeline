import requests
from config.settings import WHISPER_ENDPOINT

class WhisperService:
    @staticmethod
    def transcribe(file_path: str) -> str:
        """Envoie le fichier audio à l'API Whisper pour transcription."""
        print("Envoi du vocal initial au serveur Whisper...")
        with open(file_path, "rb") as audio:
            response = requests.post(
                WHISPER_ENDPOINT,
                files={"file": ("audio.wav", audio, "audio/wav")},
                data={"language": "ar"},
                timeout=60
            )
        
        print("Réponse serveur Whisper (initial) :", response.text)
        result = response.json()
        return result.get("text", "").strip()