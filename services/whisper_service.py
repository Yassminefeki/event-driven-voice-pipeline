import httpx
import logging
from config.settings import WHISPER_ENDPOINT

logger = logging.getLogger(__name__)

class WhisperService:
    @staticmethod
    async def transcribe(file_path: str) -> str:
        """Envoie le fichier audio à l'API Whisper pour transcription (Asynchrone)."""
        logger.info("Envoi du vocal initial au serveur Whisper...")
        
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                with open(file_path, "rb") as audio:
                    files = {"file": ("audio.wav", audio, "audio/wav")}
                    data = {"language": "ar"}
                    
                    response = await client.post(
                        WHISPER_ENDPOINT, 
                        files=files, 
                        data=data
                    )

            logger.info(f"Réponse serveur Whisper: {response.status_code} - {response.text}")
            
            # Vérifie si la requête a réussi (200 OK)
            response.raise_for_status()
            
            result = response.json()
            return result.get("text", "").strip()

        except httpx.HTTPStatusError as e:
            logger.error(f"Erreur HTTP Whisper ({e.response.status_code}): {e.response.text}")
            return ""
        except Exception as e:
            logger.error(f"Erreur lors de la communication avec Whisper: {e}")
            return ""
