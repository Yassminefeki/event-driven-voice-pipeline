import httpx
import logging
from pathlib import Path
from config.settings import WHISPER_API_KEY, WHISPER_ENDPOINT, WHISPER_LANGUAGE, WHISPER_TIMEOUT

logger = logging.getLogger(__name__)

class WhisperService:
    @staticmethod
    async def transcribe(file_path: str) -> str:
        """Envoie le fichier audio à l'API Whisper pour transcription (Asynchrone)."""
        logger.info("Envoi du vocal initial au serveur Whisper...")
        
        try:
            headers = {}
            if WHISPER_API_KEY:
                headers["Authorization"] = f"Bearer {WHISPER_API_KEY}"

            async with httpx.AsyncClient(timeout=WHISPER_TIMEOUT) as client:
                with open(file_path, "rb") as audio:
                    suffix = Path(file_path).suffix.lower()
                    content_type = "audio/ogg" if suffix == ".ogg" else "audio/wav"
                    files = {"file": (f"audio{suffix or '.wav'}", audio, content_type)}
                    data = {"language": WHISPER_LANGUAGE}
                    
                    response = await client.post(
                        WHISPER_ENDPOINT, 
                        files=files, 
                        data=data,
                        headers=headers,
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
