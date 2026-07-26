import logging
import httpx
from config.settings import WHISPER_API_KEY, WHISPER_ENDPOINT, WHISPER_LANGUAGE, WHISPER_TIMEOUT

logger = logging.getLogger(__name__)


class WhisperService:

    @staticmethod
    async def transcribe(file_path: str) -> str:
        """Sends an audio file to the Whisper HTTP endpoint and returns the transcribed text."""
        headers = {}
        if WHISPER_API_KEY:
            headers["Authorization"] = f"Bearer {WHISPER_API_KEY}"

        data = {
            "language": WHISPER_LANGUAGE,
        }

        try:
            async with httpx.AsyncClient(timeout=WHISPER_TIMEOUT) as client:
                with open(file_path, "rb") as audio_file:
                    files = {"file": (file_path, audio_file, "audio/ogg")}
                    response = await client.post(
                        WHISPER_ENDPOINT,
                        headers=headers,
                        data=data,
                        files=files,
                    )

                if response.status_code == 200:
                    result = response.json()
                    # Handle common Whisper API response structures
                    transcription = result.get("text") or result.get("transcription") or ""
                    logger.info(f"✅ Transcription successful for {file_path}")
                    return transcription.strip()
                else:
                    logger.error(f"❌ Whisper API error ({response.status_code}): {response.text}")
                    return ""

        except Exception as e:
            logger.error(f"❌ Exception occurred during Whisper transcription: {e}")
            return ""