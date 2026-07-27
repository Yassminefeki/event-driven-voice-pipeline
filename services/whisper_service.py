"""
Whisper API client (step 7: ASR Worker -> API, step 8: API -> ASR Worker).
"""
import logging
import time

import requests

from config.settings import settings

logger = logging.getLogger(__name__)


class WhisperService:
    def __init__(self, timeout_seconds: int = 60):
        self.timeout_seconds = timeout_seconds

    def transcribe(self, audio_bytes: bytes, filename: str = "audio.ogg") -> dict:
        """Returns {"text": ..., "processing_time_ms": ..., "model_version": ...}"""
        start = time.monotonic()
        response = requests.post(
            settings.whisper_endpoint,
            files={"file": (filename, audio_bytes)},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        elapsed_ms = int((time.monotonic() - start) * 1000)

        data = response.json()
        return {
            "text": data.get("text", ""),
            "processing_time_ms": elapsed_ms,
            "model_version": data.get("model", "whisper"),
            "confidence_score": data.get("confidence", None),
        }


whisper_service = WhisperService()
