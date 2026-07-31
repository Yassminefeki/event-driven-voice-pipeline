"""
Whisper API client (step 7: ASR Worker -> API, step 8: API -> ASR Worker).

CORRECTIF : sous forte charge, Whisper peut timeout ou répondre par une
erreur 5xx transitoire (surcharge, redémarrage, etc). Auparavant, la moindre
exception ici remontait telle quelle jusqu'au worker qui loggait et passait
au message suivant SANS retry -> perte du message vu le commit auto Kafka.
On ajoute donc un retry avec backoff exponentiel pour les erreurs jugées
transitoires, et on distingue explicitement l'échec définitif via
WhisperTranscriptionError pour que l'appelant puisse router vers la DLQ.
"""
import logging
import time

import requests

from config.settings import settings

logger = logging.getLogger(__name__)


class WhisperTranscriptionError(Exception):
    """Levée quand la transcription échoue définitivement (retries épuisés)."""


class WhisperService:
    def __init__(
        self,
        timeout_seconds: int = 60,
        long_timeout_seconds: int = 900,
        max_retries: int = 4,
        backoff_base_seconds: float = 1.0,
        backoff_max_seconds: float = 20.0,
    ):
        """
        timeout_seconds : timeout normal, pour un premier essai standard.
        long_timeout_seconds : timeout utilisé pour la SEULE tentative
            "longue" accordée après un premier timeout (voir transcribe()).
            Sous forte charge réelle, le pod Whisper peut légitimement mettre
            plusieurs minutes à traiter une requête à cause du backlog interne
            — ce n'est pas une panne, juste une file d'attente. Retenter en
            renvoyant l'audio dans ce cas ne ferait qu'aggraver la saturation
            (chaque retry recrée une requête entière = plus de travail pour
            le pod déjà débordé).
        """
        self.timeout_seconds = timeout_seconds
        self.long_timeout_seconds = long_timeout_seconds
        self.max_retries = max_retries
        self.backoff_base_seconds = backoff_base_seconds
        self.backoff_max_seconds = backoff_max_seconds

    def _is_retryable(self, exc: Exception) -> bool:
        """
        NE couvre plus les Timeout : un timeout sous forte charge signale
        probablement "encore en file d'attente", pas "en panne". Le retenter
        immédiatement dupliquerait le travail vers un pod déjà saturé. Les
        timeouts sont gérés séparément dans transcribe() via une unique
        tentative longue, pas via ce mécanisme de retry rapide.
        """
        if isinstance(exc, requests.exceptions.ConnectionError):
            return True
        if isinstance(exc, requests.exceptions.HTTPError):
            status = exc.response.status_code if exc.response is not None else None
            # 429 (rate limit) et 5xx (surcharge / erreur serveur) sont transitoires.
            # Les 4xx (hors 429) indiquent un problème définitif (fichier invalide, etc).
            return status == 429 or (status is not None and 500 <= status < 600)
        return False

    def _call(self, audio_bytes: bytes, filename: str, timeout: int) -> dict:
        response = requests.post(
            settings.whisper_endpoint,
            files={"file": (filename, audio_bytes)},
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()

    def _to_result(self, data: dict, start: float) -> dict:
        elapsed_ms = int((time.monotonic() - start) * 1000)
        return {
            "text": data.get("text", ""),
            "processing_time_ms": elapsed_ms,
            "model_version": data.get("model", "whisper"),
            "confidence_score": data.get("confidence", None),
        }

    def transcribe(self, audio_bytes: bytes, filename: str = "audio.ogg") -> dict:
        """
        Returns {"text": ..., "processing_time_ms": ..., "model_version": ...}
        Lève WhisperTranscriptionError si toutes les tentatives échouent.

        Stratégie :
        - Erreurs "rapides" (connexion refusée, 5xx, 429) : retry classique
          avec backoff exponentiel, jusqu'à max_retries. Ce sont des signaux
          de panne réelle, retenter vite a du sens.
        - Timeout : PAS de retry rapide. Un timeout sous charge réelle
          signifie très probablement "le pod travaille encore dessus, il y a
          juste un backlog" et non "c'est cassé". On accorde donc une unique
          tentative supplémentaire avec un timeout beaucoup plus long
          (long_timeout_seconds) plutôt que de renvoyer l'audio en boucle,
          ce qui ne ferait qu'ajouter du travail à un pod déjà saturé.
        """
        start = time.monotonic()
        last_exc: Exception | None = None
        used_long_timeout = False

        attempt = 0
        while True:
            attempt += 1
            try:
                data = self._call(audio_bytes, filename, timeout=self.timeout_seconds)
                return self._to_result(data, start)

            except requests.exceptions.Timeout as exc:
                last_exc = exc

                if used_long_timeout:
                    logger.error(
                        "Whisper: timeout persistant même avec timeout étendu "
                        "(%ds) -> échec définitif. Le pod est probablement "
                        "réellement saturé au-delà de sa capacité.",
                        self.long_timeout_seconds
                    )
                    raise WhisperTranscriptionError(str(exc)) from exc

                logger.warning(
                    "Whisper: timeout normal (%ds) — possible backlog côté pod, "
                    "on accorde UNE tentative longue (%ds) sans retry rapide "
                    "pour ne pas dupliquer la charge",
                    self.timeout_seconds, self.long_timeout_seconds
                )
                used_long_timeout = True
                try:
                    data = self._call(audio_bytes, filename, timeout=self.long_timeout_seconds)
                    return self._to_result(data, start)
                except requests.exceptions.Timeout as exc2:
                    last_exc = exc2
                    logger.error(
                        "Whisper: échec définitif après tentative longue (%ds): %s",
                        self.long_timeout_seconds, exc2
                    )
                    raise WhisperTranscriptionError(str(exc2)) from exc2
                except Exception as exc2:
                    # Erreur non-timeout pendant la tentative longue : repasse
                    # par la logique de retry normale ci-dessous.
                    last_exc = exc2
                    if not self._is_retryable(exc2) or attempt >= self.max_retries:
                        raise WhisperTranscriptionError(str(exc2)) from exc2
                    time.sleep(min(self.backoff_base_seconds * (2 ** (attempt - 1)), self.backoff_max_seconds))
                    continue

            except Exception as exc:
                last_exc = exc

                if not self._is_retryable(exc) or attempt >= self.max_retries:
                    logger.error(
                        "Whisper: échec définitif après %d tentative(s): %s",
                        attempt, exc
                    )
                    raise WhisperTranscriptionError(str(exc)) from exc

                delay = min(
                    self.backoff_base_seconds * (2 ** (attempt - 1)),
                    self.backoff_max_seconds,
                )
                logger.warning(
                    "Whisper: tentative %d/%d échouée (%s), retry dans %.1fs",
                    attempt, self.max_retries, exc, delay
                )
                time.sleep(delay)

        # Ne devrait jamais être atteint, filet de sécurité.
        raise WhisperTranscriptionError(str(last_exc))


whisper_service = WhisperService()