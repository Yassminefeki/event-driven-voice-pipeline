from elasticsearch import Elasticsearch
from config.settings import ELASTIC_URL, INDEX_NAME

class ElasticService:
    def __init__(self):
        self.es = Elasticsearch(ELASTIC_URL)

    def save_transcription(self, audio_initial: str, hypothesis: str, correction: str, wer: float, cer: float):
        """Enregistre le document complet de transcription dans l'index Elasticsearch."""
        document = {
            "audio_initial": audio_initial,
            "transcription_initiale": hypothesis,
            "correction": correction,
            "wer": wer,
            "cer": cer
        }
        #self.es.index(index=INDEX_NAME, body=document)
        print("✅ Données enregistrées dans ELK.")
