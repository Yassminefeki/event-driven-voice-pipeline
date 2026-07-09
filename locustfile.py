import os
import sys
from locust import User, task, between

# S'assure que Locust trouve tes modules locaux
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

class LocalArchitectureStressTest(User):
    wait_time = between(1.0, 3.0)

    @task
    def simulate_bot_processing(self):
        audio_path = "audio.wav"

        if not os.path.exists(audio_path):
            print(f"❌ ALERTE : Le fichier '{audio_path}' est INTROUVABLE.")
            self.environment.runner.quit()
            return

        # 1. Importation globale des fichiers
        try:
            from services.whisper_service import WhisperService
            from services.minio_service import MinioService
            from services.elastic_service import ElasticService
            from utils.metrics import calculate_metrics
        except ImportError as e:
            print(f"❌ Erreur d'importation d'un des fichiers de service : {e}")
            self.environment.runner.quit()
            return

        # 2. Simulation de l'architecture
        try:
            print("🤖 Début d'une simulation utilisateur...")

            # Initialisation des services
            minio_service = MinioService()
            elastic_service = ElasticService()

            # -- ÉTAPE A : Upload MinIO --
            with self.environment.events.request.measure("Architecture_Pipeline", "1_MinIO_Upload"):
                minio_service.upload_audio(audio_path)

            # -- ÉTAPE B : Transcription Whisper --
            with self.environment.events.request.measure("Architecture_Pipeline", "2_Whisper_Transcription"):
                text_transcribed = WhisperService.transcribe(audio_path)

            # -- ÉTAPE C : Calcul WER / CER --
            with self.environment.events.request.measure("Architecture_Pipeline", "3_Metrics_Calculation"):
                reference_text = "votre texte de reference attendu" 
                wer, cer = calculate_metrics(reference_text, text_transcribed)

            # -- ÉTAPE D : Sauvegarde Elasticsearch --
            with self.environment.events.request.measure("Architecture_Pipeline", "4_Elasticsearch_Save"):
                elastic_service.save_transcription(
                    audio_initial=audio_path, 
                    hypothesis=text_transcribed, 
                    correction="",
                    wer=wer,
                    cer=cer
                )

            print("✅ Cycle complet terminé avec succès !")

        except Exception as e:
            print(f"💥 Le pipeline a planté : {e}")