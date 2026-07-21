# Architecture du dépôt DataBot

Ce fichier décrit les fichiers principaux du dépôt, la logique Kafka et le flux de données selon le code existant.

## Vue d’ensemble

Ce dépôt est un bot Telegram qui reçoit des messages vocaux, les stocke dans MinIO, les transcrit avec Whisper, et utilise Kafka pour diffuser les événements. Il contient aussi des outils de tests / charge et un service Elasticsearch partiellement implémenté.

---

## Fichiers principaux

### `main.py`
- Point d’entrée du bot Telegram.
- Crée l’application Telegram avec `python-telegram-bot`.
- Déclare trois handlers :
  - `CallbackQueryHandler(button_handler)` pour les boutons de correction.
  - `MessageHandler(filters.VOICE, receive_voice)` pour les messages vocaux.
  - `MessageHandler(filters.TEXT & ~filters.COMMAND, receive_correction_input)` pour les textes de correction.
- Lance le polling.

---

## Configuration

### `config/settings.py`
- Charge un fichier `.env` si présent.
- Définit les variables de configuration utilisées par le bot et les services :
  - `TOKEN` : jeton Telegram
  - `WHISPER_ENDPOINT` : URL du service Whisper
  - `MINIO_URL`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_SECURE`, `BUCKET_NAME`
  - `ELASTIC_URL`, `INDEX_NAME`
- La configuration Kafka se trouve plutôt dans `services/kafka_service.py`.

---

## Mémoire temporaire

### `bot/memory.py`
- Contient `last_transcription = {}`.
- Stocke temporairement par utilisateur :
  - URL audio
  - `audio_id`
  - `object_name`
  - transcription initiale
  - état `awaiting_correction`
- C’est un cache en mémoire, donc non persistant.

---

## Services

### `services/kafka_service.py`
- `KafkaService` :
  - crée un `KafkaProducer` avec les serveurs de bootstrap Kafka.
  - sérialise la clé en UTF-8 et le message JSON en UTF-8.
  - `publish(topic, message, key=None)` envoie un message puis flush.
- Fonctions de construction de payloads :
  - `build_audio_uploaded_message(...)`
    - Renvoie un JSON contenant `audio_id`, `user_id`, `bucket`, `object_name`, `filename`.
    - Destiné au topic `audio.uploaded`.
  - `build_transcription_completed_message(...)`
    - Renvoie JSON avec `audio_url`, `transcription_initiale`, `correction`, `wer`, `cer`.
    - Destiné au topic `transcription.completed`.

### `services/minio_service.py`
- Initialise un client MinIO avec la configuration.
- `upload_audio(file_path, object_name)` :
  - téléverse un fichier WAV dans le bucket.
  - renvoie une URL HTTP vers l’objet.
- `_ensure_bucket()` :
  - crée le bucket si nécessaire.
- `download_audio(...)` existe aussi pour récupérer un objet.

### `services/whisper_service.py`
- `WhisperService.transcribe(file_path)` :
  - envoie le fichier audio via HTTP POST à `WHISPER_ENDPOINT`.
  - utilise `language=ar` dans le corps de la requête.
  - récupère `response.json()["text"]`.
- Ce service est utilisé par le bot et par le worker.

### `services/elastic_service.py`
- Classe pour Elasticsearch.
- `save_transcription(...)` prépare un document et imprime un message.
- L’appel réel à `self.es.index(...)` est commenté, donc ce service est pour l’instant un stub.

### `utils/metrics.py`
- `calculate_metrics(reference, hypothesis)` :
  - calcule WER et CER avec `jiwer`.
  - retourne `(wer, cer)`.
  - en cas d’erreur, retourne `(-1.0, -1.0)`.

---

## Logique de Kafka et flux de données

### Flux principal du bot (`bot/handlers.py`)
1. L’utilisateur envoie un message vocal.
2. `receive_voice` :
   - télécharge le fichier vocal localement via `context.bot.get_file(...)`.
   - génère un `audio_id` UUID et un nom d’objet MinIO.
   - upload dans MinIO via `minio_service.upload_audio(...)`.
   - construit le payload `audio.uploaded`.
   - publie sur Kafka avec `kafka_service.publish("audio.uploaded", payload, key=user_id)`.
   - transcrit immédiatement le fichier avec `WhisperService.transcribe(file_name)`.
   - stocke l’état temporaire dans `last_transcription[user_id]`.
   - envoie à l’utilisateur la transcription et propose :
     - `✅ Oui, corriger`
     - `❌ Non, garder`

3. Si l’utilisateur clique `keep` :
   - construit un payload `transcription.completed` avec :
     - `audio_url`
     - `transcription_initiale`
     - `correction` identique à la transcription initiale
     - `wer=0.0`, `cer=0.0`
   - publie sur `transcription.completed`.
   - supprime l’état utilisateur de `last_transcription`.

4. Si l’utilisateur clique `correct` :
   - passe l’état `awaiting_correction = True`.
   - lui demande de renvoyer le texte corrigé en réponse.

5. Quand l’utilisateur envoie le texte corrigé :
   - `receive_correction_input` calcule WER/CER entre `correction_text` et `transcription_initiale`.
   - publie `transcription.completed` avec la correction finale.
   - supprime l’état utilisateur.

### Kafka : topics et messages attendus
- Topic `audio.uploaded`
  - message envoyé par le bot après upload MinIO.
  - contient les métadonnées audio.
- Topic `transcription.completed`
  - message envoyé par le bot après validation ou correction.
  - contient le texte final et les métriques.

---

## Worker Kafka et double flux

### `whisper_worker.py`
- Consomme le topic `audio.uploaded`.
- Pour chaque message :
  1. lit `audio_id`, `user_id`, `bucket`, `object_name`.
  2. télécharge l’audio depuis MinIO localement.
  3. transcrit avec Whisper.
  4. construit un message `transcription.completed`.
  5. publie sur Kafka.
- Ce worker représente un pipeline asynchrone alternatif.
- Attention : le code du worker est actuellement incohérent avec la signature de `build_transcription_completed_message` dans `services/kafka_service.py`.
  - Le worker passe `audio_id`, `user_id`, `bucket`, `object_name` alors que la fonction attend `audio_url`, `transcription_initiale`, `correction`, `wer`, `cer`.
  - Cela signifie que ce worker ne fonctionne pas correctement en l’état.

### Interprétation du flux global
- Il y a deux façons de produire `transcription.completed` :
  - le bot lui-même (processus synchrone avec correction utilisateur)
  - le worker Kafka (processus asynchrone basé sur `audio.uploaded`), mais ce workflow est en conflit avec l’implémentation actuelle du bot.
- Le pipeline idéal selon l’architecture :
  - client -> bot -> MinIO + Kafka `audio.uploaded`
  - worker -> prend `audio.uploaded` -> transcrit -> Kafka `transcription.completed`
  - éventuellement un service de consommation `transcription.completed` enregistre dans Elasticsearch ou autre.
- Dans la version actuelle, le bot fait déjà la transcription avant même d’attendre le worker.

---

## Tests et scripts annexes

### `tests/test_kafka_payloads.py`
- Contient deux tests unitaires sur les payloads Kafka.
- Ces tests sont incorrects par rapport à l’implémentation actuelle :
  - ils vérifient `payload["topic"]`, alors que les fonctions ne mettent pas de champ `topic` dans le message.
  - `test_transcription_payload_uses_expected_topic` appelle `build_transcription_completed_message` avec des arguments différents de sa signature réelle.
- En l’état, ces tests sont probablement cassés.

### `locustfile.py`
- Script de test de charge avec Locust.
- Simule un utilisateur qui :
  1. charge un audio dans MinIO
  2. appelle `WhisperService.transcribe`
  3. calcule des métriques
  4. enregistre dans Elasticsearch
- Ce fichier sert à mesurer le pipeline mais n’est pas utilisé dans le bot Telegram.

### `miniotest.py`
- Script de stress test MinIO.
- Envoie répétitivement des fichiers audio vers MinIO.
- Utile pour valider la capacité d’upload.

---

## Points importants

- Le dépôt est centré sur un bot Telegram + Kafka + MinIO + Whisper.
- Kafka est utilisé comme bus d’événements :
  - `audio.uploaded` pour l’audio stocké
  - `transcription.completed` pour le texte final
- `MinIO` stocke les fichiers audios.
- `WhisperService` transcrit les audios via une API externe.
- La correction utilisateur est faite via Telegram interactive buttons + reply.
- `ElasticService` est présent mais pas réellement actif.
- `last_transcription` est un stockage temporaire en mémoire, donc pas fiable sur redémarrage.

---

## Résumé rapide

- `main.py` : démarre le bot Telegram.
- `bot/handlers.py` : gère les voix, bouton de correction, et publication Kafka.
- `bot/memory.py` : mémoire temporaire des transcriptions par utilisateur.
- `config/settings.py` : configuration générale.
- `services/kafka_service.py` : wrapper Kafka + construction de messages.
- `services/minio_service.py` : upload/download MinIO.
- `services/whisper_service.py` : transcription via Whisper HTTP.
- `services/elastic_service.py` : stub Elasticsearch.
- `utils/metrics.py` : calcul WER/CER.
- `whisper_worker.py` : consommateur Kafka `audio.uploaded` -> transcription -> `transcription.completed`.
- `tests/test_kafka_payloads.py` : tests incorrects / obsolètes.
- `locustfile.py` et `miniotest.py` : scripts de test de charge.
