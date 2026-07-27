# Architecture du dépôt DataBot

## Flux cible

```text
Telegram
  -> audio.uploaded
  -> MinIO Sink Connector -> MinIO
  -> ASR Worker -> Whisper API
  -> audio.transcribed
  -> Telegram Bot
  -> transcription.corrected
  -> Elasticsearch Sink Connector -> Elasticsearch
  -> Kibana
```

## Composants

### `main.py`

Point d'entrée unique du bot Telegram. Il enregistre :

- `receive_voice` pour les messages vocaux ;
- `button_handler` pour les boutons de validation et correction ;
- `receive_correction_input` pour le texte corrigé ;
- `consume_asr_results` pour consommer `audio.transcribed`.

### `bot/handlers.py`

- Télécharge temporairement le fichier OGG depuis Telegram.
- Publie les octets audio sur `audio.uploaded` via `KafkaService.publish_audio`.
- Place les métadonnées dans les headers Kafka.
- Publie les décisions utilisateur sur `transcription.corrected`.
- Ne contacte pas directement Elasticsearch.

### `bot/asr_consumer.py`

Consomme `audio.transcribed` avec le groupe `bot-asr-consumer-group`, mémorise la transcription initiale et envoie les boutons Telegram à l'utilisateur.

### `asr_worker.py`

Consomme `audio.uploaded` avec le groupe `whisper-worker-group`, écrit temporairement les octets audio, appelle `WhisperService`, puis publie le résultat sur `audio.transcribed`.

### `services/whisper_service.py`

Appelle l'endpoint Whisper configuré par `WHISPER_ENDPOINT`. La clé facultative `WHISPER_API_KEY`, la langue et le délai sont lus depuis `.env`.

### `services/kafka_service.py`

Définit les topics :

- `audio.uploaded` ;
- `audio.transcribed` ;
- `transcription.corrected`.

`publish_audio` publie les octets audio comme valeur Kafka et les métadonnées comme headers.

### MinIO Sink Connector

La configuration est dans `connectors/minio-sink.json`. Le connecteur doit consommer `audio.uploaded` et écrire les octets dans le bucket `audio-archive`.

Le format binaire est documenté dans `docs/kafka-contracts.md`. Le nom exact produit par un S3 Sink Connector doit être vérifié empiriquement, car les headers ne garantissent pas à eux seuls le nom de l'objet final.

### Elasticsearch Sink Connector

La configuration est dans `connectors/elasticsearch-sink.json`. Le connecteur doit consommer `transcription.corrected` et écrire les documents dans l'index Elasticsearch `transcription.corrected`.

### Kibana

Kibana doit utiliser un index pattern basé sur `transcription.corrected` pour afficher les documents indexés.

## Configuration

Les paramètres sont chargés depuis `.env` par `config/settings.py` :

- `TELEGRAM_BOT_TOKEN` ;
- `KAFKA_BOOTSTRAP_SERVERS` ;
- `WHISPER_ENDPOINT` et `WHISPER_API_KEY` ;
- `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET_NAME` ;
- `ELASTIC_URL`, `ELASTIC_INDEX_NAME` et les credentials Elasticsearch éventuels.

Les workers Kafka Connect doivent recevoir séparément les variables MinIO et Elasticsearch. Le `.env` du bot ne suffit pas à configurer Kafka Connect.

## Contrats Kafka

Les contrats détaillés sont définis dans `docs/kafka-contracts.md`.

Résumé :

- `audio.uploaded` : valeur binaire, headers de métadonnées, key `message_id` ;
- `audio.transcribed` : valeur JSON avec `message_id`, `user_id`, `audio_url`, `object_name` et `transcription_initiale` ;
- `transcription.corrected` : valeur JSON avec les transcriptions, `wer`, `cer` et `status`.

## Validation

L'état des vérifications réelles est consigné dans `docs/e2e-validation-report.md`. Le code ne doit être déclaré conforme qu'après :

1. un statut `RUNNING` des deux connecteurs Kafka Connect ;
2. une preuve d'objet écrit dans MinIO ;
3. une preuve de message `audio.transcribed` ;
4. une preuve de document indexé dans Elasticsearch ;
5. une preuve de document visible dans Kibana.
