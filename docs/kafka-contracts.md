# Contrats Kafka DataBot

Version : 1.0
Date : 2026-07-23

Ce document définit les contrats officiels des topics Kafka utilisés par DataBot.

## `audio.uploaded`

Ce topic transporte un fichier audio brut vers plusieurs consommateurs indépendants :

- le MinIO Sink Connector, qui écrit l'audio dans le bucket `audio-archive` ;
- l'ASR Worker, qui envoie le même audio à la Whisper API.

### Contrat de message

```text
Kafka key     : message_id encodé en UTF-8
Kafka value   : octets audio bruts, sans JSON ni Base64
Kafka headers :
  message_id  : identifiant du message Telegram
  user_id     : identifiant Telegram de l'utilisateur
  bucket      : nom du bucket MinIO cible
  object_name : nom logique de l'objet audio
  content_type: type MIME de l'audio, actuellement audio/ogg
```

La valeur Kafka est volontairement binaire. Ce format évite la surcharge du Base64 et permet au Sink Connector binaire et à l'ASR Worker de consommer exactement les mêmes octets. Les métadonnées nécessaires à la corrélation et au stockage sont dans les headers.

Ce contrat remplace l'ancien modèle JSON contenant un champ `file_content`. Aucun consumer de `audio.uploaded` ne doit tenter de désérialiser la value en JSON.

## `audio.transcribed`

Ce topic transporte le résultat produit par l'ASR Worker.

### Value JSON

```json
{
  "message_id": "123",
  "user_id": "456",
  "audio_url": "http://minio:9000/audio-archive/123.ogg",
  "object_name": "123.ogg",
  "transcription_initiale": "texte transcrit"
}
```

La key Kafka est `message_id`.

Le bot Telegram consomme ce topic avec le groupe `bot-asr-consumer-group`.

## `transcription.corrected`

Ce topic transporte la décision de l'utilisateur après validation ou correction.

### Value JSON

```json
{
  "message_id": "123",
  "user_id": "456",
  "audio_url": "http://minio:9000/audio-archive/123.ogg",
  "transcription_initiale": "texte transcrit",
  "transcription_corrigee": "texte corrigé",
  "wer": 0.12,
  "cer": 0.08,
  "status": "corrected"
}
```

`status` vaut :

- `kept` lorsque l'utilisateur valide la transcription initiale ;
- `corrected` lorsque l'utilisateur fournit une correction.

La key Kafka est `message_id`. Le message est destiné à l'Elasticsearch Sink Connector.

## Validation requise

La présence de ces contrats dans le code ne prouve pas le fonctionnement de l'infrastructure. Une validation doit encore confirmer :

- la valeur et les headers réellement présents dans `audio.uploaded` ;
- l'écriture du fichier par MinIO Sink ;
- la publication de `audio.transcribed` par le worker ;
- l'indexation de `transcription.corrected` dans Elasticsearch.
