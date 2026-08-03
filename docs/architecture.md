# Architecture detaillee — DataBot

## Vue d'ensemble

DataBot est un pipeline evenementiel : chaque etape publie sur un topic Kafka
que l'etape suivante consomme, ce qui decouple totalement les composants et
permet de rejouer/monter en charge chacun independamment.

## Flux complet

```
[ Telegram User ]
       │ 1. Voice Msg
       ▼
 [ Bot Handler ] ── 2-3. Encode Base64 ──► [ Topic: audio.uploaded ]
                                                 │
              ┌──────────────────────────────────┴──────────────────────────────────┐
              ▼ (Etape 5)                                                           ▼ (Etape 6)
  [ Kafka Connect S3 Sink ]                                                [ ASR Whisper Worker ]
              │                                                                     │ 7-8. Whisper API
              ▼                                                                     ▼
     [ MinIO: audio-archive ]                                              [ Topic: audio.transcribed ]
              │                                                                     │
              ▼                                                                     ▼
  [ S3 Publisher Service ]                                                 [ Bot ASR Consumer ]
              │                                                                     │
              ▼                                                                     ▼
     [ Topic: audio.stored ]                                              [ User Valid/Correct ]
              │                                                                     │
              └─────────────────────────┬───────────────────────────────────────────┘
                                        ▼
                        [ Topic: transcription.corrected ]
                                        │
                                        ▼
                            [ Kafka Connect ES Sink ]
                                        │
                                        ▼
                              [ Elasticsearch / Kibana ]
```

## Topics Kafka

| Topic | Producteur | Consommateur | Contenu |
|---|---|---|---|
| `audio.uploaded` | Bot Telegram | MinIO Sink Connector, ASR Worker | `message_id`, `chat_id`, `audio_base64` |
| `audio.stored` | S3 Publisher Service | Bot (via `object_name_store`) | `message_id`, `object_name`, `bucket` |
| `audio.transcribed` | ASR Worker | Bot Telegram | `message_id`, `chat_id`, `transcription` |
| `audio.uploaded.dlq` | ASR Worker | (consumer DLQ a implementer — voir roadmap) | payload original + en-tetes d'erreur |
| `transcription.corrected` | Bot Telegram | ES Sink Connector | `message_id`, `chat_id`, `original_text`, `corrected_text`, `wer`, `cer` |

## Pourquoi ce decoupage ?

- **Tolerance aux pannes** : si Whisper est indisponible, les messages
  s'accumulent dans `audio.uploaded` sans etre perdus (retention Kafka) — le
  worker les traite des que le service revient.
- **Scalabilite independante** : on peut augmenter le nombre de partitions +
  instances de l'ASR Worker sans toucher au Bot Telegram.
- **Decouplage stockage/transcription** : le MinIO Sink et l'ASR Worker
  consomment `audio.uploaded` en parallele via des groupes de consommateurs
  distincts — un ralentissement de l'un n'affecte pas l'autre.
- **Idempotence** : `message_id` utilise comme `_id` Elasticsearch garantit
  qu'un rejeu (replay) ne cree pas de doublons.

Voir aussi [`incidents.md`](incidents.md) pour l'historique des problemes
rencontres sur cette architecture, et [`runbook.md`](runbook.md) pour les
procedures d'exploitation.
