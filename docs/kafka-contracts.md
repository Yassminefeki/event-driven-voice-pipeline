# Kafka Contracts — Source of Truth

This document is the **only** authorized reference for topic names and payload
schemas. Any code that diverges from this file is a bug, not a variant.

## Topics (do not rename)

| Topic | Producer | Consumer(s) |
|---|---|---|
| `audio.uploaded` | Telegram Bot | MinIO Sink Connector, ASR Worker |
| `audio.transcribed` | ASR Worker | Telegram Bot |
| `transcription.corrected` | Telegram Bot | Elasticsearch Sink Connector |

## Correlation key

`message_id` is the Kafka record key on **every** topic. Never use `user_id`
or `chat_id` as the key — this was tried before and caused race conditions
when a user sent multiple voice messages in quick succession.

## Payloads

### `audio.uploaded`
```json
{
  "message_id": "uuid",
  "chat_id": 123456789,
  "user_id": 987654321,
  "telegram_file_id": "AwACAgQAAx...",
  "audio_url": "s3://audio-archive/<message_id>.ogg",
  "duration_seconds": 12,
  "timestamp": "2026-07-27T10:00:00Z"
}
```

### `audio.transcribed`
```json
{
  "message_id": "uuid",
  "chat_id": 123456789,
  "user_id": 987654321,
  "audio_url": "s3://audio-archive/<message_id>.ogg",
  "model_transcription": "text",
  "asr_model_version": "whisper-large-v3",
  "confidence_score": 0.94,
  "processing_time_ms": 420,
  "timestamp": "2026-07-27T10:00:01Z"
}
```

### `transcription.corrected`
```json
{
  "message_id": "uuid",
  "chat_id": 123456789,
  "user_id": 987654321,
  "audio_url": "s3://audio-archive/<message_id>.ogg",
  "model_transcription": "text",
  "user_correction": "corrected text",
  "wer": 0.0,
  "cer": 0.02,
  "is_edited": true,
  "timestamp": "2026-07-27T10:00:15Z"
}
```

## Before merging any change

- [ ] Topic name matches this file exactly
- [ ] `message_id` used as Kafka key on every `producer.send(...)`
- [ ] `producer.send(...)` result is awaited/blocked on — never fire-and-forget
- [ ] New fields added here first, then in code
