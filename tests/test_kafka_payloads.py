import unittest

from services.kafka_service import (
    AUDIO_RAW_TOPIC,
    AUDIO_TRANSCRIBED_TOPIC,
    TRANSCRIPTION_CORRECTED_TOPIC,
    build_audio_transcribed_message,
    build_transcription_corrected_message,
)


class KafkaPayloadTests(unittest.TestCase):
    def test_topics_match_architecture(self):
        self.assertEqual(AUDIO_RAW_TOPIC, "audio.uploaded")
        self.assertEqual(AUDIO_TRANSCRIBED_TOPIC, "audio.transcribed")
        self.assertEqual(TRANSCRIPTION_CORRECTED_TOPIC, "transcription.corrected")

    def test_audio_transcribed_payload_contains_transcription(self):
        payload = build_audio_transcribed_message(
            message_id="message-123",
            user_id="u1",
            audio_url="http://minio/audio.wav",
            transcription_initiale="hello world",
        )

        self.assertEqual(payload["message_id"], "message-123")
        self.assertEqual(payload["user_id"], "u1")
        self.assertEqual(payload["audio_url"], "http://minio/audio.wav")
        self.assertEqual(payload["transcription_initiale"], "hello world")

    def test_transcription_corrected_payload_contains_metrics(self):
        payload = build_transcription_corrected_message(
            message_id="message-123",
            user_id="u1",
            audio_url="http://minio/audio.wav",
            transcription_initiale="hello world",
            correction="hello world",
            wer=0.0,
            cer=0.0,
            status="kept",
        )

        self.assertEqual(payload["message_id"], "message-123")
        self.assertEqual(payload["user_id"], "u1")
        self.assertEqual(payload["audio_url"], "http://minio/audio.wav")
        self.assertEqual(payload["transcription_corrigee"], "hello world")
        self.assertEqual(payload["wer"], 0.0)
        self.assertEqual(payload["cer"], 0.0)
        self.assertEqual(payload["status"], "kept")
