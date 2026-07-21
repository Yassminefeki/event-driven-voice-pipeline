import unittest

from services.kafka_service import (
    build_audio_uploaded_message,
    build_asr_completed_message,
    build_transcription_evaluated_message,
)


class KafkaPayloadTests(unittest.TestCase):
    def test_audio_payload_uses_expected_topic_and_fields(self):
        payload = build_audio_uploaded_message(
            message_id="message-123",
            user_id="u1",
            bucket="audio-archive",
            object_name="audio.wav",
            filename="audio.wav",
        )

        self.assertEqual(payload["topic"], "audio.uploaded")
        self.assertEqual(payload["message_id"], "message-123")
        self.assertEqual(payload["bucket"], "audio-archive")
        self.assertEqual(payload["object_name"], "audio.wav")
        self.assertEqual(payload["filename"], "audio.wav")

    def test_asr_completed_payload_contains_transcription(self):
        payload = build_asr_completed_message(
            message_id="message-123",
            user_id="u1",
            audio_url="http://minio/audio.wav",
            transcription_initiale="hello world",
        )

        self.assertEqual(payload["message_id"], "message-123")
        self.assertEqual(payload["user_id"], "u1")
        self.assertEqual(payload["audio_url"], "http://minio/audio.wav")
        self.assertEqual(payload["transcription_initiale"], "hello world")

    def test_transcription_evaluated_payload_uses_expected_topic(self):
        payload = build_transcription_evaluated_message(
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
        self.assertEqual(payload["correction"], "hello world")
        self.assertEqual(payload["wer"], 0.0)
        self.assertEqual(payload["cer"], 0.0)
        self.assertEqual(payload["status"], "kept")
