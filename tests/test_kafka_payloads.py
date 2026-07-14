import unittest

from services.kafka_service import build_audio_uploaded_message, build_transcription_completed_message


class KafkaPayloadTests(unittest.TestCase):
    def test_audio_payload_uses_expected_topic_and_fields(self):
        payload = build_audio_uploaded_message(
            audio_id="audio-123",
            user_id="u1",
            bucket="audio-archive",
            object_name="audio.wav",
            filename="audio.wav",
        )

        self.assertEqual(payload["topic"], "audio.uploaded")
        self.assertEqual(payload["audio_id"], "audio-123")
        self.assertEqual(payload["bucket"], "audio-archive")
        self.assertEqual(payload["object_name"], "audio.wav")
        self.assertEqual(payload["filename"], "audio.wav")

    def test_transcription_payload_uses_expected_topic(self):
        payload = build_transcription_completed_message(
            audio_id="audio-123",
            user_id="u1",
            text="hello world",
            bucket="audio-archive",
            object_name="audio.wav",
        )

        self.assertEqual(payload["topic"], "transcription.completed")
        self.assertEqual(payload["audio_id"], "audio-123")
        self.assertEqual(payload["text"], "hello world")
