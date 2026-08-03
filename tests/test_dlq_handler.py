import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "asr-worker"))

from whisper_worker import _parse_payload  # noqa: E402


def _make_valid_payload_bytes() -> bytes:
    import base64
    import json

    payload = {
        "message_id": "abc-123",
        "chat_id": 42,
        "audio_base64": base64.b64encode(b"fake-audio-bytes").decode("utf-8"),
    }
    return json.dumps(payload).encode("utf-8")


def test_valid_payload_parses_without_error():
    raw = _make_valid_payload_bytes()
    payload, error = _parse_payload(raw)
    assert error is None
    assert payload["message_id"] == "abc-123"


def test_corrupted_json_returns_error():
    raw = b"{not valid json"
    payload, error = _parse_payload(raw)
    assert payload is None
    assert "JSON invalide" in error


def test_missing_required_key_returns_error():
    import json

    raw = json.dumps({"message_id": "abc-123", "chat_id": 42}).encode("utf-8")
    payload, error = _parse_payload(raw)
    assert payload is None
    assert "audio_base64" in error


def test_invalid_base64_returns_error():
    import json

    raw = json.dumps(
        {"message_id": "abc-123", "chat_id": 42, "audio_base64": "not-valid-base64!!!"}
    ).encode("utf-8")
    payload, error = _parse_payload(raw)
    assert payload is None
    assert "Base64 invalide" in error
