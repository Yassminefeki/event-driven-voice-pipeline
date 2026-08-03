import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from bot.db.object_name_store import ObjectNameStore  # noqa: E402


def _make_store() -> ObjectNameStore:
    tmp_dir = tempfile.mkdtemp()
    db_path = os.path.join(tmp_dir, "test_object_name_store.sqlite3")
    return ObjectNameStore(db_path=db_path)


def test_resolve_unknown_message_id_returns_none():
    store = _make_store()
    assert store.resolve("does-not-exist") is None


def test_upsert_then_resolve_returns_mapping():
    store = _make_store()
    store.upsert("msg-1", "audio-archive/msg-1.ogg", bucket="audio-archive")

    result = store.resolve("msg-1")
    assert result == {"object_name": "audio-archive/msg-1.ogg", "bucket": "audio-archive"}


def test_upsert_overwrites_existing_mapping():
    store = _make_store()
    store.upsert("msg-1", "old-object-name")
    store.upsert("msg-1", "new-object-name")

    result = store.resolve("msg-1")
    assert result["object_name"] == "new-object-name"


def test_delete_removes_mapping():
    store = _make_store()
    store.upsert("msg-1", "some-object")
    store.delete("msg-1")

    assert store.resolve("msg-1") is None
