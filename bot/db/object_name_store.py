"""
object_name_store.py

Table SQLite `message_objects` reliant un message_id Telegram
a l'object_name reel ecrit dans MinIO (bucket audio-archive).

Utilise par le Bot Telegram (etape 11) pour resoudre l'URL du fichier audio
a partir de l'evenement audio.stored publie par le S3 Publisher Service (etape 6).
"""

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_PATH = os.environ.get("OBJECT_NAME_STORE_DB", "./data/object_name_store.sqlite3")


def _ensure_parent_dir(path: str) -> None:
    parent = Path(path).parent
    if str(parent) not in ("", "."):
        parent.mkdir(parents=True, exist_ok=True)


class ObjectNameStore:
    """Acces a la table message_objects (message_id -> object_name)."""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        _ensure_parent_dir(self.db_path)
        self._init_schema()

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS message_objects (
                    message_id   TEXT PRIMARY KEY,
                    object_name  TEXT NOT NULL,
                    bucket       TEXT NOT NULL DEFAULT 'audio-archive',
                    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )

    def upsert(self, message_id: str, object_name: str, bucket: str = "audio-archive") -> None:
        """Enregistre (ou met a jour) le mapping message_id -> object_name.

        Appele quand le bot consomme audio.stored (etape 6/11).
        """
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO message_objects (message_id, object_name, bucket)
                VALUES (?, ?, ?)
                ON CONFLICT(message_id) DO UPDATE SET
                    object_name = excluded.object_name,
                    bucket = excluded.bucket
                """,
                (message_id, object_name, bucket),
            )

    def resolve(self, message_id: str) -> dict | None:
        """Retourne {"object_name": ..., "bucket": ...} ou None si absent.

        Cas "non trouve" : le Bot doit gerer ce None gracieusement, par exemple
        en informant l'utilisateur que le fichier n'est pas encore archive
        (race condition possible entre audio.transcribed et audio.stored).
        """
        with self._connect() as conn:
            row = conn.execute(
                "SELECT object_name, bucket FROM message_objects WHERE message_id = ?",
                (message_id,),
            ).fetchone()
        if row is None:
            return None
        return {"object_name": row[0], "bucket": row[1]}

    def delete(self, message_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM message_objects WHERE message_id = ?", (message_id,))
