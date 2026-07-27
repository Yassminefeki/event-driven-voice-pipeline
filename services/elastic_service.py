"""
Elasticsearch indexing layer (step 14: stores transcriptions + metadata).
NOTE: in production this index is normally populated by the Elasticsearch
Sink Connector (step 13) consuming `transcription.corrected` directly —
this client is for manual queries / backfills / tests only.
"""
import logging

from elasticsearch import Elasticsearch

from config.settings import settings

logger = logging.getLogger(__name__)


class ElasticService:
    def __init__(self):
        self._client = Elasticsearch(settings.elastic_url)

    def index_document(self, message_id: str, document: dict) -> None:
        """message_id is used as the ES _id -> upsert semantics, no duplicates."""
        self._client.index(index=settings.elastic_index, id=message_id, document=document)

    def search(self, query: dict, size: int = 20) -> list[dict]:
        result = self._client.search(index=settings.elastic_index, query=query, size=size)
        return [hit["_source"] for hit in result["hits"]["hits"]]


elastic_service = ElasticService()
