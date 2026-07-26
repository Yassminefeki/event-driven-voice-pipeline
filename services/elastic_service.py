import logging
from elasticsearch import Elasticsearch
from config.settings import ELASTIC_PASSWORD, ELASTIC_URL, ELASTIC_USERNAME, INDEX_NAME

logger = logging.getLogger(__name__)


class ElasticService:

    def __init__(self):
        basic_auth = None
        if ELASTIC_USERNAME and ELASTIC_PASSWORD:
            basic_auth = (ELASTIC_USERNAME, ELASTIC_PASSWORD)

        self.client = Elasticsearch(
            hosts=[ELASTIC_URL],
            basic_auth=basic_auth,
            request_timeout=15,
        )

    def index_document(self, document_id: str, document: dict) -> bool:
        """Indexes or updates a correction document in Elasticsearch."""
        try:
            res = self.client.index(
                index=INDEX_NAME,
                id=document_id,
                document=document,
            )
            logger.info(f"✅ Indexed document {document_id} into '{INDEX_NAME}'. Result: {res.get('result')}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to index document {document_id} in Elasticsearch: {e}")
            return False