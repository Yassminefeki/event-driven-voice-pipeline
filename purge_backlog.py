"""
Script ONE-SHOT à lancer UNE SEULE FOIS (whisper_worker.py arrêté) pour
purger le backlog du topic audio.uploaded pour le consumer group du
worker Whisper (asr-worker-group).

Même logique que purge_backlog.py, mais appliqué au topic/groupe
responsable de la transcription (source du flot de faux messages
audio.transcribed observé côté bot).

USAGE :
    1. Arrête whisper_worker.py s'il tourne quelque part (ou vérifie
       qu'aucun membre actif n'existe déjà via --describe).
    2. python3 purge_backlog_uploaded.py
    3. Relance whisper_worker.py normalement.
"""

import logging

from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

from config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def purge_backlog(topic: str, group_id: str) -> None:
    consumer = KafkaConsumer(
        bootstrap_servers=list(settings.kafka_bootstrap_servers),
        group_id=group_id,
        enable_auto_commit=False,
    )

    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        logger.error("Topic introuvable ou vide: %s", topic)
        consumer.close()
        return

    tps = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(tps)

    consumer.seek_to_end(*tps)

    end_offsets = {tp: consumer.position(tp) for tp in tps}

    for tp, offset in end_offsets.items():
        logger.info(
            "Partition %s -> nouvel offset committé = %d",
            tp.partition, offset
        )

    consumer.commit({
        tp: OffsetAndMetadata(offset, None)
        for tp, offset in end_offsets.items()
    })

    logger.info(
        "Backlog purgé pour group_id=%s sur topic=%s (%d partitions)",
        group_id, topic, len(tps)
    )

    consumer.close()


if __name__ == "__main__":
    purge_backlog(
        topic=settings.topic_audio_uploaded,
        group_id=settings.kafka_group_id_worker,
    )