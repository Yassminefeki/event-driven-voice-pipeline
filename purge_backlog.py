"""
Script ONE-SHOT à lancer UNE SEULE FOIS (bot arrêté) pour purger le
backlog du topic audio.transcribed pour le consumer group du bot.

Ce script ne supprime PAS les messages du topic Kafka lui-même (ça
nécessiterait des droits admin sur le cluster) : il avance simplement
les offsets committés du group_id du bot jusqu'à la toute fin du
topic, comme si tout le backlog actuel avait déjà été lu.

=> Tous les vieux messages (fantômes du stress test ET vrais messages
   en attente) seront ignorés. Seuls les NOUVEAUX messages publiés
   après l'exécution de ce script seront traités par le bot.

USAGE :
    1. Arrête le bot (python3 main.py) -- IMPORTANT, ne pas lancer
       ce script pendant que le bot tourne, sinon conflit de consumer
       group.
    2. python3 purge_backlog.py
    3. Relance le bot normalement : python3 main.py
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

    # Va directement à la fin de chaque partition (dernier offset dispo)
    consumer.seek_to_end(*tps)

    end_offsets = {tp: consumer.position(tp) for tp in tps}

    for tp, offset in end_offsets.items():
        logger.info(
            "Partition %s -> nouvel offset committé = %d",
            tp.partition, offset
        )

    # Committe ces offsets "fin de topic" pour le group_id du bot
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
        topic=settings.topic_audio_transcribed,
        group_id=settings.kafka_group_id_bot,
    )