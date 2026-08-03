"""
offset_manager.py

Implemente _compute_safe_commit_offsets (voir doc §4.1).

Probleme resolu : avec enable_auto_commit=True, un message dont le traitement
echoue (ou est encore en cours dans le ThreadPoolExecutor) pouvait etre saute
si l'offset suivant etait deja auto-commite -> perte irrecuperable de message.

Solution : on calcule manuellement, par partition, l'offset le plus eleve
committable sans risquer de sauter un message non traite avec succes.
"""

from collections import defaultdict
from dataclasses import dataclass
from enum import Enum


class ProcessingStatus(Enum):
    SUCCESS = "success"       # transcrit avec succes -> audio.transcribed
    FAILED_DLQ = "failed_dlq"  # echec irrecuperable -> route vers la DLQ (compte comme "traite")
    PENDING = "pending"       # encore en cours de traitement dans le lot


@dataclass(frozen=True)
class RecordResult:
    partition: int
    offset: int
    status: ProcessingStatus


def compute_safe_commit_offsets(results: list[RecordResult]) -> dict[int, int]:
    """Calcule, par partition, l'offset a committer (= offset du dernier
    message traite avec succes en continu depuis le debut du lot, + 1).

    Regles :
    - Un message SUCCESS ou FAILED_DLQ est considere "traite" (safe a depasser).
    - Un message PENDING bloque le commit de tous les offsets suivants de
      la meme partition, meme si ceux-ci sont deja traites -- pour ne jamais
      committer un offset au-dela d'un message dont on n'est pas sur du sort.
    - Cas particulier (edge case) : lot vide -> retourne {} (aucun commit).
    - Cas particulier : lot 100% en echec -> tous les messages sont FAILED_DLQ,
      donc "traites" -> l'offset est quand meme avance (les messages sont dans
      la DLQ, pas perdus).

    Retourne un dict {partition: offset_to_commit} ou offset_to_commit est
    l'offset du PROCHAIN message a lire (convention kafka-python : on commit
    l'offset du message suivant, pas celui du dernier traite).
    """
    if not results:
        return {}

    by_partition: dict[int, list[RecordResult]] = defaultdict(list)
    for r in results:
        by_partition[r.partition].append(r)

    safe_offsets: dict[int, int] = {}

    for partition, records in by_partition.items():
        records_sorted = sorted(records, key=lambda r: r.offset)

        last_safe_offset = None
        for record in records_sorted:
            if record.status == ProcessingStatus.PENDING:
                # Des qu'on rencontre un message encore en cours, on arrete :
                # on ne peut pas committer au-dela de ce point.
                break
            last_safe_offset = record.offset

        if last_safe_offset is not None:
            # +1 : on commit l'offset du PROCHAIN message a consommer
            safe_offsets[partition] = last_safe_offset + 1

    return safe_offsets
