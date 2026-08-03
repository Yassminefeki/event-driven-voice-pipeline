# Runbook — Procedures d'exploitation DataBot

## Redemarrer l'ASR Worker

```bash
cd asr-worker
python3 whisper_worker.py
```

Verifier au demarrage dans les logs :
- `enable_auto_commit=False` actif (aucun message a ete perdu pendant l'arret).
- Le worker reprend bien a partir du dernier offset committe (pas de replay
  complet ni de saut de messages).

## Verifier l'etat de la DLQ

```bash
kafka-console-consumer.sh \
  --bootstrap-server kafka1:9092 \
  --topic audio.uploaded.dlq \
  --from-beginning \
  --property print.headers=true
```

Chaque message DLQ contient `error_type` (`invalid_payload` ou
`asr_unrecoverable`), `error_reason`, et le payload original.

> ⚠️ Le rejeu automatique de la DLQ n'est pas encore implemente
> (voir `MODIFICATIONS.md` — roadmap §7). En attendant, le rejeu est manuel :
> republier le payload original sur `audio.uploaded` une fois la cause
> corrigee (ex: Whisper de nouveau disponible).

## Ajouter un broker Kafka

1. Provisionner la VM, installer Kafka.
2. Configurer `node.id` unique et ajouter la VM a `controller.quorum.voters`
   sur **tous** les brokers existants (necessite un redemarrage coordonne).
3. Redemarrer les brokers un par un (rolling restart) pour prendre en compte
   le nouveau quorum.
4. Verifier `kafka-metadata-quorum.sh --describe` pour confirmer que le
   nouveau nœud a rejoint le quorum KRaft.

## Verifier la sante des connecteurs Kafka Connect

```bash
curl http://10.110.188.124:8083/connectors
curl http://10.110.188.124:8083/connectors/minio-audio-archive-sink/status
curl http://10.110.188.124:8083/connectors/es-transcription-corrected-sink/status
```

Un connecteur en etat `FAILED` doit etre redemarre :

```bash
curl -X POST http://10.110.188.124:8083/connectors/<nom>/restart
```

## Executer le test de charge avant mise en production

```bash
cd stresstest
python3 stresstest.py run --count 500 --rate 20 --wait-seconds 30
```

Valider que le log final affiche `✅ Formule Zero Loss verifiee`. Si un ecart
est detecte, augmenter `--wait-seconds` (le traitement ASR peut prendre du
temps) avant de conclure a une vraie perte de messages.

## Verifier l'espace disque des brokers (prevention incident §6.5)

```bash
df -h /var/lib/kafka/data
du -sh /opt/kafka/logs/*
```

Alerter si l'utilisation depasse 80%.
