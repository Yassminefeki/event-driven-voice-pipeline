#!/bin/bash
#
# healthcheck.sh
#
# Healthcheck générique réutilisé par les 3 services (bot, asr-worker,
# s3-publisher). Deux vérifications :
#   1. Le process applicatif attendu (HC_PROCESS) tourne toujours.
#   2. Au moins un broker Kafka de KAFKA_BOOTSTRAP_SERVERS est joignable en TCP.
#
# Variables d'environnement attendues :
#   HC_PROCESS               : motif pgrep du process à surveiller (ex: "bot.main")
#   KAFKA_BOOTSTRAP_SERVERS  : liste "host:port,host:port,..." (déjà dans .env)

set -u

if [ -z "${HC_PROCESS:-}" ]; then
    echo "HC_PROCESS non défini, healthcheck impossible à configurer"
    exit 1
fi

# 1. Le process est-il toujours vivant ?
if ! pgrep -f "$HC_PROCESS" > /dev/null 2>&1; then
    echo "❌ Process '$HC_PROCESS' introuvable"
    exit 1
fi

# 2. Au moins un broker Kafka est-il joignable ?
if [ -z "${KAFKA_BOOTSTRAP_SERVERS:-}" ]; then
    # Pas de liste de brokers fournie : on se contente du check process.
    exit 0
fi

IFS=',' read -ra BROKERS <<< "$KAFKA_BOOTSTRAP_SERVERS"
for broker in "${BROKERS[@]}"; do
    host="${broker%%:*}"
    port="${broker##*:}"
    if timeout 3 bash -c "echo > /dev/tcp/${host}/${port}" 2>/dev/null; then
        exit 0
    fi
done

echo "❌ Aucun broker Kafka joignable parmi: $KAFKA_BOOTSTRAP_SERVERS"
exit 1
