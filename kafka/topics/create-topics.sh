#!/usr/bin/env bash
# Cree les topics applicatifs de DataBot avec RF=3
# Usage: bash create-topics.sh

set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka1:9092,kafka2:9092,kafka3:9092}"
RF=3
PARTITIONS=6

TOPICS=(
  "audio.uploaded"
  "audio.stored"
  "audio.transcribed"
  "transcription.corrected"
  "audio.uploaded.dlq"
  "transcription.corrected.dlq"
)

for topic in "${TOPICS[@]}"; do
  echo "Creation du topic: ${topic}"
  kafka-topics.sh --create \
    --if-not-exists \
    --bootstrap-server "${BOOTSTRAP}" \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${RF}"
done

echo "Topics internes Kafka Connect (compact, geres automatiquement par Connect):"
echo "  connect-configs (cleanup.policy=compact -- voir incident 6.5)"
echo "  connect-offsets"
echo "  connect-status"

echo "Termine."
