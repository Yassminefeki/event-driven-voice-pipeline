# Historique des incidents et correctifs — DataBot

## §6.5 — Saturation disque & crash Kafka Connect

**Symptome** : disque plein sur `kafka1`, provoque par la proliferation des logs.

**Cause racine** : pas de politique de retention/rotation des logs Kafka.

**Resolution** :
- Troncature des logs (`truncate -s 0`).
- Nettoyage de `journalctl`.
- Correction du topic interne `connect-configs` : remis avec `cleanup.policy=compact`
  (sans quoi Kafka Connect ne peut pas recharger correctement l'etat des connecteurs).

**Action preventive (roadmap, non fait a ce jour)** : appliquer
`MaxBackupIndex=3` dans `/opt/kafka/config/log4j.properties` sur les 3 brokers.

---

## §6.7 — Workers Kafka Connect fantomes

**Symptome** : erreurs HTTP 409 (Rebalance Conflict).

**Cause racine** : une instance parasite de Kafka Connect tournait sur `kafka1`
sans les plugins installes, provoquant des conflits de rebalance dans le
groupe `connect-cluster`.

**Resolution** : synchronisation des plugins entre `kafka1` et `kafkaconnect`.

**Action preventive** : verifier periodiquement qu'aucune instance Connect
non geree ne tourne sur les brokers (audit des process + des plugins installes).

---

## §6.10 — Incompatibilite Elasticsearch 8.15.0 vs connecteur ES Sink v14

**Symptome** : `NullPointerException` lors du bulk indexation.

**Cause racine** : le connecteur ES Sink v14 n'est pas compatible avec le
client REST utilise par Elasticsearch 8.15.0.

**Resolution** : mise a jour recommandee vers le connecteur ES Sink v15.1.0+
(utilise le client REST bas niveau). Voir `kafka/connect/elasticsearch-sink.json`
pour la config a jour.

---

## §6.11 — Cluster Elasticsearch en etat Yellow

**Symptome** : etat cluster `yellow` persistant.

**Cause racine** : `number_of_replicas` non nul sur un cluster mono-nœud —
les replicas ne peuvent jamais etre alloues sur un seul nœud.

**Resolution** : `number_of_replicas: 0` (voir `elk/elasticsearch/elasticsearch.yml`).

---

## §6.1 & §6.4 — Divergence de noms de topics

**Symptome** : messages qui n'atteignaient pas le bon consumer / connecteur.

**Cause racine** : incoherence de nommage entre composants developpes a des
moments differents.

**Resolution** : alignement strict sur les 4 topics applicatifs :
`audio.uploaded`, `audio.transcribed`, `transcription.corrected`,
`audio.uploaded.dlq` (+ `audio.stored` ajoute depuis, et `transcription.corrected.dlq`
pour la DLQ du sink ES).

**Action preventive** : les noms de topics sont maintenant centralises en
variables d'environnement (voir `.env.example`) pour eviter toute divergence
future entre composants.
