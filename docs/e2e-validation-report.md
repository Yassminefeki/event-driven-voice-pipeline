# Rapport de validation E2E DataBot

Date : 2026-07-23
Statut : BLOQUE avant le test end-to-end complet

## Périmètre

Ce rapport consigne uniquement les vérifications réellement exécutées depuis l'environnement de développement. Une étape n'est pas marquée comme réussie uniquement parce qu'elle est prévue dans le code.

## Résultats

| Étape | Résultat | Preuve ou blocage |
|---|---|---|
| Contrat `audio.uploaded` | Vérifié dans le code | `services/kafka_service.py` publie les octets audio et les headers documentés dans `docs/kafka-contracts.md`. |
| Topic ASR | Vérifié dans le code | Le worker et le consumer Telegram utilisent `audio.transcribed`. Un grep source ne trouve plus `asr.completed` ni `ASR_COMPLETED_TOPIC`. |
| Dépendances Python | Vérifié | `kafka-python`, Telegram, HTTPX, MinIO et Elasticsearch sont installés dans `.venv`; `jiwer` a été installé et ajouté à `requirements.txt`. |
| MinIO réseau | Vérifié | `http://10.110.188.120:9000/minio/health/live` répond HTTP 200. |
| Bucket MinIO | Vérifié | Le client MinIO liste `audio-archive`; le bucket existe. |
| Elasticsearch réseau | Vérifié | `http://10.110.188.120:9200/` répond HTTP 200. |
| Index Elasticsearch cible | Vérifié | `PUT /transcription.corrected` répond HTTP 200; mapping créé pour les champs du contrat. |
| Whisper endpoint | Partiel | L'hôte répond, mais une requête `HEAD` sur l'endpoint retourne HTTP 405. Aucune transcription audio n'a été envoyée dans cette validation. |
| Kafka brokers | Bloqué | `kafka1`, `kafka2` et `kafka3` ne se résolvent pas depuis cette machine. Aucun message ni metadata Kafka n'a pu être vérifié. |
| Kafka Connect | Bloqué | `kafka-connect:8083` ne se résout pas et Docker Desktop n'a pas de daemon actif. Aucun plugin ni connecteur n'a pu être vérifié ou déployé. |
| MinIO Sink Connector | Non validé | Le fichier `connectors/minio-sink.json` existe, mais aucun statut REST `RUNNING` n'est disponible. |
| Elasticsearch Sink Connector | Non validé | Le fichier `connectors/elasticsearch-sink.json` existe, mais le connecteur n'est pas déployé. |
| Kibana | Non validé | L'API testée sur `http://10.110.188.120:5601/api/status` n'a pas fourni de preuve de disponibilité. Aucun index pattern ni document Discover n'a été vérifié. |
| Test Telegram réel | Non exécuté | Un message vocal réel n'a pas été envoyé pendant cette session. |
| Test de bout en bout | Bloqué | Kafka Connect et les brokers Kafka ne sont pas accessibles depuis l'environnement courant. |

## Changements appliqués avant validation

- Le topic ASR a été harmonisé sur `audio.transcribed`.
- La constante est `AUDIO_TRANSCRIBED_TOPIC`.
- Le contrat binaire `audio.uploaded` est documenté.
- `connectors/minio-sink.json` est présent pour le Sink MinIO.
- `connectors/elasticsearch-sink.json` a été ajouté pour `transcription.corrected`.
- L'index Elasticsearch `transcription.corrected` a été créé avec un mapping explicite.
- `kafka-python` et les dépendances applicatives sont installés dans `.venv`.

## Commandes à exécuter dans l'environnement Kafka Connect

Ces commandes ne peuvent pas être exécutées depuis cette machine tant que Kafka Connect et les brokers ne sont pas accessibles.

### Vérifier Kafka Connect

```powershell
curl.exe http://<kafka-connect-host>:8083/connector-plugins
curl.exe http://<kafka-connect-host>:8083/connectors
```

### Déployer le Sink MinIO

```powershell
curl.exe -X POST http://<kafka-connect-host>:8083/connectors `
  -H "Content-Type: application/json" `
  --data-binary "@connectors/minio-sink.json"

curl.exe http://<kafka-connect-host>:8083/connectors/minio-audio-sink/status
```

### Déployer le Sink Elasticsearch

```powershell
curl.exe -X POST http://<kafka-connect-host>:8083/connectors `
  -H "Content-Type: application/json" `
  --data-binary "@connectors/elasticsearch-sink.json"

curl.exe http://<kafka-connect-host>:8083/connectors/elasticsearch-transcription-sink/status
```

Les deux statuts attendus sont `RUNNING`.

## Validation E2E restante

Après rétablissement de l'accès aux brokers Kafka et à Kafka Connect :

1. Créer ou vérifier les topics `audio.uploaded`, `audio.transcribed` et `transcription.corrected`.
2. Vérifier que le MinIO Sink consomme `audio.uploaded`.
3. Envoyer un vocal Telegram réel.
4. Lire un message `audio.uploaded` et vérifier sa key, ses headers et ses octets.
5. Vérifier l'objet réel créé dans MinIO.
6. Vérifier le message `audio.transcribed` produit par le worker.
7. Vérifier l'affichage Telegram et la publication de `transcription.corrected`.
8. Rechercher le document correspondant dans `transcription.corrected` avec Elasticsearch.
9. Créer un index pattern Kibana et vérifier le document dans Discover.
10. Ajouter les résultats et les extraits de logs à ce rapport.

## Verdict

Le code et les contrats sont préparés pour le flux cible, et plusieurs dépendances d'infrastructure répondent individuellement. Le flux complet n'est pas déclaré conforme, car les brokers Kafka, Kafka Connect, les deux Sink Connectors, Kibana et le test Telegram réel n'ont pas encore été vérifiés avec des preuves d'exécution.
