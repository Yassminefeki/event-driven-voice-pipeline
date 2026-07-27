# Diagnostic du flux Kafka DataBot

Date du diagnostic : 2026-07-23

## Conclusion

Le code ne fonctionne pas encore exactement comme le flux décrit dans le tableau. Le flux applicatif principal est partiellement câblé, mais les deux Sink Connectors et le nom du topic ASR doivent encore être alignés et déployés.

Le flux actuellement implémenté est :

```text
Telegram
  -> audio.uploaded
  -> ASR Worker
  -> Whisper API
  -> audio.transcribed
  -> Bot Telegram
  -> transcription.corrected
```

Le flux complet attendu est :

```text
Telegram
  -> audio.uploaded
  -> MinIO Sink Connector -> MinIO
  -> ASR Worker -> Whisper API
  -> audio.transcribed
  -> Telegram Bot
  -> transcription.corrected
  -> Elasticsearch Sink Connector -> Elasticsearch
  -> Kibana
```

## Évaluation étape par étape

| Étape | État actuel | Évaluation |
|---|---|---|
| 1. L'utilisateur envoie un vocal | Fonctionne | `receive_voice` est enregistré dans `main.py`. |
| 2. Le bot télécharge l'audio | Partiel | Le fichier est téléchargé, mais le message Kafka contient des octets binaires et des headers, pas un JSON complet avec `file_content`. |
| 3. Le bot publie dans Kafka | Fonctionne sous conditions | `audio.uploaded` est publié avec `publish_audio`. |
| 4. Kafka reçoit le message | Fonctionne sous conditions | Kafka doit être disponible et accepter des messages jusqu'à 20 Mo. |
| 5. MinIO Sink Connector | Non validé | Une configuration existe, mais le connecteur n'est pas installé ni déployé depuis le projet. |
| 6. L'ASR Worker consomme en parallèle | Fonctionne | `asr_worker.py` consomme `audio.uploaded` avec un groupe Kafka différent. |
| 7. Le worker appelle la Whisper API | Fonctionne sous conditions | Le worker utilise `WhisperService`, avec l'endpoint et la clé API provenant de `.env`. |
| 8. La Whisper API renvoie la transcription | Fonctionne sous conditions | Cela dépend de l'endpoint, de la clé API et du format de réponse de l'API. |
| 9. Le worker publie la transcription | Câblé | Le worker publie sur `audio.transcribed`. |
| 10. Le bot consomme la transcription | Câblé | `bot/asr_consumer.py` écoute `audio.transcribed` et est démarré par `main.py`. |
| 11. L'utilisateur valide ou corrige | Fonctionne | Les boutons Telegram sont reliés aux handlers. |
| 12. Le bot publie la correction | Fonctionne | La correction est publiée sur `transcription.corrected`. |
| 13. Elasticsearch Sink Connector | Non déployé | `connectors/elasticsearch-sink.json` existe, mais aucun statut `RUNNING` n'a été obtenu. |
| 14. Elasticsearch stocke les données | Partiellement préparé | L'index `transcription.corrected` a été créé, mais aucun document issu de Kafka n'a encore été vérifié. |
| 15. Kibana affiche les données | Non vérifiable | Aucun fichier de configuration Kibana ou preuve d'indexation n'est présent. |

## Topic ASR

Le tableau demande :

```text
audio.transcribed
```

Le code utilise désormais :

```python
ASR_TOPIC = "audio.transcribed"
```

Le worker et le consumer Telegram utilisent le même topic `audio.transcribed`.

## Format du message `audio.uploaded`

Le tableau parle d'un message JSON contenant les métadonnées. Le code actuel utilise plutôt le format suivant :

```text
Kafka value   : octets audio bruts
Kafka headers : message_id, user_id, bucket, object_name, content_type
Kafka key     : message_id
```

Ce choix est adapté à un Sink Connector binaire, mais il faut le documenter comme contrat officiel du topic `audio.uploaded`.

Le message contient notamment les headers suivants :

```text
message_id
user_id
bucket
audio object_name
content_type=audio/ogg
```

## MinIO Sink Connector

Le fichier `connectors/minio-sink.json` existe, mais cela ne suffit pas à prouver que l'étape MinIO fonctionne.

Il faut vérifier :

- que le plugin `io.confluent.connect.s3.S3SinkConnector` est installé dans Kafka Connect ;
- que `ByteArrayConverter` et `ByteArrayFormat` sont disponibles ;
- que le ConfigProvider `env` est activé dans Kafka Connect ;
- que les variables `MINIO_BUCKET_NAME`, `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY` et `MINIO_SECRET_KEY` sont accessibles au worker Kafka Connect ;
- que le connecteur est effectivement créé avec l'API REST de Kafka Connect ;
- que le bucket `audio-archive` existe ou peut être créé par le connecteur.

La configuration utilise :

```json
"format.class": "io.confluent.connect.s3.format.bytearray.ByteArrayFormat",
"value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter"
```

### Risque sur le nom du fichier

Le Sink Connector standard ne garantit pas que le fichier sera enregistré exactement sous :

```text
123.ogg
```

Même si `object_name` est présent dans les headers Kafka, le connecteur peut produire un chemin basé sur le topic, la partition et l'offset.

Par conséquent, l'URL générée par le bot :

```text
http://MINIO/audio-archive/123.ogg
```

peut ne pas correspondre au chemin réellement écrit par MinIO.

Il faut vérifier le chemin produit par le connecteur ou utiliser une configuration/plugin permettant de contrôler le nom de l'objet.

## Elasticsearch Sink Connector

La configuration Elasticsearch Sink Connector manque complètement.

Il faut ajouter et déployer un connecteur qui consomme :

```text
transcription.corrected
```

et écrit dans Elasticsearch. Exemple de configuration :

```json
{
  "name": "elasticsearch-transcription-sink",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "tasks.max": "1",
    "topics": "transcription.corrected",
    "connection.url": "http://elasticsearch:9200",
    "key.ignore": "false",
    "schema.ignore": "true",
    "behavior.on.malformed.documents": "fail",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter"
  }
}
```

La configuration réelle doit être adaptée à l'adresse Elasticsearch et à la sécurité de l'environnement.

Sans ce connecteur, le message `transcription.corrected` est publié dans Kafka mais n'est pas indexé dans Elasticsearch.

## Modifications restantes

Pour rendre le flux complètement conforme, il faut :

1. créer le topic `audio.transcribed` dans Kafka ;
2. vérifier que le worker et le consumer Telegram utilisent `audio.transcribed` ;
3. installer et déployer le MinIO Sink Connector ;
4. vérifier le chemin et le nom réel des objets écrits dans MinIO ;
5. déployer la configuration Elasticsearch Sink Connector et obtenir `RUNNING` ;
6. vérifier l'index Elasticsearch et l'indexation d'un document Kafka ;
7. configurer Kibana sur cet index ;
8. tester le flux avec un vrai message vocal ;
9. vérifier les messages dans `audio.uploaded`, le fichier dans MinIO, la transcription dans le topic ASR et le document dans Elasticsearch ;
10. exécuter les tests et le test end-to-end depuis un environnement où Kafka et Kafka Connect sont accessibles.

## Verdict final

### Partie actuellement câblée

```text
Telegram
  -> audio.uploaded
  -> ASR Worker
  -> Whisper API
  -> audio.transcribed
  -> Bot Telegram
  -> transcription.corrected
```

### Partie non garantie ou manquante

```text
audio.uploaded
  -> MinIO Sink Connector
  -> MinIO
```

et :

```text
transcription.corrected
  -> Elasticsearch Sink Connector
  -> Elasticsearch
  -> Kibana
```

Le code n'est donc pas encore entièrement conforme au flux planifié. Il est nécessaire de finaliser et déployer les deux Sink Connectors, puis de valider le flux complet sur les services d'infrastructure.
