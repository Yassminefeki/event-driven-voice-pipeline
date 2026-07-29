
from locust import User, task, between, events
from kafka import KafkaProducer
from elasticsearch import Elasticsearch
import base64
import json
import uuid
import time
import os

# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka1:9092,kafka2:9092,kafka3:9092"
)

KAFKA_TOPIC = "audio.uploaded"

ELASTIC_URL = os.getenv(
    "ELASTIC_URL",
    "http://elasticsearch:9200"
)

ELASTIC_INDEX = os.getenv(
    "ELASTIC_INDEX",
    "transcription.completed"
)

# Nombre de vocaux à envoyer
TOTAL_VOICE_MESSAGES = 100

# ============================================================
# AUDIO DE TEST
# ============================================================
# Petit fichier audio WAV de test généré automatiquement.
# Il contient quelques secondes de silence.
#
# Pour un vrai test Whisper, remplace cette partie par
# les octets d'un vrai fichier audio.

FAKE_AUDIO = base64.b64encode(
    b"FAKE_AUDIO_FOR_STRESS_TEST"
).decode("utf-8")


# ============================================================
# PRODUCER KAFKA
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None
)


# ============================================================
# ELASTICSEARCH
# ============================================================

try:
    es = Elasticsearch(ELASTIC_URL)
except Exception:
    es = None


# ============================================================
# LOCUST USER
# ============================================================

class KafkaWhisperUser(User):

    wait_time = between(0.1, 0.5)

    def on_start(self):

        self.sent = 0
        self.success = 0
        self.failed = 0

    @task
    def send_voice(self):

        if self.sent >= TOTAL_VOICE_MESSAGES:
            return

        message_id = str(uuid.uuid4())

        message = {
            "message_id": message_id,
            "user_id": "locust_test_user",
            "audio_base64": FAKE_AUDIO,
            "duration_seconds": 3,
            "timestamp": time.strftime(
                "%Y-%m-%dT%H:%M:%SZ",
                time.gmtime()
            ),
            "stress_test": True
        }

        start = time.time()

        try:

            # ------------------------------------------------
            # 1. KAFKA → audio.uploaded
            # ------------------------------------------------

            future = producer.send(
                KAFKA_TOPIC,
                key=message_id,
                value=message
            )

            future.get(timeout=10)

            producer.flush()

            self.sent += 1

            # ------------------------------------------------
            # 2. ATTENDRE WHISPER + ELASTICSEARCH
            # ------------------------------------------------

            found = False

            timeout = 60
            check_interval = 1

            elapsed = 0

            while elapsed < timeout:

                if es:

                    try:

                        result = es.search(
                            index=ELASTIC_INDEX,
                            query={
                                "term": {
                                    "message_id.keyword": message_id
                                }
                            }
                        )

                        if result["hits"]["total"]["value"] > 0:

                            found = True
                            break

                    except Exception:
                        pass

                time.sleep(check_interval)
                elapsed += check_interval

            # ------------------------------------------------
            # 3. RÉSULTAT
            # ------------------------------------------------

            response_time = (time.time() - start) * 1000

            if found:

                self.success += 1

                events.request.fire(
                    request_type="PIPELINE",
                    name="Kafka → Whisper → Elasticsearch",
                    response_time=response_time,
                    response_length=1,
                    exception=None
                )

            else:

                self.failed += 1

                events.request.fire(
                    request_type="PIPELINE",
                    name="Kafka → Whisper → Elasticsearch",
                    response_time=response_time,
                    response_length=0,
                    exception=Exception(
                        "Transcription non trouvée dans Elasticsearch"
                    )
                )

        except Exception as e:

            self.failed += 1

            response_time = (time.time() - start) * 1000

            events.request.fire(
                request_type="PIPELINE",
                name="Kafka → audio.uploaded",
                response_time=response_time,
                response_length=0,
                exception=e
            )


# ============================================================
# RAPPORT FINAL
# ============================================================

@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):

    total = TOTAL_VOICE_MESSAGES

    success = 0
    failed = 0

    if environment.stats.total.num_requests:
        success = environment.stats.total.num_requests - \
                  environment.stats.total.num_failures

        failed = environment.stats.total.num_failures

    print()
    print("=" * 70)
    print("📊 RÉSULTAT DU STRESS TEST LOCUST")
    print("=" * 70)

    print(f"🎤 Vocaux simulés : {total}")
    print(f"✅ Réussites      : {success}")
    print(f"❌ Échecs         : {failed}")

    if total > 0:

        success_rate = (success / total) * 100
        failure_rate = (failed / total) * 100

        print(f"📈 Taux de succès  : {success_rate:.2f}%")
        print(f"📉 Taux d'échec    : {failure_rate:.2f}%")

    print("=" * 70)

