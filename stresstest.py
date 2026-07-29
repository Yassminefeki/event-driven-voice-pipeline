```python
import base64
import json
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from kafka import KafkaProducer


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka1:9092,kafka2:9092,kafka3:9092"
)

TOPIC = "audio.uploaded"
SAMPLE_AUDIO_PATH = "sample_test.ogg"


# ============================================================
# CONFIGURATION DU TEST
# ============================================================

def get_user_inputs():

    print("=" * 65)
    print("🔥 TELEGRAM ASR PIPELINE - STRESS TEST 🔥")
    print("=" * 65)

    # Vérification du fichier audio
    if not os.path.isfile(SAMPLE_AUDIO_PATH):
        print()
        print(f"❌ Fichier audio introuvable : {SAMPLE_AUDIO_PATH}")
        print()
        print("Place un fichier .ogg dans :")
        print(os.path.abspath(SAMPLE_AUDIO_PATH))
        print()
        sys.exit(1)

    try:
        num_users = int(
            input("👉 Nombre d'utilisateurs simulés (ex: 50) : ")
        )

        msgs_per_user = int(
            input("👉 Nombre de vocaux par utilisateur (ex: 5) : ")
        )

        concurrency = int(
            input("👉 Nombre de threads parallèles (ex: 10) : ")
        )

    except ValueError:
        print("❌ Les valeurs doivent être des nombres entiers.")
        sys.exit(1)

    if num_users <= 0 or msgs_per_user <= 0 or concurrency <= 0:
        print("❌ Les valeurs doivent être supérieures à 0.")
        sys.exit(1)

    total_messages = num_users * msgs_per_user

    print()
    print("-" * 65)
    print("📊 CONFIGURATION DU TEST")
    print("-" * 65)
    print(f"👥 Utilisateurs simulés : {num_users}")
    print(f"🎤 Vocaux/utilisateur   : {msgs_per_user}")
    print(f"📨 Total messages       : {total_messages}")
    print(f"⚡ Threads parallèles   : {concurrency}")
    print(f"📡 Kafka                 : {KAFKA_BOOTSTRAP}")
    print(f"📌 Topic                 : {TOPIC}")
    print("-" * 65)

    confirmation = input("🚀 Lancer le test ? (y/N) : ").strip().lower()

    if confirmation != "y":
        print("❌ Test annulé.")
        sys.exit(0)

    return num_users, msgs_per_user, concurrency, total_messages


# ============================================================
# MAIN
# ============================================================

def main():

    (
        num_users,
        msgs_per_user,
        concurrency,
        total_messages
    ) = get_user_inputs()

    # --------------------------------------------------------
    # Lecture du fichier audio
    # --------------------------------------------------------

    print()
    print("🎵 Lecture du fichier audio...")

    try:
        with open(SAMPLE_AUDIO_PATH, "rb") as audio_file:
            audio_bytes = audio_file.read()

    except Exception as e:
        print(f"❌ Impossible de lire le fichier audio : {e}")
        sys.exit(1)

    audio_base64 = base64.b64encode(audio_bytes).decode("utf-8")

    print(f"✅ Audio chargé : {len(audio_bytes):,} octets")
    print(f"✅ Base64 généré : {len(audio_base64):,} caractères")

    # --------------------------------------------------------
    # Création des utilisateurs simulés
    # --------------------------------------------------------

    users = [
        {
            "user_id": 100000 + i,
            "chat_id": 900000 + i
        }
        for i in range(num_users)
    ]

    # --------------------------------------------------------
    # Création du Producer Kafka
    # --------------------------------------------------------

    print()
    print("🔌 Connexion à Kafka...")

    try:

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            value_serializer=lambda value:
                json.dumps(value).encode("utf-8"),

            # Confirmation du broker
            acks="all",

            # Retry automatique
            retries=5,

            # Timeout
            request_timeout_ms=30000,

            # Évite que Kafka bloque trop longtemps
            max_block_ms=30000
        )

    except Exception as e:

        print("❌ Impossible de créer le Kafka Producer.")
        print(e)
        sys.exit(1)

    print("✅ Producer Kafka connecté.")

    # --------------------------------------------------------
    # Génération des messages
    # --------------------------------------------------------

    tasks = []

    message_counter = int(time.time() * 1000)

    for user in users:

        for message_index in range(msgs_per_user):

            message_counter += 1

            payload = {
                "message_id": str(message_counter),

                "chat_id": user["chat_id"],

                "user_id": user["user_id"],

                "telegram_file_id":
                    f"stress-test-{message_counter}",

                "file_name":
                    f"stress-{message_counter}.ogg",

                "audio_base64":
                    audio_base64,

                "duration_seconds":
                    random.randint(3, 15),

                "timestamp":
                    time.strftime(
                        "%Y-%m-%dT%H:%M:%SZ",
                        time.gmtime()
                    ),

                "stress_test": True
            }

            tasks.append(payload)

    # Mélange des utilisateurs pour simuler
    # plusieurs utilisateurs en même temps
    random.shuffle(tasks)

    # --------------------------------------------------------
    # Envoi vers Kafka
    # --------------------------------------------------------

    print()
    print("=" * 65)
    print(f"🚀 ENVOI DE {total_messages} MESSAGES")
    print(f"📡 Destination : {TOPIC}")
    print("=" * 65)

    start_time = time.time()

    success_count = 0
    failed_count = 0

    # --------------------------------------------------------
    # Fonction d'envoi
    # --------------------------------------------------------

    def send_event(payload):

        try:

            future = producer.send(
                TOPIC,
                value=payload
            )

            # IMPORTANT :
            # attend la confirmation réelle de Kafka
            metadata = future.get(timeout=30)

            return {
                "success": True,
                "message_id": payload["message_id"],
                "partition": metadata.partition,
                "offset": metadata.offset
            }

        except Exception as e:

            return {
                "success": False,
                "message_id": payload["message_id"],
                "error": str(e)
            }

    # --------------------------------------------------------
    # Threads parallèles
    # --------------------------------------------------------

    with ThreadPoolExecutor(
        max_workers=concurrency
    ) as executor:

        futures = [
            executor.submit(send_event, payload)
            for payload in tasks
        ]

        for future in as_completed(futures):

            result = future.result()

            if result["success"]:

                success_count += 1

                print(
                    f"✅ [{success_count}/{total_messages}] "
                    f"message={result['message_id']} "
                    f"partition={result['partition']} "
                    f"offset={result['offset']}"
                )

            else:

                failed_count += 1

                print(
                    f"❌ message={result['message_id']} "
                    f"ERREUR={result['error']}"
                )

    # --------------------------------------------------------
    # Flush final
    # --------------------------------------------------------

    try:
        producer.flush(timeout=30)
    except Exception as e:
        print(f"⚠️ Erreur pendant flush : {e}")

    producer.close()

    elapsed = time.time() - start_time

    throughput = (
        total_messages / elapsed
        if elapsed > 0
        else 0
    )

    # --------------------------------------------------------
    # Résultat
    # --------------------------------------------------------

    print()
    print("=" * 65)
    print("🔥 RÉSULTAT DU STRESS TEST")
    print("=" * 65)

    print(f"📨 Messages demandés : {total_messages}")
    print(f"✅ Messages confirmés : {success_count}")
    print(f"❌ Messages échoués   : {failed_count}")
    print(f"⏱️ Temps total        : {elapsed:.2f} secondes")
    print(f"⚡ Débit              : {throughput:.2f} msg/sec")

    print("=" * 65)

    if success_count == total_messages:

        print()
        print("🎉 TEST RÉUSSI !")
        print(
            f"Kafka a confirmé les {total_messages} messages."
        )

    else:

        print()
        print("⚠️ TEST INCOMPLET !")
        print(
            f"{failed_count} messages n'ont pas été confirmés par Kafka."
        )

    print()


if __name__ == "__main__":
    main()
```
