import base64
import json
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
import os
from kafka import KafkaProducer

# Configuration Defaults
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "audio.uploaded"
SAMPLE_AUDIO_PATH = "sample_test.ogg"


def get_user_inputs():
    print("=" * 60)
    print("🔥 TELEGRAM ASR PIPELINE - STRESS TEST CONFIGURATION 🔥")
    print("=" * 60)

    # 1. Check if sample audio file exists
    if not os.path.exists(SAMPLE_AUDIO_PATH):
        print(f"❌ Error: Could not find sample audio file at '{SAMPLE_AUDIO_PATH}'")
        print(
            "Please place a valid '.ogg' file in the root directory named 'sample_test.ogg'."
        )
        sys.exit(1)

    # 2. Interactive Prompts
    try:
        num_users = int(
            input("👉 Enter the number of simulated users (e.g., 50): ")
        )
        msgs_per_user = int(
            input(
                "👉 Enter the number of voice messages per user (e.g., 5): "
            )
        )
        concurrency = int(
            input(
                "👉 Enter number of parallel worker threads for producing (e.g., 10): "
            )
        )
    except ValueError:
        print("❌ Invalid input. Please enter valid integers.")
        sys.exit(1)

    total_messages = num_users * msgs_per_user
    print("\n" + "-" * 60)
    print(f"📊 Test Summary:")
    print(f"   • Simulated Users:    {num_users}")
    print(f"   • Messages per User: {msgs_per_user}")
    print(f"   • Total Voice Notes:  {total_messages}")
    print(f"   • Parallel Threads:   {concurrency}")
    print("-" * 60)

    confirm = input("🚀 Launch stress test? (y/N): ").strip().lower()
    if confirm != "y":
        print("Aborted.")
        sys.exit(0)

    return num_users, msgs_per_user, concurrency, total_messages


def main():
    num_users, msgs_per_user, concurrency, total_messages = get_user_inputs()

    # Load audio binary & encode base64
    with open(SAMPLE_AUDIO_PATH, "rb") as f:
        audio_b64 = base64.b64encode(f.read()).decode("utf-8")

    # Generate pool of simulated user IDs and chat IDs
    users = [
        {"user_id": 100000 + i, "chat_id": 900000 + i} for i in range(num_users)
    ]

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
    )

    # Build sequence of message dispatch tasks
    tasks = []
    msg_counter = 10000
    for u in users:
        for _ in range(msgs_per_user):
            msg_counter += 1
            tasks.append(
                {
                    "message_id": msg_counter,
                    "chat_id": u["chat_id"],
                    "user_id": u["user_id"],
                    "file_name": f"{msg_counter}.ogg",
                    "audio_data": audio_b64,
                    "duration": random.randint(3, 15),
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
            )

    # Shuffle task queue to simulate random interleaved messaging across users
    random.shuffle(tasks)

    def send_event(payload):
        producer.send(TOPIC, payload)

    print(f"\n🚀 Ingesting {total_messages} events into Kafka topic '{TOPIC}'...")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        list(executor.map(send_event, tasks))

    producer.flush()
    elapsed = time.time() - start_time

    throughput = total_messages / elapsed if elapsed > 0 else total_messages
    print("\n" + "=" * 60)
    print("✅ STRESS TEST INJECTION COMPLETE")
    print(f"   • Total Sent:   {total_messages} messages")
    print(f"   • Total Time:   {elapsed:.2f} seconds")
    print(f"   • Ingestion Rate: {throughput:.1f} msg/sec")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
