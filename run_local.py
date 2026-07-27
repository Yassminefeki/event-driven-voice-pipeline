#!/usr/bin/env python3

import subprocess
import time
import signal


processes = []


def start_service(name, command):

    print(f"🚀 Starting {name}")

    process = subprocess.Popen(command)

    processes.append(process)


def stop_services():

    print("\nStopping services...")

    for process in processes:
        process.terminate()


def main():

    try:

        # Start Telegram bot
        start_service(
            "Telegram Bot",
            [
                "python3",
                "bot/BotTelegram.py"
            ]
        )


        time.sleep(3)


        # Start Whisper worker
        start_service(
            "Whisper Worker",
            [
                "python3",
                "whisper-worker/worker.py"
            ]
        )


        print("\n" + "=" * 60)
        print("✅ Local pipeline started")
        print("Press CTRL+C to stop")
        print("=" * 60)


        while True:
            time.sleep(1)


    except KeyboardInterrupt:
        stop_services()


if __name__ == "__main__":
    main()