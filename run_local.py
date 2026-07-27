#!/usr/bin/env python3

import subprocess
import time
import os
import sys


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Existing virtual environment
VENV_PYTHON = os.path.join(BASE_DIR, "venv", "bin", "python")

processes = []


def start_service(name, script):

    script_path = os.path.join(BASE_DIR, script)

    print(f"🚀 Starting {name}")
    print(f"   Running: {script_path}")

    if not os.path.exists(script_path):
        print(f"❌ File not found: {script_path}")
        sys.exit(1)

    process = subprocess.Popen(
        [VENV_PYTHON, script_path],
        cwd=BASE_DIR
    )

    processes.append(process)


def stop_services():

    print("\n🛑 Stopping local services...")

    for process in processes:
        process.terminate()


def main():

    if not os.path.exists(VENV_PYTHON):
        print("❌ Virtual environment not found:")
        print(VENV_PYTHON)
        print("\nCreate it using:")
        print("python3 -m venv venv")
        sys.exit(1)


    try:

        # Telegram Bot
        start_service(
            "Telegram Bot",
            "main.py"
        )


        time.sleep(3)


        # Whisper ASR Worker
        start_service(
            "Whisper Worker",
            "asr_worker.py"
        )


        print("\n" + "=" * 60)
        print("✅ Local pipeline started")
        print(f"🐍 Using venv: {VENV_PYTHON}")
        print("")
        print("Services:")
        print("  - Telegram Bot")
        print("  - Whisper ASR Worker")
        print("")
        print("Press CTRL+C to stop")
        print("=" * 60)


        while True:
            time.sleep(1)


    except KeyboardInterrupt:
        stop_services()


if __name__ == "__main__":
    main()