#!/usr/bin/env python3
import os
import subprocess
import sys
import time

# List of obsolete/temporary files to remove
FILES_TO_REMOVE = [
    os.path.join("services", "object_name_store.py"),
    "object_store.db",
    "audio.wav",
    "test.ogg",
    "temp.wav",
]


def run_command(cmd, check=True):
    """Executes a shell command and streams output live."""
    print(f"\n==> Executing: {' '.join(cmd)}")
    try:
        res = subprocess.run(cmd, check=check)
        return res.returncode
    except subprocess.CalledProcessError as e:
        print(f"❌ Error executing command: {e}")
        if check:
            sys.exit(1)


def cleanup():
    """Safely removes legacy files across OS platforms."""
    print("==> Cleaning up legacy files...")
    for filepath in FILES_TO_REMOVE:
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
                print(f"    Deleted: {filepath}")
            except Exception as e:
                print(f"    Failed to delete {filepath}: {e}")


def main():
    # 1. Clean up stale local files
    cleanup()

    # 2. Stop running containers
    run_command(["docker", "compose", "down", "--remove-orphans"], check=False)

    # 3. Build containers without cache
    run_command(["docker", "compose", "build", "--no-cache"])

    # 4. Bring up services in detached mode
    run_command(["docker", "compose", "up", "-d"])

    # 5. Display container status
    run_command(["docker", "compose", "ps"])

    print("\n" + "=" * 60)
    print("🚀 Pipeline started successfully! Streaming live logs...")
    print("💡 Press Ctrl+C at any time to stop log stream (containers stay up).")
    print("=" * 60 + "\n")

    time.sleep(2)

    # 6. Attach to live log stream
    try:
        run_command(["docker", "compose", "logs", "-f"], check=False)
    except KeyboardInterrupt:
        print("\n\nStopped log streaming. Docker containers are still running in the background.")


if __name__ == "__main__":
    main()