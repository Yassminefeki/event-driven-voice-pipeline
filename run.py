"""
Cross-platform runner: cleans up, rebuilds without cache, starts services,
and streams logs. Usage: python3 run.py
"""
import subprocess
import sys


def run(cmd: list[str]) -> None:
    print(f"$ {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def main() -> None:
    run(["docker", "compose", "down", "--remove-orphans"])
    run(["docker", "compose", "build", "--no-cache"])
    run(["docker", "compose", "up", "-d"])
    subprocess.run(["docker", "compose", "logs", "-f"])


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        print(f"Command failed: {exc}", file=sys.stderr)
        sys.exit(1)
