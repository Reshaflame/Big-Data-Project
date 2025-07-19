#!/usr/bin/env python3
"""
Run the e-mail Kafka producer (and any future ones) concurrently.
Each entry in PRODUCERS should be a *full command list*.
"""

import subprocess
import time
import os
import signal
import sys
from pathlib import Path

# ---  edit here if you add more producers  ------------------------------------
PRODUCERS = [
    # path, CSV-file,   sleep-min, sleep-max
    ["publish_email_send.py",
     str(Path(__file__).parent / "../data/bronze_email_send_stream.csv"),
     "0.2",
     "0.8"],
]
# ------------------------------------------------------------------------------

processes = []


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print("\nShutting down all producers …")
    for proc in processes:
        proc.terminate()
    sys.exit(0)


def main() -> None:
    signal.signal(signal.SIGINT, signal_handler)

    print("Starting Kafka producers …")
    print(f"Kafka Bootstrap Servers: {os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')}")
    print("-" * 60)

    for cmd in PRODUCERS:
        print(f"Launching {' '.join(cmd)}")
        proc = subprocess.Popen(
            ["python", "-u", *cmd],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1,
        )
        processes.append(proc)
        time.sleep(2)           # stagger start-up

    print(f"\n✓ {len(PRODUCERS)} producer(s) running.  Ctrl-C to stop.")
    print("-" * 60)

    try:
        while True:
            time.sleep(5)
            for i, proc in enumerate(processes):
                if proc.poll() is not None:      # crashed → restart
                    print(f"\n⚠️  {PRODUCERS[i][0]} stopped — restarting …")
                    processes[i] = subprocess.Popen(
                        ["python", "-u", *PRODUCERS[i]],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        universal_newlines=True,
                        bufsize=1,
                    )
    except KeyboardInterrupt:
        signal_handler(None, None)


if __name__ == "__main__":
    main()
