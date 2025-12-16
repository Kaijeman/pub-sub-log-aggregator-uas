import os
import time
import random
import uuid
from datetime import datetime, timezone
import httpx

TARGET_URL = os.environ.get("TARGET_URL", "http://aggregator:8080/publish")
TOPICS = ["auth", "billing", "search", "upload", "stats"]
SOURCE = os.environ.get("SOURCE", "publisher-1")

TOTAL = int(os.environ.get("TOTAL", "20000"))
DUP_RATE = float(os.environ.get("DUP_RATE", "0.35"))
BATCH = int(os.environ.get("BATCH", "200"))
SLEEP_MS = int(os.environ.get("SLEEP_MS", "0"))

def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def main():
    unique_count = int(TOTAL * (1.0 - DUP_RATE))
    base_ids = [str(uuid.uuid4()) for _ in range(unique_count)]
    all_ids = base_ids[:]

    dup_count = TOTAL - unique_count
    for _ in range(dup_count):
        all_ids.append(random.choice(base_ids))

    random.shuffle(all_ids)

    client = httpx.Client(timeout=30.0)
    sent = 0
    t0 = time.time()

    while sent < TOTAL:
        chunk = all_ids[sent:sent + BATCH]
        events = []
        for eid in chunk:
            topic = random.choice(TOPICS)
            events.append({
                "topic": topic,
                "event_id": eid,
                "timestamp": iso_now(),
                "source": SOURCE,
                "payload": {
                    "message": "hello",
                    "value": random.randint(1, 1000)
                }
            })

        r = client.post(TARGET_URL, json=events)
        r.raise_for_status()
        sent += len(chunk)

        if SLEEP_MS > 0:
            time.sleep(SLEEP_MS / 1000.0)

    dt = time.time() - t0
    print(f"sent={TOTAL} time_sec={dt:.2f} eps={TOTAL/dt:.2f}")

if __name__ == "__main__":
    main()
