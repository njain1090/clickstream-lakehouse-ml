import json
import time
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

PAGES = ["/", "/home", "/product", "/cart", "/checkout", "/search"]
DEVICES = ["mobile", "desktop", "tablet"]
REFERRERS = ["google", "bing", "email", "social", "direct"]
CAMPAIGNS = ["spring", "summer", "fall", "retargeting", "brand"]

LANDING_DIR = Path("landing/raw_events")


def make_event():
    """Create one fake clickstream event as a Python dict."""
    return {
        "event_id": str(uuid.uuid4()),
        "ts": datetime.now(timezone.utc).isoformat(),
        "user_id": random.randint(1, 1000),
        "session_id": str(uuid.uuid4()),
        "page": random.choice(PAGES),
        "referrer": random.choice(REFERRERS),
        "device": random.choice(DEVICES),
        "campaign": random.choice(CAMPAIGNS),
        "clicked": random.choice([0, 0, 0, 1]),
    }


def generate_files(num_files: int = 10, events_per_file: int = 200):
    LANDING_DIR.mkdir(parents=True, exist_ok=True)

    for file_idx in range(num_files):
        file_path = LANDING_DIR / f"events_{int(time.time())}_{file_idx}.jsonl"
        print(f"Writing {events_per_file} events to {file_path}")
        with file_path.open("w", encoding="utf-8") as f:
            for _ in range(events_per_file):
                ev = make_event()
                f.write(json.dumps(ev) + "\n")
                # simulate a bit of time between events
                time.sleep(0.005)


if __name__ == "__main__":
    generate_files(num_files=5, events_per_file=200)
    print("Done generating files.")

