"""Generates a fake stream of music plays and sends it to Kafka.

About 5% of events come from a bot farm that pumps one track: every bot is a
fresh account, and every play lasts just over 30 seconds, the point at which
a platform starts counting it as a stream.
"""
import json
import random
import time
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

import config

fake = Faker()

GENRES = ["Pop", "Hip-Hop", "Rock", "Electronic", "Indie", "Metal", "R&B"]
DEVICES = ["ios", "android", "web", "desktop"]
USER_POOL = [fake.uuid4() for _ in range(500)]
FRAUD_SHARE = 0.05
FRAUD_TRACK_ID = "track_bot_farm_victim_99"


def generate_event() -> dict:
    now = datetime.now(timezone.utc).isoformat()

    if random.random() < FRAUD_SHARE:
        return {
            "event_id": fake.uuid4(),
            "user_id": fake.uuid4(),  # a brand-new account every time
            "track_id": FRAUD_TRACK_ID,
            "artist_id": "artist_fraudster_666",
            "genre": "Pop",
            "duration_played_sec": random.randint(31, 35),
            "device_type": random.choice(DEVICES),  # bots disguise as normal devices
            "timestamp": now,
        }

    return {
        "event_id": fake.uuid4(),
        "user_id": random.choice(USER_POOL),
        "track_id": f"track_{random.randint(1, 100)}",
        "artist_id": f"artist_{random.randint(1, 20)}",
        "genre": random.choice(GENRES),
        "duration_played_sec": random.randint(10, 240),
        "device_type": random.choice(DEVICES),
        "timestamp": now,
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"Producing plays to '{config.STREAMS_TOPIC}'. Ctrl+C to stop.")
    sent = 0
    try:
        while True:
            event = generate_event()
            # key = track_id keeps all plays of one track in one partition
            producer.send(config.STREAMS_TOPIC, key=event["track_id"], value=event)
            sent += 1
            if sent % 100 == 0:
                print(f"sent {sent} events")
            time.sleep(random.uniform(0.05, 0.2))
    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
