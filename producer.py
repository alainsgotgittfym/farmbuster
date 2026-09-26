import json
import random
import time
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

GENRES = ['Pop', 'Hip-Hop', 'Rock', 'Electronic', 'Indie', 'Metal', 'R&B']
USER_POOL = [fake.uuid4() for _ in range(500)]

def generate_event():
    is_farm_attack = random.random() < 0.05
    
    if is_farm_attack:
        return {
            "event_id": fake.uuid4(),
            "user_id": fake.uuid4(),
            "track_id": "track_bot_farm_victim_99",
            "artist_id": "artist_fraudster_666",
            "genre": "Pop",
            "duration_played_sec": random.randint(31, 35),
            "device_type": "botnet_node",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    return {
        "event_id": fake.uuid4(),
        "user_id": random.choice(USER_POOL),
        "track_id": f"track_{random.randint(1, 100)}",
        "artist_id": f"artist_{random.randint(1, 20)}",
        "genre": random.choice(GENRES),
        "duration_played_sec": random.randint(10, 240),
        "device_type": random.choice(['ios', 'android', 'web', 'desktop']),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

if __name__ == "__main__":
    print("Генератор стримов'music_streams'...")
    try:
        while True:
            event = generate_event()
            producer.send('music_streams', value=event)
            time.sleep(random.uniform(0.05, 0.2))
    except KeyboardInterrupt:
        print("\nЗатормозили продьюс.")
        producer.close()