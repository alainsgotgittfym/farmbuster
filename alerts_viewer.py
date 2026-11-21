"""Prints fraud alerts from Kafka as they arrive."""
import json

from kafka import KafkaConsumer

import config


def main() -> None:
    consumer = KafkaConsumer(
        config.ALERTS_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    print(f"Waiting for alerts in '{config.ALERTS_TOPIC}'. Ctrl+C to stop.")
    try:
        for message in consumer:
            a = message.value
            print(
                f"[ALERT] {a['track_id']}  {a['window_start']} -> {a['window_end']}  "
                f"plays={a['stream_count']}  users={a['unique_users']}  "
                f"short_plays={a['short_play_ratio']:.0%}"
            )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
