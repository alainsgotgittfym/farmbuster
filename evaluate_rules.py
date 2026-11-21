"""Offline check of the detection rule, no Kafka or Spark needed.

Simulates many 30-second windows of the producer's traffic and compares
the current rule with the naive one ("more than 10 plays per window").

    python evaluate_rules.py --windows 2000
"""
import argparse
import random
from collections import defaultdict

import config
from producer import FRAUD_TRACK_ID, generate_event

WINDOW_SEC = 30


def simulate_window() -> dict:
    """Returns {track_id: [durations]} for one window of producer traffic."""
    plays = defaultdict(list)
    t = 0.0
    while True:
        t += random.uniform(0.05, 0.2)  # same pacing as the producer
        if t >= WINDOW_SEC:
            return plays
        event = generate_event()
        plays[event["track_id"]].append(event["duration_played_sec"])


def current_rule(durations: list) -> bool:
    short = sum(
        config.SHORT_PLAY_MIN_SEC <= d <= config.SHORT_PLAY_MAX_SEC for d in durations
    )
    return (
        len(durations) >= config.MIN_STREAMS_PER_WINDOW
        and short / len(durations) >= config.MIN_SHORT_PLAY_RATIO
    )


def naive_rule(durations: list) -> bool:
    return len(durations) > 10


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--windows", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    random.seed(args.seed)

    results = {"current": [0, 0], "naive": [0, 0]}  # [caught attacks, false alarms]
    attacked = 0
    for _ in range(args.windows):
        window = simulate_window()
        if FRAUD_TRACK_ID in window:
            attacked += 1
        for name, rule in (("current", current_rule), ("naive", naive_rule)):
            for track_id, durations in window.items():
                if rule(durations):
                    idx = 0 if track_id == FRAUD_TRACK_ID else 1
                    results[name][idx] += 1

    print(f"{args.windows} windows of {WINDOW_SEC}s, attack present in {attacked}\n")
    print(f"{'rule':<10}{'caught':>10}{'recall':>10}{'false alarms':>15}")
    for name, (caught, false_alarms) in results.items():
        recall = caught / attacked if attacked else 0
        print(f"{name:<10}{caught:>10}{recall:>10.1%}{false_alarms:>15}")


if __name__ == "__main__":
    main()
