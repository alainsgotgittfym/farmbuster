"""Shared settings for producer, detector and alert viewer.

Every value can be overridden with an environment variable or a local .env file.
"""
import os

from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
STREAMS_TOPIC = os.getenv("STREAMS_TOPIC", "music_streams")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "fraud_alerts")

# Detection rules
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "30 seconds")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "30 seconds")
MIN_STREAMS_PER_WINDOW = int(os.getenv("MIN_STREAMS_PER_WINDOW", "6"))
# Plays that barely pass the "counts as a stream" mark (30 s) are typical for bot farms
SHORT_PLAY_MIN_SEC = int(os.getenv("SHORT_PLAY_MIN_SEC", "30"))
SHORT_PLAY_MAX_SEC = int(os.getenv("SHORT_PLAY_MAX_SEC", "36"))
MIN_SHORT_PLAY_RATIO = float(os.getenv("MIN_SHORT_PLAY_RATIO", "0.7"))

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "./chk_point")
