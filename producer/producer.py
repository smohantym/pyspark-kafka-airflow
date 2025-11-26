# producer/producer.py
import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except ValueError:
        return default


def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "events")
    msgs_per_sec = env_int("MSGS_PER_SEC", 5)

    print(f"Producer starting. Kafka={bootstrap_servers}, topic={topic}, msgs/sec={msgs_per_sec}")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
    )

    try:
        while True:
            start = time.time()
            for _ in range(msgs_per_sec):
                event = {
                    "id": random.randint(0, 4),
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "value": round(random.uniform(0, 100), 3),
                    "source": "sim-producer",
                }
                producer.send(topic, value=event)
            producer.flush()
            elapsed = time.time() - start
            sleep_for = max(0.0, 1.0 - elapsed)
            time.sleep(sleep_for)
    except KeyboardInterrupt:
        print("Producer interrupted, closing...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
