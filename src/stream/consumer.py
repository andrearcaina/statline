import json
from collections import defaultdict
from uuid import uuid4

from confluent_kafka import Consumer, KafkaException

from src.serve.client import initialize, insert

from .constants import (
    BOOTSTRAP_SERVERS,
    COMMIT_EVERY,
    END_MESSAGE_TYPE,
    GROUP_ID,
    PRINT_EVERY,
    TOPIC,
)


def consumer() -> None:
    initialize()
    replay_id = str(uuid4())
    kafka_consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    kafka_consumer.subscribe([TOPIC])
    event_counts = defaultdict(int)
    dirty = set()
    received = 0

    def save_counts():
        rows = [
            {
                "replay_id": replay_id,
                "gameId": int(game),
                "period": int(period),
                "event_count": event_counts[(game, period)],
                "received": received,
            }
            for game, period in sorted(dirty)
        ]
        insert("stream_period_counts", rows)
        dirty.clear()

    try:
        print(f"listening to {TOPIC}; ClickHouse replay_id={replay_id}", flush=True)
        while True:
            message = kafka_consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())

            row = json.loads(message.value())
            if row.get("type") == END_MESSAGE_TYPE:
                save_counts()
                kafka_consumer.commit(message=message, asynchronous=False)
                print(f"done: received {received} rows; running counts saved to ClickHouse", flush=True)
                break

            event_key = (str(row["gameId"]), str(row["period"]))
            event_counts[event_key] += 1
            dirty.add(event_key)
            received += 1

            if received % COMMIT_EVERY == 0:
                # Persist before committing offsets. Exactly-once recovery is outside Part 3.
                save_counts()
                kafka_consumer.commit(message=message, asynchronous=True)

            if received == 1 or received % PRINT_EVERY == 0:
                print(f"received {received}: {event_key} has {event_counts[event_key]} events")
    except KeyboardInterrupt:
        print(f"stopped after {received} rows")
    finally:
        kafka_consumer.close()


if __name__ == "__main__":
    consumer()
