import json
from collections import defaultdict

from confluent_kafka import Consumer, KafkaException

from .constants import (
    BOOTSTRAP_SERVERS,
    COMMIT_EVERY,
    END_MESSAGE_TYPE,
    GROUP_ID,
    PRINT_EVERY,
    TOPIC,
)


def consumer() -> None:
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
    received = 0

    try:
        print(f"listening to {TOPIC}")
        while True:
            message = kafka_consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())

            row = json.loads(message.value())
            if row.get("type") == END_MESSAGE_TYPE:
                kafka_consumer.commit(message=message, asynchronous=False)
                print(f"done: received {received} rows")
                break

            event_key = (str(row["gameId"]), str(row["period"]))
            event_counts[event_key] += 1
            received += 1

            if received % COMMIT_EVERY == 0:
                kafka_consumer.commit(message=message, asynchronous=True)

            if received == 1 or received % PRINT_EVERY == 0:
                print(f"received {received}: {event_key} has {event_counts[event_key]} events")
    except KeyboardInterrupt:
        print(f"stopped after {received} rows")
    finally:
        kafka_consumer.close()


if __name__ == "__main__":
    consumer()
